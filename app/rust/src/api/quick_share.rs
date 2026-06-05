use crate::frb_generated::StreamSink;
use flutter_rust_bridge::frb;
use rqs_lib::channel::{ChannelAction, ChannelDirection, ChannelMessage};
use rqs_lib::{EndpointInfo, OutboundPayload, RQS, SendInfo, Visibility};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};
use tokio::sync::broadcast;

// ---------------------------------------------------------------------------
// Global state — Quick Share 서비스 인스턴스와 연관된 송신자들을 보관
// ---------------------------------------------------------------------------
static QS_STATE: LazyLock<Mutex<Option<QsState>>> = LazyLock::new(|| Mutex::new(None));

struct QsState {
    rqs: RQS,
    /// Outbound sends go through this sender (obtained from RQS::run).
    send_info_tx: tokio::sync::mpsc::Sender<SendInfo>,
    /// Clone of `rqs.message_sender` so stream functions can subscribe.
    message_tx: broadcast::Sender<ChannelMessage>,
}

// ---------------------------------------------------------------------------
// FRB mirror types — rqs_lib types that Dart needs as parameter/return types
// ---------------------------------------------------------------------------

#[frb(mirror(Visibility))]
#[derive(Clone, Copy)]
pub enum _Visibility {
    Visible = 0,
    Invisible = 1,
    Temporarily = 2,
}

// ---------------------------------------------------------------------------
// Dart-facing DTOs
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize)]
pub struct QsDeviceInfo {
    pub id: String,
    pub name: Option<String>,
    pub ip: Option<String>,
    pub port: Option<String>,
    pub device_type: Option<i32>,
    pub present: Option<bool>,
}

impl From<EndpointInfo> for QsDeviceInfo {
    fn from(ei: EndpointInfo) -> Self {
        Self {
            id: ei.id,
            name: ei.name,
            ip: ei.ip,
            port: ei.port,
            device_type: ei.rtype.map(|dt| dt as i32),
            present: ei.present,
        }
    }
}

#[derive(Clone, Serialize)]
pub struct QsTransferMessage {
    pub id: String,
    /// 0 = FrontToLib, 1 = LibToFront
    pub direction: i32,
    /// 0 = AcceptTransfer, 1 = RejectTransfer, 2 = CancelTransfer
    pub action: Option<i32>,
    /// 0 = Inbound, 1 = Outbound
    pub rtype: Option<i32>,
    /// Protocol state machine variant (see rqs_lib State enum)
    pub state: Option<i32>,
    /// File names being transferred
    pub file_names: Option<Vec<String>>,
    /// Total transfer size in bytes
    pub total_bytes: Option<i64>,
    /// Acknowledged bytes so far
    pub ack_bytes: Option<i64>,
    /// 4-digit pin code for verification
    pub pin_code: Option<String>,
    /// Name of the sending device
    pub sender_name: Option<String>,
    /// Text payload (URL, text, WiFi credentials)
    pub text_payload: Option<String>,
}

impl From<ChannelMessage> for QsTransferMessage {
    fn from(msg: ChannelMessage) -> Self {
        let (file_names, total_bytes, ack_bytes, pin_code, sender_name, text_payload) =
            if let Some(ref meta) = msg.meta {
                (
                    meta.files.clone(),
                    Some(meta.total_bytes as i64),
                    Some(meta.ack_bytes as i64),
                    meta.pin_code.clone(),
                    meta.source.as_ref().map(|s| s.name.clone()),
                    meta.text_payload.clone(),
                )
            } else {
                (None, None, None, None, None, None)
            };

        Self {
            id: msg.id,
            direction: msg.direction as i32,
            action: msg.action.map(|a| a as i32),
            rtype: msg.rtype.map(|t| t as i32),
            state: msg.state.map(|s| s as i32),
            file_names,
            total_bytes,
            ack_bytes,
            pin_code,
            sender_name,
            text_payload,
        }
    }
}

// ---------------------------------------------------------------------------
// Quick Share 서비스 생명주기 API
// ---------------------------------------------------------------------------

/// Quick Share 서비스를 생성한다.
/// visibility: 0=Visible, 1=Invisible
/// download_path: 수신 파일 저장 경로 (None이면 기본 다운로드 폴더)
#[frb(sync)]
pub fn qs_create_service(visibility: i32, download_path: Option<String>) -> Result<(), String> {
    let vis = match visibility {
        0 => Visibility::Visible,
        1 => Visibility::Invisible,
        _ => Visibility::Visible,
    };

    let path = download_path.map(PathBuf::from);
    let rqs = RQS::new(vis, None, path);
    let message_tx = rqs.message_sender.clone();
    let (dummy_tx, _) = tokio::sync::mpsc::channel(1);

    let mut guard = QS_STATE.lock().map_err(|e| e.to_string())?;
    if guard.is_some() {
        return Err("Quick Share service already created. Call qs_stop_service first.".to_string());
    }
    *guard = Some(QsState {
        rqs,
        send_info_tx: dummy_tx,
        message_tx,
    });
    Ok(())
}

/// Quick Share 서버를 시작한다 (mDNS 광고 + TCP 리스너).
/// qs_create_service()가 먼저 호출되어야 한다.
pub async fn qs_start_service() -> Result<(), String> {
    // MutexGuard는 Send가 아니므로 await 전에 rqs를 빼내고, await 후에 다시 넣는다.
    let mut rqs = {
        let mut guard = QS_STATE.lock().map_err(|e| e.to_string())?;
        let state = guard
            .as_mut()
            .ok_or("Quick Share service not created. Call qs_create_service first.")?;
        std::mem::replace(&mut state.rqs, RQS::new(Visibility::Invisible, None, None))
    };

    // RQS::run() spawns background tasks (mDNS server, TCP server, BLE listener)
    let (tx, _ble_rx) = rqs.run().await.map_err(|e| e.to_string())?;

    // Put rqs back and store the sender
    let mut guard = QS_STATE.lock().map_err(|e| e.to_string())?;
    if let Some(state) = guard.as_mut() {
        state.rqs = rqs;
        state.send_info_tx = tx;
    }

    Ok(())
}

/// Quick Share 서비스를 중지한다. 백그라운드 태스크들을 모두 정리한다.
pub async fn qs_stop_service() -> Result<(), String> {
    let mut rqs = {
        let mut guard = QS_STATE.lock().map_err(|e| e.to_string())?;
        match guard.as_mut() {
            Some(state) => {
                std::mem::replace(&mut state.rqs, RQS::new(Visibility::Invisible, None, None))
            }
            None => return Ok(()),
        }
    };

    rqs.stop().await;

    let mut guard = QS_STATE.lock().map_err(|e| e.to_string())?;
    *guard = None;
    Ok(())
}

/// Quick Share 가시성을 변경한다.
/// visibility: 0=Visible, 1=Invisible, 2=Temporarily (60초 후 Invisible로 전환)
#[frb(sync)]
pub fn qs_change_visibility(visibility: i32) -> Result<(), String> {
    let vis = match visibility {
        0 => Visibility::Visible,
        1 => Visibility::Invisible,
        2 => Visibility::Temporarily,
        _ => return Err(format!("Invalid visibility value: {}", visibility)),
    };
    let mut guard = QS_STATE.lock().map_err(|e| e.to_string())?;
    if let Some(state) = guard.as_mut() {
        state.rqs.change_visibility(vis);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 기기 검색 스트림
// ---------------------------------------------------------------------------

/// Quick Share 기기 검색을 시작하고 발견된 기기들을 Dart Stream으로 전달한다.
/// 각 기기는 JSON 문자열로 직렬화되어 전송된다.
/// Dart 측에서 `jsonDecode`로 파싱하여 사용한다.
pub fn qs_start_discovery(sink: StreamSink<String>) -> Result<(), String> {
    let (tx, mut rx) = broadcast::channel::<EndpointInfo>(50);

    {
        let mut guard = QS_STATE.lock().map_err(|e| e.to_string())?;
        let state = guard
            .as_mut()
            .ok_or("Quick Share service not created. Call qs_create_service first.")?;
        state.rqs.discovery(tx).map_err(|e| e.to_string())?;
    }

    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(info) => {
                    let qs_info: QsDeviceInfo = info.into();
                    if let Ok(json) = serde_json::to_string(&qs_info) {
                        if sink.add(json).is_err() {
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Discovery broadcast receiver lagged by {} messages", n);
                }
            }
        }
    });

    Ok(())
}

// ---------------------------------------------------------------------------
// 전송 상태 채널 리스너
// ---------------------------------------------------------------------------

/// Quick Share 전송 상태 (ChannelMessage)를 Dart Stream으로 전달한다.
/// LibToFront 방향의 메시지만 필터링하며, JSON 문자열로 전송된다.
pub fn qs_start_channel_listener(sink: StreamSink<String>) -> Result<(), String> {
    let mut rx = {
        let guard = QS_STATE.lock().map_err(|e| e.to_string())?;
        let state = guard
            .as_ref()
            .ok_or("Quick Share service not created. Call qs_create_service first.")?;
        state.message_tx.subscribe()
    };

    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    if msg.direction == ChannelDirection::LibToFront {
                        let qs_msg: QsTransferMessage = msg.into();
                        if let Ok(json) = serde_json::to_string(&qs_msg) {
                            if sink.add(json).is_err() {
                                break;
                            }
                        }
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Channel broadcast receiver lagged by {} messages", n);
                }
            }
        }
    });

    Ok(())
}

// ---------------------------------------------------------------------------
// 전송 제어 (Accept / Reject / Cancel)
// ---------------------------------------------------------------------------

#[frb(sync)]
pub fn qs_accept_transfer(id: String) -> Result<(), String> {
    send_action(id, ChannelAction::AcceptTransfer)
}

#[frb(sync)]
pub fn qs_reject_transfer(id: String) -> Result<(), String> {
    send_action(id, ChannelAction::RejectTransfer)
}

#[frb(sync)]
pub fn qs_cancel_transfer(id: String) -> Result<(), String> {
    send_action(id, ChannelAction::CancelTransfer)
}

fn send_action(id: String, action: ChannelAction) -> Result<(), String> {
    let guard = QS_STATE.lock().map_err(|e| e.to_string())?;
    let state = guard
        .as_ref()
        .ok_or("Quick Share service not created")?;
    let msg = ChannelMessage {
        id,
        direction: ChannelDirection::FrontToLib,
        action: Some(action),
        ..Default::default()
    };
    state.message_tx.send(msg).map_err(|e| e.to_string())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Outbound 파일 전송
// ---------------------------------------------------------------------------

/// Quick Share 프로토콜로 파일을 전송한다.
/// `ip`, `port`: 상대방 기기의 주소
/// `name`: 상대방 기기의 이름
/// `files`: 전송할 파일 경로 목록
pub async fn qs_send_files(
    ip: String,
    port: u16,
    name: String,
    files: Vec<String>,
) -> Result<(), String> {
    let addr = format!("{}:{}", ip, port);
    let send_info = SendInfo {
        id: addr.clone(),
        name,
        addr,
        ob: OutboundPayload::Files(files),
    };

    let tx = {
        let guard = QS_STATE.lock().map_err(|e| e.to_string())?;
        let state = guard
            .as_ref()
            .ok_or("Quick Share service not created. Call qs_create_service first.")?;
        state.send_info_tx.clone()
    };

    tx.send(send_info).await.map_err(|e| e.to_string())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// 헬퍼
// ---------------------------------------------------------------------------

/// Quick Share 서비스가 생성되었는지 확인한다.
#[frb(sync)]
pub fn qs_is_service_created() -> bool {
    QS_STATE.lock().map(|g| g.is_some()).unwrap_or(false)
}

/// Quick Share 서비스가 시작되었는지 (run() 호출 여부) 확인한다.
#[frb(sync)]
pub fn qs_is_service_running() -> bool {
    QS_STATE
        .lock()
        .map(|g| g.is_some())
        .unwrap_or(false)
}
