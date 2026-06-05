import 'dart:async';
import 'dart:convert';

import 'package:common/model/device.dart';
import 'package:localsend_app/provider/logging/discovery_logs_provider.dart';
import 'package:localsend_app/provider/network/nearby_devices_provider.dart';
import 'package:localsend_app/rust/api/quick_share.dart';
import 'package:logging/logging.dart';
import 'package:refena_flutter/refena_flutter.dart';

final _logger = Logger('QuickShare');

// ---------------------------------------------------------------------------
// Quick Share visibility enum
// ---------------------------------------------------------------------------

/// rqs_lib Visibility enum과 일치하는 값
enum QuickShareVisibility {
  visible(0),
  invisible(1),
  temporarily(2);

  final int value;
  const QuickShareVisibility(this.value);
}

// ---------------------------------------------------------------------------
// 상태 모델
// ---------------------------------------------------------------------------

class QuickShareState {
  final bool isCreated;
  final bool isRunning;
  final bool isDiscovering;
  final QuickShareVisibility visibility;

  const QuickShareState({
    required this.isCreated,
    required this.isRunning,
    required this.isDiscovering,
    required this.visibility,
  });

  static const initial = QuickShareState(
    isCreated: false,
    isRunning: false,
    isDiscovering: false,
    visibility: QuickShareVisibility.visible,
  );
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

final quickShareProvider = NotifierProvider<QuickShareService, QuickShareState>(
  (ref) => QuickShareService(),
);

class QuickShareService extends Notifier<QuickShareState> {
  StreamSubscription<String>? _discoverySub;
  StreamSubscription<String>? _channelSub;

  @override
  QuickShareState init() => QuickShareState.initial;

  /// Quick Share 서비스를 생성하고 시작한다.
  Future<void> startService({
    QuickShareVisibility visibility = QuickShareVisibility.visible,
    String? downloadPath,
  }) async {
    try {
      qsCreateService(visibility: visibility.value, downloadPath: downloadPath);
      await qsStartService();

      _logger.info('Quick Share service created and started (visibility: $visibility)');
      state = QuickShareState(
        isCreated: true,
        isRunning: true,
        isDiscovering: state.isDiscovering,
        visibility: visibility,
      );
    } catch (e) {
      _logger.warning('Failed to start Quick Share service', e);
    }
  }

  /// Quick Share 서비스를 중지한다.
  Future<void> stopService() async {
    try {
      await _stopDiscovery();
      await qsStopService();

      _logger.info('Quick Share service stopped');
      state = QuickShareState.initial;
    } catch (e) {
      _logger.warning('Failed to stop Quick Share service', e);
    }
  }

  /// Quick Share 기기 검색을 시작한다.
  /// 발견된 기기는 [nearbyDevicesProvider]에 자동 등록된다.
  Future<void> startDiscovery() async {
    if (state.isDiscovering) return;

    try {
      final stream = qsStartDiscovery();
      _discoverySub = stream.listen(_onDeviceDiscovered);

      _logger.info('Quick Share discovery started');
      state = QuickShareState(
        isCreated: state.isCreated,
        isRunning: state.isRunning,
        isDiscovering: true,
        visibility: state.visibility,
      );
    } catch (e) {
      _logger.warning('Failed to start Quick Share discovery', e);
    }
  }

  /// 채널 메시지 리스너를 시작한다 (전송 상태 업데이트).
  void startChannelListener() {
    try {
      final stream = qsStartChannelListener();
      _channelSub = stream.listen(_onChannelMessage);
    } catch (e) {
      _logger.warning('Failed to start channel listener', e);
    }
  }

  /// Visibility를 변경한다.
  void changeVisibility(QuickShareVisibility visibility) {
    try {
      qsChangeVisibility(visibility: visibility.value);

      state = QuickShareState(
        isCreated: state.isCreated,
        isRunning: state.isRunning,
        isDiscovering: state.isDiscovering,
        visibility: visibility,
      );
    } catch (e) {
      _logger.warning('Failed to change Quick Share visibility', e);
    }
  }

  /// 수신 전송을 수락한다.
  void acceptTransfer(String id) {
    try {
      qsAcceptTransfer(id: id);
    } catch (e) {
      _logger.warning('Failed to accept transfer', e);
    }
  }

  /// 수신 전송을 거절한다.
  void rejectTransfer(String id) {
    try {
      qsRejectTransfer(id: id);
    } catch (e) {
      _logger.warning('Failed to reject transfer', e);
    }
  }

  /// 전송을 취소한다.
  void cancelTransfer(String id) {
    try {
      qsCancelTransfer(id: id);
    } catch (e) {
      _logger.warning('Failed to cancel transfer', e);
    }
  }

  /// Quick Share로 파일을 보낸다.
  Future<void> sendFiles({
    required String ip,
    required int port,
    required String name,
    required List<String> files,
  }) async {
    await qsSendFiles(ip: ip, port: port, name: name, files: files);
  }

  // -----------------------------------------------------------------------
  // Private helpers
  // -----------------------------------------------------------------------

  Future<void> _stopDiscovery() async {
    await _discoverySub?.cancel();
    _discoverySub = null;
    await _channelSub?.cancel();
    _channelSub = null;
  }

  /// mDNS로 Quick Share 기기가 발견되면 호출된다.
  /// JSON 문자열을 파싱하여 [Device]로 변환하고 [nearbyDevicesProvider]에 등록한다.
  void _onDeviceDiscovered(String json) {
    try {
      final map = jsonDecode(json) as Map<String, dynamic>;
      final id = map['id'] as String?;
      final name = map['name'] as String?;
      final ip = map['ip'] as String?;
      final port = map['port'] as String?;
      final present = map['present'] as bool?;

      if (id == null || ip == null) return;
      if (present == false) {
        _logger.fine('Quick Share device departed: $name ($ip)');
        return;
      }

      final device = Device(
        signalingId: null,
        ip: ip,
        version: 'qs-1.0',
        port: int.tryParse(port ?? '0') ?? 0,
        https: false,
        fingerprint: id,
        alias: name ?? 'Unknown',
        deviceModel: null,
        deviceType: _mapDeviceType(map['device_type'] as int?),
        download: false,
        discoveryMethods: {const QuickShareDiscovery()},
      );

      ref.redux(nearbyDevicesProvider).dispatchAsync(RegisterDeviceAction(device));
      ref.notifier(discoveryLoggerProvider).addLog('[DISCOVER/QS] $name ($ip)');
    } catch (e) {
      _logger.fine('Failed to parse Quick Share device: $e');
    }
  }

  /// Quick Share 전송 상태 메시지를 처리한다.
  void _onChannelMessage(String json) {
    try {
      final map = jsonDecode(json) as Map<String, dynamic>;
      final stateValue = map['state'] as int?;
      final rtype = map['rtype'] as int?;
      final fileNames =
          (map['file_names'] as List<dynamic>?)?.map((e) => e as String).toList();
      final pinCode = map['pin_code'] as String?;
      final senderName = map['sender_name'] as String?;

      _logger.info(
        'QS Channel: state=$stateValue, rtype=$rtype, '
        'files=$fileNames, pin=$pinCode, sender=$senderName',
      );

      // State values from rqs_lib State enum:
      // 12 = WaitingForUserConsent, 13 = ReceivingFiles, 14 = SendingFiles
      // 15 = Disconnected, 16 = Rejected, 17 = Cancelled, 18 = Finished
      switch (stateValue) {
        case 12: // WaitingForUserConsent
          if (rtype == 0) {
            _handleIncomingTransfer(map);
          }
        case 18: // Finished
          _logger.info('Quick Share transfer finished');
        case 17: // Cancelled
        case 16: // Rejected
        case 15: // Disconnected
          _logger.info('Quick Share transfer ended: state=$stateValue');
      }
    } catch (e) {
      _logger.fine('Failed to parse Quick Share channel message: $e');
    }
  }

  void _handleIncomingTransfer(Map<String, dynamic> msg) {
    // TODO: UI에 수신 확인 다이얼로그 표시
    // msg['id']를 사용하여 acceptTransfer / rejectTransfer 호출
    _logger.info('Incoming Quick Share transfer: $msg');
  }

  static DeviceType _mapDeviceType(int? deviceType) {
    return switch (deviceType) {
      1 => DeviceType.mobile, // Phone
      2 => DeviceType.mobile, // Tablet
      3 => DeviceType.desktop, // Laptop
      _ => DeviceType.desktop,
    };
  }
}
