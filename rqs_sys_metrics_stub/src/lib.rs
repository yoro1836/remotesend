pub mod host {
    pub fn get_hostname() -> Result<String, std::io::Error> {
        // hostname crate 사용: POSIX gethostname(2) 호환
        Ok(hostname::get()?
            .into_string()
            .unwrap_or_else(|s| s.to_string_lossy().into_owned()))
    }
}
