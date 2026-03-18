/// Returns the current time in milliseconds since the Unix epoch.
pub fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Compute the decoded byte length of a base64 string without allocating.
pub fn base64_decoded_len(b64: &str) -> usize {
    let len = b64.len();
    if len == 0 || !len.is_multiple_of(4) {
        return 0;
    }
    let padding = b64
        .as_bytes()
        .iter()
        .rev()
        .take_while(|&&b| b == b'=')
        .count();
    (len * 3) / 4 - padding
}
