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
    // Valid base64 has at most 2 padding characters; more means invalid input
    if padding > 2 {
        return 0;
    }
    (len * 3) / 4 - padding
}
