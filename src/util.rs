/// Returns the current time in milliseconds since the Unix epoch.
#[cfg(not(all(feature = "wasm", target_arch = "wasm32", not(target_os = "wasi"))))]
pub fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Returns the current time in milliseconds since the Unix epoch.
#[cfg(all(feature = "wasm", target_arch = "wasm32", not(target_os = "wasi")))]
pub fn current_time_ms() -> u64 {
    js_sys::Date::now() as u64
}

pub use ferrokinesis_core::util::base64_decoded_len;
