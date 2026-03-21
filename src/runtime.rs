use std::future::Future;

#[cfg(any(
    not(target_arch = "wasm32"),
    all(target_os = "wasi", target_env = "p2", feature = "rt"),
))]
pub fn spawn_background<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(all(
    target_arch = "wasm32",
    feature = "wasm",
    not(all(target_os = "wasi", target_env = "p2"))
))]
pub fn spawn_background<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

#[cfg(any(
    not(target_arch = "wasm32"),
    all(target_os = "wasi", target_env = "p2", feature = "rt"),
))]
pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

#[cfg(all(
    target_arch = "wasm32",
    feature = "wasm",
    not(all(target_os = "wasi", target_env = "p2"))
))]
pub async fn sleep_ms(_ms: u64) {}
