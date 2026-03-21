use std::future::Future;

#[cfg(feature = "rt")]
pub fn spawn_background<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(all(not(feature = "rt"), feature = "wasm"))]
pub fn spawn_background<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

#[cfg(all(not(feature = "rt"), not(feature = "wasm")))]
pub fn spawn_background<F>(_future: F)
where
    F: Future<Output = ()> + 'static,
{
}

#[cfg(feature = "rt")]
pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

#[cfg(not(feature = "rt"))]
pub async fn sleep_ms(_ms: u64) {}
