use std::future::Future;

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn_background<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
pub fn spawn_background<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep_ms(ms: u64) {
    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
}

#[cfg(all(target_arch = "wasm32", feature = "wasm"))]
pub async fn sleep_ms(ms: u64) {
    use js_sys::{Function, Promise, Reflect};
    use wasm_bindgen::{JsCast, JsValue};

    let global = js_sys::global();
    let set_timeout = Reflect::get(&global, &JsValue::from_str("setTimeout"))
        .expect("globalThis.setTimeout unavailable")
        .dyn_into::<Function>()
        .expect("globalThis.setTimeout is not a function");

    let delay = JsValue::from_f64(ms.min(i32::MAX as u64) as f64);
    let promise = Promise::new(&mut |resolve, _reject| {
        set_timeout
            .call2(&global, &resolve, &delay)
            .expect("globalThis.setTimeout call failed");
    });

    wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .expect("globalThis.setTimeout promise rejected");
}
