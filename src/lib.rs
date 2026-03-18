pub mod actions;
pub mod config;
pub mod constants;
pub mod error;
pub mod event_stream;
pub mod health;
pub mod retention;
pub mod sequence;
pub mod server;
pub mod shard_iterator;
pub mod store;
pub mod types;
pub mod util;
pub mod validation;

use axum::Router;
use axum::middleware;
use axum::routing::{any, get};
use store::{Store, StoreOptions};

pub fn create_app(options: StoreOptions) -> (Router, Store) {
    let store = Store::new(options.clone());
    let app = Router::new()
        .route("/_health", get(health::health))
        .route("/_health/live", get(health::live))
        .route("/_health/ready", get(health::ready))
        .fallback(any(server::handler))
        .with_state(store.clone())
        .layer(middleware::from_fn(server::kinesis_413_middleware));

    if options.retention_check_interval_secs > 0 {
        let reaper_store = store.clone();
        tokio::spawn(retention::run_reaper(
            reaper_store,
            options.retention_check_interval_secs,
        ));
    }

    (app, store)
}
