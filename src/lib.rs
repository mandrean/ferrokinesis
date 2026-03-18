pub mod actions;
pub mod config;
pub mod constants;
pub mod error;
pub mod event_stream;
pub mod health;
pub mod sequence;
pub mod server;
pub mod shard_iterator;
pub mod store;
pub mod types;
pub mod util;
pub mod validation;

use axum::Router;
use axum::routing::{any, get};
use store::{Store, StoreOptions};

pub fn create_app(options: StoreOptions) -> (Router, Store) {
    let store = Store::new(options);
    let app = Router::new()
        .route("/_health", get(health::health))
        .route("/_health/live", get(health::live))
        .route("/_health/ready", get(health::ready))
        .fallback(any(server::handler))
        .with_state(store.clone());
    (app, store)
}
