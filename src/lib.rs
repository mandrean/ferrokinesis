pub mod actions;
pub mod error;
pub mod sequence;
pub mod server;
pub mod shard_iterator;
pub mod store;
pub mod types;
pub mod util;
pub mod validation;

use axum::Router;
use axum::routing::any;
use store::{Store, StoreOptions};

pub fn create_app(options: StoreOptions) -> (Router, Store) {
    let store = Store::new(options);
    let app = Router::new()
        .fallback(any(server::handler))
        .with_state(store.clone());
    (app, store)
}
