//! # ferrokinesis
//!
//! A local AWS Kinesis Data Streams emulator written in Rust.
//! Aims to exactly match real AWS Kinesis behavior, making it suitable for
//! local development and integration testing without a live AWS account.
//!
//! ## Quick start
//!
//! ```no_run
//! use ferrokinesis::{create_app, store::StoreOptions};
//!
//! #[tokio::main]
//! async fn main() {
//!     let (app, _store) = create_app(StoreOptions::default());
//!
//!     let listener = tokio::net::TcpListener::bind("127.0.0.1:4567").await.unwrap();
//!     axum::serve(listener, app).await.unwrap();
//! }
//! ```
//!
//! ## Architecture
//!
//! HTTP POST requests arrive at [`server::handler`], which parses the `X-Amz-Target`
//! header to determine the [`actions::Operation`], deserializes the JSON/CBOR body,
//! runs validation, then routes to the appropriate action handler in [`actions`].
//!
//! All persistent state lives in an in-memory [redb](https://docs.rs/redb) database
//! wrapped by [`store::Store`].
//!
//! ## AWS Kinesis documentation
//!
//! See the [Amazon Kinesis Data Streams API Reference](https://docs.aws.amazon.com/kinesis/latest/APIReference/Welcome.html)
//! for the full API specification.

#![warn(missing_docs)]

pub mod actions;
pub mod capture;
pub mod config;
#[doc(hidden)]
pub mod constants;
pub mod error;
#[doc(hidden)]
pub mod event_stream;
pub mod health;
#[cfg(feature = "mirror")]
#[doc(hidden)]
pub mod mirror;
#[doc(hidden)]
pub mod retention;
#[doc(hidden)]
pub mod sequence;
pub mod server;
#[doc(hidden)]
pub mod shard_iterator;
pub mod store;
pub mod types;
#[doc(hidden)]
pub mod util;
#[doc(hidden)]
pub mod validation;

use axum::Router;
use axum::middleware;
use axum::routing::{any, get};
use store::{Store, StoreOptions};
#[cfg(feature = "access-log")]
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};

/// Creates an Axum [`Router`] and a [`store::Store`] ready to serve the Kinesis emulator.
///
/// The router exposes:
/// - `GET /_health` — aggregated health endpoint (see [`health::health`])
/// - `GET /_health/live` — liveness probe (see [`health::live`])
/// - `GET /_health/ready` — readiness probe (see [`health::ready`])
/// - `POST /` (fallback) — the Kinesis wire protocol handler (see [`server::handler`])
///
/// If [`store::StoreOptions::retention_check_interval_secs`] is non-zero, a background
/// reaper task is spawned to periodically delete records that have exceeded the
/// stream's retention period.
///
/// # Examples
///
/// ```no_run
/// use ferrokinesis::{create_app, store::StoreOptions};
///
/// #[tokio::main]
/// async fn main() {
///     let (app, _store) = create_app(StoreOptions::default());
///
///     let listener = tokio::net::TcpListener::bind("127.0.0.1:4567").await.unwrap();
///     axum::serve(listener, app).await.unwrap();
/// }
/// ```
pub fn create_app(options: StoreOptions) -> (Router, Store) {
    create_app_with_capture(options, None)
}

/// Like [`create_app`], but accepts an optional [`capture::CaptureWriter`] to record
/// PutRecord/PutRecords calls to an NDJSON file.
pub fn create_app_with_capture(
    options: StoreOptions,
    capture: Option<capture::CaptureWriter>,
) -> (Router, Store) {
    let store = Store::with_capture(options.clone(), capture);
    let app = Router::new()
        .route("/_health", get(health::health))
        .route("/_health/live", get(health::live))
        .route("/_health/ready", get(health::ready))
        .fallback(any(server::handler))
        .with_state(store.clone())
        .layer(middleware::from_fn(server::kinesis_413_middleware));

    #[cfg(feature = "access-log")]
    let app = app.layer(
        TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new().level(tracing::Level::INFO))
            .on_response(DefaultOnResponse::new().level(tracing::Level::INFO)),
    );

    if options.retention_check_interval_secs > 0 {
        let reaper_store = store.clone();
        tokio::spawn(retention::run_reaper(
            reaper_store,
            options.retention_check_interval_secs,
        ));
    }

    (app, store)
}
