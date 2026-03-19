//! Health check endpoints for the Kinesis emulator.
//!
//! Three endpoints are registered by [`crate::create_app`]:
//! - `GET /_health` — aggregated status with component breakdown ([`health`])
//! - `GET /_health/live` — liveness probe, always 200 if the server is up ([`live`])
//! - `GET /_health/ready` — readiness probe, checks store connectivity ([`ready`])

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::store::Store;

/// `GET /_health` — aggregated health with JSON component breakdown.
pub async fn health(State(store): State<Store>) -> Response {
    match store.check_ready() {
        Ok(()) => (
            StatusCode::OK,
            axum::Json(json!({
                "status": "UP",
                "components": {
                    "store": { "status": "UP" }
                }
            })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(json!({
                "status": "DOWN",
                "components": {
                    "store": { "status": "DOWN", "detail": err.to_string() }
                }
            })),
        )
            .into_response(),
    }
}

/// `GET /_health/live` — liveness probe, always succeeds if the server is up.
pub async fn live() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}

/// `GET /_health/ready` — readiness probe, checks store connectivity.
pub async fn ready(State(store): State<Store>) -> (StatusCode, &'static str) {
    match store.check_ready() {
        Ok(()) => (StatusCode::OK, "OK"),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "Service Unavailable"),
    }
}
