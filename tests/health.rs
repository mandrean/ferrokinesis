mod common;

use common::*;
use reqwest::Method;
use reqwest::header::HeaderMap;
use serde_json::Value;

#[tokio::test]
async fn health_returns_200_with_json() {
    let server = TestServer::new().await;
    let res = server
        .raw_request(Method::GET, "/_health", HeaderMap::new(), vec![])
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["status"], "UP");
    assert_eq!(body["components"]["store"]["status"], "UP");
}

#[tokio::test]
async fn health_live_returns_200_ok() {
    let server = TestServer::new().await;
    let res = server
        .raw_request(Method::GET, "/_health/live", HeaderMap::new(), vec![])
        .await;
    assert_eq!(res.status(), 200);
    let body = res.text().await.unwrap();
    assert_eq!(body, "OK");
}

#[tokio::test]
async fn health_ready_returns_200_ok() {
    let server = TestServer::new().await;
    let res = server
        .raw_request(Method::GET, "/_health/ready", HeaderMap::new(), vec![])
        .await;
    assert_eq!(res.status(), 200);
    let body = res.text().await.unwrap();
    assert_eq!(body, "OK");
}

#[tokio::test]
async fn health_endpoints_do_not_require_auth() {
    let server = TestServer::new().await;
    // All three should succeed without any auth headers
    for path in ["/_health", "/_health/live", "/_health/ready"] {
        let res = server
            .raw_request(Method::GET, path, HeaderMap::new(), vec![])
            .await;
        assert_eq!(res.status(), 200, "expected 200 for GET {path}");
    }
}

#[tokio::test]
async fn post_health_returns_method_not_allowed() {
    let server = TestServer::new().await;
    // POST to /_health returns 405 — axum matches the path and rejects the method
    let res = server
        .raw_request(Method::POST, "/_health", HeaderMap::new(), vec![])
        .await;
    assert_eq!(res.status(), 405);
}
