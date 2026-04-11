mod common;

use common::*;
use ferrokinesis::store::StoreOptions;
use reqwest::Method;
use reqwest::header::HeaderMap;
use serde_json::Value;
use tempfile::NamedTempFile;

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
    // Health probes from orchestrators (Docker, k8s) must not require credentials
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

#[tokio::test]
async fn metrics_returns_prometheus_text_with_runtime_counters() {
    let server = TestServer::new().await;
    let name = "metrics-runtime";
    server.create_stream(name, 1).await;
    server.put_record(name, "AAAA", "pk").await;

    let desc = server.describe_stream(name).await;
    let shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
        .as_str()
        .unwrap();
    let _ = server.get_shard_iterator(name, shard_id, "LATEST").await;

    let res = server
        .raw_request(Method::GET, "/metrics", HeaderMap::new(), vec![])
        .await;
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );

    let body = res.text().await.unwrap();
    assert!(body.contains("ferrokinesis_retained_records 1"));
    assert!(body.contains("ferrokinesis_streams 1"));
    assert!(body.contains("ferrokinesis_active_iterators 1"));
    assert!(
        body.contains("ferrokinesis_requests_total{operation=\"CreateStream\",result=\"ok\"} 1")
    );
    assert!(body.contains("ferrokinesis_requests_total{operation=\"PutRecord\",result=\"ok\"} 1"));
    assert!(
        body.contains(
            "ferrokinesis_requests_total{operation=\"GetShardIterator\",result=\"ok\"} 1"
        )
    );
}

#[tokio::test]
async fn health_ready_returns_503_when_durable_state_fails_to_initialize() {
    let state_file = NamedTempFile::new().unwrap();
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        state_dir: Some(state_file.path().to_path_buf()),
        ..Default::default()
    })
    .await;

    let ready = server
        .raw_request(Method::GET, "/_health/ready", HeaderMap::new(), vec![])
        .await;
    assert_eq!(ready.status(), 503);
    assert_eq!(ready.text().await.unwrap(), "Service Unavailable");

    let health = server
        .raw_request(Method::GET, "/_health", HeaderMap::new(), vec![])
        .await;
    assert_eq!(health.status(), 503);
    let body: Value = health.json().await.unwrap();
    assert_eq!(body["status"], "DOWN");
    assert!(
        body["components"]["store"]["detail"]
            .as_str()
            .unwrap()
            .contains("failed to initialize durable state")
    );
}
