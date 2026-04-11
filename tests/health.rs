mod common;

use common::*;
use ferrokinesis::store::{DurableStateOptions, StoreOptions};
use reqwest::Method;
use reqwest::header::HeaderMap;
use serde_json::{Value, json};
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

#[cfg(not(feature = "chaos"))]
#[tokio::test]
async fn chaos_routes_are_absent_without_feature() {
    let server = TestServer::new().await;
    let res = server
        .raw_request(Method::GET, "/_chaos", HeaderMap::new(), vec![])
        .await;
    assert_eq!(res.status(), 403);
}

#[cfg(feature = "chaos")]
#[tokio::test]
async fn chaos_status_route_returns_empty_config_when_unset() {
    let server = TestServer::new().await;
    let res = server
        .raw_request(Method::GET, "/_chaos", HeaderMap::new(), vec![])
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["seed"], 0);
    assert_eq!(body["scenarios"], Value::Array(vec![]));
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
        durable: Some(DurableStateOptions {
            state_dir: state_file.path().to_path_buf(),
            snapshot_interval_secs: 0,
            max_retained_bytes: None,
        }),
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

    let (status, body) = decode_body(
        server
            .request(
                "CreateStream",
                &serde_json::json!({
                    "StreamName": "blocked-during-durable-init-failure",
                    "ShardCount": 1,
                }),
            )
            .await,
    )
    .await;
    assert_eq!(status, 500);
    assert_eq!(body["__type"], "InternalFailure");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("failed to initialize durable state")
    );
    assert!(
        !server
            .store
            .contains_stream("blocked-during-durable-init-failure")
            .await
    );

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", AMZ_JSON.parse().unwrap());
    headers.insert(
        "X-Amz-Target",
        format!("{VERSION}.CreateStream").parse().unwrap(),
    );
    let (status, body) = decode_body(
        server
            .raw_request(
                Method::POST,
                "/",
                headers,
                serde_json::to_vec(&json!({
                    "StreamName": "blocked-before-auth-check",
                    "ShardCount": 1,
                }))
                .unwrap(),
            )
            .await,
    )
    .await;
    assert_eq!(status, 500);
    assert_eq!(body["__type"], "InternalFailure");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("failed to initialize durable state")
    );
}

#[tokio::test]
async fn metrics_count_known_operation_failures_before_dispatch() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", AMZ_JSON.parse().unwrap());
    headers.insert(
        "X-Amz-Target",
        format!("{VERSION}.CreateStream").parse().unwrap(),
    );
    headers.insert(
        "Authorization",
        "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234"
            .parse()
            .unwrap(),
    );
    headers.insert("X-Amz-Date", "20150101T000000Z".parse().unwrap());

    let res = server
        .raw_request(
            Method::POST,
            "/",
            headers,
            serde_json::to_vec(&json!({
                "StreamName": "metrics-invalid-create",
                "ShardCount": "not-a-number",
            }))
            .unwrap(),
        )
        .await;
    assert_eq!(res.status(), 400);

    let body = server
        .raw_request(Method::GET, "/metrics", HeaderMap::new(), vec![])
        .await
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("ferrokinesis_requests_total{operation=\"CreateStream\",result=\"error\"} 1")
    );
}

#[tokio::test]
async fn metrics_count_known_operation_auth_and_signature_failures() {
    let server = TestServer::new().await;

    let mut missing_auth_headers = HeaderMap::new();
    missing_auth_headers.insert("Content-Type", AMZ_JSON.parse().unwrap());
    missing_auth_headers.insert(
        "X-Amz-Target",
        format!("{VERSION}.CreateStream").parse().unwrap(),
    );

    let mut invalid_signature_headers = missing_auth_headers.clone();
    invalid_signature_headers.insert(
        "Authorization",
        "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234"
            .parse()
            .unwrap(),
    );
    invalid_signature_headers.insert("X-Amz-Date", "20150101T000000Z".parse().unwrap());

    let mut incomplete_signature_headers = missing_auth_headers.clone();
    incomplete_signature_headers.insert(
        "Authorization",
        "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target"
            .parse()
            .unwrap(),
    );
    incomplete_signature_headers.insert("X-Amz-Date", "20150101T000000Z".parse().unwrap());

    assert_eq!(
        server
            .raw_request(
                Method::POST,
                "/",
                missing_auth_headers,
                serde_json::to_vec(&json!({
                    "StreamName": "metrics-missing-auth",
                    "ShardCount": 1,
                }))
                .unwrap(),
            )
            .await
            .status(),
        400
    );
    assert_eq!(
        server
            .raw_request(
                Method::POST,
                "/?X-Amz-Algorithm=AWS4-HMAC-SHA256",
                invalid_signature_headers,
                serde_json::to_vec(&json!({
                    "StreamName": "metrics-invalid-signature",
                    "ShardCount": 1,
                }))
                .unwrap(),
            )
            .await
            .status(),
        400
    );
    assert_eq!(
        server
            .raw_request(
                Method::POST,
                "/",
                incomplete_signature_headers,
                serde_json::to_vec(&json!({
                    "StreamName": "metrics-incomplete-signature",
                    "ShardCount": 1,
                }))
                .unwrap(),
            )
            .await
            .status(),
        403
    );

    let body = server
        .raw_request(Method::GET, "/metrics", HeaderMap::new(), vec![])
        .await
        .text()
        .await
        .unwrap();
    assert!(
        body.contains("ferrokinesis_requests_total{operation=\"CreateStream\",result=\"error\"} 3")
    );
    assert!(body.contains("ferrokinesis_request_failures_total{reason=\"missing_auth_token\"} 1"));
    assert!(body.contains("ferrokinesis_request_failures_total{reason=\"invalid_signature\"} 1"));
    assert!(
        body.contains("ferrokinesis_request_failures_total{reason=\"incomplete_signature\"} 1")
    );
}

#[tokio::test]
async fn metrics_count_failures_without_parsed_operation_separately() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", AMZ_JSON.parse().unwrap());
    headers.insert(
        "X-Amz-Target",
        format!("{VERSION}.NoSuchOperation").parse().unwrap(),
    );
    headers.insert(
        "Authorization",
        "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234"
            .parse()
            .unwrap(),
    );
    headers.insert("X-Amz-Date", "20150101T000000Z".parse().unwrap());

    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 400);

    let body = server
        .raw_request(Method::GET, "/metrics", HeaderMap::new(), vec![])
        .await
        .text()
        .await
        .unwrap();
    assert!(body.contains("ferrokinesis_request_failures_total{reason=\"unknown_operation\"} 1"));
}

#[tokio::test]
async fn readiness_stays_up_when_retained_byte_backpressure_is_hit() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        max_retained_bytes: Some(1),
        ..Default::default()
    })
    .await;
    let name = "retained-ready";
    server.create_stream(name, 1).await;
    server.store.metrics().set_retained(2, 1);

    let ready = server
        .raw_request(Method::GET, "/_health/ready", HeaderMap::new(), vec![])
        .await;
    assert_eq!(ready.status(), 200);
    assert_eq!(ready.text().await.unwrap(), "OK");

    let health = server
        .raw_request(Method::GET, "/_health", HeaderMap::new(), vec![])
        .await;
    assert_eq!(health.status(), 200);

    let (status, body) = decode_body(
        server
            .request(
                "PutRecord",
                &json!({
                    "StreamName": name,
                    "Data": "AAAA",
                    "PartitionKey": "pk",
                }),
            )
            .await,
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(body["__type"], "LimitExceededException");
}
