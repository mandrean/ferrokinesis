#![cfg(feature = "mirror")]

mod common;
use common::*;

use axum::Extension;
use ferrokinesis::mirror::{Mirror, RetryConfig};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Helper: start a ferrokinesis server with mirroring enabled, returning its address.
async fn start_primary_with_mirror(
    mirror_target_url: &str,
    diff: bool,
) -> (std::net::SocketAddr, ferrokinesis::store::Store) {
    start_primary_with_mirror_retry(mirror_target_url, diff, RetryConfig::default()).await
}

/// Helper: start a ferrokinesis server with mirroring and custom retry config.
async fn start_primary_with_mirror_retry(
    mirror_target_url: &str,
    diff: bool,
    retry_config: RetryConfig,
) -> (std::net::SocketAddr, ferrokinesis::store::Store) {
    let creds = aws_credential_types::Credentials::new("AKID", "secret", None, None, "mirror-test");
    let mirror = Mirror::with_credentials(
        mirror_target_url,
        diff,
        "us-east-1",
        Some(creds),
        Mirror::DEFAULT_CONCURRENCY,
        retry_config,
    );

    let options = ferrokinesis::store::StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    };
    let (app, store) = ferrokinesis::create_app(options);
    let app = app.layer(Extension(Arc::new(mirror)));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, store)
}

/// Helper: poll until the expected number of records are available.
async fn wait_for_records(target: &TestServer, stream: &str, shard: &str, expected: usize) {
    for _ in 0..50 {
        let iter = target
            .get_shard_iterator(stream, shard, "TRIM_HORIZON")
            .await;
        let records = target.get_records(&iter).await;
        if records["Records"].as_array().map_or(0, |a| a.len()) >= expected {
            return;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }
    panic!("expected {expected} records in {stream}/{shard}, timed out");
}

/// Helper: poll until the stream is ACTIVE on the given endpoint.
async fn wait_active(client: &reqwest::Client, url: &str, stream_name: &str) {
    for _ in 0..20 {
        let res = kinesis_request(
            client,
            url,
            "DescribeStream",
            &serde_json::json!({"StreamName": stream_name}),
        )
        .await;
        if res.status() == 200 {
            let body: serde_json::Value = res.json().await.unwrap();
            if body["StreamDescription"]["StreamStatus"].as_str() == Some("ACTIVE") {
                return;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    panic!("stream {stream_name} did not become ACTIVE");
}

/// Helper: make a signed Kinesis API request to a given URL.
async fn kinesis_request(
    client: &reqwest::Client,
    url: &str,
    target: &str,
    data: &serde_json::Value,
) -> reqwest::Response {
    client
        .post(url)
        .header("Content-Type", AMZ_JSON)
        .header("X-Amz-Target", format!("{VERSION}.{target}"))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        )
        .header("X-Amz-Date", "20150101T000000Z")
        .body(serde_json::to_vec(data).unwrap())
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_mirror_forwards_put_record() {
    // Start mirror target
    let target = TestServer::new().await;
    target.create_stream("mirror-test", 1).await;

    // Start primary with mirror pointing to target
    let (primary_addr, _store) = start_primary_with_mirror(&target.url(), false).await;
    let primary_url = format!("http://{primary_addr}");
    let client = reqwest::Client::new();

    // Create same stream on primary
    let res = kinesis_request(
        &client,
        &primary_url,
        "CreateStream",
        &json!({"StreamName": "mirror-test", "ShardCount": 1}),
    )
    .await;
    assert_eq!(res.status(), 200);
    wait_active(&client, &primary_url, "mirror-test").await;

    // PutRecord to primary
    let res = kinesis_request(
        &client,
        &primary_url,
        "PutRecord",
        &json!({
            "StreamName": "mirror-test",
            "Data": "dGVzdA==",
            "PartitionKey": "pk1"
        }),
    )
    .await;
    assert_eq!(res.status(), 200);

    // Wait for the mirrored record to arrive
    wait_for_records(&target, "mirror-test", "shardId-000000000000", 1).await;
}

#[tokio::test]
async fn test_mirror_forwards_put_records() {
    let target = TestServer::new().await;
    target.create_stream("mirror-batch", 1).await;

    let (primary_addr, _store) = start_primary_with_mirror(&target.url(), false).await;
    let primary_url = format!("http://{primary_addr}");
    let client = reqwest::Client::new();

    let res = kinesis_request(
        &client,
        &primary_url,
        "CreateStream",
        &json!({"StreamName": "mirror-batch", "ShardCount": 1}),
    )
    .await;
    assert_eq!(res.status(), 200);
    wait_active(&client, &primary_url, "mirror-batch").await;

    // PutRecords to primary
    let res = kinesis_request(
        &client,
        &primary_url,
        "PutRecords",
        &json!({
            "StreamName": "mirror-batch",
            "Records": [
                {"Data": "cmVjMQ==", "PartitionKey": "pk1"},
                {"Data": "cmVjMg==", "PartitionKey": "pk2"}
            ]
        }),
    )
    .await;
    assert_eq!(res.status(), 200);

    // Wait for the mirrored records to arrive
    wait_for_records(&target, "mirror-batch", "shardId-000000000000", 2).await;
}

#[tokio::test]
async fn test_mirror_does_not_forward_non_write_operations() {
    // Create a sentinel stream on both sides to act as a mirror-pipeline barrier.
    let target = TestServer::new().await;
    target.create_stream("sentinel", 1).await;

    let (primary_addr, _store) = start_primary_with_mirror(&target.url(), false).await;
    let primary_url = format!("http://{primary_addr}");
    let client = reqwest::Client::new();

    let res = kinesis_request(
        &client,
        &primary_url,
        "CreateStream",
        &json!({"StreamName": "sentinel", "ShardCount": 1}),
    )
    .await;
    assert_eq!(res.status(), 200);
    wait_active(&client, &primary_url, "sentinel").await;

    // CreateStream on primary — should NOT be mirrored
    let res = kinesis_request(
        &client,
        &primary_url,
        "CreateStream",
        &json!({"StreamName": "no-mirror", "ShardCount": 1}),
    )
    .await;
    assert_eq!(res.status(), 200);

    // Send a PutRecord (which IS mirrored) as a pipeline flush barrier.
    // Once this arrives at the target, anything queued before it has been processed.
    let res = kinesis_request(
        &client,
        &primary_url,
        "PutRecord",
        &json!({
            "StreamName": "sentinel",
            "Data": "YmFycmllcg==",
            "PartitionKey": "barrier"
        }),
    )
    .await;
    assert_eq!(res.status(), 200);
    wait_for_records(&target, "sentinel", "shardId-000000000000", 1).await;

    // Verify stream does NOT exist on mirror target — deterministic, no sleep needed
    let res = target
        .request("DescribeStream", &json!({"StreamName": "no-mirror"}))
        .await;
    let (status, _) = decode_body(res).await;
    assert_eq!(
        status, 400,
        "Stream should not exist on mirror target because CreateStream is not mirrored"
    );
}

#[tokio::test]
async fn test_mirror_retries_on_transient_failure() {
    // Shared counter: flaky mock returns 503 for the first `fail_count` requests, then proxies.
    let fail_count: usize = 2;
    let counter = Arc::new(AtomicUsize::new(0));

    // Start the real mirror target (ferrokinesis instance).
    let target = TestServer::new().await;
    target.create_stream("retry-test", 1).await;
    let target_url = target.url();

    // Build a flaky proxy: 503 for first N hits, then forward to the real target.
    let counter_clone = Arc::clone(&counter);
    let flaky_app = axum::Router::new().fallback(move |req: axum::extract::Request| {
        let counter = Arc::clone(&counter_clone);
        let target_url = target_url.clone();
        async move {
            let hit = counter.fetch_add(1, Ordering::SeqCst);
            if hit < fail_count {
                return axum::http::Response::builder()
                    .status(503)
                    .body(axum::body::Body::from("service unavailable"))
                    .unwrap();
            }
            // Proxy to real target
            let client = reqwest::Client::new();
            let (parts, body) = req.into_parts();
            let body_bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
            let mut proxy_req = client.post(&target_url).body(body_bytes.to_vec());
            for (name, value) in &parts.headers {
                proxy_req = proxy_req.header(name.as_str(), value.as_bytes());
            }
            let resp = proxy_req.send().await.unwrap();
            let status = resp.status();
            let resp_body = resp.bytes().await.unwrap();
            axum::http::Response::builder()
                .status(status.as_u16())
                .body(axum::body::Body::from(resp_body))
                .unwrap()
        }
    });

    let flaky_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let flaky_addr = flaky_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(flaky_listener, flaky_app).await.unwrap();
    });

    let flaky_url = format!("http://{flaky_addr}");

    // Start primary with mirror pointing to the flaky proxy, fast backoff for test speed.
    let retry_config = RetryConfig {
        max_retries: 3,
        initial_backoff: std::time::Duration::from_millis(10),
        max_backoff: std::time::Duration::from_millis(50),
    };
    let (primary_addr, _store) =
        start_primary_with_mirror_retry(&flaky_url, false, retry_config).await;
    let primary_url = format!("http://{primary_addr}");
    let client = reqwest::Client::new();

    // Create same stream on primary
    let res = kinesis_request(
        &client,
        &primary_url,
        "CreateStream",
        &json!({"StreamName": "retry-test", "ShardCount": 1}),
    )
    .await;
    assert_eq!(res.status(), 200);
    wait_active(&client, &primary_url, "retry-test").await;

    // PutRecord to primary — mirror will hit 503 twice, then succeed on retry
    let res = kinesis_request(
        &client,
        &primary_url,
        "PutRecord",
        &json!({
            "StreamName": "retry-test",
            "Data": "cmV0cnk=",
            "PartitionKey": "pk-retry"
        }),
    )
    .await;
    assert_eq!(res.status(), 200);

    // Wait for the mirrored record to arrive at the real target via retry
    wait_for_records(&target, "retry-test", "shardId-000000000000", 1).await;

    // Verify retries happened: counter should be > fail_count (fail_count failures + 1 success)
    let total_hits = counter.load(Ordering::SeqCst);
    assert!(
        total_hits > fail_count,
        "expected retries: counter should be > {fail_count}, got {total_hits}"
    );
}
