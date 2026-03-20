mod common;
use common::*;

use axum::Extension;
use ferrokinesis::mirror::Mirror;
use serde_json::json;
use std::sync::Arc;

/// Helper: start a ferrokinesis server with mirroring enabled, returning its address.
async fn start_primary_with_mirror(
    mirror_target_url: &str,
    diff: bool,
) -> (std::net::SocketAddr, ferrokinesis::store::Store) {
    let creds = aws_credential_types::Credentials::new("AKID", "secret", None, None, "mirror-test");
    let mirror = Mirror::with_credentials(
        mirror_target_url,
        diff,
        "us-east-1",
        Some(creds),
        Mirror::DEFAULT_CONCURRENCY,
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

    // Wait for mirror task to complete
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Verify record in mirror target
    let iter = target
        .get_shard_iterator("mirror-test", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let records = target.get_records(&iter).await;
    assert!(
        !records["Records"].as_array().unwrap().is_empty(),
        "Mirror target should have the forwarded record"
    );
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

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let iter = target
        .get_shard_iterator("mirror-batch", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let records = target.get_records(&iter).await;
    let arr = records["Records"].as_array().unwrap();
    assert_eq!(
        arr.len(),
        2,
        "Mirror target should have both forwarded records"
    );
}

#[tokio::test]
async fn test_mirror_does_not_forward_non_write_operations() {
    let target = TestServer::new().await;

    let (primary_addr, _store) = start_primary_with_mirror(&target.url(), false).await;
    let primary_url = format!("http://{primary_addr}");
    let client = reqwest::Client::new();

    // CreateStream on primary — should NOT be mirrored
    let res = kinesis_request(
        &client,
        &primary_url,
        "CreateStream",
        &json!({"StreamName": "no-mirror", "ShardCount": 1}),
    )
    .await;
    assert_eq!(res.status(), 200);

    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Verify stream does NOT exist on mirror target
    let res = target
        .request("DescribeStream", &json!({"StreamName": "no-mirror"}))
        .await;
    let (status, _) = decode_body(res).await;
    assert_eq!(
        status, 400,
        "Stream should not exist on mirror target because CreateStream is not mirrored"
    );
}
