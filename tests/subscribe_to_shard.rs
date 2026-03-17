mod common;

use common::*;
use serde_json::{Value, json};

const ACCOUNT: &str = "0000-0000-0000";
const REGION: &str = "us-east-1";

fn stream_arn(name: &str) -> String {
    format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{name}")
}

// -- Validation errors (caught before action) --

#[tokio::test]
async fn subscribe_missing_consumer_arn() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ShardId": "shardId-000000000000",
                "StartingPosition": { "Type": "TRIM_HORIZON" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn subscribe_missing_shard_id() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": format!("{}/consumer/c:1", stream_arn("any")),
                "StartingPosition": { "Type": "TRIM_HORIZON" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn subscribe_missing_starting_position() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": format!("{}/consumer/c:1", stream_arn("any")),
                "ShardId": "shardId-000000000000",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

// -- Action-layer errors --

#[tokio::test]
async fn subscribe_consumer_not_found() {
    let server = TestServer::new().await;
    let fake_arn = format!("{}/consumer/nobody:1700000000", stream_arn("sts-no-consumer"));
    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": fake_arn,
                "ShardId": "shardId-000000000000",
                "StartingPosition": { "Type": "TRIM_HORIZON" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn subscribe_consumer_not_active() {
    let server = TestServer::new().await;
    let name = "sts-not-active";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Register consumer — it starts in Creating state
    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "not-ready" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let consumer_arn = body["Consumer"]["ConsumerARN"].as_str().unwrap().to_string();

    // Subscribe immediately (consumer still Creating)
    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": consumer_arn,
                "ShardId": "shardId-000000000000",
                "StartingPosition": { "Type": "TRIM_HORIZON" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
}

#[tokio::test]
async fn subscribe_shard_not_found() {
    let server = TestServer::new().await;
    let name = "sts-no-shard";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c" }),
        )
        .await;
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": consumer_arn,
                "ShardId": "shardId-000000000099",
                "StartingPosition": { "Type": "TRIM_HORIZON" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

// -- Success path --

#[tokio::test]
async fn subscribe_success_returns_event_stream() {
    let server = TestServer::new().await;
    let name = "sts-ok";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "ok-consumer" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

    // Wait for consumer to become Active
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    // Make streaming request using raw client (don't buffer entire body)
    let mut response = server
        .client
        .post(server.url())
        .header("Content-Type", AMZ_JSON)
        .header(
            "X-Amz-Target",
            format!("{VERSION}.SubscribeToShard"),
        )
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        )
        .header("X-Amz-Date", "20150101T000000Z")
        .body(
            serde_json::to_vec(&json!({
                "ConsumerARN": consumer_arn,
                "ShardId": "shardId-000000000000",
                "StartingPosition": { "Type": "TRIM_HORIZON" },
            }))
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let ct = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("eventstream") || ct.contains("event-stream") || ct.contains("vnd.amazon"),
        "expected event-stream content-type, got: {ct}"
    );

    // Read first chunk (initial-response frame) — proves streaming started
    let chunk = response.chunk().await.unwrap();
    assert!(chunk.is_some(), "expected at least one chunk");
    // Drop response to close connection
}
