mod common;

use common::*;
use serde_json::{Value, json};

const ACCOUNT: &str = "0000-0000-0000";
const REGION: &str = "us-east-1";

fn stream_arn(name: &str) -> String {
    format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{name}")
}

/// Helper: register a consumer and wait for it to become Active.
async fn register_and_activate(server: &TestServer, stream_arn: &str, name: &str) -> String {
    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": stream_arn, "ConsumerName": name }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
    consumer_arn
}

/// Helper: send a subscribe request and assert it opens a streaming response.
async fn assert_subscribe_streams(server: &TestServer, consumer_arn: &str, request: serde_json::Value) {
    let mut response = server
        .client
        .post(server.url())
        .header("Content-Type", AMZ_JSON)
        .header("X-Amz-Target", format!("{VERSION}.SubscribeToShard"))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        )
        .header("X-Amz-Date", "20150101T000000Z")
        .body(serde_json::to_vec(&request).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        200,
        "Expected streaming 200 for consumer {consumer_arn}"
    );
    let chunk = response.chunk().await.unwrap();
    assert!(chunk.is_some(), "expected initial event stream frame");
}

#[tokio::test]
async fn subscribe_latest_position() {
    let server = TestServer::new().await;
    let name = "se-latest";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);
    let consumer_arn = register_and_activate(&server, &arn, "c-latest").await;

    assert_subscribe_streams(
        &server,
        &consumer_arn,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": { "Type": "LATEST" },
        }),
    )
    .await;
}

#[tokio::test]
async fn subscribe_at_sequence_number_position() {
    let server = TestServer::new().await;
    let name = "se-at-seq";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Put a record to get a sequence number
    let put_body = server.put_record(name, "AAAA", "pk").await;
    let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();

    let consumer_arn = register_and_activate(&server, &arn, "c-at-seq").await;

    assert_subscribe_streams(
        &server,
        &consumer_arn,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": {
                "Type": "AT_SEQUENCE_NUMBER",
                "SequenceNumber": seq,
            },
        }),
    )
    .await;
}

#[tokio::test]
async fn subscribe_after_sequence_number_position() {
    let server = TestServer::new().await;
    let name = "se-after-seq";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let put_body = server.put_record(name, "AAAA", "pk").await;
    let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();

    let consumer_arn = register_and_activate(&server, &arn, "c-after-seq").await;

    assert_subscribe_streams(
        &server,
        &consumer_arn,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": {
                "Type": "AFTER_SEQUENCE_NUMBER",
                "SequenceNumber": seq,
            },
        }),
    )
    .await;
}

#[tokio::test]
async fn subscribe_at_timestamp_position() {
    let server = TestServer::new().await;
    let name = "se-at-ts";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Put a record first
    server.put_record(name, "AAAA", "pk").await;

    let consumer_arn = register_and_activate(&server, &arn, "c-at-ts").await;

    // Use a timestamp in the far past to get all records
    assert_subscribe_streams(
        &server,
        &consumer_arn,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": {
                "Type": "AT_TIMESTAMP",
                "Timestamp": 1_000_000_000.0,
            },
        }),
    )
    .await;
}

// -- Error path tests for SubscribeToShard --

/// Non-existent consumer ARN → 400 ResourceNotFoundException
#[tokio::test]
async fn subscribe_consumer_not_found() {
    let server = TestServer::new().await;
    let name = "se-no-consumer";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);
    let fake_consumer_arn = format!("{arn}/consumer/nobody:1700000000");

    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": fake_consumer_arn,
                "ShardId": "shardId-000000000000",
                "StartingPosition": { "Type": "LATEST" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

/// Consumer registered but not yet Active → 400 ResourceInUseException
#[tokio::test]
async fn subscribe_consumer_not_active() {
    let server = TestServer::new().await;
    let name = "se-inactive";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Register but do NOT wait for activation
    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "inactive-c" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": consumer_arn,
                "ShardId": "shardId-000000000000",
                "StartingPosition": { "Type": "LATEST" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
}

/// Shard index beyond stream's shard count → 400 ResourceNotFoundException
#[tokio::test]
async fn subscribe_shard_out_of_range() {
    let server = TestServer::new().await;
    let name = "se-bad-shard";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);
    let consumer_arn = register_and_activate(&server, &arn, "c-bad-shard").await;

    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": consumer_arn,
                "ShardId": "shardId-000000000099",
                "StartingPosition": { "Type": "LATEST" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

/// Unknown starting position type → 400 InvalidArgumentException
#[tokio::test]
async fn subscribe_invalid_starting_position_type() {
    let server = TestServer::new().await;
    let name = "se-bad-pos";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);
    let consumer_arn = register_and_activate(&server, &arn, "c-bad-pos").await;

    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": consumer_arn,
                "ShardId": "shardId-000000000000",
                "StartingPosition": { "Type": "INVALID_TYPE" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    // Validation layer catches unknown enum values before the action sees them
    assert!(
        body["__type"] == "InvalidArgumentException"
            || body["__type"] == "ValidationException",
        "expected validation/argument error, got: {}",
        body["__type"]
    );
}

/// Read past the initial-response frame to exercise the streaming loop body.
/// With a pre-existing record and TRIM_HORIZON, the first event should contain the record.
#[tokio::test]
async fn subscribe_trim_horizon_reads_records_in_loop() {
    let server = TestServer::new().await;
    let name = "se-loop-records";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server.put_record(name, "AAAA", "pk").await;

    let consumer_arn = register_and_activate(&server, &arn, "c-loop").await;

    let mut response = server
        .client
        .post(server.url())
        .header("Content-Type", AMZ_JSON)
        .header("X-Amz-Target", format!("{VERSION}.SubscribeToShard"))
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

    // First chunk: initial-response frame
    let chunk1 = response.chunk().await.unwrap();
    assert!(chunk1.is_some(), "expected initial-response frame");

    // Second chunk: first SubscribeToShardEvent (exercises the streaming loop body)
    let chunk2 = tokio::time::timeout(
        tokio::time::Duration::from_secs(2),
        response.chunk(),
    )
    .await
    .expect("timed out waiting for first SubscribeToShardEvent")
    .unwrap();
    assert!(chunk2.is_some(), "expected first event frame from loop");
}

/// Subscribe to a closed shard (after scaling) to exercise the child-shards path.
#[tokio::test]
async fn subscribe_closed_shard_child_shards() {
    let server = TestServer::new().await;
    let name = "se-closed-shard";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);
    let consumer_arn = register_and_activate(&server, &arn, "c-closed").await;

    // Scale 1→2 shards, which closes shard 0
    let res = server
        .request(
            "UpdateShardCount",
            &json!({
                "StreamName": name,
                "TargetShardCount": 2,
                "ScalingType": "UNIFORM_SCALING",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let mut response = server
        .client
        .post(server.url())
        .header("Content-Type", AMZ_JSON)
        .header("X-Amz-Target", format!("{VERSION}.SubscribeToShard"))
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

    // Read initial frame
    let chunk1 = response.chunk().await.unwrap();
    assert!(chunk1.is_some());

    // Read next frame (exercises shard_closed + child_shards path, then breaks)
    let chunk2 = tokio::time::timeout(
        tokio::time::Duration::from_secs(2),
        response.chunk(),
    )
    .await
    .expect("timed out")
    .unwrap();
    assert!(chunk2.is_some(), "expected event frame for closed shard");
}
