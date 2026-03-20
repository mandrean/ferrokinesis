mod common;

use aws_smithy_eventstream::frame::read_message_from;
use aws_smithy_types::event_stream::Message;
use common::*;
use ferrokinesis::store::{Store, StoreOptions};
use ferrokinesis::types::{Consumer, ConsumerStatus};
use serde_json::{Value, json};
use std::io::Cursor;

const ACCOUNT: &str = "000000000000";
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
    let fake_arn = format!(
        "{}/consumer/nobody:1700000000",
        stream_arn("sts-no-consumer")
    );
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
    let consumer_arn = body["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

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

#[tokio::test]
async fn subscribe_empty_consumer_arn_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::subscribe_to_shard::execute_streaming(
        &store,
        json!({
            "ConsumerARN": "",
            "ShardId": "shardId-000000000000",
            "StartingPosition": { "Type": "TRIM_HORIZON" },
        }),
        "application/x-amz-json-1.1",
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "InvalidArgumentException"
    );
}

#[tokio::test]
async fn subscribe_empty_shard_id_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::subscribe_to_shard::execute_streaming(
        &store,
        json!({
            "ConsumerARN": "arn:aws:kinesis:us-east-1:000000000000:stream/t/consumer/c:1",
            "ShardId": "",
            "StartingPosition": { "Type": "TRIM_HORIZON" },
        }),
        "application/x-amz-json-1.1",
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "InvalidArgumentException"
    );
}

#[tokio::test]
async fn subscribe_stream_not_found_from_consumer_arn() {
    let store = Store::new(StoreOptions::default());

    let consumer_arn =
        "arn:aws:kinesis:us-east-1:000000000000:stream/ghost-stream/consumer/c:1700000000";
    store
        .put_consumer(
            consumer_arn,
            Consumer {
                consumer_name: "c".to_string(),
                consumer_arn: consumer_arn.to_string(),
                consumer_status: ConsumerStatus::Active,
                consumer_creation_timestamp: 1.0,
            },
        )
        .await;

    let result = ferrokinesis::actions::subscribe_to_shard::execute_streaming(
        &store,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": { "Type": "TRIM_HORIZON" },
        }),
        "application/x-amz-json-1.1",
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "ResourceNotFoundException"
    );
}

#[tokio::test]
async fn subscribe_consumer_arn_missing_consumer_segment() {
    let store = Store::new(StoreOptions::default());

    let malformed_arn = "arn-without-consumer-path-segment";
    store
        .put_consumer(
            malformed_arn,
            Consumer {
                consumer_name: "test".to_string(),
                consumer_arn: malformed_arn.to_string(),
                consumer_status: ConsumerStatus::Active,
                consumer_creation_timestamp: 1.0,
            },
        )
        .await;

    let result = ferrokinesis::actions::subscribe_to_shard::execute_streaming(
        &store,
        json!({
            "ConsumerARN": malformed_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": { "Type": "TRIM_HORIZON" },
        }),
        "application/x-amz-json-1.1",
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "InvalidArgumentException"
    );
}

#[tokio::test]
async fn subscribe_consumer_arn_unresolvable_stream_name() {
    let store = Store::new(StoreOptions::default());

    let consumer_arn = "foo/consumer/c:1700000000";
    store
        .put_consumer(
            consumer_arn,
            Consumer {
                consumer_name: "c".to_string(),
                consumer_arn: consumer_arn.to_string(),
                consumer_status: ConsumerStatus::Active,
                consumer_creation_timestamp: 1.0,
            },
        )
        .await;

    let result = ferrokinesis::actions::subscribe_to_shard::execute_streaming(
        &store,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": { "Type": "TRIM_HORIZON" },
        }),
        "application/x-amz-json-1.1",
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "ResourceNotFoundException"
    );
}

#[tokio::test]
async fn subscribe_invalid_shard_format() {
    let server = TestServer::new().await;
    let name = "test-sub-bad-shard";
    let arn = stream_arn(name);

    server.create_stream(name, 1).await;

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c-badshard" }),
        )
        .await;
    assert_eq!(res.status(), 200);
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
                "ShardId": "shardId-abc",
                "StartingPosition": { "Type": "TRIM_HORIZON" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn subscribe_at_timestamp_no_records_fallback() {
    let server = TestServer::new().await;
    let name = "test-sub-at-ts-none";
    let arn = stream_arn(name);

    server.create_stream(name, 1).await;

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c-atts-none" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    let mut response = server
        .client
        .post(server.url())
        .header("Content-Type", AMZ_JSON)
        .header("X-Amz-Target", format!("{VERSION}.SubscribeToShard"))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        )
        .header("X-Amz-Date", "20150101T000000Z")
        .body(
            serde_json::to_vec(&json!({
                "ConsumerARN": consumer_arn,
                "ShardId": "shardId-000000000000",
                "StartingPosition": {
                    "Type": "AT_TIMESTAMP",
                    "Timestamp": 1.0f64,
                },
            }))
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let chunk = response.chunk().await.unwrap();
    assert!(chunk.is_some(), "expected initial-response frame");
}

#[tokio::test]
async fn subscribe_unknown_position_type_direct() {
    let store = Store::new(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 10,
        ..Default::default()
    });
    let stream_name = "test-sub-unknown-pos";

    // Create stream via action and let it become ACTIVE
    ferrokinesis::actions::create_stream::execute(
        &store,
        json!({ "StreamName": stream_name, "ShardCount": 1 }),
    )
    .await
    .unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let arn = format!("arn:aws:kinesis:us-east-1:000000000000:stream/{stream_name}");
    let consumer_arn = format!("{arn}/consumer/c:1700000000");
    store
        .put_consumer(
            &consumer_arn,
            Consumer {
                consumer_name: "c".to_string(),
                consumer_arn: consumer_arn.clone(),
                consumer_status: ConsumerStatus::Active,
                consumer_creation_timestamp: 1.0,
            },
        )
        .await;

    let result = ferrokinesis::actions::subscribe_to_shard::execute_streaming(
        &store,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": { "Type": "BOGUS_TYPE" },
        }),
        "application/x-amz-json-1.1",
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "InvalidArgumentException"
    );
}

#[tokio::test]
async fn subscribe_primary_parent_after_merge() {
    let server = TestServer::new().await;
    let name = "test-sub-merge";
    let arn = stream_arn(name);

    server.create_stream(name, 2).await;

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "merge-c" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    let res = server
        .request(
            "MergeShards",
            &json!({
                "StreamName": name,
                "ShardToMerge": "shardId-000000000000",
                "AdjacentShardToMerge": "shardId-000000000001",
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
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
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

    let chunk1 = response.chunk().await.unwrap();
    assert!(chunk1.is_some(), "expected initial-response frame");

    let chunk2 = tokio::time::timeout(tokio::time::Duration::from_secs(2), response.chunk())
        .await
        .expect("timed out waiting for event frame")
        .unwrap();
    assert!(chunk2.is_some(), "expected event frame for merged shard");
}

// -- Helper functions --

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
async fn assert_subscribe_streams(
    server: &TestServer,
    consumer_arn: &str,
    request: serde_json::Value,
) {
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

    server.put_record(name, "AAAA", "pk").await;

    let consumer_arn = register_and_activate(&server, &arn, "c-at-ts").await;

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
    assert!(
        body["__type"] == "InvalidArgumentException" || body["__type"] == "ValidationException",
        "expected validation/argument error, got: {}",
        body["__type"]
    );
}

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

    let chunk1 = response.chunk().await.unwrap();
    assert!(chunk1.is_some(), "expected initial-response frame");

    let chunk2 = tokio::time::timeout(tokio::time::Duration::from_secs(2), response.chunk())
        .await
        .expect("timed out waiting for first SubscribeToShardEvent")
        .unwrap();
    assert!(chunk2.is_some(), "expected first event frame from loop");
}

#[tokio::test]
async fn subscribe_closed_shard_child_shards() {
    let server = TestServer::new().await;
    let name = "se-closed-shard";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);
    let consumer_arn = register_and_activate(&server, &arn, "c-closed").await;

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

    let chunk1 = response.chunk().await.unwrap();
    assert!(chunk1.is_some());

    let chunk2 = tokio::time::timeout(tokio::time::Duration::from_secs(2), response.chunk())
        .await
        .expect("timed out")
        .unwrap();
    assert!(chunk2.is_some(), "expected event frame for closed shard");
}

#[tokio::test]
async fn subscribe_to_updating_stream_returns_error() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 2000,
        shard_limit: 50,
        ..Default::default()
    })
    .await;
    let name = "cx-sub-updating";
    let arn = stream_arn(name);
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "cx-c-updating" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    // Start encryption → sets stream to UPDATING for 2000ms
    server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;

    // Subscribe immediately — stream is UPDATING
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

// -- CBOR event-stream tests --

/// Parse `count` event-stream frames from raw bytes.
fn parse_event_frames(data: &[u8], count: usize) -> Vec<Message> {
    let mut cursor = Cursor::new(data);
    let mut messages = Vec::with_capacity(count);
    for _ in 0..count {
        let msg = read_message_from(&mut cursor).expect("failed to parse event-stream frame");
        messages.push(msg);
    }
    messages
}

/// Find a header value by name in a Message.
fn header_value(msg: &Message, name: &str) -> Option<String> {
    msg.headers()
        .iter()
        .find(|h| h.name().as_str() == name)
        .and_then(|h| match h.value() {
            aws_smithy_types::event_stream::HeaderValue::String(s) => Some(s.as_str().to_string()),
            _ => None,
        })
}

#[tokio::test]
async fn subscribe_cbor_returns_cbor_event_frames() {
    let server = TestServer::new().await;
    let name = "se-cbor-frames";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server.put_record(name, "QUJDRA==", "pk-cbor").await;

    let consumer_arn = register_and_activate(&server, &arn, "c-cbor-frames").await;

    // Build CBOR-encoded request body
    let request = json!({
        "ConsumerARN": consumer_arn,
        "ShardId": "shardId-000000000000",
        "StartingPosition": { "Type": "TRIM_HORIZON" },
    });
    let mut cbor_body = Vec::new();
    ciborium::into_writer(&request, &mut cbor_body).unwrap();

    let mut response = server
        .client
        .post(server.url())
        .header("Content-Type", AMZ_CBOR)
        .header("X-Amz-Target", format!("{VERSION}.SubscribeToShard"))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        )
        .header("X-Amz-Date", "20150101T000000Z")
        .body(cbor_body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    // Collect initial-response frame
    let chunk1 = response.chunk().await.unwrap();
    assert!(chunk1.is_some(), "expected initial-response frame");
    let initial_bytes = chunk1.unwrap();
    let initial_frames = parse_event_frames(&initial_bytes, 1);
    assert_eq!(
        header_value(&initial_frames[0], ":event-type").as_deref(),
        Some("initial-response")
    );

    // Collect first event frame
    let chunk2 = tokio::time::timeout(tokio::time::Duration::from_secs(2), response.chunk())
        .await
        .expect("timed out waiting for event frame")
        .unwrap();
    assert!(chunk2.is_some(), "expected event frame");
    let event_bytes = chunk2.unwrap();
    let event_frames = parse_event_frames(&event_bytes, 1);

    // Assert content-type is CBOR
    assert_eq!(
        header_value(&event_frames[0], ":content-type").as_deref(),
        Some(AMZ_CBOR),
    );

    // Decode CBOR payload
    let payload = event_frames[0].payload().as_ref();
    let cbor_val: ciborium::Value = ciborium::from_reader(payload).expect("invalid CBOR payload");
    let decoded = ferrokinesis::server::cbor_to_json(&cbor_val);

    let records = decoded["Records"]
        .as_array()
        .expect("expected Records array");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["PartitionKey"], "pk-cbor");
    assert!(
        records[0]["SequenceNumber"].as_str().is_some(),
        "expected non-empty SequenceNumber"
    );
}

#[tokio::test]
async fn subscribe_cbor_json_equivalence() {
    let server = TestServer::new().await;
    let name = "se-cbor-equiv";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server.put_record(name, "QUJDRA==", "pk-equiv").await;

    let consumer_arn = register_and_activate(&server, &arn, "c-cbor-equiv").await;

    // -- JSON subscription --
    let json_request = json!({
        "ConsumerARN": consumer_arn,
        "ShardId": "shardId-000000000000",
        "StartingPosition": { "Type": "TRIM_HORIZON" },
    });

    let mut json_response = server
        .client
        .post(server.url())
        .header("Content-Type", AMZ_JSON)
        .header("X-Amz-Target", format!("{VERSION}.SubscribeToShard"))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        )
        .header("X-Amz-Date", "20150101T000000Z")
        .body(serde_json::to_vec(&json_request).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(json_response.status(), 200);

    // Skip initial-response
    json_response.chunk().await.unwrap();
    let json_chunk =
        tokio::time::timeout(tokio::time::Duration::from_secs(2), json_response.chunk())
            .await
            .expect("timed out waiting for JSON event")
            .unwrap()
            .expect("expected JSON event frame");

    let json_frames = parse_event_frames(&json_chunk, 1);
    let json_payload: Value =
        serde_json::from_slice(json_frames[0].payload().as_ref()).expect("invalid JSON payload");

    // -- CBOR subscription --
    let mut cbor_body = Vec::new();
    ciborium::into_writer(&json_request, &mut cbor_body).unwrap();

    let mut cbor_response = server
        .client
        .post(server.url())
        .header("Content-Type", AMZ_CBOR)
        .header("X-Amz-Target", format!("{VERSION}.SubscribeToShard"))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        )
        .header("X-Amz-Date", "20150101T000000Z")
        .body(cbor_body)
        .send()
        .await
        .unwrap();
    assert_eq!(cbor_response.status(), 200);

    // Skip initial-response
    cbor_response.chunk().await.unwrap();
    let cbor_chunk =
        tokio::time::timeout(tokio::time::Duration::from_secs(2), cbor_response.chunk())
            .await
            .expect("timed out waiting for CBOR event")
            .unwrap()
            .expect("expected CBOR event frame");

    let cbor_frames = parse_event_frames(&cbor_chunk, 1);
    let cbor_val: ciborium::Value =
        ciborium::from_reader(cbor_frames[0].payload().as_ref()).expect("invalid CBOR payload");
    let cbor_payload = ferrokinesis::server::cbor_to_json(&cbor_val);

    // Compare, ignoring volatile fields
    assert_values_equivalent(
        &json_payload,
        &cbor_payload,
        &["ContinuationSequenceNumber", "MillisBehindLatest"],
    );
}
