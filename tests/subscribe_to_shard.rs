mod common;

use common::*;
use ferrokinesis::store::{Store, StoreOptions};
use ferrokinesis::types::{Consumer, ConsumerStatus};
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
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "InvalidArgumentException");
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
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "InvalidArgumentException");
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
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "ResourceNotFoundException");
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
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "InvalidArgumentException");
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
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "ResourceNotFoundException");
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

    let arn = format!("arn:aws:kinesis:us-east-1:0000-0000-0000:stream/{stream_name}");
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
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "InvalidArgumentException");
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
