mod common;

use common::*;
use serde_json::{Value, json};

#[tokio::test]
async fn create_stream_and_describe() {
    let server = TestServer::new().await;
    let name = "test-create-describe";

    server.create_stream(name, 3).await;

    let desc = server.describe_stream(name).await;
    let sd = &desc["StreamDescription"];
    assert_eq!(sd["StreamName"], name);
    assert_eq!(sd["StreamStatus"], "ACTIVE");
    assert_eq!(sd["RetentionPeriodHours"], 24);
    assert_eq!(sd["HasMoreShards"], false);
    assert_eq!(sd["EncryptionType"], "NONE");
    assert_eq!(sd["Shards"].as_array().unwrap().len(), 3);

    // Verify shard IDs
    let shards = sd["Shards"].as_array().unwrap();
    assert_eq!(shards[0]["ShardId"], "shardId-000000000000");
    assert_eq!(shards[1]["ShardId"], "shardId-000000000001");
    assert_eq!(shards[2]["ShardId"], "shardId-000000000002");

    // Verify hash key ranges cover the full space
    assert_eq!(shards[0]["HashKeyRange"]["StartingHashKey"], "0");
    let last_end = shards[2]["HashKeyRange"]["EndingHashKey"].as_str().unwrap();
    let pow128_minus1 = "340282366920938463463374607431768211455";
    assert_eq!(last_end, pow128_minus1);

    // Verify ARN format
    let arn = sd["StreamARN"].as_str().unwrap();
    assert!(arn.starts_with("arn:aws:kinesis:"));
    assert!(arn.ends_with(&format!("stream/{name}")));

    // Enhanced monitoring
    assert_eq!(sd["EnhancedMonitoring"].as_array().unwrap().len(), 1);
    assert_eq!(
        sd["EnhancedMonitoring"][0]["ShardLevelMetrics"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn create_stream_already_exists() {
    let server = TestServer::new().await;
    let name = "test-already-exists";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("already exists"));
}

#[tokio::test]
async fn create_stream_shard_limit_exceeded() {
    let server = TestServer::with_options(ferrokinesis::store::StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 5,
        ..Default::default()
    })
    .await;

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "test-limit", "ShardCount": 6}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "LimitExceededException");
    assert!(body["message"].as_str().unwrap().contains("shard limit"));
}

// -- Validation tests --

#[tokio::test]
async fn create_stream_missing_stream_name() {
    let server = TestServer::new().await;
    let res = server
        .request("CreateStream", &json!({"ShardCount": 1}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn create_stream_missing_shard_count() {
    let server = TestServer::new().await;
    let res = server
        .request("CreateStream", &json!({"StreamName": "a"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn create_stream_invalid_stream_name_too_long() {
    let server = TestServer::new().await;
    let name = "a".repeat(129);
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn create_stream_invalid_shard_count_zero() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "test", "ShardCount": 0}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn create_stream_serialization_error_string_shard_count() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "test", "ShardCount": "1"}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "SerializationException");
}

#[tokio::test]
async fn create_stream_serialization_error_boolean_stream_name() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": true, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "SerializationException");
}

#[tokio::test]
async fn create_stream_single_shard_hash_range() {
    let server = TestServer::new().await;
    let name = "test-single-shard";
    server.create_stream(name, 1).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0]["HashKeyRange"]["StartingHashKey"], "0");
    assert_eq!(
        shards[0]["HashKeyRange"]["EndingHashKey"],
        "340282366920938463463374607431768211455"
    );
}

#[tokio::test]
async fn create_stream_creating_then_active() {
    // Use a non-zero delay so we can observe CREATING state
    let server = TestServer::with_options(ferrokinesis::store::StoreOptions {
        create_stream_ms: 200,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    })
    .await;

    let name = "test-creating-state";
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Should be CREATING immediately
    let desc = server.describe_stream(name).await;
    assert_eq!(desc["StreamDescription"]["StreamStatus"], "CREATING");
    // Shards empty while CREATING
    assert_eq!(
        desc["StreamDescription"]["Shards"]
            .as_array()
            .unwrap()
            .len(),
        0
    );

    // Wait for transition
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let desc = server.describe_stream(name).await;
    assert_eq!(desc["StreamDescription"]["StreamStatus"], "ACTIVE");
    assert_eq!(
        desc["StreamDescription"]["Shards"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}
