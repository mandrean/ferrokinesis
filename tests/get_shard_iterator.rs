mod common;

use common::*;
use serde_json::{Value, json};

#[tokio::test]
async fn get_shard_iterator_trim_horizon() {
    let server = TestServer::new().await;
    let name = "test-iter-horizon";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let iter = body["ShardIterator"].as_str().unwrap();
    assert!(!iter.is_empty());
    // Should be valid base64
    assert!(base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        iter
    )
    .is_ok());
}

#[tokio::test]
async fn get_shard_iterator_latest() {
    let server = TestServer::new().await;
    let name = "test-iter-latest";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "LATEST",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["ShardIterator"].as_str().is_some());
}

#[tokio::test]
async fn get_shard_iterator_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "nonexistent",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn get_shard_iterator_shard_not_found() {
    let server = TestServer::new().await;
    let name = "test-iter-shard-notfound";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000999",
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("does not exist"));
}

#[tokio::test]
async fn get_shard_iterator_at_sequence_number() {
    let server = TestServer::new().await;
    let name = "test-iter-at-seq";
    server.create_stream(name, 1).await;

    let put_result = server.put_record(name, "AAAA", "key1").await;
    let seq = put_result["SequenceNumber"].as_str().unwrap();

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["ShardIterator"].as_str().is_some());
}

#[tokio::test]
async fn get_shard_iterator_invalid_sequence_number() {
    let server = TestServer::new().await;
    let name = "test-iter-invalid-seq";
    server.create_stream(name, 1).await;

    // "invalid" doesn't pass validation (not a number pattern), so it returns ValidationException
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": "invalid",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn get_shard_iterator_sequence_from_wrong_shard() {
    let server = TestServer::new().await;
    let name = "test-iter-wrong-shard";
    server.create_stream(name, 2).await;

    // Put a record to shard 0 (using explicit hash key = 0)
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "AAAA",
                "PartitionKey": "key1",
                "ExplicitHashKey": "0",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let put_body: Value = res.json().await.unwrap();
    let seq = put_body["SequenceNumber"].as_str().unwrap();
    assert_eq!(put_body["ShardId"], "shardId-000000000000");

    // Try to use shard 0's sequence number with shard 1
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000001",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("Invalid StartingSequenceNumber"));
}

#[tokio::test]
async fn get_shard_iterator_trim_horizon_with_seq_number_is_error() {
    let server = TestServer::new().await;
    let name = "test-iter-horizon-seq";
    server.create_stream(name, 1).await;

    let put_result = server.put_record(name, "AAAA", "key1").await;
    let seq = put_result["SequenceNumber"].as_str().unwrap();

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
                "StartingSequenceNumber": seq,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("TRIM_HORIZON"));
}

#[tokio::test]
async fn get_shard_iterator_latest_with_seq_number_is_error() {
    let server = TestServer::new().await;
    let name = "test-iter-latest-seq";
    server.create_stream(name, 1).await;

    let put_result = server.put_record(name, "AAAA", "key1").await;
    let seq = put_result["SequenceNumber"].as_str().unwrap();

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "LATEST",
                "StartingSequenceNumber": seq,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("LATEST"));
}

#[tokio::test]
async fn get_shard_iterator_validation_missing_fields() {
    let server = TestServer::new().await;
    let res = server.request("GetShardIterator", &json!({})).await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn get_shard_iterator_at_timestamp() {
    let server = TestServer::new().await;
    let name = "test-iter-timestamp";
    server.create_stream(name, 1).await;

    server.put_record(name, "AAAA", "key1").await;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": now - 60.0,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["ShardIterator"].as_str().is_some());
}

#[tokio::test]
async fn get_shard_iterator_at_timestamp_future_error() {
    let server = TestServer::new().await;
    let name = "test-iter-timestamp-future";
    server.create_stream(name, 1).await;

    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        + 3600.0;

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": future,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("timestampInMillis"));
}
