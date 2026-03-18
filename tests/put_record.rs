mod common;

use common::*;
use ferrokinesis::sequence::{SeqObj, stringify_sequence};
use ferrokinesis::store::StoreOptions;
use num_bigint::BigUint;
use serde_json::{Value, json};

#[tokio::test]
async fn put_record_success() {
    let server = TestServer::new().await;
    let name = "test-put-record";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["ShardId"].as_str().unwrap().starts_with("shardId-"));
    assert!(!body["SequenceNumber"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn put_record_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "nonexistent",
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn put_record_multiple_records_sequential() {
    let server = TestServer::new().await;
    let name = "test-put-sequential";
    server.create_stream(name, 1).await;

    let r1 = server.put_record(name, "AAAA", "key1").await;
    let r2 = server.put_record(name, "BBBB", "key1").await;
    let r3 = server.put_record(name, "CCCC", "key1").await;

    // Sequence numbers should be strictly increasing
    let s1 = r1["SequenceNumber"].as_str().unwrap();
    let s2 = r2["SequenceNumber"].as_str().unwrap();
    let s3 = r3["SequenceNumber"].as_str().unwrap();
    assert!(s1 < s2);
    assert!(s2 < s3);
}

#[tokio::test]
async fn put_record_with_explicit_hash_key() {
    let server = TestServer::new().await;
    let name = "test-explicit-hash";
    server.create_stream(name, 3).await;

    // Use hash key 0 -> should go to first shard
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
                "ExplicitHashKey": "0",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["ShardId"], "shardId-000000000000");
}

#[tokio::test]
async fn put_record_explicit_hash_key_too_large() {
    let server = TestServer::new().await;
    let name = "test-hash-too-large";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
                "ExplicitHashKey": "340282366920938463463374607431768211456",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("ExplicitHashKey")
    );
}

#[tokio::test]
async fn put_record_validation_missing_fields() {
    let server = TestServer::new().await;
    let res = server.request("PutRecord", &json!({})).await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn put_record_validation_empty_partition_key() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "test",
                "Data": "dGVzdA==",
                "PartitionKey": "",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn put_record_to_correct_shard_by_partition_key() {
    let server = TestServer::new().await;
    let name = "test-shard-routing";
    server.create_stream(name, 3).await;

    // Put multiple records and verify they get shard IDs
    for i in 0..10 {
        let res = server
            .request(
                "PutRecord",
                &json!({
                    "StreamName": name,
                    "Data": "dGVzdA==",
                    "PartitionKey": format!("key-{i}"),
                }),
            )
            .await;
        assert_eq!(res.status(), 200);
        let body: Value = res.json().await.unwrap();
        let shard_id = body["ShardId"].as_str().unwrap();
        assert!(shard_id.starts_with("shardId-"));
    }
}

#[tokio::test]
async fn put_record_seq_for_ordering_parse_failure() {
    let server = TestServer::new().await;
    let name = "test-pr-seq-ord-fail";
    server.create_stream(name, 1).await;

    let unknown_range_seq = "10000000000000000000000000000000000000000000000000";
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "AAAA",
                "PartitionKey": "pk",
                "SequenceNumberForOrdering": unknown_range_seq,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("ExclusiveMinimumSequenceNumber")
    );
}

#[tokio::test]
async fn put_record_seq_for_ordering_future_time() {
    let server = TestServer::new().await;
    let name = "test-pr-seq-ord-fut";
    server.create_stream(name, 1).await;

    let future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 600_000;

    let future_seq = stringify_sequence(&SeqObj {
        shard_create_time: 1_000_000_000,
        seq_ix: Some(BigUint::from(0u32)),
        byte1: None,
        seq_time: Some(future_ms),
        seq_rand: None,
        shard_ix: 0,
        version: 2,
    });

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "AAAA",
                "PartitionKey": "pk",
                "SequenceNumberForOrdering": future_seq,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("ExclusiveMinimumSequenceNumber")
    );
}

#[tokio::test]
async fn put_record_on_creating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    })
    .await;
    let name = "test-pr-creating";

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 200);

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "AAAA",
                "PartitionKey": "pk",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}
