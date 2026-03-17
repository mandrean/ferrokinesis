mod common;

use common::*;
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
