mod common;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use common::*;
use ferrokinesis::store::StoreOptions;
use serde_json::{Value, json};

#[tokio::test]
async fn put_records_success() {
    let server = TestServer::new().await;
    let name = "test-put-records";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": [
                    {"Data": "AAAA", "PartitionKey": "key1"},
                    {"Data": "BBBB", "PartitionKey": "key2"},
                    {"Data": "CCCC", "PartitionKey": "key3"},
                ],
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["FailedRecordCount"], 0);
    let records = body["Records"].as_array().unwrap();
    assert_eq!(records.len(), 3);

    for record in records {
        assert!(record["ShardId"].as_str().unwrap().starts_with("shardId-"));
        assert!(!record["SequenceNumber"].as_str().unwrap().is_empty());
        assert!(record.get("ErrorCode").is_none() || record["ErrorCode"].is_null());
    }
}

#[tokio::test]
async fn put_records_large_batch_succeeds_by_default_without_limit_enforcement() {
    let server = TestServer::new().await;
    let name = "test-put-records-default-no-throttle";
    let payload = BASE64.encode(vec![b'a'; 600_000]);
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": [
                    {"Data": payload.clone(), "PartitionKey": "key"},
                    {"Data": payload.clone(), "PartitionKey": "key"},
                ],
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["FailedRecordCount"], 0);
    assert_eq!(body["Records"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn put_records_ignore_throughput_limits_on_base_branch() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        enforce_limits: true,
        ..Default::default()
    })
    .await;
    let name = "test-put-records-throughput-limits";
    let payload = BASE64.encode(vec![b'b'; 600_000]);
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": [
                    {"Data": payload.clone(), "PartitionKey": "key"},
                    {"Data": payload.clone(), "PartitionKey": "key"},
                ],
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["FailedRecordCount"], 0);
    assert_eq!(body["Records"].as_array().unwrap().len(), 2);
    assert_eq!(server.store.get_record_store(name).await.len(), 2);
}

#[tokio::test]
async fn put_records_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": "nonexistent",
                "Records": [
                    {"Data": "AAAA", "PartitionKey": "key1"},
                ],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn put_records_sequential_sequence_numbers() {
    let server = TestServer::new().await;
    let name = "test-put-records-seq";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": [
                    {"Data": "AAAA", "PartitionKey": "key1"},
                    {"Data": "BBBB", "PartitionKey": "key1"},
                    {"Data": "CCCC", "PartitionKey": "key1"},
                ],
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let records = body["Records"].as_array().unwrap();

    let s1 = records[0]["SequenceNumber"].as_str().unwrap();
    let s2 = records[1]["SequenceNumber"].as_str().unwrap();
    let s3 = records[2]["SequenceNumber"].as_str().unwrap();
    assert!(s1 < s2);
    assert!(s2 < s3);
}

#[tokio::test]
async fn put_records_validation_missing_records() {
    let server = TestServer::new().await;
    let res = server
        .request("PutRecords", &json!({"StreamName": "test"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn put_records_with_explicit_hash_key() {
    let server = TestServer::new().await;
    let name = "test-put-records-hash";
    server.create_stream(name, 3).await;

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": [
                    {"Data": "AAAA", "PartitionKey": "key1", "ExplicitHashKey": "0"},
                ],
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["Records"][0]["ShardId"], "shardId-000000000000");
}

#[tokio::test]
async fn put_records_explicit_hash_key_too_large() {
    let server = TestServer::new().await;
    let name = "test-prs-ehk";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": [{
                    "Data": "AAAA",
                    "PartitionKey": "pk",
                    "ExplicitHashKey": "340282366920938463463374607431768211456",
                }],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn put_records_on_creating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    })
    .await;
    let name = "test-prs-creating";

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 200);

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": [{"Data": "AAAA", "PartitionKey": "pk"}],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}
