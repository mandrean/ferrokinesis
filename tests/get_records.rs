mod common;

use common::*;
use serde_json::{Value, json};

#[tokio::test]
async fn get_records_empty_stream() {
    let server = TestServer::new().await;
    let name = "test-get-empty";
    server.create_stream(name, 1).await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let records = server.get_records(&iter).await;
    assert_eq!(records["Records"].as_array().unwrap().len(), 0);
    assert!(records["MillisBehindLatest"].as_u64().is_some());
    assert!(records["NextShardIterator"].as_str().is_some());
}

#[tokio::test]
async fn get_records_after_put() {
    let server = TestServer::new().await;
    let name = "test-get-after-put";
    server.create_stream(name, 1).await;

    server.put_record(name, "dGVzdDE=", "key1").await;
    server.put_record(name, "dGVzdDI=", "key2").await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), 2);

    assert_eq!(records[0]["PartitionKey"], "key1");
    assert_eq!(records[0]["Data"], "dGVzdDE=");
    assert_eq!(records[1]["PartitionKey"], "key2");
    assert_eq!(records[1]["Data"], "dGVzdDI=");

    // Verify sequence numbers are present and ordered
    let s1 = records[0]["SequenceNumber"].as_str().unwrap();
    let s2 = records[1]["SequenceNumber"].as_str().unwrap();
    assert!(s1 < s2);

    // Verify arrival timestamps
    for record in records {
        let ts = record["ApproximateArrivalTimestamp"].as_f64().unwrap();
        assert!(ts > 1_000_000_000.0);
        assert!(ts < 10_000_000_000.0);
    }
}

#[tokio::test]
async fn get_records_latest_iterator() {
    let server = TestServer::new().await;
    let name = "test-get-latest";
    server.create_stream(name, 1).await;

    // Put records before getting iterator
    server.put_record(name, "AAAA", "key1").await;
    server.put_record(name, "BBBB", "key2").await;

    // LATEST iterator should not see existing records
    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "LATEST")
        .await;
    let result = server.get_records(&iter).await;
    assert_eq!(result["Records"].as_array().unwrap().len(), 0);

    // Put a new record after getting iterator
    server.put_record(name, "CCCC", "key3").await;

    // Should see the new record with next iterator
    let next_iter = result["NextShardIterator"].as_str().unwrap();
    let result2 = server.get_records(next_iter).await;
    let records = result2["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["PartitionKey"], "key3");
}

#[tokio::test]
async fn get_records_at_sequence_number() {
    let server = TestServer::new().await;
    let name = "test-get-at-seq";
    server.create_stream(name, 1).await;

    let r1 = server.put_record(name, "AAAA", "key1").await;
    server.put_record(name, "BBBB", "key2").await;

    let seq = r1["SequenceNumber"].as_str().unwrap();
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
    let iter = body["ShardIterator"].as_str().unwrap();

    let result = server.get_records(iter).await;
    let records = result["Records"].as_array().unwrap();
    assert!(records.len() >= 1);
    assert_eq!(records[0]["PartitionKey"], "key1");
}

#[tokio::test]
async fn get_records_after_sequence_number() {
    let server = TestServer::new().await;
    let name = "test-get-after-seq";
    server.create_stream(name, 1).await;

    let r1 = server.put_record(name, "AAAA", "key1").await;
    server.put_record(name, "BBBB", "key2").await;

    let seq = r1["SequenceNumber"].as_str().unwrap();
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let iter = body["ShardIterator"].as_str().unwrap();

    let result = server.get_records(iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["PartitionKey"], "key2");
}

#[tokio::test]
async fn get_records_with_limit() {
    let server = TestServer::new().await;
    let name = "test-get-limit";
    server.create_stream(name, 1).await;

    for i in 0..5 {
        server.put_record(name, "AAAA", &format!("key{i}")).await;
    }

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;

    let res = server
        .request("GetRecords", &json!({"ShardIterator": iter, "Limit": 2}))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["Records"].as_array().unwrap().len(), 2);
    assert!(body["NextShardIterator"].as_str().is_some());
}

#[tokio::test]
async fn get_records_pagination() {
    let server = TestServer::new().await;
    let name = "test-get-pagination";
    server.create_stream(name, 1).await;

    for i in 0..5 {
        server.put_record(name, "AAAA", &format!("key{i}")).await;
    }

    let mut iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let mut total = 0;

    for _ in 0..10 {
        let res = server
            .request("GetRecords", &json!({"ShardIterator": iter, "Limit": 2}))
            .await;
        let body: Value = res.json().await.unwrap();
        let batch = body["Records"].as_array().unwrap().len();
        total += batch;
        if batch == 0 {
            break;
        }
        iter = body["NextShardIterator"].as_str().unwrap().to_string();
    }
    assert_eq!(total, 5);
}

#[tokio::test]
async fn get_records_invalid_iterator() {
    let server = TestServer::new().await;
    let res = server
        .request("GetRecords", &json!({"ShardIterator": "invalid-iterator"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn get_records_millis_behind_latest() {
    let server = TestServer::new().await;
    let name = "test-millis-behind";
    server.create_stream(name, 1).await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    // For an empty shard, MillisBehindLatest should be 0 or close to 0
    assert!(result["MillisBehindLatest"].as_u64().is_some());
}

#[tokio::test]
async fn get_records_multi_shard() {
    let server = TestServer::new().await;
    let name = "test-multi-shard";
    server.create_stream(name, 3).await;

    // Put records to shard 0 using explicit hash key = 0
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "c2hhcmQw",
                "PartitionKey": "key1",
                "ExplicitHashKey": "0",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let put_body: Value = res.json().await.unwrap();
    assert_eq!(put_body["ShardId"], "shardId-000000000000");

    // Read from shard 0
    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["Data"], "c2hhcmQw");

    // Read from shard 1 - should be empty
    let iter1 = server
        .get_shard_iterator(name, "shardId-000000000001", "TRIM_HORIZON")
        .await;
    let result1 = server.get_records(&iter1).await;
    assert_eq!(result1["Records"].as_array().unwrap().len(), 0);
}
