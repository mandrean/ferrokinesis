#![cfg(feature = "tls")]

mod common;

use common::TestServer;
use serde_json::{Value, json};

#[tokio::test]
async fn tls_health_endpoint() {
    let server = TestServer::new_tls().await;

    let res = server
        .client
        .get(format!("https://{}/_health", server.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn tls_list_streams() {
    let server = TestServer::new_tls().await;

    let res = server.tls_request("ListStreams", &json!({})).await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["StreamNames"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn tls_put_and_get_records() {
    let server = TestServer::new_tls().await;

    // Create a stream
    let res = server
        .tls_request(
            "CreateStream",
            &json!({"StreamName": "tls-test", "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 200);
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Put a record
    let res = server
        .tls_request(
            "PutRecord",
            &json!({
                "StreamName": "tls-test",
                "Data": "dGVzdA==",
                "PartitionKey": "pk1",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Get shard iterator
    let res = server
        .tls_request(
            "GetShardIterator",
            &json!({
                "StreamName": "tls-test",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let iterator = body["ShardIterator"].as_str().unwrap();

    // Get records
    let res = server
        .tls_request("GetRecords", &json!({"ShardIterator": iterator}))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let records = body["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["Data"], "dGVzdA==");
    assert_eq!(records[0]["PartitionKey"], "pk1");
}
