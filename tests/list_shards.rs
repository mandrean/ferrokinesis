mod common;

use common::*;
use serde_json::{Value, json};

#[tokio::test]
async fn list_shards_success() {
    let server = TestServer::new().await;
    let name = "test-list-shards";
    server.create_stream(name, 3).await;

    let res = server
        .request("ListShards", &json!({"StreamName": name}))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let shards = body["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 3);
    assert!(body["NextToken"].is_null());
}

#[tokio::test]
async fn list_shards_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request("ListShards", &json!({"StreamName": "nonexistent"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn list_shards_with_max_results() {
    let server = TestServer::new().await;
    let name = "test-list-shards-max";
    server.create_stream(name, 5).await;

    let res = server
        .request(
            "ListShards",
            &json!({"StreamName": name, "MaxResults": 2}),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let shards = body["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 2);
    assert!(body["NextToken"].as_str().is_some());
}

#[tokio::test]
async fn list_shards_with_exclusive_start_shard_id() {
    let server = TestServer::new().await;
    let name = "test-list-shards-start";
    server.create_stream(name, 3).await;

    let res = server
        .request(
            "ListShards",
            &json!({
                "StreamName": name,
                "ExclusiveStartShardId": "shardId-000000000000",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let shards = body["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 2);
    assert_eq!(shards[0]["ShardId"], "shardId-000000000001");
    assert_eq!(shards[1]["ShardId"], "shardId-000000000002");
}

#[tokio::test]
async fn list_shards_validation_missing_stream_name() {
    let server = TestServer::new().await;
    let res = server.request("ListShards", &json!({})).await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    // Should fail with either ValidationException or require StreamName/NextToken
    assert!(
        body["__type"] == "ValidationException"
            || body["__type"] == "InvalidArgumentException"
    );
}
