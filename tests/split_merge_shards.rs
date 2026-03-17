mod common;

use common::*;
use serde_json::{Value, json};

// -- SplitShard --

#[tokio::test]
async fn split_shard_success() {
    let server = TestServer::new().await;
    let name = "test-split-shard";
    server.create_stream(name, 1).await;

    // Get the shard's hash range
    let desc = server.describe_stream(name).await;
    let shard = &desc["StreamDescription"]["Shards"][0];
    let start: u128 = shard["HashKeyRange"]["StartingHashKey"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let end: u128 = shard["HashKeyRange"]["EndingHashKey"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let mid = (start + end) / 2;

    let res = server
        .request(
            "SplitShard",
            &json!({
                "StreamName": name,
                "ShardToSplit": "shardId-000000000000",
                "NewStartingHashKey": mid.to_string(),
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Wait for update
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();

    // Original shard should now be closed (has ending sequence number)
    assert!(shards[0]["SequenceNumberRange"]["EndingSequenceNumber"]
        .as_str()
        .is_some());

    // Two new shards should exist
    assert_eq!(shards.len(), 3);
    assert!(shards[1]["ParentShardId"].as_str().is_some());
    assert!(shards[2]["ParentShardId"].as_str().is_some());

    // New shards should be open (no ending sequence number)
    assert!(shards[1]["SequenceNumberRange"]["EndingSequenceNumber"].is_null());
    assert!(shards[2]["SequenceNumberRange"]["EndingSequenceNumber"].is_null());
}

#[tokio::test]
async fn split_shard_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "SplitShard",
            &json!({
                "StreamName": "nonexistent",
                "ShardToSplit": "shardId-000000000000",
                "NewStartingHashKey": "170141183460469231731687303715884105728",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn split_shard_invalid_hash_key() {
    let server = TestServer::new().await;
    let name = "test-split-invalid";
    server.create_stream(name, 1).await;

    // Hash key = 0 is not valid for split (must be > start+1)
    let res = server
        .request(
            "SplitShard",
            &json!({
                "StreamName": name,
                "ShardToSplit": "shardId-000000000000",
                "NewStartingHashKey": "0",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn split_shard_limit_exceeded() {
    let server = TestServer::with_options(ferrokinesis::store::StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 1,
    })
    .await;

    let name = "test-split-limit";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "SplitShard",
            &json!({
                "StreamName": name,
                "ShardToSplit": "shardId-000000000000",
                "NewStartingHashKey": "170141183460469231731687303715884105728",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "LimitExceededException");
}

// -- MergeShards --

#[tokio::test]
async fn merge_shards_success() {
    let server = TestServer::new().await;
    let name = "test-merge-shards";
    server.create_stream(name, 2).await;

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

    // Wait for update
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();

    // Original two shards should be closed
    assert!(shards[0]["SequenceNumberRange"]["EndingSequenceNumber"]
        .as_str()
        .is_some());
    assert!(shards[1]["SequenceNumberRange"]["EndingSequenceNumber"]
        .as_str()
        .is_some());

    // One new merged shard
    assert_eq!(shards.len(), 3);
    assert!(shards[2]["ParentShardId"].as_str().is_some());
    assert!(shards[2]["AdjacentParentShardId"].as_str().is_some());

    // Merged shard should cover the full range
    assert_eq!(shards[2]["HashKeyRange"]["StartingHashKey"], "0");
    assert_eq!(
        shards[2]["HashKeyRange"]["EndingHashKey"],
        "340282366920938463463374607431768211455"
    );
}

#[tokio::test]
async fn merge_shards_not_adjacent() {
    let server = TestServer::new().await;
    let name = "test-merge-not-adj";
    server.create_stream(name, 3).await;

    // Try to merge shard 0 and shard 2 (not adjacent)
    let res = server
        .request(
            "MergeShards",
            &json!({
                "StreamName": name,
                "ShardToMerge": "shardId-000000000000",
                "AdjacentShardToMerge": "shardId-000000000002",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(body["message"]
        .as_str()
        .unwrap()
        .contains("not an adjacent pair"));
}

#[tokio::test]
async fn merge_shards_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "MergeShards",
            &json!({
                "StreamName": "nonexistent",
                "ShardToMerge": "shardId-000000000000",
                "AdjacentShardToMerge": "shardId-000000000001",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn merge_shards_stream_not_active() {
    let server = TestServer::with_options(ferrokinesis::store::StoreOptions {
        create_stream_ms: 5000,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
    })
    .await;

    let name = "test-merge-creating";
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 2}),
        )
        .await;
    assert_eq!(res.status(), 200);

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
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
}
