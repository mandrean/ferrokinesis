mod common;

use common::*;
use ferrokinesis::store::StoreOptions;
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
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"]
            .as_str()
            .is_some()
    );

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
        ..Default::default()
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
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"]
            .as_str()
            .is_some()
    );
    assert!(
        shards[1]["SequenceNumberRange"]["EndingSequenceNumber"]
            .as_str()
            .is_some()
    );

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
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("not an adjacent pair")
    );
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
        ..Default::default()
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

#[tokio::test]
async fn split_shard_out_of_range() {
    let server = TestServer::new().await;
    let name = "test-ss-oor";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "SplitShard",
            &json!({
                "StreamName": name,
                "ShardToSplit": "shardId-000000000005",
                "NewStartingHashKey": "170141183460469231731687303715884105728",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn merge_shards_out_of_range() {
    let server = TestServer::new().await;
    let name = "test-ms-oor";
    server.create_stream(name, 2).await;

    let res = server
        .request(
            "MergeShards",
            &json!({
                "StreamName": name,
                "ShardToMerge": "shardId-000000000000",
                "AdjacentShardToMerge": "shardId-000000000005",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn split_shard_stream_updating() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 2000,
        shard_limit: 50,
        ..Default::default()
    })
    .await;
    let name = "test-ss-updating";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

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
    assert_eq!(body["__type"], "ResourceInUseException");
}

#[tokio::test]
async fn merge_shards_stream_updating() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 2000,
        shard_limit: 50,
        ..Default::default()
    })
    .await;
    let name = "test-ms-updating";
    server.create_stream(name, 2).await;

    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
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

// -- SplitShard / MergeShards correctness tests --

const MAX_HASH_KEY: &str = "340282366920938463463374607431768211455";

/// Helper: PutRecord with ExplicitHashKey and return the response body.
async fn put_record_with_hash_key(
    server: &TestServer,
    stream: &str,
    data: &str,
    partition_key: &str,
    explicit_hash_key: &str,
) -> Value {
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": stream,
                "Data": data,
                "PartitionKey": partition_key,
                "ExplicitHashKey": explicit_hash_key,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

/// Helper: split a single-shard stream at the midpoint and return (mid, shards).
async fn split_at_midpoint(server: &TestServer, name: &str) -> (String, Vec<Value>) {
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
    server.wait_for_stream_active(name).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"]
        .as_array()
        .unwrap()
        .clone();
    (mid.to_string(), shards)
}

// -- Test 1: Exact child hash key ranges after split --

#[tokio::test]
async fn split_shard_exact_child_hash_ranges() {
    let server = TestServer::new().await;
    let name = "test-split-exact-ranges";
    server.create_stream(name, 1).await;

    let (mid, shards) = split_at_midpoint(&server, name).await;
    let mid_val: u128 = mid.parse().unwrap();

    assert_eq!(shards.len(), 3);

    // Parent closed
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"]
            .as_str()
            .is_some()
    );

    // Child 1: [0, mid - 1]
    assert_eq!(shards[1]["HashKeyRange"]["StartingHashKey"], "0");
    assert_eq!(
        shards[1]["HashKeyRange"]["EndingHashKey"],
        (mid_val - 1).to_string()
    );
    assert_eq!(shards[1]["ParentShardId"], "shardId-000000000000");
    assert!(shards[1]["AdjacentParentShardId"].is_null());

    // Child 2: [mid, 2^128 - 1]
    assert_eq!(shards[2]["HashKeyRange"]["StartingHashKey"], mid);
    assert_eq!(shards[2]["HashKeyRange"]["EndingHashKey"], MAX_HASH_KEY);
    assert_eq!(shards[2]["ParentShardId"], "shardId-000000000000");
    assert!(shards[2]["AdjacentParentShardId"].is_null());

    // Both children are open
    assert!(shards[1]["SequenceNumberRange"]["EndingSequenceNumber"].is_null());
    assert!(shards[2]["SequenceNumberRange"]["EndingSequenceNumber"].is_null());
}

// -- Test 2: Asymmetric split at minimum valid hash key --

#[tokio::test]
async fn split_shard_asymmetric_hash_ranges() {
    let server = TestServer::new().await;
    let name = "test-split-asym";
    server.create_stream(name, 1).await;

    // Minimum valid split point: must be > start + 1 = 0 + 1 = 1, so "2" is valid
    let res = server
        .request(
            "SplitShard",
            &json!({
                "StreamName": name,
                "ShardToSplit": "shardId-000000000000",
                "NewStartingHashKey": "2",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    server.wait_for_stream_active(name).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();

    assert_eq!(shards.len(), 3);

    // Child 1: [0, 1] — tiny sliver
    assert_eq!(shards[1]["HashKeyRange"]["StartingHashKey"], "0");
    assert_eq!(shards[1]["HashKeyRange"]["EndingHashKey"], "1");

    // Child 2: [2, 2^128 - 1] — nearly full range
    assert_eq!(shards[2]["HashKeyRange"]["StartingHashKey"], "2");
    assert_eq!(shards[2]["HashKeyRange"]["EndingHashKey"], MAX_HASH_KEY);
}

// -- Test 3: Partial merge with exact lineage --

#[tokio::test]
async fn merge_shards_partial_range_with_lineage() {
    let server = TestServer::new().await;
    let name = "test-merge-partial";
    server.create_stream(name, 3).await;

    // Capture shard ranges before merge
    let desc = server.describe_stream(name).await;
    let orig_shards = desc["StreamDescription"]["Shards"].as_array().unwrap();
    let shard0_start = orig_shards[0]["HashKeyRange"]["StartingHashKey"]
        .as_str()
        .unwrap()
        .to_string();
    let shard1_end = orig_shards[1]["HashKeyRange"]["EndingHashKey"]
        .as_str()
        .unwrap()
        .to_string();
    let shard2_start = orig_shards[2]["HashKeyRange"]["StartingHashKey"]
        .as_str()
        .unwrap()
        .to_string();
    let shard2_end = orig_shards[2]["HashKeyRange"]["EndingHashKey"]
        .as_str()
        .unwrap()
        .to_string();

    // Merge shards 0 + 1, leaving shard 2 unaffected
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
    server.wait_for_stream_active(name).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();

    assert_eq!(shards.len(), 4); // 3 original + 1 merged child

    // Shards 0 and 1 closed
    assert!(
        shards[0]["SequenceNumberRange"]["EndingSequenceNumber"]
            .as_str()
            .is_some()
    );
    assert!(
        shards[1]["SequenceNumberRange"]["EndingSequenceNumber"]
            .as_str()
            .is_some()
    );

    // Shard 2 unchanged and still open
    assert_eq!(
        shards[2]["HashKeyRange"]["StartingHashKey"]
            .as_str()
            .unwrap(),
        shard2_start
    );
    assert_eq!(
        shards[2]["HashKeyRange"]["EndingHashKey"].as_str().unwrap(),
        shard2_end
    );
    assert!(shards[2]["SequenceNumberRange"]["EndingSequenceNumber"].is_null());

    // Merged child: partial range [shard0.start, shard1.end], NOT full range
    assert_eq!(
        shards[3]["HashKeyRange"]["StartingHashKey"]
            .as_str()
            .unwrap(),
        shard0_start
    );
    assert_eq!(
        shards[3]["HashKeyRange"]["EndingHashKey"].as_str().unwrap(),
        shard1_end
    );
    assert_ne!(
        shards[3]["HashKeyRange"]["EndingHashKey"].as_str().unwrap(),
        MAX_HASH_KEY,
        "merged range should be partial, not cover full hash space"
    );

    // Exact lineage
    assert_eq!(shards[3]["ParentShardId"], "shardId-000000000000");
    assert_eq!(shards[3]["AdjacentParentShardId"], "shardId-000000000001");
    assert!(shards[3]["SequenceNumberRange"]["EndingSequenceNumber"].is_null());
}

// -- Test 4: Split parent EndingSequenceNumber validity --

#[tokio::test]
async fn split_shard_ending_sequence_number_valid() {
    let server = TestServer::new().await;
    let name = "test-split-end-seq";
    server.create_stream(name, 1).await;

    // Put 2 records and save their sequence numbers
    let r1 = server.put_record(name, "dGVzdDE=", "pk1").await;
    let r2 = server.put_record(name, "dGVzdDI=", "pk2").await;
    let seq1 = r1["SequenceNumber"].as_str().unwrap().to_string();
    let seq2 = r2["SequenceNumber"].as_str().unwrap().to_string();

    // Split
    let (_, shards) = split_at_midpoint(&server, name).await;
    let end_seq = shards[0]["SequenceNumberRange"]["EndingSequenceNumber"]
        .as_str()
        .unwrap();

    // EndingSequenceNumber must be lexicographically greater than both record sequence numbers
    assert!(
        end_seq > seq1.as_str(),
        "EndingSequenceNumber {end_seq} should be > record seq {seq1}"
    );
    assert!(
        end_seq > seq2.as_str(),
        "EndingSequenceNumber {end_seq} should be > record seq {seq2}"
    );
}

// -- Test 5: Merge parents EndingSequenceNumber validity --

#[tokio::test]
async fn merge_shards_ending_sequence_numbers_valid() {
    let server = TestServer::new().await;
    let name = "test-merge-end-seq";
    server.create_stream(name, 2).await;

    // Get shard 1's starting hash key for explicit routing
    let desc = server.describe_stream(name).await;
    let shard1_start = desc["StreamDescription"]["Shards"][1]["HashKeyRange"]["StartingHashKey"]
        .as_str()
        .unwrap()
        .to_string();

    // Put a record to each shard via ExplicitHashKey
    let r0 = put_record_with_hash_key(&server, name, "dGVzdDE=", "pk0", "0").await;
    let r1 = put_record_with_hash_key(&server, name, "dGVzdDI=", "pk1", &shard1_start).await;
    let seq0 = r0["SequenceNumber"].as_str().unwrap().to_string();
    let seq1 = r1["SequenceNumber"].as_str().unwrap().to_string();

    // Merge
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
    server.wait_for_stream_active(name).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();

    let end_seq0 = shards[0]["SequenceNumberRange"]["EndingSequenceNumber"]
        .as_str()
        .unwrap();
    let end_seq1 = shards[1]["SequenceNumberRange"]["EndingSequenceNumber"]
        .as_str()
        .unwrap();

    assert!(
        end_seq0 > seq0.as_str(),
        "Shard 0 EndingSequenceNumber {end_seq0} should be > record seq {seq0}"
    );
    assert!(
        end_seq1 > seq1.as_str(),
        "Shard 1 EndingSequenceNumber {end_seq1} should be > record seq {seq1}"
    );
}

// -- Test 6: Record routing to correct child after split --

#[tokio::test]
async fn put_record_routes_to_correct_child_after_split() {
    let server = TestServer::new().await;
    let name = "test-split-routing";
    server.create_stream(name, 1).await;

    let (mid, shards) = split_at_midpoint(&server, name).await;
    let child1_id = shards[1]["ShardId"].as_str().unwrap();
    let child2_id = shards[2]["ShardId"].as_str().unwrap();

    // ExplicitHashKey "0" → child 1 (lower range)
    let r1 = put_record_with_hash_key(&server, name, "dGVzdA==", "pk1", "0").await;
    assert_eq!(
        r1["ShardId"].as_str().unwrap(),
        child1_id,
        "hash key 0 should route to child 1"
    );

    // ExplicitHashKey = mid → child 2 (upper range)
    let r2 = put_record_with_hash_key(&server, name, "dGVzdA==", "pk2", &mid).await;
    assert_eq!(
        r2["ShardId"].as_str().unwrap(),
        child2_id,
        "hash key {mid} should route to child 2"
    );

    // ExplicitHashKey = max → child 2
    let r3 = put_record_with_hash_key(&server, name, "dGVzdA==", "pk3", MAX_HASH_KEY).await;
    assert_eq!(
        r3["ShardId"].as_str().unwrap(),
        child2_id,
        "hash key max should route to child 2"
    );
}

// -- Test 7: Record routing to merged shard after merge --

#[tokio::test]
async fn put_record_routes_to_merged_shard_after_merge() {
    let server = TestServer::new().await;
    let name = "test-merge-routing";
    server.create_stream(name, 3).await;

    let desc = server.describe_stream(name).await;
    let orig_shards = desc["StreamDescription"]["Shards"].as_array().unwrap();
    let shard1_start = orig_shards[1]["HashKeyRange"]["StartingHashKey"]
        .as_str()
        .unwrap()
        .to_string();
    let shard2_start = orig_shards[2]["HashKeyRange"]["StartingHashKey"]
        .as_str()
        .unwrap()
        .to_string();

    // Merge shards 0 + 1
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
    server.wait_for_stream_active(name).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();
    let merged_id = shards[3]["ShardId"].as_str().unwrap();

    // Record targeting old shard 0's range → merged child
    let r1 = put_record_with_hash_key(&server, name, "dGVzdA==", "pk1", "0").await;
    assert_eq!(
        r1["ShardId"].as_str().unwrap(),
        merged_id,
        "old shard 0 range should route to merged child"
    );

    // Record targeting old shard 1's range → merged child
    let r2 = put_record_with_hash_key(&server, name, "dGVzdA==", "pk2", &shard1_start).await;
    assert_eq!(
        r2["ShardId"].as_str().unwrap(),
        merged_id,
        "old shard 1 range should route to merged child"
    );

    // Record targeting shard 2's range → shard 2 (unaffected)
    let r3 = put_record_with_hash_key(&server, name, "dGVzdA==", "pk3", &shard2_start).await;
    assert_eq!(
        r3["ShardId"].as_str().unwrap(),
        "shardId-000000000002",
        "shard 2 should be unaffected by merge"
    );
}

// -- Test 8: Pre-split records readable from closed parent via TRIM_HORIZON --

#[tokio::test]
async fn parent_shard_records_readable_after_split() {
    let server = TestServer::new().await;
    let name = "test-split-read-parent";
    server.create_stream(name, 1).await;

    // Put 3 records before split
    let r1 = server.put_record(name, "cmVjb3JkMQ==", "pk1").await;
    let r2 = server.put_record(name, "cmVjb3JkMg==", "pk2").await;
    let r3 = server.put_record(name, "cmVjb3JkMw==", "pk3").await;
    let seq1 = r1["SequenceNumber"].as_str().unwrap().to_string();
    let seq2 = r2["SequenceNumber"].as_str().unwrap().to_string();
    let seq3 = r3["SequenceNumber"].as_str().unwrap().to_string();

    // Split the shard
    let _ = split_at_midpoint(&server, name).await;

    // TRIM_HORIZON on closed parent should return all pre-split records
    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();

    assert_eq!(
        records.len(),
        3,
        "all 3 pre-split records should be returned"
    );
    assert_eq!(records[0]["SequenceNumber"].as_str().unwrap(), seq1);
    assert_eq!(records[1]["SequenceNumber"].as_str().unwrap(), seq2);
    assert_eq!(records[2]["SequenceNumber"].as_str().unwrap(), seq3);
    assert_eq!(records[0]["Data"], "cmVjb3JkMQ==");
    assert_eq!(records[1]["Data"], "cmVjb3JkMg==");
    assert_eq!(records[2]["Data"], "cmVjb3JkMw==");
}

// -- Test 9: AT_SEQUENCE_NUMBER / AFTER_SEQUENCE_NUMBER on closed parent --

#[tokio::test]
async fn closed_parent_at_after_sequence_number_iterators() {
    let server = TestServer::new().await;
    let name = "test-split-seq-iters";
    server.create_stream(name, 1).await;

    // Put 3 records
    let r1 = server.put_record(name, "cmVjMQ==", "pk1").await;
    let r2 = server.put_record(name, "cmVjMg==", "pk2").await;
    let r3 = server.put_record(name, "cmVjMw==", "pk3").await;
    let seq1 = r1["SequenceNumber"].as_str().unwrap().to_string();
    let seq2 = r2["SequenceNumber"].as_str().unwrap().to_string();
    let seq3 = r3["SequenceNumber"].as_str().unwrap().to_string();

    // Split
    let _ = split_at_midpoint(&server, name).await;

    // AT_SEQUENCE_NUMBER with seq2 → should return seq2 and seq3
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq2,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let iter_at = body["ShardIterator"].as_str().unwrap();

    let result = server.get_records(iter_at).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(
        records.len(),
        2,
        "AT_SEQUENCE_NUMBER should return seq2 + seq3"
    );
    assert_eq!(records[0]["SequenceNumber"].as_str().unwrap(), seq2);
    assert_eq!(records[1]["SequenceNumber"].as_str().unwrap(), seq3);

    // AFTER_SEQUENCE_NUMBER with seq2 → should return only seq3
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq2,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let iter_after = body["ShardIterator"].as_str().unwrap();

    let result = server.get_records(iter_after).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(
        records.len(),
        1,
        "AFTER_SEQUENCE_NUMBER should return only seq3"
    );
    assert_eq!(records[0]["SequenceNumber"].as_str().unwrap(), seq3);

    // Also verify AT_SEQUENCE_NUMBER with seq1 returns all 3
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq1,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let iter_all = body["ShardIterator"].as_str().unwrap();

    let result = server.get_records(iter_all).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(
        records.len(),
        3,
        "AT_SEQUENCE_NUMBER with seq1 should return all 3"
    );
}

// -- Test 10: Closed parent returns null NextShardIterator (KCL exhaustion signal) --

#[tokio::test]
async fn closed_parent_returns_null_next_shard_iterator() {
    let server = TestServer::new().await;
    let name = "test-split-null-iter";
    server.create_stream(name, 1).await;

    // Put 1 record
    server.put_record(name, "dGVzdA==", "pk1").await;

    // Split
    let _ = split_at_midpoint(&server, name).await;

    // TRIM_HORIZON on closed parent
    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;

    // First GetRecords should return the record
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);

    // Continue reading until NextShardIterator is null (shard exhausted)
    let mut next_iter = result["NextShardIterator"].as_str().map(|s| s.to_string());
    let mut found_null = false;

    for _ in 0..5 {
        match next_iter {
            None => {
                found_null = true;
                break;
            }
            Some(ref it) => {
                let result = server.get_records(it).await;
                assert_eq!(
                    result["Records"].as_array().unwrap().len(),
                    0,
                    "no more records after first read"
                );
                next_iter = result["NextShardIterator"].as_str().map(|s| s.to_string());
            }
        }
    }

    assert!(
        found_null,
        "NextShardIterator must become null for exhausted closed shard"
    );

    // Meanwhile, child shards should return non-null NextShardIterator (they're open)
    let child_iter = server
        .get_shard_iterator(name, "shardId-000000000001", "TRIM_HORIZON")
        .await;
    let child_result = server.get_records(&child_iter).await;
    assert!(
        child_result["NextShardIterator"].as_str().is_some(),
        "open child shard should have non-null NextShardIterator"
    );
}

// -- Test 11: Child shards receive and serve records after split --

#[tokio::test]
async fn child_shards_receive_and_serve_records_after_split() {
    let server = TestServer::new().await;
    let name = "test-split-child-records";
    server.create_stream(name, 1).await;

    let (mid, shards) = split_at_midpoint(&server, name).await;
    let child1_id = shards[1]["ShardId"].as_str().unwrap().to_string();
    let child2_id = shards[2]["ShardId"].as_str().unwrap().to_string();

    // Put 2 records to child 1 (lower range, hash key "0")
    put_record_with_hash_key(&server, name, "Y2hpbGQxLXIx", "pk1", "0").await;
    put_record_with_hash_key(&server, name, "Y2hpbGQxLXIy", "pk2", "1").await;

    // Put 2 records to child 2 (upper range)
    put_record_with_hash_key(&server, name, "Y2hpbGQyLXIx", "pk3", &mid).await;
    put_record_with_hash_key(&server, name, "Y2hpbGQyLXIy", "pk4", MAX_HASH_KEY).await;

    // Read from child 1
    let iter1 = server
        .get_shard_iterator(name, &child1_id, "TRIM_HORIZON")
        .await;
    let result1 = server.get_records(&iter1).await;
    let records1 = result1["Records"].as_array().unwrap();
    assert_eq!(records1.len(), 2, "child 1 should have 2 records");
    assert_eq!(records1[0]["Data"], "Y2hpbGQxLXIx");
    assert_eq!(records1[1]["Data"], "Y2hpbGQxLXIy");
    assert!(
        result1["NextShardIterator"].as_str().is_some(),
        "open child shard should have non-null NextShardIterator"
    );

    // Read from child 2
    let iter2 = server
        .get_shard_iterator(name, &child2_id, "TRIM_HORIZON")
        .await;
    let result2 = server.get_records(&iter2).await;
    let records2 = result2["Records"].as_array().unwrap();
    assert_eq!(records2.len(), 2, "child 2 should have 2 records");
    assert_eq!(records2[0]["Data"], "Y2hpbGQyLXIx");
    assert_eq!(records2[1]["Data"], "Y2hpbGQyLXIy");
    assert!(
        result2["NextShardIterator"].as_str().is_some(),
        "open child shard should have non-null NextShardIterator"
    );
}

// -- Test 12: LATEST iterator on closed parent after split returns empty --

#[tokio::test]
async fn closed_parent_latest_iterator_returns_empty() {
    let server = TestServer::new().await;
    let name = "test-split-latest-closed";
    server.create_stream(name, 1).await;

    // Put records before split
    server.put_record(name, "cmVjMQ==", "pk1").await;
    server.put_record(name, "cmVjMg==", "pk2").await;

    // Split the shard (closes the parent)
    let _ = split_at_midpoint(&server, name).await;

    // LATEST on closed parent — should return empty records
    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "LATEST")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();

    assert_eq!(
        records.len(),
        0,
        "LATEST on closed parent should return no records"
    );

    // NextShardIterator should become null (shard exhausted)
    let mut next_iter = result["NextShardIterator"].as_str().map(|s| s.to_string());
    let mut found_null = false;

    for _ in 0..5 {
        match next_iter {
            None => {
                found_null = true;
                break;
            }
            Some(ref it) => {
                let result = server.get_records(it).await;
                assert_eq!(result["Records"].as_array().unwrap().len(), 0);
                next_iter = result["NextShardIterator"].as_str().map(|s| s.to_string());
            }
        }
    }

    assert!(
        found_null,
        "NextShardIterator must become null for closed parent with LATEST iterator"
    );
}

// -- Test 13: Split a child shard (re-split) creates third generation --

#[tokio::test]
async fn split_child_shard_creates_third_generation() {
    let server = TestServer::new().await;
    let name = "test-split-resplit";
    server.create_stream(name, 1).await;

    // First split: parent → child1 (lower) + child2 (upper)
    let (_, shards) = split_at_midpoint(&server, name).await;
    assert_eq!(shards.len(), 3);

    let child2_id = shards[2]["ShardId"].as_str().unwrap().to_string();
    let child2_start: u128 = shards[2]["HashKeyRange"]["StartingHashKey"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let child2_end: u128 = shards[2]["HashKeyRange"]["EndingHashKey"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let child2_mid = child2_start + (child2_end - child2_start) / 2;

    // Second split: child2 → grandchild1 (lower) + grandchild2 (upper)
    let res = server
        .request(
            "SplitShard",
            &json!({
                "StreamName": name,
                "ShardToSplit": child2_id,
                "NewStartingHashKey": child2_mid.to_string(),
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    server.wait_for_stream_active(name).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();

    // 5 total: parent + child1 + child2 (closed) + grandchild1 + grandchild2
    assert_eq!(shards.len(), 5);

    // child2 should now be closed
    assert!(
        shards[2]["SequenceNumberRange"]["EndingSequenceNumber"]
            .as_str()
            .is_some(),
        "child2 should be closed after re-split"
    );

    // Grandchildren should reference child2 as parent
    assert_eq!(shards[3]["ParentShardId"], child2_id);
    assert_eq!(shards[4]["ParentShardId"], child2_id);
    assert!(shards[3]["AdjacentParentShardId"].is_null());
    assert!(shards[4]["AdjacentParentShardId"].is_null());

    // Grandchild hash ranges partition child2's range exactly
    // Grandchild 1: [child2_start, child2_mid - 1]
    assert_eq!(
        shards[3]["HashKeyRange"]["StartingHashKey"]
            .as_str()
            .unwrap(),
        child2_start.to_string()
    );
    assert_eq!(
        shards[3]["HashKeyRange"]["EndingHashKey"].as_str().unwrap(),
        (child2_mid - 1).to_string()
    );

    // Grandchild 2: [child2_mid, child2_end]
    assert_eq!(
        shards[4]["HashKeyRange"]["StartingHashKey"]
            .as_str()
            .unwrap(),
        child2_mid.to_string()
    );
    assert_eq!(
        shards[4]["HashKeyRange"]["EndingHashKey"].as_str().unwrap(),
        child2_end.to_string()
    );

    // Both grandchildren are open
    assert!(shards[3]["SequenceNumberRange"]["EndingSequenceNumber"].is_null());
    assert!(shards[4]["SequenceNumberRange"]["EndingSequenceNumber"].is_null());

    // Verify record routing to grandchildren
    let gc1_id = shards[3]["ShardId"].as_str().unwrap();
    let gc2_id = shards[4]["ShardId"].as_str().unwrap();
    let child2_start_str = child2_start.to_string();
    let child2_mid_str = child2_mid.to_string();

    let r1 = put_record_with_hash_key(&server, name, "dGVzdA==", "pk1", &child2_start_str).await;
    assert_eq!(
        r1["ShardId"].as_str().unwrap(),
        gc1_id,
        "hash key at child2_start should route to grandchild 1"
    );

    let r2 = put_record_with_hash_key(&server, name, "dGVzdA==", "pk2", &child2_mid_str).await;
    assert_eq!(
        r2["ShardId"].as_str().unwrap(),
        gc2_id,
        "hash key at child2_mid should route to grandchild 2"
    );

    let r3 = put_record_with_hash_key(&server, name, "dGVzdA==", "pk3", MAX_HASH_KEY).await;
    assert_eq!(
        r3["ShardId"].as_str().unwrap(),
        gc2_id,
        "hash key max should route to grandchild 2"
    );
}

// -- Test 14: Merged child shard serves records put to both parents --

#[tokio::test]
async fn merged_child_shard_serves_records_from_both_parents() {
    let server = TestServer::new().await;
    let name = "test-merge-read-child";
    server.create_stream(name, 2).await;

    // Get shard 1's starting hash key for explicit routing
    let desc = server.describe_stream(name).await;
    let shard1_start = desc["StreamDescription"]["Shards"][1]["HashKeyRange"]["StartingHashKey"]
        .as_str()
        .unwrap()
        .to_string();

    // Put a record to each parent shard
    put_record_with_hash_key(&server, name, "cGFyZW50MA==", "pk0", "0").await;
    put_record_with_hash_key(&server, name, "cGFyZW50MQ==", "pk1", &shard1_start).await;

    // Merge shards 0 + 1
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
    server.wait_for_stream_active(name).await;

    // Put a record to the merged child
    put_record_with_hash_key(&server, name, "bWVyZ2Vk", "pk2", "0").await;

    // Read from merged child via TRIM_HORIZON
    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();
    let merged_id = shards[2]["ShardId"].as_str().unwrap();

    let iter = server
        .get_shard_iterator(name, merged_id, "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();

    assert_eq!(
        records.len(),
        1,
        "merged child should serve the record put after merge"
    );
    assert_eq!(records[0]["Data"], "bWVyZ2Vk");

    // Parent shards should still serve their pre-merge records
    let iter0 = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result0 = server.get_records(&iter0).await;
    let records0 = result0["Records"].as_array().unwrap();
    assert_eq!(records0.len(), 1, "parent 0 should still have its record");
    assert_eq!(records0[0]["Data"], "cGFyZW50MA==");

    let iter1 = server
        .get_shard_iterator(name, "shardId-000000000001", "TRIM_HORIZON")
        .await;
    let result1 = server.get_records(&iter1).await;
    let records1 = result1["Records"].as_array().unwrap();
    assert_eq!(records1.len(), 1, "parent 1 should still have its record");
    assert_eq!(records1[0]["Data"], "cGFyZW50MQ==");
}
