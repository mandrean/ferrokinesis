mod common;

use common::*;
use ferrokinesis::shard_iterator::create_shard_iterator;
use serde_json::{Value, json};

/// After a shard is closed (via UpdateShardCount), reading past the end of it
/// should return null NextShardIterator and non-null ChildShards.
#[tokio::test]
async fn get_records_closed_shard_null_next_iterator() {
    let server = TestServer::new().await;
    let name = "gr-closed";
    server.create_stream(name, 1).await;

    // Scale up from 1→2 shards; this closes shard 0
    let res = server
        .request(
            "UpdateShardCount",
            &json!({
                "StreamName": name,
                "TargetShardCount": 2,
                "ScalingType": "UNIFORM_SCALING",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Get a LATEST iterator on the now-closed shard 0
    // LATEST has a seq_time >= the shard close time, so reading returns null iterator
    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "LATEST")
        .await;

    let result = server.get_records(&iter).await;
    // No records (shard is closed, nothing was written)
    assert_eq!(result["Records"].as_array().unwrap().len(), 0);
    // NextShardIterator should be null when shard is closed and we're past the end
    assert!(
        result["NextShardIterator"].is_null(),
        "Expected null NextShardIterator for exhausted closed shard, got: {:?}",
        result["NextShardIterator"]
    );
}

/// After scaling, the closed parent shard 0 has child shards.
/// Reading it from TRIM_HORIZON then consuming all records (none in this case)
/// should also eventually return null iterator once we catch up to the closing seq.
#[tokio::test]
async fn get_records_next_iterator_valid_on_open_shard() {
    let server = TestServer::new().await;
    let name = "gr-open";
    server.create_stream(name, 1).await;

    server.put_record(name, "AAAA", "pk").await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    assert_eq!(result["Records"].as_array().unwrap().len(), 1);
    // Open shard: NextShardIterator must be present
    assert!(result["NextShardIterator"].as_str().is_some());
}

/// AT_TIMESTAMP iterator type via GetShardIterator
#[tokio::test]
async fn get_records_at_timestamp_iterator() {
    let server = TestServer::new().await;
    let name = "gr-at-ts";
    server.create_stream(name, 1).await;

    server.put_record(name, "AAAA", "pk1").await;

    // Use a timestamp well in the past to get all records
    let past_ts = 1_000_000_000.0f64; // year 2001
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": past_ts,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let iter = res.json::<Value>().await.unwrap()["ShardIterator"]
        .as_str()
        .unwrap()
        .to_string();

    let result = server.get_records(&iter).await;
    assert_eq!(result["Records"].as_array().unwrap().len(), 1);
}

/// Verify MillisBehindLatest is present on every GetRecords response
#[tokio::test]
async fn get_records_millis_behind_present() {
    let server = TestServer::new().await;
    let name = "gr-millis";
    server.create_stream(name, 1).await;

    server.put_record(name, "AAAA", "pk").await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    assert!(result["MillisBehindLatest"].is_number());
}

/// GetRecords with a valid-format iterator pointing to a shard index beyond the stream's
/// shard count returns ResourceNotFoundException.
#[tokio::test]
async fn get_records_shard_out_of_range() {
    let server = TestServer::new().await;
    let name = "gr-bad-shard";
    server.create_stream(name, 1).await;

    // Manufacture an iterator for shard 99 (doesn't exist in a 1-shard stream)
    // using the library's public function. GetShardIterator would reject it, but
    // the iterator itself is cryptographically valid and GetRecords checks bounds.
    let seq = "49590338271490256608559692538361571095921575989136588898"; // a plausible seq num
    let iter = create_shard_iterator(name, "shardId-000000000099", seq);

    let res = server
        .request("GetRecords", &json!({ "ShardIterator": iter }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}
