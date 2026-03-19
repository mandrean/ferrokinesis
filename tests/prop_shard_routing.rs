// Case count rationale: 100 cases — each test creates streams and does PutRecord calls,
// so moderate count balances coverage against server round-trip cost.
mod common;
use common::*;

use ferrokinesis::store::StoreOptions;
use num_bigint::BigUint;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

/// Extract the numeric shard index from a shard ID like "shardId-000000000042"
fn shard_index(shard_id: &str) -> u64 {
    shard_id
        .strip_prefix("shardId-")
        .unwrap()
        .parse::<u64>()
        .unwrap()
}

/// P6: Every partition key routes to a valid shard within [0, N).
#[test]
fn prop_partition_key_routes_to_valid_shard() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 200,
        ..Default::default()
    }));

    let mut runner = TestRunner::new(Config {
        cases: 100,
        ..Config::default()
    });

    // Create streams with different shard counts up front
    let stream_names: Vec<(String, u32)> = (1..=10)
        .map(|n| {
            let name = unique_stream_name("prop-route");
            rt.block_on(server.create_stream(&name, n));
            (name, n)
        })
        .collect();

    let strategy = (0..10usize, "[a-zA-Z0-9_.\\-]{1,256}");

    runner
        .run(&strategy, |(stream_idx, partition_key)| {
            let (stream_name, shard_count) = &stream_names[stream_idx];

            let shard_id = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": stream_name,
                            "Data": "dGVzdA==",
                            "PartitionKey": partition_key,
                        }),
                    )
                    .await;
                assert_eq!(res.status(), 200);
                let body: serde_json::Value = res.json().await.unwrap();
                body["ShardId"].as_str().unwrap().to_string()
            });

            let idx = shard_index(&shard_id);
            prop_assert!(
                idx < *shard_count as u64,
                "shard index {} out of range [0, {}) for partition key {:?}",
                idx,
                shard_count,
                partition_key
            );

            Ok(())
        })
        .unwrap();
}

/// P7: ExplicitHashKey overrides partition key hashing and routes to the
/// shard whose hash range contains the explicit key.
#[test]
fn prop_explicit_hash_key_routes_correctly() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let shard_count = 4u32;
    let stream_name = unique_stream_name("prop-ehk");
    rt.block_on(server.create_stream(&stream_name, shard_count));

    // Get shard hash ranges
    let shard_ranges: Vec<(String, BigUint, BigUint)> = rt.block_on(async {
        let desc = server.describe_stream(&stream_name).await;
        desc["StreamDescription"]["Shards"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                let id = s["ShardId"].as_str().unwrap().to_string();
                let start: BigUint = s["HashKeyRange"]["StartingHashKey"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                let end: BigUint = s["HashKeyRange"]["EndingHashKey"]
                    .as_str()
                    .unwrap()
                    .parse()
                    .unwrap();
                (id, start, end)
            })
            .collect()
    });

    let mut runner = TestRunner::new(Config {
        cases: 100,
        ..Config::default()
    });

    // Generate hash keys as u128 then convert to string
    runner
        .run(&any::<u128>(), |hash_key_val| {
            let hash_key_str = hash_key_val.to_string();

            let returned_shard_id = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": stream_name,
                            "Data": "dGVzdA==",
                            "PartitionKey": "ignored",
                            "ExplicitHashKey": hash_key_str,
                        }),
                    )
                    .await;
                assert_eq!(res.status(), 200);
                let body: serde_json::Value = res.json().await.unwrap();
                body["ShardId"].as_str().unwrap().to_string()
            });

            let hash_key_big = BigUint::from(hash_key_val);
            let expected_shard = shard_ranges
                .iter()
                .find(|(_, start, end)| hash_key_big >= *start && hash_key_big <= *end)
                .map(|(id, _, _)| id.clone());

            prop_assert_eq!(
                Some(returned_shard_id.clone()),
                expected_shard,
                "explicit hash key {} routed to {} but expected range match",
                hash_key_str,
                returned_shard_id
            );

            Ok(())
        })
        .unwrap();
}

/// P8: Same partition key always routes to the same shard (determinism).
#[test]
fn prop_same_partition_key_same_shard() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let stream_name = unique_stream_name("prop-determ");
    rt.block_on(server.create_stream(&stream_name, 4));

    let mut runner = TestRunner::new(Config {
        cases: 100,
        ..Config::default()
    });

    runner
        .run(&"[a-zA-Z0-9_.\\-]{1,256}", |partition_key| {
            let (shard1, shard2) = rt.block_on(async {
                let req = json!({
                    "StreamName": stream_name,
                    "Data": "dGVzdA==",
                    "PartitionKey": partition_key,
                });
                let r1 = server.request("PutRecord", &req).await;
                assert_eq!(r1.status(), 200);
                let b1: serde_json::Value = r1.json().await.unwrap();

                let r2 = server.request("PutRecord", &req).await;
                assert_eq!(r2.status(), 200);
                let b2: serde_json::Value = r2.json().await.unwrap();

                (
                    b1["ShardId"].as_str().unwrap().to_string(),
                    b2["ShardId"].as_str().unwrap().to_string(),
                )
            });

            prop_assert_eq!(
                shard1,
                shard2,
                "partition key {:?} routed to different shards",
                partition_key
            );
            Ok(())
        })
        .unwrap();
}
