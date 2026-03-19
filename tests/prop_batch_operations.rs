mod common;
use common::*;

use num_bigint::BigUint;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_stream_name() -> String {
    format!("prop-batch-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// P13: PutRecords response count equals request count, with zero failures.
/// P14: Every response record has a valid ShardId and SequenceNumber.
#[test]
fn prop_batch_response_count_and_fields() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let stream_name = unique_stream_name();
    rt.block_on(server.create_stream(&stream_name, 4));

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    runner
        .run(&(1u32..=50), |batch_size| {
            let body = rt.block_on(async {
                let records: Vec<serde_json::Value> = (0..batch_size)
                    .map(|i| {
                        json!({
                            "Data": "dGVzdA==",
                            "PartitionKey": format!("key-{}", i),
                        })
                    })
                    .collect();

                let res = server
                    .request(
                        "PutRecords",
                        &json!({
                            "StreamName": stream_name,
                            "Records": records,
                        }),
                    )
                    .await;
                assert_eq!(res.status(), 200);
                let body: serde_json::Value = res.json().await.unwrap();
                body
            });

            // P13: Response count == request count
            let response_records = body["Records"].as_array().unwrap();
            prop_assert_eq!(
                response_records.len(),
                batch_size as usize,
                "response record count {} != request count {}",
                response_records.len(),
                batch_size
            );

            // P13: FailedRecordCount == 0
            let failed = body["FailedRecordCount"].as_u64().unwrap();
            prop_assert_eq!(
                failed,
                0,
                "FailedRecordCount is {} for batch of {}",
                failed,
                batch_size
            );

            // P14: Every record has valid ShardId and SequenceNumber
            for (i, rec) in response_records.iter().enumerate() {
                let shard_id = rec["ShardId"].as_str().unwrap();
                prop_assert!(
                    shard_id.starts_with("shardId-"),
                    "record {} has invalid ShardId: {:?}",
                    i,
                    shard_id
                );

                let seq_num = rec["SequenceNumber"].as_str().unwrap();
                prop_assert!(!seq_num.is_empty(), "record {} has empty SequenceNumber", i);
            }

            Ok(())
        })
        .unwrap();
}

/// P15: PutRecords with ExplicitHashKey routes each record to the correct shard.
#[test]
fn prop_batch_explicit_hash_key_routing() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let shard_count = 4u32;
    let stream_name = unique_stream_name();
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
        cases: 50,
        ..Config::default()
    });

    // Generate a small batch of hash keys
    let strategy = proptest::collection::vec(any::<u128>(), 1..=10);

    runner
        .run(&strategy, |hash_keys| {
            let response_records = rt.block_on(async {
                let records: Vec<serde_json::Value> = hash_keys
                    .iter()
                    .map(|hk| {
                        json!({
                            "Data": "dGVzdA==",
                            "PartitionKey": "ignored",
                            "ExplicitHashKey": hk.to_string(),
                        })
                    })
                    .collect();

                let res = server
                    .request(
                        "PutRecords",
                        &json!({
                            "StreamName": stream_name,
                            "Records": records,
                        }),
                    )
                    .await;
                assert_eq!(res.status(), 200);
                let body: serde_json::Value = res.json().await.unwrap();
                body["Records"].as_array().unwrap().clone()
            });

            for (i, hk) in hash_keys.iter().enumerate() {
                let returned_shard = response_records[i]["ShardId"].as_str().unwrap();
                let hk_big = BigUint::from(*hk);

                let expected_shard = shard_ranges
                    .iter()
                    .find(|(_, start, end)| hk_big >= *start && hk_big <= *end)
                    .map(|(id, _, _)| id.as_str());

                prop_assert_eq!(
                    Some(returned_shard),
                    expected_shard,
                    "record {} with ExplicitHashKey {} routed to wrong shard",
                    i,
                    hk
                );
            }

            Ok(())
        })
        .unwrap();
}
