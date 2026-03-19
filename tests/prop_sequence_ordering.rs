// Case count rationale: 50 cases — each iteration does multiple PutRecord/PutRecords
// calls plus GetRecords, so lower count keeps total runtime reasonable.
mod common;
use common::*;

use ferrokinesis::store::StoreOptions;
use num_bigint::BigUint;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

/// P11: Sequential PutRecord calls produce strictly increasing sequence numbers.
#[test]
fn prop_sequential_puts_increasing_sequence_numbers() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 200,
        ..Default::default()
    }));

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    runner
        .run(&(2u32..=20), |record_count| {
            let stream_name = unique_stream_name("prop-seq");

            let seq_nums: Vec<String> = rt.block_on(async {
                server.create_stream(&stream_name, 1).await;

                let mut seqs = Vec::with_capacity(record_count as usize);
                for _ in 0..record_count {
                    let res = server
                        .request(
                            "PutRecord",
                            &json!({
                                "StreamName": stream_name,
                                "Data": "dGVzdA==",
                                "PartitionKey": "same-key",
                            }),
                        )
                        .await;
                    assert_eq!(res.status(), 200);
                    let body: serde_json::Value = res.json().await.unwrap();
                    seqs.push(body["SequenceNumber"].as_str().unwrap().to_string());
                }
                seqs
            });

            let parsed: Vec<BigUint> = seq_nums
                .iter()
                .map(|s| s.parse::<BigUint>().unwrap())
                .collect();

            for i in 0..parsed.len() - 1 {
                prop_assert!(
                    parsed[i] < parsed[i + 1],
                    "sequence number ordering violated at index {}: {} >= {}",
                    i,
                    parsed[i],
                    parsed[i + 1]
                );
            }

            Ok(())
        })
        .unwrap();
}

/// P12: Within a PutRecords batch, sequence numbers on the same shard are
/// strictly increasing.
#[test]
fn prop_batch_sequence_numbers_increasing() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 200,
        ..Default::default()
    }));

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    runner
        .run(&(2u32..=50), |batch_size| {
            let stream_name = unique_stream_name("prop-seq");

            let seq_nums: Vec<String> = rt.block_on(async {
                server.create_stream(&stream_name, 1).await;

                let records: Vec<serde_json::Value> = (0..batch_size)
                    .map(|_| {
                        json!({
                            "Data": "dGVzdA==",
                            "PartitionKey": "same-key",
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

                body["Records"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r["SequenceNumber"].as_str().unwrap().to_string())
                    .collect()
            });

            prop_assert_eq!(seq_nums.len(), batch_size as usize);

            let parsed: Vec<BigUint> = seq_nums
                .iter()
                .map(|s| s.parse::<BigUint>().unwrap())
                .collect();

            for i in 0..parsed.len() - 1 {
                prop_assert!(
                    parsed[i] < parsed[i + 1],
                    "batch sequence ordering violated at index {}: {} >= {}",
                    i,
                    parsed[i],
                    parsed[i + 1]
                );
            }

            Ok(())
        })
        .unwrap();
}
