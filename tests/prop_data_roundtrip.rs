mod common;
use common::*;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

/// P9: PutRecord → GetRecords round-trip preserves data integrity.
#[test]
fn prop_data_roundtrip_integrity() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    // Single shard with fixed partition key so we can retrieve records via a known
    // shard iterator. Cross-shard routing is tested in prop_shard_routing.rs.
    let stream_name = unique_stream_name("prop-data-rt");
    rt.block_on(server.create_stream(&stream_name, 1));

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    // Generate arbitrary byte sequences 1..=4096, then base64-encode
    let strategy = proptest::collection::vec(any::<u8>(), 1..=4096);

    runner
        .run(&strategy, |raw_bytes| {
            let encoded = STANDARD.encode(&raw_bytes);

            let records = rt.block_on(async {
                // Put the record
                let put_res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": stream_name,
                            "Data": encoded,
                            "PartitionKey": "roundtrip-key",
                        }),
                    )
                    .await;
                assert_eq!(put_res.status(), 200);
                let put_body: serde_json::Value = put_res.json().await.unwrap();
                let seq_num = put_body["SequenceNumber"].as_str().unwrap().to_string();

                // Get an iterator at the exact sequence number
                let iter_res = server
                    .request(
                        "GetShardIterator",
                        &json!({
                            "StreamName": stream_name,
                            "ShardId": "shardId-000000000000",
                            "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                            "StartingSequenceNumber": seq_num,
                        }),
                    )
                    .await;
                assert_eq!(iter_res.status(), 200);
                let iter_body: serde_json::Value = iter_res.json().await.unwrap();
                let iterator = iter_body["ShardIterator"].as_str().unwrap();

                let rec_body = server.get_records(iterator).await;
                rec_body["Records"].as_array().unwrap().clone()
            });

            prop_assert!(
                !records.is_empty(),
                "no records returned for data of length {}",
                raw_bytes.len()
            );

            // The first record should match our data
            let returned_data = records[0]["Data"].as_str().unwrap();
            let returned_bytes = STANDARD.decode(returned_data).unwrap();
            prop_assert_eq!(
                &returned_bytes,
                &raw_bytes,
                "data mismatch for input of length {}",
                raw_bytes.len()
            );

            Ok(())
        })
        .unwrap();
}

/// P10: PartitionKey round-trips correctly through PutRecord → GetRecords.
#[test]
fn prop_partition_key_roundtrip() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let stream_name = unique_stream_name("prop-pk-rt");
    rt.block_on(server.create_stream(&stream_name, 1));

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    runner
        .run(&"[a-zA-Z0-9_.\\-]{1,256}", |partition_key| {
            let returned_pk = rt.block_on(async {
                let put_res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": stream_name,
                            "Data": "dGVzdA==",
                            "PartitionKey": partition_key,
                        }),
                    )
                    .await;
                assert_eq!(put_res.status(), 200);
                let put_body: serde_json::Value = put_res.json().await.unwrap();
                let seq_num = put_body["SequenceNumber"].as_str().unwrap().to_string();

                let iter_res = server
                    .request(
                        "GetShardIterator",
                        &json!({
                            "StreamName": stream_name,
                            "ShardId": "shardId-000000000000",
                            "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                            "StartingSequenceNumber": seq_num,
                        }),
                    )
                    .await;
                assert_eq!(iter_res.status(), 200);
                let iter_body: serde_json::Value = iter_res.json().await.unwrap();
                let iterator = iter_body["ShardIterator"].as_str().unwrap();

                let rec_body = server.get_records(iterator).await;
                let records = rec_body["Records"].as_array().unwrap();
                records[0]["PartitionKey"].as_str().unwrap().to_string()
            });

            prop_assert_eq!(
                &returned_pk,
                &partition_key,
                "partition key mismatch on round-trip"
            );
            Ok(())
        })
        .unwrap();
}
