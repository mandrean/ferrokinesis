// Case count rationale: 50 cases — this file exercises multi-request JSON/CBOR
// equivalence and mixed-format round-trips, so we keep coverage high while
// bounding total server round-trips.
mod common;
use common::*;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ferrokinesis::store::StoreOptions;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

/// Volatile keys that differ between separate JSON and CBOR requests
/// (because each is a distinct write producing a different sequence number).
const VOLATILE_KEYS: &[&str] = &["SequenceNumber"];

fn cbor_field<'a>(val: &'a ciborium::Value, key: &str) -> Option<&'a ciborium::Value> {
    let ciborium::Value::Map(entries) = val else {
        return None;
    };

    entries.iter().find_map(|(k, v)| match k {
        ciborium::Value::Text(text) if text == key => Some(v),
        _ => None,
    })
}

/// P16: PutRecord via JSON and CBOR produce structurally equivalent responses.
#[test]
fn prop_put_record_cbor_json_equivalent() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let stream_name = unique_stream_name("prop-cbor");
    rt.block_on(server.create_stream(&stream_name, 4));

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    let strategy = "[a-zA-Z0-9_.\\-]{1,64}";

    runner
        .run(&strategy, |partition_key| {
            let (json_body, cbor_body) = rt.block_on(async {
                let req = json!({
                    "StreamName": stream_name,
                    "Data": "dGVzdA==",
                    "PartitionKey": partition_key,
                });
                let ((json_status, json_body), (cbor_status, cbor_body)) =
                    server.request_both("PutRecord", &req).await;
                assert_eq!(json_status, 200);
                assert_eq!(cbor_status, 200);
                (json_body, cbor_body)
            });

            // ShardId must be identical (same routing for same partition key)
            prop_assert_eq!(
                json_body["ShardId"].as_str(),
                cbor_body["ShardId"].as_str(),
                "ShardId mismatch for partition key {:?}",
                partition_key
            );

            // Structural equivalence ignoring sequence numbers
            assert_values_equivalent(&json_body, &cbor_body, VOLATILE_KEYS);

            Ok(())
        })
        .unwrap();
}

/// P17: DescribeStream is fully equivalent via JSON and CBOR.
#[test]
fn prop_describe_stream_cbor_json_equivalent() {
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
        .run(&"[a-zA-Z0-9_.\\-]{1,50}", |name_prefix| {
            let stream_name = format!("{}-{}", name_prefix, unique_stream_name("prop-cbor"));
            let stream_name = if stream_name.len() > 128 {
                stream_name.chars().take(128).collect::<String>()
            } else {
                stream_name
            };

            let (json_body, cbor_body) = rt.block_on(async {
                server.create_stream(&stream_name, 1).await;
                let ((json_status, json_body), (cbor_status, cbor_body)) = server
                    .request_both("DescribeStream", &json!({"StreamName": stream_name}))
                    .await;
                assert_eq!(json_status, 200);
                assert_eq!(cbor_status, 200);
                (json_body, cbor_body)
            });

            assert_values_equivalent(&json_body, &cbor_body, &[]);
            Ok(())
        })
        .unwrap();
}

/// P18: PutRecords batch responses are equivalent via JSON and CBOR.
#[test]
fn prop_put_records_cbor_json_equivalent() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let stream_name = unique_stream_name("prop-cbor");
    rt.block_on(server.create_stream(&stream_name, 4));

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    let strategy = (1u32..=10, "[a-zA-Z0-9_.\\-]{1,64}");

    runner
        .run(&strategy, |(batch_size, partition_key)| {
            let (json_body, cbor_body) = rt.block_on(async {
                let records: Vec<serde_json::Value> = (0..batch_size)
                    .map(|i| {
                        json!({
                            "Data": "dGVzdA==",
                            "PartitionKey": format!("{}-{}", partition_key, i),
                        })
                    })
                    .collect();

                let req = json!({
                    "StreamName": stream_name,
                    "Records": records,
                });

                let ((json_status, json_body), (cbor_status, cbor_body)) =
                    server.request_both("PutRecords", &req).await;
                assert_eq!(json_status, 200);
                assert_eq!(cbor_status, 200);
                (json_body, cbor_body)
            });

            // FailedRecordCount should be identical
            prop_assert_eq!(
                &json_body["FailedRecordCount"],
                &cbor_body["FailedRecordCount"],
                "FailedRecordCount mismatch"
            );

            // Record count should match
            let json_records = json_body["Records"].as_array().unwrap();
            let cbor_records = cbor_body["Records"].as_array().unwrap();
            prop_assert_eq!(
                json_records.len(),
                cbor_records.len(),
                "response record count mismatch"
            );

            // Each record's ShardId should match (same routing)
            for (i, (jr, cr)) in json_records.iter().zip(cbor_records.iter()).enumerate() {
                prop_assert_eq!(
                    jr["ShardId"].as_str(),
                    cr["ShardId"].as_str(),
                    "ShardId mismatch at record {}",
                    i
                );
            }

            Ok(())
        })
        .unwrap();
}

/// P31: PutRecord via JSON and GetRecords via CBOR preserve raw Data bytes.
#[test]
fn prop_put_record_json_get_records_cbor_roundtrip() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let stream_name = unique_stream_name("prop-cross-jc");
    rt.block_on(server.create_stream(&stream_name, 1));

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    let strategy = proptest::collection::vec(any::<u8>(), 1..=4096);

    runner
        .run(&strategy, |raw_bytes| {
            let encoded = STANDARD.encode(&raw_bytes);

            let (seq_num, cbor_body) = rt.block_on(async {
                let put_res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": stream_name,
                            "Data": encoded,
                            "PartitionKey": "cross-format-json",
                        }),
                    )
                    .await;
                assert_eq!(put_res.status(), 200);
                let put_body: serde_json::Value = put_res.json().await.unwrap();
                let seq_num = put_body["SequenceNumber"].as_str().unwrap().to_string();
                let shard_id = put_body["ShardId"].as_str().unwrap().to_string();

                let iter_res = decode_body(
                    server
                        .cbor_request(
                            "GetShardIterator",
                            &json!({
                                "StreamName": stream_name,
                                "ShardId": shard_id,
                                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                                "StartingSequenceNumber": seq_num,
                            }),
                        )
                        .await,
                )
                .await;
                assert_eq!(iter_res.0, 200);
                let iterator = iter_res.1["ShardIterator"].as_str().unwrap().to_string();

                let get_res = server
                    .cbor_request("GetRecords", &json!({"ShardIterator": iterator}))
                    .await;
                assert_eq!(get_res.status(), 200);
                let content_type = get_res
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                assert!(
                    content_type.contains("cbor"),
                    "expected CBOR response, got {content_type}"
                );

                let body_bytes = get_res.bytes().await.unwrap();
                let cbor_body: ciborium::Value = ciborium::from_reader(&body_bytes[..]).unwrap();
                (seq_num, cbor_body)
            });

            let records = match cbor_field(&cbor_body, "Records") {
                Some(ciborium::Value::Array(records)) => records,
                other => panic!("expected Records array in CBOR response, got {other:?}"),
            };
            prop_assert!(
                !records.is_empty(),
                "no records returned for data of length {}",
                raw_bytes.len()
            );

            let data_bytes = match cbor_field(&records[0], "Data") {
                Some(ciborium::Value::Bytes(data)) => data,
                other => panic!("expected CBOR byte string for Data, got {other:?}"),
            };
            prop_assert_eq!(
                data_bytes.as_slice(),
                raw_bytes.as_slice(),
                "raw CBOR bytes mismatch for input of length {}",
                raw_bytes.len()
            );

            let normalized = ferrokinesis::server::cbor_to_json(&cbor_body);
            let normalized_records = normalized["Records"].as_array().unwrap();
            prop_assert_eq!(
                normalized_records[0]["SequenceNumber"].as_str(),
                Some(seq_num.as_str()),
                "GetRecords did not return the record written in this case"
            );

            Ok(())
        })
        .unwrap();
}

/// P32: PutRecord via CBOR and GetRecords via JSON preserve raw Data bytes.
#[test]
fn prop_put_record_cbor_get_records_json_roundtrip() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let stream_name = unique_stream_name("prop-cross-cj");
    rt.block_on(server.create_stream(&stream_name, 1));

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    let strategy = proptest::collection::vec(any::<u8>(), 1..=4096);

    runner
        .run(&strategy, |raw_bytes| {
            let encoded = STANDARD.encode(&raw_bytes);

            let (seq_num, json_body) = rt.block_on(async {
                let put_resp = decode_body(
                    server
                        .cbor_request_raw_data(
                            "PutRecord",
                            &json!({
                                "StreamName": stream_name,
                                "Data": encoded,
                                "PartitionKey": "cross-format-cbor",
                            }),
                            "Data",
                            &raw_bytes,
                        )
                        .await,
                )
                .await;
                assert_eq!(put_resp.0, 200);
                let seq_num = put_resp.1["SequenceNumber"].as_str().unwrap().to_string();
                let shard_id = put_resp.1["ShardId"].as_str().unwrap().to_string();

                let iter_res = server
                    .request(
                        "GetShardIterator",
                        &json!({
                            "StreamName": stream_name,
                            "ShardId": shard_id,
                            "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                            "StartingSequenceNumber": seq_num,
                        }),
                    )
                    .await;
                assert_eq!(iter_res.status(), 200);
                let iter_body: serde_json::Value = iter_res.json().await.unwrap();
                let iterator = iter_body["ShardIterator"].as_str().unwrap().to_string();

                let get_res = server
                    .request("GetRecords", &json!({"ShardIterator": iterator}))
                    .await;
                assert_eq!(get_res.status(), 200);
                let json_body: serde_json::Value = get_res.json().await.unwrap();
                (seq_num, json_body)
            });

            let records = json_body["Records"].as_array().unwrap();
            prop_assert!(
                !records.is_empty(),
                "no records returned for data of length {}",
                raw_bytes.len()
            );

            let data_b64 = records[0]["Data"]
                .as_str()
                .expect("expected JSON Data to be a base64 string");
            let returned_bytes = STANDARD.decode(data_b64).unwrap();
            prop_assert_eq!(
                returned_bytes.as_slice(),
                raw_bytes.as_slice(),
                "JSON/base64 round-trip mismatch for input of length {}",
                raw_bytes.len()
            );
            prop_assert_eq!(
                records[0]["SequenceNumber"].as_str(),
                Some(seq_num.as_str()),
                "GetRecords did not return the record written in this case"
            );

            Ok(())
        })
        .unwrap();
}
