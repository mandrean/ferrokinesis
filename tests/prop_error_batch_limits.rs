// Case count rationale: 50 cases each — P28 tests validation (fast), P29 tests
// handler-level aggregate check with large payloads (slower, needs higher body limit).
mod common;
use common::*;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use ferrokinesis::store::StoreOptions;
use proptest::prelude::*;
use serde_json::json;

/// P28: PutRecords with > 500 records returns ValidationException.
///
/// The validation layer rejects Records arrays with more than 500 entries
/// (len_lte(500)) before the request reaches dispatch.
#[test]
fn prop_put_records_exceeds_500_records() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let mut runner = prop_runner(50);

    runner
        .run(&(501u32..=550), |record_count| {
            let records: Vec<serde_json::Value> = (0..record_count)
                .map(|_| {
                    json!({
                        "Data": "dGVzdA==",
                        "PartitionKey": "k",
                    })
                })
                .collect();

            let (status, body) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecords",
                        &json!({
                            // Stream doesn't need to exist — validation rejects before dispatch.
                            "StreamName": "any-stream",
                            "Records": records,
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(status, 400, "expected 400 for {} records", record_count);
            let err_type = body["__type"].as_str().unwrap_or("");
            prop_assert!(
                err_type.contains("ValidationException"),
                "expected ValidationException for {} records, got {:?}",
                record_count,
                err_type,
            );

            Ok(())
        })
        .unwrap();
}

/// P29: PutRecords with > 5 MB total payload returns InvalidArgumentException.
///
/// Each record stays under 1 MB individually, but the aggregate decoded data +
/// partition key payload exceeds 5,242,880 bytes.
#[test]
fn prop_put_records_exceeds_5mb_total() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    // Need a higher body limit since the JSON body with large base64 payloads exceeds
    // axum's default 2 MiB limit.
    let server = rt.block_on(TestServer::with_body_limit(
        StoreOptions {
            create_stream_ms: 0,
            delete_stream_ms: 0,
            update_stream_ms: 0,
            shard_limit: 50,
            ..Default::default()
        },
        10 * 1024 * 1024,
    ));

    let stream_name = unique_stream_name("prop-batch-5mb");
    rt.block_on(server.create_stream(&stream_name, 4));

    let mut runner = prop_runner(50);

    // Generate 6-10 records, each sized so total exceeds 5 MB but each stays under 1 MB.
    runner
        .run(&(6u32..=10), |n| {
            let data_size = 5_242_881usize / n as usize + 1;
            let raw_bytes = vec![0x41u8; data_size];
            let b64_data = BASE64.encode(&raw_bytes);

            let records: Vec<serde_json::Value> = (0..n)
                .map(|_| {
                    json!({
                        "Data": b64_data,
                        "PartitionKey": "k",
                    })
                })
                .collect();

            let (status, body) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecords",
                        &json!({
                            "StreamName": stream_name,
                            "Records": records,
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(
                status,
                400,
                "expected 400 for {} records of {} decoded bytes each",
                n,
                data_size,
            );
            prop_assert_eq!(
                body["__type"].as_str().unwrap_or(""),
                "InvalidArgumentException",
                "expected InvalidArgumentException for {} records totaling > 5 MB",
                n,
            );

            Ok(())
        })
        .unwrap();
}
