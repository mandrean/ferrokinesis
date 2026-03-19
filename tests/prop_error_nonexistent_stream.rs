// Case count rationale: 50 cases each — server round-trips with no stream setup,
// moderate count balances coverage and speed.
mod common;
use common::*;

use proptest::prelude::*;
use serde_json::json;

/// P20: PutRecord to a non-existent stream returns ResourceNotFoundException.
#[test]
fn prop_put_record_nonexistent_stream() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let mut runner = prop_runner(50);

    runner
        .run(&"[a-zA-Z0-9_.\\-]{1,50}", |name_prefix| {
            let stream_name = unique_stream_name(&name_prefix);

            let (status, body) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": stream_name,
                            "Data": "dGVzdA==",
                            "PartitionKey": "pk",
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(
                status,
                400,
                "expected 400 for non-existent stream {:?}",
                stream_name
            );
            prop_assert_eq!(
                body["__type"].as_str().unwrap_or(""),
                "ResourceNotFoundException",
                "expected ResourceNotFoundException for stream {:?}",
                stream_name,
            );
            Ok(())
        })
        .unwrap();
}

/// P21: PutRecords to a non-existent stream returns ResourceNotFoundException.
#[test]
fn prop_put_records_nonexistent_stream() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let mut runner = prop_runner(50);

    runner
        .run(&"[a-zA-Z0-9_.\\-]{1,50}", |name_prefix| {
            let stream_name = unique_stream_name(&name_prefix);

            let (status, body) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecords",
                        &json!({
                            "StreamName": stream_name,
                            "Records": [
                                {"Data": "dGVzdA==", "PartitionKey": "pk"}
                            ],
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(
                status,
                400,
                "expected 400 for non-existent stream {:?}",
                stream_name
            );
            prop_assert_eq!(
                body["__type"].as_str().unwrap_or(""),
                "ResourceNotFoundException",
                "expected ResourceNotFoundException for stream {:?}",
                stream_name,
            );
            Ok(())
        })
        .unwrap();
}

/// P22: GetShardIterator on a non-existent stream returns ResourceNotFoundException.
#[test]
fn prop_get_shard_iterator_nonexistent_stream() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let mut runner = prop_runner(50);

    runner
        .run(&"[a-zA-Z0-9_.\\-]{1,50}", |name_prefix| {
            let stream_name = unique_stream_name(&name_prefix);

            let (status, body) = rt.block_on(async {
                let res = server
                    .request(
                        "GetShardIterator",
                        &json!({
                            "StreamName": stream_name,
                            "ShardId": "shardId-000000000000",
                            "ShardIteratorType": "TRIM_HORIZON",
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(
                status,
                400,
                "expected 400 for non-existent stream {:?}",
                stream_name
            );
            prop_assert_eq!(
                body["__type"].as_str().unwrap_or(""),
                "ResourceNotFoundException",
                "expected ResourceNotFoundException for stream {:?}",
                stream_name,
            );
            Ok(())
        })
        .unwrap();
}
