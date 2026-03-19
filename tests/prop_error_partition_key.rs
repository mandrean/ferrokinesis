// Case count rationale: 50 cases — validation rejects before dispatch so these are
// fast, but server round-trips still dominate.
mod common;
use common::*;

use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

/// P26: Empty or >256 char partition key returns ValidationException.
///
/// The validation layer rejects PartitionKey values that are empty (len_gte(1))
/// or longer than 256 characters (len_lte(256)) before the request reaches dispatch.
#[test]
fn prop_invalid_partition_key_rejected() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    let invalid_keys = prop_oneof![
        // Empty string (violates len_gte(1))
        3 => Just(String::new()),
        // Too long (257-512 chars, violates len_lte(256))
        10 => "[a-zA-Z0-9]{257,512}".prop_map(String::from),
    ];

    runner
        .run(&invalid_keys, |pk| {
            // Test PutRecord
            let (status_pr, body_pr) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": "any-stream",
                            "Data": "dGVzdA==",
                            "PartitionKey": pk,
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(
                status_pr,
                400,
                "PutRecord: expected 400 for partition key len={}",
                pk.len()
            );
            let err_type_pr = body_pr["__type"].as_str().unwrap_or("");
            prop_assert!(
                err_type_pr.contains("ValidationException")
                    || err_type_pr.contains("SerializationException"),
                "PutRecord: expected ValidationException or SerializationException, got {:?}",
                err_type_pr,
            );

            // Test PutRecords
            let (status_prs, body_prs) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecords",
                        &json!({
                            "StreamName": "any-stream",
                            "Records": [
                                {"Data": "dGVzdA==", "PartitionKey": pk}
                            ],
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(
                status_prs,
                400,
                "PutRecords: expected 400 for partition key len={}",
                pk.len()
            );
            let err_type_prs = body_prs["__type"].as_str().unwrap_or("");
            prop_assert!(
                err_type_prs.contains("ValidationException")
                    || err_type_prs.contains("SerializationException"),
                "PutRecords: expected ValidationException or SerializationException, got {:?}",
                err_type_prs,
            );

            Ok(())
        })
        .unwrap();
}
