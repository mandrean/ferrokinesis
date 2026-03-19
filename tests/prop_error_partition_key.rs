// Case count rationale: 50 cases — validation rejects before dispatch so these are
// fast, but server round-trips still dominate.
mod common;
use common::*;

use proptest::prelude::*;
use serde_json::json;

/// P26: Invalid partition key (empty or >256 chars) returns ValidationException.
///
/// The validation layer rejects PartitionKey values that are empty (len_gte(1))
/// or longer than 256 characters (len_lte(256)) before the request reaches dispatch.
#[test]
fn prop_invalid_partition_key_rejected() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let mut runner = prop_runner(50);

    let invalid_keys = prop_oneof![
        // Empty key
        Just(String::new()),
        // Oversized key (257..512 chars)
        "[a-zA-Z0-9]{257,512}".prop_map(String::from),
    ];

    runner
        .run(&invalid_keys, |pk| {
            // Test PutRecord
            let (status_pr, body_pr) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecord",
                        &json!({
                            // Stream doesn't need to exist — validation rejects before dispatch.
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
            prop_assert_eq!(
                body_pr["__type"].as_str().unwrap_or(""),
                "ValidationException",
                "PutRecord: expected ValidationException, got {:?}",
                body_pr["__type"],
            );

            // Test PutRecords
            let (status_prs, body_prs) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecords",
                        &json!({
                            // Stream doesn't need to exist — validation rejects before dispatch.
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
            prop_assert_eq!(
                body_prs["__type"].as_str().unwrap_or(""),
                "ValidationException",
                "PutRecords: expected ValidationException, got {:?}",
                body_prs["__type"],
            );

            Ok(())
        })
        .unwrap();
}
