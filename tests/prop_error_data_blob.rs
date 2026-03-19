// Case count rationale: 50 cases — each case generates a >1 MB base64 payload,
// so moderate count keeps total test time reasonable.
mod common;
use common::*;

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

/// P27: Data blob > 1 MB returns ValidationException.
///
/// The validation layer rejects Data blobs whose decoded length exceeds 1,048,576 bytes
/// (len_lte(1048576)) before the request reaches dispatch.
#[test]
fn prop_oversized_data_blob_rejected() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    // Generate decoded byte counts just over 1 MB
    runner
        .run(&(1_048_577usize..=1_100_000), |raw_len| {
            let raw_bytes = vec![0x41u8; raw_len];
            let b64_data = BASE64.encode(&raw_bytes);

            let (status, body) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": "any-stream",
                            "Data": b64_data,
                            "PartitionKey": "pk",
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(
                status,
                400,
                "expected 400 for data blob of {} decoded bytes",
                raw_len
            );
            let err_type = body["__type"].as_str().unwrap_or("");
            prop_assert!(
                err_type.contains("ValidationException"),
                "expected ValidationException, got {:?} for {} decoded bytes",
                err_type,
                raw_len,
            );

            Ok(())
        })
        .unwrap();
}
