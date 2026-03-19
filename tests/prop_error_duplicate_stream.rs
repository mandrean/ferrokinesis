// Case count rationale: 100 cases — CreateStream calls are cheap with zero delay,
// so high count provides strong coverage of name generation.
mod common;
use common::*;

use ferrokinesis::store::StoreOptions;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

/// P30: Duplicate CreateStream returns ResourceInUseException.
///
/// Creating a stream with the same name as an existing stream should be rejected.
#[test]
fn prop_duplicate_create_stream() {
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

    runner
        .run(&"[a-zA-Z0-9_.\\-]{1,50}", |name_prefix| {
            let stream_name = unique_stream_name(&name_prefix);

            let (first_status, second_status, body) = rt.block_on(async {
                // First create should succeed
                let res1 = server
                    .request(
                        "CreateStream",
                        &json!({"StreamName": stream_name, "ShardCount": 1}),
                    )
                    .await;
                let status1 = res1.status().as_u16();

                // Second create with same name should fail
                let res2 = server
                    .request(
                        "CreateStream",
                        &json!({"StreamName": stream_name, "ShardCount": 1}),
                    )
                    .await;
                let (status2, body2) = decode_body(res2).await;
                (status1, status2, body2)
            });

            prop_assert_eq!(
                first_status,
                200,
                "first CreateStream for {:?} should succeed",
                stream_name,
            );
            prop_assert_eq!(
                second_status,
                400,
                "duplicate CreateStream for {:?} should return 400",
                stream_name,
            );
            prop_assert_eq!(
                body["__type"].as_str().unwrap_or(""),
                "ResourceInUseException",
                "expected ResourceInUseException for duplicate stream {:?}",
                stream_name,
            );

            Ok(())
        })
        .unwrap();
}
