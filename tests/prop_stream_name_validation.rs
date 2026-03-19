mod common;
use common::*;

use ferrokinesis::store::StoreOptions;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

/// P4: Valid stream names (matching [a-zA-Z0-9_.-]{1,128}) are accepted.
#[test]
fn prop_valid_stream_names_accepted() {
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
            // Truncate to 128 chars max
            let stream_name = if stream_name.len() > 128 {
                stream_name.chars().take(128).collect::<String>()
            } else {
                stream_name
            };

            let status = rt.block_on(async {
                let res = server
                    .request(
                        "CreateStream",
                        &json!({"StreamName": stream_name, "ShardCount": 1}),
                    )
                    .await;
                res.status().as_u16()
            });

            prop_assert_eq!(
                status,
                200,
                "valid stream name {:?} was rejected",
                stream_name
            );
            Ok(())
        })
        .unwrap();
}

/// P5: Invalid stream names are rejected with 400.
#[test]
fn prop_invalid_stream_names_rejected() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let mut runner = TestRunner::new(Config {
        cases: 100,
        ..Config::default()
    });

    let invalid_names = prop_oneof![
        // Empty string
        Just(String::new()),
        // Too long (129-200 chars of valid characters)
        "[a-zA-Z0-9_.\\-]{129,200}",
        // Contains characters outside [a-zA-Z0-9_.-]
        "[a-zA-Z0-9_.\\-]{0,10}[!@#$%^&*()+=\\[\\]{}|;:',<>?/~`][a-zA-Z0-9_.\\-]{0,10}",
        // Whitespace characters
        "[a-zA-Z0-9_.\\-]{1,10}[\\t\\n\\r ][a-zA-Z0-9_.\\-]{1,10}",
        // Control characters and null bytes
        Just("test\x00stream".to_string()),
        Just("test\tstream".to_string()),
    ];

    runner
        .run(&invalid_names, |name| {
            let status = rt.block_on(async {
                let res = server
                    .request(
                        "CreateStream",
                        &json!({"StreamName": name, "ShardCount": 1}),
                    )
                    .await;
                res.status().as_u16()
            });

            prop_assert_eq!(status, 400, "invalid stream name {:?} was accepted", name);
            Ok(())
        })
        .unwrap();
}
