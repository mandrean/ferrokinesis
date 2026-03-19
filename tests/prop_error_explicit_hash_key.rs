// Case count rationale: 50 cases each — server round-trips with hash key validation,
// moderate count balances coverage and speed.
mod common;
use common::*;

use num_bigint::BigUint;
use num_traits::One;
use proptest::prelude::*;
use serde_json::json;

/// Strategy generating ExplicitHashKey values in [2^128, 10^39-1].
///
/// These pass the regex validation `0|([1-9]\d{0,38})` but fail the handler's
/// range check. Covers both the near-2^128 boundary and the near-ceiling region.
fn out_of_range_ehk_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        // Near 2^128 boundary
        (0u64..=10_000_000_000).prop_map(|offset| {
            let base = BigUint::one() << 128;
            let val: BigUint = base + BigUint::from(offset);
            val.to_string()
        }),
        // Near 10^39 - 1 ceiling (max value passing regex)
        (0u64..=10_000_000_000).prop_map(|offset| {
            let ceiling: BigUint = BigUint::from(10u64).pow(39) - BigUint::one();
            (ceiling - BigUint::from(offset)).to_string()
        }),
    ]
}

/// P24: ExplicitHashKey >= 2^128 returns InvalidArgumentException (PutRecord).
///
/// Values in [2^128, 10^39-1] pass the regex validation `0|([1-9]\d{0,38})` but
/// fail the handler's range check.
#[test]
fn prop_explicit_hash_key_out_of_range_put_record() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let stream_name = unique_stream_name("prop-ehk-pr");
    rt.block_on(server.create_stream(&stream_name, 1));

    let mut runner = prop_runner(50);

    runner
        .run(&out_of_range_ehk_strategy(), |ehk| {
            let (status, body) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecord",
                        &json!({
                            "StreamName": stream_name,
                            "Data": "dGVzdA==",
                            "PartitionKey": "pk",
                            "ExplicitHashKey": ehk,
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(status, 400, "expected 400 for ExplicitHashKey {}", ehk);
            prop_assert_eq!(
                body["__type"].as_str().unwrap_or(""),
                "InvalidArgumentException",
                "expected InvalidArgumentException for ExplicitHashKey {}",
                ehk,
            );

            let msg = body["message"].as_str().unwrap_or("");
            prop_assert!(
                msg.contains("ExplicitHashKey"),
                "error message should mention ExplicitHashKey, got: {msg}",
            );

            Ok(())
        })
        .unwrap();
}

/// P25: ExplicitHashKey >= 2^128 returns InvalidArgumentException (PutRecords).
#[test]
fn prop_explicit_hash_key_out_of_range_put_records() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::new());

    let stream_name = unique_stream_name("prop-ehk-prs");
    rt.block_on(server.create_stream(&stream_name, 1));

    let mut runner = prop_runner(50);

    runner
        .run(&out_of_range_ehk_strategy(), |ehk| {
            let (status, body) = rt.block_on(async {
                let res = server
                    .request(
                        "PutRecords",
                        &json!({
                            "StreamName": stream_name,
                            "Records": [
                                {
                                    "Data": "dGVzdA==",
                                    "PartitionKey": "pk",
                                    "ExplicitHashKey": ehk,
                                }
                            ],
                        }),
                    )
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(status, 400, "expected 400 for ExplicitHashKey {}", ehk);
            prop_assert_eq!(
                body["__type"].as_str().unwrap_or(""),
                "InvalidArgumentException",
                "expected InvalidArgumentException for ExplicitHashKey {}",
                ehk,
            );

            let msg = body["message"].as_str().unwrap_or("");
            prop_assert!(
                msg.contains("ExplicitHashKey"),
                "error message should mention ExplicitHashKey, got: {msg}",
            );

            Ok(())
        })
        .unwrap();
}
