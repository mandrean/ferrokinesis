// Case count rationale: 256 cases — pure function test with no I/O, so high count
// is cheap and provides strong coverage of the sequence number encoding space.

use ferrokinesis::sequence::{SeqObj, parse_sequence, stringify_sequence};
use num_bigint::BigUint;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

/// P19: stringify_sequence(parse_sequence(s)) = identity for version-2 sequences.
///
/// Generates SeqObj components within valid ranges, stringifies to a sequence number,
/// parses it back, and re-stringifies. The two strings must be identical.
#[test]
fn prop_sequence_roundtrip_identity() {
    let mut runner = TestRunner::new(Config {
        cases: 256,
        ..Config::default()
    });

    // Strategy: generate SeqObj fields within valid ranges for version 2.
    // shard_create_time and seq_time must be multiples of 1000 (stored as seconds).
    let strategy = (
        // shard_create_time in ms (must be multiple of 1000)
        (1_000u64..=1_800_000_000).prop_map(|secs| secs * 1000),
        // shard_ix: 0..=99
        0i64..=99,
        // seq_ix: 0..=100_000
        0u64..=100_000,
        // seq_time offset from shard_create_time (0..=1_000_000 seconds, as ms multiple of 1000)
        (0u64..=1_000_000).prop_map(|secs| secs * 1000),
    );

    runner
        .run(&strategy, |(shard_create_time, shard_ix, seq_ix, seq_time_offset)| {
            let seq_time = shard_create_time + seq_time_offset;

            let obj = SeqObj {
                shard_create_time,
                seq_ix: Some(BigUint::from(seq_ix)),
                byte1: None,
                seq_time: Some(seq_time),
                seq_rand: None,
                shard_ix,
                version: 2,
            };

            let s1 = stringify_sequence(&obj);
            let parsed = parse_sequence(&s1).map_err(|e| {
                TestCaseError::Fail(format!("parse_sequence failed: {e}").into())
            })?;
            let s2 = stringify_sequence(&parsed);

            prop_assert_eq!(
                &s1,
                &s2,
                "roundtrip failed for shard_create_time={}, shard_ix={}, seq_ix={}, seq_time={}",
                shard_create_time,
                shard_ix,
                seq_ix,
                seq_time,
            );

            Ok(())
        })
        .unwrap();
}
