mod common;

use ferrokinesis::sequence::partition_key_to_hash_key;
use num_bigint::BigUint;
use num_traits::One;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};

/// Reproduce the shard hash formula from create_stream.rs:47-58
fn compute_shard_ranges(shard_count: u32) -> Vec<(BigUint, BigUint)> {
    let pow_128 = BigUint::one() << 128;
    let shard_hash = &pow_128 / BigUint::from(shard_count);
    let mut ranges = Vec::with_capacity(shard_count as usize);
    for i in 0..shard_count {
        let start = &shard_hash * BigUint::from(i);
        let end = if i < shard_count - 1 {
            &shard_hash * BigUint::from(i + 1) - BigUint::one()
        } else {
            &pow_128 - BigUint::one()
        };
        ranges.push((start, end));
    }
    ranges
}

/// P1: For any shard count in [1, 200], hash ranges partition [0, 2^128-1]
/// with no gaps, no overlaps, and every shard is non-degenerate.
/// This is exhaustive (not sampling-based), so a plain loop is used.
#[test]
fn prop_hash_space_fully_partitioned() {
    let pow_128 = BigUint::one() << 128;
    let max_hash = &pow_128 - BigUint::one();

    for shard_count in 1..=200u32 {
        let ranges = compute_shard_ranges(shard_count);

        // First shard starts at 0
        assert_eq!(&ranges[0].0, &BigUint::ZERO);

        // Last shard ends at 2^128 - 1
        assert_eq!(&ranges[ranges.len() - 1].1, &max_hash);

        // Every shard is non-degenerate (start <= end)
        for (i, (start, end)) in ranges.iter().enumerate() {
            assert!(
                start <= end,
                "shard_count={shard_count}: shard {i} is degenerate: start={start} > end={end}",
            );
        }

        // Adjacent shards are contiguous: end[i] + 1 == start[i+1]
        for i in 0..ranges.len() - 1 {
            let expected_next_start = &ranges[i].1 + BigUint::one();
            assert_eq!(
                &ranges[i + 1].0,
                &expected_next_start,
                "shard_count={}: gap or overlap between shard {} and {}: end[{}]={}, start[{}]={}",
                shard_count,
                i,
                i + 1,
                i,
                ranges[i].1,
                i + 1,
                ranges[i + 1].0,
            );
        }
    }
}

/// P2: MD5 hash of any partition key is within [0, 2^128).
#[test]
fn prop_md5_hash_in_range() {
    let pow_128 = BigUint::one() << 128;

    let mut runner = TestRunner::new(Config {
        cases: 256,
        ..Config::default()
    });

    runner
        .run(&"[\\PC]{1,256}", |pk| {
            let hash = partition_key_to_hash_key(&pk);
            prop_assert!(
                hash < pow_128,
                "hash {} for partition key {:?} is >= 2^128",
                hash,
                pk
            );
            Ok(())
        })
        .unwrap();
}

/// P3: partition_key_to_hash_key is deterministic.
#[test]
fn prop_hash_deterministic() {
    let mut runner = TestRunner::new(Config {
        cases: 256,
        ..Config::default()
    });

    runner
        .run(&"[\\PC]{1,256}", |pk| {
            let h1 = partition_key_to_hash_key(&pk);
            let h2 = partition_key_to_hash_key(&pk);
            prop_assert_eq!(h1, h2, "non-deterministic hash for key {:?}", pk);
            Ok(())
        })
        .unwrap();
}
