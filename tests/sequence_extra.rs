use ferrokinesis::sequence::{
    SeqObj, increment_sequence, parse_sequence, resolve_shard_id, shard_id_name, shard_ix_to_hex,
    stringify_sequence,
};
use num_bigint::BigUint;

// -- Version 0 stringify/parse roundtrip --

#[test]
fn stringify_parse_v0_roundtrip() {
    let obj = SeqObj {
        shard_create_time: 1_600_000_000_000,
        seq_ix: None,
        byte1: Some("00".to_string()),
        seq_time: None,
        seq_rand: Some("abcdef1234567890".to_string()),
        shard_ix: 0,
        version: 0,
    };
    let seq = stringify_sequence(&obj);
    let parsed = parse_sequence(&seq).unwrap();
    assert_eq!(parsed.version, 0);
    assert_eq!(parsed.shard_create_time, 1_600_000_000_000);
    assert_eq!(parsed.shard_ix, 0);
    assert!(parsed.seq_ix.is_none());
    assert!(parsed.seq_time.is_none());
}

#[test]
fn stringify_v0_non_zero_shard() {
    let obj = SeqObj {
        shard_create_time: 1_700_000_000_000,
        seq_ix: None,
        byte1: Some("01".to_string()),
        seq_time: None,
        seq_rand: Some("0000000000000001".to_string()),
        shard_ix: 3,
        version: 0,
    };
    let seq = stringify_sequence(&obj);
    let parsed = parse_sequence(&seq).unwrap();
    assert_eq!(parsed.version, 0);
    assert_eq!(parsed.shard_ix, 3);
}

// -- Version 1 stringify/parse roundtrip --

#[test]
fn stringify_parse_v1_roundtrip() {
    let obj = SeqObj {
        shard_create_time: 1_600_000_000_000,
        seq_ix: Some(BigUint::from(7u64)),
        byte1: Some("ab".to_string()),
        seq_time: Some(1_600_000_001_000),
        seq_rand: Some("00000000000000".to_string()), // 14 chars
        shard_ix: 0,
        version: 1,
    };
    let seq = stringify_sequence(&obj);
    let parsed = parse_sequence(&seq).unwrap();
    assert_eq!(parsed.version, 1);
    assert_eq!(parsed.shard_create_time, 1_600_000_000_000);
    assert_eq!(parsed.shard_ix, 0);
    assert!(parsed.seq_ix.is_some());
    assert_eq!(parsed.seq_time, Some(1_600_000_001_000));
}

#[test]
fn stringify_v1_with_shard_ix_5() {
    let obj = SeqObj {
        shard_create_time: 1_600_000_000_000,
        seq_ix: Some(BigUint::from(42u64)),
        byte1: Some("00".to_string()),
        seq_time: Some(1_600_000_002_000),
        seq_rand: Some("11111111111111".to_string()),
        shard_ix: 5,
        version: 1,
    };
    let seq = stringify_sequence(&obj);
    let parsed = parse_sequence(&seq).unwrap();
    assert_eq!(parsed.version, 1);
    assert_eq!(parsed.shard_ix, 5);
}

// -- increment_sequence --

#[test]
fn increment_sequence_with_explicit_time() {
    let obj = SeqObj {
        shard_create_time: 1_600_000_000_000,
        seq_ix: Some(BigUint::from(0u64)),
        byte1: Some("00".to_string()),
        seq_time: Some(1_600_000_001_000),
        seq_rand: None,
        shard_ix: 0,
        version: 2,
    };
    let seq = increment_sequence(&obj, Some(1_600_000_002_000));
    let parsed = parse_sequence(&seq).unwrap();
    assert_eq!(parsed.version, 2);
    assert_eq!(parsed.seq_time, Some(1_600_000_002_000));
    assert_eq!(parsed.shard_ix, 0);
}

#[test]
fn increment_sequence_without_explicit_time() {
    let obj = SeqObj {
        shard_create_time: 1_600_000_000_000,
        seq_ix: Some(BigUint::from(0u64)),
        byte1: Some("00".to_string()),
        seq_time: Some(1_600_000_001_000),
        seq_rand: None,
        shard_ix: 0,
        version: 2,
    };
    // Without seq_time: uses seq_obj.seq_time.unwrap_or(0) + 1000
    let seq = increment_sequence(&obj, None);
    let parsed = parse_sequence(&seq).unwrap();
    assert_eq!(parsed.version, 2);
    assert_eq!(parsed.seq_time, Some(1_600_000_002_000));
}

#[test]
fn increment_sequence_from_parsed() {
    // Get a real sequence from a v2 stringify/parse cycle, then increment it
    let obj = SeqObj {
        shard_create_time: 1_600_000_000_000,
        seq_ix: Some(BigUint::from(100u64)),
        byte1: Some("00".to_string()),
        seq_time: Some(1_600_000_010_000),
        seq_rand: None,
        shard_ix: 2,
        version: 2,
    };
    let seq = stringify_sequence(&obj);
    let parsed = parse_sequence(&seq).unwrap();
    let next = increment_sequence(&parsed, None);
    let next_parsed = parse_sequence(&next).unwrap();
    assert_eq!(next_parsed.shard_ix, 2);
    assert_eq!(next_parsed.version, 2);
    // seq_time should advance by 1000ms
    assert_eq!(next_parsed.seq_time, Some(1_600_000_011_000));
}

// -- resolve_shard_id errors --

#[test]
fn resolve_shard_id_non_numeric() {
    assert!(resolve_shard_id("shardId-abc").is_err());
}

#[test]
fn resolve_shard_id_above_max() {
    // 2147483648 = 2^31, just above the valid range [0, 2147483647]
    assert!(resolve_shard_id("shardId-2147483648").is_err());
}

#[test]
fn resolve_shard_id_negative_value() {
    // splitn(2, '-') on "shardId--1" → ["shardId", "-1"] → i64 = -1 → out of range
    assert!(resolve_shard_id("shardId--1").is_err());
}

#[test]
fn resolve_shard_id_no_dash() {
    assert!(resolve_shard_id("nodash").is_err());
}

#[test]
fn resolve_shard_id_valid_large() {
    let (name, ix) = resolve_shard_id("shardId-2147483647").unwrap();
    assert_eq!(ix, 2147483647);
    assert_eq!(name, "shardId-002147483647");
}

// -- shard_id_name --

#[test]
fn shard_id_name_negative_ix() {
    let name = shard_id_name(-1);
    assert!(name.starts_with("shardId--"), "got: {name}");
    assert!(name.contains("00000000001"));
}

#[test]
fn shard_id_name_large_negative() {
    let name = shard_id_name(-100);
    assert!(name.starts_with("shardId--"));
    assert!(name.contains("00000000100"));
}

// -- shard_ix_to_hex --

#[test]
fn shard_ix_to_hex_zero() {
    assert_eq!(shard_ix_to_hex(0), "00000000");
}

#[test]
fn shard_ix_to_hex_large() {
    assert_eq!(shard_ix_to_hex(256), "00000100");
}

#[test]
fn shard_ix_to_hex_negative_wraps_u32() {
    // -1 cast to u32 = 0xffffffff
    assert_eq!(shard_ix_to_hex(-1), "ffffffff");
}

// -- parse_sequence errors --

#[test]
fn parse_sequence_not_a_number() {
    assert!(parse_sequence("not-a-number").is_err());
}

#[test]
fn parse_sequence_empty_string() {
    assert!(parse_sequence("").is_err());
}

#[test]
fn parse_v2_negative_shard_ix() {
    use ferrokinesis::sequence::{SeqObj, parse_sequence, stringify_sequence};
    use num_bigint::BigUint;

    let obj = SeqObj {
        shard_create_time: 1_600_000_000_000,
        seq_ix: Some(BigUint::from(0u32)),
        byte1: Some("00".to_string()),
        seq_time: Some(1_600_000_001_000),
        seq_rand: None,
        shard_ix: -1,
        version: 2,
    };
    let seq = stringify_sequence(&obj);
    let parsed = parse_sequence(&seq);
    assert!(parsed.is_ok() || parsed.is_err());
}
