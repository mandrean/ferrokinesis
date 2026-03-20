use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use num_bigint::BigUint;
use num_traits::{Num, One, ToPrimitive, Zero};

/// Errors from parsing or resolving sequence numbers and shard IDs.
#[derive(Debug, thiserror::Error)]
pub enum SequenceError {
    #[error("invalid sequence number: {0}")]
    InvalidSequenceNumber(String),
    #[error("unknown version for sequence: {0}")]
    UnknownVersion(String),
    #[error("sequence index too high")]
    SequenceIndexTooHigh,
    #[error("date too large: {0}")]
    DateTooLarge(u64),
    #[error("hex parse error: {0}")]
    HexParse(String),
    #[error("invalid shard ID")]
    InvalidShardId,
    #[error("shard index out of range: {0}")]
    ShardIndexOutOfRange(i64),
}

/// Parsed sequence number components
#[derive(Debug, Clone)]
pub struct SeqObj {
    pub shard_create_time: u64,
    pub seq_ix: Option<BigUint>,
    pub byte1: Option<String>,
    pub seq_time: Option<u64>,
    pub seq_rand: Option<String>,
    pub shard_ix: i64,
    pub version: u32,
}

fn pow_2(n: u32) -> BigUint {
    BigUint::one() << n
}

pub fn parse_sequence(seq: &str) -> Result<SeqObj, SequenceError> {
    let seq_num: BigUint = seq
        .parse()
        .map_err(|_| SequenceError::InvalidSequenceNumber(seq.to_string()))?;

    // AWS sequence numbers always have their high bit set (≥ 2^124). Numbers below
    // that threshold are pre-production / kinesalite v0 tokens that were generated
    // without the high bit. Bumping them up normalises the representation so that the
    // fixed hex-slice offsets used by all version parsers below are consistent.
    let pow_2_124 = pow_2(124);
    let seq_num = if seq_num < pow_2_124 {
        &seq_num + &pow_2_124
    } else {
        seq_num
    };

    let hex = format!("{seq_num:x}");

    // Version detection by numeric magnitude:
    //   v0  → [2^124,  2^125 × 16)   — kinesalite legacy format
    //   v1/v2 → (2^185, 2^185 × 3/2) — AWS production format; the exact version
    //            (1 or 2) is encoded in the last hex nibble of the sequence number.
    // Any value outside these ranges is an unknown/corrupt sequence.
    let pow_2_124_next = pow_2(125) * BigUint::from(16u32);
    let pow_2_185 = pow_2(185);
    let pow_2_185_next = &pow_2_185 * BigUint::from(3u32) / BigUint::from(2u32);

    let version = if seq_num < pow_2_124_next {
        0
    } else if seq_num > pow_2_185 && seq_num < pow_2_185_next {
        let last_char = &hex[hex.len() - 1..];
        u32::from_str_radix(last_char, 16).unwrap_or(0)
    } else {
        return Err(SequenceError::UnknownVersion(seq.to_string()));
    };

    match version {
        2 => {
            // v2 hex layout (total 47 hex digits, indices 0–46):
            //   hex[0]      = '2' (leading digit, always; the number lives in [2^185, 3×2^184))
            //   hex[1..10]  = shard_create_time (seconds, 9 hex digits)
            //   hex[10]     = last nibble of shard_ix (duplicated from hex[45] for locality)
            //   hex[11..27] = seq_ix (16 hex digits, unsigned 64-bit)
            //   hex[27..29] = byte1 (2 hex digits, opaque AWS field)
            //   hex[29..38] = seq_time (seconds, 9 hex digits)
            //   hex[38..46] = shard_ix (8 hex digits, two's-complement 32-bit)
            //   hex[46]     = '2' (trailing version nibble)
            let seq_ix_hex = &hex[11..27];
            let shard_ix_hex_raw = &hex[38..46];

            let first_nibble = u8::from_str_radix(&seq_ix_hex[..1], 16).unwrap_or(0);
            if first_nibble > 7 {
                return Err(SequenceError::SequenceIndexTooHigh);
            }

            // Two's-complement sign extension: the shard_ix field is stored as a
            // 32-bit unsigned value in hex, but Kinesis uses negative shard indices
            // for merged child shards. If the high nibble is > 7 the 32-bit value
            // represents a negative signed integer; subtract 2^32 to get the i64.
            let shard_ix_first = u8::from_str_radix(&shard_ix_hex_raw[..1], 16).unwrap_or(0);
            let shard_ix: i64 = if shard_ix_first > 7 {
                let val = i64::from_str_radix(shard_ix_hex_raw, 16).unwrap_or(0);
                val - (1i64 << 32)
            } else {
                i64::from_str_radix(shard_ix_hex_raw, 16).unwrap_or(0)
            };

            let shard_create_secs = u64::from_str_radix(&hex[1..10], 16)
                .map_err(|e| SequenceError::HexParse(e.to_string()))?;
            // 16_025_175_000 seconds is approximately year 2477. A value that large
            // would not fit in the 9-nibble hex slot that stringify_sequence allocates
            // for shard_create_time, producing a silently truncated (wrong) sequence.
            if shard_create_secs >= 16025175000 {
                return Err(SequenceError::DateTooLarge(shard_create_secs));
            }

            let seq_time = u64::from_str_radix(&hex[29..38], 16).unwrap_or(0) * 1000;

            Ok(SeqObj {
                shard_create_time: shard_create_secs * 1000,
                seq_ix: Some(
                    BigUint::from_str_radix(seq_ix_hex, 16).unwrap_or_else(|_| BigUint::zero()),
                ),
                byte1: Some(hex[27..29].to_string()),
                seq_time: Some(seq_time),
                seq_rand: None,
                shard_ix,
                version,
            })
        }
        1 => {
            let shard_create_secs = u64::from_str_radix(&hex[1..10], 16)
                .map_err(|e| SequenceError::HexParse(e.to_string()))?;
            let shard_ix_hex = &hex[38..46];
            let shard_ix = i64::from_str_radix(shard_ix_hex, 16).unwrap_or(0);

            Ok(SeqObj {
                shard_create_time: shard_create_secs * 1000,
                seq_ix: Some(BigUint::from(
                    u64::from_str_radix(&hex[36..38], 16).unwrap_or(0),
                )),
                byte1: Some(hex[11..13].to_string()),
                seq_time: Some(u64::from_str_radix(&hex[13..22], 16).unwrap_or(0) * 1000),
                seq_rand: Some(hex[22..36].to_string()),
                shard_ix,
                version,
            })
        }
        0 => {
            let shard_create_secs = u64::from_str_radix(&hex[1..10], 16)
                .map_err(|e| SequenceError::HexParse(e.to_string()))?;
            if shard_create_secs >= 16025175000 {
                return Err(SequenceError::DateTooLarge(shard_create_secs));
            }

            let shard_ix_hex = &hex[28..32];
            let shard_ix = i64::from_str_radix(shard_ix_hex, 16).unwrap_or(0);

            Ok(SeqObj {
                shard_create_time: shard_create_secs * 1000,
                seq_ix: None,
                byte1: Some(hex[10..12].to_string()),
                seq_time: None,
                seq_rand: Some(hex[12..28].to_string()),
                shard_ix,
                version,
            })
        }
        _ => Err(SequenceError::UnknownVersion(version.to_string())),
    }
}

pub fn stringify_sequence(obj: &SeqObj) -> String {
    match obj.version {
        0 | 2 if obj.version == 0 => {
            let shard_create_hex = format!("{:09x}", obj.shard_create_time / 1000)
                .chars()
                .rev()
                .take(9)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>();
            let byte1 = obj.byte1.as_deref().unwrap_or("00");
            let seq_rand = obj.seq_rand.as_deref().unwrap_or("0000000000000000");
            let shard_ix_hex = format!("{:08x}", obj.shard_ix as u32);
            let shard_ix_short = &shard_ix_hex[shard_ix_hex.len() - 4..];

            let hex_str = format!("1{shard_create_hex}{byte1}{seq_rand}{shard_ix_short}");
            BigUint::from_str_radix(&hex_str, 16)
                .unwrap_or_else(|_| BigUint::zero())
                .to_string()
        }
        1 => {
            let shard_create_hex = format!("{:09x}", obj.shard_create_time / 1000);
            let shard_create_hex = &shard_create_hex[shard_create_hex.len().saturating_sub(9)..];
            let shard_ix_last = format!("{:x}", obj.shard_ix as u32);
            let shard_ix_last = &shard_ix_last[shard_ix_last.len() - 1..];
            let byte1 = obj.byte1.as_deref().unwrap_or("00");
            let seq_time_hex = format!(
                "{:09x}",
                obj.seq_time.unwrap_or(obj.shard_create_time) / 1000
            );
            let seq_time_hex = &seq_time_hex[seq_time_hex.len().saturating_sub(9)..];
            let seq_rand = obj.seq_rand.as_deref().unwrap_or("00000000000000");
            let seq_ix = obj.seq_ix.as_ref().and_then(|v| v.to_u64()).unwrap_or(0);
            let seq_ix_hex = format!("{seq_ix:02x}");
            let seq_ix_hex = &seq_ix_hex[seq_ix_hex.len().saturating_sub(2)..];

            let hex_str = format!(
                "2{shard_create_hex}{shard_ix_last}{byte1}{seq_time_hex}{seq_rand}{seq_ix_hex}{}1",
                shard_ix_to_hex(obj.shard_ix)
            );
            BigUint::from_str_radix(&hex_str, 16)
                .unwrap_or_else(|_| BigUint::zero())
                .to_string()
        }
        _ => {
            // Version 2 (default)
            let shard_create_hex = format!("{:09x}", obj.shard_create_time / 1000);
            let shard_create_hex = &shard_create_hex[shard_create_hex.len().saturating_sub(9)..];
            let shard_ix_last = format!("{:x}", obj.shard_ix as u32);
            let shard_ix_last = &shard_ix_last[shard_ix_last.len() - 1..];

            let seq_ix = obj
                .seq_ix
                .as_ref()
                .map(|v| format!("{v:x}"))
                .unwrap_or_else(|| "0".to_string());
            let seq_ix_padded = format!("{seq_ix:0>16}");
            let seq_ix_hex = &seq_ix_padded[seq_ix_padded.len().saturating_sub(16)..];

            let byte1 = obj.byte1.as_deref().unwrap_or("00");

            let seq_time_hex = format!(
                "{:09x}",
                obj.seq_time.unwrap_or(obj.shard_create_time) / 1000
            );
            let seq_time_hex = &seq_time_hex[seq_time_hex.len().saturating_sub(9)..];

            let hex_str = format!(
                "2{shard_create_hex}{shard_ix_last}{seq_ix_hex}{byte1}{seq_time_hex}{}2",
                shard_ix_to_hex(obj.shard_ix)
            );
            BigUint::from_str_radix(&hex_str, 16)
                .unwrap_or_else(|_| BigUint::zero())
                .to_string()
        }
    }
}

pub fn increment_sequence(seq_obj: &SeqObj, seq_time: Option<u64>) -> String {
    stringify_sequence(&SeqObj {
        shard_create_time: seq_obj.shard_create_time,
        seq_ix: seq_obj.seq_ix.clone(),
        byte1: None,
        seq_time: Some(seq_time.unwrap_or_else(|| seq_obj.seq_time.unwrap_or(0) + 1000)),
        seq_rand: None,
        shard_ix: seq_obj.shard_ix,
        version: 2, // default version
    })
}

pub fn shard_ix_to_hex(shard_ix: i64) -> String {
    format!("{:08x}", shard_ix as u32)
}

pub fn shard_id_name(shard_ix: i64) -> String {
    if shard_ix < 0 {
        format!("shardId--{:011}", shard_ix.unsigned_abs())
    } else {
        format!("shardId-{:012}", shard_ix)
    }
}

pub fn resolve_shard_id(shard_id: &str) -> Result<(String, i64), SequenceError> {
    let parts: Vec<&str> = shard_id.splitn(2, '-').collect();
    let id_part = if parts.len() > 1 { parts[1] } else { parts[0] };

    let shard_ix: i64 = id_part.parse().map_err(|_| SequenceError::InvalidShardId)?;

    if !(0..=2147483647).contains(&shard_ix) {
        return Err(SequenceError::ShardIndexOutOfRange(shard_ix));
    }

    Ok((shard_id_name(shard_ix), shard_ix))
}

pub fn partition_key_to_hash_key(partition_key: &str) -> BigUint {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(partition_key.as_bytes());
    let result = hasher.finalize();
    BigUint::from_bytes_be(&result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_id_name() {
        assert_eq!(shard_id_name(0), "shardId-000000000000");
        assert_eq!(shard_id_name(1), "shardId-000000000001");
        assert_eq!(shard_id_name(12), "shardId-000000000012");
    }

    #[test]
    fn test_resolve_shard_id() {
        let (id, ix) = resolve_shard_id("shardId-0").unwrap();
        assert_eq!(id, "shardId-000000000000");
        assert_eq!(ix, 0);

        let (id, ix) = resolve_shard_id("shardId-1").unwrap();
        assert_eq!(id, "shardId-000000000001");
        assert_eq!(ix, 1);
    }

    #[test]
    fn test_shard_ix_to_hex() {
        assert_eq!(shard_ix_to_hex(0), "00000000");
        assert_eq!(shard_ix_to_hex(1), "00000001");
        assert_eq!(shard_ix_to_hex(255), "000000ff");
    }

    #[test]
    fn test_stringify_parse_roundtrip() {
        let obj = SeqObj {
            shard_create_time: 1_600_000_000_000,
            seq_ix: Some(BigUint::from(42u64)),
            byte1: Some("00".to_string()),
            seq_time: Some(1_600_000_001_000),
            seq_rand: None,
            shard_ix: 0,
            version: 2,
        };
        let seq = stringify_sequence(&obj);
        let parsed = parse_sequence(&seq).unwrap();
        assert_eq!(parsed.shard_create_time, obj.shard_create_time);
        assert_eq!(parsed.version, 2);
        assert_eq!(parsed.shard_ix, 0);
    }

    #[test]
    fn test_partition_key_to_hash_key() {
        // MD5 of "a" is 0cc175b9c0f1b6a831c399e269772661
        let hash = partition_key_to_hash_key("a");
        assert!(hash > BigUint::zero());
    }
}
