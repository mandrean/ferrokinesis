use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use num_bigint::BigUint;
use num_traits::{Num, One, Zero};

/// Maximum sequence sub-index value (`i64::MAX` as `u64`).
///
/// Used as the seq_ix when constructing closing sequence numbers for split/merge
/// operations. This ensures no future record could produce a sequence number ≥
/// the ending sequence, making the shard-closed invariant unconditionally safe.
pub const MAX_SEQ_IX: u64 = 0x7FFF_FFFF_FFFF_FFFF;

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

/// Decomposed components of a Kinesis sequence number.
///
/// Sequence numbers encode shard creation time, shard index, a per-record
/// sub-index, and a format version (v0, v1, or v2) inside a single decimal
/// string. This struct holds the parsed fields so callers can inspect or
/// reconstruct them without re-parsing.
#[derive(Debug, Clone)]
pub struct SeqObj {
    pub shard_create_time: u64,
    pub seq_ix: Option<u64>,
    pub byte1: Option<String>,
    pub seq_time: Option<u64>,
    pub seq_rand: Option<String>,
    pub shard_ix: i64,
    pub version: u32,
}

fn pow_2(n: u32) -> BigUint {
    BigUint::one() << n
}

/// Parse a decimal sequence number string into its [`SeqObj`] components.
///
/// Detects version (v0/v1/v2) from the high nibble of the underlying
/// 128-bit value and extracts shard creation time, shard index, sub-index,
/// and auxiliary fields accordingly.
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
                seq_ix: Some(u64::from_str_radix(seq_ix_hex, 16).unwrap_or(0)),
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
                seq_ix: Some(u64::from_str_radix(&hex[36..38], 16).unwrap_or(0)),
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

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Write `val` as a zero-padded lowercase hex string of exactly `width` nibbles
/// into `buf`. `buf.len()` must equal `width`.
fn write_hex_u64(buf: &mut [u8], val: u64, width: usize) {
    debug_assert_eq!(buf.len(), width);
    for i in 0..width {
        buf[i] = HEX_CHARS[((val >> ((width - 1 - i) * 4)) & 0xF) as usize];
    }
}

/// Write `val` as a zero-padded lowercase hex string of exactly `width` nibbles.
fn write_hex_u32(buf: &mut [u8], val: u32, width: usize) {
    debug_assert_eq!(buf.len(), width);
    for i in 0..width {
        buf[i] = HEX_CHARS[((val >> ((width - 1 - i) * 4)) & 0xF) as usize];
    }
}

/// Copy exactly `width` ASCII hex bytes from `src` into `buf`, right-aligned
/// and zero-padded if `src` is shorter.
fn write_hex_str(buf: &mut [u8], src: &str, width: usize) {
    debug_assert_eq!(buf.len(), width);
    let src_bytes = src.as_bytes();
    let src_len = src_bytes.len().min(width);
    let pad = width - src_len;
    buf[..pad].fill(b'0');
    buf[pad..].copy_from_slice(&src_bytes[src_bytes.len() - src_len..]);
}

/// Serialize a [`SeqObj`] back into its decimal sequence number string.
pub fn stringify_sequence(obj: &SeqObj) -> String {
    match obj.version {
        0 | 2 if obj.version == 0 => {
            // v0 hex layout: "1" + shard_create(9) + byte1(2) + seq_rand(16) + shard_ix(4) = 32
            let mut buf = [b'0'; 32];
            buf[0] = b'1';
            write_hex_u64(&mut buf[1..10], obj.shard_create_time / 1000, 9);
            write_hex_str(&mut buf[10..12], obj.byte1.as_deref().unwrap_or("00"), 2);
            write_hex_str(
                &mut buf[12..28],
                obj.seq_rand.as_deref().unwrap_or("0000000000000000"),
                16,
            );
            write_hex_u32(&mut buf[28..32], obj.shard_ix as u32, 4);

            let hex_str = core::str::from_utf8(&buf).unwrap();
            BigUint::from_str_radix(hex_str, 16)
                .unwrap_or_else(|_| BigUint::zero())
                .to_string()
        }
        1 => {
            // v1 hex layout: "2" + shard_create(9) + shard_ix_last(1) + byte1(2) + seq_time(9)
            //                + seq_rand(14) + seq_ix(2) + shard_ix(8) + "1" = 47
            let mut buf = [b'0'; 47];
            buf[0] = b'2';
            write_hex_u64(&mut buf[1..10], obj.shard_create_time / 1000, 9);
            buf[10] = HEX_CHARS[((obj.shard_ix as u32) & 0xF) as usize];
            write_hex_str(&mut buf[11..13], obj.byte1.as_deref().unwrap_or("00"), 2);
            write_hex_u64(
                &mut buf[13..22],
                obj.seq_time.unwrap_or(obj.shard_create_time) / 1000,
                9,
            );
            write_hex_str(
                &mut buf[22..36],
                obj.seq_rand.as_deref().unwrap_or("00000000000000"),
                14,
            );
            write_hex_u64(&mut buf[36..38], obj.seq_ix.unwrap_or(0), 2);
            write_hex_u32(&mut buf[38..46], obj.shard_ix as u32, 8);
            buf[46] = b'1';

            let hex_str = core::str::from_utf8(&buf).unwrap();
            BigUint::from_str_radix(hex_str, 16)
                .unwrap_or_else(|_| BigUint::zero())
                .to_string()
        }
        _ => {
            // v2 hex layout (default): "2" + shard_create(9) + shard_ix_last(1) + seq_ix(16)
            //                          + byte1(2) + seq_time(9) + shard_ix(8) + "2" = 47
            let mut buf = [b'0'; 47];
            buf[0] = b'2';
            write_hex_u64(&mut buf[1..10], obj.shard_create_time / 1000, 9);
            buf[10] = HEX_CHARS[((obj.shard_ix as u32) & 0xF) as usize];
            write_hex_u64(&mut buf[11..27], obj.seq_ix.unwrap_or(0), 16);
            write_hex_str(&mut buf[27..29], obj.byte1.as_deref().unwrap_or("00"), 2);
            write_hex_u64(
                &mut buf[29..38],
                obj.seq_time.unwrap_or(obj.shard_create_time) / 1000,
                9,
            );
            write_hex_u32(&mut buf[38..46], obj.shard_ix as u32, 8);
            buf[46] = b'2';

            let hex_str = core::str::from_utf8(&buf).unwrap();
            BigUint::from_str_radix(hex_str, 16)
                .unwrap_or_else(|_| BigUint::zero())
                .to_string()
        }
    }
}

/// Advance the timestamp to produce the next sequence number (v2 format).
pub fn increment_sequence(seq_obj: &SeqObj, seq_time: Option<u64>) -> String {
    stringify_sequence(&SeqObj {
        shard_create_time: seq_obj.shard_create_time,
        seq_ix: seq_obj.seq_ix,
        byte1: None,
        seq_time: Some(seq_time.unwrap_or_else(|| seq_obj.seq_time.unwrap_or(0) + 1000)),
        seq_rand: None,
        shard_ix: seq_obj.shard_ix,
        version: 2, // default version
    })
}

/// Format a shard index as an 8-digit hex string (two's-complement `u32`).
pub fn shard_ix_to_hex(shard_ix: i64) -> String {
    format!("{:08x}", shard_ix as u32)
}

/// Format a shard index as the canonical `shardId-NNNNNNNNNNNN` string.
pub fn shard_id_name(shard_ix: i64) -> String {
    if shard_ix < 0 {
        format!("shardId--{:011}", shard_ix.unsigned_abs())
    } else {
        format!("shardId-{:012}", shard_ix)
    }
}

/// Parse a shard ID string into its canonical form and numeric index.
///
/// Accepts both `"shardId-000000000001"` and bare `"1"` formats.
/// Returns `(canonical_id, shard_index)`.
pub fn resolve_shard_id(shard_id: &str) -> Result<(String, i64), SequenceError> {
    let parts: Vec<&str> = shard_id.splitn(2, '-').collect();
    let id_part = if parts.len() > 1 { parts[1] } else { parts[0] };

    let shard_ix: i64 = id_part.parse().map_err(|_| SequenceError::InvalidShardId)?;

    if !(0..=2147483647).contains(&shard_ix) {
        return Err(SequenceError::ShardIndexOutOfRange(shard_ix));
    }

    Ok((shard_id_name(shard_ix), shard_ix))
}

/// MD5-hash a partition key and return the result as a 128-bit integer.
///
/// Kinesis uses this hash to determine which shard a record belongs to.
pub fn partition_key_to_hash_key(partition_key: &str) -> u128 {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(partition_key.as_bytes());
    let result = hasher.finalize();
    u128::from_be_bytes(result.into())
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
            seq_ix: Some(42),
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
        assert!(hash > 0);
    }
}
