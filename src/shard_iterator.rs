use crate::util::current_time_ms;
use aes::cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit, block_padding::Pkcs7};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};

/// Errors from decoding a shard iterator token.
#[derive(Debug, thiserror::Error)]
pub enum ShardIteratorError {
    #[error("invalid base64")]
    InvalidBase64,
    #[error("invalid length")]
    InvalidLength,
    #[error("base64 encoding mismatch")]
    EncodingMismatch,
    #[error("invalid version header")]
    InvalidVersion,
    #[error("decryption failed")]
    DecryptionFailed,
    #[error("invalid UTF-8")]
    InvalidUtf8,
    #[error("malformed iterator payload")]
    MalformedPayload,
    #[error("invalid timestamp")]
    InvalidTimestamp,
}

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

pub const ITERATOR_PWD_KEY: [u8; 32] = [
    0x11, 0x33, 0xa5, 0xa8, 0x33, 0x66, 0x6b, 0x49, 0xab, 0xf2, 0x8c, 0x8b, 0xa3, 0x02, 0x93, 0x0f,
    0x0b, 0x2f, 0xb2, 0x40, 0xdc, 0xcd, 0x43, 0xcf, 0x4d, 0xfb, 0xc0, 0xca, 0x91, 0xf1, 0x77, 0x51,
];

pub const ITERATOR_PWD_IV: [u8; 16] = [
    0x7b, 0xf1, 0x39, 0xdb, 0xab, 0xbe, 0xa2, 0xd9, 0x99, 0x5d, 0x6f, 0xca, 0xe1, 0xdf, 0xf7, 0xda,
];

/// Create a shard iterator token (AES-256-CBC encrypted)
pub fn create_shard_iterator(stream_name: &str, shard_id: &str, seq: &str) -> String {
    let now = current_time_ms() as u128;

    let encrypt_str = format!(
        "{:014}/{stream_name}/{shard_id}/{seq}/{}",
        now,
        "0".repeat(36)
    );

    let plaintext = encrypt_str.as_bytes();

    let cipher = Aes256CbcEnc::new(&ITERATOR_PWD_KEY.into(), &ITERATOR_PWD_IV.into());

    // Manually pad and encrypt
    let block_size = 16;
    let pad_len = block_size - (plaintext.len() % block_size);
    let mut buf = plaintext.to_vec();
    buf.resize(plaintext.len() + pad_len, pad_len as u8);
    let encrypted_len = buf.len();
    cipher
        .encrypt_padded_mut::<Pkcs7>(&mut buf, plaintext.len())
        .unwrap();
    buf.truncate(encrypted_len);

    let mut buffer = vec![0u8; 8];
    buffer[7] = 1; // Version marker [0,0,0,0,0,0,0,1]
    buffer.extend_from_slice(&buf);

    BASE64.encode(&buffer)
}

/// Decode a shard iterator token, returning (iterator_time_ms, stream_name, shard_id, seq_no)
pub fn decode_shard_iterator(
    iterator: &str,
) -> Result<(u64, String, String, String), ShardIteratorError> {
    let buffer = BASE64
        .decode(iterator)
        .map_err(|_| ShardIteratorError::InvalidBase64)?;

    if buffer.len() < 152 || buffer.len() > 280 {
        return Err(ShardIteratorError::InvalidLength);
    }

    // Re-encode to check it matches (catches padding issues)
    if BASE64.encode(&buffer) != iterator {
        return Err(ShardIteratorError::EncodingMismatch);
    }

    // Check version header
    if buffer[..8] != [0, 0, 0, 0, 0, 0, 0, 1] {
        return Err(ShardIteratorError::InvalidVersion);
    }

    let cipher = Aes256CbcDec::new(&ITERATOR_PWD_KEY.into(), &ITERATOR_PWD_IV.into());
    let mut ciphertext = buffer[8..].to_vec();
    let decrypted = cipher
        .decrypt_padded_mut::<Pkcs7>(&mut ciphertext)
        .map_err(|_| ShardIteratorError::DecryptionFailed)?;

    let plaintext =
        String::from_utf8(decrypted.to_vec()).map_err(|_| ShardIteratorError::InvalidUtf8)?;
    let pieces: Vec<&str> = plaintext.split('/').collect();

    if pieces.len() != 5 {
        return Err(ShardIteratorError::MalformedPayload);
    }

    let iterator_time: u64 = pieces[0]
        .parse()
        .map_err(|_| ShardIteratorError::InvalidTimestamp)?;
    let stream_name = pieces[1].to_string();
    let shard_id = pieces[2].to_string();
    let seq_no = pieces[3].to_string();

    Ok((iterator_time, stream_name, shard_id, seq_no))
}
