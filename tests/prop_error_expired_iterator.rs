// Case count rationale: 50 cases — each case involves a server round-trip with an
// AES-encrypted iterator token, moderate count balances coverage and speed.
mod common;
use common::*;

use aes::cipher::{BlockEncryptMut, KeyIvInit, block_padding::Pkcs7};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use ferrokinesis::store::StoreOptions;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestRunner};
use serde_json::json;

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;

/// These constants mirror the private constants in shard_iterator.rs.
const ITERATOR_PWD_KEY: [u8; 32] = [
    0x11, 0x33, 0xa5, 0xa8, 0x33, 0x66, 0x6b, 0x49, 0xab, 0xf2, 0x8c, 0x8b, 0xa3, 0x02, 0x93, 0x0f,
    0x0b, 0x2f, 0xb2, 0x40, 0xdc, 0xcd, 0x43, 0xcf, 0x4d, 0xfb, 0xc0, 0xca, 0x91, 0xf1, 0x77, 0x51,
];

const ITERATOR_PWD_IV: [u8; 16] = [
    0x7b, 0xf1, 0x39, 0xdb, 0xab, 0xbe, 0xa2, 0xd9, 0x99, 0x5d, 0x6f, 0xca, 0xe1, 0xdf, 0xf7, 0xda,
];

fn create_iterator_with_timestamp(
    stream_name: &str,
    shard_id: &str,
    seq: &str,
    timestamp_ms: u64,
) -> String {
    let encrypt_str = format!(
        "{:014}/{stream_name}/{shard_id}/{seq}/{}",
        timestamp_ms,
        "0".repeat(36)
    );

    let plaintext = encrypt_str.as_bytes();
    let cipher = Aes256CbcEnc::new(&ITERATOR_PWD_KEY.into(), &ITERATOR_PWD_IV.into());

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
    buffer[7] = 1;
    buffer.extend_from_slice(&buf);

    BASE64.encode(&buffer)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// P23: Expired shard iterator returns ExpiredIteratorException.
///
/// With a 1-second TTL, any iterator whose timestamp is >= 2 seconds in the past
/// should be rejected as expired.
#[test]
fn prop_expired_iterator_returns_error() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let server = rt.block_on(TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        iterator_ttl_seconds: 1,
        ..Default::default()
    }));

    let name = unique_stream_name("prop-exp-iter");
    let (seq, shard_id) = rt.block_on(async {
        server.create_stream(&name, 1).await;
        let put_body = server.put_record(&name, "AAAA", "pk").await;
        let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();
        (seq, "shardId-000000000000".to_string())
    });

    let mut runner = TestRunner::new(Config {
        cases: 50,
        ..Config::default()
    });

    // Generate ms offsets past the 1-second TTL (2s to 600s)
    runner
        .run(&(2_000u64..=600_000), |offset_ms| {
            let ts = now_ms() - offset_ms;
            let expired_iter = create_iterator_with_timestamp(&name, &shard_id, &seq, ts);

            let (status, body) = rt.block_on(async {
                let res = server
                    .request("GetRecords", &json!({ "ShardIterator": expired_iter }))
                    .await;
                decode_body(res).await
            });

            prop_assert_eq!(
                status,
                400,
                "expected 400 for expired iterator with offset {}ms",
                offset_ms
            );
            prop_assert_eq!(
                body["__type"].as_str().unwrap_or(""),
                "ExpiredIteratorException",
                "expected ExpiredIteratorException for offset {}ms",
                offset_ms,
            );

            let msg = body["message"].as_str().unwrap_or("");
            prop_assert!(!msg.is_empty(), "expected non-empty error message");

            Ok(())
        })
        .unwrap();
}
