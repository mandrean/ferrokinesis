mod common;

use aes::cipher::{BlockEncryptMut, KeyIvInit, block_padding::Pkcs7};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use common::*;
use ferrokinesis::store::StoreOptions;
use serde_json::{Value, json};

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;

/// These constants mirror the private constants in shard_iterator.rs.
/// They are the fixed AES-256-CBC key/IV used by the local mock server.
const ITERATOR_PWD_KEY: [u8; 32] = [
    0x11, 0x33, 0xa5, 0xa8, 0x33, 0x66, 0x6b, 0x49, 0xab, 0xf2, 0x8c, 0x8b, 0xa3, 0x02, 0x93, 0x0f,
    0x0b, 0x2f, 0xb2, 0x40, 0xdc, 0xcd, 0x43, 0xcf, 0x4d, 0xfb, 0xc0, 0xca, 0x91, 0xf1, 0x77, 0x51,
];

const ITERATOR_PWD_IV: [u8; 16] = [
    0x7b, 0xf1, 0x39, 0xdb, 0xab, 0xbe, 0xa2, 0xd9, 0x99, 0x5d, 0x6f, 0xca, 0xe1, 0xdf, 0xf7, 0xda,
];

/// Create a shard iterator token with an explicit timestamp (in ms).
/// Mirrors the logic in shard_iterator::create_shard_iterator.
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

/// An iterator created > 5 minutes ago should return ExpiredIteratorException.
/// This exercises the `to_amz_utc_string` / `days_to_date` helper paths.
#[tokio::test]
async fn get_records_expired_iterator_returns_error() {
    let server = TestServer::new().await;
    let name = "gr-exp-iter";
    server.create_stream(name, 1).await;

    // Put a record to get a valid sequence number
    let put_body = server.put_record(name, "AAAA", "pk").await;
    let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();

    // Create an iterator whose timestamp is 10 minutes in the past
    let old_ts = now_ms() - 600_001;
    let expired_iter = create_iterator_with_timestamp(name, "shardId-000000000000", &seq, old_ts);

    let res = server
        .request("GetRecords", &json!({ "ShardIterator": expired_iter }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ExpiredIteratorException");
    // Message should contain date strings produced by to_amz_utc_string
    let msg = body["message"].as_str().unwrap_or("");
    assert!(!msg.is_empty(), "expected a message with timestamp info");
}

/// An iterator with timestamp == 0 should return InvalidArgumentException.
#[tokio::test]
async fn get_records_zero_timestamp_iterator_is_invalid() {
    let server = TestServer::new().await;
    let name = "gr-zero-ts";
    server.create_stream(name, 1).await;

    let put_body = server.put_record(name, "AAAA", "pk").await;
    let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();

    let iter = create_iterator_with_timestamp(name, "shardId-000000000000", &seq, 0);

    let res = server
        .request("GetRecords", &json!({ "ShardIterator": iter }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

/// An iterator with a future timestamp should return InvalidArgumentException.
#[tokio::test]
async fn get_records_future_timestamp_iterator_is_invalid() {
    let server = TestServer::new().await;
    let name = "gr-future-ts";
    server.create_stream(name, 1).await;

    let put_body = server.put_record(name, "AAAA", "pk").await;
    let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();

    let future_ts = now_ms() + 60_000; // 1 minute in the future
    let iter = create_iterator_with_timestamp(name, "shardId-000000000000", &seq, future_ts);

    let res = server
        .request("GetRecords", &json!({ "ShardIterator": iter }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

/// UpdateMaxRecordSize on a CREATING stream returns ResourceInUseException.
#[tokio::test]
async fn update_max_record_size_stream_not_active() {
    // Use a server with a 500ms create delay so the stream stays in CREATING state
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    })
    .await;
    let name = "umrs-creating";
    let arn = format!("arn:aws:kinesis:us-east-1:000000000000:stream/{name}");

    // Create the stream but DON'T wait for it to become active
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 200);
    // Do NOT sleep — stream is still in CREATING state

    let res = server
        .request(
            "UpdateMaxRecordSize",
            &json!({ "StreamARN": arn, "MaxRecordSizeInKiB": 4096 }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
}

/// UpdateStreamWarmThroughput on a CREATING stream returns ResourceInUseException.
#[tokio::test]
async fn update_warm_throughput_stream_not_active() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    })
    .await;
    let name = "uwt-creating";

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 200);

    let res = server
        .request(
            "UpdateStreamWarmThroughput",
            &json!({ "StreamName": name, "WarmThroughputMiBps": 50 }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
}

/// Configurable TTL: a 1-second TTL should expire an iterator older than 1 second.
#[tokio::test]
async fn get_records_configurable_ttl_expires_iterator() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        iterator_ttl_seconds: 1,
        ..Default::default()
    })
    .await;
    let name = "gr-custom-ttl";
    server.create_stream(name, 1).await;

    let put_body = server.put_record(name, "AAAA", "pk").await;
    let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();

    // Create an iterator 2 seconds in the past — should be expired with 1s TTL
    let old_ts = now_ms() - 2_000;
    let expired_iter = create_iterator_with_timestamp(name, "shardId-000000000000", &seq, old_ts);

    let res = server
        .request("GetRecords", &json!({ "ShardIterator": expired_iter }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ExpiredIteratorException");
}

/// An iterator within the custom TTL should NOT be expired.
#[tokio::test]
async fn get_records_within_custom_ttl_is_valid() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        iterator_ttl_seconds: 600,
        ..Default::default()
    })
    .await;
    let name = "gr-custom-ttl-valid";
    server.create_stream(name, 1).await;

    let put_body = server.put_record(name, "AAAA", "pk").await;
    let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();

    // Create an iterator 5 minutes in the past — within 10-minute TTL
    let old_ts = now_ms() - 300_000;
    let iter = create_iterator_with_timestamp(name, "shardId-000000000000", &seq, old_ts);

    let res = server
        .request("GetRecords", &json!({ "ShardIterator": iter }))
        .await;
    assert_eq!(res.status(), 200);
}
