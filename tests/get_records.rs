mod common;

use aes::cipher::{BlockEncryptMut, KeyIvInit, block_padding::Pkcs7};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use common::*;
use ferrokinesis::shard_iterator::create_shard_iterator;
use ferrokinesis::types::StoredRecordRef;
use ferrokinesis::util::current_time_ms;
use serde_json::{Value, json};

#[tokio::test]
async fn get_records_empty_stream() {
    let server = TestServer::new().await;
    let name = "test-get-empty";
    server.create_stream(name, 1).await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let records = server.get_records(&iter).await;
    assert_eq!(records["Records"].as_array().unwrap().len(), 0);
    assert!(records["MillisBehindLatest"].as_u64().is_some());
    assert!(records["NextShardIterator"].as_str().is_some());
}

#[tokio::test]
async fn get_records_after_put() {
    let server = TestServer::new().await;
    let name = "test-get-after-put";
    server.create_stream(name, 1).await;

    server.put_record(name, "dGVzdDE=", "key1").await;
    server.put_record(name, "dGVzdDI=", "key2").await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), 2);

    assert_eq!(records[0]["PartitionKey"], "key1");
    assert_eq!(records[0]["Data"], "dGVzdDE=");
    assert_eq!(records[1]["PartitionKey"], "key2");
    assert_eq!(records[1]["Data"], "dGVzdDI=");

    // Verify sequence numbers are present and ordered
    let s1 = records[0]["SequenceNumber"].as_str().unwrap();
    let s2 = records[1]["SequenceNumber"].as_str().unwrap();
    assert!(s1 < s2);

    // Verify arrival timestamps
    for record in records {
        let ts = record["ApproximateArrivalTimestamp"].as_f64().unwrap();
        assert!(ts > 1_000_000_000.0);
        assert!(ts < 10_000_000_000.0);
    }
}

#[tokio::test]
async fn get_records_serializes_whole_second_timestamps_as_integers_and_omits_encryption_type() {
    let server = TestServer::new().await;
    let name = "test-get-whole-second-timestamp";
    server.create_stream(name, 1).await;

    let alloc = server.store.allocate_sequence(name, &0).await.unwrap();
    let timestamp_secs = (current_time_ms() / 1000) as f64;
    server
        .store
        .put_record(
            name,
            &alloc.stream_key,
            &StoredRecordRef {
                partition_key: "key1",
                data: "AAAA",
                approximate_arrival_timestamp: timestamp_secs,
            },
        )
        .await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let record = &result["Records"][0];

    assert_eq!(
        record["ApproximateArrivalTimestamp"].as_i64(),
        Some(timestamp_secs as i64)
    );
    assert!(
        record.get("EncryptionType").is_none(),
        "GetRecords should not synthesize per-record encryption metadata from stream state"
    );
}

#[tokio::test]
async fn get_records_latest_iterator() {
    let server = TestServer::new().await;
    let name = "test-get-latest";
    server.create_stream(name, 1).await;

    // Put records before getting iterator
    server.put_record(name, "AAAA", "key1").await;
    server.put_record(name, "BBBB", "key2").await;

    // LATEST iterator should not see existing records
    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "LATEST")
        .await;
    let result = server.get_records(&iter).await;
    assert_eq!(result["Records"].as_array().unwrap().len(), 0);

    // Put a new record after getting iterator
    server.put_record(name, "CCCC", "key3").await;

    // Should see the new record with next iterator
    let next_iter = result["NextShardIterator"].as_str().unwrap();
    let result2 = server.get_records(next_iter).await;
    let records = result2["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["PartitionKey"], "key3");
}

#[tokio::test]
async fn get_records_at_sequence_number() {
    let server = TestServer::new().await;
    let name = "test-get-at-seq";
    server.create_stream(name, 1).await;

    let r1 = server.put_record(name, "AAAA", "key1").await;
    server.put_record(name, "BBBB", "key2").await;

    let seq = r1["SequenceNumber"].as_str().unwrap();
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let iter = body["ShardIterator"].as_str().unwrap();

    let result = server.get_records(iter).await;
    let records = result["Records"].as_array().unwrap();
    assert!(!records.is_empty());
    assert_eq!(records[0]["PartitionKey"], "key1");
}

#[tokio::test]
async fn get_records_after_sequence_number() {
    let server = TestServer::new().await;
    let name = "test-get-after-seq";
    server.create_stream(name, 1).await;

    let r1 = server.put_record(name, "AAAA", "key1").await;
    server.put_record(name, "BBBB", "key2").await;

    let seq = r1["SequenceNumber"].as_str().unwrap();
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let iter = body["ShardIterator"].as_str().unwrap();

    let result = server.get_records(iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["PartitionKey"], "key2");
}

#[tokio::test]
async fn get_records_with_limit() {
    let server = TestServer::new().await;
    let name = "test-get-limit";
    server.create_stream(name, 1).await;

    for i in 0..5 {
        server.put_record(name, "AAAA", &format!("key{i}")).await;
    }

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;

    let res = server
        .request("GetRecords", &json!({"ShardIterator": iter, "Limit": 2}))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["Records"].as_array().unwrap().len(), 2);
    assert!(body["NextShardIterator"].as_str().is_some());
}

#[tokio::test]
async fn get_records_pagination() {
    let server = TestServer::new().await;
    let name = "test-get-pagination";
    server.create_stream(name, 1).await;

    for i in 0..5 {
        server.put_record(name, "AAAA", &format!("key{i}")).await;
    }

    let mut iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let mut total = 0;

    for _ in 0..10 {
        let res = server
            .request("GetRecords", &json!({"ShardIterator": iter, "Limit": 2}))
            .await;
        let body: Value = res.json().await.unwrap();
        let batch = body["Records"].as_array().unwrap().len();
        total += batch;
        if batch == 0 {
            break;
        }
        iter = body["NextShardIterator"].as_str().unwrap().to_string();
    }
    assert_eq!(total, 5);
}

#[tokio::test]
async fn get_records_invalid_iterator() {
    let server = TestServer::new().await;
    let res = server
        .request("GetRecords", &json!({"ShardIterator": "invalid-iterator"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn get_records_millis_behind_latest() {
    let server = TestServer::new().await;
    let name = "test-millis-behind";
    server.create_stream(name, 1).await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    // For an empty shard, MillisBehindLatest should be 0 or close to 0
    assert!(result["MillisBehindLatest"].as_u64().is_some());
}

#[tokio::test]
async fn get_records_multi_shard() {
    let server = TestServer::new().await;
    let name = "test-multi-shard";
    server.create_stream(name, 3).await;

    // Put records to shard 0 using explicit hash key = 0
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "c2hhcmQw",
                "PartitionKey": "key1",
                "ExplicitHashKey": "0",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let put_body: Value = res.json().await.unwrap();
    assert_eq!(put_body["ShardId"], "shardId-000000000000");

    // Read from shard 0
    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["Data"], "c2hhcmQw");

    // Read from shard 1 - should be empty
    let iter1 = server
        .get_shard_iterator(name, "shardId-000000000001", "TRIM_HORIZON")
        .await;
    let result1 = server.get_records(&iter1).await;
    assert_eq!(result1["Records"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn get_records_purges_old_records() {
    let server = TestServer::new().await;
    let name = "test-gr-purge";
    server.create_stream(name, 1).await;

    server.put_record(name, "AAAA", "pk").await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;

    let result = server.get_records(&iter).await;
    assert_eq!(result["Records"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn get_records_iterator_wrong_length() {
    let server = TestServer::new().await;

    let short_buf = vec![0u8; 20];
    let short_iter = BASE64.encode(&short_buf);

    let res = server
        .request("GetRecords", &json!({ "ShardIterator": short_iter }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn get_records_iterator_wrong_version_header() {
    let server = TestServer::new().await;

    let mut buf = vec![0u8; 160];
    buf[7] = 2; // wrong version
    let iter = BASE64.encode(&buf);

    let res = server
        .request("GetRecords", &json!({ "ShardIterator": iter }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn get_records_iterator_shard_id_bad_format() {
    type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;

    const KEY: [u8; 32] = [
        0x11, 0x33, 0xa5, 0xa8, 0x33, 0x66, 0x6b, 0x49, 0xab, 0xf2, 0x8c, 0x8b, 0xa3, 0x02, 0x93,
        0x0f, 0x0b, 0x2f, 0xb2, 0x40, 0xdc, 0xcd, 0x43, 0xcf, 0x4d, 0xfb, 0xc0, 0xca, 0x91, 0xf1,
        0x77, 0x51,
    ];
    const IV: [u8; 16] = [
        0x7b, 0xf1, 0x39, 0xdb, 0xab, 0xbe, 0xa2, 0xd9, 0x99, 0x5d, 0x6f, 0xca, 0xe1, 0xdf, 0xf7,
        0xda,
    ];

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let shard_id = "shardId-abc";
    let stream_name = "valid-stream";
    let seq = "49590338271490256608559692538361571095921575989136588898";

    let encrypt_str = format!(
        "{:014}/{stream_name}/{shard_id}/{seq}/{}",
        now_ms,
        "0".repeat(36)
    );

    let plaintext = encrypt_str.as_bytes();
    let cipher = Aes256CbcEnc::new(&KEY.into(), &IV.into());
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
    let iterator = BASE64.encode(&buffer);

    let server = TestServer::new().await;
    let res = server
        .request("GetRecords", &json!({ "ShardIterator": iterator }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn get_records_invalid_stream_name_in_iterator() {
    type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;

    const KEY: [u8; 32] = [
        0x11, 0x33, 0xa5, 0xa8, 0x33, 0x66, 0x6b, 0x49, 0xab, 0xf2, 0x8c, 0x8b, 0xa3, 0x02, 0x93,
        0x0f, 0x0b, 0x2f, 0xb2, 0x40, 0xdc, 0xcd, 0x43, 0xcf, 0x4d, 0xfb, 0xc0, 0xca, 0x91, 0xf1,
        0x77, 0x51,
    ];
    const IV: [u8; 16] = [
        0x7b, 0xf1, 0x39, 0xdb, 0xab, 0xbe, 0xa2, 0xd9, 0x99, 0x5d, 0x6f, 0xca, 0xe1, 0xdf, 0xf7,
        0xda,
    ];

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let stream_name = "invalid!stream";
    let seq = "49590338271490256608559692538361571095921575989136588898";
    let shard_id = "shardId-000000000000";

    let encrypt_str = format!(
        "{:014}/{stream_name}/{shard_id}/{seq}/{}",
        now_ms,
        "0".repeat(36)
    );

    let plaintext = encrypt_str.as_bytes();
    let cipher = Aes256CbcEnc::new(&KEY.into(), &IV.into());
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
    let iterator = BASE64.encode(&buffer);

    let server = TestServer::new().await;
    let res = server
        .request("GetRecords", &json!({ "ShardIterator": iterator }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

/// After a shard is closed (via UpdateShardCount), reading past the end returns
/// null NextShardIterator.
#[tokio::test]
async fn get_records_closed_shard_null_next_iterator() {
    let server = TestServer::new().await;
    let name = "gr-closed";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "UpdateShardCount",
            &json!({
                "StreamName": name,
                "TargetShardCount": 2,
                "ScalingType": "UNIFORM_SCALING",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "LATEST")
        .await;

    let result = server.get_records(&iter).await;
    assert_eq!(result["Records"].as_array().unwrap().len(), 0);
    assert!(
        result["NextShardIterator"].is_null(),
        "Expected null NextShardIterator for exhausted closed shard, got: {:?}",
        result["NextShardIterator"]
    );
}

/// Open shard: NextShardIterator must always be present.
#[tokio::test]
async fn get_records_next_iterator_valid_on_open_shard() {
    let server = TestServer::new().await;
    let name = "gr-open";
    server.create_stream(name, 1).await;

    server.put_record(name, "AAAA", "pk").await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    assert_eq!(result["Records"].as_array().unwrap().len(), 1);
    assert!(result["NextShardIterator"].as_str().is_some());
}

/// AT_TIMESTAMP iterator type via GetShardIterator.
#[tokio::test]
async fn get_records_at_timestamp_iterator() {
    let server = TestServer::new().await;
    let name = "gr-at-ts";
    server.create_stream(name, 1).await;

    server.put_record(name, "AAAA", "pk1").await;

    let past_ts = 1_000_000_000.0f64; // year 2001
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": past_ts,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let iter = res.json::<Value>().await.unwrap()["ShardIterator"]
        .as_str()
        .unwrap()
        .to_string();

    let result = server.get_records(&iter).await;
    assert_eq!(result["Records"].as_array().unwrap().len(), 1);
}

/// MillisBehindLatest is present on every GetRecords response.
#[tokio::test]
async fn get_records_millis_behind_present() {
    let server = TestServer::new().await;
    let name = "gr-millis";
    server.create_stream(name, 1).await;

    server.put_record(name, "AAAA", "pk").await;

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    assert!(result["MillisBehindLatest"].is_number());
}

/// A valid-format iterator pointing to a shard beyond the stream's shard count
/// returns ResourceNotFoundException.
#[tokio::test]
async fn get_records_shard_out_of_range() {
    let server = TestServer::new().await;
    let name = "gr-bad-shard";
    server.create_stream(name, 1).await;

    let seq = "49590338271490256608559692538361571095921575989136588898";
    let iter = create_shard_iterator(name, "shardId-000000000099", seq, current_time_ms());

    let res = server
        .request("GetRecords", &json!({ "ShardIterator": iter }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}
