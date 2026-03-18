mod common;

use base64::{Engine, engine::general_purpose::STANDARD};
use common::*;
use ferrokinesis::store::StoreOptions;
use num_bigint::BigUint;
use num_traits::{One, Zero};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate `byte_count` zero-filled bytes and return the base64-encoded string.
fn make_b64_data(byte_count: usize) -> String {
    let bytes = vec![0u8; byte_count];
    STANDARD.encode(&bytes)
}

/// Build a `Vec<Value>` of PutRecords entries.
fn make_records_batch(count: usize, data: &str, key_prefix: &str) -> Vec<Value> {
    (0..count)
        .map(|i| {
            json!({
                "Data": data,
                "PartitionKey": format!("{key_prefix}{i}"),
            })
        })
        .collect()
}

// ===========================================================================
// Group A: PutRecord Max Record Size
// These tests exercise the validation layer's existing per-record 1 MB limit
// (enforced in src/validation/rules.rs via Data field's len_lte(1048576)),
// not the new batch-level 5 MB code.
// ===========================================================================

#[tokio::test]
async fn put_record_exactly_1mb_succeeds() {
    let server = TestServer::new().await;
    let name = "stress-pr-1mb-ok";
    server.create_stream(name, 1).await;

    let data = make_b64_data(1_048_576);
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": data,
                "PartitionKey": "k",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["ShardId"].as_str().is_some());
    assert!(body["SequenceNumber"].as_str().is_some());
}

#[tokio::test]
async fn put_record_over_1mb_rejected() {
    let server = TestServer::new().await;
    let name = "stress-pr-1mb-over";
    server.create_stream(name, 1).await;

    let data = make_b64_data(1_048_577);
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": data,
                "PartitionKey": "k",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn put_record_exactly_1mb_roundtrip() {
    let server = TestServer::new().await;
    let name = "stress-pr-1mb-rt";
    server.create_stream(name, 1).await;

    let data = make_b64_data(1_048_576);
    let put_res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": data,
                "PartitionKey": "k",
            }),
        )
        .await;
    assert_eq!(put_res.status(), 200);

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["Data"].as_str().unwrap(), data);
}

// ===========================================================================
// Group B: PutRecords Batch Limits
// ===========================================================================

#[tokio::test]
async fn put_records_500_entries_succeeds() {
    let server = TestServer::new().await;
    let name = "stress-prs-500-ok";
    server.create_stream(name, 1).await;

    let records = make_records_batch(500, "AAAA", "k");
    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": records,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["FailedRecordCount"], 0);
    assert_eq!(body["Records"].as_array().unwrap().len(), 500);
}

#[tokio::test]
async fn put_records_501_entries_rejected() {
    let server = TestServer::new().await;
    let name = "stress-prs-501";
    server.create_stream(name, 1).await;

    let records = make_records_batch(501, "AAAA", "k");
    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": records,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
    let msg = body["message"].as_str().unwrap();
    assert!(
        msg.contains("less than or equal to 500"),
        "expected '500' constraint in message, got: {msg}"
    );
}

#[tokio::test]
async fn put_records_5mb_total_succeeds() {
    // Need a higher body limit since base64 of 5 MB is ~6.7 MB plus JSON framing
    let opts = StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    };
    let server = TestServer::with_body_limit(opts, 10 * 1024 * 1024).await;
    let name = "stress-prs-5mb-ok";
    server.create_stream(name, 1).await;

    // 500 records, each with decoded size that totals exactly 5 MB
    // 5_242_880 / 500 = 10_485 bytes per record (data only, ignoring key)
    // But we must account for partition key bytes too.
    // Use 1-byte keys ("0"-"9" etc padded) to keep it simple.
    // key "k" = 1 byte, so per-record payload = data_bytes + 1
    // We need total = 5_242_880, with 500 records each having 1-byte key:
    // 500 * (data_bytes + 1) = 5_242_880
    // data_bytes = 10_485 - 1 = 10_484
    // total = 500 * (10_484 + 1) = 500 * 10_485 = 5_242_500 < 5_242_880 ✓
    // Let's just make it exactly 5 MB:
    // Use 10 records with large data. 5_242_880 / 10 = 524_288 per record.
    // key "k" = 1 byte => data = 524_287 bytes each.
    // total = 10 * (524_287 + 1) = 5_242_880 ✓
    // Partition keys "0" through "9" are ASCII digits, each exactly 1 UTF-8 byte.
    let data = make_b64_data(524_287);
    let records: Vec<Value> = (0..10)
        .map(|i| {
            json!({
                "Data": data,
                "PartitionKey": format!("{i}"),
            })
        })
        .collect();

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": records,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["FailedRecordCount"], 0);
}

#[tokio::test]
async fn put_records_over_5mb_total_rejected() {
    let opts = StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    };
    let server = TestServer::with_body_limit(opts, 10 * 1024 * 1024).await;
    let name = "stress-prs-5mb-over";
    server.create_stream(name, 1).await;

    // 10 records, each with 524_288 decoded bytes + 1-byte key = 524_289 per record
    // total = 10 * 524_289 = 5_242_890, intentionally 10 bytes over the 5_242_880 limit.
    let data = make_b64_data(524_288);
    let records: Vec<Value> = (0..10)
        .map(|i| {
            json!({
                "Data": data,
                "PartitionKey": format!("{i}"),
            })
        })
        .collect();

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": records,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

// ===========================================================================
// Group C: High Shard Count (slow — ignored by default)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn create_stream_200_shards_hash_continuity() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 300,
        ..Default::default()
    })
    .await;
    let name = "stress-200-hash";
    server.create_stream(name, 200).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 200);

    // First shard starts at "0"
    let first_start: BigUint = shards[0]["HashKeyRange"]["StartingHashKey"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(first_start.is_zero(), "first shard should start at 0");

    // Contiguous: each shard's start = previous shard's end + 1
    for i in 1..shards.len() {
        let prev_end: BigUint = shards[i - 1]["HashKeyRange"]["EndingHashKey"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let cur_start: BigUint = shards[i]["HashKeyRange"]["StartingHashKey"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(
            cur_start,
            &prev_end + BigUint::one(),
            "shard {i} not contiguous with shard {}",
            i - 1
        );
    }

    // Last shard ends at 2^128 - 1
    let last_end: BigUint = shards[199]["HashKeyRange"]["EndingHashKey"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let max_hash = (BigUint::one() << 128) - BigUint::one();
    assert_eq!(last_end, max_hash, "last shard should end at 2^128-1");
}

#[tokio::test]
#[ignore]
async fn create_stream_200_shards_routing() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 300,
        ..Default::default()
    })
    .await;
    let name = "stress-200-route";
    server.create_stream(name, 200).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();

    // Put records targeting specific shards via ExplicitHashKey
    for target_shard in [0usize, 99, 199] {
        let start_key = shards[target_shard]["HashKeyRange"]["StartingHashKey"]
            .as_str()
            .unwrap();
        let expected_shard_id = shards[target_shard]["ShardId"].as_str().unwrap();

        let res = server
            .request(
                "PutRecord",
                &json!({
                    "StreamName": name,
                    "Data": "AAAA",
                    "PartitionKey": "pk",
                    "ExplicitHashKey": start_key,
                }),
            )
            .await;
        assert_eq!(res.status(), 200);
        let body: Value = res.json().await.unwrap();
        assert_eq!(
            body["ShardId"].as_str().unwrap(),
            expected_shard_id,
            "record with hash key {start_key} should land in shard {target_shard}"
        );
    }
}

// ===========================================================================
// Group D: Deep Buffer Pagination (slow — ignored by default)
// ===========================================================================

#[tokio::test]
#[ignore]
async fn get_records_10000_pagination() {
    let server = TestServer::new().await;
    let name = "stress-10k-pag";
    server.create_stream(name, 1).await;

    // Put 10,000 records in batches of 500
    let small_data = make_b64_data(16);
    for batch_start in (0..10_000).step_by(500) {
        let records = make_records_batch(500, &small_data, &format!("b{batch_start}-"));
        let res = server
            .request(
                "PutRecords",
                &json!({
                    "StreamName": name,
                    "Records": records,
                }),
            )
            .await;
        assert_eq!(res.status(), 200);
    }

    // Paginate through all records
    let mut iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let mut total = 0usize;
    let mut prev_seq = String::new();
    let mut iterations = 0;

    loop {
        let result = server.get_records(&iter).await;
        let records = result["Records"].as_array().unwrap();
        if records.is_empty() {
            break;
        }
        for r in records {
            let seq = r["SequenceNumber"].as_str().unwrap().to_string();
            if !prev_seq.is_empty() {
                assert!(
                    seq > prev_seq,
                    "sequence numbers must be strictly increasing"
                );
            }
            prev_seq = seq;
            total += 1;
        }
        iter = result["NextShardIterator"].as_str().unwrap().to_string();
        iterations += 1;
        assert!(
            iterations < 1000,
            "too many iterations, likely infinite loop"
        );
    }
    assert_eq!(total, 10_000);
}

#[tokio::test]
#[ignore]
async fn get_records_10000_custom_limit() {
    let server = TestServer::new().await;
    let name = "stress-10k-lim";
    server.create_stream(name, 1).await;

    let small_data = make_b64_data(16);
    for batch_start in (0..10_000).step_by(500) {
        let records = make_records_batch(500, &small_data, &format!("b{batch_start}-"));
        let res = server
            .request(
                "PutRecords",
                &json!({
                    "StreamName": name,
                    "Records": records,
                }),
            )
            .await;
        assert_eq!(res.status(), 200);
    }

    let mut iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let mut total = 0usize;
    let mut iterations = 0usize;

    loop {
        let res = server
            .request("GetRecords", &json!({"ShardIterator": iter, "Limit": 500}))
            .await;
        assert_eq!(res.status(), 200);
        let body: Value = res.json().await.unwrap();
        let records = body["Records"].as_array().unwrap();
        if records.is_empty() {
            break;
        }
        total += records.len();
        iter = body["NextShardIterator"].as_str().unwrap().to_string();
        iterations += 1;
        assert!(iterations < 1000, "too many iterations");
    }
    assert_eq!(total, 10_000);
    // 10,000 records / 500 limit = exactly 20 iterations.
    // In test conditions (fresh stream, no retention-aged records to skip),
    // GetRecords returns exactly `limit` records per call.
    assert_eq!(
        iterations, 20,
        "expected exactly 20 iterations, got {iterations}"
    );
}

// ===========================================================================
// Group E: Unicode and Binary Data
// ===========================================================================

#[tokio::test]
async fn put_record_all_byte_values() {
    let server = TestServer::new().await;
    let name = "stress-all-bytes";
    server.create_stream(name, 1).await;

    // All 256 byte values 0x00..=0xFF
    let original: Vec<u8> = (0..=255u8).collect();
    let b64_data = STANDARD.encode(&original);

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": b64_data,
                "PartitionKey": "bytes",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);

    let returned_b64 = records[0]["Data"].as_str().unwrap();
    let decoded = STANDARD.decode(returned_b64).unwrap();
    assert_eq!(decoded, original);
}

#[tokio::test]
async fn put_record_multibyte_utf8_partition_key() {
    let server = TestServer::new().await;
    let name = "stress-utf8-key";
    server.create_stream(name, 1).await;

    // Emoji, CJK, accented characters
    let keys = ["🔥🚀💯", "日本語テスト", "àéîõü", "mixed-🎉-café"];

    for key in &keys {
        let res = server
            .request(
                "PutRecord",
                &json!({
                    "StreamName": name,
                    "Data": "AAAA",
                    "PartitionKey": key,
                }),
            )
            .await;
        assert_eq!(res.status(), 200);
    }

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let records = result["Records"].as_array().unwrap();
    assert_eq!(records.len(), keys.len());

    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            records[i]["PartitionKey"].as_str().unwrap(),
            *key,
            "partition key mismatch at index {i}"
        );
    }
}

#[tokio::test]
async fn put_records_mixed_binary_and_unicode() {
    let server = TestServer::new().await;
    let name = "stress-mixed-bin";
    server.create_stream(name, 1).await;

    // Various binary payloads with unicode keys
    let entries: Vec<(Vec<u8>, &str)> = vec![
        (vec![0x00; 64], "null-bytes-空"),
        ((0..=255u8).collect(), "all-bytes-全"),
        (vec![0xFF; 128], "high-bytes-高"),
    ];

    let records: Vec<Value> = entries
        .iter()
        .map(|(data, key)| {
            json!({
                "Data": STANDARD.encode(data),
                "PartitionKey": key,
            })
        })
        .collect();

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": records,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["FailedRecordCount"], 0);

    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    let returned = result["Records"].as_array().unwrap();
    assert_eq!(returned.len(), entries.len());

    for (i, (original_data, key)) in entries.iter().enumerate() {
        assert_eq!(returned[i]["PartitionKey"].as_str().unwrap(), *key);
        let decoded = STANDARD
            .decode(returned[i]["Data"].as_str().unwrap())
            .unwrap();
        assert_eq!(decoded, *original_data, "data mismatch at index {i}");
    }
}

// ===========================================================================
// Group F: StreamARN Support
// ===========================================================================

#[tokio::test]
async fn put_records_via_stream_arn_succeeds() {
    let server = TestServer::new().await;
    let name = "stress-prs-arn";
    server.create_stream(name, 1).await;

    let arn = server.get_stream_arn(name).await;

    let records = make_records_batch(5, "AAAA", "k");
    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamARN": arn,
                "Records": records,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["FailedRecordCount"], 0);
    assert_eq!(body["Records"].as_array().unwrap().len(), 5);
}

#[tokio::test]
async fn put_record_via_stream_arn_succeeds() {
    let server = TestServer::new().await;
    let name = "stress-pr-arn";
    server.create_stream(name, 1).await;

    let arn = server.get_stream_arn(name).await;

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamARN": arn,
                "Data": "AAAA",
                "PartitionKey": "k",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["ShardId"].as_str().is_some());
    assert!(body["SequenceNumber"].as_str().is_some());
}
