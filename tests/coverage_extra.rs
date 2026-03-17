/// Tests targeting previously uncovered code paths to improve overall coverage.
mod common;

use common::*;
use ferrokinesis::store::StoreOptions;
use ferrokinesis::types::{ConsumerStatus, StreamStatus};
use serde_json::{Value, json};

const ACCOUNT: &str = "0000-0000-0000";
const REGION: &str = "us-east-1";

fn stream_arn(name: &str) -> String {
    format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{name}")
}

// --- StopStreamEncryption error paths ---

/// StopStreamEncryption on a stream that is not KMS-encrypted (lines 30-37)
#[tokio::test]
async fn stop_encryption_on_none_encrypted_stream() {
    let server = TestServer::new().await;
    let name = "cx-stop-none";
    server.create_stream(name, 1).await;

    // Stream is NONE-encrypted by default; stop encryption should fail
    let res = server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("not encrypted with KMS")
    );
}

/// StopStreamEncryption on a CREATING stream (lines 20-27, StreamStatus::Creating Display)
#[tokio::test]
async fn stop_encryption_on_creating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
    })
    .await;
    let name = "cx-stop-creating";

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 200);
    // Do NOT sleep — stream stays in CREATING state

    let res = server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("CREATING"));
}

/// StopStreamEncryption on an UPDATING stream (StreamStatus::Updating Display)
#[tokio::test]
async fn stop_encryption_on_updating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 500,
        shard_limit: 50,
    })
    .await;
    let name = "cx-stop-updating";
    server.create_stream(name, 1).await;

    // StartStreamEncryption sets stream to UPDATING, returns 200 immediately
    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Immediately try to stop — stream is still UPDATING
    let res = server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("UPDATING"));
}

// --- StartStreamEncryption error paths ---

/// StartStreamEncryption on a CREATING stream
#[tokio::test]
async fn start_encryption_on_creating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
    })
    .await;
    let name = "cx-start-creating";

    server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    // No sleep — stream is CREATING

    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("CREATING"));
}

// --- StreamStatus::Deleting Display ---

/// StartStreamEncryption on a DELETING stream (StreamStatus::Deleting Display)
#[tokio::test]
async fn start_encryption_on_deleting_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 500,
        update_stream_ms: 0,
        shard_limit: 50,
    })
    .await;
    let name = "cx-start-deleting";
    server.create_stream(name, 1).await;

    // DeleteStream sets status to DELETING, then deletes after 500ms
    let res = server
        .request("DeleteStream", &json!({"StreamName": name}))
        .await;
    assert_eq!(res.status(), 200);

    // Immediately try StartStreamEncryption — stream is DELETING
    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("DELETING"));
}

// --- RemoveTagsFromStream invalid characters ---

/// RemoveTagsFromStream with a tag key containing an invalid character (lines 21-29)
#[tokio::test]
async fn remove_tags_invalid_char_in_key() {
    let server = TestServer::new().await;
    let name = "cx-tags-inv-char";
    server.create_stream(name, 1).await;

    server
        .request(
            "AddTagsToStream",
            &json!({"StreamName": name, "Tags": {"valid-key": "value"}}),
        )
        .await;

    let res = server
        .request(
            "RemoveTagsFromStream",
            &json!({
                "StreamName": name,
                "TagKeys": ["invalid!key"],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

// --- GetShardIterator AT_TIMESTAMP with no matching records ---

/// AT_TIMESTAMP iterator with no records in stream — exercises the "no record found" else branch
#[tokio::test]
async fn get_shard_iterator_at_timestamp_no_records() {
    let server = TestServer::new().await;
    let name = "cx-gsi-no-recs";
    server.create_stream(name, 1).await;

    // No records put; AT_TIMESTAMP with year-2001 timestamp → found_seq = None
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": 1_000_000_000.0f64,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["ShardIterator"].as_str().is_some());
}

// --- UpdateStreamWarmThroughput via ARN on CREATING stream ---

/// UpdateStreamWarmThroughput using ARN while stream is still CREATING
#[tokio::test]
async fn update_warm_throughput_by_arn_on_creating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
    })
    .await;
    let name = "cx-uwt-arn-creat";
    let arn = stream_arn(name);

    server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    // No sleep — stream is CREATING

    let res = server
        .request(
            "UpdateStreamWarmThroughput",
            &json!({ "StreamARN": arn, "WarmThroughputMiBps": 50 }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
}

// --- UpdateMaxRecordSize missing ARN (exercises action dead-code bypass) ---

/// UpdateMaxRecordSize missing StreamARN: caught by action's own guard (line 10-15)
/// Note: the validation layer may also catch this, but either way returns 400.
#[tokio::test]
async fn update_max_record_size_missing_arn_action_guard() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "UpdateMaxRecordSize",
            &json!({ "MaxRecordSizeInKiB": 4096 }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

// --- GetRecords AT_TIMESTAMP iterator with record after requested timestamp ---

/// AT_TIMESTAMP where a record exists but its arrival time is after the requested timestamp.
/// Exercises the "else" path in get_shard_iterator (record not found at/before ts).
#[tokio::test]
async fn get_shard_iterator_at_timestamp_record_after_ts() {
    let server = TestServer::new().await;
    let name = "cx-gsi-after-ts";
    server.create_stream(name, 1).await;

    // Put a record (its arrival_timestamp ≈ now)
    server.put_record(name, "AAAA", "pk").await;

    // Request AT_TIMESTAMP with the unix epoch (year 1970) — no record existed then
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": name,
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": 1.0f64,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    // Result is valid (uses fallback shard_seq when record's arrival > ts)
    assert!(body["ShardIterator"].as_str().is_some());
}

// --- Subscribe to stream that is not Active ---

/// SubscribeToShard when the stream is in UPDATING state
#[tokio::test]
async fn subscribe_to_updating_stream_returns_error() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 2000,
        shard_limit: 50,
    })
    .await;
    let name = "cx-sub-updating";
    let arn = stream_arn(name);
    server.create_stream(name, 1).await;

    // Register a consumer and wait for it to activate
    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "cx-c-updating" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    // Start encryption → sets stream to UPDATING for 2000ms
    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Subscribe immediately — stream is UPDATING
    let res = server
        .request(
            "SubscribeToShard",
            &json!({
                "ConsumerARN": consumer_arn,
                "ShardId": "shardId-000000000000",
                "StartingPosition": { "Type": "LATEST" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
}

// --- types.rs Display implementations ---

/// Exercise StreamStatus::Active Display (not triggered by any error-path test)
#[test]
fn stream_status_active_display() {
    assert_eq!(StreamStatus::Active.to_string(), "ACTIVE");
}

/// Exercise all ConsumerStatus Display variants (not used in error messages)
#[test]
fn consumer_status_display_all_variants() {
    assert_eq!(ConsumerStatus::Creating.to_string(), "CREATING");
    assert_eq!(ConsumerStatus::Deleting.to_string(), "DELETING");
    assert_eq!(ConsumerStatus::Active.to_string(), "ACTIVE");
}

// --- StoreOptions::default() ---

/// Exercise StoreOptions::default()
#[test]
fn store_options_default_values() {
    use ferrokinesis::store::StoreOptions;
    let opts = StoreOptions::default();
    assert_eq!(opts.create_stream_ms, 500);
    assert_eq!(opts.delete_stream_ms, 500);
    assert_eq!(opts.update_stream_ms, 500);
    assert_eq!(opts.shard_limit, 10);
}

// --- UpdateMaxRecordSize with ARN that has no "/" (stream_name_from_arn fails) ---

/// StreamARN without "/" passes string validation but fails stream name resolution
/// (exercises lines 31-36 in update_max_record_size.rs that are unreachable via normal ARN)
#[tokio::test]
async fn update_max_record_size_arn_without_slash() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "UpdateMaxRecordSize",
            // A valid-length string without "/" that passes validation but fails name resolution
            &json!({ "StreamARN": "arn-without-any-slash-chars", "MaxRecordSizeInKiB": 4096 }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

// --- error.rs: server_error(Some) and Debug/Clone coverage ---

/// Exercise server_error with Some arguments and Debug/Clone impls for error types
#[test]
fn error_server_error_with_some_args() {
    use ferrokinesis::error::KinesisErrorResponse;
    let err = KinesisErrorResponse::server_error(Some("CustomError"), Some("custom message"));
    assert_eq!(err.status_code, 500);
    assert_eq!(err.body.__type, "CustomError");
    assert_eq!(err.body.message.as_deref(), Some("custom message"));

    // Exercise Debug impl for KinesisError and KinesisErrorResponse
    let _ = format!("{:?}", err.body);
    let _ = format!("{:?}", err);

    // Exercise Clone
    let cloned = err.clone();
    assert_eq!(cloned.status_code, 500);
}

/// Exercise client_error with None message (covers the None branch of message.map)
#[test]
fn error_client_error_none_message() {
    use ferrokinesis::error::KinesisErrorResponse;
    let err = KinesisErrorResponse::client_error("SomeError", None);
    assert_eq!(err.status_code, 400);
    assert!(err.body.message.is_none());
    let _ = format!("{:?}", err);
}

// --- store.rs: get_records_range coverage ---

/// Direct call to store.get_records_range exercises that method
#[tokio::test]
async fn store_get_records_range_direct() {
    use ferrokinesis::store::Store;
    use ferrokinesis::types::StoredRecord;
    let store = Store::new(StoreOptions::default());

    // Put a record directly
    store
        .put_record(
            "test-grr",
            "aabbccdd/seq001",
            StoredRecord {
                partition_key: "pk".to_string(),
                data: "AAAA".to_string(),
                approximate_arrival_timestamp: 1.0,
            },
        )
        .await;

    // get_records_range covers lines 369-393 in store.rs
    let records = store
        .get_records_range("test-grr", "aabbccdd/", "aabbccde/")
        .await;
    assert_eq!(records.len(), 1);

    // Empty range returns nothing
    let empty = store
        .get_records_range("test-grr", "ffffffff/", "ffffffff0/")
        .await;
    assert_eq!(empty.len(), 0);
}
