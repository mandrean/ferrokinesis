mod common;
use common::*;

use ferrokinesis::capture::{CaptureOp, CaptureWriter, read_capture_file, scrub_partition_key};
use ferrokinesis::store::StoreOptions;
use serde_json::json;
use tempfile::NamedTempFile;

fn test_options() -> StoreOptions {
    StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    }
}

#[tokio::test]
async fn capture_put_record() {
    let capture_file = NamedTempFile::new().unwrap();
    let writer = CaptureWriter::new(capture_file.path(), false).unwrap();
    let server = TestServer::with_capture(test_options(), writer).await;

    server.create_stream("cap-test", 1).await;
    server.put_record("cap-test", "aGVsbG8=", "pk1").await;

    let records = read_capture_file(capture_file.path()).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].op, CaptureOp::PutRecord);
    assert_eq!(records[0].stream, "cap-test");
    assert_eq!(records[0].partition_key, "pk1");
    assert_eq!(records[0].data, "aGVsbG8=");
    assert!(!records[0].shard_id.is_empty());
    assert!(!records[0].sequence_number.is_empty());
    assert!(records[0].explicit_hash_key.is_none());
}

#[tokio::test]
async fn capture_put_records_batch() {
    let capture_file = NamedTempFile::new().unwrap();
    let writer = CaptureWriter::new(capture_file.path(), false).unwrap();
    let server = TestServer::with_capture(test_options(), writer).await;

    server.create_stream("cap-batch", 1).await;
    let resp = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": "cap-batch",
                "Records": [
                    {"Data": "YQ==", "PartitionKey": "k1"},
                    {"Data": "Yg==", "PartitionKey": "k2"},
                    {"Data": "Yw==", "PartitionKey": "k3"},
                ]
            }),
        )
        .await;
    assert_eq!(resp.status(), 200);

    let records = read_capture_file(capture_file.path()).unwrap();
    assert_eq!(records.len(), 3);
    for r in &records {
        assert_eq!(r.op, CaptureOp::PutRecords);
        assert_eq!(r.stream, "cap-batch");
        assert!(!r.shard_id.is_empty());
        assert!(!r.sequence_number.is_empty());
    }
    assert_eq!(records[0].partition_key, "k1");
    assert_eq!(records[1].partition_key, "k2");
    assert_eq!(records[2].partition_key, "k3");
}

#[tokio::test]
async fn capture_with_scrub() {
    let capture_file = NamedTempFile::new().unwrap();
    let writer = CaptureWriter::new(capture_file.path(), true).unwrap();
    let server = TestServer::with_capture(test_options(), writer).await;

    server.create_stream("cap-scrub", 1).await;
    server
        .put_record("cap-scrub", "aGVsbG8=", "my-secret-key")
        .await;

    let records = read_capture_file(capture_file.path()).unwrap();
    assert_eq!(records.len(), 1);
    // Partition key should be scrubbed (hex MD5)
    assert_ne!(records[0].partition_key, "my-secret-key");
    assert_eq!(records[0].partition_key.len(), 32); // MD5 hex
    // Should be deterministic
    assert_eq!(
        records[0].partition_key,
        scrub_partition_key("my-secret-key")
    );
}

#[tokio::test]
async fn capture_does_not_affect_non_put_operations() {
    let capture_file = NamedTempFile::new().unwrap();
    let writer = CaptureWriter::new(capture_file.path(), false).unwrap();
    let server = TestServer::with_capture(test_options(), writer).await;

    // These operations should NOT produce capture lines
    server.create_stream("cap-noput", 1).await;
    server.describe_stream("cap-noput").await;
    let resp = server.request("ListStreams", &json!({})).await;
    assert_eq!(resp.status(), 200);

    let records = read_capture_file(capture_file.path()).unwrap();
    assert_eq!(records.len(), 0);
}

#[tokio::test]
async fn capture_replay_round_trip() {
    // Step 1: Capture records from server A
    let capture_file = NamedTempFile::new().unwrap();
    let writer = CaptureWriter::new(capture_file.path(), false).unwrap();
    let server_a = TestServer::with_capture(test_options(), writer).await;

    server_a.create_stream("rt-stream", 1).await;
    server_a
        .put_record("rt-stream", "cmVjb3JkMQ==", "pk-a")
        .await;
    server_a
        .put_record("rt-stream", "cmVjb3JkMg==", "pk-b")
        .await;

    let captured = read_capture_file(capture_file.path()).unwrap();
    assert_eq!(captured.len(), 2);

    // Step 2: Replay to server B
    let server_b = TestServer::new().await;
    server_b.create_stream("rt-stream", 1).await;

    let client = reqwest::Client::new();
    for record in &captured {
        let mut body = json!({
            "StreamName": record.stream,
            "Data": record.data,
            "PartitionKey": record.partition_key,
        });
        if let Some(ref ehk) = record.explicit_hash_key {
            body["ExplicitHashKey"] = serde_json::Value::String(ehk.clone());
        }

        let resp = client
            .post(server_b.url())
            .header("Content-Type", AMZ_JSON)
            .header("X-Amz-Target", format!("{VERSION}.PutRecord"))
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
            )
            .header("X-Amz-Date", "20150101T000000Z")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
    }

    // Step 3: Verify records exist on server B
    let iter = server_b
        .get_shard_iterator("rt-stream", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let records_body = server_b.get_records(&iter).await;
    let recs = records_body["Records"].as_array().unwrap();
    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0]["Data"], "cmVjb3JkMQ==");
    assert_eq!(recs[1]["Data"], "cmVjb3JkMg==");
}

#[tokio::test]
async fn capture_explicit_hash_key_preserved() {
    let capture_file = NamedTempFile::new().unwrap();
    let writer = CaptureWriter::new(capture_file.path(), false).unwrap();
    let server = TestServer::with_capture(test_options(), writer).await;

    server.create_stream("cap-ehk", 1).await;
    let resp = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "cap-ehk",
                "Data": "dGVzdA==",
                "PartitionKey": "pk1",
                "ExplicitHashKey": "12345",
            }),
        )
        .await;
    assert_eq!(resp.status(), 200);

    let records = read_capture_file(capture_file.path()).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].explicit_hash_key.as_deref(), Some("12345"));
}
