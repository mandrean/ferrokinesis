mod common;

use common::{TestServer, ferro_command};
use ferrokinesis::store::StoreOptions;
use ferrokinesis_core::capture::{CaptureOp, CaptureRecord};
use serde_json::Value;
use std::process::Stdio;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn parse_tail_json(stdout: &[u8]) -> Vec<Value> {
    serde_json::from_slice(stdout).unwrap()
}

#[tokio::test]
async fn streams_create_list_describe_and_delete() {
    let server = TestServer::new().await;

    let output = ferro_command(&server)
        .args(["streams", "create", "cli-stream", "--wait"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = ferro_command(&server)
        .args(["streams", "list"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cli-stream"));

    let output = ferro_command(&server)
        .args(["--json", "streams", "describe", "cli-stream"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["StreamName"], "cli-stream");
    assert_eq!(body["StreamStatus"], "ACTIVE");

    let output = ferro_command(&server)
        .args(["streams", "delete", "cli-stream", "--wait"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());

    let output = ferro_command(&server)
        .args(["streams", "list"])
        .output()
        .await
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains("cli-stream"));
}

#[tokio::test]
async fn shards_and_consumers_commands_work() {
    let server = TestServer::new().await;
    server.create_stream("topology", 2).await;

    let output = ferro_command(&server)
        .args(["--ndjson", "shards", "list", "topology"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.lines().count(), 2);

    let output = ferro_command(&server)
        .args(["consumers", "register", "topology", "c1"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    server.wait_for_consumer_active("topology", "c1").await;

    let output = ferro_command(&server)
        .args(["--json", "consumers", "list", "topology"])
        .output()
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["Consumers"].as_array().unwrap().len(), 1);

    let output = ferro_command(&server)
        .args(["--json", "consumers", "describe", "topology", "c1"])
        .output()
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(body["ConsumerName"], "c1");
    assert_eq!(body["ConsumerStatus"], "ACTIVE");

    let output = ferro_command(&server)
        .args(["consumers", "deregister", "topology", "c1"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
}

#[tokio::test]
async fn put_supports_text_file_stdin_and_base64() {
    let server = TestServer::new().await;
    server.create_stream("puts", 1).await;

    let output = ferro_command(&server)
        .args(["put", "puts", "hello", "--partition-key", "pk1"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());

    let file = NamedTempFile::new().unwrap();
    tokio::fs::write(file.path(), b"from-file").await.unwrap();
    let output = ferro_command(&server)
        .args([
            "put",
            "puts",
            "--file",
            file.path().to_str().unwrap(),
            "--partition-key",
            "pk2",
        ])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());

    let mut child = ferro_command(&server)
        .args(["put", "puts", "--stdin", "--partition-key", "pk3"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"from-stdin")
        .await
        .unwrap();
    let output = child.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = ferro_command(&server)
        .args([
            "put",
            "puts",
            "YjY0",
            "--base64",
            "--partition-key",
            "pk4",
            "--explicit-hash-key",
            "12345",
        ])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());

    let base64_file = NamedTempFile::new().unwrap();
    tokio::fs::write(base64_file.path(), b"YjY0LWZpbGUK\n")
        .await
        .unwrap();
    let output = ferro_command(&server)
        .args([
            "put",
            "puts",
            "--file",
            base64_file.path().to_str().unwrap(),
            "--base64",
            "--partition-key",
            "pk5",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut child = ferro_command(&server)
        .args([
            "put",
            "puts",
            "--stdin",
            "--base64",
            "--partition-key",
            "pk6",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"YjY0LXN0ZGluCg==\n")
        .await
        .unwrap();
    let output = child.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let iterator = server
        .get_shard_iterator("puts", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let records = server.get_records(&iterator).await["Records"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(records.len(), 6);
    assert_eq!(records[0]["Data"], "aGVsbG8=");
    assert_eq!(records[1]["Data"], "ZnJvbS1maWxl");
    assert_eq!(records[2]["Data"], "ZnJvbS1zdGRpbg==");
    assert_eq!(records[3]["Data"], "YjY0");
    assert_eq!(records[4]["Data"], "YjY0LWZpbGUK");
    assert_eq!(records[5]["Data"], "YjY0LXN0ZGluCg==");
}

#[tokio::test]
async fn tail_polling_reads_record_and_honors_limit() {
    let server = TestServer::new().await;
    server.create_stream("tail-stream", 1).await;

    let child = ferro_command(&server)
        .args([
            "--json",
            "tail",
            "tail-stream",
            "--from",
            "trim-horizon",
            "--limit",
            "1",
            "--poll-interval",
            "100ms",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;
    server.put_record("tail-stream", "aGVsbG8=", "pk1").await;

    let output = tokio::time::timeout(
        tokio::time::Duration::from_secs(10),
        child.wait_with_output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = parse_tail_json(&output.stdout);
    assert_eq!(body.len(), 1);
    assert_eq!(body[0]["PartitionKey"], "pk1");
    assert_eq!(body[0]["Data"], "aGVsbG8=");
}

#[tokio::test]
async fn tail_polling_refreshes_expired_iterator() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        iterator_ttl_seconds: 1,
        ..Default::default()
    })
    .await;
    server.create_stream("tail-expire", 1).await;

    let child = ferro_command(&server)
        .args([
            "--json",
            "tail",
            "tail-expire",
            "--from",
            "latest",
            "--limit",
            "1",
            "--poll-interval",
            "1500ms",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(3200)).await;
    server
        .put_record("tail-expire", "cmVmcmVzaA==", "pk-exp")
        .await;

    let output = tokio::time::timeout(
        tokio::time::Duration::from_secs(12),
        child.wait_with_output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = parse_tail_json(&output.stdout);
    assert_eq!(body.len(), 1);
    assert_eq!(body[0]["PartitionKey"], "pk-exp");
}

#[tokio::test]
async fn tail_efo_reads_records_and_missing_consumer_is_helpful() {
    let server = TestServer::new().await;
    server.create_stream("efo-stream", 1).await;

    let output = ferro_command(&server)
        .args(["tail", "efo-stream", "--consumer", "missing"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("consumers register"));

    server.register_consumer("efo-stream", "reader").await;
    server
        .wait_for_consumer_active("efo-stream", "reader")
        .await;

    let child = ferro_command(&server)
        .args([
            "--json",
            "tail",
            "efo-stream",
            "--consumer",
            "reader",
            "--from",
            "trim-horizon",
            "--limit",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;
    server.put_record("efo-stream", "ZWZv", "pk-efo").await;

    let output = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        child.wait_with_output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = parse_tail_json(&output.stdout);
    assert_eq!(body.len(), 1);
    assert_eq!(body[0]["PartitionKey"], "pk-efo");
}

#[tokio::test]
async fn tail_efo_resubscribes_after_session_rollover() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        subscribe_to_shard_session_ms: 250,
        ..Default::default()
    })
    .await;
    server.create_stream("efo-rollover", 1).await;
    server.register_consumer("efo-rollover", "reader").await;
    server
        .wait_for_consumer_active("efo-rollover", "reader")
        .await;

    let child = ferro_command(&server)
        .args([
            "--json",
            "tail",
            "efo-rollover",
            "--consumer",
            "reader",
            "--from",
            "trim-horizon",
            "--limit",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(700)).await;
    server
        .put_record("efo-rollover", "cm9sbG92ZXI=", "pk-rollover")
        .await;

    let output = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        child.wait_with_output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = parse_tail_json(&output.stdout);
    assert_eq!(body.len(), 1);
    assert_eq!(body[0]["PartitionKey"], "pk-rollover");
}

#[tokio::test]
async fn tail_json_rejects_unbounded_follow() {
    let server = TestServer::new().await;
    server.create_stream("tail-json", 1).await;

    let output = ferro_command(&server)
        .args(["--json", "tail", "tail-json"])
        .output()
        .await
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--ndjson"));
    assert!(stderr.contains("--limit") || stderr.contains("--no-follow"));
}

#[tokio::test]
async fn tail_limit_zero_is_rejected_by_clap() {
    let server = TestServer::new().await;
    let output = ferro_command(&server)
        .args(["tail", "tail-zero", "--limit", "0"])
        .output()
        .await
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--limit"));
    assert!(stderr.contains("0"));
    assert!(stderr.contains("non-zero") || stderr.contains("zero"));
}

#[tokio::test]
async fn tail_follow_flags_conflict() {
    let server = TestServer::new().await;
    server.create_stream("tail-conflict", 1).await;

    let output = ferro_command(&server)
        .args(["tail", "tail-conflict", "--follow", "--no-follow"])
        .output()
        .await
        .unwrap();

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be used with"));
}

#[tokio::test]
async fn replay_supports_stream_override_and_skip_bad_lines() {
    let server = TestServer::new().await;
    server.create_stream("replay-target", 1).await;

    let capture = NamedTempFile::new().unwrap();
    let lines = [
        serde_json::to_string(&CaptureRecord {
            op: CaptureOp::PutRecord,
            ts: 1,
            stream: "ignored".into(),
            partition_key: "pk1".into(),
            data: "aGVsbG8=".into(),
            explicit_hash_key: None,
            sequence_number: "1".into(),
            shard_id: "shardId-000000000000".into(),
        })
        .unwrap(),
        "{bad json}".into(),
    ];
    tokio::fs::write(capture.path(), lines.join("\n"))
        .await
        .unwrap();

    let output = ferro_command(&server)
        .args([
            "replay",
            capture.path().to_str().unwrap(),
            "--stream",
            "replay-target",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let iterator = server
        .get_shard_iterator("replay-target", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let records = server.get_records(&iterator).await["Records"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["PartitionKey"], "pk1");
}

#[tokio::test]
async fn replay_fail_fast_propagates_errors() {
    let server = TestServer::new().await;
    let capture = NamedTempFile::new().unwrap();
    let line = serde_json::to_string(&CaptureRecord {
        op: CaptureOp::PutRecord,
        ts: 1,
        stream: "missing-stream".into(),
        partition_key: "pk1".into(),
        data: "aGVsbG8=".into(),
        explicit_hash_key: None,
        sequence_number: "1".into(),
        shard_id: "shardId-000000000000".into(),
    })
    .unwrap();
    tokio::fs::write(capture.path(), format!("{line}\n"))
        .await
        .unwrap();

    let output = ferro_command(&server)
        .args(["replay", capture.path().to_str().unwrap(), "--fail-fast"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("ResourceNotFoundException"));
}

#[tokio::test]
async fn api_call_and_health_work() {
    let server = TestServer::new().await;
    server.create_stream("api-stream", 1).await;

    let output = ferro_command(&server)
        .args(["--json", "api", "call", "ListStreams"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let body: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        body["StreamNames"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "api-stream")
    );

    let output = ferro_command(&server)
        .args(["api", "call", "SubscribeToShard"])
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("tail --consumer"));

    let output = ferro_command(&server)
        .args(["health"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
}

#[tokio::test]
async fn tail_ndjson_emits_one_object_per_line() {
    let server = TestServer::new().await;
    server.create_stream("tail-ndjson", 1).await;

    let mut child = ferro_command(&server)
        .args([
            "--ndjson",
            "tail",
            "tail-ndjson",
            "--from",
            "trim-horizon",
            "--limit",
            "1",
        ])
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(400)).await;
    server
        .put_record("tail-ndjson", "bmRqc29u", "pk-json")
        .await;

    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout).lines();
    let line = tokio::time::timeout(tokio::time::Duration::from_secs(10), reader.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let body: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(body["PartitionKey"], "pk-json");
}
