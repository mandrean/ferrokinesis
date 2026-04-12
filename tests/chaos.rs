#![cfg(feature = "chaos")]

mod common;

use common::*;
use ferrokinesis::capture::{CaptureWriter, read_capture_file};
use ferrokinesis::chaos::load_chaos_config;
use ferrokinesis::store::StoreOptions;
use reqwest::Method;
use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderValue};
use serde_json::{Value, json};
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;

fn load_test_chaos_config(config: Value) -> ferrokinesis::chaos::ChaosConfig {
    let file = NamedTempFile::new().unwrap();
    std::fs::write(file.path(), serde_json::to_vec(&config).unwrap()).unwrap();
    load_chaos_config(file.path()).unwrap()
}

fn chaos_options(config: Value) -> StoreOptions {
    StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        chaos: load_test_chaos_config(config),
        ..Default::default()
    }
}

async fn chaos_status(server: &TestServer) -> Value {
    let res = server
        .raw_request(Method::GET, "/_chaos", HeaderMap::new(), vec![])
        .await;
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

async fn chaos_toggle(server: &TestServer, path: &str, body: Value) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    server
        .raw_request(
            Method::POST,
            path,
            headers,
            serde_json::to_vec(&body).unwrap(),
        )
        .await
}

#[tokio::test]
async fn chaos_runtime_api_can_toggle_scenarios() {
    let server = TestServer::with_options(chaos_options(json!({
        "seed": 7,
        "scenarios": [
            {"id": "fail", "type": "error_rate", "rate": 1.0, "operations": ["CreateStream"]},
            {"id": "slow", "type": "latency", "p99_add_ms": 250, "operations": ["ListStreams"]}
        ]
    })))
    .await;

    let body = chaos_status(&server).await;
    assert_eq!(body["scenarios"][0]["enabled"], false);
    assert_eq!(body["scenarios"][1]["enabled"], false);

    let res = chaos_toggle(&server, "/_chaos/enable", json!({"ids": ["slow"]})).await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["scenarios"][0]["enabled"], false);
    assert_eq!(body["scenarios"][1]["enabled"], true);

    let res = chaos_toggle(&server, "/_chaos/enable", json!({"ids": ["missing"]})).await;
    assert_eq!(res.status(), 400);

    let res = chaos_toggle(&server, "/_chaos/disable", json!({})).await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["scenarios"][0]["enabled"], false);
    assert_eq!(body["scenarios"][1]["enabled"], false);
}

#[tokio::test]
async fn chaos_error_rate_can_return_internal_failure() {
    let server = TestServer::with_options(chaos_options(json!({
        "seed": 11,
        "scenarios": [
            {
                "id": "fail",
                "type": "error_rate",
                "rate": 1.0,
                "enabled": true,
                "operations": ["CreateStream"],
                "error": "InternalFailure"
            }
        ]
    })))
    .await;

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "chaos-internal", "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 500);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InternalFailure");
}

#[tokio::test]
async fn chaos_error_rate_can_return_service_unavailable() {
    let server = TestServer::with_options(chaos_options(json!({
        "seed": 13,
        "scenarios": [
            {
                "id": "outage",
                "type": "error_rate",
                "rate": 1.0,
                "enabled": true,
                "operations": ["ListStreams"],
                "error": "ServiceUnavailable"
            }
        ]
    })))
    .await;

    let res = server.request("ListStreams", &json!({})).await;
    assert_eq!(res.status(), 503);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ServiceUnavailable");
}

#[tokio::test]
async fn chaos_throughput_burst_recovers_between_windows() {
    let server = TestServer::with_options(chaos_options(json!({
        "seed": 17,
        "scenarios": [
            {
                "id": "burst",
                "type": "throughput_burst",
                "enabled": false,
                "burst_duration_ms": 80,
                "burst_interval_ms": 200
            }
        ]
    })))
    .await;
    let name = "chaos-burst";
    server.create_stream(name, 1).await;

    let res = chaos_toggle(&server, "/_chaos/enable", json!({"ids": ["burst"]})).await;
    assert_eq!(res.status(), 200);

    let first = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "YQ==",
                "PartitionKey": "pk"
            }),
        )
        .await;
    assert_eq!(first.status(), 400);
    let body: Value = first.json().await.unwrap();
    assert_eq!(body["__type"], "ProvisionedThroughputExceededException");

    tokio::time::sleep(Duration::from_millis(140)).await;

    let second = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": name,
                "Data": "Yg==",
                "PartitionKey": "pk"
            }),
        )
        .await;
    assert_eq!(second.status(), 200);
}

#[tokio::test]
async fn chaos_latency_injects_noticeable_delay() {
    let server = TestServer::with_options(chaos_options(json!({
        "seed": 19,
        "scenarios": [
            {
                "id": "slow",
                "type": "latency",
                "enabled": true,
                "p99_add_ms": 150,
                "operations": ["ListStreams"]
            }
        ]
    })))
    .await;

    let mut delayed = false;
    for _ in 0..400 {
        let started = Instant::now();
        let res = server.request("ListStreams", &json!({})).await;
        assert_eq!(res.status(), 200);
        if started.elapsed() >= Duration::from_millis(100) {
            delayed = true;
            break;
        }
    }

    assert!(delayed, "expected at least one delayed response");
}

#[tokio::test]
async fn chaos_partial_failure_preserves_failed_records_and_skips_capture() {
    let capture_file = NamedTempFile::new().unwrap();
    let writer = CaptureWriter::new(capture_file.path(), false).unwrap();
    let server = TestServer::with_capture(
        chaos_options(json!({
            "seed": 23,
            "scenarios": [
                {
                    "id": "partial",
                    "type": "partial_failure",
                    "enabled": true,
                    "rate": 1.0,
                    "error": "ProvisionedThroughputExceededException"
                }
            ]
        })),
        writer,
    )
    .await;
    let name = "chaos-partial";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": name,
                "Records": [
                    {"Data": "YQ==", "PartitionKey": "pk-1"},
                    {"Data": "Yg==", "PartitionKey": "pk-2"},
                    {"Data": "Yw==", "PartitionKey": "pk-3"}
                ]
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["FailedRecordCount"], 3);
    let records = body["Records"].as_array().unwrap();
    assert_eq!(records.len(), 3);
    for record in records {
        assert_eq!(
            record["ErrorCode"],
            "ProvisionedThroughputExceededException"
        );
        assert!(record.get("SequenceNumber").is_none());
    }

    let desc = server.describe_stream(name).await;
    let shard_id = desc["StreamDescription"]["Shards"][0]["ShardId"]
        .as_str()
        .unwrap();
    let iterator = server
        .get_shard_iterator(name, shard_id, "TRIM_HORIZON")
        .await;
    let records = server.get_records(&iterator).await;
    assert_eq!(records["Records"], Value::Array(vec![]));

    let captured = read_capture_file(capture_file.path()).unwrap();
    assert!(captured.is_empty());
}
