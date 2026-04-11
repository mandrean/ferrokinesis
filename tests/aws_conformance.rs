mod common;

use common::{TestServer, decode_body};
use serde_json::{Value, json};

#[derive(serde::Deserialize)]
struct GoldenFile {
    #[allow(dead_code)]
    operation: String,
    #[allow(dead_code)]
    scenario: String,
    http_status: u16,
    required_headers: Vec<String>,
    body: Value,
}

fn load_golden(path: &str) -> GoldenFile {
    let full = format!("{}/tests/golden/{path}", env!("CARGO_MANIFEST_DIR"));
    let data =
        std::fs::read_to_string(&full).unwrap_or_else(|e| panic!("failed to read {full}: {e}"));
    serde_json::from_str(&data).unwrap_or_else(|e| panic!("failed to parse {full}: {e}"))
}

/// Fields whose **values** are dynamic (different every run) but whose **type** must match.
const DYNAMIC_FIELDS: &[&str] = &[
    "SequenceNumber",
    "ShardIterator",
    "NextShardIterator",
    "ApproximateArrivalTimestamp",
    "StreamCreationTimestamp",
    "StartingSequenceNumber",
    "EndingSequenceNumber",
    "StreamARN",
];

fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Recursively compare two JSON values for structural compatibility.
///
/// Checks that field names, nesting, and value types match. Dynamic fields
/// (timestamps, sequence numbers, etc.) are checked for type only.
fn assert_shape_matches(golden: &Value, actual: &Value, path: &str) {
    match (golden, actual) {
        (Value::Object(g), Value::Object(a)) => {
            for key in g.keys() {
                assert!(
                    a.contains_key(key),
                    "Missing field at {path}.{key}: golden has it but actual does not.\n  \
                     Golden keys: {:?}\n  Actual keys: {:?}",
                    sorted_keys(g),
                    sorted_keys(a),
                );
                assert_shape_matches(&g[key], &a[key], &format!("{path}.{key}"));
            }
            for key in a.keys() {
                assert!(
                    g.contains_key(key),
                    "Unexpected field at {path}.{key}: actual has it but golden does not.\n  \
                     Golden keys: {:?}\n  Actual keys: {:?}",
                    sorted_keys(g),
                    sorted_keys(a),
                );
            }
        }
        (Value::Array(g), Value::Array(a)) => {
            assert_eq!(
                g.len(),
                a.len(),
                "Array length mismatch at {path}: expected {}, got {}",
                g.len(),
                a.len(),
            );
            for (index, (golden_item, actual_item)) in g.iter().zip(a.iter()).enumerate() {
                assert_shape_matches(golden_item, actual_item, &format!("{path}[{index}]"));
            }
        }
        _ => {
            // For dynamic fields, only check that the type matches
            let field_name = path.rsplit('.').next().unwrap_or(path);
            if DYNAMIC_FIELDS.contains(&field_name) {
                assert_eq!(
                    value_type_name(golden),
                    value_type_name(actual),
                    "Type mismatch at {path} (dynamic field): expected {}, got {}",
                    value_type_name(golden),
                    value_type_name(actual),
                );
            } else {
                assert_eq!(
                    golden, actual,
                    "Value mismatch at {path}: expected {golden:?}, got {actual:?}",
                );
            }
        }
    }
}

fn sorted_keys(map: &serde_json::Map<String, Value>) -> Vec<&String> {
    let mut keys: Vec<_> = map.keys().collect();
    keys.sort();
    keys
}

fn assert_conformance(
    golden: &GoldenFile,
    status: u16,
    headers: &reqwest::header::HeaderMap,
    body: &Value,
) {
    // 1. Status code
    assert_eq!(
        golden.http_status, status,
        "HTTP status mismatch: golden={}, actual={}",
        golden.http_status, status,
    );

    // 2. Required headers
    for header_name in &golden.required_headers {
        assert!(
            headers.contains_key(header_name.as_str()),
            "Missing required header: {header_name}",
        );
    }

    // 3. Body shape
    match (&golden.body, body) {
        (Value::Null, Value::Null) => {} // both empty — OK
        (Value::Null, _) => panic!("Expected empty body but got: {body}"),
        (_, Value::Null) => panic!("Expected body but got empty response"),
        _ => assert_shape_matches(&golden.body, body, "$"),
    }
}

#[test]
fn shape_matches_checks_every_array_element_and_length() {
    let golden = json!({
        "Records": [
            {"SequenceNumber": "dynamic", "PartitionKey": "pk1"},
            {"SequenceNumber": "dynamic", "PartitionKey": "pk2"}
        ]
    });
    let actual = json!({
        "Records": [
            {"SequenceNumber": "other", "PartitionKey": "pk1"},
            {"SequenceNumber": "other", "PartitionKey": "wrong"}
        ]
    });

    let err = std::panic::catch_unwind(|| assert_shape_matches(&golden, &actual, "$"))
        .expect_err("tail mismatch should fail");
    let msg = if let Some(msg) = err.downcast_ref::<String>() {
        msg.clone()
    } else if let Some(msg) = err.downcast_ref::<&str>() {
        msg.to_string()
    } else {
        String::new()
    };
    assert!(msg.contains("$.Records[1].PartitionKey"));

    let truncated = json!({
        "Records": [
            {"SequenceNumber": "other", "PartitionKey": "pk1"}
        ]
    });
    let err = std::panic::catch_unwind(|| assert_shape_matches(&golden, &truncated, "$"))
        .expect_err("truncated array should fail");
    let msg = if let Some(msg) = err.downcast_ref::<String>() {
        msg.clone()
    } else if let Some(msg) = err.downcast_ref::<&str>() {
        msg.to_string()
    } else {
        String::new()
    };
    assert!(msg.contains("Array length mismatch"));
}

#[test]
fn shape_matches_checks_error_messages_exactly() {
    let golden = json!({
        "__type": "ExpiredIteratorException",
        "message": "Iterator expired."
    });
    let actual = json!({
        "__type": "ExpiredIteratorException",
        "message": "Iterator expired at some other time."
    });

    let err = std::panic::catch_unwind(|| assert_shape_matches(&golden, &actual, "$"))
        .expect_err("message mismatch should fail");
    let msg = if let Some(msg) = err.downcast_ref::<String>() {
        msg.clone()
    } else if let Some(msg) = err.downcast_ref::<&str>() {
        msg.to_string()
    } else {
        String::new()
    };
    assert!(msg.contains("$.message"));
}

// ─── Happy path tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn conformance_create_stream() {
    let server = TestServer::new().await;
    let golden = load_golden("happy/create_stream.json");

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "test-stream", "ShardCount": 1}),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_describe_stream() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("happy/describe_stream.json");

    let res = server
        .request("DescribeStream", &json!({"StreamName": "test-stream"}))
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_describe_stream_summary() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("happy/describe_stream_summary.json");

    let res = server
        .request(
            "DescribeStreamSummary",
            &json!({"StreamName": "test-stream"}),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_list_streams() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("happy/list_streams.json");

    let res = server.request("ListStreams", &json!({})).await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_put_record() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("happy/put_record.json");

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "test-stream",
                "Data": "dGVzdA==",
                "PartitionKey": "pk1",
            }),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_put_records() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("happy/put_records.json");

    let res = server
        .request(
            "PutRecords",
            &json!({
                "StreamName": "test-stream",
                "Records": [
                    {"Data": "dGVzdA==", "PartitionKey": "pk1"}
                ],
            }),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_get_shard_iterator_trim_horizon() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("happy/get_shard_iterator_trim_horizon.json");

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_get_shard_iterator_latest() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("happy/get_shard_iterator_latest.json");

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "LATEST",
            }),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_get_shard_iterator_at_sequence_number() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let put_res = server.put_record("test-stream", "dGVzdA==", "pk1").await;
    let seq_num = put_res["SequenceNumber"].as_str().unwrap();
    let golden = load_golden("happy/get_shard_iterator_at_sequence_number.json");

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq_num,
            }),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_get_shard_iterator_after_sequence_number() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let put_res = server.put_record("test-stream", "dGVzdA==", "pk1").await;
    let seq_num = put_res["SequenceNumber"].as_str().unwrap();
    let golden = load_golden("happy/get_shard_iterator_after_sequence_number.json");

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AFTER_SEQUENCE_NUMBER",
                "StartingSequenceNumber": seq_num,
            }),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_get_shard_iterator_at_timestamp() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    server.put_record("test-stream", "dGVzdA==", "pk1").await;
    let golden = load_golden("happy/get_shard_iterator_at_timestamp.json");

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": 0.0,
            }),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_get_records() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    server.put_record("test-stream", "dGVzdA==", "pk1").await;
    let iter = server
        .get_shard_iterator("test-stream", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let golden = load_golden("happy/get_records.json");

    let res = server
        .request("GetRecords", &json!({"ShardIterator": iter}))
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_list_shards() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("happy/list_shards.json");

    let res = server
        .request("ListShards", &json!({"StreamName": "test-stream"}))
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

// ─── Error path tests ───────────────────────────────────────────────────────

#[tokio::test]
async fn conformance_create_stream_already_exists() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("error/create_stream_already_exists.json");

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "test-stream", "ShardCount": 1}),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_describe_stream_not_found() {
    let server = TestServer::new().await;
    let golden = load_golden("error/describe_stream_not_found.json");

    let res = server
        .request("DescribeStream", &json!({"StreamName": "nonexistent"}))
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_put_record_stream_not_found() {
    let server = TestServer::new().await;
    let golden = load_golden("error/put_record_stream_not_found.json");

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "nonexistent",
                "Data": "dGVzdA==",
                "PartitionKey": "pk1",
            }),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_get_records_expired_iterator() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("error/get_records_expired_iterator.json");

    // Create a server with a very short TTL so the iterator expires quickly
    let server2 = TestServer::with_options(ferrokinesis::store::StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        iterator_ttl_seconds: 1,
        ..Default::default()
    })
    .await;
    server2.create_stream("test-stream", 1).await;
    let iter = server2
        .get_shard_iterator("test-stream", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

    let res = server2
        .request("GetRecords", &json!({"ShardIterator": iter}))
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}

#[tokio::test]
async fn conformance_get_shard_iterator_invalid_shard() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let golden = load_golden("error/get_shard_iterator_invalid_shard.json");

    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "test-stream",
                "ShardId": "shardId-000000000099",
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        )
        .await;

    let headers = res.headers().clone();
    let (status, body) = decode_body(res).await;
    assert_conformance(&golden, status, &headers, &body);
}
