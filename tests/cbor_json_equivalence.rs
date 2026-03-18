mod common;

use base64::Engine;
use common::{TestServer, assert_values_equivalent, decode_body};
use ferrokinesis::store::StoreOptions;
use serde_json::json;

// Volatile keys that may differ between calls due to timing or randomness.
const VOLATILE_KEYS: &[&str] = &[
    "NextShardIterator",
    "SequenceNumber",
    "ShardIterator",
    "x-amzn-RequestId",
];

// ─── Group A: Read-only operation equivalence ────────────────────────────────

#[tokio::test]
async fn equiv_list_streams_empty() {
    let server = TestServer::new().await;
    let payload = json!({"Limit": 100});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("ListStreams", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_values_equivalent(&json_body, &cbor_body, &[]);
}

#[tokio::test]
async fn equiv_list_streams_with_data() {
    let server = TestServer::new().await;
    server.create_stream("stream-a", 1).await;
    server.create_stream("stream-b", 2).await;

    let payload = json!({"Limit": 100});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("ListStreams", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_values_equivalent(&json_body, &cbor_body, &[]);
}

#[tokio::test]
async fn equiv_describe_stream() {
    let server = TestServer::new().await;
    server.create_stream("equiv-ds", 2).await;

    let payload = json!({"StreamName": "equiv-ds"});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("DescribeStream", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_values_equivalent(&json_body, &cbor_body, &[]);
}

#[tokio::test]
async fn equiv_describe_stream_summary() {
    let server = TestServer::new().await;
    server.create_stream("equiv-dss", 1).await;

    let payload = json!({"StreamName": "equiv-dss"});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("DescribeStreamSummary", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_values_equivalent(&json_body, &cbor_body, &[]);
}

#[tokio::test]
async fn equiv_list_shards() {
    let server = TestServer::new().await;
    server.create_stream("equiv-ls", 3).await;

    let payload = json!({"StreamName": "equiv-ls"});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("ListShards", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_values_equivalent(&json_body, &cbor_body, &["NextToken"]);
}

// ─── Group B: Write operation equivalence ────────────────────────────────────

#[tokio::test]
async fn equiv_create_stream() {
    let server = TestServer::new().await;

    let (json_status, _) = decode_body(
        server
            .request(
                "CreateStream",
                &json!({"StreamName": "cs-json", "ShardCount": 1}),
            )
            .await,
    )
    .await;
    let (cbor_status, _) = decode_body(
        server
            .cbor_request(
                "CreateStream",
                &json!({"StreamName": "cs-cbor", "ShardCount": 1}),
            )
            .await,
    )
    .await;
    assert_eq!(json_status, cbor_status);
    assert_eq!(json_status, 200);

    // Verify the CBOR-created stream actually exists
    let (desc_status, _) = decode_body(
        server
            .request("DescribeStream", &json!({"StreamName": "cs-cbor"}))
            .await,
    )
    .await;
    assert_eq!(desc_status, 200);
}

#[tokio::test]
async fn equiv_put_record() {
    let server = TestServer::new().await;
    server.create_stream("equiv-pr", 1).await;

    let data = base64::engine::general_purpose::STANDARD.encode(b"hello");
    let payload = json!({
        "StreamName": "equiv-pr",
        "Data": data,
        "PartitionKey": "pk-1",
    });

    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("PutRecord", &payload).await;
    assert_eq!(json_status, cbor_status);
    // ShardId should be the same for the same partition key
    assert_eq!(json_body["ShardId"], cbor_body["ShardId"]);
    assert_values_equivalent(&json_body, &cbor_body, &["SequenceNumber"]);
}

#[tokio::test]
async fn equiv_put_records() {
    let server = TestServer::new().await;
    server.create_stream("equiv-prs", 2).await;

    let data = base64::engine::general_purpose::STANDARD.encode(b"batch-data");
    let payload = json!({
        "StreamName": "equiv-prs",
        "Records": [
            {"Data": data, "PartitionKey": "pk-a"},
            {"Data": data, "PartitionKey": "pk-b"},
        ],
    });

    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("PutRecords", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_values_equivalent(&json_body, &cbor_body, &["SequenceNumber"]);
}

#[tokio::test]
async fn equiv_get_shard_iterator() {
    let server = TestServer::new().await;
    server.create_stream("equiv-gsi", 1).await;

    let payload = json!({
        "StreamName": "equiv-gsi",
        "ShardId": "shardId-000000000000",
        "ShardIteratorType": "TRIM_HORIZON",
    });

    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("GetShardIterator", &payload).await;
    assert_eq!(json_status, cbor_status);
    // Both should have a ShardIterator field (values will differ)
    assert!(json_body.get("ShardIterator").is_some());
    assert!(cbor_body.get("ShardIterator").is_some());
}

#[tokio::test]
async fn equiv_get_records_empty() {
    let server = TestServer::new().await;
    server.create_stream("equiv-gre", 1).await;

    // Get iterators via JSON (same underlying state)
    let json_iter = server
        .get_shard_iterator("equiv-gre", "shardId-000000000000", "TRIM_HORIZON")
        .await;

    let json_resp = decode_body(
        server
            .request("GetRecords", &json!({"ShardIterator": json_iter}))
            .await,
    )
    .await;

    // Get a fresh iterator for CBOR
    let cbor_iter_resp = decode_body(
        server
            .cbor_request(
                "GetShardIterator",
                &json!({
                    "StreamName": "equiv-gre",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            )
            .await,
    )
    .await;
    let cbor_iter = cbor_iter_resp.1["ShardIterator"].as_str().unwrap();

    let cbor_resp = decode_body(
        server
            .cbor_request("GetRecords", &json!({"ShardIterator": cbor_iter}))
            .await,
    )
    .await;

    assert_eq!(json_resp.0, cbor_resp.0);
    assert_eq!(json_resp.1["Records"].as_array().unwrap().len(), 0);
    assert_eq!(cbor_resp.1["Records"].as_array().unwrap().len(), 0);
    assert_values_equivalent(&json_resp.1, &cbor_resp.1, VOLATILE_KEYS);
}

#[tokio::test]
async fn equiv_get_records_with_data() {
    let server = TestServer::new().await;
    server.create_stream("equiv-grd", 1).await;

    // Put records via JSON
    let data = base64::engine::general_purpose::STANDARD.encode(b"test-payload");
    server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "equiv-grd",
                "Data": data,
                "PartitionKey": "pk-1",
            }),
        )
        .await;

    // Read via JSON
    let json_iter = server
        .get_shard_iterator("equiv-grd", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let json_resp = decode_body(
        server
            .request("GetRecords", &json!({"ShardIterator": json_iter}))
            .await,
    )
    .await;

    // Read via CBOR
    let cbor_iter_resp = decode_body(
        server
            .cbor_request(
                "GetShardIterator",
                &json!({
                    "StreamName": "equiv-grd",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            )
            .await,
    )
    .await;
    let cbor_iter = cbor_iter_resp.1["ShardIterator"].as_str().unwrap();
    let cbor_resp = decode_body(
        server
            .cbor_request("GetRecords", &json!({"ShardIterator": cbor_iter}))
            .await,
    )
    .await;

    assert_eq!(json_resp.0, cbor_resp.0);
    let json_records = json_resp.1["Records"].as_array().unwrap();
    let cbor_records = cbor_resp.1["Records"].as_array().unwrap();
    assert_eq!(json_records.len(), cbor_records.len());
    assert_eq!(json_records.len(), 1);

    // Data should be identical after decoding
    assert_eq!(json_records[0]["Data"], cbor_records[0]["Data"]);
    assert_eq!(
        json_records[0]["PartitionKey"],
        cbor_records[0]["PartitionKey"]
    );
    assert_values_equivalent(&json_resp.1, &cbor_resp.1, VOLATILE_KEYS);

    // Verify decoded bytes match the original payload
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(json_records[0]["Data"].as_str().unwrap())
        .unwrap();
    assert_eq!(decoded, b"test-payload");
}

// ─── Group C: Cross-format Data round-trips ──────────────────────────────────

#[tokio::test]
async fn cross_put_json_get_cbor() {
    let server = TestServer::new().await;
    server.create_stream("cross-jc", 1).await;

    let raw = b"hello world";
    let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
    server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "cross-jc",
                "Data": b64,
                "PartitionKey": "pk",
            }),
        )
        .await;

    // Read via CBOR
    let iter_resp = decode_body(
        server
            .cbor_request(
                "GetShardIterator",
                &json!({
                    "StreamName": "cross-jc",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            )
            .await,
    )
    .await;
    let iter = iter_resp.1["ShardIterator"].as_str().unwrap();

    let resp = decode_body(
        server
            .cbor_request("GetRecords", &json!({"ShardIterator": iter}))
            .await,
    )
    .await;

    let records = resp.1["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    let data_b64 = records[0]["Data"].as_str().unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(data_b64)
        .unwrap();
    assert_eq!(decoded, raw);
}

#[tokio::test]
async fn cross_put_cbor_get_json() {
    let server = TestServer::new().await;
    server.create_stream("cross-cj", 1).await;

    let raw = b"hello from cbor";
    let b64 = base64::engine::general_purpose::STANDARD.encode(raw);

    // Put via CBOR with byte string Data
    let resp = server
        .cbor_request_raw_data(
            "PutRecord",
            &json!({
                "StreamName": "cross-cj",
                "Data": b64,  // placeholder, will be replaced by raw bytes
                "PartitionKey": "pk",
            }),
            "Data",
            raw,
        )
        .await;
    assert_eq!(resp.status(), 200);

    // Read via JSON
    let iter = server
        .get_shard_iterator("cross-cj", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let resp = decode_body(
        server
            .request("GetRecords", &json!({"ShardIterator": iter}))
            .await,
    )
    .await;

    let records = resp.1["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    let data_b64 = records[0]["Data"].as_str().unwrap();
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(data_b64)
        .unwrap();
    assert_eq!(decoded, raw);
}

#[tokio::test]
async fn cross_put_records_json_get_cbor() {
    let server = TestServer::new().await;
    server.create_stream("cross-prs-jc", 1).await;

    let raw = b"batch-item";
    let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
    server
        .request(
            "PutRecords",
            &json!({
                "StreamName": "cross-prs-jc",
                "Records": [
                    {"Data": b64, "PartitionKey": "pk1"},
                ],
            }),
        )
        .await;

    let iter_resp = decode_body(
        server
            .cbor_request(
                "GetShardIterator",
                &json!({
                    "StreamName": "cross-prs-jc",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            )
            .await,
    )
    .await;
    let iter = iter_resp.1["ShardIterator"].as_str().unwrap();

    let resp = decode_body(
        server
            .cbor_request("GetRecords", &json!({"ShardIterator": iter}))
            .await,
    )
    .await;

    let records = resp.1["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(records[0]["Data"].as_str().unwrap())
        .unwrap();
    assert_eq!(decoded, raw);
}

#[tokio::test]
async fn cross_put_records_cbor_get_json() {
    let server = TestServer::new().await;
    server.create_stream("cross-prs-cj", 1).await;

    let raw = b"cbor-batch";
    let b64 = base64::engine::general_purpose::STANDARD.encode(raw);

    let payload = json!({
        "StreamName": "cross-prs-cj",
        "Records": [
            {"Data": b64, "PartitionKey": "pk1"},
        ],
    });
    let resp = server
        .cbor_request_raw_data("PutRecords", &payload, "Records.*.Data", raw)
        .await;
    assert_eq!(resp.status(), 200);

    // Read via JSON
    let iter = server
        .get_shard_iterator("cross-prs-cj", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let resp = decode_body(
        server
            .request("GetRecords", &json!({"ShardIterator": iter}))
            .await,
    )
    .await;

    let records = resp.1["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(records[0]["Data"].as_str().unwrap())
        .unwrap();
    assert_eq!(decoded, raw);
}

#[tokio::test]
async fn cross_empty_data() {
    let server = TestServer::new().await;
    server.create_stream("cross-empty", 1).await;

    let empty_b64 = base64::engine::general_purpose::STANDARD.encode(b"");

    // Put via JSON with empty Data
    server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "cross-empty",
                "Data": empty_b64,
                "PartitionKey": "pk",
            }),
        )
        .await;

    // Put via CBOR with empty byte string
    let resp = server
        .cbor_request_raw_data(
            "PutRecord",
            &json!({
                "StreamName": "cross-empty",
                "Data": empty_b64,
                "PartitionKey": "pk2",
            }),
            "Data",
            b"",
        )
        .await;
    assert_eq!(resp.status(), 200);

    // Read all via JSON
    let iter = server
        .get_shard_iterator("cross-empty", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let resp = decode_body(
        server
            .request("GetRecords", &json!({"ShardIterator": iter}))
            .await,
    )
    .await;

    let records = resp.1["Records"].as_array().unwrap();
    assert_eq!(records.len(), 2);
    for r in records {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(r["Data"].as_str().unwrap())
            .unwrap();
        assert!(decoded.is_empty(), "Expected empty data, got {decoded:?}");
    }

    // Read all via CBOR
    let iter_resp = decode_body(
        server
            .cbor_request(
                "GetShardIterator",
                &json!({
                    "StreamName": "cross-empty",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            )
            .await,
    )
    .await;
    let iter = iter_resp.1["ShardIterator"].as_str().unwrap();
    let cbor_resp = decode_body(
        server
            .cbor_request("GetRecords", &json!({"ShardIterator": iter}))
            .await,
    )
    .await;

    let records = cbor_resp.1["Records"].as_array().unwrap();
    assert_eq!(records.len(), 2);
    for r in records {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(r["Data"].as_str().unwrap())
            .unwrap();
        assert!(decoded.is_empty(), "Expected empty data, got {decoded:?}");
    }
}

#[tokio::test]
async fn cross_binary_data() {
    let server = TestServer::new().await;
    server.create_stream("cross-bin", 1).await;

    let raw = vec![0x00, 0xFF, 0xFE, 0x80, 0x01, 0x7F];
    let b64 = base64::engine::general_purpose::STANDARD.encode(&raw);

    // Put via JSON
    server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "cross-bin",
                "Data": b64,
                "PartitionKey": "pk-json",
            }),
        )
        .await;

    // Put via CBOR byte string
    let resp = server
        .cbor_request_raw_data(
            "PutRecord",
            &json!({
                "StreamName": "cross-bin",
                "Data": b64,
                "PartitionKey": "pk-cbor",
            }),
            "Data",
            &raw,
        )
        .await;
    assert_eq!(resp.status(), 200);

    // Read all via JSON
    let iter = server
        .get_shard_iterator("cross-bin", "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let resp = decode_body(
        server
            .request("GetRecords", &json!({"ShardIterator": iter}))
            .await,
    )
    .await;

    let records = resp.1["Records"].as_array().unwrap();
    assert_eq!(records.len(), 2);
    for r in records {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(r["Data"].as_str().unwrap())
            .unwrap();
        assert_eq!(decoded, raw, "Binary data mismatch via JSON read");
    }

    // Read all via CBOR
    let iter_resp = decode_body(
        server
            .cbor_request(
                "GetShardIterator",
                &json!({
                    "StreamName": "cross-bin",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            )
            .await,
    )
    .await;
    let iter = iter_resp.1["ShardIterator"].as_str().unwrap();
    let cbor_resp = decode_body(
        server
            .cbor_request("GetRecords", &json!({"ShardIterator": iter}))
            .await,
    )
    .await;

    let records = cbor_resp.1["Records"].as_array().unwrap();
    assert_eq!(records.len(), 2);
    for r in records {
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(r["Data"].as_str().unwrap())
            .unwrap();
        assert_eq!(decoded, raw, "Binary data mismatch via CBOR read");
    }
}

#[tokio::test]
async fn cross_large_data() {
    let server = TestServer::new().await;
    server.create_stream("cross-large", 1).await;

    // Near the 1MB limit: 1048576 - 256 bytes to stay under with some margin
    let raw = vec![0xAB_u8; 1048576 - 256];
    let b64 = base64::engine::general_purpose::STANDARD.encode(&raw);

    // Put via JSON
    let resp = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "cross-large",
                "Data": b64,
                "PartitionKey": "pk",
            }),
        )
        .await;
    assert_eq!(resp.status(), 200);

    // Read via CBOR
    let iter_resp = decode_body(
        server
            .cbor_request(
                "GetShardIterator",
                &json!({
                    "StreamName": "cross-large",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            )
            .await,
    )
    .await;
    let iter = iter_resp.1["ShardIterator"].as_str().unwrap();
    let cbor_resp = decode_body(
        server
            .cbor_request("GetRecords", &json!({"ShardIterator": iter}))
            .await,
    )
    .await;

    let records = cbor_resp.1["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(records[0]["Data"].as_str().unwrap())
        .unwrap();
    assert_eq!(decoded.len(), raw.len());
    assert_eq!(decoded, raw);
}

// ─── Group D: Error response equivalence ─────────────────────────────────────

#[tokio::test]
async fn equiv_error_resource_not_found() {
    let server = TestServer::new().await;
    let payload = json!({"StreamName": "nonexistent"});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("DescribeStream", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_eq!(json_status, 400);
    assert_eq!(json_body["__type"], cbor_body["__type"]);
    assert_eq!(json_body["message"], cbor_body["message"]);
}

#[tokio::test]
async fn equiv_error_validation() {
    let server = TestServer::new().await;
    // Missing required ShardCount
    let payload = json!({"StreamName": "test"});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("CreateStream", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_eq!(json_status, 400);
    assert_eq!(json_body["__type"], cbor_body["__type"]);
    assert_eq!(json_body["message"], cbor_body["message"]);
}

#[tokio::test]
async fn equiv_error_serialization() {
    let server = TestServer::new().await;
    // Wrong type: ShardCount should be integer, not string
    let payload = json!({"StreamName": "test", "ShardCount": "notanumber"});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("CreateStream", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_eq!(json_status, 400);
    assert_eq!(json_body["__type"], cbor_body["__type"]);
}

#[tokio::test]
async fn equiv_error_resource_in_use() {
    let server = TestServer::new().await;
    server.create_stream("dup-stream", 1).await;

    let payload = json!({"StreamName": "dup-stream", "ShardCount": 1});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("CreateStream", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_eq!(json_status, 400);
    assert_eq!(json_body["__type"], cbor_body["__type"]);
    assert_eq!(json_body["message"], cbor_body["message"]);
}

#[tokio::test]
async fn equiv_error_limit_exceeded() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 1,
        ..Default::default()
    })
    .await;
    server.create_stream("takes-all", 1).await;

    let payload = json!({"StreamName": "one-too-many", "ShardCount": 1});
    let ((json_status, json_body), (cbor_status, cbor_body)) =
        server.request_both("CreateStream", &payload).await;
    assert_eq!(json_status, cbor_status);
    assert_eq!(json_status, 400);
    assert_eq!(json_body["__type"], cbor_body["__type"]);
    assert_eq!(json_body["message"], cbor_body["message"]);
}
