//! Error conformance test suite.
//!
//! Verifies that error responses match the exact format AWS SDKs expect:
//! HTTP status codes, `__type` values, message field casing, and the
//! `x-amzn-ErrorType` header.

mod common;

use common::*;
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Helper: assert an error response has the expected shape
// ---------------------------------------------------------------------------

fn assert_error_shape(body: &Value, expected_type: &str) {
    assert_eq!(
        body["__type"].as_str().unwrap(),
        expected_type,
        "expected __type={expected_type}, got {:?}",
        body["__type"]
    );
}

fn assert_has_error_type_header(res: &reqwest::Response, expected_type: &str) {
    let header = res
        .headers()
        .get("x-amzn-errortype")
        .expect("x-amzn-ErrorType header should be present")
        .to_str()
        .unwrap();
    assert_eq!(header, expected_type);
}

// ===========================================================================
// Status code + __type tests (JSON)
// ===========================================================================

#[tokio::test]
async fn error_resource_not_found_status_and_type() {
    let server = TestServer::new().await;
    let res = server
        .request("DescribeStream", &json!({"StreamName": "nonexistent"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "ResourceNotFoundException");
}

#[tokio::test]
async fn error_resource_in_use_status_and_type() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;

    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "test-stream", "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "ResourceInUseException");
}

#[tokio::test]
async fn error_invalid_argument_status_and_type() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "test-stream",
                "Data": "dGVzdA==",
                "PartitionKey": "key",
                "ExplicitHashKey": "999999999999999999999999999999999999999999"
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "InvalidArgumentException");
}

#[tokio::test]
async fn error_limit_exceeded_status_and_type() {
    let server = TestServer::with_options(ferrokinesis::store::StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 2,
        ..Default::default()
    })
    .await;

    // Create a stream with 2 shards (at the limit)
    server.create_stream("s1", 2).await;

    // Try to create another stream (should exceed limit)
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "s2", "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "LimitExceededException");
}

#[tokio::test]
async fn error_expired_iterator_status_and_type() {
    let server = TestServer::with_options(ferrokinesis::store::StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        iterator_ttl_seconds: 0, // Immediate expiry
        ..Default::default()
    })
    .await;
    server.create_stream("test-stream", 1).await;

    let iterator = server
        .get_shard_iterator("test-stream", "shardId-000000000000", "TRIM_HORIZON")
        .await;

    // Wait a moment so the iterator expires (TTL=0)
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = server
        .request("GetRecords", &json!({"ShardIterator": iterator}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "ExpiredIteratorException");
}

#[tokio::test]
async fn error_serialization_exception_status_and_type() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_static(
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        ),
    );
    headers.insert("X-Amz-Date", HeaderValue::from_static("20150101T000000Z"));
    let res = server
        .raw_request(Method::POST, "/", headers, b"not-json".to_vec())
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "SerializationException");
}

#[tokio::test]
async fn error_validation_exception_status_and_type() {
    let server = TestServer::new().await;
    // 129-char StreamName exceeds the 128-char len_lte validation → ValidationException
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "a".repeat(129), "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "ValidationException");
}

#[tokio::test]
async fn error_unknown_operation_status_and_type() {
    let server = TestServer::new().await;
    let mut headers = signed_headers();
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.FakeOperation"),
    );
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "UnknownOperationException");
}

#[tokio::test]
async fn error_missing_auth_token_status_and_type() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "MissingAuthenticationTokenException");
}

#[tokio::test]
async fn error_incomplete_signature_status_403() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    headers.insert("Authorization", HeaderValue::from_static("X"));
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 403);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "IncompleteSignatureException");
}

#[tokio::test]
async fn error_invalid_signature_status_and_type() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    headers.insert("Authorization", HeaderValue::from_static("X"));
    let res = server
        .raw_request(Method::POST, "/?X-Amz-Algorithm", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "InvalidSignatureException");
}

// ===========================================================================
// Message casing tests
// ===========================================================================

#[tokio::test]
async fn message_casing_lowercase_for_resource_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request("DescribeStream", &json!({"StreamName": "nonexistent"}))
        .await;
    let body: Value = res.json().await.unwrap();
    assert!(
        body.get("message").is_some(),
        "ResourceNotFoundException should have lowercase 'message'"
    );
    assert!(
        body.get("Message").is_none(),
        "ResourceNotFoundException should NOT have uppercase 'Message'"
    );
}

#[tokio::test]
async fn message_casing_lowercase_for_missing_auth_token() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    let body: Value = res.json().await.unwrap();
    assert!(
        body.get("message").is_some(),
        "MissingAuthenticationTokenException should have lowercase 'message'"
    );
    assert!(
        body.get("Message").is_none(),
        "MissingAuthenticationTokenException should NOT have uppercase 'Message'"
    );
}

#[tokio::test]
async fn message_casing_lowercase_for_invalid_argument() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "test-stream",
                "Data": "dGVzdA==",
                "PartitionKey": "key",
                "ExplicitHashKey": "999999999999999999999999999999999999999999"
            }),
        )
        .await;
    let body: Value = res.json().await.unwrap();
    assert!(
        body.get("message").is_some(),
        "InvalidArgumentException should have lowercase 'message'"
    );
    assert!(
        body.get("Message").is_none(),
        "InvalidArgumentException should NOT have uppercase 'Message'"
    );
}

#[tokio::test]
async fn message_casing_uppercase_for_serialization_exception() {
    let server = TestServer::new().await;
    // Trigger a serialization error from the validation layer (e.g., wrong type for a field)
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": 12345, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "SerializationException");
    assert!(
        body.get("Message").is_some(),
        "SerializationException should have uppercase 'Message'"
    );
    assert!(
        body.get("message").is_none(),
        "SerializationException should NOT have lowercase 'message'"
    );
}

// ===========================================================================
// x-amzn-ErrorType header tests
// ===========================================================================

#[tokio::test]
async fn error_type_header_on_resource_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request("DescribeStream", &json!({"StreamName": "nonexistent"}))
        .await;
    assert_has_error_type_header(&res, "ResourceNotFoundException");
}

#[tokio::test]
async fn error_type_header_on_resource_in_use() {
    let server = TestServer::new().await;
    server.create_stream("test-stream", 1).await;
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "test-stream", "ShardCount": 1}),
        )
        .await;
    assert_has_error_type_header(&res, "ResourceInUseException");
}

#[tokio::test]
async fn error_type_header_on_missing_auth_token() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_has_error_type_header(&res, "MissingAuthenticationTokenException");
}

#[tokio::test]
async fn error_type_header_on_incomplete_signature() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    headers.insert("Authorization", HeaderValue::from_static("X"));
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_has_error_type_header(&res, "IncompleteSignatureException");
}

#[tokio::test]
async fn error_type_header_on_invalid_signature() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    headers.insert("Authorization", HeaderValue::from_static("X"));
    let res = server
        .raw_request(Method::POST, "/?X-Amz-Algorithm", headers, b"{}".to_vec())
        .await;
    assert_has_error_type_header(&res, "InvalidSignatureException");
}

#[tokio::test]
async fn error_type_header_on_unknown_operation() {
    let server = TestServer::new().await;
    let mut headers = signed_headers();
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.FakeOperation"),
    );
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_has_error_type_header(&res, "UnknownOperationException");
}

/// Auth headers are included so this test is isolated to body-emptiness
/// handling only (mirrors `cbor_error_serialization_exception_no_body`).
#[tokio::test]
async fn error_type_header_on_serialization_exception() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_static(
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        ),
    );
    headers.insert("X-Amz-Date", HeaderValue::from_static("20150101T000000Z"));
    let res = server.raw_request(Method::POST, "/", headers, vec![]).await;
    assert_has_error_type_header(&res, "SerializationException");
}

#[tokio::test]
async fn error_type_header_matches_body_type() {
    let server = TestServer::new().await;
    let res = server
        .request("DescribeStream", &json!({"StreamName": "nonexistent"}))
        .await;

    let header_type = res
        .headers()
        .get("x-amzn-errortype")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let body: Value = res.json().await.unwrap();
    let body_type = body["__type"].as_str().unwrap();

    assert_eq!(header_type, body_type);
}

// ===========================================================================
// CBOR error tests
// ===========================================================================

#[tokio::test]
async fn cbor_error_resource_not_found() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_CBOR));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.DescribeStream"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_static(
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        ),
    );
    headers.insert("X-Amz-Date", HeaderValue::from_static("20150101T000000Z"));

    let mut cbor_body = Vec::new();
    ciborium::into_writer(&json!({"StreamName": "nonexistent"}), &mut cbor_body).unwrap();

    let res = server
        .raw_request(Method::POST, "/", headers, cbor_body)
        .await;
    assert_eq!(res.status(), 400);

    let ct = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, AMZ_CBOR);

    let error_type_header = res
        .headers()
        .get("x-amzn-errortype")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    let bytes = res.bytes().await.unwrap();
    let body: Value = ciborium::from_reader(&bytes[..]).unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
    assert_eq!(error_type_header, "ResourceNotFoundException");
    assert!(body.get("message").is_some());
}

#[tokio::test]
async fn cbor_error_resource_in_use() {
    let server = TestServer::new().await;
    server.create_stream("dup-stream", 1).await;

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_CBOR));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.CreateStream"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_static(
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        ),
    );
    headers.insert("X-Amz-Date", HeaderValue::from_static("20150101T000000Z"));

    let mut cbor_body = Vec::new();
    ciborium::into_writer(
        &json!({"StreamName": "dup-stream", "ShardCount": 1}),
        &mut cbor_body,
    )
    .unwrap();

    let res = server
        .raw_request(Method::POST, "/", headers, cbor_body)
        .await;
    assert_eq!(res.status(), 400);

    let bytes = res.bytes().await.unwrap();
    let body: Value = ciborium::from_reader(&bytes[..]).unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body.get("message").is_some());
}

/// Sending an empty body should return SerializationException regardless of
/// the target operation being valid.  Auth headers are included so this test
/// is isolated to body-emptiness handling only.
#[tokio::test]
async fn cbor_error_serialization_exception_no_body() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_CBOR));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_static(
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        ),
    );
    headers.insert("X-Amz-Date", HeaderValue::from_static("20150101T000000Z"));
    let res = server.raw_request(Method::POST, "/", headers, vec![]).await;
    let (status, body) = decode_body(res).await;
    assert_eq!(status, 400);
    assert_eq!(body["__type"], "SerializationException");
}

#[tokio::test]
async fn cbor_error_message_casing() {
    let server = TestServer::new().await;

    // CBOR ResourceNotFoundException should use lowercase "message"
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_CBOR));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.DescribeStream"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_static(
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        ),
    );
    headers.insert("X-Amz-Date", HeaderValue::from_static("20150101T000000Z"));

    let mut cbor_body = Vec::new();
    ciborium::into_writer(&json!({"StreamName": "nonexistent"}), &mut cbor_body).unwrap();

    let res = server
        .raw_request(Method::POST, "/", headers, cbor_body)
        .await;
    let bytes = res.bytes().await.unwrap();
    let body: Value = ciborium::from_reader(&bytes[..]).unwrap();

    assert!(
        body.get("message").is_some(),
        "CBOR ResourceNotFoundException should have lowercase 'message'"
    );
    assert!(
        body.get("Message").is_none(),
        "CBOR ResourceNotFoundException should NOT have uppercase 'Message'"
    );
}

// ===========================================================================
// Error message content tests
// ===========================================================================

#[tokio::test]
async fn error_message_contains_stream_name_and_account_id() {
    let server = TestServer::new().await;
    let res = server
        .request("DescribeStream", &json!({"StreamName": "my-test-stream"}))
        .await;
    let body: Value = res.json().await.unwrap();
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains("my-test-stream"),
        "Error message should contain stream name"
    );
    assert!(
        message.contains("000000000000"),
        "Error message should contain account ID"
    );
}

#[tokio::test]
async fn error_message_stream_not_found_format() {
    let server = TestServer::new().await;
    let res = server
        .request("DescribeStream", &json!({"StreamName": "foo"}))
        .await;
    let body: Value = res.json().await.unwrap();
    assert_eq!(
        body["message"].as_str().unwrap(),
        "Stream foo under account 000000000000 not found."
    );
}

#[tokio::test]
async fn error_message_stream_already_exists_format() {
    let server = TestServer::new().await;
    server.create_stream("bar", 1).await;
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": "bar", "ShardCount": 1}),
        )
        .await;
    let body: Value = res.json().await.unwrap();
    assert_eq!(
        body["message"].as_str().unwrap(),
        "Stream bar under account 000000000000 already exists."
    );
}

// ===========================================================================
// Server error (500) conformance tests
// ===========================================================================

/// NOTE: This test relies on an internal implementation detail — passing
/// `StartingSequenceNumber: "0"` triggers a version-0 sequence parse failure
/// that surfaces as a 500.  If that code path is ever changed to return a 4xx,
/// this test will fail and a new way to provoke a 500 will be needed.
#[tokio::test]
async fn error_server_error_500_shape() {
    let server = TestServer::new().await;
    server.create_stream("test-500", 1).await;

    // AT_SEQUENCE_NUMBER with "0" triggers a version-0 sequence parse → 500
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "test-500",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": "0",
            }),
        )
        .await;

    assert_eq!(res.status(), 500);
    assert_has_error_type_header(&res, "InternalFailure");
    let body: Value = res.json().await.unwrap();
    assert_error_shape(&body, "InternalFailure");
    assert!(
        body.get("message").is_none(),
        "Server error with no message should omit 'message'"
    );
    assert!(
        body.get("Message").is_none(),
        "Server error should not use uppercase 'Message'"
    );
}
