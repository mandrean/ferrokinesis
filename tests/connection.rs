mod common;

use common::*;
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{Value, json};

// -- Basic connection tests --

#[tokio::test]
async fn cbor_unknown_operation_if_post_no_auth() {
    let server = TestServer::new().await;
    let res = server
        .raw_request(Method::POST, "/", HeaderMap::new(), vec![])
        .await;
    let (status, body) = decode_body(res).await;
    assert_eq!(status, 400);
    assert_eq!(body["__type"], "UnknownOperationException");
}

#[tokio::test]
async fn access_denied_if_get() {
    let server = TestServer::new().await;
    let res = server
        .raw_request(Method::GET, "/", signed_headers(), vec![])
        .await;
    assert_eq!(res.status(), 403);
    let body = res.text().await.unwrap();
    assert!(body.contains("AccessDeniedException"));
    assert!(body.contains("Unable to determine service/operation name to be authorized"));
}

#[tokio::test]
async fn access_denied_if_put() {
    let server = TestServer::new().await;
    let res = server
        .raw_request(Method::PUT, "/", signed_headers(), vec![])
        .await;
    assert_eq!(res.status(), 403);
    let body = res.text().await.unwrap();
    assert!(body.contains("AccessDeniedException"));
}

#[tokio::test]
async fn access_denied_if_delete() {
    let server = TestServer::new().await;
    let res = server
        .raw_request(Method::DELETE, "/", signed_headers(), vec![])
        .await;
    assert_eq!(res.status(), 403);
    let body = res.text().await.unwrap();
    assert!(body.contains("AccessDeniedException"));
}

#[tokio::test]
async fn access_denied_if_body_and_no_content_type() {
    let server = TestServer::new().await;
    // No content-type, no target, just signed - body is "{}" but no valid content type
    let mut headers = HeaderMap::new();
    headers.insert(
        "Authorization",
        HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=a, SignedHeaders=b, Signature=c"),
    );
    headers.insert("X-Amz-Date", HeaderValue::from_static("20150101T000000Z"));
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 403);
    let body = res.text().await.unwrap();
    assert!(body.contains("AccessDeniedException"));
}

#[tokio::test]
async fn cbor_unknown_operation_if_random_target() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("X-Amz-Target", HeaderValue::from_static("Whatever"));
    let res = server.raw_request(Method::POST, "/", headers, vec![]).await;
    let (status, body) = decode_body(res).await;
    assert_eq!(status, 400);
    assert_eq!(body["__type"], "UnknownOperationException");
}

#[tokio::test]
async fn cbor_unknown_operation_if_incomplete_action() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStream"),
    );
    let res = server.raw_request(Method::POST, "/", headers, vec![]).await;
    let (status, body) = decode_body(res).await;
    assert_eq!(status, 400);
    assert_eq!(body["__type"], "UnknownOperationException");
}

#[tokio::test]
async fn cbor_serialization_exception_if_no_content_type() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server.raw_request(Method::POST, "/", headers, vec![]).await;
    let (status, body) = decode_body(res).await;
    assert_eq!(status, 400);
    assert_eq!(body["__type"], "SerializationException");
}

// -- JSON connection tests --

#[tokio::test]
async fn json_unknown_operation_if_no_target() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    let res = server.raw_request(Method::POST, "/", headers, vec![]).await;
    assert_eq!(res.status(), 400);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, AMZ_JSON);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "UnknownOperationException");
}

#[tokio::test]
async fn json_serialization_exception_if_no_body() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server.raw_request(Method::POST, "/", headers, vec![]).await;
    assert_eq!(res.status(), 400);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, AMZ_JSON);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "SerializationException");
}

#[tokio::test]
async fn json_serialization_exception_if_non_json_body() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server
        .raw_request(Method::POST, "/", headers, b"hello".to_vec())
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "SerializationException");
}

#[tokio::test]
async fn json_missing_auth_token_if_no_auth() {
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
    assert_eq!(body["__type"], "MissingAuthenticationTokenException");
    assert_eq!(body["message"], "Missing Authentication Token");
}

#[tokio::test]
async fn json_incomplete_signature_if_invalid_auth() {
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
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "IncompleteSignatureException");
    let msg = body["message"].as_str().unwrap();
    assert!(msg.contains("Authorization header requires 'Credential' parameter."));
    assert!(msg.contains("Authorization header requires 'Signature' parameter."));
    assert!(msg.contains("Authorization header requires 'SignedHeaders' parameter."));
    assert!(msg.contains("Authorization=X"));
}

#[tokio::test]
async fn json_invalid_signature_if_both_auth_header_and_query() {
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
    assert_eq!(body["__type"], "InvalidSignatureException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("Found both 'X-Amz-Algorithm'")
    );
}

#[tokio::test]
async fn json_incomplete_signature_if_empty_query_params() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server
        .raw_request(Method::POST, "/?X-Amz-Algorithm", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "IncompleteSignatureException");
    let msg = body["message"].as_str().unwrap();
    assert!(msg.contains("AWS query-string parameters must include 'X-Amz-Algorithm'."));
    assert!(msg.contains("Re-examine the query-string parameters."));
}

#[tokio::test]
async fn json_incomplete_signature_if_missing_signed_headers_query() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server
        .raw_request(
            Method::POST,
            "/?X-Amz-Algorithm=a&X-Amz-Credential=b&X-Amz-Signature=c&X-Amz-Date=d",
            headers,
            b"{}".to_vec(),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "IncompleteSignatureException");
    let msg = body["message"].as_str().unwrap();
    assert!(msg.contains("AWS query-string parameters must include 'X-Amz-SignedHeaders'."));
    assert!(msg.contains("Re-examine the query-string parameters."));
}

// -- CORS tests --

#[tokio::test]
async fn cors_options_with_origin() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Origin", HeaderValue::from_static("whatever"));
    let res = server
        .raw_request(Method::OPTIONS, "/", headers, vec![])
        .await;
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        "*"
    );
    assert_eq!(
        res.headers()
            .get("access-control-max-age")
            .unwrap()
            .to_str()
            .unwrap(),
        "172800"
    );
    assert_eq!(
        res.headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap(),
        "0"
    );
    assert!(res.headers().get("x-amz-id-2").is_none());
}

#[tokio::test]
async fn cors_options_with_origin_and_request_headers() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Origin", HeaderValue::from_static("whatever"));
    headers.insert(
        "Access-Control-Request-Headers",
        HeaderValue::from_static("a, b, c"),
    );
    let res = server
        .raw_request(Method::OPTIONS, "/", headers, vec![])
        .await;
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.headers()
            .get("access-control-allow-headers")
            .unwrap()
            .to_str()
            .unwrap(),
        "a, b, c"
    );
}

#[tokio::test]
async fn cors_options_with_origin_and_request_method() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Origin", HeaderValue::from_static("whatever"));
    headers.insert(
        "Access-Control-Request-Headers",
        HeaderValue::from_static("a, b, c"),
    );
    headers.insert(
        "Access-Control-Request-Method",
        HeaderValue::from_static("d"),
    );
    let res = server
        .raw_request(Method::OPTIONS, "/", headers, vec![])
        .await;
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.headers()
            .get("access-control-allow-methods")
            .unwrap()
            .to_str()
            .unwrap(),
        "d"
    );
}

#[tokio::test]
async fn cors_expose_headers_on_post_with_origin() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Origin", HeaderValue::from_static("whatever"));
    let res = server.raw_request(Method::POST, "/", headers, vec![]).await;
    assert_eq!(
        res.headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        "*"
    );
    assert!(res.headers().get("access-control-expose-headers").is_some());
}

// -- Response header tests --

#[tokio::test]
async fn response_has_request_id_header() {
    let server = TestServer::new().await;
    let res = server.request("ListStreams", &json!({})).await;
    let request_id = res
        .headers()
        .get("x-amzn-requestid")
        .unwrap()
        .to_str()
        .unwrap();
    // UUID format
    assert!(
        regex::Regex::new(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$")
            .unwrap()
            .is_match(request_id)
    );
}

#[tokio::test]
async fn response_has_amz_id_2_header() {
    let server = TestServer::new().await;
    let res = server.request("ListStreams", &json!({})).await;
    let id2 = res.headers().get("x-amz-id-2").unwrap().to_str().unwrap();
    let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, id2).unwrap();
    assert!(decoded.len() >= 64);
}

// -- Deprecated application/json content type tests --

#[tokio::test]
async fn deprecated_json_serialization_exception_on_invalid_body() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server
        .raw_request(Method::POST, "/", headers, b"hello".to_vec())
        .await;
    assert_eq!(res.status(), 400);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert_eq!(ct, "application/json");
    let body: Value = res.json().await.unwrap();
    assert_eq!(
        body["Output"]["__type"],
        "com.amazon.coral.service#SerializationException"
    );
    assert_eq!(body["Version"], "1.0");
}

#[tokio::test]
async fn deprecated_json_unknown_operation_on_valid_body() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static("application/json"));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 404);
    let body: Value = res.json().await.unwrap();
    assert_eq!(
        body["Output"]["__type"],
        "com.amazon.coral.service#UnknownOperationException"
    );
    assert_eq!(body["Version"], "1.0");
}

// -- Partial auth header tests --

#[tokio::test]
async fn incomplete_signature_missing_signed_headers() {
    let server = TestServer::new().await;
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_static("AWS4- Signature=b Credential=a"),
    );
    headers.insert("Date", HeaderValue::from_static("a"));
    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "IncompleteSignatureException");
    let msg = body["message"].as_str().unwrap();
    // Should only complain about missing SignedHeaders (Credential and Signature are present)
    assert!(msg.contains("Authorization header requires 'SignedHeaders' parameter."));
    assert!(!msg.contains("'Credential' parameter"));
    assert!(!msg.contains("'Signature' parameter"));
}

#[tokio::test]
async fn server_body_too_large_returns_413() {
    let limit = 7 * 1024 * 1024;
    let server = TestServer::with_body_limit(
        ferrokinesis::store::StoreOptions {
            create_stream_ms: 0,
            delete_stream_ms: 0,
            update_stream_ms: 0,
            shard_limit: 50,
            ..Default::default()
        },
        limit,
    )
    .await;

    let huge_body = vec![b'x'; limit + 1];

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
        .raw_request(Method::POST, "/", headers, huge_body)
        .await;
    let (status, body) = decode_body(res).await;
    assert_eq!(status, 413);
    assert_eq!(body["__type"], "SerializationException");
    assert!(body["Message"].as_str().is_some());
}

#[tokio::test]
async fn server_invalid_content_type_with_valid_target() {
    let server = TestServer::new().await;

    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static("text/plain"));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );

    let res = server
        .raw_request(Method::POST, "/", headers, b"{}".to_vec())
        .await;
    assert_eq!(res.status(), 404);
    let body_text = res.text().await.unwrap();
    assert!(body_text.contains("UnknownOperationException"));
}
