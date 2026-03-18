mod common;

use common::*;
use ferrokinesis::store::StoreOptions;
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderValue};

fn default_options() -> StoreOptions {
    StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
    }
}

/// Build a minimal set of valid Kinesis request headers
fn kinesis_headers() -> HeaderMap {
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
    headers
}

// --- Tests for the axum DefaultBodyLimit layer ---

#[tokio::test]
async fn custom_limit_rejects_body_one_byte_over() {
    let limit = 512;
    let server = TestServer::with_body_limit(default_options(), limit).await;

    let body = vec![b'x'; limit + 1];
    let res = server
        .raw_request(Method::POST, "/", kinesis_headers(), body)
        .await;

    assert_eq!(res.status(), 413);
}

#[tokio::test]
async fn custom_limit_allows_body_at_exact_limit() {
    let limit = 512;
    let server = TestServer::with_body_limit(default_options(), limit).await;

    // Body exactly at the limit — axum's DefaultBodyLimit should pass it through
    let body = vec![b'{'; limit];
    let res = server
        .raw_request(Method::POST, "/", kinesis_headers(), body)
        .await;

    assert_ne!(res.status(), 413, "body at exact limit should not be rejected by DefaultBodyLimit");
}

#[tokio::test]
async fn custom_limit_allows_small_valid_body() {
    let limit = 1024;
    let server = TestServer::with_body_limit(default_options(), limit).await;

    let res = server
        .raw_request(Method::POST, "/", kinesis_headers(), b"{}".to_vec())
        .await;

    assert_ne!(res.status(), 413);
}

#[tokio::test]
async fn custom_limit_rejects_large_body_well_over_limit() {
    let limit = 1024; // 1 KB
    let server = TestServer::with_body_limit(default_options(), limit).await;

    let body = vec![b'x'; limit * 10];
    let res = server
        .raw_request(Method::POST, "/", kinesis_headers(), body)
        .await;

    assert_eq!(res.status(), 413);
}

// --- Default 7 MB limit (matching --max-request-body-mb default) ---

#[tokio::test]
async fn default_7mb_limit_rejects_body_over_7mb() {
    let limit = 7 * 1024 * 1024;
    let server = TestServer::with_body_limit(default_options(), limit).await;

    let body = vec![b'x'; limit + 1];
    let res = server
        .raw_request(Method::POST, "/", kinesis_headers(), body)
        .await;

    assert_eq!(res.status(), 413);
}

#[tokio::test]
async fn default_7mb_limit_allows_small_body() {
    let limit = 7 * 1024 * 1024;
    let server = TestServer::with_body_limit(default_options(), limit).await;

    let res = server
        .raw_request(Method::POST, "/", kinesis_headers(), b"{}".to_vec())
        .await;

    assert_ne!(res.status(), 413);
}

// --- Larger limit allows bodies that a smaller limit would reject ---

#[tokio::test]
async fn larger_limit_allows_body_rejected_by_smaller_limit() {
    // A body of 800 bytes is rejected by a 512-byte limit but accepted by a 1 KB limit.
    // This confirms that the configured limit is actually applied.
    let body = vec![b'x'; 800];

    let small_server = TestServer::with_body_limit(default_options(), 512).await;
    let res = small_server
        .raw_request(Method::POST, "/", kinesis_headers(), body.clone())
        .await;
    assert_eq!(res.status(), 413, "512-byte limit must reject 800-byte body");

    let large_server = TestServer::with_body_limit(default_options(), 1024).await;
    let res = large_server
        .raw_request(Method::POST, "/", kinesis_headers(), body)
        .await;
    assert_ne!(res.status(), 413, "1 KB limit must not reject 800-byte body at the axum layer");
}

// --- mb-to-bytes conversion correctness ---

#[tokio::test]
async fn limit_conversion_1mb_rejects_body_of_1mb_plus_1() {
    // 1 MB = 1_048_576 bytes
    let limit = 1_048_576usize;
    let server = TestServer::with_body_limit(default_options(), limit).await;

    let body = vec![b'x'; limit + 1];
    let res = server
        .raw_request(Method::POST, "/", kinesis_headers(), body)
        .await;

    assert_eq!(res.status(), 413);
}

#[tokio::test]
async fn limit_conversion_1mb_accepts_body_of_exactly_1mb() {
    let limit = 1_048_576usize;
    let server = TestServer::with_body_limit(default_options(), limit).await;

    let body = vec![b'x'; limit];
    let res = server
        .raw_request(Method::POST, "/", kinesis_headers(), body)
        .await;

    assert_ne!(res.status(), 413);
}
