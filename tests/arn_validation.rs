mod common;

use common::*;
use serde_json::{Value, json};

// -- StreamARN validation --

#[tokio::test]
async fn valid_stream_arn_accepted() {
    let server = TestServer::new().await;
    server.create_stream("arn-valid", 1).await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/arn-valid",
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn stream_arn_missing_stream_prefix_rejected() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:my-stream",
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn stream_arn_wrong_service_rejected() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamARN": "arn:aws:s3:us-east-1:000000000000:stream/my-stream",
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn stream_arn_non_numeric_account_rejected() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:abcdefghijkl:stream/my-stream",
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn stream_arn_short_account_rejected() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:12345:stream/my-stream",
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn stream_arn_govcloud_partition_accepted() {
    let server = TestServer::new().await;
    server.create_stream("arn-gov", 1).await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamARN": "arn:aws-us-gov:kinesis:us-gov-west-1:000000000000:stream/arn-gov",
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    // Should pass validation (may fail at stream lookup, but not with ValidationException)
    let body: Value = res.json().await.unwrap();
    assert_ne!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn stream_arn_china_partition_accepted() {
    let server = TestServer::new().await;
    server.create_stream("arn-cn", 1).await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamARN": "arn:aws-cn:kinesis:cn-north-1:000000000000:stream/arn-cn",
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    let body: Value = res.json().await.unwrap();
    assert_ne!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn stream_arn_exceeding_max_length_rejected() {
    let server = TestServer::new().await;
    // Build a valid-shaped ARN that exceeds the 2048-char limit
    let long_name = "x".repeat(2048);
    let arn = format!("arn:aws:kinesis:us-east-1:000000000000:stream/{long_name}");
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamARN": arn,
                "Data": "dGVzdA==",
                "PartitionKey": "key1",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

// -- ResourceARN validation --

#[tokio::test]
async fn resource_arn_accepts_stream_arn() {
    let server = TestServer::new().await;
    server.create_stream("res-arn-stream", 1).await;
    let res = server
        .request(
            "TagResource",
            &json!({
                "ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/res-arn-stream",
                "Tags": {"env": "test"},
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn resource_arn_accepts_consumer_arn() {
    let server = TestServer::new().await;
    server.create_stream("res-arn-cons", 1).await;
    // Register a consumer to get a valid consumer ARN
    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/res-arn-cons",
                "ConsumerName": "my-consumer",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let consumer_arn = body["Consumer"]["ConsumerARN"].as_str().unwrap();

    let res = server
        .request(
            "TagResource",
            &json!({
                "ResourceARN": consumer_arn,
                "Tags": {"env": "test"},
            }),
        )
        .await;
    // Should pass validation (consumer ARN matches resource_arn pattern)
    // TagResource returns 200 with empty body on success
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn resource_arn_rejects_non_kinesis_arn() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "TagResource",
            &json!({
                "ResourceARN": "arn:aws:s3:::my-bucket",
                "Tags": {"env": "test"},
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

// -- ConsumerARN validation --

#[tokio::test]
async fn consumer_arn_valid_format_accepted() {
    let server = TestServer::new().await;
    server.create_stream("cons-arn-valid", 1).await;
    let res = server
        .request(
            "DeregisterStreamConsumer",
            &json!({
                "ConsumerARN": "arn:aws:kinesis:us-east-1:000000000000:stream/cons-arn-valid/consumer/my-consumer:1700000000",
            }),
        )
        .await;
    // Should pass validation (may fail at consumer lookup, but not with ValidationException)
    let body: Value = res.json().await.unwrap();
    assert_ne!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn consumer_arn_missing_consumer_segment_rejected() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "DeregisterStreamConsumer",
            &json!({
                "ConsumerARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn consumer_arn_missing_timestamp_rejected() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "DeregisterStreamConsumer",
            &json!({
                "ConsumerARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s/consumer/c",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn consumer_arn_garbage_rejected() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "DeregisterStreamConsumer",
            &json!({
                "ConsumerARN": "not-an-arn-at-all",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}
