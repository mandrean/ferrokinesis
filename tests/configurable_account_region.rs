mod common;

use common::*;
use ferrokinesis::store::StoreOptions;
use serde_json::json;

#[tokio::test]
async fn custom_account_id_appears_in_stream_arn() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        aws_account_id: Some("123456789012".to_string()),
        aws_region: Some("eu-west-1".to_string()),
    })
    .await;

    server.create_stream("test-custom-arn", 1).await;
    let body = server.describe_stream("test-custom-arn").await;
    let arn = body["StreamDescription"]["StreamARN"].as_str().unwrap();

    assert_eq!(
        arn, "arn:aws:kinesis:eu-west-1:123456789012:stream/test-custom-arn",
        "ARN should use custom account ID and region"
    );
}

#[tokio::test]
async fn default_account_id_and_region_backward_compat() {
    let server = TestServer::new().await;

    server.create_stream("test-default-arn", 1).await;
    let body = server.describe_stream("test-default-arn").await;
    let arn = body["StreamDescription"]["StreamARN"].as_str().unwrap();

    assert!(
        arn.contains("us-east-1"),
        "Default region should be us-east-1, got: {arn}"
    );
    assert!(
        arn.contains("000000000000"),
        "Default account should be 000000000000, got: {arn}"
    );
}

#[tokio::test]
async fn custom_region_in_error_messages() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        aws_account_id: Some("999888777666".to_string()),
        ..Default::default()
    })
    .await;

    // Try to describe a non-existent stream — the error message includes the account ID
    let res = server
        .request("DescribeStream", &json!({"StreamName": "no-such-stream"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: serde_json::Value = res.json().await.unwrap();
    let msg = body["message"].as_str().unwrap_or("");
    assert!(
        msg.contains("999888777666"),
        "Error message should include custom account ID, got: {msg}"
    );
}

#[tokio::test]
async fn custom_region_in_consumer_arn() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        aws_account_id: Some("111222333444".to_string()),
        aws_region: Some("ap-southeast-1".to_string()),
    })
    .await;

    server.create_stream("test-consumer-arn", 1).await;

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({
                "StreamARN": "arn:aws:kinesis:ap-southeast-1:111222333444:stream/test-consumer-arn",
                "ConsumerName": "my-consumer",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    let consumer_arn = body["Consumer"]["ConsumerARN"].as_str().unwrap();

    assert!(
        consumer_arn.starts_with("arn:aws:kinesis:ap-southeast-1:111222333444:stream/test-consumer-arn/consumer/my-consumer:"),
        "Consumer ARN should use custom account/region, got: {consumer_arn}"
    );
}

#[tokio::test]
async fn account_id_strips_non_digits() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        aws_account_id: Some("1234-5678-9012".to_string()),
        aws_region: Some("us-west-2".to_string()),
    })
    .await;

    server.create_stream("test-strip-digits", 1).await;
    let body = server.describe_stream("test-strip-digits").await;
    let arn = body["StreamDescription"]["StreamARN"].as_str().unwrap();

    assert!(
        arn.contains("123456789012"),
        "Non-digit characters should be stripped from account ID, got: {arn}"
    );
}
