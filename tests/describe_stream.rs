mod common;

use common::*;
use serde_json::{Value, json};

#[tokio::test]
async fn describe_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request("DescribeStream", &json!({"StreamName": "nonexistent"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
    assert!(body["message"].as_str().unwrap().contains("not found"));
}

#[tokio::test]
async fn describe_stream_validation_missing_name() {
    let server = TestServer::new().await;
    let res = server.request("DescribeStream", &json!({})).await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn describe_stream_full_response() {
    let server = TestServer::new().await;
    let name = "test-describe-full";
    server.create_stream(name, 2).await;

    let desc = server.describe_stream(name).await;
    let sd = &desc["StreamDescription"];

    assert_eq!(sd["StreamName"], name);
    assert_eq!(sd["StreamStatus"], "ACTIVE");
    assert_eq!(sd["RetentionPeriodHours"], 24);
    assert_eq!(sd["HasMoreShards"], false);
    assert_eq!(sd["EncryptionType"], "NONE");
    assert!(sd["StreamARN"].as_str().unwrap().contains(name));
    assert!(sd["StreamCreationTimestamp"].as_f64().unwrap() > 0.0);

    let shards = sd["Shards"].as_array().unwrap();
    assert_eq!(shards.len(), 2);

    // First shard starts at 0
    assert_eq!(shards[0]["HashKeyRange"]["StartingHashKey"], "0");
    // Last shard ends at 2^128 - 1
    assert_eq!(
        shards[1]["HashKeyRange"]["EndingHashKey"],
        "340282366920938463463374607431768211455"
    );

    // Each shard has a sequence number range
    for shard in shards {
        assert!(
            !shard["SequenceNumberRange"]["StartingSequenceNumber"]
                .as_str()
                .unwrap()
                .is_empty()
        );
        assert!(shard["SequenceNumberRange"]["EndingSequenceNumber"].is_null());
    }
}

#[tokio::test]
async fn describe_stream_serialization_error() {
    let server = TestServer::new().await;
    let res = server
        .request("DescribeStream", &json!({"StreamName": true}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "SerializationException");
}
