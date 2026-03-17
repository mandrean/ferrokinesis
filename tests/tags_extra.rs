mod common;

use common::*;
use serde_json::{Value, json};

const ACCOUNT: &str = "0000-0000-0000";
const REGION: &str = "us-east-1";

fn stream_arn(name: &str) -> String {
    format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{name}")
}

// -- TagResource / UntagResource with non-stream ARN (consumer ARN) --

/// TagResource on a consumer ARN uses the resource-tags table path, not the stream path.
#[tokio::test]
async fn tag_resource_consumer_arn() {
    let server = TestServer::new().await;
    let name = "te-consumer-tag";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Register a consumer
    let consumer_arn = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "tag-me" }),
        )
        .await
        .json::<Value>()
        .await
        .unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

    // Tag the consumer (non-stream ARN path)
    let res = server
        .request(
            "TagResource",
            &json!({ "ResourceARN": consumer_arn, "Tags": {"env": "test"} }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Verify tags are retrievable
    let body: Value = server
        .request(
            "ListTagsForResource",
            &json!({ "ResourceARN": consumer_arn }),
        )
        .await
        .json()
        .await
        .unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert!(
        tags.iter()
            .any(|t| t["Key"] == "env" && t["Value"] == "test")
    );
}

#[tokio::test]
async fn untag_resource_consumer_arn() {
    let server = TestServer::new().await;
    let name = "te-consumer-untag";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let consumer_arn = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "untag-me" }),
        )
        .await
        .json::<Value>()
        .await
        .unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

    // Tag
    server
        .request(
            "TagResource",
            &json!({ "ResourceARN": consumer_arn, "Tags": {"a": "1", "b": "2"} }),
        )
        .await;

    // Untag
    let res = server
        .request(
            "UntagResource",
            &json!({ "ResourceARN": consumer_arn, "TagKeys": ["a"] }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let body: Value = server
        .request(
            "ListTagsForResource",
            &json!({ "ResourceARN": consumer_arn }),
        )
        .await
        .json()
        .await
        .unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert!(!tags.iter().any(|t| t["Key"] == "a"));
    assert!(tags.iter().any(|t| t["Key"] == "b"));
}

/// Tags with >50 entries on a non-stream ARN hit the resource table limit check.
#[tokio::test]
async fn tag_resource_consumer_arn_over_50() {
    let server = TestServer::new().await;
    let name = "te-consumer-limit";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let consumer_arn = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "limit-me" }),
        )
        .await
        .json::<Value>()
        .await
        .unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

    // Add 50 tags in batches of 10
    for batch in 0..5 {
        let mut tags = serde_json::Map::new();
        for i in 0..10 {
            tags.insert(format!("key{:02}", batch * 10 + i), json!("v"));
        }
        let res = server
            .request(
                "TagResource",
                &json!({ "ResourceARN": consumer_arn, "Tags": tags }),
            )
            .await;
        assert_eq!(res.status(), 200);
    }

    // 51st tag should fail
    let res = server
        .request(
            "TagResource",
            &json!({ "ResourceARN": consumer_arn, "Tags": {"extra": "tag"} }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

// -- Invalid tag characters --

#[tokio::test]
async fn tag_resource_invalid_key_chars() {
    let server = TestServer::new().await;
    let name = "te-invalid-key";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Key contains '!' which is not in the allowed set
    let res = server
        .request(
            "TagResource",
            &json!({ "ResourceARN": arn, "Tags": {"foo!bar": "value"} }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn tag_resource_invalid_value_chars() {
    let server = TestServer::new().await;
    let name = "te-invalid-val";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Value contains '&' which is not allowed
    let res = server
        .request(
            "TagResource",
            &json!({ "ResourceARN": arn, "Tags": {"key": "val&ue"} }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}
