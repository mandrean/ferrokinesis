mod common;

use common::*;
use ferrokinesis::store::{Store, StoreOptions};
use serde_json::{Value, json};

const ACCOUNT: &str = "000000000000";
const REGION: &str = "us-east-1";

fn stream_arn(name: &str) -> String {
    format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{name}")
}

// -- TagResource (stream ARN) --

#[tokio::test]
async fn tag_resource_stream_success() {
    let server = TestServer::new().await;
    let name = "tr-stream-tag";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "TagResource",
            &json!({ "ResourceARN": arn, "Tags": {"env": "test", "team": "infra"} }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let res = server
        .request("ListTagsForResource", &json!({ "ResourceARN": arn }))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert!(
        tags.iter()
            .any(|t| t["Key"] == "env" && t["Value"] == "test")
    );
    assert!(
        tags.iter()
            .any(|t| t["Key"] == "team" && t["Value"] == "infra")
    );
}

#[tokio::test]
async fn tag_resource_missing_arn() {
    let server = TestServer::new().await;
    let res = server
        .request("TagResource", &json!({ "Tags": {"k": "v"} }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn tag_resource_missing_tags() {
    let server = TestServer::new().await;
    let name = "tr-no-tags";
    server.create_stream(name, 1).await;
    let res = server
        .request("TagResource", &json!({ "ResourceARN": stream_arn(name) }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn tag_resource_over_50_limit() {
    let server = TestServer::new().await;
    let name = "tr-limit";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Add 50 tags in 5 batches of 10
    for batch in 0..5 {
        let mut tags = serde_json::Map::new();
        for i in 0..10 {
            tags.insert(format!("key{:02}", batch * 10 + i), json!("v"));
        }
        let res = server
            .request("TagResource", &json!({ "ResourceARN": arn, "Tags": tags }))
            .await;
        assert_eq!(res.status(), 200);
    }

    let res = server
        .request(
            "TagResource",
            &json!({ "ResourceARN": arn, "Tags": {"extra": "tag"} }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn tag_resource_overwrites_existing() {
    let server = TestServer::new().await;
    let name = "tr-overwrite";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request(
            "TagResource",
            &json!({ "ResourceARN": arn, "Tags": {"k": "old"} }),
        )
        .await;
    server
        .request(
            "TagResource",
            &json!({ "ResourceARN": arn, "Tags": {"k": "new"} }),
        )
        .await;

    let body: Value = server
        .request("ListTagsForResource", &json!({ "ResourceARN": arn }))
        .await
        .json()
        .await
        .unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Key"], "k");
    assert_eq!(tags[0]["Value"], "new");
}

// -- UntagResource (stream ARN) --

#[tokio::test]
async fn untag_resource_stream_success() {
    let server = TestServer::new().await;
    let name = "tr-untag";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request(
            "TagResource",
            &json!({ "ResourceARN": arn, "Tags": {"a": "1", "b": "2", "c": "3"} }),
        )
        .await;

    let res = server
        .request(
            "UntagResource",
            &json!({ "ResourceARN": arn, "TagKeys": ["a", "c"] }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let body: Value = server
        .request("ListTagsForResource", &json!({ "ResourceARN": arn }))
        .await
        .json()
        .await
        .unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert!(!tags.iter().any(|t| t["Key"] == "a"));
    assert!(tags.iter().any(|t| t["Key"] == "b"));
    assert!(!tags.iter().any(|t| t["Key"] == "c"));
}

#[tokio::test]
async fn untag_resource_missing_arn() {
    let server = TestServer::new().await;
    let res = server
        .request("UntagResource", &json!({ "TagKeys": ["k"] }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn untag_resource_missing_tag_keys() {
    let server = TestServer::new().await;
    let name = "tr-untag-nokeys";
    server.create_stream(name, 1).await;
    let res = server
        .request("UntagResource", &json!({ "ResourceARN": stream_arn(name) }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn untag_resource_nonexistent_key_is_noop() {
    let server = TestServer::new().await;
    let name = "tr-untag-noop";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request(
            "TagResource",
            &json!({ "ResourceARN": arn, "Tags": {"k": "v"} }),
        )
        .await;

    let res = server
        .request(
            "UntagResource",
            &json!({ "ResourceARN": arn, "TagKeys": ["nonexistent"] }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let body: Value = server
        .request("ListTagsForResource", &json!({ "ResourceARN": arn }))
        .await
        .json()
        .await
        .unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert!(tags.iter().any(|t| t["Key"] == "k" && t["Value"] == "v"));
}

// -- ListTagsForResource --

#[tokio::test]
async fn list_tags_for_resource_empty() {
    let server = TestServer::new().await;
    let name = "tr-list-empty";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request("ListTagsForResource", &json!({ "ResourceARN": arn }))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["Tags"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn list_tags_for_resource_missing_arn() {
    let server = TestServer::new().await;
    let res = server.request("ListTagsForResource", &json!({})).await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn list_tags_for_resource_stream_arn() {
    let server = TestServer::new().await;
    let name = "test-ltfr-stream";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request(
            "AddTagsToStream",
            &json!({ "StreamName": name, "Tags": {"env": "prod"} }),
        )
        .await;

    let res = server
        .request("ListTagsForResource", &json!({ "ResourceARN": arn }))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert!(
        tags.iter()
            .any(|t| t["Key"] == "env" && t["Value"] == "prod")
    );
}

#[tokio::test]
async fn list_tags_for_resource_stream_arn_not_found() {
    let server = TestServer::new().await;
    let arn = stream_arn("test-ltfr-nofound");

    let res = server
        .request("ListTagsForResource", &json!({ "ResourceARN": arn }))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["Tags"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn untag_resource_stream_arn() {
    let server = TestServer::new().await;
    let name = "test-utr-stream";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request(
            "AddTagsToStream",
            &json!({ "StreamName": name, "Tags": {"key-a": "val-a", "key-b": "val-b"} }),
        )
        .await;

    let res = server
        .request(
            "UntagResource",
            &json!({ "ResourceARN": arn, "TagKeys": ["key-a"] }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let body: Value = server
        .request("ListTagsForStream", &json!({ "StreamName": name }))
        .await
        .json()
        .await
        .unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert!(!tags.iter().any(|t| t["Key"] == "key-a"));
    assert!(tags.iter().any(|t| t["Key"] == "key-b"));
}

#[tokio::test]
async fn list_tags_for_resource_empty_arn_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::list_tags_for_resource::execute(
        &store,
        json!({ "ResourceARN": "" }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "InvalidArgumentException"
    );
}

#[tokio::test]
async fn untag_resource_empty_arn_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::untag_resource::execute(
        &store,
        json!({ "ResourceARN": "", "TagKeys": ["key"] }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "InvalidArgumentException"
    );
}

#[tokio::test]
async fn untag_resource_tag_keys_not_array_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::untag_resource::execute(
        &store,
        json!({
            "ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
            "TagKeys": "not-an-array",
        }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "InvalidArgumentException"
    );
}

#[tokio::test]
async fn tag_resource_empty_arn_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::tag_resource::execute(
        &store,
        json!({ "ResourceARN": "", "Tags": {"key": "value"} }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "InvalidArgumentException"
    );
}

#[tokio::test]
async fn tag_resource_tags_not_object_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::tag_resource::execute(
        &store,
        json!({
            "ResourceARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
            "Tags": "not-an-object",
        }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        result.unwrap_err().body.error_type,
        "InvalidArgumentException"
    );
}

// -- TagResource / UntagResource with consumer ARN (non-stream path) --

/// TagResource on a consumer ARN uses the resource-tags table, not the stream path.
#[tokio::test]
async fn tag_resource_consumer_arn() {
    let server = TestServer::new().await;
    let name = "te-consumer-tag";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

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

    let res = server
        .request(
            "TagResource",
            &json!({ "ResourceARN": consumer_arn, "Tags": {"env": "test"} }),
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

    server
        .request(
            "TagResource",
            &json!({ "ResourceARN": consumer_arn, "Tags": {"a": "1", "b": "2"} }),
        )
        .await;

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

/// Tags beyond 50 on a non-stream ARN hit the resource table limit.
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

#[tokio::test]
async fn tag_resource_invalid_key_chars() {
    let server = TestServer::new().await;
    let name = "te-invalid-key";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

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
