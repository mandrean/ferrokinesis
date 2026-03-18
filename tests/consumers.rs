mod common;

use common::*;
use serde_json::{Value, json};

const ACCOUNT: &str = "000000000000";
const REGION: &str = "us-east-1";

fn stream_arn(name: &str) -> String {
    format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{name}")
}

// -- DescribeLimits --

#[tokio::test]
async fn describe_limits_empty() {
    let server = TestServer::new().await;
    let res = server.request("DescribeLimits", &json!({})).await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["ShardLimit"], 50);
    assert_eq!(body["OpenShardCount"], 0);
    assert!(body["OnDemandStreamCount"].is_number());
    assert!(body["OnDemandStreamCountLimit"].is_number());
}

#[tokio::test]
async fn describe_limits_counts_open_shards() {
    let server = TestServer::new().await;
    server.create_stream("lim-a", 2).await;
    server.create_stream("lim-b", 3).await;

    let body: Value = server
        .request("DescribeLimits", &json!({}))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["OpenShardCount"], 5);
}

// -- DescribeStreamConsumer --

#[tokio::test]
async fn describe_consumer_by_arn() {
    let server = TestServer::new().await;
    let name = "dsc-by-arn";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c1" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .request(
            "DescribeStreamConsumer",
            &json!({ "ConsumerARN": consumer_arn }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["ConsumerDescription"]["ConsumerName"], "c1");
    assert_eq!(body["ConsumerDescription"]["ConsumerARN"], consumer_arn);
}

#[tokio::test]
async fn describe_consumer_by_name_and_stream() {
    let server = TestServer::new().await;
    let name = "dsc-by-name";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c2" }),
        )
        .await;

    let res = server
        .request(
            "DescribeStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c2" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["ConsumerDescription"]["ConsumerName"], "c2");
}

#[tokio::test]
async fn describe_consumer_not_found() {
    let server = TestServer::new().await;
    let fake_arn = format!("{}/consumer/nobody:1700000000", stream_arn("dsc-missing"));
    let res = server
        .request(
            "DescribeStreamConsumer",
            &json!({ "ConsumerARN": fake_arn }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn describe_consumer_missing_identifiers() {
    let server = TestServer::new().await;
    let res = server.request("DescribeStreamConsumer", &json!({})).await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

// -- DeregisterStreamConsumer --

#[tokio::test]
async fn deregister_consumer_by_arn() {
    let server = TestServer::new().await;
    let name = "drg-by-arn";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c3" }),
        )
        .await;
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

    let res = server
        .request(
            "DeregisterStreamConsumer",
            &json!({ "ConsumerARN": consumer_arn }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn deregister_consumer_by_name_and_stream() {
    let server = TestServer::new().await;
    let name = "drg-by-name";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c4" }),
        )
        .await;

    let res = server
        .request(
            "DeregisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c4" }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn deregister_consumer_not_found_by_arn() {
    let server = TestServer::new().await;
    let fake_arn = format!("{}/consumer/nobody:1700000000", stream_arn("drg-missing"));
    let res = server
        .request(
            "DeregisterStreamConsumer",
            &json!({ "ConsumerARN": fake_arn }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn deregister_consumer_not_found_by_name() {
    let server = TestServer::new().await;
    let name = "drg-name-missing";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "DeregisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "nonexistent" }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn deregister_consumer_missing_identifiers() {
    let server = TestServer::new().await;
    let res = server.request("DeregisterStreamConsumer", &json!({})).await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn consumer_full_lifecycle() {
    let server = TestServer::new().await;
    let name = "c-lifecycle";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Register
    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "lc" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();

    // Describe
    let body: Value = server
        .request(
            "DescribeStreamConsumer",
            &json!({ "ConsumerARN": consumer_arn }),
        )
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["ConsumerDescription"]["ConsumerARN"], consumer_arn);

    // Deregister
    let res = server
        .request(
            "DeregisterStreamConsumer",
            &json!({ "ConsumerARN": consumer_arn }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn register_consumer_already_exists() {
    let server = TestServer::new().await;
    let name = "test-rsc-exists";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c-dup" }),
        )
        .await;
    assert_eq!(res.status(), 200);
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c-dup" }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("already exists"));
}

#[tokio::test]
async fn register_consumer_limit_exceeded() {
    let server = TestServer::new().await;
    let name = "test-rsc-limit";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    for i in 0..20 {
        let res = server
            .request(
                "RegisterStreamConsumer",
                &json!({ "StreamARN": arn, "ConsumerName": format!("c-{i:02}") }),
            )
            .await;
        assert_eq!(
            res.status(),
            200,
            "consumer {i} registration should succeed"
        );
    }

    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": arn, "ConsumerName": "c-21" }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "LimitExceededException");
}
