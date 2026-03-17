mod common;

use common::*;
use serde_json::{Value, json};

const ACCOUNT: &str = "0000-0000-0000";
const REGION: &str = "us-east-1";

fn stream_arn(name: &str) -> String {
    format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{name}")
}

/// Helper: register a consumer and wait for it to become Active.
async fn register_and_activate(server: &TestServer, stream_arn: &str, name: &str) -> String {
    let res = server
        .request(
            "RegisterStreamConsumer",
            &json!({ "StreamARN": stream_arn, "ConsumerName": name }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let consumer_arn = res.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(tokio::time::Duration::from_millis(600)).await;
    consumer_arn
}

/// Helper: send a subscribe request and assert it opens a streaming response.
async fn assert_subscribe_streams(server: &TestServer, consumer_arn: &str, request: serde_json::Value) {
    let mut response = server
        .client
        .post(server.url())
        .header("Content-Type", AMZ_JSON)
        .header("X-Amz-Target", format!("{VERSION}.SubscribeToShard"))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        )
        .header("X-Amz-Date", "20150101T000000Z")
        .body(serde_json::to_vec(&request).unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        200,
        "Expected streaming 200 for consumer {consumer_arn}"
    );
    let chunk = response.chunk().await.unwrap();
    assert!(chunk.is_some(), "expected initial event stream frame");
}

#[tokio::test]
async fn subscribe_latest_position() {
    let server = TestServer::new().await;
    let name = "se-latest";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);
    let consumer_arn = register_and_activate(&server, &arn, "c-latest").await;

    assert_subscribe_streams(
        &server,
        &consumer_arn,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": { "Type": "LATEST" },
        }),
    )
    .await;
}

#[tokio::test]
async fn subscribe_at_sequence_number_position() {
    let server = TestServer::new().await;
    let name = "se-at-seq";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Put a record to get a sequence number
    let put_body = server.put_record(name, "AAAA", "pk").await;
    let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();

    let consumer_arn = register_and_activate(&server, &arn, "c-at-seq").await;

    assert_subscribe_streams(
        &server,
        &consumer_arn,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": {
                "Type": "AT_SEQUENCE_NUMBER",
                "SequenceNumber": seq,
            },
        }),
    )
    .await;
}

#[tokio::test]
async fn subscribe_after_sequence_number_position() {
    let server = TestServer::new().await;
    let name = "se-after-seq";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let put_body = server.put_record(name, "AAAA", "pk").await;
    let seq = put_body["SequenceNumber"].as_str().unwrap().to_string();

    let consumer_arn = register_and_activate(&server, &arn, "c-after-seq").await;

    assert_subscribe_streams(
        &server,
        &consumer_arn,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": {
                "Type": "AFTER_SEQUENCE_NUMBER",
                "SequenceNumber": seq,
            },
        }),
    )
    .await;
}

#[tokio::test]
async fn subscribe_at_timestamp_position() {
    let server = TestServer::new().await;
    let name = "se-at-ts";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // Put a record first
    server.put_record(name, "AAAA", "pk").await;

    let consumer_arn = register_and_activate(&server, &arn, "c-at-ts").await;

    // Use a timestamp in the far past to get all records
    assert_subscribe_streams(
        &server,
        &consumer_arn,
        json!({
            "ConsumerARN": consumer_arn,
            "ShardId": "shardId-000000000000",
            "StartingPosition": {
                "Type": "AT_TIMESTAMP",
                "Timestamp": 1_000_000_000.0,
            },
        }),
    )
    .await;
}
