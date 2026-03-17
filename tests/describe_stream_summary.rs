mod common;

use common::*;
use serde_json::{Value, json};

#[tokio::test]
async fn describe_stream_summary_success() {
    let server = TestServer::new().await;
    let name = "test-summary";
    server.create_stream(name, 3).await;

    let res = server
        .request(
            "DescribeStreamSummary",
            &json!({"StreamName": name}),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let summary = &body["StreamDescriptionSummary"];

    assert_eq!(summary["StreamName"], name);
    assert_eq!(summary["StreamStatus"], "ACTIVE");
    assert_eq!(summary["RetentionPeriodHours"], 24);
    assert_eq!(summary["OpenShardCount"], 3);
    assert_eq!(summary["ConsumerCount"], 0);
    assert_eq!(summary["EncryptionType"], "NONE");
    assert!(summary["StreamARN"].as_str().unwrap().contains(name));
    assert!(summary["StreamCreationTimestamp"].as_f64().unwrap() > 0.0);
}

#[tokio::test]
async fn describe_stream_summary_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "DescribeStreamSummary",
            &json!({"StreamName": "nonexistent"}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn describe_stream_summary_validation() {
    let server = TestServer::new().await;
    let res = server
        .request("DescribeStreamSummary", &json!({}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}
