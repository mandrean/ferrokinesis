mod common;

use common::*;
use serde_json::{Value, json};

#[tokio::test]
async fn increase_retention_period() {
    let server = TestServer::new().await;
    let name = "test-increase-retention";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "IncreaseStreamRetentionPeriod",
            &json!({
                "StreamName": name,
                "RetentionPeriodHours": 48,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let desc = server.describe_stream(name).await;
    assert_eq!(desc["StreamDescription"]["RetentionPeriodHours"], 48);
}

#[tokio::test]
async fn increase_retention_period_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "IncreaseStreamRetentionPeriod",
            &json!({
                "StreamName": "nonexistent",
                "RetentionPeriodHours": 48,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn increase_retention_period_below_current() {
    let server = TestServer::new().await;
    let name = "test-increase-below";
    server.create_stream(name, 1).await;

    // Try to "increase" to 12 hours (below current 24)
    let res = server
        .request(
            "IncreaseStreamRetentionPeriod",
            &json!({
                "StreamName": name,
                "RetentionPeriodHours": 12,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn decrease_retention_period() {
    let server = TestServer::new().await;
    let name = "test-decrease-retention";
    server.create_stream(name, 1).await;

    // First increase to 48
    server
        .request(
            "IncreaseStreamRetentionPeriod",
            &json!({
                "StreamName": name,
                "RetentionPeriodHours": 48,
            }),
        )
        .await;

    // Then decrease to 24
    let res = server
        .request(
            "DecreaseStreamRetentionPeriod",
            &json!({
                "StreamName": name,
                "RetentionPeriodHours": 24,
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let desc = server.describe_stream(name).await;
    assert_eq!(desc["StreamDescription"]["RetentionPeriodHours"], 24);
}

#[tokio::test]
async fn decrease_retention_period_below_24() {
    let server = TestServer::new().await;
    let name = "test-decrease-below-24";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "DecreaseStreamRetentionPeriod",
            &json!({
                "StreamName": name,
                "RetentionPeriodHours": 12,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn decrease_retention_period_above_current() {
    let server = TestServer::new().await;
    let name = "test-decrease-above";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "DecreaseStreamRetentionPeriod",
            &json!({
                "StreamName": name,
                "RetentionPeriodHours": 48,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn decrease_retention_period_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "DecreaseStreamRetentionPeriod",
            &json!({
                "StreamName": "nonexistent",
                "RetentionPeriodHours": 24,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn increase_retention_validation_missing_fields() {
    let server = TestServer::new().await;
    let res = server
        .request("IncreaseStreamRetentionPeriod", &json!({}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn decrease_retention_validation_missing_fields() {
    let server = TestServer::new().await;
    let res = server
        .request("DecreaseStreamRetentionPeriod", &json!({}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}
