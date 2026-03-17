mod common;

use common::*;
use serde_json::{Value, json};

// -- EnableEnhancedMonitoring --

#[tokio::test]
async fn enable_monitoring_specific_metric() {
    let server = TestServer::new().await;
    let name = "em-specific";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "EnableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["IncomingBytes"] }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["StreamName"], name);
    assert!(body["StreamARN"].as_str().is_some());
    // CurrentShardLevelMetrics was empty before
    assert_eq!(
        body["CurrentShardLevelMetrics"].as_array().unwrap().len(),
        0
    );
    let desired = body["DesiredShardLevelMetrics"].as_array().unwrap();
    assert!(desired.iter().any(|m| m == "IncomingBytes"));
}

#[tokio::test]
async fn enable_monitoring_add_to_existing() {
    let server = TestServer::new().await;
    let name = "em-additive";
    server.create_stream(name, 1).await;

    // Enable first metric
    server
        .request(
            "EnableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["IncomingBytes"] }),
        )
        .await;

    // Enable second metric — should accumulate
    let res = server
        .request(
            "EnableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["OutgoingBytes"] }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let current = body["CurrentShardLevelMetrics"].as_array().unwrap();
    assert!(current.iter().any(|m| m == "IncomingBytes"));
    let desired = body["DesiredShardLevelMetrics"].as_array().unwrap();
    assert!(desired.iter().any(|m| m == "IncomingBytes"));
    assert!(desired.iter().any(|m| m == "OutgoingBytes"));
}

#[tokio::test]
async fn enable_monitoring_all() {
    let server = TestServer::new().await;
    let name = "em-all";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "EnableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["ALL"] }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let desired = body["DesiredShardLevelMetrics"].as_array().unwrap();
    // ALL expands to 7 metrics
    assert_eq!(desired.len(), 7);
}

#[tokio::test]
async fn enable_monitoring_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "EnableEnhancedMonitoring",
            &json!({ "StreamName": "nonexistent", "ShardLevelMetrics": ["IncomingBytes"] }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn enable_monitoring_missing_metrics() {
    let server = TestServer::new().await;
    let name = "em-no-metrics";
    server.create_stream(name, 1).await;
    let res = server
        .request("EnableEnhancedMonitoring", &json!({ "StreamName": name }))
        .await;
    assert_eq!(res.status(), 400);
}

// -- DisableEnhancedMonitoring --

#[tokio::test]
async fn disable_monitoring_specific_metric() {
    let server = TestServer::new().await;
    let name = "dm-specific";
    server.create_stream(name, 1).await;

    // Enable two metrics first
    server
        .request(
            "EnableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["IncomingBytes", "OutgoingBytes"] }),
        )
        .await;

    // Disable one
    let res = server
        .request(
            "DisableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["IncomingBytes"] }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let desired = body["DesiredShardLevelMetrics"].as_array().unwrap();
    assert!(!desired.iter().any(|m| m == "IncomingBytes"));
    assert!(desired.iter().any(|m| m == "OutgoingBytes"));
}

#[tokio::test]
async fn disable_monitoring_all() {
    let server = TestServer::new().await;
    let name = "dm-all";
    server.create_stream(name, 1).await;

    // Enable everything
    server
        .request(
            "EnableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["ALL"] }),
        )
        .await;

    // Disable ALL
    let res = server
        .request(
            "DisableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["ALL"] }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let desired = body["DesiredShardLevelMetrics"].as_array().unwrap();
    assert_eq!(desired.len(), 0);
}

#[tokio::test]
async fn disable_monitoring_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "DisableEnhancedMonitoring",
            &json!({ "StreamName": "nonexistent", "ShardLevelMetrics": ["IncomingBytes"] }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn disable_monitoring_missing_metrics() {
    let server = TestServer::new().await;
    let name = "dm-no-metrics";
    server.create_stream(name, 1).await;
    let res = server
        .request("DisableEnhancedMonitoring", &json!({ "StreamName": name }))
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn monitoring_full_lifecycle() {
    let server = TestServer::new().await;
    let name = "em-lifecycle";
    server.create_stream(name, 1).await;

    // Enable IncomingRecords and OutgoingRecords
    server
        .request(
            "EnableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["IncomingRecords", "OutgoingRecords"] }),
        )
        .await;

    // Disable IncomingRecords
    let body: Value = server
        .request(
            "DisableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["IncomingRecords"] }),
        )
        .await
        .json()
        .await
        .unwrap();

    let desired = body["DesiredShardLevelMetrics"].as_array().unwrap();
    assert!(!desired.iter().any(|m| m == "IncomingRecords"));
    assert!(desired.iter().any(|m| m == "OutgoingRecords"));

    // Disable ALL — clears remaining
    let body: Value = server
        .request(
            "DisableEnhancedMonitoring",
            &json!({ "StreamName": name, "ShardLevelMetrics": ["ALL"] }),
        )
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(
        body["DesiredShardLevelMetrics"].as_array().unwrap().len(),
        0
    );
}
