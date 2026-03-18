mod common;

use common::*;
use serde_json::{Value, json};

#[tokio::test]
async fn delete_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request("DeleteStream", &json!({"StreamName": "nonexistent-stream"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
    assert!(body["message"].as_str().unwrap().contains("not found"));
}

#[tokio::test]
async fn delete_stream_success() {
    let server = TestServer::new().await;
    let name = "test-delete-success";
    server.create_stream(name, 1).await;

    let res = server
        .request("DeleteStream", &json!({"StreamName": name}))
        .await;
    assert_eq!(res.status(), 200);

    // Wait for deletion
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Should be gone
    let res = server
        .request("DescribeStream", &json!({"StreamName": name}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn delete_stream_deleting_state() {
    let server = TestServer::with_options(ferrokinesis::store::StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 200,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    })
    .await;

    let name = "test-deleting-state";
    server.create_stream(name, 1).await;

    let res = server
        .request("DeleteStream", &json!({"StreamName": name}))
        .await;
    assert_eq!(res.status(), 200);

    // Should be DELETING
    let desc = server.describe_stream(name).await;
    assert_eq!(desc["StreamDescription"]["StreamStatus"], "DELETING");

    // Wait for deletion
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    // Should be gone
    let res = server
        .request("DescribeStream", &json!({"StreamName": name}))
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn delete_stream_validation_missing_name() {
    let server = TestServer::new().await;
    let res = server.request("DeleteStream", &json!({})).await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}
