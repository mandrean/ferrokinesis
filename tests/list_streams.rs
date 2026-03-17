mod common;

use common::*;
use serde_json::{Value, json};

#[tokio::test]
async fn list_streams_empty() {
    let server = TestServer::new().await;
    let res = server.request("ListStreams", &json!({})).await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["HasMoreStreams"], false);
    assert_eq!(body["StreamNames"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn list_streams_with_streams() {
    let server = TestServer::new().await;
    server.create_stream("aaa-stream", 1).await;
    server.create_stream("bbb-stream", 1).await;
    server.create_stream("ccc-stream", 1).await;

    let res = server.request("ListStreams", &json!({})).await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["HasMoreStreams"], false);
    let names: Vec<&str> = body["StreamNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(names.contains(&"aaa-stream"));
    assert!(names.contains(&"bbb-stream"));
    assert!(names.contains(&"ccc-stream"));
}

#[tokio::test]
async fn list_streams_with_limit() {
    let server = TestServer::new().await;
    server.create_stream("list-a", 1).await;
    server.create_stream("list-b", 1).await;
    server.create_stream("list-c", 1).await;

    let res = server.request("ListStreams", &json!({"Limit": 2})).await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["HasMoreStreams"], true);
    assert_eq!(body["StreamNames"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn list_streams_with_exclusive_start() {
    let server = TestServer::new().await;
    server.create_stream("ls-alpha", 1).await;
    server.create_stream("ls-beta", 1).await;
    server.create_stream("ls-gamma", 1).await;

    let res = server
        .request(
            "ListStreams",
            &json!({"ExclusiveStartStreamName": "ls-beta"}),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let names: Vec<&str> = body["StreamNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert!(!names.contains(&"ls-alpha"));
    assert!(!names.contains(&"ls-beta"));
    assert!(names.contains(&"ls-gamma"));
}

#[tokio::test]
async fn list_streams_pagination() {
    let server = TestServer::new().await;
    server.create_stream("page-a", 1).await;
    server.create_stream("page-b", 1).await;
    server.create_stream("page-c", 1).await;

    // First page
    let res = server.request("ListStreams", &json!({"Limit": 1})).await;
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["HasMoreStreams"], true);
    let first = body["StreamNames"][0].as_str().unwrap().to_string();

    // Second page
    let res = server
        .request(
            "ListStreams",
            &json!({"Limit": 1, "ExclusiveStartStreamName": first}),
        )
        .await;
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["HasMoreStreams"], true);
    let second = body["StreamNames"][0].as_str().unwrap().to_string();
    assert_ne!(first, second);

    // Third page
    let res = server
        .request(
            "ListStreams",
            &json!({"Limit": 1, "ExclusiveStartStreamName": second}),
        )
        .await;
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["HasMoreStreams"], false);
    assert_eq!(body["StreamNames"].as_array().unwrap().len(), 1);
}
