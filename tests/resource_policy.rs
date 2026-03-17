mod common;

use common::*;
use serde_json::{json, Value};

const ACCOUNT: &str = "0000-0000-0000";
const REGION: &str = "us-east-1";

fn stream_arn(name: &str) -> String {
    format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{name}")
}

const POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"kinesis:*","Resource":"*"}]}"#;

// -- PutResourcePolicy --

#[tokio::test]
async fn put_resource_policy_success() {
    let server = TestServer::new().await;
    let name = "rp-put";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "PutResourcePolicy",
            &json!({ "ResourceARN": arn, "Policy": POLICY }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn put_resource_policy_missing_arn() {
    let server = TestServer::new().await;
    let res = server
        .request("PutResourcePolicy", &json!({ "Policy": POLICY }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn put_resource_policy_invalid_json() {
    let server = TestServer::new().await;
    let name = "rp-invalid-json";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "PutResourcePolicy",
            &json!({ "ResourceARN": stream_arn(name), "Policy": "not-valid-json{" }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

// -- GetResourcePolicy --

#[tokio::test]
async fn get_resource_policy_after_put() {
    let server = TestServer::new().await;
    let name = "rp-get";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request("PutResourcePolicy", &json!({ "ResourceARN": arn, "Policy": POLICY }))
        .await;

    let res = server
        .request("GetResourcePolicy", &json!({ "ResourceARN": arn }))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["Policy"].as_str().unwrap(), POLICY);
}

#[tokio::test]
async fn get_resource_policy_empty_returns_empty_string() {
    let server = TestServer::new().await;
    let name = "rp-get-empty";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request("GetResourcePolicy", &json!({ "ResourceARN": arn }))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    // No policy stored yet — returns empty string
    assert_eq!(body["Policy"].as_str().unwrap_or(""), "");
}

#[tokio::test]
async fn get_resource_policy_missing_arn() {
    let server = TestServer::new().await;
    let res = server
        .request("GetResourcePolicy", &json!({}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

// -- DeleteResourcePolicy --

#[tokio::test]
async fn delete_resource_policy_success() {
    let server = TestServer::new().await;
    let name = "rp-delete";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request("PutResourcePolicy", &json!({ "ResourceARN": arn, "Policy": POLICY }))
        .await;

    let res = server
        .request("DeleteResourcePolicy", &json!({ "ResourceARN": arn }))
        .await;
    assert_eq!(res.status(), 200);

    // Policy should be gone
    let body: Value = server
        .request("GetResourcePolicy", &json!({ "ResourceARN": arn }))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["Policy"].as_str().unwrap_or(""), "");
}

#[tokio::test]
async fn delete_resource_policy_missing_arn() {
    let server = TestServer::new().await;
    let res = server
        .request("DeleteResourcePolicy", &json!({}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn delete_resource_policy_nonexistent_is_noop() {
    let server = TestServer::new().await;
    let name = "rp-delete-noop";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    // No policy set — delete should still succeed
    let res = server
        .request("DeleteResourcePolicy", &json!({ "ResourceARN": arn }))
        .await;
    assert_eq!(res.status(), 200);
}

// -- Policy round-trip: put → get → delete → get --

#[tokio::test]
async fn resource_policy_full_lifecycle() {
    let server = TestServer::new().await;
    let name = "rp-lifecycle";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request("PutResourcePolicy", &json!({ "ResourceARN": arn, "Policy": POLICY }))
        .await;

    let body: Value = server
        .request("GetResourcePolicy", &json!({ "ResourceARN": arn }))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["Policy"].as_str().unwrap(), POLICY);

    // Overwrite with a different policy
    let policy2 = r#"{"Version":"2012-10-17","Statement":[]}"#;
    server
        .request("PutResourcePolicy", &json!({ "ResourceARN": arn, "Policy": policy2 }))
        .await;

    let body: Value = server
        .request("GetResourcePolicy", &json!({ "ResourceARN": arn }))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["Policy"].as_str().unwrap(), policy2);

    server
        .request("DeleteResourcePolicy", &json!({ "ResourceARN": arn }))
        .await;

    let body: Value = server
        .request("GetResourcePolicy", &json!({ "ResourceARN": arn }))
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["Policy"].as_str().unwrap_or(""), "");
}
