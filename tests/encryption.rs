mod common;

use common::*;
use ferrokinesis::store::StoreOptions;
use serde_json::{Value, json};

// -- StartStreamEncryption --

#[tokio::test]
async fn start_encryption_success() {
    let server = TestServer::new().await;
    let name = "enc-start";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "arn:aws:kms:us-east-1:0000-0000-0000:key/test-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Wait for async transition back to ACTIVE
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = server
        .request("DescribeStreamSummary", &json!({ "StreamName": name }))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let desc = &body["StreamDescriptionSummary"];
    assert_eq!(desc["EncryptionType"], "KMS");
    assert_eq!(
        desc["KeyId"],
        "arn:aws:kms:us-east-1:0000-0000-0000:key/test-key"
    );
}

#[tokio::test]
async fn start_encryption_wrong_type() {
    let server = TestServer::new().await;
    let name = "enc-wrong-type";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "NONE",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn start_encryption_stream_not_found() {
    let server = TestServer::new().await;

    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": "nonexistent",
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

// -- StopStreamEncryption --

#[tokio::test]
async fn stop_encryption_success() {
    let server = TestServer::new().await;
    let name = "enc-stop";
    server.create_stream(name, 1).await;

    // Start encryption first
    server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let res = server
        .request("DescribeStreamSummary", &json!({ "StreamName": name }))
        .await;
    let body: Value = res.json().await.unwrap();
    let desc = &body["StreamDescriptionSummary"];
    assert_eq!(desc["EncryptionType"], "NONE");
    assert!(desc["KeyId"].is_null());
}

#[tokio::test]
async fn stop_encryption_wrong_type() {
    let server = TestServer::new().await;
    let name = "enc-stop-wrong";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "AES256",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn stop_encryption_stream_not_found() {
    let server = TestServer::new().await;

    let res = server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": "nonexistent",
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

// -- Full encryption lifecycle --

#[tokio::test]
async fn encryption_lifecycle() {
    let server = TestServer::new().await;
    let name = "enc-lifecycle";
    server.create_stream(name, 1).await;

    // Initially NONE
    let body = server.describe_stream(name).await;
    assert_eq!(body["StreamDescription"]["EncryptionType"], "NONE");

    // Enable KMS
    server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "alias/my-key",
            }),
        )
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let body = server.describe_stream(name).await;
    assert_eq!(body["StreamDescription"]["EncryptionType"], "KMS");

    // Disable
    server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "alias/my-key",
            }),
        )
        .await;
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let body = server.describe_stream(name).await;
    assert_eq!(body["StreamDescription"]["EncryptionType"], "NONE");
}

#[tokio::test]
async fn stop_encryption_on_none_encrypted_stream() {
    let server = TestServer::new().await;
    let name = "cx-stop-none";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("not encrypted with KMS")
    );
}

#[tokio::test]
async fn stop_encryption_on_creating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
    })
    .await;
    let name = "cx-stop-creating";

    server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;

    let res = server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("CREATING"));
}

#[tokio::test]
async fn stop_encryption_on_updating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 500,
        shard_limit: 50,
    })
    .await;
    let name = "cx-stop-updating";
    server.create_stream(name, 1).await;

    server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;

    let res = server
        .request(
            "StopStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("UPDATING"));
}

#[tokio::test]
async fn start_encryption_on_creating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
    })
    .await;
    let name = "cx-start-creating";

    server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;

    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("CREATING"));
}

#[tokio::test]
async fn start_encryption_on_deleting_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 500,
        update_stream_ms: 0,
        shard_limit: 50,
    })
    .await;
    let name = "cx-start-deleting";
    server.create_stream(name, 1).await;

    server
        .request("DeleteStream", &json!({"StreamName": name}))
        .await;

    let res = server
        .request(
            "StartStreamEncryption",
            &json!({
                "StreamName": name,
                "EncryptionType": "KMS",
                "KeyId": "my-key",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
    assert!(body["message"].as_str().unwrap().contains("DELETING"));
}
