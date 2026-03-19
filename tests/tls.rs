#![cfg(feature = "tls")]

mod common;

use common::TestServer;
use serde_json::{Value, json};
use std::process::Command;

#[tokio::test]
async fn tls_health_endpoint() {
    let server = TestServer::new_tls().await;

    let res = server
        .client
        .get(format!("https://{}/_health", server.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn tls_list_streams() {
    let server = TestServer::new_tls().await;

    let res = server.tls_request("ListStreams", &json!({})).await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert!(body["StreamNames"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn tls_put_and_get_records() {
    let server = TestServer::new_tls().await;

    // Create a stream
    let res = server
        .tls_request(
            "CreateStream",
            &json!({"StreamName": "tls-test", "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 200);
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Put a record
    let res = server
        .tls_request(
            "PutRecord",
            &json!({
                "StreamName": "tls-test",
                "Data": "dGVzdA==",
                "PartitionKey": "pk1",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Get shard iterator
    let res = server
        .tls_request(
            "GetShardIterator",
            &json!({
                "StreamName": "tls-test",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let iterator = body["ShardIterator"].as_str().unwrap();

    // Get records
    let res = server
        .tls_request("GetRecords", &json!({"ShardIterator": iterator}))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let records = body["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["Data"], "dGVzdA==");
    assert_eq!(records[0]["PartitionKey"], "pk1");
}

#[test]
fn generate_cert_creates_valid_pem_files() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");

    let output = Command::new(env!("CARGO_BIN_EXE_ferrokinesis"))
        .args([
            "generate-cert",
            "--cert-out",
            cert_path.to_str().unwrap(),
            "--key-out",
            key_path.to_str().unwrap(),
        ])
        .output()
        .expect("failed to run ferrokinesis");

    assert!(
        output.status.success(),
        "generate-cert failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    assert!(cert_path.exists(), "cert.pem not created");
    assert!(key_path.exists(), "key.pem not created");

    let cert_content = std::fs::read_to_string(&cert_path).unwrap();
    let key_content = std::fs::read_to_string(&key_path).unwrap();
    assert!(
        cert_content.contains("BEGIN CERTIFICATE"),
        "cert.pem missing PEM marker"
    );
    assert!(
        key_content.contains("BEGIN PRIVATE KEY"),
        "key.pem missing PEM marker"
    );

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let key_perms = std::fs::metadata(&key_path).unwrap().permissions();
        assert_eq!(
            key_perms.mode() & 0o777,
            0o600,
            "key.pem should have 0600 permissions, got {:o}",
            key_perms.mode() & 0o777
        );
    }
}

#[test]
fn generate_cert_with_custom_sans() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");

    let output = Command::new(env!("CARGO_BIN_EXE_ferrokinesis"))
        .args([
            "generate-cert",
            "--cert-out",
            cert_path.to_str().unwrap(),
            "--key-out",
            key_path.to_str().unwrap(),
            "--san",
            "example.com",
            "--san",
            "10.0.0.1",
        ])
        .output()
        .expect("failed to run ferrokinesis");

    assert!(
        output.status.success(),
        "generate-cert with custom SANs failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(cert_path.exists());
    assert!(key_path.exists());

    let cert_content = std::fs::read_to_string(&cert_path).unwrap();
    assert!(cert_content.contains("BEGIN CERTIFICATE"));
}
