mod common;

use common::TestServer;
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{Value, json};

const DDB_JSON: &str = "application/x-amz-json-1.0";
const DDB_VERSION: &str = "DynamoDB_20120810";

async fn ddb_request(server: &TestServer, operation: &str, payload: &Value) -> reqwest::Response {
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static(DDB_JSON));
    headers.insert(
        "X-Amz-Target",
        HeaderValue::from_str(&format!("{DDB_VERSION}.{operation}")).unwrap(),
    );
    headers.insert(
        "Authorization",
        HeaderValue::from_static(
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/dynamodb/aws4_request, \
             SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        ),
    );
    headers.insert("X-Amz-Date", HeaderValue::from_static("20150101T000000Z"));

    server
        .raw_request(
            Method::POST,
            "/",
            headers,
            serde_json::to_vec(payload).unwrap(),
        )
        .await
}

#[tokio::test]
async fn dynamodb_create_and_describe_table() {
    let server = TestServer::new().await;

    let create_res = ddb_request(
        &server,
        "CreateTable",
        &json!({
            "TableName": "kcl-lease-table",
            "AttributeDefinitions": [
                {"AttributeName": "leaseKey", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "leaseKey", "KeyType": "HASH"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;
    assert_eq!(create_res.status(), 200);
    let create_body: Value = create_res.json().await.unwrap();
    assert_eq!(
        create_body["TableDescription"]["TableName"],
        "kcl-lease-table"
    );
    assert_eq!(create_body["TableDescription"]["TableStatus"], "ACTIVE");
    assert!(
        create_body["TableDescription"]["TableArn"]
            .as_str()
            .unwrap()
            .contains("table/kcl-lease-table")
    );

    let describe_res = ddb_request(
        &server,
        "DescribeTable",
        &json!({
            "TableName": "kcl-lease-table"
        }),
    )
    .await;
    assert_eq!(describe_res.status(), 200);
    let describe_body: Value = describe_res.json().await.unwrap();
    assert_eq!(describe_body["Table"]["TableName"], "kcl-lease-table");
    assert_eq!(describe_body["Table"]["TableStatus"], "ACTIVE");

    // Same listener still serves Kinesis traffic.
    let list_streams = server.request("ListStreams", &json!({})).await;
    assert_eq!(list_streams.status(), 200);
}

#[tokio::test]
async fn dynamodb_unknown_operation_returns_dynamodb_error_shape() {
    let server = TestServer::new().await;
    let res = ddb_request(&server, "NotAnOperation", &json!({})).await;
    assert_eq!(res.status(), 400);
    assert_eq!(
        res.headers().get("content-type").unwrap().to_str().unwrap(),
        DDB_JSON
    );
    let body: Value = res.json().await.unwrap();
    assert_eq!(
        body["__type"],
        "com.amazonaws.dynamodb.v20120810#UnknownOperationException"
    );
}

#[tokio::test]
async fn dynamodb_create_table_twice_returns_resource_in_use() {
    let server = TestServer::new().await;
    let payload = json!({
        "TableName": "kcl-lease-table-dup",
        "AttributeDefinitions": [
            {"AttributeName": "leaseKey", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "leaseKey", "KeyType": "HASH"}
        ]
    });

    let first = ddb_request(&server, "CreateTable", &payload).await;
    assert_eq!(first.status(), 200);

    let second = ddb_request(&server, "CreateTable", &payload).await;
    assert_eq!(second.status(), 400);
    let body: Value = second.json().await.unwrap();
    assert_eq!(
        body["__type"],
        "com.amazonaws.dynamodb.v20120810#ResourceInUseException"
    );
}
