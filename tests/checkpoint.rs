mod common;

use common::TestServer;
use ferrokinesis::store::StoreOptions;
use reqwest::Client;
use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{Value, json};
use std::time::Duration;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

const DDB_JSON: &str = "application/x-amz-json-1.0";
const DDB_VERSION: &str = "DynamoDB_20120810";

async fn ddb_request_to(
    client: &Client,
    url: &str,
    operation: &str,
    payload: &Value,
) -> reqwest::Response {
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

    client
        .request(Method::POST, url)
        .headers(headers)
        .body(serde_json::to_vec(payload).unwrap())
        .send()
        .await
        .unwrap()
}

async fn ddb_request(server: &TestServer, operation: &str, payload: &Value) -> reqwest::Response {
    ddb_request_to(&server.client, &server.url(), operation, payload).await
}

struct RestartableServer {
    url: String,
    client: Client,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl RestartableServer {
    async fn start(options: StoreOptions) -> Self {
        let (app, _store) = ferrokinesis::create_app(options);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            ferrokinesis::serve_plain_http(listener, app, async {
                let _ = shutdown_rx.await;
            })
            .await
        });

        Self {
            url: format!("http://{addr}"),
            client: Client::new(),
            shutdown_tx: Some(shutdown_tx),
            task,
        }
    }

    async fn wait_ready(&self) {
        for _ in 0..40 {
            if let Ok(response) = self
                .client
                .get(format!("{}/_health/ready", self.url))
                .send()
                .await
                && response.status().is_success()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("server did not become ready");
    }

    async fn shutdown(mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.task.await.unwrap().unwrap();
    }
}

fn checkpoint_store_options(state_dir: &std::path::Path) -> StoreOptions {
    StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        state_dir: Some(state_dir.to_path_buf()),
        snapshot_interval_secs: 30,
        ..Default::default()
    }
}

fn gsi_create_table_payload(table_name: &str) -> Value {
    json!({
        "TableName": table_name,
        "AttributeDefinitions": [
            {"AttributeName": "leaseKey", "AttributeType": "S"},
            {"AttributeName": "leaseOwner", "AttributeType": "S"}
        ],
        "KeySchema": [
            {"AttributeName": "leaseKey", "KeyType": "HASH"}
        ],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [
            {
                "IndexName": "leaseOwner-index",
                "KeySchema": [
                    {"AttributeName": "leaseOwner", "KeyType": "HASH"}
                ],
                "Projection": {
                    "ProjectionType": "ALL"
                }
            }
        ]
    })
}

fn assert_table_has_gsi(table: &Value, table_name: &str) {
    assert_eq!(table["TableName"], table_name);
    assert_eq!(table["TableStatus"], "ACTIVE");
    assert_eq!(table["GlobalSecondaryIndexes"].as_array().unwrap().len(), 1);
    assert_eq!(
        table["GlobalSecondaryIndexes"][0]["IndexName"],
        "leaseOwner-index"
    );
    assert_eq!(table["GlobalSecondaryIndexes"][0]["IndexStatus"], "ACTIVE");
    assert_eq!(
        table["GlobalSecondaryIndexes"][0]["Projection"]["ProjectionType"],
        "ALL"
    );
    assert!(
        table["GlobalSecondaryIndexes"][0]["IndexArn"]
            .as_str()
            .unwrap()
            .contains(&format!("table/{table_name}/index/leaseOwner-index"))
    );
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
async fn dynamodb_create_and_describe_table_with_gsi_metadata() {
    let server = TestServer::new().await;
    let payload = gsi_create_table_payload("kcl-lease-table-with-gsi");

    let create_res = ddb_request(&server, "CreateTable", &payload).await;
    assert_eq!(create_res.status(), 200);
    let create_body: Value = create_res.json().await.unwrap();
    assert_table_has_gsi(&create_body["TableDescription"], "kcl-lease-table-with-gsi");

    let describe_res = ddb_request(
        &server,
        "DescribeTable",
        &json!({
            "TableName": "kcl-lease-table-with-gsi"
        }),
    )
    .await;
    assert_eq!(describe_res.status(), 200);
    let describe_body: Value = describe_res.json().await.unwrap();
    assert_table_has_gsi(&describe_body["Table"], "kcl-lease-table-with-gsi");
}

#[tokio::test]
async fn checkpoint_tables_survive_durable_restart() {
    let state_dir = tempdir().unwrap();
    let payload = gsi_create_table_payload("kcl-lease-table-durable");

    let server = RestartableServer::start(checkpoint_store_options(state_dir.path())).await;
    server.wait_ready().await;
    let create_res = ddb_request_to(&server.client, &server.url, "CreateTable", &payload).await;
    assert_eq!(create_res.status(), 200);
    server.shutdown().await;

    let restarted = RestartableServer::start(checkpoint_store_options(state_dir.path())).await;
    restarted.wait_ready().await;

    let ready_res = restarted
        .client
        .get(format!("{}/_health/ready", restarted.url))
        .send()
        .await
        .unwrap();
    assert_eq!(ready_res.status(), 200);

    let describe_res = ddb_request_to(
        &restarted.client,
        &restarted.url,
        "DescribeTable",
        &json!({
            "TableName": "kcl-lease-table-durable"
        }),
    )
    .await;
    assert_eq!(describe_res.status(), 200);
    let describe_body: Value = describe_res.json().await.unwrap();
    assert_table_has_gsi(&describe_body["Table"], "kcl-lease-table-durable");

    restarted.shutdown().await;
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
