use ferrokinesis::store::StoreOptions;
use reqwest::Client;
use serde_json::{Value, json};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::process::Command;

pub const AMZ_JSON: &str = "application/x-amz-json-1.1";
pub const VERSION: &str = "Kinesis_20131202";

pub struct TestServer {
    pub addr: SocketAddr,
    pub client: Client,
}

impl TestServer {
    pub async fn new() -> Self {
        Self::with_options(StoreOptions {
            create_stream_ms: 0,
            delete_stream_ms: 0,
            update_stream_ms: 0,
            shard_limit: 50,
            ..Default::default()
        })
        .await
    }

    pub async fn with_options(options: StoreOptions) -> Self {
        let (app, _store) = ferrokinesis::create_app(options);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            addr,
            client: Client::new(),
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub async fn request(&self, target: &str, data: &Value) -> reqwest::Response {
        self.client
            .post(self.url())
            .header("Content-Type", AMZ_JSON)
            .header("X-Amz-Target", format!("{VERSION}.{target}"))
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
            )
            .header("X-Amz-Date", "20150101T000000Z")
            .body(serde_json::to_vec(data).unwrap())
            .send()
            .await
            .unwrap()
    }

    pub async fn create_stream(&self, name: &str, shard_count: u32) {
        let response = self
            .request(
                "CreateStream",
                &json!({
                    "StreamName": name,
                    "ShardCount": shard_count,
                }),
            )
            .await;
        assert_eq!(response.status(), 200);
    }

    pub async fn put_record(&self, stream: &str, data: &str, partition_key: &str) {
        let response = self
            .request(
                "PutRecord",
                &json!({
                    "StreamName": stream,
                    "Data": data,
                    "PartitionKey": partition_key,
                }),
            )
            .await;
        assert_eq!(response.status(), 200);
    }

    pub async fn describe_stream_summary(&self, stream: &str) -> Value {
        let response = self
            .request(
                "DescribeStreamSummary",
                &json!({
                    "StreamName": stream,
                }),
            )
            .await;
        assert_eq!(response.status(), 200);
        response.json::<Value>().await.unwrap()
    }

    pub async fn stream_arn(&self, stream: &str) -> String {
        self.describe_stream_summary(stream).await["StreamDescriptionSummary"]["StreamARN"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub async fn register_consumer(&self, stream: &str, consumer: &str) -> String {
        let stream_arn = self.stream_arn(stream).await;
        let response = self
            .request(
                "RegisterStreamConsumer",
                &json!({
                    "StreamARN": stream_arn,
                    "ConsumerName": consumer,
                }),
            )
            .await;
        assert_eq!(response.status(), 200);
        response.json::<Value>().await.unwrap()["Consumer"]["ConsumerARN"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub async fn wait_for_consumer_active(&self, stream: &str, consumer: &str) {
        let stream_arn = self.stream_arn(stream).await;
        for _ in 0..20 {
            let response = self
                .request(
                    "DescribeStreamConsumer",
                    &json!({
                        "StreamARN": stream_arn,
                        "ConsumerName": consumer,
                    }),
                )
                .await;
            if response.status() == 200 {
                let body = response.json::<Value>().await.unwrap();
                if body["ConsumerDescription"]["ConsumerStatus"].as_str() == Some("ACTIVE") {
                    return;
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        panic!("consumer did not become ACTIVE");
    }

    pub async fn get_shard_iterator(
        &self,
        stream: &str,
        shard_id: &str,
        iterator_type: &str,
    ) -> String {
        let response = self
            .request(
                "GetShardIterator",
                &json!({
                    "StreamName": stream,
                    "ShardId": shard_id,
                    "ShardIteratorType": iterator_type,
                }),
            )
            .await;
        assert_eq!(response.status(), 200);
        response.json::<Value>().await.unwrap()["ShardIterator"]
            .as_str()
            .unwrap()
            .to_string()
    }

    pub async fn get_records(&self, iterator: &str) -> Value {
        let response = self
            .request(
                "GetRecords",
                &json!({
                    "ShardIterator": iterator,
                }),
            )
            .await;
        assert_eq!(response.status(), 200);
        response.json::<Value>().await.unwrap()
    }
}

pub fn ferro_command(server: &TestServer) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ferro"));
    command.env("FERROKINESIS_ENDPOINT", server.url());
    command
}
