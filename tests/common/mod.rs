#![allow(dead_code)]

use axum::extract::DefaultBodyLimit;
use ferrokinesis::store::StoreOptions;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::{Value, json};
use std::net::SocketAddr;
use tokio::net::TcpListener;

pub const AMZ_JSON: &str = "application/x-amz-json-1.1";
pub const AMZ_CBOR: &str = "application/x-amz-cbor-1.1";
pub const VERSION: &str = "Kinesis_20131202";

/// Decode a response body as either JSON or CBOR, depending on the content type
pub async fn decode_body(res: reqwest::Response) -> (u16, Value) {
    let status = res.status().as_u16();
    let ct = res
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = res.bytes().await.unwrap();
    if bytes.is_empty() {
        return (status, Value::Null);
    }
    if ct.contains("cbor") {
        let val: Value = ciborium::from_reader(&bytes[..]).unwrap_or(Value::Null);
        (status, val)
    } else {
        let val: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, val)
    }
}

pub struct TestServer {
    pub addr: SocketAddr,
    pub client: Client,
    pub store: ferrokinesis::store::Store,
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
        let (app, store) = ferrokinesis::create_app(options);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        TestServer {
            addr,
            client: Client::new(),
            store,
        }
    }

    pub async fn with_body_limit(options: StoreOptions, max_body_bytes: usize) -> Self {
        let (app, store) = ferrokinesis::create_app(options);
        let app = app.layer(DefaultBodyLimit::max(max_body_bytes));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        TestServer {
            addr,
            client: Client::new(),
            store,
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Make a signed Kinesis API request (JSON content type)
    pub async fn request(&self, target: &str, data: &Value) -> reqwest::Response {
        self.signed_request_to(self.url(), target, data).await
    }

    async fn signed_request_to(
        &self,
        url: String,
        target: &str,
        data: &Value,
    ) -> reqwest::Response {
        self.client
            .post(url)
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

    /// Make a raw HTTP request with full control over headers/body
    pub async fn raw_request(
        &self,
        method: reqwest::Method,
        path: &str,
        headers: HeaderMap,
        body: Vec<u8>,
    ) -> reqwest::Response {
        self.client
            .request(method, format!("http://{}{}", self.addr, path))
            .headers(headers)
            .body(body)
            .send()
            .await
            .unwrap()
    }

    /// Helper: create a stream and wait for it to become active
    pub async fn create_stream(&self, name: &str, shard_count: u32) {
        let res = self
            .request(
                "CreateStream",
                &json!({"StreamName": name, "ShardCount": shard_count}),
            )
            .await;
        assert_eq!(res.status(), 200, "Failed to create stream {name}");

        // With create_stream_ms=0, should be immediately active
        // but give a small buffer for the tokio task to run
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    /// Helper: describe a stream
    pub async fn describe_stream(&self, name: &str) -> Value {
        let res = self
            .request("DescribeStream", &json!({"StreamName": name}))
            .await;
        assert_eq!(res.status(), 200);
        res.json().await.unwrap()
    }

    /// Helper: put a record and return the response
    pub async fn put_record(&self, stream: &str, data: &str, partition_key: &str) -> Value {
        let res = self
            .request(
                "PutRecord",
                &json!({
                    "StreamName": stream,
                    "Data": data,
                    "PartitionKey": partition_key,
                }),
            )
            .await;
        assert_eq!(res.status(), 200);
        res.json().await.unwrap()
    }

    /// Helper: get a shard iterator
    pub async fn get_shard_iterator(
        &self,
        stream: &str,
        shard_id: &str,
        iterator_type: &str,
    ) -> String {
        let res = self
            .request(
                "GetShardIterator",
                &json!({
                    "StreamName": stream,
                    "ShardId": shard_id,
                    "ShardIteratorType": iterator_type,
                }),
            )
            .await;
        assert_eq!(res.status(), 200);
        let body: Value = res.json().await.unwrap();
        body["ShardIterator"].as_str().unwrap().to_string()
    }

    /// Helper: get a stream's ARN via DescribeStream
    pub async fn get_stream_arn(&self, name: &str) -> String {
        let desc = self.describe_stream(name).await;
        desc["StreamDescription"]["StreamARN"]
            .as_str()
            .unwrap()
            .to_string()
    }

    /// Helper: get records
    pub async fn get_records(&self, iterator: &str) -> Value {
        let res = self
            .request("GetRecords", &json!({"ShardIterator": iterator}))
            .await;
        assert_eq!(res.status(), 200);
        res.json().await.unwrap()
    }
}

#[cfg(feature = "tls")]
impl TestServer {
    /// Create a test server with TLS using a self-signed certificate
    pub async fn new_tls() -> Self {
        let options = StoreOptions {
            create_stream_ms: 0,
            delete_stream_ms: 0,
            update_stream_ms: 0,
            shard_limit: 50,
            ..Default::default()
        };
        let (app, store) = ferrokinesis::create_app(options);

        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])
            .expect("failed to generate self-signed cert");

        let cert_pem = cert.cert.pem();
        let key_pem = cert.signing_key.serialize_pem();

        let tls_config = axum_server::tls_rustls::RustlsConfig::from_pem(
            cert_pem.into_bytes(),
            key_pem.into_bytes(),
        )
        .await
        .expect("failed to build RustlsConfig");

        let handle = axum_server::Handle::new();
        let handle_clone = handle.clone();

        tokio::spawn(async move {
            axum_server::bind_rustls("127.0.0.1:0".parse().unwrap(), tls_config)
                .handle(handle_clone)
                .serve(app.into_make_service())
                .await
                .unwrap();
        });

        let addr = handle.listening().await.unwrap();

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();

        TestServer {
            addr,
            client,
            store,
        }
    }

    pub fn tls_url(&self) -> String {
        format!("https://{}", self.addr)
    }

    /// Make a signed Kinesis API request over TLS
    pub async fn tls_request(&self, target: &str, data: &Value) -> reqwest::Response {
        self.signed_request_to(self.tls_url(), target, data).await
    }
}

/// Build auth headers for signed requests
pub fn signed_headers() -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("Content-Type", HeaderValue::from_static(AMZ_JSON));
    h.insert(
        "X-Amz-Target",
        HeaderValue::from_static("Kinesis_20131202.ListStreams"),
    );
    h.insert(
        "Authorization",
        HeaderValue::from_static(
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        ),
    );
    h.insert("X-Amz-Date", HeaderValue::from_static("20150101T000000Z"));
    h
}
