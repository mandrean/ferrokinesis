#![allow(dead_code)]

use axum::extract::DefaultBodyLimit;
use base64::Engine;
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

    /// Make a signed Kinesis API request (CBOR content type)
    pub async fn cbor_request(&self, target: &str, data: &Value) -> reqwest::Response {
        let mut buf = Vec::new();
        ciborium::into_writer(data, &mut buf).unwrap();
        self.client
            .post(self.url())
            .header("Content-Type", AMZ_CBOR)
            .header("X-Amz-Target", format!("{VERSION}.{target}"))
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
            )
            .header("X-Amz-Date", "20150101T000000Z")
            .body(buf)
            .send()
            .await
            .unwrap()
    }

    /// Send the same request as both JSON and CBOR, decode both responses
    pub async fn request_both(&self, target: &str, data: &Value) -> ((u16, Value), (u16, Value)) {
        let json_resp = decode_body(self.request(target, data).await).await;
        let cbor_resp = decode_body(self.cbor_request(target, data).await).await;
        (json_resp, cbor_resp)
    }

    /// Make a CBOR request with Data encoded as CBOR byte string (major type 2).
    /// This simulates real SDK v2 behavior where Blob fields are CBOR bytes, not base64 text.
    /// `raw_data` is the raw bytes (not base64-encoded).
    pub async fn cbor_request_raw_data(
        &self,
        target: &str,
        fields: &Value,
        data_field_path: &str,
        raw_data: &[u8],
    ) -> reqwest::Response {
        let cbor_val = json_to_cbor_with_bytes(fields, data_field_path, raw_data);
        let mut buf = Vec::new();
        ciborium::into_writer(&cbor_val, &mut buf).unwrap();
        self.client
            .post(self.url())
            .header("Content-Type", AMZ_CBOR)
            .header("X-Amz-Target", format!("{VERSION}.{target}"))
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
            )
            .header("X-Amz-Date", "20150101T000000Z")
            .body(buf)
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

/// Decode a CBOR response body, handling byte strings in Data fields by converting
/// them to base64 strings (matching JSON representation) for comparison.
pub async fn decode_cbor_body(res: reqwest::Response) -> (u16, Value) {
    let status = res.status().as_u16();
    let bytes = res.bytes().await.unwrap();
    if bytes.is_empty() {
        return (status, Value::Null);
    }
    let cbor_val: ciborium::Value =
        ciborium::from_reader(&bytes[..]).unwrap_or(ciborium::Value::Null);
    let json_val = cbor_to_json(&cbor_val);
    (status, json_val)
}

/// Convert ciborium::Value to serde_json::Value, mapping CBOR byte strings
/// to base64-encoded JSON strings.
pub fn cbor_to_json(val: &ciborium::Value) -> Value {
    match val {
        ciborium::Value::Null => Value::Null,
        ciborium::Value::Bool(b) => Value::Bool(*b),
        ciborium::Value::Integer(n) => {
            let n: i128 = (*n).into();
            Value::Number(serde_json::Number::from(n as i64))
        }
        ciborium::Value::Float(f) => {
            serde_json::Number::from_f64(*f)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        ciborium::Value::Text(s) => Value::String(s.clone()),
        ciborium::Value::Bytes(b) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        ciborium::Value::Array(arr) => {
            Value::Array(arr.iter().map(cbor_to_json).collect())
        }
        ciborium::Value::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                let key = match k {
                    ciborium::Value::Text(s) => s.clone(),
                    _ => format!("{k:?}"),
                };
                obj.insert(key, cbor_to_json(v));
            }
            Value::Object(obj)
        }
        ciborium::Value::Tag(_, inner) => cbor_to_json(inner),
        _ => Value::Null,
    }
}

/// Convert a serde_json::Value to ciborium::Value, replacing the field at
/// `data_field_path` (e.g., "Data" or "Records.*.Data") with CBOR Bytes.
/// `raw_data` is the raw bytes for that field.
pub fn json_to_cbor_with_bytes(
    val: &Value,
    data_field_path: &str,
    raw_data: &[u8],
) -> ciborium::Value {
    json_to_cbor_inner(val, data_field_path, raw_data)
}

fn json_to_cbor_inner(val: &Value, path: &str, raw_data: &[u8]) -> ciborium::Value {
    match val {
        Value::Null => ciborium::Value::Null,
        Value::Bool(b) => ciborium::Value::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ciborium::Value::Integer(i.into())
            } else if let Some(f) = n.as_f64() {
                ciborium::Value::Float(f)
            } else {
                ciborium::Value::Null
            }
        }
        Value::String(s) => ciborium::Value::Text(s.clone()),
        Value::Array(arr) => {
            // Handle "Records.*.Data" style paths
            let (first, rest) = path.split_once('.').unwrap_or((path, ""));
            if first == "*" {
                ciborium::Value::Array(
                    arr.iter()
                        .map(|item| json_to_cbor_inner(item, rest, raw_data))
                        .collect(),
                )
            } else {
                ciborium::Value::Array(
                    arr.iter()
                        .map(|item| json_to_cbor_inner(item, "", &[]))
                        .collect(),
                )
            }
        }
        Value::Object(map) => {
            let (first, rest) = path.split_once('.').unwrap_or((path, ""));
            ciborium::Value::Map(
                map.iter()
                    .map(|(k, v)| {
                        let cbor_key = ciborium::Value::Text(k.clone());
                        let cbor_val = if k == first && rest.is_empty() {
                            // This is the target field — replace with Bytes
                            ciborium::Value::Bytes(raw_data.to_vec())
                        } else if k == first {
                            // Traverse deeper
                            json_to_cbor_inner(v, rest, raw_data)
                        } else {
                            json_to_cbor_inner(v, "", &[])
                        };
                        (cbor_key, cbor_val)
                    })
                    .collect(),
            )
        }
    }
}

/// Remove specified keys from a JSON Value (recursive).
pub fn strip_keys(val: &mut Value, keys: &[&str]) {
    match val {
        Value::Object(map) => {
            for key in keys {
                map.remove(*key);
            }
            for v in map.values_mut() {
                strip_keys(v, keys);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                strip_keys(item, keys);
            }
        }
        _ => {}
    }
}

/// Assert two JSON Values are structurally equivalent, ignoring specified volatile keys.
pub fn assert_values_equivalent(a: &Value, b: &Value, ignore_keys: &[&str]) {
    let mut a = a.clone();
    let mut b = b.clone();
    strip_keys(&mut a, ignore_keys);
    strip_keys(&mut b, ignore_keys);
    assert_eq!(
        a, b,
        "Values not equivalent after stripping {:?}:\n  left:  {}\n  right: {}",
        ignore_keys,
        serde_json::to_string_pretty(&a).unwrap(),
        serde_json::to_string_pretty(&b).unwrap(),
    );
}
