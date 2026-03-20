use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use ferrokinesis::store::StoreOptions;
use reqwest::Client;
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

const AMZ_JSON: &str = "application/x-amz-json-1.1";
const VERSION: &str = "Kinesis_20131202";

struct BenchServer {
    addr: SocketAddr,
    client: Client,
}

impl BenchServer {
    async fn new(shard_limit: u32) -> Self {
        let options = StoreOptions {
            create_stream_ms: 0,
            delete_stream_ms: 0,
            update_stream_ms: 0,
            shard_limit,
            ..Default::default()
        };
        let (app, _store) = ferrokinesis::create_app(options, None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        BenchServer {
            addr,
            client: Client::new(),
        }
    }

    async fn request(&self, target: &str, data: &Value) -> Value {
        let res = self
            .client
            .post(format!("http://{}", self.addr))
            .header("Content-Type", AMZ_JSON)
            .header("X-Amz-Target", format!("{VERSION}.{target}"))
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
                 SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
            )
            .header("X-Amz-Date", "20150101T000000Z")
            .body(serde_json::to_vec(data).unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), 200, "{target} failed");
        let bytes = res.bytes().await.unwrap();
        if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        }
    }

    async fn create_stream(&self, name: &str, shard_count: u32) {
        self.request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": shard_count}),
        )
        .await;
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    async fn put_record(&self, stream: &str, data: &str, partition_key: &str) -> Value {
        self.request(
            "PutRecord",
            &json!({
                "StreamName": stream,
                "Data": data,
                "PartitionKey": partition_key,
            }),
        )
        .await
    }

    async fn get_shard_iterator(&self, stream: &str, shard_id: &str) -> String {
        let body = self
            .request(
                "GetShardIterator",
                &json!({
                    "StreamName": stream,
                    "ShardId": shard_id,
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            )
            .await;
        body["ShardIterator"].as_str().unwrap().to_string()
    }

    async fn get_records(&self, iterator: &str) -> Value {
        self.request("GetRecords", &json!({"ShardIterator": iterator}))
            .await
    }
}

// ---------------------------------------------------------------------------
// Benchmark groups
// ---------------------------------------------------------------------------

fn bench_put_record(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(BenchServer::new(200));
    rt.block_on(server.create_stream("put-record-bench", 1));

    // 100-byte base64-encoded payload
    let data = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8; 100]);

    c.bench_function("put_record/single", |b| {
        b.to_async(&rt)
            .iter(|| async { server.put_record("put-record-bench", &data, "pk").await });
    });
}

fn bench_put_records(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(BenchServer::new(200));

    let data = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8; 100]);

    let mut group = c.benchmark_group("put_records");
    for batch_size in [10, 100, 500] {
        let stream = format!("put-records-{batch_size}");
        rt.block_on(server.create_stream(&stream, 1));

        let records: Vec<Value> = (0..batch_size)
            .map(|i| {
                json!({
                    "Data": data,
                    "PartitionKey": format!("pk-{i}"),
                })
            })
            .collect();
        let payload = json!({
            "StreamName": stream,
            "Records": records,
        });

        group.bench_function(format!("batch_{batch_size}"), |b| {
            b.to_async(&rt)
                .iter(|| async { server.request("PutRecords", &payload).await });
        });
    }
    group.finish();
}

fn bench_get_records(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(BenchServer::new(200));

    let data = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8; 100]);

    let mut group = c.benchmark_group("get_records");
    for count in [10, 100, 1000] {
        let stream = format!("get-records-{count}");
        rt.block_on(server.create_stream(&stream, 1));

        // Pre-populate records
        for i in 0..count {
            rt.block_on(server.put_record(&stream, &data, &format!("pk-{i}")));
        }

        let stream_clone = stream.clone();

        group.bench_function(format!("{count}_records"), |b| {
            b.to_async(&rt).iter(|| async {
                let iter = server
                    .get_shard_iterator(&stream_clone, "shardId-000000000000")
                    .await;
                server.get_records(&iter).await
            });
        });
    }
    group.finish();
}

fn bench_create_stream(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(BenchServer::new(500_000));
    let counter = AtomicU64::new(0);

    c.bench_function("create_stream/single", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let n = counter.fetch_add(1, Ordering::Relaxed);
                format!("bench-stream-{n}")
            },
            |name| {
                let s = &server;
                async move {
                    s.request(
                        "CreateStream",
                        &json!({"StreamName": name, "ShardCount": 1}),
                    )
                    .await
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_describe_stream(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(BenchServer::new(200));
    rt.block_on(server.create_stream("describe-bench", 4));

    c.bench_function("describe_stream/single", |b| {
        b.to_async(&rt).iter(|| async {
            server
                .request("DescribeStream", &json!({"StreamName": "describe-bench"}))
                .await
        });
    });
}

fn bench_list_streams(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let server = rt.block_on(BenchServer::new(200));

    // Pre-create 100 streams (covers both 10 and 100 cases)
    for i in 0..100 {
        rt.block_on(server.create_stream(&format!("list-bench-{i:03}"), 1));
    }

    let mut group = c.benchmark_group("list_streams");
    for limit in [10, 100] {
        let payload = json!({"Limit": limit});
        group.bench_function(format!("{limit}_streams"), |b| {
            b.to_async(&rt)
                .iter(|| async { server.request("ListStreams", &payload).await });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_put_record,
    bench_put_records,
    bench_get_records,
    bench_create_stream,
    bench_describe_stream,
    bench_list_streams,
);
criterion_main!(benches);
