// ferrokinesis quickstart — AWS SDK for Rust
//
// Prerequisites:
//   - ferrokinesis running (docker run -p 4567:4567 ghcr.io/mandrean/ferrokinesis)
//
// Usage:
//   cargo run
//   KINESIS_ENDPOINT=http://localhost:4567 cargo run

use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::{ShardIteratorType, StreamStatus};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = std::env::var("KINESIS_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4567".to_string());

    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(&endpoint)
        .load()
        .await;
    let client = aws_sdk_kinesis::Client::new(&config);

    let stream = "rust-example";

    // Create a stream with 2 shards
    println!("==> CreateStream");
    client
        .create_stream()
        .stream_name(stream)
        .shard_count(2)
        .send()
        .await?;

    // Wait for stream to become ACTIVE
    for _ in 0..30 {
        let desc = client
            .describe_stream()
            .stream_name(stream)
            .send()
            .await?;
        let status = desc
            .stream_description()
            .unwrap()
            .stream_status()
            .clone();
        if status == StreamStatus::Active {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // Put a record
    println!("==> PutRecord");
    let put = client
        .put_record()
        .stream_name(stream)
        .partition_key("pk1")
        .data(Blob::new("hello world"))
        .send()
        .await?;
    let shard_id = put.shard_id().to_string();

    // Get records
    println!("==> GetRecords");
    let iter = client
        .get_shard_iterator()
        .stream_name(stream)
        .shard_id(&shard_id)
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await?;

    let records = client
        .get_records()
        .shard_iterator(iter.shard_iterator().unwrap())
        .send()
        .await?;

    for record in records.records() {
        let data = std::str::from_utf8(record.data().as_ref()).unwrap();
        println!("{}: {}", record.partition_key(), data);
    }

    // Clean up
    println!("==> DeleteStream");
    client
        .delete_stream()
        .stream_name(stream)
        .send()
        .await?;

    println!("Done.");
    Ok(())
}
