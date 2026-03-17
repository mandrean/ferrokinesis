mod common;

use aws_credential_types::Credentials;
use aws_sdk_kinesis::Client;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::{PutRecordsRequestEntry, ShardIteratorType, StreamStatus};
use common::TestServer;

async fn sdk_client(server: &TestServer) -> Client {
    let config = aws_sdk_kinesis::Config::builder()
        .credentials_provider(Credentials::for_tests())
        .region(aws_sdk_kinesis::config::Region::new("us-east-1"))
        .endpoint_url(server.url())
        .behavior_version(aws_sdk_kinesis::config::BehaviorVersion::latest())
        .build();
    Client::from_conf(config)
}

#[tokio::test]
async fn sdk_stream_lifecycle() {
    let server = TestServer::new().await;
    let client = sdk_client(&server).await;

    // Create stream
    client
        .create_stream()
        .stream_name("sdk-test")
        .shard_count(2)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Describe stream
    let desc = client
        .describe_stream()
        .stream_name("sdk-test")
        .send()
        .await
        .unwrap();
    let stream = desc.stream_description().unwrap();
    assert_eq!(stream.stream_name(), "sdk-test");
    assert_eq!(stream.stream_status(), &StreamStatus::Active);
    assert_eq!(stream.shards().len(), 2);

    // List streams
    let list = client.list_streams().send().await.unwrap();
    assert!(list.stream_names().contains(&"sdk-test".to_string()));

    // Put a record
    let put = client
        .put_record()
        .stream_name("sdk-test")
        .data(Blob::new(b"hello world"))
        .partition_key("pk-1")
        .send()
        .await
        .unwrap();

    // Get shard iterator for the shard that received the record
    let iter = client
        .get_shard_iterator()
        .stream_name("sdk-test")
        .shard_id(put.shard_id())
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();

    // Get records
    let records = client
        .get_records()
        .shard_iterator(iter.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(records.records().len(), 1);
    assert_eq!(records.records()[0].data().as_ref(), b"hello world");
    assert_eq!(records.records()[0].partition_key(), "pk-1");

    // Delete stream
    client
        .delete_stream()
        .stream_name("sdk-test")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn sdk_batch_put_records() {
    let server = TestServer::new().await;
    let client = sdk_client(&server).await;

    client
        .create_stream()
        .stream_name("sdk-batch")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Batch put 5 records
    let entries: Vec<PutRecordsRequestEntry> = (0..5)
        .map(|i| {
            PutRecordsRequestEntry::builder()
                .data(Blob::new(format!("record-{i}").into_bytes()))
                .partition_key(format!("pk-{i}"))
                .build()
                .unwrap()
        })
        .collect();

    let result = client
        .put_records()
        .stream_name("sdk-batch")
        .set_records(Some(entries))
        .send()
        .await
        .unwrap();

    assert_eq!(result.failed_record_count().unwrap_or(0), 0);
    assert_eq!(result.records().len(), 5);

    // Read all records back
    let iter = client
        .get_shard_iterator()
        .stream_name("sdk-batch")
        .shard_id("shardId-000000000000")
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();

    let records = client
        .get_records()
        .shard_iterator(iter.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(records.records().len(), 5);
}

#[tokio::test]
async fn sdk_consumers() {
    let server = TestServer::new().await;
    let client = sdk_client(&server).await;

    client
        .create_stream()
        .stream_name("sdk-consumers")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Get stream ARN
    let desc = client
        .describe_stream()
        .stream_name("sdk-consumers")
        .send()
        .await
        .unwrap();
    let stream_arn = desc.stream_description().unwrap().stream_arn().to_string();

    // Register consumer
    let reg = client
        .register_stream_consumer()
        .stream_arn(&stream_arn)
        .consumer_name("my-consumer")
        .send()
        .await
        .unwrap();
    let consumer = reg.consumer().unwrap();
    assert_eq!(consumer.consumer_name(), "my-consumer");

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // List consumers
    let list = client
        .list_stream_consumers()
        .stream_arn(&stream_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(list.consumers().len(), 1);
    assert_eq!(list.consumers()[0].consumer_name(), "my-consumer");

    // Deregister consumer
    client
        .deregister_stream_consumer()
        .consumer_arn(consumer.consumer_arn())
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn sdk_tagging() {
    let server = TestServer::new().await;
    let client = sdk_client(&server).await;

    client
        .create_stream()
        .stream_name("sdk-tags")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Add tags
    client
        .add_tags_to_stream()
        .stream_name("sdk-tags")
        .tags("env", "test")
        .tags("project", "ferrokinesis")
        .send()
        .await
        .unwrap();

    // List tags
    let tags = client
        .list_tags_for_stream()
        .stream_name("sdk-tags")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 2);

    // Remove a tag
    client
        .remove_tags_from_stream()
        .stream_name("sdk-tags")
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_stream()
        .stream_name("sdk-tags")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
    assert_eq!(tags.tags()[0].key(), "project");
    assert_eq!(tags.tags()[0].value().unwrap_or_default(), "ferrokinesis");
}
