use ferrokinesis::actions::{Operation, dispatch};
use ferrokinesis::persistence::Persistence;
use ferrokinesis::store::{Store, StoreOptions};
use ferrokinesis::types::StreamStatus;
use serde_json::json;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;

fn durable_options(state_dir: &Path, snapshot_interval_secs: u64) -> StoreOptions {
    StoreOptions {
        create_stream_ms: 0,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        state_dir: Some(state_dir.to_path_buf()),
        snapshot_interval_secs,
        ..Default::default()
    }
}

async fn create_active_stream(store: &Store, name: &str) {
    dispatch(
        store,
        Operation::CreateStream,
        json!({
            "StreamName": name,
            "ShardCount": 1,
        }),
    )
    .await
    .unwrap();
    for _ in 0..50 {
        let stream = store.get_stream(name).await.unwrap();
        if stream.stream_status == StreamStatus::Active && !stream.shards.is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("stream {name} did not become ACTIVE");
}

async fn put_record(store: &Store, name: &str, data: &str, partition_key: &str) {
    dispatch(
        store,
        Operation::PutRecord,
        json!({
            "StreamName": name,
            "Data": data,
            "PartitionKey": partition_key,
        }),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn durable_store_replays_wal_after_restart() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let stream_name = "durable-wal-replay";

    let store = Store::new(options.clone());
    create_active_stream(&store, stream_name).await;
    put_record(&store, stream_name, "AAAA", "pk").await;
    drop(store);

    let persisted = Persistence::new(dir.path().to_path_buf()).unwrap().load();
    assert!(persisted.is_ok(), "{persisted:?}");

    let recovered = Store::new(options);
    let ready = recovered.check_ready();
    assert!(ready.is_ok(), "{ready:?}");

    let stream = recovered.get_stream(stream_name).await.unwrap();
    assert_eq!(stream.stream_status, StreamStatus::Active);

    let records = recovered.get_record_store(stream_name).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records.values().next().unwrap().partition_key,
        "pk",
        "recovered record should preserve payload metadata"
    );
}

#[tokio::test]
async fn durable_store_restores_from_snapshot_after_restart() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 1);
    let stream_name = "durable-snapshot-restore";

    let store = Store::new(options.clone());
    create_active_stream(&store, stream_name).await;
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    put_record(&store, stream_name, "AAAA", "pk").await;
    drop(store);

    let persisted = Persistence::new(dir.path().to_path_buf()).unwrap().load();
    assert!(persisted.is_ok(), "{persisted:?}");
    assert!(dir.path().join("snapshot.bin").exists());
    assert_eq!(fs::metadata(dir.path().join("wal.log")).unwrap().len(), 0);

    let recovered = Store::new(options);
    let ready = recovered.check_ready();
    assert!(ready.is_ok(), "{ready:?}");
    assert_eq!(recovered.get_record_store(stream_name).await.len(), 1);
}

#[tokio::test]
async fn durable_store_marks_itself_unready_when_wal_is_corrupted() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let stream_name = "durable-corrupt-wal";

    let store = Store::new(options.clone());
    create_active_stream(&store, stream_name).await;
    put_record(&store, stream_name, "AAAA", "pk").await;
    drop(store);

    let mut wal = OpenOptions::new()
        .append(true)
        .open(dir.path().join("wal.log"))
        .unwrap();
    wal.write_all(&[0xde, 0xad, 0xbe]).unwrap();
    wal.sync_all().unwrap();

    let recovered = Store::new(options);
    let err = recovered.check_ready().unwrap_err().to_string();
    assert!(err.contains("wal"));
}

#[tokio::test]
async fn durable_store_preserves_internal_stream_fields() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let stream_name = "durable-hidden-fields";

    let store = Store::new(options.clone());
    create_active_stream(&store, stream_name).await;
    store
        .update_stream(stream_name, |stream| {
            stream.tags.insert("env".to_string(), "prod".to_string());
            stream.key_id = Some("arn:aws:kms:eu-west-1:123456789012:key/abc".to_string());
            stream.warm_throughput_mibps = 7;
            stream.max_record_size_kib = 4096;
            Ok(())
        })
        .await
        .unwrap();
    drop(store);

    let recovered = Store::new(options);
    let stream = recovered.get_stream(stream_name).await.unwrap();
    assert_eq!(stream.tags.get("env").map(String::as_str), Some("prod"));
    assert_eq!(
        stream.key_id.as_deref(),
        Some("arn:aws:kms:eu-west-1:123456789012:key/abc")
    );
    assert_eq!(stream.warm_throughput_mibps, 7);
    assert_eq!(stream.max_record_size_kib, 4096);
}
