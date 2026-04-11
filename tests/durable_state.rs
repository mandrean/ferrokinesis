use ferrokinesis::actions::{Operation, dispatch};
use ferrokinesis::persistence::{Persistence, WalEntry};
use ferrokinesis::sequence;
use ferrokinesis::store::{Store, StoreOptions};
use ferrokinesis::types::{ConsumerStatus, EncryptionType, StoredRecord, StreamStatus};
use ferrokinesis::util::current_time_ms;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use tempfile::tempdir;

fn durable_options(state_dir: &Path, snapshot_interval_secs: u64) -> StoreOptions {
    StoreOptions {
        create_stream_ms: 50,
        delete_stream_ms: 50,
        update_stream_ms: 50,
        shard_limit: 50,
        state_dir: Some(state_dir.to_path_buf()),
        snapshot_interval_secs,
        ..Default::default()
    }
}

async fn create_active_stream(store: &Store, name: &str) -> String {
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
    for _ in 0..80 {
        let stream = store.get_stream(name).await.unwrap();
        if stream.stream_status == StreamStatus::Active && !stream.shards.is_empty() {
            return stream.stream_arn;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("stream {name} did not become ACTIVE");
}

async fn split_stream(store: &Store, name: &str, shard_id: &str) {
    let stream = store.get_stream(name).await.unwrap();
    let shard = stream
        .shards
        .iter()
        .find(|candidate| candidate.shard_id == shard_id)
        .unwrap();
    let mid = (shard.hash_key_range.start_u128() + shard.hash_key_range.end_u128()) / 2;

    dispatch(
        store,
        Operation::SplitShard,
        json!({
            "StreamName": name,
            "ShardToSplit": shard_id,
            "NewStartingHashKey": mid.to_string(),
        }),
    )
    .await
    .unwrap();
}

async fn request_update_shard_count(store: &Store, name: &str, target_shard_count: u32) {
    dispatch(
        store,
        Operation::UpdateShardCount,
        json!({
            "StreamName": name,
            "TargetShardCount": target_shard_count,
        }),
    )
    .await
    .unwrap();
}

async fn wait_for_stream_active(store: &Store, name: &str, min_open_shards: usize) {
    for _ in 0..80 {
        let stream = store.get_stream(name).await.unwrap();
        let open_shards = stream
            .shards
            .iter()
            .filter(|shard| shard.sequence_number_range.ending_sequence_number.is_none())
            .count();
        if stream.stream_status == StreamStatus::Active && open_shards >= min_open_shards {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("stream {name} did not become ACTIVE with {min_open_shards} open shards");
}

async fn wait_for_stream_deleted(store: &Store, name: &str) {
    for _ in 0..80 {
        if !store.contains_stream(name).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("stream {name} was not deleted");
}

async fn register_consumer(store: &Store, stream_arn: &str, consumer_name: &str) -> String {
    let body = dispatch(
        store,
        Operation::RegisterStreamConsumer,
        json!({
            "StreamARN": stream_arn,
            "ConsumerName": consumer_name,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    body["Consumer"]["ConsumerARN"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn wait_for_consumer_status(store: &Store, consumer_arn: &str, expected: ConsumerStatus) {
    for _ in 0..80 {
        if let Some(consumer) = store.get_consumer(consumer_arn).await
            && consumer.consumer_status == expected
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("consumer {consumer_arn} did not reach {expected}");
}

async fn wait_for_consumer_deleted(store: &Store, consumer_arn: &str) {
    for _ in 0..80 {
        if store.get_consumer(consumer_arn).await.is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("consumer {consumer_arn} was not deleted");
}

async fn put_record(
    store: &Store,
    name: &str,
    data: &str,
    partition_key: &str,
) -> Result<Option<Value>, ferrokinesis::error::KinesisErrorResponse> {
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
}

async fn put_records(
    store: &Store,
    name: &str,
    records: &[(&str, &str)],
) -> Result<Option<Value>, ferrokinesis::error::KinesisErrorResponse> {
    dispatch(
        store,
        Operation::PutRecords,
        json!({
            "StreamName": name,
            "Records": records
                .iter()
                .map(|(data, partition_key)| json!({"Data": data, "PartitionKey": partition_key}))
                .collect::<Vec<_>>(),
        }),
    )
    .await
}

fn latest_snapshot_from_wal(state_dir: &Path) -> Value {
    let (_, entries) = Persistence::new(state_dir.to_path_buf())
        .unwrap()
        .load()
        .unwrap()
        .unwrap();
    let snapshot = entries
        .into_iter()
        .rev()
        .find_map(|entry| match entry {
            WalEntry::Snapshot(snapshot) => Some(snapshot),
        })
        .expect("wal snapshot");
    serde_json::to_value(snapshot).unwrap()
}

fn write_legacy_snapshot(state_dir: &Path, mut snapshot: Value) {
    snapshot
        .as_object_mut()
        .unwrap()
        .remove("pending_transitions");
    fs::write(
        state_dir.join("snapshot.bin"),
        serde_json::to_vec(&snapshot).unwrap(),
    )
    .unwrap();
    fs::write(state_dir.join("wal.log"), []).unwrap();
}

fn break_wal(state_dir: &Path) {
    let wal_path = state_dir.join("wal.log");
    let _ = fs::remove_file(&wal_path);
    fs::create_dir(&wal_path).unwrap();
}

async fn insert_backdated_record(store: &Store, stream_name: &str, seq_ix: u64, seq_time: u64) {
    let stream = store.get_stream(stream_name).await.unwrap();
    let shard = &stream.shards[0];
    let shard_create_time =
        sequence::parse_sequence(&shard.sequence_number_range.starting_sequence_number)
            .unwrap()
            .shard_create_time;
    let seq = sequence::stringify_sequence(&sequence::SeqObj {
        shard_create_time,
        seq_ix: Some(seq_ix),
        seq_time: Some(seq_time),
        shard_ix: 0,
        byte1: None,
        seq_rand: None,
        version: 2,
    });
    let key = format!("{}/{}", sequence::shard_ix_to_hex(0), seq);
    store
        .put_record(
            stream_name,
            &key,
            &StoredRecord {
                partition_key: format!("pk-{seq_ix}"),
                data: "AAAA".to_string(),
                approximate_arrival_timestamp: (seq_time / 1000) as f64,
            },
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
    put_record(&store, stream_name, "AAAA", "pk").await.unwrap();
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
    put_record(&store, stream_name, "AAAA", "pk").await.unwrap();
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
    put_record(&store, stream_name, "AAAA", "pk").await.unwrap();
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

#[tokio::test]
async fn durable_store_resumes_create_stream_transition_after_restart() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());

    dispatch(
        &store,
        Operation::CreateStream,
        json!({"StreamName": "restart-create", "ShardCount": 1}),
    )
    .await
    .unwrap();
    drop(store);

    let recovered = Store::new(options);
    wait_for_stream_active(&recovered, "restart-create", 1).await;
}

#[tokio::test]
async fn durable_store_resumes_delete_stream_transition_after_restart() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());
    create_active_stream(&store, "restart-delete").await;

    dispatch(
        &store,
        Operation::DeleteStream,
        json!({"StreamName": "restart-delete"}),
    )
    .await
    .unwrap();
    drop(store);

    let recovered = Store::new(options);
    wait_for_stream_deleted(&recovered, "restart-delete").await;
}

#[tokio::test]
async fn durable_store_resumes_register_consumer_transition_after_restart() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());
    let stream_arn = create_active_stream(&store, "restart-register-consumer").await;
    let consumer_arn = register_consumer(&store, &stream_arn, "consumer-a").await;
    drop(store);

    let recovered = Store::new(options);
    wait_for_consumer_status(&recovered, &consumer_arn, ConsumerStatus::Active).await;
}

#[tokio::test]
async fn durable_store_resumes_deregister_consumer_transition_after_restart() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());
    let stream_arn = create_active_stream(&store, "restart-deregister-consumer").await;
    let consumer_arn = register_consumer(&store, &stream_arn, "consumer-a").await;
    wait_for_consumer_status(&store, &consumer_arn, ConsumerStatus::Active).await;

    dispatch(
        &store,
        Operation::DeregisterStreamConsumer,
        json!({"ConsumerARN": consumer_arn}),
    )
    .await
    .unwrap();
    drop(store);

    let recovered = Store::new(options);
    wait_for_consumer_deleted(&recovered, &consumer_arn).await;
}

#[tokio::test]
async fn durable_store_resumes_update_shard_count_transition_after_restart() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());
    create_active_stream(&store, "restart-reshard").await;

    dispatch(
        &store,
        Operation::UpdateShardCount,
        json!({"StreamName": "restart-reshard", "TargetShardCount": 2}),
    )
    .await
    .unwrap();
    drop(store);

    let recovered = Store::new(options);
    wait_for_stream_active(&recovered, "restart-reshard", 2).await;
}

#[tokio::test]
async fn durable_store_resumes_start_encryption_transition_after_restart() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());
    create_active_stream(&store, "restart-start-encryption").await;

    dispatch(
        &store,
        Operation::StartStreamEncryption,
        json!({
            "StreamName": "restart-start-encryption",
            "EncryptionType": "KMS",
            "KeyId": "key-123",
        }),
    )
    .await
    .unwrap();
    drop(store);

    let recovered = Store::new(options);
    wait_for_stream_active(&recovered, "restart-start-encryption", 1).await;
    let stream = recovered
        .get_stream("restart-start-encryption")
        .await
        .unwrap();
    assert_eq!(stream.encryption_type, EncryptionType::Kms);
}

#[tokio::test]
async fn durable_store_resumes_stop_encryption_transition_after_restart() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());
    create_active_stream(&store, "restart-stop-encryption").await;

    dispatch(
        &store,
        Operation::StartStreamEncryption,
        json!({
            "StreamName": "restart-stop-encryption",
            "EncryptionType": "KMS",
            "KeyId": "key-123",
        }),
    )
    .await
    .unwrap();
    wait_for_stream_active(&store, "restart-stop-encryption", 1).await;

    dispatch(
        &store,
        Operation::StopStreamEncryption,
        json!({
            "StreamName": "restart-stop-encryption",
            "EncryptionType": "KMS",
        }),
    )
    .await
    .unwrap();
    drop(store);

    let recovered = Store::new(options);
    wait_for_stream_active(&recovered, "restart-stop-encryption", 1).await;
    let stream = recovered
        .get_stream("restart-stop-encryption")
        .await
        .unwrap();
    assert_eq!(stream.encryption_type, EncryptionType::None);
    assert!(stream.key_id.is_none());
}

#[tokio::test]
async fn superseded_create_stream_transition_does_not_poison_store() {
    let dir = tempdir().unwrap();
    let mut options = durable_options(dir.path(), 0);
    options.create_stream_ms = 100;
    options.delete_stream_ms = 100;
    let store = Store::new(options);

    create_active_stream(&store, "steady-stream").await;

    dispatch(
        &store,
        Operation::CreateStream,
        json!({
            "StreamName": "race-delete",
            "ShardCount": 1,
        }),
    )
    .await
    .unwrap();
    dispatch(
        &store,
        Operation::DeleteStream,
        json!({
            "StreamName": "race-delete",
        }),
    )
    .await
    .unwrap();

    wait_for_stream_deleted(&store, "race-delete").await;
    assert!(store.check_ready().is_ok());
    put_record(&store, "steady-stream", "AAAA", "pk")
        .await
        .unwrap();
}

#[tokio::test]
async fn superseded_register_consumer_transition_does_not_poison_store() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options);
    let stream_arn = create_active_stream(&store, "consumer-race").await;

    let consumer_arn = register_consumer(&store, &stream_arn, "consumer-a").await;
    dispatch(
        &store,
        Operation::DeregisterStreamConsumer,
        json!({
            "ConsumerARN": consumer_arn,
        }),
    )
    .await
    .unwrap();

    wait_for_consumer_deleted(&store, &consumer_arn).await;
    assert!(store.check_ready().is_ok());
}

#[tokio::test]
async fn legacy_snapshot_recovers_consumer_creating_without_transition_metadata() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());
    let stream_arn = create_active_stream(&store, "legacy-consumer-creating").await;
    let consumer_arn = register_consumer(&store, &stream_arn, "consumer-a").await;
    drop(store);

    let snapshot = latest_snapshot_from_wal(dir.path());
    write_legacy_snapshot(dir.path(), snapshot);

    let recovered = Store::new(options);
    wait_for_consumer_status(&recovered, &consumer_arn, ConsumerStatus::Active).await;
}

#[tokio::test]
async fn legacy_snapshot_recovers_stream_deleting_without_transition_metadata() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());
    create_active_stream(&store, "legacy-stream-deleting").await;

    dispatch(
        &store,
        Operation::DeleteStream,
        json!({"StreamName": "legacy-stream-deleting"}),
    )
    .await
    .unwrap();
    drop(store);

    let snapshot = latest_snapshot_from_wal(dir.path());
    write_legacy_snapshot(dir.path(), snapshot);

    let recovered = Store::new(options);
    wait_for_stream_deleted(&recovered, "legacy-stream-deleting").await;
}

#[tokio::test]
async fn legacy_snapshot_marks_store_unhealthy_for_ambiguous_stream_transition() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());

    dispatch(
        &store,
        Operation::CreateStream,
        json!({"StreamName": "legacy-stream-creating", "ShardCount": 1}),
    )
    .await
    .unwrap();
    drop(store);

    let snapshot = latest_snapshot_from_wal(dir.path());
    write_legacy_snapshot(dir.path(), snapshot);

    let recovered = Store::new(options);
    let err = recovered.check_ready().unwrap_err().to_string();
    assert!(err.contains("ambiguous persisted stream transition"));
}

#[tokio::test]
async fn durable_store_rejects_put_record_after_wal_failure_and_rolls_back_state() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options);
    create_active_stream(&store, "wal-failure").await;

    fs::remove_file(dir.path().join("wal.log")).unwrap();
    fs::create_dir(dir.path().join("wal.log")).unwrap();

    let err = put_record(&store, "wal-failure", "AAAA", "pk")
        .await
        .unwrap_err();
    assert_eq!(err.status_code, 500);
    let batch_err = put_records(&store, "wal-failure", &[("QUFBQQ==", "pk-a")])
        .await
        .unwrap_err();
    assert_eq!(batch_err.status_code, 500);
    assert!(store.check_ready().is_err());
    assert!(store.get_record_store("wal-failure").await.is_empty());
}

#[tokio::test]
async fn durable_store_rolls_back_create_stream_when_persistence_fails() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options);

    break_wal(dir.path());

    let err = dispatch(
        &store,
        Operation::CreateStream,
        json!({
            "StreamName": "create-rollback",
            "ShardCount": 1,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status_code, 500);
    assert!(!store.contains_stream("create-rollback").await);
}

#[tokio::test]
async fn durable_store_reserves_pending_create_shards_after_restart() {
    let dir = tempdir().unwrap();
    let mut options = durable_options(dir.path(), 0);
    options.create_stream_ms = 500;
    options.shard_limit = 1;

    let store = Store::new(options.clone());
    dispatch(
        &store,
        Operation::CreateStream,
        json!({
            "StreamName": "pending-reserved",
            "ShardCount": 1,
        }),
    )
    .await
    .unwrap();
    drop(store);

    let recovered = Store::new(options);
    let err = dispatch(
        &recovered,
        Operation::CreateStream,
        json!({
            "StreamName": "pending-second",
            "ShardCount": 1,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.body.error_type, "LimitExceededException");
}

#[tokio::test]
async fn durable_store_reserves_pending_split_shards_after_restart() {
    let dir = tempdir().unwrap();
    let mut options = durable_options(dir.path(), 0);
    options.update_stream_ms = 500;
    options.shard_limit = 2;

    let store = Store::new(options.clone());
    create_active_stream(&store, "pending-split-reserved").await;
    split_stream(&store, "pending-split-reserved", "shardId-000000000000").await;
    drop(store);

    let recovered = Store::new(options);
    let err = dispatch(
        &recovered,
        Operation::CreateStream,
        json!({
            "StreamName": "blocked-after-split-restart",
            "ShardCount": 1,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.body.error_type, "LimitExceededException");
}

#[tokio::test]
async fn durable_store_reserves_pending_update_shard_count_after_restart() {
    let dir = tempdir().unwrap();
    let mut options = durable_options(dir.path(), 0);
    options.update_stream_ms = 500;
    options.shard_limit = 2;

    let store = Store::new(options.clone());
    create_active_stream(&store, "pending-reshard-reserved").await;
    request_update_shard_count(&store, "pending-reshard-reserved", 2).await;
    drop(store);

    let recovered = Store::new(options);
    let err = dispatch(
        &recovered,
        Operation::CreateStream,
        json!({
            "StreamName": "blocked-after-reshard-restart",
            "ShardCount": 1,
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.body.error_type, "LimitExceededException");
}

#[tokio::test]
async fn durable_store_rolls_back_delete_stream_when_persistence_fails() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options);
    create_active_stream(&store, "delete-rollback").await;

    break_wal(dir.path());

    let err = dispatch(
        &store,
        Operation::DeleteStream,
        json!({
            "StreamName": "delete-rollback",
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status_code, 500);

    let stream = store.get_stream("delete-rollback").await.unwrap();
    assert_eq!(stream.stream_status, StreamStatus::Active);
}

#[tokio::test]
async fn durable_store_rolls_back_register_consumer_when_persistence_fails() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options);
    let stream_arn = create_active_stream(&store, "consumer-rollback").await;

    break_wal(dir.path());

    let err = dispatch(
        &store,
        Operation::RegisterStreamConsumer,
        json!({
            "StreamARN": stream_arn,
            "ConsumerName": "consumer-a",
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status_code, 500);
    assert!(
        store
            .list_consumers_for_stream(&stream_arn)
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn durable_store_rolls_back_account_settings_when_persistence_fails() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options);

    break_wal(dir.path());

    let err = dispatch(
        &store,
        Operation::UpdateAccountSettings,
        json!({
            "MinimumThroughputBillingCommitment": {
                "Status": "ACTIVE",
                "CommitmentDuration": "P30D"
            }
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.status_code, 500);
    assert_eq!(store.get_account_settings().await, json!({}));
}

#[tokio::test]
async fn durable_store_rolls_back_delete_record_keys_when_persistence_fails() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options);
    store
        .put_record(
            "delete-keys-rollback",
            "aabbccdd/seq001",
            &StoredRecord {
                partition_key: "pk".to_string(),
                data: "AAAA".to_string(),
                approximate_arrival_timestamp: 1.0,
            },
        )
        .await
        .unwrap();

    let expected_bytes = store.metrics().retained_bytes();
    let expected_records = store.metrics().retained_records();
    break_wal(dir.path());

    store
        .delete_record_keys("delete-keys-rollback", &["aabbccdd/seq001".to_string()])
        .await;

    assert_eq!(
        store.get_record_store("delete-keys-rollback").await.len(),
        1
    );
    assert_eq!(store.metrics().retained_bytes(), expected_bytes);
    assert_eq!(store.metrics().retained_records(), expected_records);
    assert!(store.check_ready().is_err());
}

#[tokio::test]
async fn durable_store_rolls_back_delete_expired_records_when_persistence_fails() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options);
    create_active_stream(&store, "expired-rollback").await;

    let expired_time = current_time_ms() - 25 * 60 * 60 * 1000;
    insert_backdated_record(&store, "expired-rollback", 1, expired_time).await;

    let expected_bytes = store.metrics().retained_bytes();
    let expected_records = store.metrics().retained_records();
    break_wal(dir.path());

    let deleted = store.delete_expired_records("expired-rollback", 24).await;

    assert_eq!(deleted, 0);
    assert_eq!(store.get_record_store("expired-rollback").await.len(), 1);
    assert_eq!(store.metrics().retained_bytes(), expected_bytes);
    assert_eq!(store.metrics().retained_records(), expected_records);
    assert!(store.check_ready().is_err());
}

#[tokio::test]
async fn durable_store_rejects_subsequent_writes_after_snapshot_failure() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 1);
    let store = Store::new(options);
    create_active_stream(&store, "snapshot-failure").await;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    fs::create_dir(dir.path().join("snapshot.bin.tmp")).unwrap();

    put_record(&store, "snapshot-failure", "AAAA", "pk-1")
        .await
        .unwrap();
    assert!(store.check_ready().is_err());

    let err = put_record(&store, "snapshot-failure", "BBBB", "pk-2")
        .await
        .unwrap_err();
    assert_eq!(err.status_code, 500);
    let batch_err = put_records(&store, "snapshot-failure", &[("QkJCQg==", "pk-a")])
        .await
        .unwrap_err();
    assert_eq!(batch_err.status_code, 500);
}

#[tokio::test]
async fn recovered_store_rejects_writes_when_replay_failed() {
    let dir = tempdir().unwrap();
    let options = durable_options(dir.path(), 0);
    let store = Store::new(options.clone());
    create_active_stream(&store, "replay-failure").await;
    put_record(&store, "replay-failure", "AAAA", "pk")
        .await
        .unwrap();
    drop(store);

    let mut wal = OpenOptions::new()
        .append(true)
        .open(dir.path().join("wal.log"))
        .unwrap();
    wal.write_all(&[0xde, 0xad, 0xbe]).unwrap();
    wal.sync_all().unwrap();

    let recovered = Store::new(options);
    let err = put_record(&recovered, "replay-failure", "BBBB", "pk-2")
        .await
        .unwrap_err();
    assert_eq!(err.status_code, 500);
    let batch_err = put_records(&recovered, "replay-failure", &[("QkJCQg==", "pk-a")])
        .await
        .unwrap_err();
    assert_eq!(batch_err.status_code, 500);
}

#[tokio::test]
async fn retained_bytes_cap_rejects_put_record_and_put_records_without_growth() {
    let dir = tempdir().unwrap();
    let mut options = durable_options(dir.path(), 0);
    options.max_retained_bytes = Some(10);
    let store = Store::new(options);
    create_active_stream(&store, "retained-cap").await;

    let err = put_record(&store, "retained-cap", "QUFBQQ==", "pk")
        .await
        .unwrap_err();
    assert_eq!(err.body.error_type, "LimitExceededException");
    assert_eq!(store.metrics().retained_bytes(), 0);
    assert_eq!(store.metrics().retained_records(), 0);

    let err = put_records(
        &store,
        "retained-cap",
        &[("QUFBQQ==", "pk-a"), ("QkJCQg==", "pk-b")],
    )
    .await
    .unwrap_err();
    assert_eq!(err.body.error_type, "LimitExceededException");
    assert_eq!(store.metrics().retained_bytes(), 0);
    assert_eq!(store.metrics().retained_records(), 0);
}

#[tokio::test]
async fn delete_record_keys_uses_actual_deletions_for_metrics() {
    let store = Store::new(StoreOptions::default());
    store
        .put_record(
            "metrics-delete",
            "aabbccdd/seq001",
            &ferrokinesis::types::StoredRecord {
                partition_key: "pk".to_string(),
                data: "AAAA".to_string(),
                approximate_arrival_timestamp: 1.0,
            },
        )
        .await
        .unwrap();

    store
        .delete_record_keys(
            "metrics-delete",
            &["aabbccdd/seq001".to_string(), "aabbccdd/seq001".to_string()],
        )
        .await;

    assert_eq!(store.metrics().retained_records(), 0);
    assert_eq!(store.metrics().retained_bytes(), 0);
}
