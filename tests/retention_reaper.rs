mod common;

use common::TestServer;
use ferrokinesis::retention;
use ferrokinesis::sequence;
use ferrokinesis::store::StoreOptions;
use ferrokinesis::types::StoredRecord;
use ferrokinesis::util::current_time_ms;
use num_bigint::BigUint;
use serde_json::json;

/// Insert a record with a fabricated sequence number at the given `seq_time`.
async fn insert_backdated_record(
    server: &TestServer,
    stream_name: &str,
    shard_ix: i64,
    shard_create_time: u64,
    seq_ix: u64,
    seq_time: u64,
) {
    let seq = sequence::stringify_sequence(&sequence::SeqObj {
        shard_create_time,
        seq_ix: Some(BigUint::from(seq_ix)),
        seq_time: Some(seq_time),
        shard_ix,
        byte1: None,
        seq_rand: None,
        version: 2,
    });
    let key = format!("{}/{}", sequence::shard_ix_to_hex(shard_ix), seq);
    let record = StoredRecord {
        partition_key: format!("pk-{seq_ix}"),
        data: base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!("data-{seq_ix}"),
        ),
        approximate_arrival_timestamp: (seq_time as f64) / 1000.0,
    };
    server.store.put_record(stream_name, &key, record).await;
}

/// Get the shard_create_time from a stream's first shard.
async fn shard_create_time(server: &TestServer, stream_name: &str) -> u64 {
    let desc = server.describe_stream(stream_name).await;
    let start_seq = desc["StreamDescription"]["Shards"][0]["SequenceNumberRange"]
        ["StartingSequenceNumber"]
        .as_str()
        .unwrap();
    sequence::parse_sequence(start_seq)
        .unwrap()
        .shard_create_time
}

#[tokio::test]
async fn reaper_removes_expired_records() {
    let server = TestServer::new().await;
    let name = "test-reaper-removes";
    server.create_stream(name, 1).await;

    let sct = shard_create_time(&server, name).await;
    let now = current_time_ms();
    let old_time = now - 25 * 60 * 60 * 1000; // 25h ago

    for i in 0..3 {
        insert_backdated_record(&server, name, 0, sct, i, old_time + i * 1000).await;
    }

    // Verify records exist in store before sweep
    let records = server.store.get_record_store(name).await;
    assert_eq!(records.len(), 3);

    // Sweep
    retention::sweep_once(&server.store).await;

    // All expired records should be gone from store
    let records = server.store.get_record_store(name).await;
    assert_eq!(records.len(), 0);
}

#[tokio::test]
async fn reaper_preserves_non_expired_records() {
    let server = TestServer::new().await;
    let name = "test-reaper-preserves";
    server.create_stream(name, 1).await;

    let sct = shard_create_time(&server, name).await;
    let now = current_time_ms();
    let old_time = now - 25 * 60 * 60 * 1000; // 25h ago

    // Insert 2 old records
    for i in 0..2 {
        insert_backdated_record(&server, name, 0, sct, i, old_time + i * 1000).await;
    }

    // Insert 2 current records (via API so they get proper timestamps)
    server.put_record(name, "dGVzdDE=", "pk1").await;
    server.put_record(name, "dGVzdDI=", "pk2").await;

    // Verify 4 total records in store
    let records = server.store.get_record_store(name).await;
    assert_eq!(records.len(), 4);

    // Sweep
    retention::sweep_once(&server.store).await;

    // Only current records should remain in store
    let records = server.store.get_record_store(name).await;
    assert_eq!(records.len(), 2);

    // And they should be visible via GetRecords
    let iter = server
        .get_shard_iterator(name, "shardId-000000000000", "TRIM_HORIZON")
        .await;
    let result = server.get_records(&iter).await;
    assert_eq!(result["Records"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn reaper_respects_per_stream_retention() {
    let server = TestServer::new().await;
    let short = "test-reaper-short-retention";
    let long = "test-reaper-long-retention";
    server.create_stream(short, 1).await;
    server.create_stream(long, 1).await;

    // Increase long stream retention to 168h
    let res = server
        .request(
            "IncreaseStreamRetentionPeriod",
            &json!({ "StreamName": long, "RetentionPeriodHours": 168 }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let sct_short = shard_create_time(&server, short).await;
    let sct_long = shard_create_time(&server, long).await;
    let now = current_time_ms();
    let age_30h = now - 30 * 60 * 60 * 1000; // 30h ago

    // Insert 30h-old records into both streams
    for i in 0..2 {
        insert_backdated_record(&server, short, 0, sct_short, i, age_30h + i * 1000).await;
        insert_backdated_record(&server, long, 0, sct_long, i, age_30h + i * 1000).await;
    }

    // Sweep
    retention::sweep_once(&server.store).await;

    // Short retention (24h) stream should be trimmed
    let records = server.store.get_record_store(short).await;
    assert_eq!(records.len(), 0);

    // Long retention (168h) stream should still have records
    let records = server.store.get_record_store(long).await;
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn reaper_skips_non_active_streams() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 100_000, // Stay in CREATING for a long time
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    })
    .await;

    let name = "test-reaper-creating";

    // Create stream — it will remain in CREATING state
    let res = server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;
    assert_eq!(res.status(), 200);
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify it's in CREATING state
    let desc = server.describe_stream(name).await;
    assert_eq!(
        desc["StreamDescription"]["StreamStatus"]
            .as_str()
            .unwrap(),
        "CREATING"
    );

    // Stream in CREATING state has empty shards vec, so compute shard_create_time
    // using the same logic as create_stream.rs (current_time - 2000ms)
    let sct = current_time_ms() - 2000;

    let now = current_time_ms();
    let old_time = now - 25 * 60 * 60 * 1000;

    // Insert old records directly via store
    for i in 0..2 {
        insert_backdated_record(&server, name, 0, sct, i, old_time + i * 1000).await;
    }

    // Sweep
    retention::sweep_once(&server.store).await;

    // Records should NOT be removed (stream is not ACTIVE)
    let record_store = server.store.get_record_store(name).await;
    assert_eq!(record_store.len(), 2);
}
