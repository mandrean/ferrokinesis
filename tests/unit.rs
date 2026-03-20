use ferrokinesis::store::StoreOptions;
use ferrokinesis::types::{ConsumerStatus, StoredRecord, StoredRecordRef, StreamStatus};

#[test]
fn stream_status_display_all_variants() {
    assert_eq!(StreamStatus::Active.to_string(), "ACTIVE");
    assert_eq!(StreamStatus::Creating.to_string(), "CREATING");
    assert_eq!(StreamStatus::Deleting.to_string(), "DELETING");
    assert_eq!(StreamStatus::Updating.to_string(), "UPDATING");
}

#[test]
fn consumer_status_display_all_variants() {
    assert_eq!(ConsumerStatus::Creating.to_string(), "CREATING");
    assert_eq!(ConsumerStatus::Deleting.to_string(), "DELETING");
    assert_eq!(ConsumerStatus::Active.to_string(), "ACTIVE");
}

#[test]
fn store_options_default_values() {
    let opts = StoreOptions::default();
    assert_eq!(opts.create_stream_ms, 500);
    assert_eq!(opts.delete_stream_ms, 500);
    assert_eq!(opts.update_stream_ms, 500);
    assert_eq!(opts.shard_limit, 10);
}

#[tokio::test]
async fn store_get_records_range_direct() {
    use ferrokinesis::store::Store;
    use ferrokinesis::types::StoredRecord;

    let store = Store::new(StoreOptions::default());
    store
        .put_record(
            "test-grr",
            "aabbccdd/seq001",
            &StoredRecord {
                partition_key: "pk".to_string(),
                data: "AAAA".to_string(),
                approximate_arrival_timestamp: 1.0,
            },
        )
        .await;

    let records = store
        .get_records_range("test-grr", "aabbccdd/", "aabbccde/")
        .await;
    assert_eq!(records.len(), 1);

    let empty = store
        .get_records_range("test-grr", "ffffffff/", "ffffffff0/")
        .await;
    assert_eq!(empty.len(), 0);
}

#[test]
fn error_server_error_with_some_args() {
    use ferrokinesis::error::KinesisErrorResponse;

    let err = KinesisErrorResponse::server_error(Some("CustomError"), Some("custom message"));
    assert_eq!(err.status_code, 500);
    assert_eq!(err.body.error_type, "CustomError");
    assert_eq!(err.body.message.as_deref(), Some("custom message"));

    let _ = format!("{:?}", err.body);
    let _ = format!("{:?}", err);
    let cloned = err.clone();
    assert_eq!(cloned.status_code, 500);
}

#[test]
fn error_client_error_none_message() {
    use ferrokinesis::error::KinesisErrorResponse;

    let err = KinesisErrorResponse::client_error("SomeError", None);
    assert_eq!(err.status_code, 400);
    assert!(err.body.message.is_none());
    let _ = format!("{:?}", err);
}

/// Ensures `StoredRecordRef` (write path) and `StoredRecord` (read path)
/// stay in sync for postcard's positional serialization.
#[test]
fn postcard_roundtrip_stored_record_ref_to_stored_record() {
    let record_ref = StoredRecordRef {
        partition_key: "pk-1",
        data: "dGVzdCBkYXRh",
        approximate_arrival_timestamp: 1742464000.123,
    };

    let bytes = postcard::to_allocvec(&record_ref).expect("serialize StoredRecordRef");
    let record: StoredRecord = postcard::from_bytes(&bytes).expect("deserialize StoredRecord");

    assert_eq!(record.partition_key, record_ref.partition_key);
    assert_eq!(record.data, record_ref.data);
    assert_eq!(
        record.approximate_arrival_timestamp,
        record_ref.approximate_arrival_timestamp
    );
}
