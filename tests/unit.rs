use ferrokinesis::store::StoreOptions;
use ferrokinesis::types::{ConsumerStatus, StreamStatus};

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
            StoredRecord {
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

#[tokio::test]
async fn store_with_streams_write_direct() {
    use ferrokinesis::store::Store;

    let store = Store::new(StoreOptions::default());
    let count = store
        .with_streams_write(|streams, _opts, _account, _region| streams.len())
        .await;
    assert_eq!(count, 0);
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
