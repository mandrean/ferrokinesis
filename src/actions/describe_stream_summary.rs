use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let stream = store.get_stream(stream_name).await?;

    let open_shard_count = stream
        .shards
        .iter()
        .filter(|s| s.sequence_number_range.ending_sequence_number.is_none())
        .count();

    let consumer_count = store
        .list_consumers_for_stream(&stream.stream_arn)
        .await
        .len();

    tracing::trace!(
        stream = %stream_name,
        open_shard_count,
        consumer_count,
        "stream summary described"
    );

    let mut summary = json!({
        "ConsumerCount": consumer_count,
        "EncryptionType": stream.encryption_type,
        "EnhancedMonitoring": stream.enhanced_monitoring,
        "OpenShardCount": open_shard_count,
        "RetentionPeriodHours": stream.retention_period_hours,
        "StreamARN": stream.stream_arn,
        "StreamCreationTimestamp": stream.stream_creation_timestamp,
        "StreamModeDetails": stream.stream_mode_details,
        "StreamName": stream.stream_name,
        "StreamStatus": stream.stream_status,
    });

    if let Some(ref key_id) = stream.key_id {
        summary["KeyId"] = json!(key_id);
    }
    Ok(Some(json!({
        "StreamDescriptionSummary": summary
    })))
}
