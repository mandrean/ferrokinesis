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
    Ok(Some(json!({
        "StreamDescriptionSummary": {
            "RetentionPeriodHours": stream.retention_period_hours,
            "EnhancedMonitoring": stream.enhanced_monitoring,
            "EncryptionType": stream.encryption_type,
            "KeyId": stream.key_id,
            "StreamARN": stream.stream_arn,
            "StreamName": stream.stream_name,
            "StreamStatus": stream.stream_status,
            "StreamCreationTimestamp": stream.stream_creation_timestamp,
            "StreamModeDetails": stream.stream_mode_details,
            "OpenShardCount": open_shard_count,
            "ConsumerCount": consumer_count,
        }
    })))
}
