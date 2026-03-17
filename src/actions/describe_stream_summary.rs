use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data["StreamName"].as_str().unwrap_or("");
    let stream = store.get_stream(stream_name).await?;

    let open_shard_count = stream
        .shards
        .iter()
        .filter(|s| s.sequence_number_range.ending_sequence_number.is_none())
        .count();

    Ok(Some(json!({
        "StreamDescriptionSummary": {
            "RetentionPeriodHours": stream.retention_period_hours,
            "EnhancedMonitoring": stream.enhanced_monitoring,
            "EncryptionType": stream.encryption_type,
            "StreamARN": stream.stream_arn,
            "StreamName": stream.stream_name,
            "StreamStatus": stream.stream_status,
            "StreamCreationTimestamp": stream.stream_creation_timestamp,
            "OpenShardCount": open_shard_count,
            "ConsumerCount": 0,
        }
    })))
}
