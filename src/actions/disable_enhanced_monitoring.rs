use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let metrics = data[constants::SHARD_LEVEL_METRICS]
        .as_array()
        .ok_or_else(|| {
            KinesisErrorResponse::client_error(constants::SERIALIZATION_EXCEPTION, None)
        })?;

    let to_remove: Vec<String> = metrics
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    let result = store
        .update_stream(stream_name, |stream| {
            let current: Vec<String> = stream
                .enhanced_monitoring
                .first()
                .map(|m| m.shard_level_metrics.clone())
                .unwrap_or_default();

            let desired: Vec<String> = if to_remove.contains(&"ALL".to_string()) {
                vec![]
            } else {
                current
                    .iter()
                    .filter(|m| !to_remove.contains(m))
                    .cloned()
                    .collect()
            };

            stream.enhanced_monitoring = vec![crate::types::EnhancedMonitoring {
                shard_level_metrics: desired.clone(),
            }];

            Ok(json!({
                "StreamName": stream.stream_name,
                "StreamARN": stream.stream_arn,
                "CurrentShardLevelMetrics": current,
                "DesiredShardLevelMetrics": desired,
            }))
        })
        .await?;

    Ok(Some(result))
}
