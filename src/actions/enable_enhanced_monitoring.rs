use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

const ALL_METRICS: &[&str] = &[
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds",
];

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data["StreamName"].as_str().unwrap_or("");
    let metrics = data["ShardLevelMetrics"]
        .as_array()
        .ok_or_else(|| {
            KinesisErrorResponse::client_error("SerializationException", None)
        })?;

    let requested: Vec<String> = metrics
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

            let mut desired = current.clone();
            for metric in &requested {
                if metric == "ALL" {
                    desired = ALL_METRICS.iter().map(|s| s.to_string()).collect();
                    break;
                }
                if !desired.contains(metric) {
                    desired.push(metric.clone());
                }
            }

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
