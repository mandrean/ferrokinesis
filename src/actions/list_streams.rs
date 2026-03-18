use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let limit = data[constants::LIMIT].as_u64().unwrap_or(10) as usize;
    let start_name = data[constants::EXCLUSIVE_START_STREAM_NAME].as_str();

    let all_names = store.list_stream_names().await;
    let names: Vec<&String> = if let Some(start) = start_name {
        all_names
            .iter()
            .filter(|k| k.as_str() > start)
            .take(limit + 1)
            .collect()
    } else {
        all_names.iter().take(limit + 1).collect()
    };

    let has_more = names.len() > limit;
    let stream_names: Vec<&str> = names.iter().take(limit).map(|s| s.as_str()).collect();

    let mut summaries = Vec::with_capacity(stream_names.len());
    for name in &stream_names {
        if let Ok(stream) = store.get_stream(name).await {
            summaries.push(json!({
                "StreamARN": stream.stream_arn,
                "StreamCreationTimestamp": stream.stream_creation_timestamp,
                "StreamModeDetails": stream.stream_mode_details,
                "StreamName": stream.stream_name,
                "StreamStatus": stream.stream_status,
            }));
        }
    }

    Ok(Some(json!({
        "HasMoreStreams": has_more,
        "StreamNames": stream_names,
        "StreamSummaries": summaries,
    })))
}
