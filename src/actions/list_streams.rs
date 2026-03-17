use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let limit = data[constants::LIMIT].as_u64().unwrap_or(10) as usize;
    let start_name = data["ExclusiveStartStreamName"].as_str();

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

    Ok(Some(json!({
        "StreamNames": stream_names,
        "HasMoreStreams": has_more,
    })))
}
