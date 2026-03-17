use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data["StreamName"].as_str().unwrap_or("");
    let limit = data["Limit"].as_u64().unwrap_or(100) as usize;
    let exclusive_start = data["ExclusiveStartTagKey"].as_str();

    let stream = store.get_stream(stream_name).await?;

    let mut keys: Vec<&String> = stream.tags.keys().collect();
    keys.sort();

    if let Some(start) = exclusive_start {
        keys.retain(|k| k.as_str() > start);
    }

    let has_more_tags = keys.len() > limit;
    let tags: Vec<Value> = keys
        .iter()
        .take(limit)
        .map(|key| {
            json!({
                "Key": key,
                "Value": stream.tags[key.as_str()],
            })
        })
        .collect();

    Ok(Some(json!({
        "Tags": tags,
        "HasMoreTags": has_more_tags,
    })))
}
