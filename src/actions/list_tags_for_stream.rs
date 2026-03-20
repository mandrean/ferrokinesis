use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let limit = data[constants::LIMIT].as_u64().unwrap_or(100) as usize;
    let exclusive_start = data[constants::EXCLUSIVE_START_TAG_KEY].as_str();

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

    tracing::trace!(
        stream = stream_name,
        tags = tags.len(),
        has_more_tags,
        "stream tags listed"
    );
    Ok(Some(json!({
        "Tags": tags,
        "HasMoreTags": has_more_tags,
    })))
}
