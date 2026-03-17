use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str();
    let next_token = data[constants::NEXT_TOKEN].as_str();
    let max_results = data[constants::MAX_RESULTS].as_u64().unwrap_or(10000) as usize;

    if stream_name.is_none() && next_token.is_none() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("Either NextToken or StreamName should be provided."),
        ));
    }
    if stream_name.is_some() && next_token.is_some() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("NextToken and StreamName cannot be provided together."),
        ));
    }

    let name = stream_name.unwrap_or("");
    let stream = store.get_stream(name).await?;

    let exclusive_start = data[constants::EXCLUSIVE_START_SHARD_ID].as_str();

    let filtered_shards: Vec<_> = if let Some(start_id) = exclusive_start {
        stream
            .shards
            .iter()
            .filter(|s| s.shard_id.as_str() > start_id)
            .cloned()
            .collect()
    } else {
        stream.shards.clone()
    };

    let has_more = filtered_shards.len() > max_results;
    let output_shards: Vec<_> = filtered_shards.into_iter().take(max_results).collect();

    let mut result = json!({
        "Shards": output_shards,
    });

    if has_more {
        // Use the last shard ID as a simple next token
        if let Some(last) = output_shards.last() {
            result["NextToken"] = json!(last.shard_id);
        }
    }

    Ok(Some(result))
}
