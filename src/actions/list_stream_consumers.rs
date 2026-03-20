use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_arn = data[constants::STREAM_ARN].as_str().unwrap_or("");
    let max_results = data[constants::MAX_RESULTS].as_u64().unwrap_or(100) as usize;

    // Verify stream exists
    let stream_name = store.stream_name_from_arn(stream_arn).unwrap_or_default();
    store.get_stream(&stream_name).await?;

    let consumers = store.list_consumers_for_stream(stream_arn).await;

    let has_more = consumers.len() > max_results;
    let output: Vec<_> = consumers.into_iter().take(max_results).collect();

    let mut result = json!({
        "Consumers": output,
    });

    if has_more {
        result["NextToken"] = json!("next");
    }

    tracing::trace!(stream = %stream_name, consumers = output.len(), has_more, "consumers listed");
    Ok(Some(result))
}
