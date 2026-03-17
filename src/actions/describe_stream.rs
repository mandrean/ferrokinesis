use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data["StreamName"].as_str().unwrap_or("");
    let stream = store.get_stream(stream_name).await?;

    Ok(Some(json!({
        "StreamDescription": stream
    })))
}
