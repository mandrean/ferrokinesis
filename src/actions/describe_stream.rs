use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let stream = store.get_stream(stream_name).await?;

    tracing::trace!(stream = stream_name, "stream described");
    Ok(Some(json!({
        "StreamDescription": stream
    })))
}
