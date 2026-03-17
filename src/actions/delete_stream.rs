use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use crate::types::StreamStatus;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");

    // Verify stream exists and set to DELETING
    store
        .update_stream(stream_name, |stream| {
            stream.stream_status = StreamStatus::Deleting;
            Ok(())
        })
        .await?;

    // Delete after delay
    let store_clone = store.clone();
    let name = stream_name.to_string();
    let delay = store.options.delete_stream_ms;
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
        store_clone.delete_stream(&name).await;
    });

    Ok(None)
}
