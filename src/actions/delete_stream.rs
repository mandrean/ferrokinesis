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
    tracing::info!(stream = stream_name, "stream deletion initiated");

    // Delete after delay
    let store_clone = store.clone();
    let name = stream_name.to_string();
    let delay = store.options.delete_stream_ms;
    crate::runtime::spawn_background(async move {
        crate::runtime::sleep_ms(delay).await;
        store_clone.delete_stream(&name).await;
    });

    Ok(None)
}
