use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use crate::types::StreamMode;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_arn = data[constants::STREAM_ARN].as_str().unwrap_or("");
    let stream_mode: StreamMode =
        serde_json::from_value(data[constants::STREAM_MODE_DETAILS]["StreamMode"].clone())
            .map_err(|_| {
                KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some("StreamMode must be PROVISIONED or ON_DEMAND."),
                )
            })?;

    let stream_name = store.stream_name_from_arn(stream_arn).unwrap_or_default();

    store
        .update_stream(&stream_name, |stream| {
            stream.stream_mode_details.stream_mode = stream_mode;
            Ok(())
        })
        .await?;

    tracing::trace!(stream = %stream_name, mode = ?stream_mode, "stream mode updated");
    Ok(None)
}
