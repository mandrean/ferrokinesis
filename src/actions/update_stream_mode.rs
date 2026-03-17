use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_arn = data[constants::STREAM_ARN].as_str().unwrap_or("");
    let stream_mode = data[constants::STREAM_MODE_DETAILS]["StreamMode"]
        .as_str()
        .unwrap_or("");

    if stream_mode != "PROVISIONED" && stream_mode != "ON_DEMAND" {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("StreamMode must be PROVISIONED or ON_DEMAND."),
        ));
    }

    let stream_name = store
        .stream_name_from_arn(stream_arn)
        .unwrap_or_default();

    store
        .update_stream(&stream_name, |stream| {
            stream.stream_mode_details.stream_mode = stream_mode.to_string();
            Ok(())
        })
        .await?;

    Ok(None)
}
