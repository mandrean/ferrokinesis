use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use crate::types::StreamStatus;
use serde_json::Value;

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let encryption_type = data[constants::ENCRYPTION_TYPE].as_str().unwrap_or("");

    if encryption_type != "KMS" {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("EncryptionType must be KMS."),
        ));
    }

    store
        .update_stream(stream_name, |stream| {
            if stream.stream_status != StreamStatus::Active {
                return Err(KinesisErrorResponse::client_error(
                    constants::RESOURCE_IN_USE,
                    Some(&format!(
                        "Stream {} under account {} not ACTIVE, instead in state {}",
                        stream_name, store.aws_account_id, stream.stream_status
                    )),
                ));
            }

            if stream.encryption_type != "KMS" {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(&format!(
                        "Stream {} under account {} is not encrypted with KMS.",
                        stream_name, store.aws_account_id
                    )),
                ));
            }

            stream.stream_status = StreamStatus::Updating;
            Ok(())
        })
        .await?;

    let store_clone = store.clone();
    let name = stream_name.to_string();
    let delay = store.options.update_stream_ms;
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
        let _ = store_clone
            .update_stream(&name, |stream| {
                stream.stream_status = StreamStatus::Active;
                stream.encryption_type = "NONE".to_string();
                stream.key_id = None;
                Ok(())
            })
            .await;
    });

    Ok(None)
}
