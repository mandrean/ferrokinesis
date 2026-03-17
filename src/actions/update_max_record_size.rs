use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use crate::types::StreamStatus;
use serde_json::Value;

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_arn = data[constants::STREAM_ARN].as_str().unwrap_or("");

    if stream_arn.is_empty() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("StreamARN is required."),
        ));
    }

    let max_record_size_kib = data["MaxRecordSizeInKiB"]
        .as_i64()
        .ok_or_else(|| {
            KinesisErrorResponse::client_error(
                constants::INVALID_ARGUMENT,
                Some("MaxRecordSizeInKiB is required."),
            )
        })?;

    if !(1024..=10240).contains(&max_record_size_kib) {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("MaxRecordSizeInKiB must be between 1024 and 10240."),
        ));
    }

    let name = store
        .stream_name_from_arn(stream_arn)
        .ok_or_else(|| {
            KinesisErrorResponse::client_error(
                constants::RESOURCE_NOT_FOUND,
                Some("Could not resolve stream from ARN."),
            )
        })?;

    store
        .update_stream(&name, |stream| {
            if stream.stream_status != StreamStatus::Active {
                return Err(KinesisErrorResponse::client_error(
                    constants::RESOURCE_IN_USE,
                    Some(&format!(
                        "Stream {} under account {} is not ACTIVE.",
                        name, store.aws_account_id
                    )),
                ));
            }

            stream.max_record_size_kib = max_record_size_kib as u32;
            Ok(())
        })
        .await?;

    Ok(None)
}
