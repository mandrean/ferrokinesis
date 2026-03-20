use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let retention_hours = data[constants::RETENTION_PERIOD_HOURS]
        .as_i64()
        .unwrap_or(0) as u32;

    if retention_hours < 24 {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some(&format!(
                "Minimum allowed retention period is 24 hours. Requested retention period ({} hours) is too short.",
                retention_hours
            )),
        ));
    }

    store
        .update_stream(stream_name, |stream| {
            if stream.retention_period_hours > retention_hours {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(&format!(
                        "Requested retention period ({} hours) for stream {} can not be shorter than existing retention period ({} hours). Use DecreaseRetentionPeriod API.",
                        retention_hours, stream_name, stream.retention_period_hours
                    )),
                ));
            }
            stream.retention_period_hours = retention_hours;
            Ok(())
        })
        .await?;

    tracing::trace!(
        stream = %stream_name,
        retention_hours,
        "retention period increased"
    );
    Ok(None)
}
