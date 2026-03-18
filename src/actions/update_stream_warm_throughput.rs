use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use crate::types::StreamStatus;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let name = store.resolve_stream_name(&data)?;

    let target_mibps = data[constants::WARM_THROUGHPUT_MIBPS]
        .as_i64()
        .ok_or_else(|| {
            KinesisErrorResponse::client_error(
                constants::INVALID_ARGUMENT,
                Some("WarmThroughputMiBps is required."),
            )
        })? as u32;

    let result = store
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

            let current = stream.warm_throughput_mibps;
            stream.warm_throughput_mibps = target_mibps;

            Ok(json!({
                "StreamARN": stream.stream_arn,
                "StreamName": stream.stream_name,
                "WarmThroughput": {
                    "CurrentMiBps": current,
                    "TargetMiBps": target_mibps,
                }
            }))
        })
        .await?;

    Ok(Some(result))
}
