use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::{PendingTransition, Store, TransitionMutation};
use crate::types::StreamStatus;
use crate::util::current_time_ms;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let encryption_type = data[constants::ENCRYPTION_TYPE].as_str().unwrap_or("");
    let key_id = data[constants::KEY_ID].as_str().unwrap_or("");

    if encryption_type != constants::ENCRYPTION_KMS {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("EncryptionType must be KMS."),
        ));
    }

    let delay = store.options.update_stream_ms;
    let transition = PendingTransition::StartStreamEncryption {
        stream_name: stream_name.to_string(),
        ready_at_ms: current_time_ms().saturating_add(delay),
    };

    store
        .update_stream_with_transition(
            stream_name,
            TransitionMutation::Upsert(transition.clone()),
            |stream| {
                if stream.stream_status != StreamStatus::Active {
                    return Err(KinesisErrorResponse::client_error(
                        constants::RESOURCE_IN_USE,
                        Some(&format!(
                            "Stream {} under account {} not ACTIVE, instead in state {}",
                            stream_name, store.aws_account_id, stream.stream_status
                        )),
                    ));
                }

                stream.stream_status = StreamStatus::Updating;
                stream.key_id = Some(key_id.to_string());
                Ok(())
            },
        )
        .await?;
    tracing::info!(stream = stream_name, "encryption started");
    store.schedule_transition(transition);

    Ok(None)
}
