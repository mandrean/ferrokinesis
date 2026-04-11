use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::{PendingTransition, Store, TransitionMutation};
use crate::types::*;
use crate::util::current_time_ms;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let target_shard_count = data[constants::TARGET_SHARD_COUNT].as_i64().unwrap_or(0) as u32;

    let delay = store.options.update_stream_ms;
    let transition = PendingTransition::UpdateShardCount {
        stream_name: stream_name.to_string(),
        ready_at_ms: current_time_ms().saturating_add(delay),
        target_shard_count,
    };

    let current_count = store
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

                let current_count = stream
                    .shards
                    .iter()
                    .filter(|s| s.sequence_number_range.ending_sequence_number.is_none())
                    .count() as u32;

                if target_shard_count == current_count {
                    return Err(KinesisErrorResponse::client_error(
                        constants::INVALID_ARGUMENT,
                        Some(&format!(
                            "TargetShardCount {} is the same as the current shard count {}.",
                            target_shard_count, current_count
                        )),
                    ));
                }

                stream.stream_status = StreamStatus::Updating;
                Ok(current_count)
            },
        )
        .await?;
    store.schedule_transition(transition);

    tracing::trace!(
        stream = %stream_name,
        current_count,
        target_shard_count,
        "shard count update initiated"
    );
    Ok(Some(json!({
        "StreamName": stream_name,
        "CurrentShardCount": current_count,
        "TargetShardCount": target_shard_count,
    })))
}
