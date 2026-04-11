use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::{PendingTransition, Store};
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
        .update_shard_count_with_reservation(stream_name, target_shard_count, transition.clone())
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
