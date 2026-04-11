use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::{PendingTransition, Store};
use crate::util::current_time_ms;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let shard_to_split = data[constants::SHARD_TO_SPLIT].as_str().unwrap_or("");
    let new_starting_hash_key = data[constants::NEW_STARTING_HASH_KEY]
        .as_str()
        .unwrap_or("");

    let (shard_id, shard_ix) = sequence::resolve_shard_id(shard_to_split).map_err(|_| {
        KinesisErrorResponse::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some(&format!(
                "Could not find shard {} in stream {} under account {}.",
                shard_to_split, stream_name, store.aws_account_id
            )),
        )
    })?;

    let delay = store.options.update_stream_ms;
    let transition = PendingTransition::SplitShard {
        stream_name: stream_name.to_string(),
        ready_at_ms: current_time_ms().saturating_add(delay),
        shard_to_split: shard_id.clone(),
        new_starting_hash_key: new_starting_hash_key.to_string(),
    };

    store
        .split_shard_with_reservation(
            stream_name,
            &shard_id,
            shard_ix,
            new_starting_hash_key,
            transition.clone(),
        )
        .await?;
    tracing::info!(stream = stream_name, "shard split");
    store.schedule_transition(transition);

    Ok(None)
}
