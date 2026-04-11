use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::{PendingTransition, Store, TransitionMutation};
use crate::types::*;
use crate::util::current_time_ms;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let shard_to_merge = data[constants::SHARD_TO_MERGE].as_str().unwrap_or("");
    let adjacent_shard = data[constants::ADJACENT_SHARD_TO_MERGE]
        .as_str()
        .unwrap_or("");

    let shard_names = [shard_to_merge, adjacent_shard];
    let mut shard_ids = Vec::new();
    let mut shard_ixs = Vec::new();

    for name in &shard_names {
        let (id, ix) = sequence::resolve_shard_id(name).map_err(|_| {
            KinesisErrorResponse::client_error(
                constants::RESOURCE_NOT_FOUND,
                Some(&format!(
                    "Could not find shard {} in stream {} under account {}.",
                    name, stream_name, store.aws_account_id
                )),
            )
        })?;
        shard_ids.push(id);
        shard_ixs.push(ix);
    }

    let delay = store.options.update_stream_ms;
    let transition = PendingTransition::MergeShards {
        stream_name: stream_name.to_string(),
        ready_at_ms: current_time_ms().saturating_add(delay),
        shard_to_merge: shard_ids[0].clone(),
        adjacent_shard_to_merge: shard_ids[1].clone(),
    };

    store
        .update_stream_with_transition(stream_name, TransitionMutation::Upsert(transition.clone()), |stream| {
            if stream.stream_status != StreamStatus::Active {
                return Err(KinesisErrorResponse::client_error(
                    constants::RESOURCE_IN_USE,
                    Some(&format!(
                        "Stream {} under account {} not ACTIVE, instead in state {}",
                        stream_name, store.aws_account_id, stream.stream_status
                    )),
                ));
            }

            for (i, &ix) in shard_ixs.iter().enumerate() {
                if ix >= stream.shards.len() as i64 {
                    return Err(KinesisErrorResponse::client_error(
                        constants::RESOURCE_NOT_FOUND,
                        Some(&format!(
                            "Could not find shard {} in stream {} under account {}.",
                            shard_ids[i], stream_name, store.aws_account_id
                        )),
                    ));
                }
            }

            let end0 = stream.shards[shard_ixs[0] as usize]
                .hash_key_range
                .end_u128();
            let start1 = stream.shards[shard_ixs[1] as usize]
                .hash_key_range
                .start_u128();

            // Kinesis requires the two shards to be adjacent — their hash ranges must
            // be contiguous with no gap. `checked_add` handles the theoretical edge case
            // where end0 == u128::MAX (impossible in practice since there'd be no room
            // for another shard, but safe by construction).
            if end0.checked_add(1) != Some(start1) {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(&format!(
                        "Shards {} and {} in stream {} under account {} are not an adjacent pair of shards eligible for merging",
                        shard_ids[0], shard_ids[1], stream_name, store.aws_account_id
                    )),
                ));
            }

            stream.stream_status = StreamStatus::Updating;
            Ok(())
        })
        .await?;
    tracing::info!(stream = stream_name, "shards merged");
    store.schedule_transition(transition);

    Ok(None)
}
