use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::{PendingTransition, Store, TransitionMutation};
use crate::types::*;
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

    // Get shard sum across all streams (separate read transaction)
    let shard_sum = store.sum_open_shards().await;

    let delay = store.options.update_stream_ms;
    let transition = PendingTransition::SplitShard {
        stream_name: stream_name.to_string(),
        ready_at_ms: current_time_ms().saturating_add(delay),
        shard_to_split: shard_id.clone(),
        new_starting_hash_key: new_starting_hash_key.to_string(),
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

            if shard_ix >= stream.shards.len() as i64 {
                return Err(KinesisErrorResponse::client_error(
                    constants::RESOURCE_NOT_FOUND,
                    Some(&format!(
                        "Could not find shard {} in stream {} under account {}.",
                        shard_id, stream_name, store.aws_account_id
                    )),
                ));
            }

            if shard_sum + 1 > store.options.shard_limit {
                return Err(KinesisErrorResponse::client_error(
                    constants::LIMIT_EXCEEDED,
                    Some(&format!(
                        "This request would exceed the shard limit for the account {} in {}. \
                         Current shard count for the account: {}. Limit: {}. \
                         Number of additional shards that would have resulted from this request: 1. \
                         Refer to the AWS Service Limits page \
                         (http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) \
                         for current limits and how to request higher limits.",
                        store.aws_account_id, store.aws_region, shard_sum, store.options.shard_limit
                    )),
                ));
            }

            let hash_key: u128 = new_starting_hash_key.parse().unwrap_or(0);
            let shard = &stream.shards[shard_ix as usize];
            let shard_start = shard.hash_key_range.start_u128();
            let shard_end = shard.hash_key_range.end_u128();

            // Strict interior constraint: the split key must be > start+1 AND < end.
            // Equal to start+1 would give the lower child an empty hash range [start, start];
            // equal to end would give the upper child an empty range [end, end]. Either
            // degenerate case would prevent any partition key from routing to that child.
            if hash_key <= shard_start + 1 || hash_key >= shard_end {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(&format!(
                        "NewStartingHashKey {} used in SplitShard() on shard {} in stream {} under account {} \
                         is not both greater than one plus the shard's StartingHashKey {} and less than the shard's EndingHashKey {}.",
                        new_starting_hash_key, shard_id, stream_name, store.aws_account_id,
                        shard.hash_key_range.starting_hash_key, shard.hash_key_range.ending_hash_key
                    )),
                ));
            }

            stream.stream_status = StreamStatus::Updating;
            Ok(())
        })
        .await?;
    tracing::info!(stream = stream_name, "shard split");
    store.schedule_transition(transition);

    Ok(None)
}
