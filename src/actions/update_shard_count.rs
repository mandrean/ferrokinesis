use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::Store;
use crate::types::*;
use crate::util::current_time_ms;
use num_bigint::BigUint;
use num_traits::{Num, One, Zero};
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let target_shard_count = data[constants::TARGET_SHARD_COUNT].as_i64().unwrap_or(0) as u32;

    let (current_count, stream_name_owned) = store
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
            Ok((current_count, stream.stream_name.clone()))
        })
        .await?;

    // Perform the resharding asynchronously
    let store_clone = store.clone();
    let delay = store.options.update_stream_ms;

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;

        let _ = store_clone
            .update_stream(&stream_name_owned, |stream| {
                let now = current_time_ms();
                // Use the maximum possible seq_ix (0x7fffffffffffffff) for the closing
                // sequence number. This ensures no future record written to this shard
                // could ever produce a sequence number that compares as ≥ the ending
                // sequence, making the shard-closed invariant unconditionally safe.
                let max_seq_ix = BigUint::from_str_radix("7fffffffffffffff", 16)
                    .unwrap_or_else(|_| BigUint::zero());

                // Close all current open shards
                let open_indices: Vec<usize> = stream
                    .shards
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| s.sequence_number_range.ending_sequence_number.is_none())
                    .map(|(i, _)| i)
                    .collect();

                for &ix in &open_indices {
                    let create_time = sequence::parse_sequence(
                        &stream.shards[ix]
                            .sequence_number_range
                            .starting_sequence_number,
                    )
                    .map(|s| s.shard_create_time)
                    .unwrap_or(0);

                    stream.shards[ix]
                        .sequence_number_range
                        .ending_sequence_number =
                        Some(sequence::stringify_sequence(&sequence::SeqObj {
                            shard_create_time: create_time,
                            shard_ix: ix as i64,
                            seq_ix: Some(max_seq_ix.clone()),
                            seq_time: Some(now),
                            byte1: None,
                            seq_rand: None,
                            version: 2,
                        }));
                }

                // Create new shards with uniform hash distribution
                let pow_128 = BigUint::one() << 128;
                let shard_hash = &pow_128 / BigUint::from(target_shard_count);

                for i in 0..target_shard_count {
                    let new_ix = stream.shards.len() as i64;
                    let start: BigUint = &shard_hash * BigUint::from(i);
                    let end: BigUint = if i < target_shard_count - 1 {
                        &shard_hash * BigUint::from(i + 1) - BigUint::one()
                    } else {
                        &pow_128 - BigUint::one()
                    };

                    stream.shards.push(Shard {
                        shard_id: sequence::shard_id_name(new_ix),
                        // UpdateShardCount is a full reshard, not a split/merge; new shards have
                        // no parent lineage relationship with the old shards.
                        parent_shard_id: None,
                        adjacent_parent_shard_id: None,
                        hash_key_range: HashKeyRange {
                            starting_hash_key: start.to_string(),
                            ending_hash_key: end.to_string(),
                        },
                        sequence_number_range: SequenceNumberRange {
                            starting_sequence_number: sequence::stringify_sequence(
                                &sequence::SeqObj {
                                    // Child's create_time is 1 second ahead of the parent's closing
                                    // timestamp so child sequence numbers always sort lexically after
                                    // the parent's last sequence (the token format encodes create_time
                                    // in hex[1..10], so a higher create_time produces a larger number).
                                    shard_create_time: now + 1000,
                                    shard_ix: new_ix,
                                    seq_ix: None,
                                    seq_time: None,
                                    byte1: None,
                                    seq_rand: None,
                                    version: 2,
                                },
                            ),
                            ending_sequence_number: None,
                        },
                    });
                }

                stream.stream_status = StreamStatus::Active;
                Ok(())
            })
            .await;
    });

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
