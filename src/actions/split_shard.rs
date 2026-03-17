use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::Store;
use crate::types::*;
use crate::util::current_time_ms;
use num_bigint::BigUint;
use num_traits::{Num, One, Zero};
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

    let (shard_start, shard_end, hash_key) = store
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

            let hash_key: BigUint = new_starting_hash_key
                .parse()
                .unwrap_or_else(|_| BigUint::zero());
            let shard = &stream.shards[shard_ix as usize];
            let shard_start: BigUint = shard
                .hash_key_range
                .starting_hash_key
                .parse()
                .unwrap_or_else(|_| BigUint::zero());
            let shard_end: BigUint = shard
                .hash_key_range
                .ending_hash_key
                .parse()
                .unwrap_or_else(|_| BigUint::zero());

            if hash_key <= &shard_start + BigUint::one() || hash_key >= shard_end {
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

            Ok((shard_start, shard_end, hash_key))
        })
        .await?;

    let store_clone = store.clone();
    let name = stream_name.to_string();
    let delay = store.options.update_stream_ms;
    let shard_id_clone = shard_id.clone();

    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;

        let _ = store_clone
            .update_stream(&name, |stream| {
                let now = current_time_ms();
                stream.stream_status = StreamStatus::Active;

                let max_seq_ix = BigUint::from_str_radix("7fffffffffffffff", 16)
                    .unwrap_or_else(|_| BigUint::zero());

                let shard = &mut stream.shards[shard_ix as usize];
                let create_time =
                    sequence::parse_sequence(&shard.sequence_number_range.starting_sequence_number)
                        .map(|s| s.shard_create_time)
                        .unwrap_or(0);

                shard.sequence_number_range.ending_sequence_number =
                    Some(sequence::stringify_sequence(&sequence::SeqObj {
                        shard_create_time: create_time,
                        shard_ix,
                        seq_ix: Some(max_seq_ix),
                        seq_time: Some(now),
                        byte1: None,
                        seq_rand: None,
                        version: 2,
                    }));

                let new_ix1 = stream.shards.len() as i64;
                stream.shards.push(Shard {
                    parent_shard_id: Some(shard_id_clone.clone()),
                    adjacent_parent_shard_id: None,
                    hash_key_range: HashKeyRange {
                        starting_hash_key: shard_start.to_string(),
                        ending_hash_key: (&hash_key - BigUint::one()).to_string(),
                    },
                    sequence_number_range: SequenceNumberRange {
                        starting_sequence_number: sequence::stringify_sequence(&sequence::SeqObj {
                            shard_create_time: now + 1000,
                            shard_ix: new_ix1,
                            seq_ix: None,
                            seq_time: None,
                            byte1: None,
                            seq_rand: None,
                            version: 2,
                        }),
                        ending_sequence_number: None,
                    },
                    shard_id: sequence::shard_id_name(new_ix1),
                });

                let new_ix2 = stream.shards.len() as i64;
                stream.shards.push(Shard {
                    parent_shard_id: Some(shard_id_clone.clone()),
                    adjacent_parent_shard_id: None,
                    hash_key_range: HashKeyRange {
                        starting_hash_key: hash_key.to_string(),
                        ending_hash_key: shard_end.to_string(),
                    },
                    sequence_number_range: SequenceNumberRange {
                        starting_sequence_number: sequence::stringify_sequence(&sequence::SeqObj {
                            shard_create_time: now + 1000,
                            shard_ix: new_ix2,
                            seq_ix: None,
                            seq_time: None,
                            byte1: None,
                            seq_rand: None,
                            version: 2,
                        }),
                        ending_sequence_number: None,
                    },
                    shard_id: sequence::shard_id_name(new_ix2),
                });

                Ok(())
            })
            .await;
    });

    Ok(None)
}
