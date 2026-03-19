use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::Store;
use crate::types::{StoredRecordRef, StreamStatus};
use crate::util::current_time_ms;
use num_bigint::BigUint;
use num_traits::{One, Zero};
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = store.resolve_stream_name(&data)?;

    let partition_key = data[constants::PARTITION_KEY].as_str().unwrap_or("");
    let record_data = data[constants::DATA].as_str().unwrap_or("");
    let explicit_hash_key = data[constants::EXPLICIT_HASH_KEY].as_str();
    let seq_for_ordering = data[constants::SEQUENCE_NUMBER_FOR_ORDERING].as_str();

    let hash_key = if let Some(ehk) = explicit_hash_key {
        let hk: BigUint = ehk.parse().unwrap_or_else(|_| BigUint::zero());
        let pow_128 = BigUint::one() << 128;
        if hk >= pow_128 {
            return Err(KinesisErrorResponse::client_error(
                constants::INVALID_ARGUMENT,
                Some(&format!(
                    "Invalid ExplicitHashKey. ExplicitHashKey must be in the range: [0, 2^128-1]. Specified value was {ehk}"
                )),
            ));
        }
        hk
    } else {
        sequence::partition_key_to_hash_key(partition_key)
    };

    if let Some(seq_ord) = seq_for_ordering {
        match sequence::parse_sequence(seq_ord) {
            Ok(seq_obj) => {
                if seq_obj.seq_time.unwrap_or(0) > current_time_ms() {
                    return Err(KinesisErrorResponse::client_error(
                        constants::INVALID_ARGUMENT,
                        Some(&format!(
                            "ExclusiveMinimumSequenceNumber {} used in PutRecord on stream {} under account {} is invalid.",
                            seq_ord, stream_name, store.aws_account_id
                        )),
                    ));
                }
            }
            Err(_) => {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(&format!(
                        "ExclusiveMinimumSequenceNumber {} used in PutRecord on stream {} under account {} is invalid.",
                        seq_ord, stream_name, store.aws_account_id
                    )),
                ));
            }
        }
    }

    let (shard_id, seq_num, stream_key, now) = store
        .update_stream(&stream_name, |stream| {
            if !matches!(
                stream.stream_status,
                StreamStatus::Active | StreamStatus::Updating
            ) {
                return Err(KinesisErrorResponse::stream_not_found(
                    &stream_name,
                    &store.aws_account_id,
                ));
            }

            // Find the appropriate shard
            let mut shard_ix = 0i64;
            let mut shard_id = String::new();
            let mut shard_create_time = 0u64;

            for (i, shard) in stream.shards.iter().enumerate() {
                if shard.sequence_number_range.ending_sequence_number.is_none() {
                    let start: BigUint = shard
                        .hash_key_range
                        .starting_hash_key
                        .parse()
                        .unwrap_or_else(|_| BigUint::zero());
                    let end: BigUint = shard
                        .hash_key_range
                        .ending_hash_key
                        .parse()
                        .unwrap_or_else(|_| BigUint::zero());
                    if hash_key >= start && hash_key <= end {
                        shard_ix = i as i64;
                        shard_id = shard.shard_id.clone();
                        shard_create_time = sequence::parse_sequence(
                            &shard.sequence_number_range.starting_sequence_number,
                        )
                        .map(|s| s.shard_create_time)
                        .unwrap_or(0);
                        break;
                    }
                }
            }

            let seq_ix_ix = (shard_ix as usize) / 5;
            let now = current_time_ms().max(shard_create_time);

            // Ensure seq_ix vec is large enough
            while stream.seq_ix.len() <= seq_ix_ix {
                stream.seq_ix.push(None);
            }

            if stream.seq_ix[seq_ix_ix].is_none() {
                stream.seq_ix[seq_ix_ix] = Some(if shard_create_time == now { 1 } else { 0 });
            }

            let current_seq_ix = stream.seq_ix[seq_ix_ix].unwrap_or(0);
            let seq_num = sequence::stringify_sequence(&sequence::SeqObj {
                shard_create_time,
                seq_ix: Some(BigUint::from(current_seq_ix)),
                byte1: None,
                seq_time: Some(now),
                seq_rand: None,
                shard_ix,
                version: 2,
            });

            let stream_key = format!("{}/{}", sequence::shard_ix_to_hex(shard_ix), seq_num);
            stream.seq_ix[seq_ix_ix] = Some(current_seq_ix + 1);

            Ok((shard_id, seq_num, stream_key, now))
        })
        .await?;

    let record = StoredRecordRef {
        partition_key,
        data: record_data,
        approximate_arrival_timestamp: now as f64 / 1000.0,
    };

    store.put_record(&stream_name, &stream_key, &record).await;

    Ok(Some(json!({
        "ShardId": shard_id,
        "SequenceNumber": seq_num,
    })))
}
