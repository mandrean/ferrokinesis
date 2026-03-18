use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::Store;
use crate::types::{StoredRecord, StreamStatus};
use crate::util::current_time_ms;
use num_bigint::BigUint;
use num_traits::{One, Zero};
use serde_json::{Value, json};

struct SeqPiece {
    shard_ix: i64,
    shard_id: String,
    shard_create_time: u64,
}

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let records = data[constants::RECORDS].as_array().ok_or_else(|| {
        KinesisErrorResponse::client_error(constants::SERIALIZATION_EXCEPTION, None)
    })?;

    // Validate total batch payload size (5 MB limit)
    const MAX_BATCH_BYTES: usize = 5_242_880;
    let total_payload: usize = records
        .iter()
        .map(|r| {
            let data_bytes = r["Data"]
                .as_str()
                .and_then(|s| STANDARD.decode(s).ok())
                .map(|v| v.len())
                .unwrap_or(0);
            let key_bytes = r["PartitionKey"].as_str().map(|s| s.len()).unwrap_or(0);
            data_bytes + key_bytes
        })
        .sum();
    if total_payload > MAX_BATCH_BYTES {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("Records size exceeds 5 MB limit"),
        ));
    }

    // Pre-compute hash keys (no stream access needed)
    let pow_128 = BigUint::one() << 128;
    let mut hash_keys = Vec::with_capacity(records.len());

    for record in records {
        let partition_key = record["PartitionKey"].as_str().unwrap_or("");
        let explicit_hash_key = record["ExplicitHashKey"].as_str();

        let hash_key = if let Some(ehk) = explicit_hash_key {
            let hk: BigUint = ehk.parse().unwrap_or_else(|_| BigUint::zero());
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
        hash_keys.push(hash_key);
    }

    let (return_records, batch) = store
        .update_stream(stream_name, |stream| {
            if !matches!(
                stream.stream_status,
                StreamStatus::Active | StreamStatus::Updating
            ) {
                return Err(KinesisErrorResponse::client_error(
                    constants::RESOURCE_NOT_FOUND,
                    Some(&format!(
                        "Stream {} under account {} not found.",
                        stream_name, store.aws_account_id
                    )),
                ));
            }

            let mut seq_pieces = Vec::with_capacity(records.len());

            for (idx, _record) in records.iter().enumerate() {
                let hash_key = &hash_keys[idx];
                let mut piece = SeqPiece {
                    shard_ix: 0,
                    shard_id: String::new(),
                    shard_create_time: 0,
                };

                for (j, shard) in stream.shards.iter().enumerate() {
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
                        if *hash_key >= start && *hash_key <= end {
                            piece.shard_ix = j as i64;
                            piece.shard_id = shard.shard_id.clone();
                            piece.shard_create_time = sequence::parse_sequence(
                                &shard.sequence_number_range.starting_sequence_number,
                            )
                            .map(|s| s.shard_create_time)
                            .unwrap_or(0);
                            break;
                        }
                    }
                }

                seq_pieces.push(piece);
            }

            let mut batch_ops: Vec<Option<(String, StoredRecord)>> = vec![None; records.len()];
            let mut return_records: Vec<Value> = vec![json!(null); records.len()];

            for shard_ix in 0..stream.shards.len() as i64 {
                for (i, record) in records.iter().enumerate() {
                    if seq_pieces[i].shard_ix != shard_ix {
                        continue;
                    }

                    let seq_ix_ix = (shard_ix as usize) / 5;
                    let now = current_time_ms().max(seq_pieces[i].shard_create_time);

                    while stream.seq_ix.len() <= seq_ix_ix {
                        stream.seq_ix.push(None);
                    }

                    if stream.seq_ix[seq_ix_ix].is_none() {
                        stream.seq_ix[seq_ix_ix] =
                            Some(if seq_pieces[i].shard_create_time == now {
                                1
                            } else {
                                0
                            });
                    }

                    let current_seq_ix = stream.seq_ix[seq_ix_ix].unwrap_or(0);
                    let seq_num = sequence::stringify_sequence(&sequence::SeqObj {
                        shard_create_time: seq_pieces[i].shard_create_time,
                        seq_ix: Some(BigUint::from(current_seq_ix)),
                        byte1: None,
                        seq_time: Some(now),
                        seq_rand: None,
                        shard_ix,
                        version: 2,
                    });

                    let stream_key = format!("{}/{}", sequence::shard_ix_to_hex(shard_ix), seq_num);
                    stream.seq_ix[seq_ix_ix] = Some(current_seq_ix + 1);

                    let partition_key = record["PartitionKey"].as_str().unwrap_or("");
                    let record_data = record["Data"].as_str().unwrap_or("");

                    batch_ops[i] = Some((
                        stream_key,
                        StoredRecord {
                            partition_key: partition_key.to_string(),
                            data: record_data.to_string(),
                            approximate_arrival_timestamp: now as f64 / 1000.0,
                        },
                    ));

                    return_records[i] = json!({
                        "ShardId": seq_pieces[i].shard_id,
                        "SequenceNumber": seq_num,
                    });
                }
            }

            let batch: Vec<(String, StoredRecord)> = batch_ops.into_iter().flatten().collect();
            Ok((return_records, batch))
        })
        .await?;

    store.put_records_batch(stream_name, batch).await;

    Ok(Some(json!({
        "FailedRecordCount": 0,
        "Records": return_records,
    })))
}
