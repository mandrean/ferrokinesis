use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::Store;
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

    store
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

    // Schedule transition
    let store_clone = store.clone();
    let name = stream_name.to_string();
    let delay = store.options.update_stream_ms;
    let shard_ixs_clone = shard_ixs.clone();
    let shard_ids_clone = shard_ids.clone();

    crate::runtime::spawn_background(async move {
        crate::runtime::sleep_ms(delay).await;

        let _ = store_clone
            .update_stream(&name, |stream| {
                let now = current_time_ms();
                stream.stream_status = StreamStatus::Active;

                for &ix in &shard_ixs_clone {
                    let shard = &mut stream.shards[ix as usize];
                    let create_time = sequence::parse_sequence(
                        &shard.sequence_number_range.starting_sequence_number,
                    )
                    .map(|s| s.shard_create_time)
                    .unwrap_or(0);

                    shard.sequence_number_range.ending_sequence_number =
                        Some(sequence::stringify_sequence(&sequence::SeqObj {
                            shard_create_time: create_time,
                            shard_ix: ix,
                            seq_ix: Some(sequence::MAX_SEQ_IX),
                            seq_time: Some(now),
                            byte1: None,
                            seq_rand: None,
                            version: 2,
                        }));
                }

                let new_ix = stream.shards.len() as i64;
                let starting_hash = stream.shards[shard_ixs_clone[0] as usize]
                    .hash_key_range
                    .starting_hash_key
                    .clone();
                let ending_hash = stream.shards[shard_ixs_clone[1] as usize]
                    .hash_key_range
                    .ending_hash_key
                    .clone();

                stream.shards.push(Shard {
                    parent_shard_id: Some(shard_ids_clone[0].clone()),
                    adjacent_parent_shard_id: Some(shard_ids_clone[1].clone()),
                    hash_key_range: HashKeyRange::new(starting_hash, ending_hash),
                    sequence_number_range: SequenceNumberRange {
                        starting_sequence_number: sequence::stringify_sequence(&sequence::SeqObj {
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
                        }),
                        ending_sequence_number: None,
                    },
                    shard_id: sequence::shard_id_name(new_ix),
                });

                Ok(())
            })
            .await;
    });

    Ok(None)
}
