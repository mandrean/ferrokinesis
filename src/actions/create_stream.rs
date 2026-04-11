use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::{PendingTransition, Store};
use crate::types::*;
use crate::util::current_time_ms;
use num_bigint::BigUint;
use num_traits::One;
use serde_json::Value;

// Backdate the shard creation timestamp by 2 seconds (kinesalite compatibility).
// This guarantees shard_create_time < now when the first record is written, so
// seq_ix is initialised to 0 rather than the special-case 1 used when a record
// arrives in the exact same millisecond as the shard's creation.
const SEQ_ADJUST_MS: u64 = 2000;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let shard_count = data[constants::SHARD_COUNT].as_i64().unwrap_or(0) as u32;

    let pow_128 = BigUint::one() << 128;
    let shard_hash = &pow_128 / BigUint::from(shard_count);
    let create_time = current_time_ms() - SEQ_ADJUST_MS;

    let mut shards = Vec::with_capacity(shard_count as usize);
    for i in 0..shard_count {
        let start: BigUint = &shard_hash * BigUint::from(i);
        let end: BigUint = if i < shard_count - 1 {
            &shard_hash * BigUint::from(i + 1) - BigUint::one()
        } else {
            &pow_128 - BigUint::one()
        };

        shards.push(Shard {
            shard_id: sequence::shard_id_name(i as i64),
            hash_key_range: HashKeyRange::new(start.to_string(), end.to_string()),
            sequence_number_range: SequenceNumberRange {
                starting_sequence_number: sequence::stringify_sequence(&sequence::SeqObj {
                    shard_create_time: create_time,
                    seq_ix: None,
                    byte1: None,
                    seq_time: None,
                    seq_rand: None,
                    shard_ix: i as i64,
                    version: 2,
                }),
                ending_sequence_number: None,
            },
            parent_shard_id: None,
            adjacent_parent_shard_id: None,
        });
    }

    let delay = store.options.create_stream_ms;
    let transition = PendingTransition::CreateStream {
        stream_name: stream_name.to_string(),
        ready_at_ms: current_time_ms().saturating_add(delay),
        shards: shards.clone(),
    };

    let stream = StreamBuilder::new(
        stream_name.to_string(),
        format!(
            "arn:aws:kinesis:{}:{}:stream/{}",
            store.aws_region, store.aws_account_id, stream_name
        ),
        StreamStatus::Creating,
        EpochSeconds((create_time / 1000) as f64),
        vec![], // Start empty while CREATING
    )
    .build();

    store
        .create_stream_with_reservation(stream_name, shard_count, stream, transition.clone())
        .await?;
    tracing::info!(stream = stream_name, shards = shard_count, "stream created");
    store.schedule_transition(transition);

    Ok(None) // Empty 200 response
}
