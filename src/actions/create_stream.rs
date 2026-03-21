use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::Store;
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

    // Check if stream already exists
    if store.contains_stream(stream_name).await {
        return Err(KinesisErrorResponse::stream_in_use(
            stream_name,
            &store.aws_account_id,
        ));
    }

    // Check shard limit
    let shard_sum = store.sum_open_shards().await;
    if shard_sum + shard_count > store.options.shard_limit {
        return Err(KinesisErrorResponse::client_error(
            constants::LIMIT_EXCEEDED,
            Some(&format!(
                "This request would exceed the shard limit for the account {} in {}. \
                 Current shard count for the account: {}. Limit: {}. \
                 Number of additional shards that would have resulted from this request: {}. \
                 Refer to the AWS Service Limits page \
                 (http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) \
                 for current limits and how to request higher limits.",
                store.aws_account_id,
                store.aws_region,
                shard_sum,
                store.options.shard_limit,
                shard_count
            )),
        ));
    }

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
            hash_key_range: HashKeyRange {
                starting_hash_key: start.to_string(),
                ending_hash_key: end.to_string(),
            },
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

    store.put_stream(stream_name, stream).await;
    tracing::info!(stream = stream_name, shards = shard_count, "stream created");

    // Transition to ACTIVE after delay
    let store_clone = store.clone();
    let name = stream_name.to_string();
    let delay = store.options.create_stream_ms;
    crate::runtime::spawn_background(async move {
        crate::runtime::sleep_ms(delay).await;
        let _ = store_clone
            .update_stream(&name, |stream| {
                stream.stream_status = StreamStatus::Active;
                stream.shards = shards;
                Ok(())
            })
            .await;
    });

    Ok(None) // Empty 200 response
}
