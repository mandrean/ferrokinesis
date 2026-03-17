use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::Store;
use crate::types::*;
use crate::util::current_time_ms;
use num_bigint::BigUint;
use num_traits::One;
use serde_json::Value;
use std::collections::BTreeMap;

const SEQ_ADJUST_MS: u64 = 2000;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let shard_count = data["ShardCount"].as_i64().unwrap_or(0) as u32;

    // Check if stream already exists
    if store.contains_stream(stream_name).await {
        return Err(KinesisErrorResponse::client_error(
            constants::RESOURCE_IN_USE,
            Some(&format!(
                "Stream {} under account {} already exists.",
                stream_name, store.aws_account_id
            )),
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

    let stream = Stream {
        retention_period_hours: 24,
        enhanced_monitoring: vec![EnhancedMonitoring {
            shard_level_metrics: vec![],
        }],
        encryption_type: "NONE".to_string(),
        has_more_shards: false,
        shards: vec![], // Start empty while CREATING
        stream_arn: format!(
            "arn:aws:kinesis:{}:{}:stream/{}",
            store.aws_region, store.aws_account_id, stream_name
        ),
        stream_name: stream_name.to_string(),
        stream_status: StreamStatus::Creating,
        stream_creation_timestamp: (create_time as f64) / 1000.0,
        stream_mode_details: StreamModeDetails {
            stream_mode: "PROVISIONED".to_string(),
        },
        seq_ix: vec![None; (shard_count as usize).div_ceil(5)],
        tags: BTreeMap::new(),
        key_id: None,
        warm_throughput_mibps: 0,
        max_record_size_kib: 1024,
    };

    store.put_stream(stream_name, stream).await;

    // Transition to ACTIVE after delay
    let store_clone = store.clone();
    let name = stream_name.to_string();
    let delay = store.options.create_stream_ms;
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
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
