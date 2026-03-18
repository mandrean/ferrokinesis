use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::event_stream;
use crate::sequence;
use crate::store::Store;
use crate::types::{ShardIteratorType, StreamStatus};
use crate::util::current_time_ms;
use axum::body::Body;
use bytes::Bytes;
use num_bigint::BigUint;
use serde_json::{Value, json};

/// Maximum subscription duration (5 minutes)
const MAX_SUBSCRIPTION_MS: u64 = 300_000;
/// Poll interval for new records
const POLL_INTERVAL_MS: u64 = 200;

/// Execute SubscribeToShard. Returns a streaming Body instead of JSON.
pub async fn execute_streaming(store: &Store, data: Value) -> Result<Body, KinesisErrorResponse> {
    let consumer_arn = data[constants::CONSUMER_ARN].as_str().unwrap_or("");
    let shard_id_input = data[constants::SHARD_ID].as_str().unwrap_or("");
    let starting_position = &data[constants::STARTING_POSITION];

    if consumer_arn.is_empty() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("ConsumerARN is required."),
        ));
    }

    if shard_id_input.is_empty() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("ShardId is required."),
        ));
    }

    // Verify consumer exists
    let consumer = store.get_consumer(consumer_arn).await.ok_or_else(|| {
        KinesisErrorResponse::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some(&format!("Consumer {} not found.", consumer_arn)),
        )
    })?;

    // Extract stream ARN from consumer ARN
    // Consumer ARN: arn:aws:kinesis:{region}:{account}:stream/{name}/consumer/{consumer}:{ts}
    let stream_arn_end = consumer_arn.find("/consumer/").ok_or_else(|| {
        KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("Invalid ConsumerARN format."),
        )
    })?;
    let stream_arn = &consumer_arn[..stream_arn_end];

    let stream_name = store.stream_name_from_arn(stream_arn).ok_or_else(|| {
        KinesisErrorResponse::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some("Could not resolve stream from ConsumerARN."),
        )
    })?;

    let stream = store.get_stream(&stream_name).await?;

    if stream.stream_status != StreamStatus::Active {
        return Err(KinesisErrorResponse::client_error(
            constants::RESOURCE_IN_USE,
            Some(&format!(
                "Stream {} under account {} is not ACTIVE.",
                stream_name, store.aws_account_id
            )),
        ));
    }

    if consumer.consumer_status != crate::types::ConsumerStatus::Active {
        return Err(KinesisErrorResponse::client_error(
            constants::RESOURCE_IN_USE,
            Some("Consumer is not ACTIVE."),
        ));
    }

    // Resolve shard
    let (shard_id, shard_ix) = sequence::resolve_shard_id(shard_id_input).map_err(|_| {
        KinesisErrorResponse::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some(&format!("Shard {} not found.", shard_id_input)),
        )
    })?;

    if shard_ix >= stream.shards.len() as i64 {
        return Err(KinesisErrorResponse::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some(&format!("Shard {} not found.", shard_id)),
        ));
    }

    // Resolve starting position to a sequence number
    let iterator_type: ShardIteratorType = serde_json::from_value(
        starting_position
            .get("Type")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
    .map_err(|_| {
        KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some(
                "StartingPosition.Type is required and must be one of: TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, AT_TIMESTAMP.",
            ),
        )
    })?;

    let shard_seq = &stream.shards[shard_ix as usize]
        .sequence_number_range
        .starting_sequence_number;
    let shard_seq_obj = sequence::parse_sequence(shard_seq)
        .map_err(|_| KinesisErrorResponse::server_error(None, None))?;

    let now = current_time_ms();

    let start_seq = match iterator_type {
        ShardIteratorType::TrimHorizon => shard_seq.clone(),
        ShardIteratorType::Latest => {
            let seq_ix = stream
                .seq_ix
                .get(shard_ix as usize / 5)
                .and_then(|v| *v)
                .unwrap_or(0);
            sequence::stringify_sequence(&sequence::SeqObj {
                shard_create_time: shard_seq_obj.shard_create_time,
                seq_ix: Some(BigUint::from(seq_ix)),
                seq_time: Some(now),
                shard_ix: shard_seq_obj.shard_ix,
                byte1: None,
                seq_rand: None,
                version: 2,
            })
        }
        ShardIteratorType::AtSequenceNumber => starting_position
            .get("SequenceNumber")
            .and_then(|v| v.as_str())
            .unwrap_or(shard_seq)
            .to_string(),
        ShardIteratorType::AfterSequenceNumber => {
            let seq_str = starting_position
                .get("SequenceNumber")
                .and_then(|v| v.as_str())
                .unwrap_or(shard_seq);
            let seq_obj = sequence::parse_sequence(seq_str).map_err(|_| {
                KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some("Invalid SequenceNumber."),
                )
            })?;
            sequence::increment_sequence(&seq_obj, None)
        }
        ShardIteratorType::AtTimestamp => {
            let ts = starting_position
                .get("Timestamp")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);

            let record_store = store.get_record_store(&stream_name).await;
            let range_start = format!("{}/", sequence::shard_ix_to_hex(shard_ix));
            let range_end = sequence::shard_ix_to_hex(shard_ix + 1);

            let mut found_seq = None;
            for (key, record) in record_store.range(range_start..range_end) {
                if record.approximate_arrival_timestamp >= ts {
                    found_seq = key.split('/').nth(1).map(|s| s.to_string());
                    break;
                }
            }
            found_seq.unwrap_or_else(|| shard_seq.clone())
        }
    };

    // Spawn the streaming task
    let store = store.clone();
    let stream_name = stream_name.to_string();
    let shard_id = shard_id.to_string();

    let stream = async_stream::stream! {
        let mut current_seq = start_seq;
        let start_time = current_time_ms();

        // Send initial response frame
        yield Ok::<Bytes, std::io::Error>(Bytes::from(event_stream::encode_initial_response()));

        loop {
            let now = current_time_ms();

            // Check 5-minute timeout
            if now - start_time >= MAX_SUBSCRIPTION_MS {
                break;
            }

            // Fetch records from current position
            let stream_data = match store.get_stream(&stream_name).await {
                Ok(s) => s,
                Err(_) => break,
            };

            let record_store = store.get_record_store(&stream_name).await;
            let cutoff_time = now - (stream_data.retention_period_hours as u64 * 60 * 60 * 1000);
            let range_start = format!("{}/{}", sequence::shard_ix_to_hex(shard_ix), current_seq);
            let range_end = sequence::shard_ix_to_hex(shard_ix + 1);

            let mut records = Vec::new();
            let mut last_seq_obj = None;

            for (key, record) in record_store.range(range_start..range_end).take(10000) {
                let seq_num = match key.split('/').nth(1) {
                    Some(s) => s.to_string(),
                    None => continue,
                };

                if let Ok(obj) = sequence::parse_sequence(&seq_num)
                    && obj.seq_time.unwrap_or(0) < cutoff_time
                {
                    continue;
                }

                records.push(json!({
                    "Data": record.data,
                    "PartitionKey": record.partition_key,
                    "SequenceNumber": seq_num,
                    "ApproximateArrivalTimestamp": record.approximate_arrival_timestamp,
                }));

                if let Ok(obj) = sequence::parse_sequence(&seq_num) {
                    last_seq_obj = Some(obj);
                }
            }

            // Compute next sequence and continuation
            let continuation_seq = if let Some(ref last) = last_seq_obj {
                sequence::increment_sequence(last, None)
            } else {
                current_seq.clone()
            };

            // Check for child shards (shard was split/merged)
            let mut child_shards: Vec<Value> = Vec::new();
            let shard = &stream_data.shards[shard_ix as usize];
            let shard_closed = shard.sequence_number_range.ending_sequence_number.is_some();

            if shard_closed {
                // Find child shards that reference this shard as parent
                for s in &stream_data.shards {
                    let is_child = s.parent_shard_id.as_deref() == Some(&shard_id)
                        || s.adjacent_parent_shard_id.as_deref() == Some(&shard_id);
                    if is_child {
                        let mut parent_shards = vec![json!(shard_id)];
                        if let Some(ref adj) = s.adjacent_parent_shard_id
                            && adj != &shard_id {
                                parent_shards.push(json!(adj));
                            }
                        child_shards.push(json!({
                            "ShardId": s.shard_id,
                            "ParentShards": parent_shards,
                            "HashKeyRange": s.hash_key_range,
                        }));
                    }
                }
            }

            let millis_behind = 0u64; // Mock: always caught up

            let event = json!({
                "Records": records,
                "ContinuationSequenceNumber": continuation_seq,
                "MillisBehindLatest": millis_behind,
                "ChildShards": child_shards,
            });

            let payload = serde_json::to_vec(&event).unwrap();
            yield Ok(Bytes::from(event_stream::encode_subscribe_event(&payload)));

            // Update position for next poll
            current_seq = continuation_seq;

            // If shard is closed and we've consumed everything, stop
            if shard_closed && records.is_empty() {
                break;
            }

            // Wait before polling again
            tokio::time::sleep(tokio::time::Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    };

    Ok(Body::from_stream(stream))
}
