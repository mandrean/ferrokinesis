#[cfg(feature = "server")]
use crate::capture::{CaptureOp, CaptureRecordRef};
use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::Store;
use crate::types::StoredRecordRef;
use crate::util::base64_decoded_len;
use crate::util::current_time_ms;
use serde_json::{Value, json};
#[cfg(feature = "server")]
use std::borrow::Cow;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = store.resolve_stream_name(&data)?;

    let partition_key = data[constants::PARTITION_KEY].as_str().unwrap_or("");
    let record_data = data[constants::DATA].as_str().unwrap_or("");
    let explicit_hash_key = data[constants::EXPLICIT_HASH_KEY].as_str();
    let seq_for_ordering = data[constants::SEQUENCE_NUMBER_FOR_ORDERING].as_str();

    let hash_key: u128 = if let Some(ehk) = explicit_hash_key {
        ehk.parse::<u128>().map_err(|_| {
            KinesisErrorResponse::client_error(
                constants::INVALID_ARGUMENT,
                Some(&format!(
                    "Invalid ExplicitHashKey. ExplicitHashKey must be in the range: [0, 2^128-1]. Specified value was {ehk}"
                )),
            )
        })?
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

    let alloc = store.allocate_sequence(&stream_name, &hash_key).await?;

    let record = StoredRecordRef {
        partition_key,
        data: record_data,
        approximate_arrival_timestamp: (alloc.now / 1000) as f64,
    };

    let decoded_len = {
        let decoded = base64_decoded_len(record_data);
        if decoded > 0 || record_data.is_empty() {
            decoded
        } else {
            record_data.len()
        }
    } as u64;

    store
        .try_reserve_shard_throughput(&stream_name, &alloc.shard_id, decoded_len, alloc.now)
        .await?;

    store
        .put_record(&stream_name, &alloc.stream_key, &record)
        .await?;

    #[cfg(feature = "server")]
    if let Some(ref writer) = store.capture_writer {
        let capture_record = CaptureRecordRef {
            op: CaptureOp::PutRecord,
            ts: alloc.now,
            stream: &stream_name,
            partition_key: Cow::Borrowed(partition_key),
            data: record_data,
            explicit_hash_key,
            sequence_number: &alloc.seq_num,
            shard_id: &alloc.shard_id,
        };
        writer.write_record(&capture_record);
    }

    tracing::trace!(stream = %stream_name, shard = %alloc.shard_id, partition_key, "record put");
    Ok(Some(json!({
        "ShardId": alloc.shard_id,
        "SequenceNumber": alloc.seq_num,
    })))
}
