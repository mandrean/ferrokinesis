#[cfg(feature = "server")]
use crate::capture::{CaptureOp, CaptureRecordRef};
use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::{ShardThroughputReservation, Store};
use crate::types::StoredRecordRef;
use serde_json::{Value, json};
#[cfg(feature = "server")]
use std::borrow::Cow;

#[cfg(feature = "server")]
fn is_capture_eligible(resp: &Value) -> bool {
    resp.get(constants::ERROR_CODE).is_none_or(|v| v.is_null())
}

#[cfg(feature = "server")]
fn build_capture_refs<'a>(
    records: &'a [Value],
    return_records: &'a [Value],
    timestamps: &'a [u64],
    stream: &'a str,
) -> Vec<CaptureRecordRef<'a>> {
    records
        .iter()
        .zip(return_records.iter())
        .zip(timestamps.iter())
        .filter(|((_, resp), _)| is_capture_eligible(resp))
        .map(|((req, resp), &ts)| CaptureRecordRef {
            op: CaptureOp::PutRecords,
            ts,
            stream,
            partition_key: Cow::Borrowed(req[constants::PARTITION_KEY].as_str().unwrap_or("")),
            data: req[constants::DATA].as_str().unwrap_or(""),
            explicit_hash_key: req[constants::EXPLICIT_HASH_KEY].as_str(),
            sequence_number: resp[constants::SEQUENCE_NUMBER].as_str().unwrap_or(""),
            shard_id: resp[constants::SHARD_ID].as_str().unwrap_or(""),
        })
        .collect()
}
pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = store.resolve_stream_name(&data)?;

    let records = data[constants::RECORDS].as_array().ok_or_else(|| {
        KinesisErrorResponse::client_error(constants::SERIALIZATION_EXCEPTION, None)
    })?;

    // NOTE: Per-record 1 MB data limits are enforced by the validation layer
    // (validation/rules.rs) before this handler runs. This only checks the
    // aggregate batch payload against the 5 MB limit.
    const MAX_BATCH_BYTES: usize = 5_242_880;
    let total_payload: usize = records
        .iter()
        .map(|r| {
            let data_bytes = r["Data"]
                .as_str()
                .map(|s| {
                    let decoded = crate::util::base64_decoded_len(s);
                    if decoded > 0 || s.is_empty() {
                        decoded
                    } else {
                        s.len()
                    }
                })
                .unwrap_or(0);
            // AWS counts partition key contribution as UTF-8 byte length
            let key_bytes = r["PartitionKey"].as_str().map(|s| s.len()).unwrap_or(0);
            data_bytes + key_bytes
        })
        .sum();
    if total_payload > MAX_BATCH_BYTES {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("Records' total data + partition key payload exceeds 5242880 bytes"),
        ));
    }

    // Pre-compute hash keys (no stream access needed)
    let mut hash_keys: Vec<u128> = Vec::with_capacity(records.len());

    for record in records {
        let partition_key = record["PartitionKey"].as_str().unwrap_or("");
        let explicit_hash_key = record["ExplicitHashKey"].as_str();

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
        hash_keys.push(hash_key);
    }

    let allocations = store
        .allocate_sequences_batch(&stream_name, &hash_keys)
        .await?;

    let mut reservations = Vec::with_capacity(records.len());
    let mut return_records: Vec<Value> = Vec::with_capacity(records.len());
    let mut batch: Vec<(String, StoredRecordRef<'_>)> = Vec::with_capacity(records.len());

    for (i, record) in records.iter().enumerate() {
        let alloc = &allocations[i];
        let partition_key = record["PartitionKey"].as_str().unwrap_or("");
        let record_data = record["Data"].as_str().unwrap_or("");
        let decoded_len = {
            let decoded = crate::util::base64_decoded_len(record_data);
            if decoded > 0 || record_data.is_empty() {
                decoded
            } else {
                record_data.len()
            }
        } as u64;

        reservations.push(ShardThroughputReservation {
            stream_name: &stream_name,
            shard_id: &alloc.shard_id,
            bytes: decoded_len,
            now_ms: alloc.now,
        });

        batch.push((
            alloc.stream_key.clone(),
            StoredRecordRef {
                partition_key,
                data: record_data,
                approximate_arrival_timestamp: (alloc.now / 1000) as f64,
            },
        ));

        return_records.push(json!({
            "ShardId": alloc.shard_id,
            "SequenceNumber": alloc.seq_num,
        }));
    }

    store
        .try_reserve_shard_throughput_batch(&reservations)
        .await?;

    store.put_records_batch(&stream_name, &batch).await;

    #[cfg(feature = "server")]
    if let Some(ref writer) = store.capture_writer {
        let timestamps: Vec<u64> = allocations.iter().map(|a| a.now).collect();
        let capture_refs = build_capture_refs(records, &return_records, &timestamps, &stream_name);
        writer.write_records(&capture_refs);
    }

    tracing::trace!(stream = %stream_name, records = batch.len(), "records put");
    Ok(Some(json!({
        "FailedRecordCount": 0,
        "Records": return_records,
    })))
}

#[cfg(all(test, feature = "server"))]
mod tests {
    use super::*;
    use crate::capture::{CaptureOp, CaptureWriter, read_capture_file};
    use serde_json::json;
    use tempfile::NamedTempFile;

    /// Verifies that the PutRecords capture path filters out entries whose
    /// response contains a non-null `ErrorCode`, while keeping entries with
    /// no `ErrorCode` or a null one.
    #[test]
    fn build_capture_refs_filters_failed_entries() {
        let capture_file = NamedTempFile::new().unwrap();
        let writer = CaptureWriter::new(capture_file.path(), false).unwrap();

        let stream_name = "test-stream";
        let ts = 1_234_567_890u64;

        let records: Vec<Value> = vec![
            json!({
                constants::PARTITION_KEY: "ok-key",
                constants::DATA: "b2s=",
            }),
            json!({
                constants::PARTITION_KEY: "fail-key",
                constants::DATA: "ZmFpbA==",
            }),
            json!({
                constants::PARTITION_KEY: "null-err-key",
                constants::DATA: "bnVsbA==",
            }),
        ];

        let return_records: Vec<Value> = vec![
            json!({
                "SequenceNumber": "seq-1",
                "ShardId": "shardId-000000000000"
            }),
            json!({
                "ErrorCode": "ProvisionedThroughputExceededException",
                "ErrorMessage": "Rate exceeded for shard"
            }),
            json!({
                "SequenceNumber": "seq-3",
                "ShardId": "shardId-000000000000",
                "ErrorCode": null
            }),
        ];

        let timestamps = vec![ts; records.len()];
        let capture_refs = build_capture_refs(&records, &return_records, &timestamps, stream_name);
        writer.write_records(&capture_refs);

        let captured = read_capture_file(capture_file.path()).unwrap();
        // Only the first and third records should be captured
        assert_eq!(captured.len(), 2);

        assert_eq!(captured[0].op, CaptureOp::PutRecords);
        assert_eq!(captured[0].partition_key, "ok-key");
        assert_eq!(captured[0].data, "b2s=");
        assert_eq!(captured[0].sequence_number, "seq-1");
        assert_eq!(captured[0].shard_id, "shardId-000000000000");

        assert_eq!(captured[1].op, CaptureOp::PutRecords);
        assert_eq!(captured[1].partition_key, "null-err-key");
        assert_eq!(captured[1].data, "bnVsbA==");
        assert_eq!(captured[1].sequence_number, "seq-3");
        assert_eq!(captured[1].shard_id, "shardId-000000000000");
    }

    /// Verifies that when ALL records in a PutRecords batch fail, no capture
    /// records are written.
    #[test]
    fn build_capture_refs_all_failed_writes_nothing() {
        let capture_file = NamedTempFile::new().unwrap();
        let writer = CaptureWriter::new(capture_file.path(), false).unwrap();

        let stream_name = "test-stream";
        let ts = 1_234_567_890u64;

        let records: Vec<Value> = vec![
            json!({
                constants::PARTITION_KEY: "k1",
                constants::DATA: "YQ==",
            }),
            json!({
                constants::PARTITION_KEY: "k2",
                constants::DATA: "Yg==",
            }),
        ];

        let return_records: Vec<Value> = vec![
            json!({
                "ErrorCode": "InternalFailure",
                "ErrorMessage": "Internal error"
            }),
            json!({
                "ErrorCode": "ProvisionedThroughputExceededException",
                "ErrorMessage": "Rate exceeded"
            }),
        ];

        let timestamps = vec![ts; records.len()];
        let capture_refs = build_capture_refs(&records, &return_records, &timestamps, stream_name);
        writer.write_records(&capture_refs);

        let captured = read_capture_file(capture_file.path()).unwrap();
        assert_eq!(captured.len(), 0);
    }
}
