use crate::capture::{CaptureOp, CaptureRecordRef};
use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::sequence;
use crate::store::Store;
use crate::types::{StoredRecordRef, StreamStatus};
use crate::util::current_time_ms;
use num_bigint::BigUint;
use num_traits::{One, Zero};
use serde_json::{Value, json};
use std::borrow::Cow;

struct SeqPiece {
    shard_ix: i64,
    shard_id: String,
    shard_create_time: u64,
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

            let mut batch_ops: Vec<Option<(String, StoredRecordRef<'_>)>> =
                (0..records.len()).map(|_| None).collect();
            let mut return_records: Vec<Value> = vec![json!(null); records.len()];

            for shard_ix in 0..stream.shards.len() as i64 {
                for (i, record) in records.iter().enumerate() {
                    if seq_pieces[i].shard_ix != shard_ix {
                        continue;
                    }

                    // kinesalite groups every 5 consecutive shards into a shared sequence-index
                    // bucket; seq_ix is incremented per-bucket rather than per-shard. This
                    // emulates that bucketing exactly so sequence numbers match real AWS ordering.
                    let seq_ix_ix = (shard_ix as usize) / 5;
                    // Clamp `now` to at least the shard's own creation time. A record timestamp
                    // that precedes the shard's start would produce a sequence number that sorts
                    // before the shard's starting sequence number — an impossible ordering.
                    let now = current_time_ms().max(seq_pieces[i].shard_create_time);

                    while stream.seq_ix.len() <= seq_ix_ix {
                        stream.seq_ix.push(None);
                    }

                    // Start seq_ix at 1 (not 0) when the shard was created in the same
                    // millisecond as this write, so the first record's sequence number is
                    // strictly greater than the shard's starting sequence number.
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
                        StoredRecordRef {
                            partition_key,
                            data: record_data,
                            approximate_arrival_timestamp: (now / 1000) as f64,
                        },
                    ));

                    return_records[i] = json!({
                        "ShardId": seq_pieces[i].shard_id,
                        "SequenceNumber": seq_num,
                    });
                }
            }

            let batch: Vec<(String, StoredRecordRef<'_>)> =
                batch_ops.into_iter().flatten().collect();
            Ok((return_records, batch))
        })
        .await?;

    store.put_records_batch(&stream_name, &batch).await;

    if let Some(ref writer) = store.capture_writer {
        let ts = current_time_ms();
        let capture_refs: Vec<CaptureRecordRef<'_>> = records
            .iter()
            .zip(return_records.iter())
            .filter(|(_, resp)| resp.get(constants::ERROR_CODE).is_none_or(|v| v.is_null()))
            .map(|(req, resp)| CaptureRecordRef {
                op: CaptureOp::PutRecords,
                ts,
                stream: &stream_name,
                partition_key: Cow::Borrowed(req[constants::PARTITION_KEY].as_str().unwrap_or("")),
                data: req[constants::DATA].as_str().unwrap_or(""),
                explicit_hash_key: req[constants::EXPLICIT_HASH_KEY].as_str(),
                sequence_number: resp[constants::SEQUENCE_NUMBER].as_str().unwrap_or(""),
                shard_id: resp[constants::SHARD_ID].as_str().unwrap_or(""),
            })
            .collect();
        writer.write_records(&capture_refs);
    }

    tracing::trace!(stream = %stream_name, records = batch.len(), "records put");
    Ok(Some(json!({
        "FailedRecordCount": 0,
        "Records": return_records,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::{CaptureOp, CaptureWriter, read_capture_file};
    use serde_json::json;
    use tempfile::NamedTempFile;

    /// Verifies that the PutRecords capture path filters out entries whose
    /// response contains a non-null `ErrorCode`, while keeping entries with
    /// no `ErrorCode` or a null one.
    #[test]
    fn write_capture_records_filters_failed_put_records_entries() {
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

        let capture_refs: Vec<CaptureRecordRef<'_>> = records
            .iter()
            .zip(return_records.iter())
            .filter(|(_, resp)| resp.get(constants::ERROR_CODE).is_none_or(|v| v.is_null()))
            .map(|(req, resp)| CaptureRecordRef {
                op: CaptureOp::PutRecords,
                ts,
                stream: stream_name,
                partition_key: Cow::Borrowed(req[constants::PARTITION_KEY].as_str().unwrap_or("")),
                data: req[constants::DATA].as_str().unwrap_or(""),
                explicit_hash_key: req[constants::EXPLICIT_HASH_KEY].as_str(),
                sequence_number: resp[constants::SEQUENCE_NUMBER].as_str().unwrap_or(""),
                shard_id: resp[constants::SHARD_ID].as_str().unwrap_or(""),
            })
            .collect();
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
    fn write_capture_records_all_failed_writes_nothing() {
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

        let capture_refs: Vec<CaptureRecordRef<'_>> = records
            .iter()
            .zip(return_records.iter())
            .filter(|(_, resp)| resp.get(constants::ERROR_CODE).is_none_or(|v| v.is_null()))
            .map(|(req, resp)| CaptureRecordRef {
                op: CaptureOp::PutRecords,
                ts,
                stream: stream_name,
                partition_key: Cow::Borrowed(req[constants::PARTITION_KEY].as_str().unwrap_or("")),
                data: req[constants::DATA].as_str().unwrap_or(""),
                explicit_hash_key: req[constants::EXPLICIT_HASH_KEY].as_str(),
                sequence_number: resp[constants::SEQUENCE_NUMBER].as_str().unwrap_or(""),
                shard_id: resp[constants::SHARD_ID].as_str().unwrap_or(""),
            })
            .collect();
        writer.write_records(&capture_refs);

        let captured = read_capture_file(capture_file.path()).unwrap();
        assert_eq!(captured.len(), 0);
    }
}
