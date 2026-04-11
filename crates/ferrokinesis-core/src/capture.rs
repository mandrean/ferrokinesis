use alloc::string::String;
#[cfg(feature = "std")]
use alloc::vec::Vec;
use serde::{Deserialize, Serialize};

/// The capture operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CaptureOp {
    /// A single `PutRecord` call.
    PutRecord,
    /// A `PutRecords` batch call.
    PutRecords,
}

/// Owned capture record used for deserialization (replay) and tests.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureRecord {
    /// Which operation produced this record.
    pub op: CaptureOp,
    /// Timestamp in milliseconds since epoch.
    pub ts: u64,
    /// Stream name the record was written to.
    pub stream: String,
    /// Partition key (possibly scrubbed).
    pub partition_key: String,
    /// Record data (base64-encoded).
    pub data: String,
    /// Explicit hash key, if provided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explicit_hash_key: Option<String>,
    /// Sequence number from the response (informational).
    pub sequence_number: String,
    /// Shard ID from the response (informational).
    pub shard_id: String,
}

/// Reads an NDJSON capture file into a `Vec<CaptureRecord>`.
///
/// Blank lines are silently skipped. Malformed lines are logged and skipped.
///
/// Note: loads the entire file into memory. For very large capture files,
/// consider a streaming approach in the future.
#[cfg(feature = "std")]
pub fn read_capture_file(path: &std::path::Path) -> std::io::Result<Vec<CaptureRecord>> {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<CaptureRecord>(&line) {
            Ok(record) => records.push(record),
            Err(error) => eprintln!("capture: skipping malformed line: {error}"),
        }
    }
    Ok(records)
}
