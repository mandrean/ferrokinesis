//! Stream capture and replay support.
//!
//! [`CaptureWriter`] records PutRecord/PutRecords calls to NDJSON files.
//! [`read_capture_file`] reads them back for replay.

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// A single captured record in NDJSON format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureRecord {
    /// Operation name: `"PutRecord"` or `"PutRecords"`.
    pub op: String,
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

/// Thread-safe writer that appends [`CaptureRecord`]s as NDJSON lines.
///
/// All [`Store`](crate::store::Store) clones share the same writer via `Arc`.
#[derive(Clone)]
pub struct CaptureWriter {
    inner: Arc<Mutex<BufWriter<File>>>,
    scrub: bool,
}

impl CaptureWriter {
    /// Opens (or creates) a file in append mode for capture output.
    pub fn new(path: &Path, scrub: bool) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(BufWriter::new(file))),
            scrub,
        })
    }

    /// Writes a single capture record as one NDJSON line.
    ///
    /// Failures are logged via tracing and never propagated — capture must not
    /// affect the response path.
    pub fn write_record(&self, record: &CaptureRecord) {
        let Ok(mut line) = (if self.scrub {
            let mut scrubbed = record.clone();
            scrubbed.partition_key = scrub_partition_key(&scrubbed.partition_key);
            serde_json::to_vec(&scrubbed)
        } else {
            serde_json::to_vec(record)
        }) else {
            tracing::warn!("capture: failed to serialize record");
            return;
        };
        line.push(b'\n');
        let Ok(mut writer) = self.inner.lock() else {
            tracing::error!("capture: failed to acquire lock");
            return;
        };
        if let Err(e) = writer.write_all(&line).and_then(|()| writer.flush()) {
            tracing::warn!("capture: write error: {e}");
        }
    }
}

/// Reads an NDJSON capture file into a `Vec<CaptureRecord>`.
///
/// Blank lines are silently skipped. Malformed lines are logged to stderr and skipped.
pub fn read_capture_file(path: &Path) -> io::Result<Vec<CaptureRecord>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<CaptureRecord>(&line) {
            Ok(r) => records.push(r),
            Err(e) => eprintln!("capture: skipping malformed line: {e}"),
        }
    }
    Ok(records)
}

/// Deterministic anonymisation of a partition key.
///
/// Returns the hex-encoded MD5 hash of the key. This preserves shard
/// distribution because the emulator already uses MD5 for hash key computation.
pub fn scrub_partition_key(key: &str) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(key.as_bytes());
    let result = hasher.finalize();
    result.iter().fold(String::with_capacity(32), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scrub_is_deterministic() {
        let a = scrub_partition_key("my-key");
        let b = scrub_partition_key("my-key");
        assert_eq!(a, b);
        assert_ne!(a, "my-key");
        // MD5 hex is 32 chars
        assert_eq!(a.len(), 32);
    }

    #[test]
    fn scrub_different_keys_differ() {
        let a = scrub_partition_key("key-1");
        let b = scrub_partition_key("key-2");
        assert_ne!(a, b);
    }
}
