//! Stream capture support and shared replay format helpers.
//!
//! [`CaptureWriter`] records PutRecord/PutRecords calls to NDJSON files.
//! [`read_capture_file`] reads them back for replay.

pub use ferrokinesis_core::capture::{CaptureOp, CaptureRecord, read_capture_file};
use serde::Serialize;
use std::borrow::Cow;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Borrowing capture record for zero-copy serialization on the hot path.
///
/// Mirrors [`CaptureRecord`] but borrows string fields from the request/response
/// `Value`s, avoiding per-field `String` allocations. The `partition_key` uses
/// `Cow` so the scrub path can substitute an owned hash without cloning the
/// entire record.
#[derive(Serialize)]
pub struct CaptureRecordRef<'a> {
    /// Which operation produced this record.
    pub op: CaptureOp,
    /// Timestamp in milliseconds since epoch.
    pub ts: u64,
    /// Stream name the record was written to.
    pub stream: &'a str,
    /// Partition key — borrowed when unscrubbed, owned MD5 hex when scrubbed.
    pub partition_key: Cow<'a, str>,
    /// Record data (base64-encoded).
    pub data: &'a str,
    /// Explicit hash key, if provided.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explicit_hash_key: Option<&'a str>,
    /// Sequence number from the response (informational).
    pub sequence_number: &'a str,
    /// Shard ID from the response (informational).
    pub shard_id: &'a str,
}

/// Thread-safe writer that appends capture records as NDJSON lines.
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
    pub fn write_record(&self, record: &CaptureRecordRef<'_>) {
        self.write_records(std::slice::from_ref(record));
    }

    /// Writes multiple capture records, acquiring the lock once and flushing
    /// once at the end. Preferred over repeated [`write_record`](Self::write_record)
    /// calls for batch operations like `PutRecords`.
    pub fn write_records(&self, records: &[CaptureRecordRef<'_>]) {
        // Serialize all records before acquiring the lock
        let mut lines: Vec<Vec<u8>> = Vec::with_capacity(records.len());
        for record in records {
            let Ok(mut line) = (if self.scrub {
                // Only replace partition_key — avoids cloning the entire record
                let scrubbed = CaptureRecordRef {
                    op: record.op,
                    ts: record.ts,
                    stream: record.stream,
                    partition_key: Cow::Owned(scrub_partition_key(&record.partition_key)),
                    data: record.data,
                    explicit_hash_key: record.explicit_hash_key,
                    sequence_number: record.sequence_number,
                    shard_id: record.shard_id,
                };
                serde_json::to_vec(&scrubbed)
            } else {
                serde_json::to_vec(record)
            }) else {
                tracing::warn!("capture: failed to serialize record");
                continue;
            };
            line.push(b'\n');
            lines.push(line);
        }
        if lines.is_empty() {
            return;
        }
        let Ok(mut writer) = self.inner.lock() else {
            tracing::error!("capture: failed to acquire lock");
            return;
        };
        for line in &lines {
            if let Err(e) = writer.write_all(line) {
                tracing::warn!("capture: write error: {e}");
                return;
            }
        }
        if let Err(e) = writer.flush() {
            tracing::warn!("capture: flush error: {e}");
        }
    }
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
    fn write_records_empty_slice_is_noop() {
        let capture_file = tempfile::NamedTempFile::new().unwrap();
        let writer = CaptureWriter::new(capture_file.path(), false).unwrap();
        writer.write_records(&[]);
        let captured = read_capture_file(capture_file.path()).unwrap();
        assert_eq!(captured.len(), 0);
    }

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
