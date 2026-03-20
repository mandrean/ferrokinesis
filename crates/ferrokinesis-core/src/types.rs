//! Domain types mirroring the AWS Kinesis Data Streams data model.
//!
//! These types are used throughout the emulator and serialized directly into
//! API responses. Serde attributes ensure the wire format matches the real
//! Kinesis JSON/CBOR encoding.

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt;
use serde::{Deserialize, Serialize, Serializer};

/// Returns `true` when `f` is a finite, whole-number value that round-trips
/// losslessly through `i64` — i.e. it can safely be emitted as an integer.
#[allow(clippy::cast_possible_truncation)]
pub fn is_whole_epoch(f: f64) -> bool {
    f.fract() == 0.0 && f.is_finite() && (f as i64 as f64) == f
}

/// Serializes an `f64` as an `i64` when it has no fractional part.
///
/// Real AWS Kinesis encodes timestamps as whole-number epoch seconds. The AWS
/// Java SDK v2 reads CBOR numbers via `Double.toString()` → `StringToInstant`,
/// and `Long.parseLong` rejects scientific notation (e.g. `"1.77E9"`).
/// Emitting integer-valued timestamps as `i64` avoids this: serde_json writes
/// `1773966938` (no `.0` suffix), and `json_to_cbor_impl` converts it to a
/// CBOR integer that Java reads cleanly.
fn serialize_epoch_seconds<S: Serializer>(val: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    #[allow(clippy::cast_possible_truncation)]
    if is_whole_epoch(*val) {
        serializer.serialize_i64(*val as i64)
    } else {
        serializer.serialize_f64(*val)
    }
}

/// Lifecycle state of a Kinesis data stream.
///
/// See the [AWS Kinesis stream lifecycle documentation](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StreamDescription.html).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamStatus {
    /// The stream is being created. Read and write operations are not available.
    Creating,
    /// The stream is being deleted.
    Deleting,
    /// The stream is ready for read and write operations.
    Active,
    /// The stream configuration is being updated (e.g. resharding or retention change).
    Updating,
}

impl fmt::Display for StreamStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Creating => write!(f, "CREATING"),
            Self::Deleting => write!(f, "DELETING"),
            Self::Active => write!(f, "ACTIVE"),
            Self::Updating => write!(f, "UPDATING"),
        }
    }
}

/// Lifecycle state of an enhanced fan-out consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsumerStatus {
    /// The consumer is being registered.
    Creating,
    /// The consumer is being deregistered.
    Deleting,
    /// The consumer is ready for `SubscribeToShard` calls.
    Active,
}

impl fmt::Display for ConsumerStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Creating => write!(f, "CREATING"),
            Self::Deleting => write!(f, "DELETING"),
            Self::Active => write!(f, "ACTIVE"),
        }
    }
}

/// An enhanced fan-out consumer registered to a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Consumer {
    /// Unique name for this consumer within the stream.
    pub consumer_name: String,
    /// ARN assigned to this consumer by Kinesis.
    #[serde(rename = "ConsumerARN")]
    pub consumer_arn: String,
    /// Current lifecycle state of the consumer.
    pub consumer_status: ConsumerStatus,
    /// Unix timestamp (seconds) when this consumer was created.
    #[serde(serialize_with = "serialize_epoch_seconds")]
    pub consumer_creation_timestamp: f64,
}

/// Server-side encryption type applied to a stream.
///
/// Used with `StartStreamEncryption` / `StopStreamEncryption`.
#[doc(alias = "StartStreamEncryption")]
#[doc(alias = "StopStreamEncryption")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionType {
    /// Data is encrypted using AWS Key Management Service (KMS).
    #[serde(rename = "KMS")]
    Kms,
    /// Data is not encrypted.
    #[serde(rename = "NONE")]
    None,
}

/// Capacity mode of a Kinesis data stream.
///
/// Used with `UpdateStreamMode`.
#[doc(alias = "UpdateStreamMode")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamMode {
    /// Stream capacity is managed by specifying a fixed shard count.
    Provisioned,
    /// Stream capacity scales automatically based on throughput demand.
    OnDemand,
}

/// Starting position type for a shard iterator.
///
/// Used with `GetShardIterator`.
#[doc(alias = "GetShardIterator")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ShardIteratorType {
    /// Start at the oldest available record in the shard.
    TrimHorizon,
    /// Start just after the most recently written record.
    Latest,
    /// Start at the record with the specified sequence number.
    AtSequenceNumber,
    /// Start just after the record with the specified sequence number.
    AfterSequenceNumber,
    /// Start at the first record at or after the specified timestamp.
    AtTimestamp,
}

/// Stream mode configuration wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StreamModeDetails {
    /// The capacity mode of the stream.
    pub stream_mode: StreamMode,
}

/// Full stream descriptor including shards, status, and metadata.
///
/// This is the primary type stored in the store and returned
/// in `DescribeStream` / `DescribeStreamSummary` responses.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Stream {
    /// Record retention period in hours (24–8760).
    pub retention_period_hours: u32,
    /// Shard-level CloudWatch metric configuration for this stream.
    pub enhanced_monitoring: Vec<EnhancedMonitoring>,
    /// Server-side encryption type currently applied to the stream.
    pub encryption_type: EncryptionType,
    /// Whether more shards are available beyond those in `shards` (pagination).
    pub has_more_shards: bool,
    /// List of shards that make up this stream.
    pub shards: Vec<Shard>,
    /// ARN of this stream.
    #[serde(rename = "StreamARN")]
    pub stream_arn: String,
    /// Name of this stream.
    pub stream_name: String,
    /// Current lifecycle state of this stream.
    pub stream_status: StreamStatus,
    /// Unix timestamp (seconds) when this stream was created.
    #[serde(serialize_with = "serialize_epoch_seconds")]
    pub stream_creation_timestamp: f64,
    /// Capacity mode details for this stream.
    pub stream_mode_details: StreamModeDetails,

    // Hidden fields (not returned in API responses)
    #[serde(skip)]
    #[doc(hidden)]
    pub seq_ix: Vec<Option<u64>>,
    #[serde(skip)]
    #[doc(hidden)]
    pub tags: BTreeMap<String, String>,
    #[serde(skip)]
    #[doc(hidden)]
    pub key_id: Option<String>,
    #[serde(skip)]
    #[doc(hidden)]
    pub warm_throughput_mibps: u32,
    #[serde(skip)]
    #[doc(hidden)]
    pub max_record_size_kib: u32,
}

impl Stream {
    /// Internal constructor used by [`StreamBuilder::build()`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        retention_period_hours: u32,
        enhanced_monitoring: Vec<EnhancedMonitoring>,
        encryption_type: EncryptionType,
        has_more_shards: bool,
        shards: Vec<Shard>,
        stream_arn: String,
        stream_name: String,
        stream_status: StreamStatus,
        stream_creation_timestamp: f64,
        stream_mode_details: StreamModeDetails,
        seq_ix: Vec<Option<u64>>,
        tags: BTreeMap<String, String>,
        key_id: Option<String>,
        warm_throughput_mibps: u32,
        max_record_size_kib: u32,
    ) -> Self {
        Self {
            retention_period_hours,
            enhanced_monitoring,
            encryption_type,
            has_more_shards,
            shards,
            stream_arn,
            stream_name,
            stream_status,
            stream_creation_timestamp,
            stream_mode_details,
            seq_ix,
            tags,
            key_id,
            warm_throughput_mibps,
            max_record_size_kib,
        }
    }
}

/// Ergonomic builder for [`Stream`].
///
/// Required parameters are supplied via [`new()`](StreamBuilder::new);
/// optional fields have sensible defaults matching `CreateStream` behaviour
/// and can be overridden with setter methods.
///
/// # Example
///
/// ```
/// # use ferrokinesis_core::types::*;
/// let stream = StreamBuilder::new(
///     "my-stream".into(),
///     "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream".into(),
///     StreamStatus::Creating,
///     1700000000.0,
///     vec![],
///     vec![None],
/// )
/// .retention_period_hours(48)
/// .build();
///
/// assert_eq!(stream.retention_period_hours, 48);
/// ```
pub struct StreamBuilder {
    stream_name: String,
    stream_arn: String,
    stream_status: StreamStatus,
    stream_creation_timestamp: f64,
    shards: Vec<Shard>,
    seq_ix: Vec<Option<u64>>,
    retention_period_hours: u32,
    enhanced_monitoring: Vec<EnhancedMonitoring>,
    encryption_type: EncryptionType,
    has_more_shards: bool,
    stream_mode_details: StreamModeDetails,
    tags: BTreeMap<String, String>,
    key_id: Option<String>,
    warm_throughput_mibps: u32,
    max_record_size_kib: u32,
}

impl StreamBuilder {
    /// Create a new builder with all required fields.
    pub fn new(
        stream_name: String,
        stream_arn: String,
        stream_status: StreamStatus,
        stream_creation_timestamp: f64,
        shards: Vec<Shard>,
        seq_ix: Vec<Option<u64>>,
    ) -> Self {
        Self {
            stream_name,
            stream_arn,
            stream_status,
            stream_creation_timestamp,
            shards,
            seq_ix,
            retention_period_hours: 24,
            enhanced_monitoring: alloc::vec![EnhancedMonitoring {
                shard_level_metrics: alloc::vec![],
            }],
            encryption_type: EncryptionType::None,
            has_more_shards: false,
            stream_mode_details: StreamModeDetails {
                stream_mode: StreamMode::Provisioned,
            },
            tags: BTreeMap::new(),
            key_id: None,
            warm_throughput_mibps: 0,
            max_record_size_kib: 1024,
        }
    }

    /// Set the retention period in hours (default: 24).
    pub fn retention_period_hours(mut self, hours: u32) -> Self {
        self.retention_period_hours = hours;
        self
    }

    /// Set enhanced monitoring configuration.
    pub fn enhanced_monitoring(mut self, monitoring: Vec<EnhancedMonitoring>) -> Self {
        self.enhanced_monitoring = monitoring;
        self
    }

    /// Set the encryption type (default: `None`).
    pub fn encryption_type(mut self, encryption_type: EncryptionType) -> Self {
        self.encryption_type = encryption_type;
        self
    }

    /// Set whether the stream has more shards (default: `false`).
    pub fn has_more_shards(mut self, has_more: bool) -> Self {
        self.has_more_shards = has_more;
        self
    }

    /// Set stream mode details (default: `Provisioned`).
    pub fn stream_mode_details(mut self, details: StreamModeDetails) -> Self {
        self.stream_mode_details = details;
        self
    }

    /// Set initial tags (default: empty).
    pub fn tags(mut self, tags: BTreeMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    /// Set KMS key ID for encryption (default: `None`).
    pub fn key_id(mut self, key_id: Option<String>) -> Self {
        self.key_id = key_id;
        self
    }

    /// Set warm throughput in MiB/s (default: 0).
    pub fn warm_throughput_mibps(mut self, mibps: u32) -> Self {
        self.warm_throughput_mibps = mibps;
        self
    }

    /// Set maximum record size in KiB (default: 1024).
    pub fn max_record_size_kib(mut self, kib: u32) -> Self {
        self.max_record_size_kib = kib;
        self
    }

    /// Consume the builder and produce a [`Stream`].
    pub fn build(self) -> Stream {
        Stream::new(
            self.retention_period_hours,
            self.enhanced_monitoring,
            self.encryption_type,
            self.has_more_shards,
            self.shards,
            self.stream_arn,
            self.stream_name,
            self.stream_status,
            self.stream_creation_timestamp,
            self.stream_mode_details,
            self.seq_ix,
            self.tags,
            self.key_id,
            self.warm_throughput_mibps,
            self.max_record_size_kib,
        )
    }
}

/// Shard-level CloudWatch metric configuration for a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EnhancedMonitoring {
    /// The shard-level metrics that are enabled. An empty list means no metrics.
    pub shard_level_metrics: Vec<String>,
}

/// Describes a single shard within a Kinesis data stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Shard {
    /// Unique identifier for this shard (e.g. `"shardId-000000000000"`).
    pub shard_id: String,
    /// MD5 hash key range covered by this shard.
    pub hash_key_range: HashKeyRange,
    /// Sequence number range assigned to this shard.
    pub sequence_number_range: SequenceNumberRange,
    /// ID of the parent shard that this shard was split or merged from, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_shard_id: Option<String>,
    /// ID of the adjacent parent shard involved in a merge, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_parent_shard_id: Option<String>,
}

/// Inclusive range of MD5 hash keys covered by a shard.
///
/// The full key space (`0` to `2^128 - 1`) is divided among the shards in a stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct HashKeyRange {
    /// Inclusive lower bound of the hash key range (decimal string).
    pub starting_hash_key: String,
    /// Inclusive upper bound of the hash key range (decimal string).
    pub ending_hash_key: String,
}

/// The range of sequence numbers assigned to a shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SequenceNumberRange {
    /// The sequence number of the first record written to the shard.
    pub starting_sequence_number: String,
    /// The sequence number of the last record in the shard, if the shard is closed.
    /// `None` for open (active) shards.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ending_sequence_number: Option<String>,
}

/// A record as stored internally in the emulator.
///
/// Does not include the sequence number (the key is stored separately in the
/// record store). Clients receive [`ResponseRecord`] which includes the sequence number.
///
/// INVARIANT: Field order and types must exactly match [`StoredRecordRef`].
/// `postcard` serializes by position, not name — a mismatch silently corrupts data.
/// See `postcard_roundtrip_stored_record_ref_to_stored_record` test in `tests/unit.rs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StoredRecord {
    /// Partition key used to assign the record to a shard.
    pub partition_key: String,
    /// Base64-encoded record payload.
    pub data: String,
    /// Unix timestamp (seconds) when the record arrived at the stream.
    pub approximate_arrival_timestamp: f64,
}

/// Borrowing variant of [`StoredRecord`] for zero-copy writes.
///
/// INVARIANT: Field order and types must exactly match [`StoredRecord`].
/// `postcard` serializes by position, not name — a mismatch silently corrupts data.
/// See `postcard_roundtrip_stored_record_ref_to_stored_record` test in `tests/unit.rs`.
#[derive(Serialize)]
pub struct StoredRecordRef<'a> {
    /// Partition key used to assign the record to a shard.
    pub partition_key: &'a str,
    /// Base64-encoded record payload.
    pub data: &'a str,
    /// Unix timestamp (seconds) when the record arrived at the stream.
    pub approximate_arrival_timestamp: f64,
}

/// A record as returned to clients by `GetRecords` and `SubscribeToShard`.
///
/// Borrows from the underlying [`StoredRecord`] to avoid intermediate
/// `serde_json::Value` allocations.
#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseRecord<'a> {
    /// Partition key used to assign the record to a shard.
    pub partition_key: &'a str,
    /// Base64-encoded record payload.
    pub data: &'a str,
    /// Unix timestamp (seconds) when the record arrived at the stream.
    #[serde(serialize_with = "serialize_epoch_seconds")]
    pub approximate_arrival_timestamp: f64,
    /// The sequence number of this record within its shard.
    pub sequence_number: &'a str,
}
