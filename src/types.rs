//! Domain types mirroring the AWS Kinesis Data Streams data model.
//!
//! These types are used throughout the emulator and serialized directly into
//! API responses. Serde attributes ensure the wire format matches the real
//! Kinesis JSON/CBOR encoding.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

impl std::fmt::Display for StreamStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Display for ConsumerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
/// This is the primary type stored in the [`crate::store::Store`] and returned
/// in `DescribeStream` / `DescribeStreamSummary` responses.
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
    pub stream_creation_timestamp: f64,
    /// Capacity mode details for this stream.
    pub stream_mode_details: StreamModeDetails,

    // Hidden fields (not returned in API responses)
    #[serde(skip)]
    pub(crate) seq_ix: Vec<Option<u64>>,
    #[serde(skip)]
    pub(crate) tags: BTreeMap<String, String>,
    #[serde(skip)]
    pub(crate) key_id: Option<String>,
    #[serde(skip)]
    pub(crate) warm_throughput_mibps: u32,
    #[serde(skip)]
    pub(crate) max_record_size_kib: u32,
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
/// record store). Clients receive [`OutputRecord`] which includes the sequence number.
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

/// A record as returned to clients by `GetRecords` / `SubscribeToShard`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OutputRecord {
    /// Partition key used to assign the record to a shard.
    pub partition_key: String,
    /// Base64-encoded record payload.
    pub data: String,
    /// Unix timestamp (seconds) when the record arrived at the stream.
    pub approximate_arrival_timestamp: f64,
    /// The sequence number of this record within its shard.
    pub sequence_number: String,
}
