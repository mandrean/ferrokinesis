use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamStatus {
    Creating,
    Deleting,
    Active,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConsumerStatus {
    Creating,
    Deleting,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Consumer {
    pub consumer_name: String,
    #[serde(rename = "ConsumerARN")]
    pub consumer_arn: String,
    pub consumer_status: ConsumerStatus,
    pub consumer_creation_timestamp: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionType {
    #[serde(rename = "KMS")]
    Kms,
    #[serde(rename = "NONE")]
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StreamMode {
    Provisioned,
    OnDemand,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ShardIteratorType {
    TrimHorizon,
    Latest,
    AtSequenceNumber,
    AfterSequenceNumber,
    AtTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StreamModeDetails {
    pub stream_mode: StreamMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Stream {
    pub retention_period_hours: u32,
    pub enhanced_monitoring: Vec<EnhancedMonitoring>,
    pub encryption_type: EncryptionType,
    pub has_more_shards: bool,
    pub shards: Vec<Shard>,
    #[serde(rename = "StreamARN")]
    pub stream_arn: String,
    pub stream_name: String,
    pub stream_status: StreamStatus,
    pub stream_creation_timestamp: f64,
    pub stream_mode_details: StreamModeDetails,

    // Hidden fields (not returned in API responses)
    #[serde(skip)]
    pub seq_ix: Vec<Option<u64>>,
    #[serde(skip)]
    pub tags: BTreeMap<String, String>,
    #[serde(skip)]
    pub key_id: Option<String>,
    #[serde(skip)]
    pub warm_throughput_mibps: u32,
    #[serde(skip)]
    pub max_record_size_kib: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EnhancedMonitoring {
    pub shard_level_metrics: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Shard {
    pub shard_id: String,
    pub hash_key_range: HashKeyRange,
    pub sequence_number_range: SequenceNumberRange,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_shard_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub adjacent_parent_shard_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct HashKeyRange {
    pub starting_hash_key: String,
    pub ending_hash_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SequenceNumberRange {
    pub starting_sequence_number: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ending_sequence_number: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StoredRecord {
    pub partition_key: String,
    pub data: String,
    pub approximate_arrival_timestamp: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OutputRecord {
    pub partition_key: String,
    pub data: String,
    pub approximate_arrival_timestamp: f64,
    pub sequence_number: String,
}
