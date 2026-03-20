//! Kinesis API operation identifiers.

use crate::validation::{self, FieldDef};
use alloc::vec;
use alloc::vec::Vec;
use core::fmt;
use core::str::FromStr;

/// All valid Kinesis API operations.
///
/// Adding a variant here causes the compiler to flag any exhaustiveness gaps
/// in `dispatch` and `validation_rules` — never use wildcard `_` arms.
///
/// Each variant corresponds to the operation name from the `X-Amz-Target` header
/// (e.g. `Kinesis_20131202.PutRecord`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Adds or updates tags on a stream.
    #[doc(alias = "Kinesis_20131202.AddTagsToStream")]
    AddTagsToStream,
    /// Creates a new Kinesis data stream.
    #[doc(alias = "Kinesis_20131202.CreateStream")]
    CreateStream,
    /// Decreases the stream's data retention period.
    #[doc(alias = "Kinesis_20131202.DecreaseStreamRetentionPeriod")]
    DecreaseStreamRetentionPeriod,
    /// Deletes a resource-based policy from a stream or consumer.
    #[doc(alias = "Kinesis_20131202.DeleteResourcePolicy")]
    DeleteResourcePolicy,
    /// Deletes a Kinesis data stream and all its shards and data.
    #[doc(alias = "Kinesis_20131202.DeleteStream")]
    DeleteStream,
    /// Deregisters an enhanced fan-out consumer from a stream.
    #[doc(alias = "Kinesis_20131202.DeregisterStreamConsumer")]
    DeregisterStreamConsumer,
    /// Returns the limits for the current account and region.
    #[doc(alias = "Kinesis_20131202.DescribeAccountSettings")]
    DescribeAccountSettings,
    /// Describes the shard limits and usage for the account.
    #[doc(alias = "Kinesis_20131202.DescribeLimits")]
    DescribeLimits,
    /// Returns detailed information about a stream, including its shards.
    #[doc(alias = "Kinesis_20131202.DescribeStream")]
    DescribeStream,
    /// Returns detailed information about a registered consumer.
    #[doc(alias = "Kinesis_20131202.DescribeStreamConsumer")]
    DescribeStreamConsumer,
    /// Returns a summary of the stream without shard-level detail.
    #[doc(alias = "Kinesis_20131202.DescribeStreamSummary")]
    DescribeStreamSummary,
    /// Disables enhanced shard-level CloudWatch metrics for a stream.
    #[doc(alias = "Kinesis_20131202.DisableEnhancedMonitoring")]
    DisableEnhancedMonitoring,
    /// Enables enhanced shard-level CloudWatch metrics for a stream.
    #[doc(alias = "Kinesis_20131202.EnableEnhancedMonitoring")]
    EnableEnhancedMonitoring,
    /// Gets data records from a shard using a shard iterator.
    #[doc(alias = "Kinesis_20131202.GetRecords")]
    GetRecords,
    /// Returns the resource-based policy for a stream or consumer.
    #[doc(alias = "Kinesis_20131202.GetResourcePolicy")]
    GetResourcePolicy,
    /// Returns a shard iterator for reading records from a shard.
    #[doc(alias = "Kinesis_20131202.GetShardIterator")]
    GetShardIterator,
    /// Increases the stream's data retention period.
    #[doc(alias = "Kinesis_20131202.IncreaseStreamRetentionPeriod")]
    IncreaseStreamRetentionPeriod,
    /// Lists the shards in a stream and provides information about each.
    #[doc(alias = "Kinesis_20131202.ListShards")]
    ListShards,
    /// Lists the consumers registered to a stream.
    #[doc(alias = "Kinesis_20131202.ListStreamConsumers")]
    ListStreamConsumers,
    /// Lists the Kinesis data streams in the current account and region.
    #[doc(alias = "Kinesis_20131202.ListStreams")]
    ListStreams,
    /// Lists the tags for a Kinesis resource (stream or consumer).
    #[doc(alias = "Kinesis_20131202.ListTagsForResource")]
    ListTagsForResource,
    /// Lists the tags for a stream (legacy operation; prefer `ListTagsForResource`).
    #[doc(alias = "Kinesis_20131202.ListTagsForStream")]
    ListTagsForStream,
    /// Merges two adjacent shards in a stream into a single shard.
    #[doc(alias = "Kinesis_20131202.MergeShards")]
    MergeShards,
    /// Writes a single data record into a stream.
    #[doc(alias = "Kinesis_20131202.PutRecord")]
    PutRecord,
    /// Writes multiple data records into a stream in a single call.
    #[doc(alias = "Kinesis_20131202.PutRecords")]
    PutRecords,
    /// Attaches a resource-based policy to a stream or consumer.
    #[doc(alias = "Kinesis_20131202.PutResourcePolicy")]
    PutResourcePolicy,
    /// Registers an enhanced fan-out consumer with a stream.
    #[doc(alias = "Kinesis_20131202.RegisterStreamConsumer")]
    RegisterStreamConsumer,
    /// Removes tags from a stream.
    #[doc(alias = "Kinesis_20131202.RemoveTagsFromStream")]
    RemoveTagsFromStream,
    /// Splits a shard into two new shards.
    #[doc(alias = "Kinesis_20131202.SplitShard")]
    SplitShard,
    /// Enables server-side encryption using KMS for a stream.
    #[doc(alias = "Kinesis_20131202.StartStreamEncryption")]
    StartStreamEncryption,
    /// Disables server-side encryption for a stream.
    #[doc(alias = "Kinesis_20131202.StopStreamEncryption")]
    StopStreamEncryption,
    /// Subscribes to receive data records from a shard via HTTP/2 event stream.
    #[doc(alias = "Kinesis_20131202.SubscribeToShard")]
    SubscribeToShard,
    /// Adds or updates tags on a Kinesis resource.
    #[doc(alias = "Kinesis_20131202.TagResource")]
    TagResource,
    /// Removes tags from a Kinesis resource.
    #[doc(alias = "Kinesis_20131202.UntagResource")]
    UntagResource,
    /// Updates account-level settings.
    #[doc(alias = "Kinesis_20131202.UpdateAccountSettings")]
    UpdateAccountSettings,
    /// Updates the maximum record size for a stream.
    #[doc(alias = "Kinesis_20131202.UpdateMaxRecordSize")]
    UpdateMaxRecordSize,
    /// Updates the shard count of a stream.
    #[doc(alias = "Kinesis_20131202.UpdateShardCount")]
    UpdateShardCount,
    /// Switches a stream between `PROVISIONED` and `ON_DEMAND` capacity modes.
    #[doc(alias = "Kinesis_20131202.UpdateStreamMode")]
    UpdateStreamMode,
    /// Updates the warm throughput configuration of a stream.
    #[doc(alias = "Kinesis_20131202.UpdateStreamWarmThroughput")]
    UpdateStreamWarmThroughput,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for Operation {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "AddTagsToStream" => Ok(Self::AddTagsToStream),
            "CreateStream" => Ok(Self::CreateStream),
            "DecreaseStreamRetentionPeriod" => Ok(Self::DecreaseStreamRetentionPeriod),
            "DeleteResourcePolicy" => Ok(Self::DeleteResourcePolicy),
            "DeleteStream" => Ok(Self::DeleteStream),
            "DeregisterStreamConsumer" => Ok(Self::DeregisterStreamConsumer),
            "DescribeAccountSettings" => Ok(Self::DescribeAccountSettings),
            "DescribeLimits" => Ok(Self::DescribeLimits),
            "DescribeStream" => Ok(Self::DescribeStream),
            "DescribeStreamConsumer" => Ok(Self::DescribeStreamConsumer),
            "DescribeStreamSummary" => Ok(Self::DescribeStreamSummary),
            "DisableEnhancedMonitoring" => Ok(Self::DisableEnhancedMonitoring),
            "EnableEnhancedMonitoring" => Ok(Self::EnableEnhancedMonitoring),
            "GetRecords" => Ok(Self::GetRecords),
            "GetResourcePolicy" => Ok(Self::GetResourcePolicy),
            "GetShardIterator" => Ok(Self::GetShardIterator),
            "IncreaseStreamRetentionPeriod" => Ok(Self::IncreaseStreamRetentionPeriod),
            "ListShards" => Ok(Self::ListShards),
            "ListStreamConsumers" => Ok(Self::ListStreamConsumers),
            "ListStreams" => Ok(Self::ListStreams),
            "ListTagsForResource" => Ok(Self::ListTagsForResource),
            "ListTagsForStream" => Ok(Self::ListTagsForStream),
            "MergeShards" => Ok(Self::MergeShards),
            "PutRecord" => Ok(Self::PutRecord),
            "PutRecords" => Ok(Self::PutRecords),
            "PutResourcePolicy" => Ok(Self::PutResourcePolicy),
            "RegisterStreamConsumer" => Ok(Self::RegisterStreamConsumer),
            "RemoveTagsFromStream" => Ok(Self::RemoveTagsFromStream),
            "SplitShard" => Ok(Self::SplitShard),
            "StartStreamEncryption" => Ok(Self::StartStreamEncryption),
            "StopStreamEncryption" => Ok(Self::StopStreamEncryption),
            "SubscribeToShard" => Ok(Self::SubscribeToShard),
            "TagResource" => Ok(Self::TagResource),
            "UntagResource" => Ok(Self::UntagResource),
            "UpdateAccountSettings" => Ok(Self::UpdateAccountSettings),
            "UpdateMaxRecordSize" => Ok(Self::UpdateMaxRecordSize),
            "UpdateShardCount" => Ok(Self::UpdateShardCount),
            "UpdateStreamMode" => Ok(Self::UpdateStreamMode),
            "UpdateStreamWarmThroughput" => Ok(Self::UpdateStreamWarmThroughput),
            _ => Err(()),
        }
    }
}

impl Operation {
    /// Returns the validation rules for this operation.
    pub fn validation_rules(&self) -> Vec<(&'static str, FieldDef)> {
        use validation::rules;
        match self {
            Operation::AddTagsToStream => rules::add_tags_to_stream(),
            Operation::CreateStream => rules::create_stream(),
            Operation::DecreaseStreamRetentionPeriod => rules::decrease_stream_retention_period(),
            Operation::DeleteResourcePolicy => rules::delete_resource_policy(),
            Operation::DeleteStream => rules::delete_stream(),
            Operation::DeregisterStreamConsumer => rules::deregister_stream_consumer(),
            Operation::DescribeAccountSettings => vec![],
            Operation::DescribeLimits => vec![],
            Operation::DescribeStream => rules::describe_stream(),
            Operation::DescribeStreamConsumer => rules::describe_stream_consumer(),
            Operation::DescribeStreamSummary => rules::describe_stream_summary(),
            Operation::DisableEnhancedMonitoring => rules::disable_enhanced_monitoring(),
            Operation::EnableEnhancedMonitoring => rules::enable_enhanced_monitoring(),
            Operation::GetRecords => rules::get_records(),
            Operation::GetResourcePolicy => rules::get_resource_policy(),
            Operation::GetShardIterator => rules::get_shard_iterator(),
            Operation::IncreaseStreamRetentionPeriod => rules::increase_stream_retention_period(),
            Operation::ListShards => rules::list_shards(),
            Operation::ListStreamConsumers => rules::list_stream_consumers(),
            Operation::ListStreams => rules::list_streams(),
            Operation::ListTagsForResource => rules::list_tags_for_resource(),
            Operation::ListTagsForStream => rules::list_tags_for_stream(),
            Operation::MergeShards => rules::merge_shards(),
            Operation::PutRecord => rules::put_record(),
            Operation::PutRecords => rules::put_records(),
            Operation::PutResourcePolicy => rules::put_resource_policy(),
            Operation::RegisterStreamConsumer => rules::register_stream_consumer(),
            Operation::RemoveTagsFromStream => rules::remove_tags_from_stream(),
            Operation::SplitShard => rules::split_shard(),
            Operation::StartStreamEncryption => rules::start_stream_encryption(),
            Operation::StopStreamEncryption => rules::stop_stream_encryption(),
            Operation::SubscribeToShard => rules::subscribe_to_shard(),
            Operation::TagResource => rules::tag_resource(),
            Operation::UntagResource => rules::untag_resource(),
            Operation::UpdateAccountSettings => rules::update_account_settings(),
            Operation::UpdateMaxRecordSize => rules::update_max_record_size(),
            Operation::UpdateShardCount => rules::update_shard_count(),
            Operation::UpdateStreamMode => rules::update_stream_mode(),
            Operation::UpdateStreamWarmThroughput => rules::update_stream_warm_throughput(),
        }
    }
}
