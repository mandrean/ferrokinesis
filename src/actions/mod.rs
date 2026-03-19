//! Operation dispatch for the Kinesis emulator.
//!
//! [`Operation`] enumerates all 39 supported Kinesis API operations.
//! [`dispatch`] routes a parsed operation to its handler function.
//!
//! Action handler submodules are internal implementation details; import
//! [`Operation`] and [`dispatch`] directly from this module.

#[doc(hidden)]
pub mod add_tags_to_stream;
#[doc(hidden)]
pub mod create_stream;
#[doc(hidden)]
pub mod decrease_stream_retention_period;
#[doc(hidden)]
pub mod delete_resource_policy;
#[doc(hidden)]
pub mod delete_stream;
#[doc(hidden)]
pub mod deregister_stream_consumer;
#[doc(hidden)]
pub mod describe_account_settings;
#[doc(hidden)]
pub mod describe_limits;
#[doc(hidden)]
pub mod describe_stream;
#[doc(hidden)]
pub mod describe_stream_consumer;
#[doc(hidden)]
pub mod describe_stream_summary;
#[doc(hidden)]
pub mod disable_enhanced_monitoring;
#[doc(hidden)]
pub mod enable_enhanced_monitoring;
#[doc(hidden)]
pub mod get_records;
#[doc(hidden)]
pub mod get_resource_policy;
#[doc(hidden)]
pub mod get_shard_iterator;
#[doc(hidden)]
pub mod increase_stream_retention_period;
#[doc(hidden)]
pub mod list_shards;
#[doc(hidden)]
pub mod list_stream_consumers;
#[doc(hidden)]
pub mod list_streams;
#[doc(hidden)]
pub mod list_tags_for_resource;
#[doc(hidden)]
pub mod list_tags_for_stream;
#[doc(hidden)]
pub mod merge_shards;
#[doc(hidden)]
pub mod put_record;
#[doc(hidden)]
pub mod put_records;
#[doc(hidden)]
pub mod put_resource_policy;
#[doc(hidden)]
pub mod register_stream_consumer;
#[doc(hidden)]
pub mod remove_tags_from_stream;
#[doc(hidden)]
pub mod split_shard;
#[doc(hidden)]
pub mod start_stream_encryption;
#[doc(hidden)]
pub mod stop_stream_encryption;
#[doc(hidden)]
pub mod subscribe_to_shard;
#[doc(hidden)]
pub mod tag_resource;
#[doc(hidden)]
pub mod untag_resource;
#[doc(hidden)]
pub mod update_account_settings;
#[doc(hidden)]
pub mod update_max_record_size;
#[doc(hidden)]
pub mod update_shard_count;
#[doc(hidden)]
pub mod update_stream_mode;
#[doc(hidden)]
pub mod update_stream_warm_throughput;

use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

/// All valid Kinesis API operations.
///
/// Adding a variant here causes the compiler to flag any exhaustiveness gaps
/// in [`dispatch`] and `server::get_validation_rules` — never use wildcard `_` arms.
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

impl std::str::FromStr for Operation {
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

/// Routes a parsed [`Operation`] to its handler.
///
/// Returns `Ok(None)` for operations that produce an empty 200 response, or
/// `Ok(Some(body))` with a JSON response body. `SubscribeToShard` is handled
/// separately in `server.rs` via `execute_streaming` and should never reach
/// this function during normal operation.
///
/// # Errors
///
/// Returns [`KinesisErrorResponse`] if the underlying handler returns an error,
/// or an `InternalFailure` (HTTP 500) if `SubscribeToShard` is dispatched here
/// (which should never happen in practice).
pub async fn dispatch(
    store: &Store,
    operation: Operation,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    match operation {
        Operation::AddTagsToStream => add_tags_to_stream::execute(store, data).await,
        Operation::CreateStream => create_stream::execute(store, data).await,
        Operation::DecreaseStreamRetentionPeriod => {
            decrease_stream_retention_period::execute(store, data).await
        }
        Operation::DeleteResourcePolicy => delete_resource_policy::execute(store, data).await,
        Operation::DeleteStream => delete_stream::execute(store, data).await,
        Operation::DeregisterStreamConsumer => {
            deregister_stream_consumer::execute(store, data).await
        }
        Operation::DescribeAccountSettings => describe_account_settings::execute(store, data).await,
        Operation::DescribeLimits => describe_limits::execute(store, data).await,
        Operation::DescribeStream => describe_stream::execute(store, data).await,
        Operation::DescribeStreamConsumer => describe_stream_consumer::execute(store, data).await,
        Operation::DescribeStreamSummary => describe_stream_summary::execute(store, data).await,
        Operation::DisableEnhancedMonitoring => {
            disable_enhanced_monitoring::execute(store, data).await
        }
        Operation::EnableEnhancedMonitoring => {
            enable_enhanced_monitoring::execute(store, data).await
        }
        Operation::GetRecords => get_records::execute(store, data).await,
        Operation::GetResourcePolicy => get_resource_policy::execute(store, data).await,
        Operation::GetShardIterator => get_shard_iterator::execute(store, data).await,
        Operation::IncreaseStreamRetentionPeriod => {
            increase_stream_retention_period::execute(store, data).await
        }
        Operation::ListShards => list_shards::execute(store, data).await,
        Operation::ListStreamConsumers => list_stream_consumers::execute(store, data).await,
        Operation::ListStreams => list_streams::execute(store, data).await,
        Operation::ListTagsForResource => list_tags_for_resource::execute(store, data).await,
        Operation::ListTagsForStream => list_tags_for_stream::execute(store, data).await,
        Operation::MergeShards => merge_shards::execute(store, data).await,
        Operation::PutRecord => put_record::execute(store, data).await,
        Operation::PutRecords => put_records::execute(store, data).await,
        Operation::PutResourcePolicy => put_resource_policy::execute(store, data).await,
        Operation::RegisterStreamConsumer => register_stream_consumer::execute(store, data).await,
        Operation::RemoveTagsFromStream => remove_tags_from_stream::execute(store, data).await,
        Operation::SplitShard => split_shard::execute(store, data).await,
        Operation::StartStreamEncryption => start_stream_encryption::execute(store, data).await,
        Operation::StopStreamEncryption => stop_stream_encryption::execute(store, data).await,
        Operation::SubscribeToShard => {
            // Handled separately in server.rs via execute_streaming; should not reach here.
            Err(KinesisErrorResponse::server_error(None, None))
        }
        Operation::TagResource => tag_resource::execute(store, data).await,
        Operation::UntagResource => untag_resource::execute(store, data).await,
        Operation::UpdateAccountSettings => update_account_settings::execute(store, data).await,
        Operation::UpdateMaxRecordSize => update_max_record_size::execute(store, data).await,
        Operation::UpdateShardCount => update_shard_count::execute(store, data).await,
        Operation::UpdateStreamMode => update_stream_mode::execute(store, data).await,
        Operation::UpdateStreamWarmThroughput => {
            update_stream_warm_throughput::execute(store, data).await
        }
    }
}
