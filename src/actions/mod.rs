pub mod add_tags_to_stream;
pub mod create_stream;
pub mod decrease_stream_retention_period;
pub mod delete_resource_policy;
pub mod delete_stream;
pub mod deregister_stream_consumer;
pub mod describe_account_settings;
pub mod describe_limits;
pub mod describe_stream;
pub mod describe_stream_consumer;
pub mod describe_stream_summary;
pub mod disable_enhanced_monitoring;
pub mod enable_enhanced_monitoring;
pub mod get_records;
pub mod get_resource_policy;
pub mod get_shard_iterator;
pub mod increase_stream_retention_period;
pub mod list_shards;
pub mod list_stream_consumers;
pub mod list_streams;
pub mod list_tags_for_resource;
pub mod list_tags_for_stream;
pub mod merge_shards;
pub mod put_record;
pub mod put_records;
pub mod put_resource_policy;
pub mod register_stream_consumer;
pub mod remove_tags_from_stream;
pub mod split_shard;
pub mod start_stream_encryption;
pub mod stop_stream_encryption;
pub mod subscribe_to_shard;
pub mod tag_resource;
pub mod untag_resource;
pub mod update_account_settings;
pub mod update_max_record_size;
pub mod update_shard_count;
pub mod update_stream_mode;
pub mod update_stream_warm_throughput;

use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

/// All valid Kinesis API operations. Adding a variant here causes the compiler
/// to flag any exhaustiveness gaps in `dispatch` and `server::get_validation_rules`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    AddTagsToStream,
    CreateStream,
    DecreaseStreamRetentionPeriod,
    DeleteResourcePolicy,
    DeleteStream,
    DeregisterStreamConsumer,
    DescribeAccountSettings,
    DescribeLimits,
    DescribeStream,
    DescribeStreamConsumer,
    DescribeStreamSummary,
    DisableEnhancedMonitoring,
    EnableEnhancedMonitoring,
    GetRecords,
    GetResourcePolicy,
    GetShardIterator,
    IncreaseStreamRetentionPeriod,
    ListShards,
    ListStreamConsumers,
    ListStreams,
    ListTagsForResource,
    ListTagsForStream,
    MergeShards,
    PutRecord,
    PutRecords,
    PutResourcePolicy,
    RegisterStreamConsumer,
    RemoveTagsFromStream,
    SplitShard,
    StartStreamEncryption,
    StopStreamEncryption,
    SubscribeToShard,
    TagResource,
    UntagResource,
    UpdateAccountSettings,
    UpdateMaxRecordSize,
    UpdateShardCount,
    UpdateStreamMode,
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
