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

use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

pub async fn dispatch(
    store: &Store,
    operation: &str,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    match operation {
        "AddTagsToStream" => add_tags_to_stream::execute(store, data).await,
        "CreateStream" => create_stream::execute(store, data).await,
        "DecreaseStreamRetentionPeriod" => {
            decrease_stream_retention_period::execute(store, data).await
        }
        "DeleteResourcePolicy" => delete_resource_policy::execute(store, data).await,
        "DeleteStream" => delete_stream::execute(store, data).await,
        "DeregisterStreamConsumer" => deregister_stream_consumer::execute(store, data).await,
        "DescribeAccountSettings" => describe_account_settings::execute(store, data).await,
        "DescribeLimits" => describe_limits::execute(store, data).await,
        "DescribeStream" => describe_stream::execute(store, data).await,
        "DescribeStreamConsumer" => describe_stream_consumer::execute(store, data).await,
        "DescribeStreamSummary" => describe_stream_summary::execute(store, data).await,
        "DisableEnhancedMonitoring" => disable_enhanced_monitoring::execute(store, data).await,
        "EnableEnhancedMonitoring" => enable_enhanced_monitoring::execute(store, data).await,
        "GetRecords" => get_records::execute(store, data).await,
        "GetResourcePolicy" => get_resource_policy::execute(store, data).await,
        "GetShardIterator" => get_shard_iterator::execute(store, data).await,
        "IncreaseStreamRetentionPeriod" => {
            increase_stream_retention_period::execute(store, data).await
        }
        "ListShards" => list_shards::execute(store, data).await,
        "ListStreamConsumers" => list_stream_consumers::execute(store, data).await,
        "ListStreams" => list_streams::execute(store, data).await,
        "ListTagsForResource" => list_tags_for_resource::execute(store, data).await,
        "ListTagsForStream" => list_tags_for_stream::execute(store, data).await,
        "MergeShards" => merge_shards::execute(store, data).await,
        "PutRecord" => put_record::execute(store, data).await,
        "PutRecords" => put_records::execute(store, data).await,
        "PutResourcePolicy" => put_resource_policy::execute(store, data).await,
        "RegisterStreamConsumer" => register_stream_consumer::execute(store, data).await,
        "RemoveTagsFromStream" => remove_tags_from_stream::execute(store, data).await,
        "SplitShard" => split_shard::execute(store, data).await,
        "StartStreamEncryption" => start_stream_encryption::execute(store, data).await,
        "StopStreamEncryption" => stop_stream_encryption::execute(store, data).await,
        "TagResource" => tag_resource::execute(store, data).await,
        "UntagResource" => untag_resource::execute(store, data).await,
        "UpdateAccountSettings" => update_account_settings::execute(store, data).await,
        "UpdateMaxRecordSize" => update_max_record_size::execute(store, data).await,
        "UpdateShardCount" => update_shard_count::execute(store, data).await,
        "UpdateStreamMode" => update_stream_mode::execute(store, data).await,
        "UpdateStreamWarmThroughput" => update_stream_warm_throughput::execute(store, data).await,
        _ => Err(KinesisErrorResponse::client_error(
            constants::UNKNOWN_OPERATION,
            None,
        )),
    }
}
