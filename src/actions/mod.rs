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

pub use ferrokinesis_core::operation::Operation;

use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

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
