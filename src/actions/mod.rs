pub mod add_tags_to_stream;
pub mod create_stream;
pub mod decrease_stream_retention_period;
pub mod delete_stream;
pub mod describe_stream;
pub mod describe_stream_summary;
pub mod get_records;
pub mod get_shard_iterator;
pub mod increase_stream_retention_period;
pub mod list_shards;
pub mod list_streams;
pub mod list_tags_for_stream;
pub mod merge_shards;
pub mod put_record;
pub mod put_records;
pub mod remove_tags_from_stream;
pub mod split_shard;

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
        "DeleteStream" => delete_stream::execute(store, data).await,
        "DescribeStream" => describe_stream::execute(store, data).await,
        "DescribeStreamSummary" => describe_stream_summary::execute(store, data).await,
        "GetRecords" => get_records::execute(store, data).await,
        "GetShardIterator" => get_shard_iterator::execute(store, data).await,
        "IncreaseStreamRetentionPeriod" => {
            increase_stream_retention_period::execute(store, data).await
        }
        "ListShards" => list_shards::execute(store, data).await,
        "ListStreams" => list_streams::execute(store, data).await,
        "ListTagsForStream" => list_tags_for_stream::execute(store, data).await,
        "MergeShards" => merge_shards::execute(store, data).await,
        "PutRecord" => put_record::execute(store, data).await,
        "PutRecords" => put_records::execute(store, data).await,
        "RemoveTagsFromStream" => remove_tags_from_stream::execute(store, data).await,
        "SplitShard" => split_shard::execute(store, data).await,
        _ => Err(KinesisErrorResponse::client_error(
            "UnknownOperationException",
            None,
        )),
    }
}
