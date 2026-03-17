use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(
    store: &Store,
    _data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let open_shard_count = store.sum_open_shards().await;

    Ok(Some(json!({
        "ShardLimit": store.options.shard_limit,
        "OpenShardCount": open_shard_count,
        "OnDemandStreamCount": 0,
        "OnDemandStreamCountLimit": 0,
    })))
}
