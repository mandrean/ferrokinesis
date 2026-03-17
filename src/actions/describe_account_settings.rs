use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(
    _store: &Store,
    _data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    Ok(Some(json!({
        "MinimumThroughputBillingCommitment": {}
    })))
}
