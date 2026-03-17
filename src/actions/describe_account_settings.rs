use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, _data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let settings = store.get_account_settings().await;
    Ok(Some(json!({
        "MinimumThroughputBillingCommitment": settings
    })))
}
