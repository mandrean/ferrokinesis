use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, _data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let settings = store.get_account_settings().await;
    tracing::trace!("account settings described");
    Ok(Some(json!({
        constants::MINIMUM_THROUGHPUT_BILLING_COMMITMENT: settings
    })))
}
