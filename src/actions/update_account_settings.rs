use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let commitment = data
        .get("MinimumThroughputBillingCommitment")
        .ok_or_else(|| {
            KinesisErrorResponse::client_error(
                constants::INVALID_ARGUMENT,
                Some("MinimumThroughputBillingCommitment is required."),
            )
        })?;

    let status = commitment
        .get("Status")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if status.is_empty() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("MinimumThroughputBillingCommitment.Status is required."),
        ));
    }

    // Merge the input into existing settings
    let mut settings = store.get_account_settings().await;
    if let Some(obj) = settings.as_object_mut() {
        for (k, v) in commitment.as_object().unwrap_or(&serde_json::Map::new()) {
            obj.insert(k.clone(), v.clone());
        }
    } else {
        settings = commitment.clone();
    }

    store.put_account_settings(&settings).await;

    Ok(Some(json!({
        "MinimumThroughputBillingCommitment": settings
    })))
}
