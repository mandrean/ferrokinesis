use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(
    _store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    // In a mock server, just echo back the input
    let status = data
        .get("MinimumThroughputBillingCommitment")
        .and_then(|v| v.get("Status"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if status.is_empty() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("MinimumThroughputBillingCommitment.Status is required."),
        ));
    }

    Ok(Some(json!({
        "MinimumThroughputBillingCommitment": {
            "Status": status,
        }
    })))
}
