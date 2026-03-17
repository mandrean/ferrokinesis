use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let resource_arn = data[constants::RESOURCE_ARN].as_str().unwrap_or("");
    let policy = data[constants::POLICY].as_str().unwrap_or("");

    if resource_arn.is_empty() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("ResourceARN is required."),
        ));
    }

    // Validate the policy is valid JSON
    if serde_json::from_str::<Value>(policy).is_err() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("Policy must be valid JSON."),
        ));
    }

    store.put_policy(resource_arn, policy).await;
    Ok(None)
}
