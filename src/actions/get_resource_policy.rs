use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let resource_arn = data[constants::RESOURCE_ARN].as_str().unwrap_or("");

    if resource_arn.is_empty() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("ResourceARN is required."),
        ));
    }

    let policy = store.get_policy(resource_arn).await.unwrap_or_default();

    tracing::trace!(resource_arn, "resource policy retrieved");
    Ok(Some(json!({
        "Policy": policy,
    })))
}
