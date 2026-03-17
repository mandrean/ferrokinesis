use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let resource_arn = data["ResourceARN"].as_str().unwrap_or("");

    if resource_arn.is_empty() {
        return Err(KinesisErrorResponse::client_error(
            "InvalidArgumentException",
            Some("ResourceARN is required."),
        ));
    }

    store.delete_policy(resource_arn).await;
    Ok(None)
}
