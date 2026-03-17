use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

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

    let policy = store.get_policy(resource_arn).await.unwrap_or_default();

    Ok(Some(json!({
        "Policy": policy,
    })))
}
