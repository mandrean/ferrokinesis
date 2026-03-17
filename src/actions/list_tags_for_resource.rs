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

    let tags = if resource_arn.contains(":stream/") && !resource_arn.contains("/consumer/") {
        // Stream resource - use stream's built-in tags
        if let Some(stream_name) = store.stream_name_from_arn(resource_arn) {
            match store.get_stream(&stream_name).await {
                Ok(stream) => stream.tags,
                Err(_) => std::collections::BTreeMap::new(),
            }
        } else {
            std::collections::BTreeMap::new()
        }
    } else {
        // Non-stream resource
        store.get_resource_tags(resource_arn).await
    };

    let tags_array: Vec<Value> = tags
        .iter()
        .map(|(k, v)| json!({"Key": k, "Value": v}))
        .collect();

    Ok(Some(json!({
        "Tags": tags_array,
    })))
}
