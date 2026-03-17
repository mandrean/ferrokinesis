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

    let tag_keys: Vec<String> = data["TagKeys"]
        .as_array()
        .ok_or_else(|| {
            KinesisErrorResponse::client_error(
                "InvalidArgumentException",
                Some("TagKeys is required."),
            )
        })?
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    // Check if this is a stream ARN
    if resource_arn.contains(":stream/") && !resource_arn.contains("/consumer/") {
        if let Some(stream_name) = store.stream_name_from_arn(resource_arn) {
            store
                .update_stream(&stream_name, |stream| {
                    for key in &tag_keys {
                        stream.tags.remove(key);
                    }
                    Ok(())
                })
                .await?;
            return Ok(None);
        }
    }

    // For non-stream resources
    let mut existing = store.get_resource_tags(resource_arn).await;
    for key in &tag_keys {
        existing.remove(key);
    }
    store.put_resource_tags(resource_arn, &existing).await;

    Ok(None)
}
