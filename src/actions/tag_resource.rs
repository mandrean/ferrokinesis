use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let resource_arn = data[constants::RESOURCE_ARN].as_str().unwrap_or("");

    if resource_arn.is_empty() {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("ResourceARN is required."),
        ));
    }

    let tags = data[constants::TAGS]
        .as_object()
        .ok_or_else(|| {
            KinesisErrorResponse::client_error(
                constants::INVALID_ARGUMENT,
                Some("Tags is required."),
            )
        })?
        .clone();

    let invalid_char_re = regex::Regex::new(r"[^\p{L}\d\s_./@=+\-]").unwrap();

    for (key, value) in &tags {
        let val_str = value.as_str().unwrap_or("");
        if invalid_char_re.is_match(key) || invalid_char_re.is_match(val_str) {
            return Err(KinesisErrorResponse::client_error(
                constants::INVALID_ARGUMENT,
                Some(
                    "Some tags contain invalid characters. Valid characters: \
                     Unicode letters, digits, white space, _ . / = + - % @.",
                ),
            ));
        }
    }

    // Check if this is a stream ARN (contains :stream/ but not /consumer/)
    if resource_arn.contains(":stream/") && !resource_arn.contains("/consumer/") {
        if let Some(stream_name) = store.stream_name_from_arn(resource_arn) {
            store
                .update_stream(&stream_name, |stream| {
                    let mut all_keys: std::collections::HashSet<&str> =
                        stream.tags.keys().map(|k| k.as_str()).collect();
                    for key in tags.keys() {
                        all_keys.insert(key.as_str());
                    }
                    if all_keys.len() > 50 {
                        return Err(KinesisErrorResponse::client_error(
                            constants::INVALID_ARGUMENT,
                            Some("A resource cannot have more than 50 tags."),
                        ));
                    }

                    for (key, value) in &tags {
                        if let Some(v) = value.as_str() {
                            stream.tags.insert(key.clone(), v.to_string());
                        }
                    }
                    Ok(())
                })
                .await?;
            return Ok(None);
        }
    }

    // For non-stream resources (consumers, etc.), use the resource tags table
    let mut existing = store.get_resource_tags(resource_arn).await;
    for (key, value) in &tags {
        if let Some(v) = value.as_str() {
            existing.insert(key.clone(), v.to_string());
        }
    }
    if existing.len() > 50 {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("A resource cannot have more than 50 tags."),
        ));
    }
    store.put_resource_tags(resource_arn, &existing).await;

    Ok(None)
}
