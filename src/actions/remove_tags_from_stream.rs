use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let tag_keys = data[constants::TAG_KEYS].as_array().ok_or_else(|| {
        KinesisErrorResponse::client_error(constants::SERIALIZATION_EXCEPTION, None)
    })?;

    let keys: Vec<String> = tag_keys
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();

    store
        .update_stream(stream_name, |stream| {
            let invalid_char_re = regex::Regex::new(r"[^\p{L}\d\s_./@=+\-]").unwrap();

            if keys.iter().any(|s| invalid_char_re.is_match(s)) {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(
                        "Some tags contain invalid characters. Valid characters: \
                         Unicode letters, digits, white space, _ . / = + - % @.",
                    ),
                ));
            }

            if keys.iter().any(|s| s.contains('%')) {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(&format!(
                        "Failed to remove tags from stream {} under account {} \
                         because some tags contained illegal characters. The allowed characters are \
                         Unicode letters, white-spaces, '_',',','/','=','+','-','@'.",
                        stream_name, store.aws_account_id
                    )),
                ));
            }

            for key in &keys {
                stream.tags.remove(key.as_str());
            }

            Ok(())
        })
        .await?;

    tracing::trace!(stream = %stream_name, tags = keys.len(), "tags removed");
    Ok(None)
}
