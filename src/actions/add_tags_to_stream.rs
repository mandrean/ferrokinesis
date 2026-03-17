use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::Value;

pub async fn execute(
    store: &Store,
    data: Value,
) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");
    let tags = data[constants::TAGS]
        .as_object()
        .ok_or_else(|| KinesisErrorResponse::client_error(constants::SERIALIZATION_EXCEPTION, None))?
        .clone();

    store
        .update_stream(stream_name, |stream| {
            let keys: Vec<&String> = tags.keys().collect();
            let values: Vec<&str> = tags.values().filter_map(|v| v.as_str()).collect();

            let invalid_char_re = regex::Regex::new(r"[^\p{L}\d\s_./@=+\-]").unwrap();
            let all_strings: Vec<&str> = keys
                .iter()
                .map(|k| k.as_str())
                .chain(values.iter().copied())
                .collect();

            if all_strings.iter().any(|s| invalid_char_re.is_match(s)) {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(
                        "Some tags contain invalid characters. Valid characters: \
                         Unicode letters, digits, white space, _ . / = + - % @.",
                    ),
                ));
            }

            if all_strings.iter().any(|s| s.contains('%')) {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(&format!(
                        "Failed to add tags to stream {} under account {} \
                         because some tags contained illegal characters. The allowed characters are \
                         Unicode letters, white-spaces, '_',',','/','=','+','-','@'.",
                        stream_name, store.aws_account_id
                    )),
                ));
            }

            // Count total unique keys after merge
            let mut all_keys: std::collections::HashSet<&str> =
                stream.tags.keys().map(|k| k.as_str()).collect();
            for key in &keys {
                all_keys.insert(key.as_str());
            }

            if all_keys.len() > 50 {
                return Err(KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some(&format!(
                        "Failed to add tags to stream {} under account {} \
                         because a given stream cannot have more than 10 tags associated with it.",
                        stream_name, store.aws_account_id
                    )),
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

    Ok(None)
}
