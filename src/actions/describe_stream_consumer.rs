use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let consumer_arn = data[constants::CONSUMER_ARN].as_str();
    let stream_arn = data[constants::STREAM_ARN].as_str();
    let consumer_name = data[constants::CONSUMER_NAME].as_str();

    let consumer = if let Some(arn) = consumer_arn {
        store.get_consumer(arn).await
    } else if let (Some(s_arn), Some(c_name)) = (stream_arn, consumer_name) {
        store.find_consumer(s_arn, c_name).await
    } else {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("Must specify either ConsumerARN, or both StreamARN and ConsumerName."),
        ));
    };

    let consumer = consumer.ok_or_else(|| {
        KinesisErrorResponse::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some("Consumer not found."),
        )
    })?;

    tracing::trace!(consumer_arn = %consumer.consumer_arn, consumer_name = %consumer.consumer_name, "consumer described");
    Ok(Some(json!({
        "ConsumerDescription": consumer,
    })))
}
