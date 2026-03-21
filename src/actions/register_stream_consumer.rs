use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use crate::types::{Consumer, ConsumerStatus, EpochSeconds};
use crate::util::current_time_ms;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_arn = data[constants::STREAM_ARN].as_str().unwrap_or("");
    let consumer_name = data[constants::CONSUMER_NAME].as_str().unwrap_or("");

    // Verify stream exists
    let stream_name = store.stream_name_from_arn(stream_arn).unwrap_or_default();
    store.get_stream(&stream_name).await?;

    // Check if consumer already exists
    if let Some(existing) = store.find_consumer(stream_arn, consumer_name).await
        && existing.consumer_status != ConsumerStatus::Deleting
    {
        return Err(KinesisErrorResponse::client_error(
            constants::RESOURCE_IN_USE,
            Some(&format!(
                "Consumer {} under stream {} already exists.",
                consumer_name, stream_arn
            )),
        ));
    }

    // Check consumer limit (20 per stream)
    let consumers = store.list_consumers_for_stream(stream_arn).await;
    let active_count = consumers
        .iter()
        .filter(|c| c.consumer_status != ConsumerStatus::Deleting)
        .count();
    if active_count >= 20 {
        return Err(KinesisErrorResponse::client_error(
            constants::LIMIT_EXCEEDED,
            Some("You have reached the maximum number of registered consumers for this stream."),
        ));
    }

    let now = current_time_ms();
    let creation_ts = EpochSeconds((now / 1000) as f64);
    let consumer_arn = format!("{}/consumer/{}:{}", stream_arn, consumer_name, now / 1000);

    let consumer = Consumer {
        consumer_name: consumer_name.to_string(),
        consumer_arn: consumer_arn.clone(),
        consumer_status: ConsumerStatus::Creating,
        consumer_creation_timestamp: creation_ts,
    };

    store.put_consumer(&consumer_arn, consumer.clone()).await;
    tracing::info!(
        stream = stream_arn,
        consumer = consumer_name,
        "consumer registered"
    );

    // Transition to ACTIVE after a short delay
    let store_clone = store.clone();
    let arn = consumer_arn.clone();
    crate::runtime::spawn_background(async move {
        crate::runtime::sleep_ms(500).await;
        if let Some(mut c) = store_clone.get_consumer(&arn).await {
            c.consumer_status = ConsumerStatus::Active;
            store_clone.put_consumer(&arn, c).await;
        }
    });

    Ok(Some(json!({
        "Consumer": consumer,
    })))
}
