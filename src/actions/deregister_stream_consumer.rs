use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::{PendingTransition, Store, TransitionMutation};
use crate::types::ConsumerStatus;
use crate::util::current_time_ms;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let consumer_arn = data[constants::CONSUMER_ARN].as_str();
    let stream_arn = data[constants::STREAM_ARN].as_str();
    let consumer_name = data[constants::CONSUMER_NAME].as_str();

    let resolved_arn = if let Some(arn) = consumer_arn {
        arn.to_string()
    } else if let (Some(s_arn), Some(c_name)) = (stream_arn, consumer_name) {
        let consumer = store.find_consumer(s_arn, c_name).await.ok_or_else(|| {
            KinesisErrorResponse::client_error(
                constants::RESOURCE_NOT_FOUND,
                Some(&format!(
                    "Consumer {} under stream {} not found.",
                    c_name, s_arn
                )),
            )
        })?;
        consumer.consumer_arn
    } else {
        return Err(KinesisErrorResponse::client_error(
            constants::INVALID_ARGUMENT,
            Some("Must specify either ConsumerARN, or both StreamARN and ConsumerName."),
        ));
    };

    let consumer = store.get_consumer(&resolved_arn).await.ok_or_else(|| {
        KinesisErrorResponse::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some(&format!("Consumer {} not found.", resolved_arn)),
        )
    })?;

    // Set to DELETING
    let mut updated = consumer;
    updated.consumer_status = ConsumerStatus::Deleting;
    let transition = PendingTransition::DeregisterConsumer {
        consumer_arn: resolved_arn.clone(),
        ready_at_ms: current_time_ms().saturating_add(500),
    };
    store
        .put_consumer_with_transition(
            &resolved_arn,
            updated,
            TransitionMutation::Upsert(transition.clone()),
        )
        .await?;
    tracing::info!(consumer_arn = %resolved_arn, "consumer deregistered");
    store.schedule_transition(transition);

    Ok(None)
}
