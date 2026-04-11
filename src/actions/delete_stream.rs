use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::{PendingTransition, Store, TransitionMutation};
use crate::types::StreamStatus;
use crate::util::current_time_ms;
use serde_json::Value;

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let stream_name = data[constants::STREAM_NAME].as_str().unwrap_or("");

    // Verify stream exists and set to DELETING
    let delay = store.options.delete_stream_ms;
    let transition = PendingTransition::DeleteStream {
        stream_name: stream_name.to_string(),
        ready_at_ms: current_time_ms().saturating_add(delay),
    };

    store
        .update_stream_with_transition(
            stream_name,
            TransitionMutation::Upsert(transition.clone()),
            |stream| {
                stream.stream_status = StreamStatus::Deleting;
                Ok(())
            },
        )
        .await?;
    tracing::info!(stream = stream_name, "stream deletion initiated");
    store.schedule_transition(transition);

    Ok(None)
}
