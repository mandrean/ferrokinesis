use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use serde_json::{Value, json};

pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse> {
    let limit = data[constants::LIMIT].as_u64().unwrap_or(10) as usize;
    let start_name = data[constants::EXCLUSIVE_START_STREAM_NAME].as_str();

    let all_names = store.list_stream_names().await;
    let mut snapshots = Vec::with_capacity(limit + 1);

    for name in all_names {
        if start_name.is_some_and(|start| name.as_str() <= start) {
            continue;
        }

        if let Ok(stream) = store.get_stream(&name).await {
            let stream_name = stream.stream_name.clone();
            snapshots.push((
                stream_name,
                json!({
                    "StreamARN": stream.stream_arn,
                    "StreamCreationTimestamp": stream.stream_creation_timestamp,
                    "StreamModeDetails": stream.stream_mode_details,
                    "StreamName": stream.stream_name,
                    "StreamStatus": stream.stream_status,
                }),
            ));

            if snapshots.len() > limit {
                break;
            }
        }
    }

    let has_more = snapshots.len() > limit;
    let snapshots: Vec<_> = snapshots.into_iter().take(limit).collect();
    let stream_names: Vec<String> = snapshots.iter().map(|(name, _)| name.clone()).collect();
    let summaries: Vec<Value> = snapshots.into_iter().map(|(_, summary)| summary).collect();

    tracing::trace!(streams = stream_names.len(), has_more, "streams listed");

    Ok(Some(json!({
        "HasMoreStreams": has_more,
        "StreamNames": stream_names,
        "StreamSummaries": summaries,
    })))
}
