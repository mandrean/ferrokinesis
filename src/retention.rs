use crate::store::Store;
use crate::types::StreamStatus;

pub async fn run_reaper(store: Store, interval_secs: u64) {
    let interval_ms = interval_secs.saturating_mul(1000);
    sweep_once(&store).await;
    loop {
        crate::runtime::sleep_ms(interval_ms).await;
        sweep_once(&store).await;
    }
}

pub async fn sweep_once(store: &Store) {
    let stream_names = store.list_stream_names().await;
    for name in stream_names {
        let stream = match store.get_stream(&name).await {
            Ok(s) => s,
            Err(_) => continue,
        };
        if stream.stream_status != StreamStatus::Active {
            continue;
        }
        let deleted = store
            .delete_expired_records(&name, stream.retention_period_hours)
            .await;
        if deleted > 0 {
            tracing::debug!(stream = %name, deleted, "retention: trimmed expired records");
        }
    }
}
