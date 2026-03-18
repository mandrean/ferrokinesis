use crate::store::Store;
use crate::types::StreamStatus;

pub async fn run_reaper(store: Store, interval_secs: u64) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
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
            eprintln!("retention: trimmed {deleted} expired record(s) from stream {name}");
        }
    }
}
