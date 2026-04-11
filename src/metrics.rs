use crate::actions::Operation;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::Mutex;

const ALL_OPERATIONS: &[Operation] = &[
    Operation::AddTagsToStream,
    Operation::CreateStream,
    Operation::DecreaseStreamRetentionPeriod,
    Operation::DeleteResourcePolicy,
    Operation::DeleteStream,
    Operation::DeregisterStreamConsumer,
    Operation::DescribeAccountSettings,
    Operation::DescribeLimits,
    Operation::DescribeStream,
    Operation::DescribeStreamConsumer,
    Operation::DescribeStreamSummary,
    Operation::DisableEnhancedMonitoring,
    Operation::EnableEnhancedMonitoring,
    Operation::GetRecords,
    Operation::GetResourcePolicy,
    Operation::GetShardIterator,
    Operation::IncreaseStreamRetentionPeriod,
    Operation::ListShards,
    Operation::ListStreamConsumers,
    Operation::ListStreams,
    Operation::ListTagsForResource,
    Operation::ListTagsForStream,
    Operation::MergeShards,
    Operation::PutRecord,
    Operation::PutRecords,
    Operation::PutResourcePolicy,
    Operation::RegisterStreamConsumer,
    Operation::RemoveTagsFromStream,
    Operation::SplitShard,
    Operation::StartStreamEncryption,
    Operation::StopStreamEncryption,
    Operation::SubscribeToShard,
    Operation::TagResource,
    Operation::UntagResource,
    Operation::UpdateAccountSettings,
    Operation::UpdateMaxRecordSize,
    Operation::UpdateShardCount,
    Operation::UpdateStreamMode,
    Operation::UpdateStreamWarmThroughput,
];

#[derive(Debug)]
pub struct AppMetrics {
    retained_bytes: AtomicU64,
    retained_records: AtomicU64,
    rejected_writes_total: AtomicU64,
    mirror_dropped_total: AtomicU64,
    replay_complete: AtomicBool,
    last_snapshot_ms: AtomicU64,
    last_request_duration_micros: AtomicU64,
    current_streams: AtomicU64,
    current_shards: AtomicU64,
    request_counters: Vec<OperationMetrics>,
    active_iterators: Mutex<VecDeque<u64>>,
    iterator_ttl_ms: u64,
}

fn operation_name(operation: &Operation) -> &'static str {
    match operation {
        Operation::AddTagsToStream => "AddTagsToStream",
        Operation::CreateStream => "CreateStream",
        Operation::DecreaseStreamRetentionPeriod => "DecreaseStreamRetentionPeriod",
        Operation::DeleteResourcePolicy => "DeleteResourcePolicy",
        Operation::DeleteStream => "DeleteStream",
        Operation::DeregisterStreamConsumer => "DeregisterStreamConsumer",
        Operation::DescribeAccountSettings => "DescribeAccountSettings",
        Operation::DescribeLimits => "DescribeLimits",
        Operation::DescribeStream => "DescribeStream",
        Operation::DescribeStreamConsumer => "DescribeStreamConsumer",
        Operation::DescribeStreamSummary => "DescribeStreamSummary",
        Operation::DisableEnhancedMonitoring => "DisableEnhancedMonitoring",
        Operation::EnableEnhancedMonitoring => "EnableEnhancedMonitoring",
        Operation::GetRecords => "GetRecords",
        Operation::GetResourcePolicy => "GetResourcePolicy",
        Operation::GetShardIterator => "GetShardIterator",
        Operation::IncreaseStreamRetentionPeriod => "IncreaseStreamRetentionPeriod",
        Operation::ListShards => "ListShards",
        Operation::ListStreamConsumers => "ListStreamConsumers",
        Operation::ListStreams => "ListStreams",
        Operation::ListTagsForResource => "ListTagsForResource",
        Operation::ListTagsForStream => "ListTagsForStream",
        Operation::MergeShards => "MergeShards",
        Operation::PutRecord => "PutRecord",
        Operation::PutRecords => "PutRecords",
        Operation::PutResourcePolicy => "PutResourcePolicy",
        Operation::RegisterStreamConsumer => "RegisterStreamConsumer",
        Operation::RemoveTagsFromStream => "RemoveTagsFromStream",
        Operation::SplitShard => "SplitShard",
        Operation::StartStreamEncryption => "StartStreamEncryption",
        Operation::StopStreamEncryption => "StopStreamEncryption",
        Operation::SubscribeToShard => "SubscribeToShard",
        Operation::TagResource => "TagResource",
        Operation::UntagResource => "UntagResource",
        Operation::UpdateAccountSettings => "UpdateAccountSettings",
        Operation::UpdateMaxRecordSize => "UpdateMaxRecordSize",
        Operation::UpdateShardCount => "UpdateShardCount",
        Operation::UpdateStreamMode => "UpdateStreamMode",
        Operation::UpdateStreamWarmThroughput => "UpdateStreamWarmThroughput",
    }
}

#[derive(Debug)]
struct OperationMetrics {
    ok_total: AtomicU64,
    error_total: AtomicU64,
    duration_count: AtomicU64,
    duration_micros_sum: AtomicU64,
}

impl Default for OperationMetrics {
    fn default() -> Self {
        Self {
            ok_total: AtomicU64::new(0),
            error_total: AtomicU64::new(0),
            duration_count: AtomicU64::new(0),
            duration_micros_sum: AtomicU64::new(0),
        }
    }
}

impl AppMetrics {
    pub fn new(iterator_ttl_seconds: u64) -> Arc<Self> {
        let request_counters = std::iter::repeat_with(OperationMetrics::default)
            .take(39)
            .collect();
        Arc::new(Self {
            retained_bytes: AtomicU64::new(0),
            retained_records: AtomicU64::new(0),
            rejected_writes_total: AtomicU64::new(0),
            mirror_dropped_total: AtomicU64::new(0),
            replay_complete: AtomicBool::new(true),
            last_snapshot_ms: AtomicU64::new(0),
            last_request_duration_micros: AtomicU64::new(0),
            current_streams: AtomicU64::new(0),
            current_shards: AtomicU64::new(0),
            request_counters,
            active_iterators: Mutex::new(VecDeque::new()),
            iterator_ttl_ms: iterator_ttl_seconds.saturating_mul(1000),
        })
    }

    pub fn set_retained(&self, bytes: u64, records: u64) {
        self.retained_bytes.store(bytes, Ordering::Relaxed);
        self.retained_records.store(records, Ordering::Relaxed);
    }

    pub fn add_retained(&self, bytes: u64, records: u64) {
        self.retained_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.retained_records.fetch_add(records, Ordering::Relaxed);
    }

    pub fn remove_retained(&self, bytes: u64, records: u64) {
        self.retained_bytes.fetch_sub(bytes, Ordering::Relaxed);
        self.retained_records.fetch_sub(records, Ordering::Relaxed);
    }

    pub fn retained_bytes(&self) -> u64 {
        self.retained_bytes.load(Ordering::Relaxed)
    }

    pub fn retained_records(&self) -> u64 {
        self.retained_records.load(Ordering::Relaxed)
    }

    pub fn increment_rejected_writes(&self) {
        self.rejected_writes_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_mirror_dropped(&self) {
        self.mirror_dropped_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_replay_complete(&self, value: bool) {
        self.replay_complete.store(value, Ordering::Relaxed);
    }

    pub fn set_last_snapshot_ms(&self, value: u64) {
        self.last_snapshot_ms.store(value, Ordering::Relaxed);
    }

    pub fn set_topology(&self, streams: u64, shards: u64) {
        self.current_streams.store(streams, Ordering::Relaxed);
        self.current_shards.store(shards, Ordering::Relaxed);
    }

    pub fn record_request(&self, operation: Operation, ok: bool, duration_micros: u64) {
        let op = &self.request_counters[operation as usize];
        if ok {
            op.ok_total.fetch_add(1, Ordering::Relaxed);
        } else {
            op.error_total.fetch_add(1, Ordering::Relaxed);
        }
        op.duration_count.fetch_add(1, Ordering::Relaxed);
        op.duration_micros_sum
            .fetch_add(duration_micros, Ordering::Relaxed);
        self.last_request_duration_micros
            .store(duration_micros, Ordering::Relaxed);
    }

    pub async fn record_iterator(&self, now_ms: u64) {
        let mut active = self.active_iterators.lock().await;
        active.push_back(now_ms);
        self.prune_iterators_locked(&mut active, now_ms);
    }

    pub async fn active_iterator_count(&self, now_ms: u64) -> usize {
        let mut active = self.active_iterators.lock().await;
        self.prune_iterators_locked(&mut active, now_ms);
        active.len()
    }

    pub async fn render(&self, now_ms: u64) -> String {
        let active_iterators = self.active_iterator_count(now_ms).await;
        let mut out = String::new();
        out.push_str("# TYPE ferrokinesis_retained_bytes gauge\n");
        out.push_str(&format!(
            "ferrokinesis_retained_bytes {}\n",
            self.retained_bytes()
        ));
        out.push_str("# TYPE ferrokinesis_retained_records gauge\n");
        out.push_str(&format!(
            "ferrokinesis_retained_records {}\n",
            self.retained_records()
        ));
        out.push_str("# TYPE ferrokinesis_rejected_writes_total counter\n");
        out.push_str(&format!(
            "ferrokinesis_rejected_writes_total {}\n",
            self.rejected_writes_total.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE ferrokinesis_mirror_dropped_total counter\n");
        out.push_str(&format!(
            "ferrokinesis_mirror_dropped_total {}\n",
            self.mirror_dropped_total.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE ferrokinesis_replay_complete gauge\n");
        out.push_str(&format!(
            "ferrokinesis_replay_complete {}\n",
            u8::from(self.replay_complete.load(Ordering::Relaxed))
        ));
        out.push_str("# TYPE ferrokinesis_last_snapshot_timestamp_ms gauge\n");
        out.push_str(&format!(
            "ferrokinesis_last_snapshot_timestamp_ms {}\n",
            self.last_snapshot_ms.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE ferrokinesis_streams gauge\n");
        out.push_str(&format!(
            "ferrokinesis_streams {}\n",
            self.current_streams.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE ferrokinesis_open_shards gauge\n");
        out.push_str(&format!(
            "ferrokinesis_open_shards {}\n",
            self.current_shards.load(Ordering::Relaxed)
        ));
        out.push_str("# TYPE ferrokinesis_active_iterators gauge\n");
        out.push_str(&format!(
            "ferrokinesis_active_iterators {}\n",
            active_iterators
        ));

        for (index, operation) in ALL_OPERATIONS.iter().enumerate() {
            let metrics = &self.request_counters[index];
            let op = operation_name(operation);
            out.push_str(&format!(
                "ferrokinesis_requests_total{{operation=\"{op}\",result=\"ok\"}} {}\n",
                metrics.ok_total.load(Ordering::Relaxed)
            ));
            out.push_str(&format!(
                "ferrokinesis_requests_total{{operation=\"{op}\",result=\"error\"}} {}\n",
                metrics.error_total.load(Ordering::Relaxed)
            ));
            out.push_str(&format!(
                "ferrokinesis_request_duration_seconds_count{{operation=\"{op}\"}} {}\n",
                metrics.duration_count.load(Ordering::Relaxed)
            ));
            out.push_str(&format!(
                "ferrokinesis_request_duration_seconds_sum{{operation=\"{op}\"}} {}\n",
                metrics.duration_micros_sum.load(Ordering::Relaxed) as f64 / 1_000_000.0
            ));
        }

        out
    }

    fn prune_iterators_locked(&self, active: &mut VecDeque<u64>, now_ms: u64) {
        while let Some(created_at) = active.front().copied() {
            if now_ms.saturating_sub(created_at) <= self.iterator_ttl_ms {
                break;
            }
            active.pop_front();
        }
    }
}
