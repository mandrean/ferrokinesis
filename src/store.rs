//! Concurrent in-memory store for streams, records, consumers, and policies.
//!
//! [`Store`] is the central state object threaded through all action handlers.
//! It is cheap to clone — all clones share the same underlying [`Arc`]-wrapped
//! concurrent data structures.
//!
//! Concurrency model:
//! - **Cross-stream**: [`DashMap`] provides near-lock-free concurrent access
//!   to different streams.
//! - **Per-stream**: [`tokio::sync::RwLock`] on stream metadata allows concurrent
//!   readers with exclusive writers.
//! - **Per-shard records**: Each shard has its own [`RwLock`]`<`[`BTreeMap`]`>`,
//!   so reads/writes to different shards never contend.
//!
//! [`StoreOptions`] controls runtime behaviour such as simulated delays and
//! account identity. Pass it to [`crate::create_app`] to wire everything up.

use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::metrics::AppMetrics;
#[cfg(not(target_arch = "wasm32"))]
use crate::persistence::{
    Persistence, PersistentSnapshot, PersistentStream, SnapshotShardRecords, WalEntry,
};
use crate::sequence;
use crate::types::{Consumer, StoredRecord, Stream, StreamStatus};
use crate::util::current_time_ms;
use dashmap::DashMap;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock};

const DEFAULT_SHARD_WRITE_BYTES_PER_SEC: u64 = 1024 * 1024;
const DEFAULT_SHARD_WRITE_RECORDS_PER_SEC: u64 = 1000;

/// Errors that can occur when probing store health.
#[derive(Debug, thiserror::Error)]
pub enum StoreHealthError {
    /// A read transaction could not be started.
    #[error("db read failed: {0}")]
    ReadFailed(String),
    /// A required table could not be opened within the read transaction.
    #[error("table open failed: {0}")]
    TableOpenFailed(String),
}

#[derive(Default)]
struct StoreHealthState {
    replay_complete: AtomicBool,
    durable_ok: AtomicBool,
    last_error: StdRwLock<Option<String>>,
}

#[cfg(not(target_arch = "wasm32"))]
struct PersistenceState {
    backend: Persistence,
    io_lock: Mutex<()>,
    snapshot_interval_secs: u64,
    last_snapshot_ms: AtomicU64,
}

/// Runtime configuration passed to [`crate::create_app`].
///
/// All fields have sensible defaults via [`StoreOptions::default`].
///
/// # Examples
///
/// ```rust
/// use ferrokinesis::store::StoreOptions;
///
/// let opts = StoreOptions {
///     shard_limit: 50,
///     aws_region: "eu-west-1".to_string(),
///     ..StoreOptions::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct StoreOptions {
    /// Simulated delay for `CreateStream` in milliseconds. Defaults to `500`.
    pub create_stream_ms: u64,
    /// Simulated delay for `DeleteStream` in milliseconds. Defaults to `500`.
    pub delete_stream_ms: u64,
    /// Simulated delay for stream-update operations in milliseconds. Defaults to `500`.
    pub update_stream_ms: u64,
    /// Maximum total number of open shards across all streams. Defaults to `10`.
    pub shard_limit: u32,
    /// Shard iterator TTL in seconds. Iterators older than this are expired. Defaults to `300`.
    pub iterator_ttl_seconds: u64,
    /// Background retention-reaper check interval in seconds.
    /// Set to `0` (default) to disable the reaper.
    pub retention_check_interval_secs: u64,
    /// Enable AWS-like shard write throughput throttling. Disabled by default.
    pub enforce_limits: bool,
    /// Directory used to persist state with WAL + snapshots.
    pub state_dir: Option<PathBuf>,
    /// Snapshot interval in seconds when durable mode is enabled.
    pub snapshot_interval_secs: u64,
    /// Hard cap on retained serialized record bytes.
    pub max_retained_bytes: Option<u64>,
    /// Simulated AWS account ID (12 digits). Defaults to `"000000000000"`.
    pub aws_account_id: String,
    /// Simulated AWS region. Defaults to `"us-east-1"`.
    pub aws_region: String,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            create_stream_ms: 500,
            delete_stream_ms: 500,
            update_stream_ms: 500,
            shard_limit: 10,
            iterator_ttl_seconds: 300,
            retention_check_interval_secs: 0,
            enforce_limits: false,
            state_dir: None,
            snapshot_interval_secs: 30,
            max_retained_bytes: None,
            aws_account_id: "000000000000".to_string(),
            aws_region: "us-east-1".to_string(),
        }
    }
}

/// Per-shard sequence counter for lock-free sequence number generation.
struct ShardSeqState {
    /// Atomic counter — `fetch_add(1)` returns the next seq_ix to use.
    counter: AtomicU64,
}

/// Per-stream entry holding metadata and per-shard sequence state.
///
/// Lock ordering (to prevent deadlock): `stream` before `shard_seq` before record maps.
struct StreamEntry {
    /// Stream metadata. Read-locked on the hot path; write-locked for admin ops.
    stream: RwLock<Stream>,
    /// Per-shard sequence counters, indexed by shard position.
    /// Read-locked on the hot path (AtomicU64 inside is lock-free).
    /// Write-locked only when topology changes (split/merge adds shards).
    shard_seq: RwLock<Vec<ShardSeqState>>,
}

/// Result of allocating a sequence number for a record.
pub struct SequenceAllocation {
    /// The shard ID the record was routed to.
    pub shard_id: String,
    /// The generated sequence number string.
    pub seq_num: String,
    /// Composite key `"{shard_hex}/{seq_num}"` for record storage.
    pub stream_key: String,
    /// Timestamp used in the sequence number (milliseconds).
    pub now: u64,
}

/// Per-shard record map: shard_hex → sorted records.
type ShardRecords = DashMap<String, Arc<RwLock<BTreeMap<String, Vec<u8>>>>>;

struct ShardThroughputWindow {
    window_start_ms: u64,
    bytes: u64,
    records: u64,
}

/// Shared inner state behind an [`Arc`] for cheap [`Store`] clones.
struct StoreInner {
    /// Stream metadata entries.
    streams: DashMap<String, Arc<StreamEntry>>,
    /// Per-stream, per-shard records. Outer key: stream name.
    /// Decoupled from streams so records can exist independently.
    stream_records: DashMap<String, ShardRecords>,
    consumers: DashMap<String, Vec<u8>>,
    policies: DashMap<String, String>,
    resource_tags: DashMap<String, BTreeMap<String, String>>,
    account_settings: RwLock<Value>,
    throughput_windows: DashMap<String, Arc<Mutex<ShardThroughputWindow>>>,
}

/// Handle to the concurrent in-memory stream store.
///
/// Cheap to clone — all clones share the same underlying `Arc<StoreInner>`.
/// Inject this into Axum handlers via `State<Store>`.
#[derive(Clone)]
pub struct Store {
    /// Runtime configuration used by action handlers.
    pub options: StoreOptions,
    /// The effective AWS account ID (digits only, length-validated at construction).
    pub aws_account_id: String,
    /// The simulated AWS region.
    pub aws_region: String,
    inner: Arc<StoreInner>,
    metrics: Arc<AppMetrics>,
    health: Arc<StoreHealthState>,
    #[cfg(not(target_arch = "wasm32"))]
    persistence: Option<Arc<PersistenceState>>,
    /// Optional capture writer for recording PutRecord/PutRecords calls.
    #[cfg(feature = "server")]
    pub(crate) capture_writer: Option<crate::capture::CaptureWriter>,
}

/// Extract the shard hex prefix from a shard key like `"{shard_hex}/{seq_num}"`.
fn shard_hex_from_key(key: &str) -> &str {
    key.split('/').next().unwrap_or("")
}

/// Get or create the per-shard record map for a stream.
fn ensure_shard_map<'a>(
    stream_records: &'a DashMap<String, ShardRecords>,
    stream_name: &str,
) -> dashmap::mapref::one::Ref<'a, String, ShardRecords> {
    stream_records
        .entry(stream_name.to_string())
        .or_default()
        .downgrade()
}

impl Store {
    /// Creates a new store.
    ///
    /// Strips non-digit characters from `options.aws_account_id` and warns if
    /// the result is not exactly 12 digits.
    pub fn new(options: StoreOptions) -> Self {
        #[cfg(feature = "server")]
        {
            Self::build(options, None)
        }

        #[cfg(not(feature = "server"))]
        {
            Self::build(options)
        }
    }

    #[cfg(feature = "server")]
    /// Creates a new store with an optional [`crate::capture::CaptureWriter`]
    /// to record PutRecord/PutRecords calls to an NDJSON file.
    ///
    /// Strips non-digit characters from `options.aws_account_id` and warns if
    /// the result is not exactly 12 digits.
    pub fn with_capture(
        options: StoreOptions,
        capture_writer: Option<crate::capture::CaptureWriter>,
    ) -> Self {
        Self::build(options, capture_writer)
    }

    fn build(
        options: StoreOptions,
        #[cfg(feature = "server")] capture_writer: Option<crate::capture::CaptureWriter>,
    ) -> Self {
        let aws_account_id: String = options
            .aws_account_id
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect();

        if aws_account_id.len() != 12 {
            tracing::warn!(
                "AWS account ID has {} digits after stripping non-digits (expected 12)",
                aws_account_id.len()
            );
        }

        let aws_region = options.aws_region.clone();
        let metrics = AppMetrics::new(options.iterator_ttl_seconds);
        let health = Arc::new(StoreHealthState {
            replay_complete: AtomicBool::new(true),
            durable_ok: AtomicBool::new(true),
            last_error: StdRwLock::new(None),
        });

        let inner = Arc::new(StoreInner {
            streams: DashMap::new(),
            stream_records: DashMap::new(),
            consumers: DashMap::new(),
            policies: DashMap::new(),
            resource_tags: DashMap::new(),
            account_settings: RwLock::new(Value::Object(Default::default())),
        });

        let store = Self {
            options,
            aws_account_id,
            aws_region,
            inner,
            metrics,
            health,
            #[cfg(not(target_arch = "wasm32"))]
            persistence: None,
            #[cfg(feature = "server")]
            capture_writer,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let mut store = store;

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(state_dir) = store.options.state_dir.clone() {
            match Persistence::new(state_dir) {
                Ok(backend) => {
                    let persistence = Arc::new(PersistenceState {
                        backend,
                        io_lock: Mutex::new(()),
                        snapshot_interval_secs: store.options.snapshot_interval_secs,
                        last_snapshot_ms: AtomicU64::new(0),
                    });
                    if let Err(err) = store.load_persistent_state(&persistence) {
                        store.mark_unhealthy(err);
                    }
                    store.persistence = Some(persistence);
                }
                Err(err) => {
                    store.mark_unhealthy(format!("failed to initialize durable state: {err}"))
                }
            }
        }

        store.refresh_topology_metrics_sync();
        store
    }

    /// Returns the shared application metrics registry.
    pub fn metrics(&self) -> Arc<AppMetrics> {
        Arc::clone(&self.metrics)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn load_persistent_state(&mut self, persistence: &Arc<PersistenceState>) -> Result<(), String> {
        let Some((snapshot, entries)) =
            persistence.backend.load().map_err(|err| err.to_string())?
        else {
            return Ok(());
        };

        persistence
            .last_snapshot_ms
            .store(snapshot.created_at_ms, Ordering::Relaxed);
        self.metrics.set_last_snapshot_ms(snapshot.created_at_ms);
        self.restore_snapshot(snapshot)?;
        self.metrics.set_replay_complete(false);
        self.health.replay_complete.store(false, Ordering::Relaxed);

        for entry in entries {
            self.apply_wal_entry(entry)?;
        }

        self.metrics.set_replay_complete(true);
        self.health.replay_complete.store(true, Ordering::Relaxed);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn apply_wal_entry(&self, entry: WalEntry) -> Result<(), String> {
        match entry {
            WalEntry::Snapshot(snapshot) => self.restore_snapshot(snapshot),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn restore_snapshot(&self, snapshot: PersistentSnapshot) -> Result<(), String> {
        self.inner.streams.clear();
        self.inner.stream_records.clear();
        self.inner.consumers.clear();
        self.inner.policies.clear();
        self.inner.resource_tags.clear();
        if let Ok(mut settings) = self.inner.account_settings.try_write() {
            *settings = if snapshot.account_settings_json.is_empty() {
                Value::Object(Default::default())
            } else {
                serde_json::from_slice(&snapshot.account_settings_json)
                    .map_err(|err| format!("failed to decode persisted account settings: {err}"))?
            };
        }

        for stream in snapshot.streams {
            let (name, stream_value, shard_counters) = stream.into_parts();
            let entry = Arc::new(StreamEntry {
                stream: RwLock::new(stream_value),
                shard_seq: RwLock::new(
                    shard_counters
                        .into_iter()
                        .map(|counter| ShardSeqState {
                            counter: AtomicU64::new(counter),
                        })
                        .collect(),
                ),
            });
            self.inner.streams.insert(name, entry);
        }

        for shard_records in snapshot.records {
            let shard_map = self
                .inner
                .stream_records
                .entry(shard_records.stream_name)
                .or_default();
            let records = shard_records.records.into_iter().collect();
            shard_map.insert(shard_records.shard_hex, Arc::new(RwLock::new(records)));
        }

        for (consumer_arn, bytes) in snapshot.consumers {
            self.inner.consumers.insert(consumer_arn, bytes);
        }
        for (resource_arn, policy) in snapshot.policies {
            self.inner.policies.insert(resource_arn, policy);
        }
        for (resource_arn, tags) in snapshot.resource_tags {
            self.inner.resource_tags.insert(resource_arn, tags);
        }

        self.metrics
            .set_retained(snapshot.retained_bytes, snapshot.retained_records);
        self.refresh_topology_metrics_sync();
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn export_snapshot(&self) -> Result<PersistentSnapshot, String> {
        let mut streams = Vec::new();
        for entry in self.inner.streams.iter() {
            let stream = entry.value().stream.read().await.clone();
            let shard_seq = entry
                .value()
                .shard_seq
                .read()
                .await
                .iter()
                .map(|state| state.counter.load(Ordering::Relaxed))
                .collect();
            streams.push(PersistentStream::from_stream(
                entry.key().clone(),
                &stream,
                shard_seq,
            ));
        }

        let mut records = Vec::new();
        for stream_entry in self.inner.stream_records.iter() {
            for shard_entry in stream_entry.value().iter() {
                let shard_records_map = shard_entry.value().read().await.clone();
                let shard_records = shard_records_map.into_iter().collect();
                records.push(SnapshotShardRecords {
                    stream_name: stream_entry.key().clone(),
                    shard_hex: shard_entry.key().clone(),
                    records: shard_records,
                });
            }
        }

        Ok(PersistentSnapshot {
            created_at_ms: current_time_ms(),
            streams,
            records,
            consumers: self
                .inner
                .consumers
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect(),
            policies: self
                .inner
                .policies
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect(),
            resource_tags: self
                .inner
                .resource_tags
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect(),
            account_settings_json: {
                let account_settings = self.inner.account_settings.read().await.clone();
                serde_json::to_vec(&account_settings)
                    .map_err(|err| format!("failed to encode snapshot account settings: {err}"))?
            },
            retained_bytes: self.metrics.retained_bytes(),
            retained_records: self.metrics.retained_records(),
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn persist_snapshot(&self, snapshot: &PersistentSnapshot) {
        let Some(persistence) = &self.persistence else {
            return;
        };
        let backend = persistence.backend.clone();
        let snapshot = snapshot.clone();
        if let Err(err) = tokio::task::spawn_blocking(move || {
            backend.append_wal_entry(&WalEntry::Snapshot(snapshot))
        })
        .await
        .map_err(|err| err.to_string())
        .and_then(|res| res.map_err(|err| err.to_string()))
        {
            self.mark_unhealthy(format!("failed to persist wal entry: {err}"));
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn persist_current_state(&self) {
        if self.persistence.is_none() {
            return;
        }
        let Some(persistence) = &self.persistence else {
            return;
        };
        let _guard = persistence.io_lock.lock().await;
        let snapshot = match self.export_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                self.mark_unhealthy(err.clone());
                tracing::warn!("{err}");
                return;
            }
        };

        self.persist_snapshot(&snapshot).await;
        if !self.health.durable_ok.load(Ordering::Relaxed) {
            return;
        }

        if persistence.snapshot_interval_secs == 0 {
            return;
        }

        let now = snapshot.created_at_ms;
        let last_snapshot = persistence.last_snapshot_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last_snapshot)
            < persistence.snapshot_interval_secs.saturating_mul(1000)
        {
            return;
        }

        let backend = persistence.backend.clone();
        match tokio::task::spawn_blocking(move || backend.write_snapshot(&snapshot)).await {
            Ok(Ok(())) => {
                persistence.last_snapshot_ms.store(now, Ordering::Relaxed);
                self.metrics.set_last_snapshot_ms(now);
            }
            Ok(Err(err)) => {
                let message = format!("failed to write snapshot: {err}");
                self.mark_unhealthy(message.clone());
                tracing::warn!("{message}");
            }
            Err(err) => {
                let message = format!("snapshot task failed: {err}");
                self.mark_unhealthy(message.clone());
                tracing::warn!("{message}");
            }
        }
    }

    #[cfg(target_arch = "wasm32")]
    async fn persist_current_state(&self) {}

    #[cfg(not(target_arch = "wasm32"))]
    fn mark_unhealthy(&self, message: String) {
        self.health.durable_ok.store(false, Ordering::Relaxed);
        if let Ok(mut guard) = self.health.last_error.write() {
            *guard = Some(message);
        }
        self.health.replay_complete.store(false, Ordering::Relaxed);
        self.metrics.set_replay_complete(false);
    }

    fn refresh_topology_metrics_sync(&self) {
        let streams = self.inner.streams.len() as u64;
        let mut open_shards = 0u64;
        for entry in self.inner.streams.iter() {
            if let Ok(stream) = entry.value().stream.try_read() {
                open_shards += stream
                    .shards
                    .iter()
                    .filter(|s| s.sequence_number_range.ending_sequence_number.is_none())
                    .count() as u64;
            }
        }
        self.metrics.set_topology(streams, open_shards);
    }

    async fn refresh_topology_metrics(&self) {
        let streams = self.inner.streams.len() as u64;
        let mut open_shards = 0u64;
        for entry in self.inner.streams.iter() {
            let stream = entry.value().stream.read().await;
            open_shards += stream
                .shards
                .iter()
                .filter(|s| s.sequence_number_range.ending_sequence_number.is_none())
                .count() as u64;
        }
        self.metrics.set_topology(streams, open_shards);
    }

    /// Renders the current metrics registry in Prometheus text format.
    pub async fn render_metrics(&self) -> String {
        self.metrics.render(current_time_ms()).await
    }

    /// Records the creation time for a new shard iterator.
    pub async fn record_iterator_created(&self, now_ms: u64) {
        self.metrics.record_iterator(now_ms).await;
    }

    fn retained_capacity_exceeded(&self) -> bool {
        self.options
            .max_retained_bytes
            .is_some_and(|limit| self.metrics.retained_bytes() > limit)
    }

    // --- Stream operations ---

    /// Retrieves a stream by name.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisErrorResponse`] (`ResourceNotFoundException`) if the stream does not exist.
    pub async fn get_stream(&self, name: &str) -> Result<Stream, KinesisErrorResponse> {
        let entry = self
            .inner
            .streams
            .get(name)
            .map(|e| e.value().clone())
            .ok_or_else(|| KinesisErrorResponse::stream_not_found(name, &self.aws_account_id))?;
        Ok(entry.stream.read().await.clone())
    }

    /// Inserts or replaces a stream by name.
    pub async fn put_stream(&self, name: &str, stream: Stream) {
        if let Some(existing) = self.inner.streams.get(name) {
            // Update metadata, preserve existing records and seq state.
            let mut guard = existing.stream.write().await;
            *guard = stream;
        } else {
            let shard_seq = build_shard_seq(&stream);
            let entry = Arc::new(StreamEntry {
                stream: RwLock::new(stream),
                shard_seq: RwLock::new(shard_seq),
            });
            self.inner.streams.insert(name.to_string(), entry);
        }
        self.refresh_topology_metrics().await;
        self.persist_current_state().await;
    }

    /// Deletes a stream and all of its records.
    pub async fn delete_stream(&self, name: &str) {
        if let Some(records) = self.inner.stream_records.get(name) {
            let mut bytes = 0u64;
            let mut count = 0u64;
            for shard in records.iter() {
                if let Ok(shard_records) = shard.value().try_read() {
                    count += shard_records.len() as u64;
                    bytes += shard_records
                        .values()
                        .map(|value| value.len() as u64)
                        .sum::<u64>();
                }
            }
            self.metrics.remove_retained(bytes, count);
        }
        self.inner.streams.remove(name);
        self.inner.stream_records.remove(name);
        self.clear_throughput_windows_for_stream(name);
        self.refresh_topology_metrics().await;
        self.persist_current_state().await;
        self.refresh_topology_metrics().await;
        self.persist_current_state().await;
    }

    /// Returns `true` if a stream with the given name exists.
    pub async fn contains_stream(&self, name: &str) -> bool {
        self.inner.streams.contains_key(name)
    }

    /// Returns all stream names in sorted order.
    pub async fn list_stream_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.inner.streams.iter().map(|e| e.key().clone()).collect();
        names.sort();
        names
    }

    /// Atomically reads, mutates, and writes a stream (read-modify-write).
    ///
    /// The closure `f` receives a mutable reference to the stream and must
    /// return `Ok(R)` to commit the change, or `Err(e)` to abort.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisErrorResponse`] (`ResourceNotFoundException`) if the stream
    /// does not exist, or any error returned by the closure `f`.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use ferrokinesis::store::Store;
    /// # async fn example(store: &Store) -> Result<(), ferrokinesis::error::KinesisErrorResponse> {
    /// store.update_stream("my-stream", |stream| {
    ///     stream.retention_period_hours = 48;
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_stream<F, R>(&self, name: &str, f: F) -> Result<R, KinesisErrorResponse>
    where
        F: FnOnce(&mut Stream) -> Result<R, KinesisErrorResponse>,
    {
        let entry = self
            .inner
            .streams
            .get(name)
            .map(|e| e.value().clone())
            .ok_or_else(|| KinesisErrorResponse::stream_not_found(name, &self.aws_account_id))?;
        let mut stream = entry.stream.write().await;
        let result = f(&mut stream)?;
        // Sync shard_seq if the closure added shards (split/merge/reshard).
        sync_shard_seq(&entry, &stream).await;
        drop(stream);
        self.refresh_topology_metrics().await;
        self.persist_current_state().await;
        Ok(result)
    }

    /// Returns the total number of open (non-closed) shards across all streams.
    pub async fn sum_open_shards(&self) -> u32 {
        let mut sum = 0u32;
        for entry in self.inner.streams.iter() {
            let stream = entry.value().stream.read().await;
            sum += stream
                .shards
                .iter()
                .filter(|s| s.sequence_number_range.ending_sequence_number.is_none())
                .count() as u32;
        }
        sum
    }

    // --- Record operations ---

    /// Returns all records for a stream as a map from shard-key to record.
    ///
    /// The map key is `"{shard_hex}/{seq_num}"` (the composite key with the
    /// stream-name prefix stripped).
    ///
    /// Used by integration tests only — production code uses
    /// [`get_records_range_limited`](Self::get_records_range_limited) or
    /// [`find_first_record_at_timestamp`](Self::find_first_record_at_timestamp).
    pub async fn get_record_store(&self, stream_name: &str) -> BTreeMap<String, StoredRecord> {
        let shard_map = match self.inner.stream_records.get(stream_name) {
            Some(m) => m,
            None => return BTreeMap::new(),
        };
        let mut all_records = BTreeMap::new();
        for shard_entry in shard_map.iter() {
            let records = shard_entry.value().read().await;
            for (k, v) in records.iter() {
                let record: StoredRecord = postcard::from_bytes(v).unwrap();
                all_records.insert(k.clone(), record);
            }
        }
        all_records
    }

    /// Stores a single record under the given composite shard key.
    pub async fn put_record<R: Serialize>(
        &self,
        stream_name: &str,
        key: &str,
        record: &R,
    ) -> Result<(), KinesisErrorResponse> {
        let bytes = postcard::to_allocvec(record).map_err(|err| {
            let message = err.to_string();
            KinesisErrorResponse::server_error(None, Some(&message))
        })?;
        let shard_hex = shard_hex_from_key(key);
        let shard_map = ensure_shard_map(&self.inner.stream_records, stream_name);
        let records_arc = shard_map
            .entry(shard_hex.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())))
            .value()
            .clone();
        drop(shard_map);
        let mut records = records_arc.write().await;
        if let Some(previous) = records.insert(key.to_string(), bytes.clone()) {
            self.metrics.remove_retained(previous.len() as u64, 1);
        }
        self.metrics.add_retained(bytes.len() as u64, 1);
        drop(records);
        self.persist_current_state().await;
        Ok(())
    }

    /// Stores multiple records in a single batch.
    pub async fn put_records_batch<R: Serialize>(
        &self,
        stream_name: &str,
        batch: &[(String, R)],
    ) -> Result<(), KinesisErrorResponse> {
        // Phase 1: collect Arc refs and serialized bytes while holding the DashMap ref.
        let pending: Result<Vec<_>, KinesisErrorResponse> = {
            let shard_map = ensure_shard_map(&self.inner.stream_records, stream_name);
            batch
                .iter()
                .map(|(key, record)| {
                    let bytes = postcard::to_allocvec(record).map_err(|err| {
                        let message = err.to_string();
                        KinesisErrorResponse::server_error(None, Some(&message))
                    })?;
                    let shard_hex = shard_hex_from_key(key);
                    let records_arc = shard_map
                        .entry(shard_hex.to_string())
                        .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())))
                        .value()
                        .clone();
                    Ok((records_arc, key.clone(), bytes))
                })
                .collect()
        }; // shard_map Ref dropped here — no DashMap lock held across await.
        let pending = pending?;

        // Phase 2: insert records under per-shard write locks only.
        for (records_arc, key, bytes) in pending {
            let mut records = records_arc.write().await;
            if let Some(previous) = records.insert(key, bytes.clone()) {
                self.metrics.remove_retained(previous.len() as u64, 1);
            }
            self.metrics.add_retained(bytes.len() as u64, 1);
        }
        self.persist_current_state().await;
        Ok(())
    }

    /// Deletes records whose sequence-number–embedded timestamp is older than
    /// the retention cutoff. Returns the number of records removed.
    pub async fn delete_expired_records(&self, stream_name: &str, retention_hours: u32) -> usize {
        let now = crate::util::current_time_ms();
        let cutoff_time = now - (retention_hours as u64 * 60 * 60 * 1000);

        // Phase 1: collect Arc refs while holding the DashMap Ref.
        let shard_arcs: Vec<_> = match self.inner.stream_records.get(stream_name) {
            Some(shard_map) => shard_map.iter().map(|e| e.value().clone()).collect(),
            None => return 0,
        }; // DashMap Ref dropped here.

        // Phase 2: scan and delete under per-shard locks only.
        let mut total_deleted = 0;
        let mut bytes_deleted = 0u64;
        for records_arc in shard_arcs {
            let keys_to_delete: Vec<String> = {
                let records = records_arc.read().await;
                records
                    .iter()
                    .filter_map(|(key, _)| {
                        let seq_num = key.split('/').nth(1)?;
                        let seq_obj = crate::sequence::parse_sequence(seq_num).ok()?;
                        if seq_obj.seq_time.unwrap_or(0) < cutoff_time {
                            Some(key.clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            };

            if !keys_to_delete.is_empty() {
                let mut records = records_arc.write().await;
                for key in &keys_to_delete {
                    if let Some(bytes) = records.remove(key) {
                        bytes_deleted += bytes.len() as u64;
                    }
                }
                total_deleted += keys_to_delete.len();
            }
        }

        if total_deleted > 0 {
            self.metrics
                .remove_retained(bytes_deleted, total_deleted as u64);
            self.persist_current_state().await;
        }
        total_deleted
    }

    /// Deletes records by their shard keys within a stream.
    pub async fn delete_record_keys(&self, stream_name: &str, keys: &[String]) {
        // Phase 1: collect Arc refs while holding the DashMap Ref.
        let pending: Vec<_> = {
            let shard_map = match self.inner.stream_records.get(stream_name) {
                Some(m) => m,
                None => return,
            };
            keys.iter()
                .filter_map(|key| {
                    let shard_hex = shard_hex_from_key(key);
                    shard_map
                        .get(shard_hex)
                        .map(|r| (r.value().clone(), key.clone()))
                })
                .collect()
        }; // DashMap Ref dropped here.

        // Phase 2: delete under per-shard write locks only.
        let pending_len = pending.len() as u64;
        let mut bytes_deleted = 0u64;
        for (records_arc, key) in pending {
            let mut records = records_arc.write().await;
            if let Some(bytes) = records.remove(&key) {
                bytes_deleted += bytes.len() as u64;
            }
        }
        if pending_len > 0 {
            self.metrics.remove_retained(bytes_deleted, pending_len);
            self.persist_current_state().await;
        }
    }

    /// Returns records in the given shard-key range for a stream.
    ///
    /// Both `range_start` and `range_end` are shard keys of the form
    /// `"{shard_hex}/{seq_num}"`.
    ///
    /// Used by integration tests only — production code uses
    /// [`get_records_range_limited`](Self::get_records_range_limited) or
    /// [`find_first_record_at_timestamp`](Self::find_first_record_at_timestamp).
    pub async fn get_records_range(
        &self,
        stream_name: &str,
        range_start: &str,
        range_end: &str,
    ) -> Vec<(String, StoredRecord)> {
        let shard_map = match self.inner.stream_records.get(stream_name) {
            Some(m) => m,
            None => return Vec::new(),
        };
        let shard_hex = shard_hex_from_key(range_start);
        let records_arc = match shard_map.get(shard_hex) {
            Some(r) => r.value().clone(),
            None => return Vec::new(),
        };
        let records = records_arc.read().await;
        records
            .range(range_start.to_string()..range_end.to_string())
            .map(|(k, v)| {
                let record: StoredRecord = postcard::from_bytes(v).unwrap();
                (k.clone(), record)
            })
            .collect()
    }

    /// Get records in a specific range for a stream, with a limit on the number returned
    pub async fn get_records_range_limited(
        &self,
        stream_name: &str,
        range_start: &str,
        range_end: &str,
        limit: usize,
    ) -> Vec<(String, StoredRecord)> {
        let shard_map = match self.inner.stream_records.get(stream_name) {
            Some(m) => m,
            None => return Vec::new(),
        };
        let shard_hex = shard_hex_from_key(range_start);
        let records_arc = match shard_map.get(shard_hex) {
            Some(r) => r.value().clone(),
            None => return Vec::new(),
        };
        let records = records_arc.read().await;
        records
            .range(range_start.to_string()..range_end.to_string())
            .take(limit)
            .map(|(k, v)| {
                let record: StoredRecord = postcard::from_bytes(v).unwrap();
                (k.clone(), record)
            })
            .collect()
    }

    /// Find the first record at or after a given timestamp in a range
    pub async fn find_first_record_at_timestamp(
        &self,
        stream_name: &str,
        range_start: &str,
        range_end: &str,
        timestamp: f64,
    ) -> Option<(String, StoredRecord)> {
        let shard_map = self.inner.stream_records.get(stream_name)?;
        let shard_hex = shard_hex_from_key(range_start);
        let records_arc = shard_map.get(shard_hex)?.value().clone();
        let records = records_arc.read().await;
        for (k, v) in records.range(range_start.to_string()..range_end.to_string()) {
            let record: StoredRecord = postcard::from_bytes(v).unwrap();
            if record.approximate_arrival_timestamp >= timestamp {
                return Some((k.clone(), record));
            }
        }
        None
    }

    // --- Consumer operations ---

    /// Inserts or replaces a consumer keyed by its ARN.
    pub async fn put_consumer(&self, consumer_arn: &str, consumer: Consumer) {
        let bytes = serde_json::to_vec(&consumer).unwrap();
        self.inner.consumers.insert(consumer_arn.to_string(), bytes);
        self.persist_current_state().await;
    }

    /// Returns the consumer with the given ARN, or `None` if not found.
    pub async fn get_consumer(&self, consumer_arn: &str) -> Option<Consumer> {
        self.inner
            .consumers
            .get(consumer_arn)
            .map(|entry| serde_json::from_slice(entry.value()).unwrap())
    }

    /// Deletes the consumer with the given ARN.
    pub async fn delete_consumer(&self, consumer_arn: &str) {
        self.inner.consumers.remove(consumer_arn);
        self.persist_current_state().await;
    }

    /// Returns all consumers whose ARN belongs to the given stream ARN.
    pub async fn list_consumers_for_stream(&self, stream_arn: &str) -> Vec<Consumer> {
        let prefix = format!("{stream_arn}/consumer/");
        self.inner
            .consumers
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| serde_json::from_slice(entry.value()).unwrap())
            .collect()
    }

    /// Finds a consumer by stream ARN and consumer name.
    ///
    /// Returns `None` if no consumer with the given name is registered to the stream.
    pub async fn find_consumer(&self, stream_arn: &str, consumer_name: &str) -> Option<Consumer> {
        let consumers = self.list_consumers_for_stream(stream_arn).await;
        consumers
            .into_iter()
            .find(|c| c.consumer_name == consumer_name)
    }

    // --- Resource policy operations ---

    /// Stores a resource policy JSON string for the given resource ARN.
    pub async fn put_policy(&self, resource_arn: &str, policy: &str) {
        self.inner
            .policies
            .insert(resource_arn.to_string(), policy.to_string());
        self.persist_current_state().await;
    }

    /// Returns the resource policy for the given ARN, or `None` if none is set.
    pub async fn get_policy(&self, resource_arn: &str) -> Option<String> {
        self.inner
            .policies
            .get(resource_arn)
            .map(|entry| entry.value().clone())
    }

    /// Deletes the resource policy for the given ARN.
    pub async fn delete_policy(&self, resource_arn: &str) {
        self.inner.policies.remove(resource_arn);
        self.persist_current_state().await;
    }

    /// Extracts the stream name from a Kinesis stream ARN.
    ///
    /// ARN format: `arn:aws:kinesis:{region}:{account}:stream/{name}`
    ///
    /// Returns `None` if the ARN does not contain a `/`.
    pub fn stream_name_from_arn(&self, arn: &str) -> Option<String> {
        arn.split("/").nth(1).map(|s| s.to_string())
    }

    /// Resolves the stream name from a JSON request body.
    ///
    /// Accepts bodies with `StreamName`, `StreamARN`, or both. AWS rejects requests
    /// that supply both fields simultaneously, and this method does the same.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisErrorResponse`] (`InvalidArgumentException`) if both
    /// `StreamName` and `StreamARN` are provided, or if neither is provided.
    /// Returns `ResourceNotFoundException` if an ARN is provided but the stream
    /// name cannot be resolved from it.
    pub fn resolve_stream_name(&self, data: &Value) -> Result<String, KinesisErrorResponse> {
        let stream_name_raw = data[constants::STREAM_NAME].as_str().unwrap_or("");
        let stream_arn = data[constants::STREAM_ARN].as_str().unwrap_or("");

        if !stream_name_raw.is_empty() && !stream_arn.is_empty() {
            return Err(KinesisErrorResponse::client_error(
                constants::INVALID_ARGUMENT,
                Some("StreamARN and StreamName cannot be provided together."),
            ));
        }

        if !stream_name_raw.is_empty() {
            Ok(stream_name_raw.to_string())
        } else if !stream_arn.is_empty() {
            self.stream_name_from_arn(stream_arn).ok_or_else(|| {
                KinesisErrorResponse::client_error(
                    constants::RESOURCE_NOT_FOUND,
                    Some("Could not resolve stream from ARN."),
                )
            })
        } else {
            Err(KinesisErrorResponse::client_error(
                constants::INVALID_ARGUMENT,
                Some("Either StreamName or StreamARN must be provided."),
            ))
        }
    }

    // --- Resource tag operations (for non-stream resources like consumers) ---

    /// Returns the tag map for the given resource ARN, or an empty map if none are set.
    pub async fn get_resource_tags(&self, resource_arn: &str) -> BTreeMap<String, String> {
        self.inner
            .resource_tags
            .get(resource_arn)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    /// Replaces the tag map for the given resource ARN.
    pub async fn put_resource_tags(&self, resource_arn: &str, tags: &BTreeMap<String, String>) {
        self.inner
            .resource_tags
            .insert(resource_arn.to_string(), tags.clone());
        self.persist_current_state().await;
    }

    // --- Account settings operations ---

    /// Returns the account-level settings object, or an empty JSON object if none are set.
    pub async fn get_account_settings(&self) -> Value {
        self.inner.account_settings.read().await.clone()
    }

    /// Stores the account-level settings object.
    pub async fn put_account_settings(&self, settings: &Value) {
        let mut guard = self.inner.account_settings.write().await;
        *guard = settings.clone();
        drop(guard);
        self.persist_current_state().await;
    }

    /// Probes store health. Always succeeds for the in-memory store.
    ///
    /// # Errors
    ///
    /// Returns [`StoreHealthError`] if the store is not ready. With the current
    /// in-memory implementation this never happens, but the signature is kept
    /// for API compatibility.
    pub fn check_ready(&self) -> Result<(), StoreHealthError> {
        if !self.health.durable_ok.load(Ordering::Relaxed) {
            let detail = self
                .health
                .last_error
                .read()
                .ok()
                .and_then(|guard| guard.clone())
                .unwrap_or_else(|| "durable state is not ready".to_string());
            return Err(StoreHealthError::ReadFailed(detail));
        }
        if !self.health.replay_complete.load(Ordering::Relaxed) {
            return Err(StoreHealthError::ReadFailed(
                "durable replay is not complete".to_string(),
            ));
        }
        if self.retained_capacity_exceeded() {
            let limit = self.options.max_retained_bytes.unwrap();
            return Err(StoreHealthError::ReadFailed(format!(
                "retained bytes limit exceeded: {} > {}",
                self.metrics.retained_bytes(),
                limit
            )));
        }
        Ok(())
    }

    // --- Lock-free hot-path operations ---

    /// Allocates a sequence number for a single record (lock-free on the hot path).
    ///
    /// 1. Read-locks stream metadata to route `hash_key` → shard (shared, non-blocking).
    /// 2. Atomically increments the per-shard counter (lock-free `fetch_add`).
    /// 3. Generates the sequence number string (pure computation).
    ///
    /// The caller is responsible for storing the record via [`put_record`](Self::put_record).
    pub async fn allocate_sequence(
        &self,
        name: &str,
        hash_key: &u128,
    ) -> Result<SequenceAllocation, KinesisErrorResponse> {
        let entry = self
            .inner
            .streams
            .get(name)
            .map(|e| e.value().clone())
            .ok_or_else(|| KinesisErrorResponse::stream_not_found(name, &self.aws_account_id))?;

        // Read-lock stream metadata (shared — concurrent PutRecords don't block each other).
        let stream = entry.stream.read().await;
        if !matches!(
            stream.stream_status,
            StreamStatus::Active | StreamStatus::Updating
        ) {
            return Err(KinesisErrorResponse::stream_not_found(
                name,
                &self.aws_account_id,
            ));
        }

        let (shard_ix, shard_id, shard_create_time) = route_hash_to_shard(&stream, hash_key);
        drop(stream); // Release read lock before atomic ops.

        // Lock-free sequence generation via per-shard AtomicU64.
        let shard_seq = entry.shard_seq.read().await;
        let now = current_time_ms().max(shard_create_time);
        let current_seq_ix = shard_seq
            .get(shard_ix as usize)
            .map(|s| s.counter.fetch_add(1, Ordering::Relaxed))
            .unwrap_or(0);
        drop(shard_seq);

        let seq_num = sequence::stringify_sequence(&sequence::SeqObj {
            shard_create_time,
            seq_ix: Some(current_seq_ix),
            byte1: None,
            seq_time: Some(now),
            seq_rand: None,
            shard_ix,
            version: 2,
        });
        let stream_key = format!("{}/{}", sequence::shard_ix_to_hex(shard_ix), seq_num);

        Ok(SequenceAllocation {
            shard_id,
            seq_num,
            stream_key,
            now,
        })
    }

    /// Allocates sequence numbers for a batch of records (lock-free on the hot path).
    ///
    /// Same concurrency properties as [`allocate_sequence`](Self::allocate_sequence)
    /// but processes multiple records in a single stream read-lock acquisition.
    pub async fn allocate_sequences_batch(
        &self,
        name: &str,
        hash_keys: &[u128],
    ) -> Result<Vec<SequenceAllocation>, KinesisErrorResponse> {
        let entry = self
            .inner
            .streams
            .get(name)
            .map(|e| e.value().clone())
            .ok_or_else(|| KinesisErrorResponse::stream_not_found(name, &self.aws_account_id))?;

        let stream = entry.stream.read().await;
        if !matches!(
            stream.stream_status,
            StreamStatus::Active | StreamStatus::Updating
        ) {
            return Err(KinesisErrorResponse::stream_not_found(
                name,
                &self.aws_account_id,
            ));
        }

        let shard_seq = entry.shard_seq.read().await;
        let mut allocations = Vec::with_capacity(hash_keys.len());

        for hash_key in hash_keys {
            let (shard_ix, shard_id, shard_create_time) = route_hash_to_shard(&stream, hash_key);
            let now = current_time_ms().max(shard_create_time);
            let current_seq_ix = shard_seq
                .get(shard_ix as usize)
                .map(|s| s.counter.fetch_add(1, Ordering::Relaxed))
                .unwrap_or(0);

            let seq_num = sequence::stringify_sequence(&sequence::SeqObj {
                shard_create_time,
                seq_ix: Some(current_seq_ix),
                byte1: None,
                seq_time: Some(now),
                seq_rand: None,
                shard_ix,
                version: 2,
            });
            let stream_key = format!("{}/{}", sequence::shard_ix_to_hex(shard_ix), seq_num);

            allocations.push(SequenceAllocation {
                shard_id,
                seq_num,
                stream_key,
                now,
            });
        }

        Ok(allocations)
    }

    /// Returns the current per-shard sequence counter (the next seq_ix that would
    /// be assigned). Used by LATEST iterators to position at the write frontier.
    pub async fn current_shard_seq(&self, name: &str, shard_ix: i64) -> u64 {
        if let Some(entry) = self.inner.streams.get(name).map(|e| e.value().clone()) {
            let shard_seq = entry.shard_seq.read().await;
            if let Some(seq_state) = shard_seq.get(shard_ix as usize) {
                return seq_state.counter.load(Ordering::Relaxed);
            }
        }
        0
    }

    /// Reserves shard write throughput for a pending record.
    ///
    /// When `enforce_limits` is disabled this is a no-op. When enabled, writes
    /// are capped per stream/shard at 1 MiB/s and 1000 records/s.
    pub async fn try_reserve_shard_throughput(
        &self,
        stream_name: &str,
        shard_id: &str,
        bytes: u64,
        now_ms: u64,
    ) -> Result<(), KinesisErrorResponse> {
        if !self.options.enforce_limits {
            return Ok(());
        }

        let key = throughput_window_key(stream_name, shard_id);
        let window = self
            .inner
            .throughput_windows
            .entry(key)
            .or_insert_with(|| {
                Arc::new(Mutex::new(ShardThroughputWindow {
                    window_start_ms: now_ms,
                    bytes: 0,
                    records: 0,
                }))
            })
            .clone();

        let mut window = window.lock().await;
        roll_throughput_window(&mut window, now_ms);
        reserve_throughput_window(&mut window, bytes)?;
        Ok(())
    }

    fn clear_throughput_windows_for_stream(&self, stream_name: &str) {
        let prefix = format!("{stream_name}:");
        self.inner
            .throughput_windows
            .retain(|key, _| !key.starts_with(&prefix));
    }

    pub(crate) fn clear_throughput_windows_for_shards<I, S>(&self, stream_name: &str, shard_ids: I)
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let keys = shard_ids
            .into_iter()
            .map(|shard_id| throughput_window_key(stream_name, shard_id.as_ref()))
            .collect::<Vec<_>>();

        if keys.is_empty() {
            return;
        }

        self.inner
            .throughput_windows
            .retain(|key, _| !keys.iter().any(|candidate| candidate == key));
    }

    #[doc(hidden)]
    pub fn has_throughput_window(&self, stream_name: &str, shard_id: &str) -> bool {
        self.inner
            .throughput_windows
            .contains_key(&throughput_window_key(stream_name, shard_id))
    }
}

// --- Free functions ---

/// Build per-shard sequence state from a stream's shard list.
fn build_shard_seq(stream: &Stream) -> Vec<ShardSeqState> {
    stream
        .shards
        .iter()
        .map(|_| ShardSeqState {
            // Skip index 0: the shard's starting_sequence_number is generated with
            // seq_ix=0. If the first PutRecord lands in the same millisecond as shard
            // creation, seq_time would also match, so seq_ix must be ≥1 to guarantee
            // every record's sequence number is strictly greater than the starting one.
            counter: AtomicU64::new(1),
        })
        .collect()
}

/// Extend shard_seq if the stream has more shards than tracked counters.
async fn sync_shard_seq(entry: &StreamEntry, stream: &Stream) {
    let mut shard_seq = entry.shard_seq.write().await;
    while shard_seq.len() < stream.shards.len() {
        shard_seq.push(ShardSeqState {
            // Skip index 0: the shard's starting_sequence_number is generated with
            // seq_ix=0. If the first PutRecord lands in the same millisecond as shard
            // creation, seq_time would also match, so seq_ix must be ≥1 to guarantee
            // every record's sequence number is strictly greater than the starting one.
            counter: AtomicU64::new(1),
        });
    }
}

/// Route a hash key to the appropriate open shard. Returns `(shard_ix, shard_id, create_time_ms)`.
fn route_hash_to_shard(stream: &Stream, hash_key: &u128) -> (i64, String, u64) {
    for (i, shard) in stream.shards.iter().enumerate() {
        if shard.sequence_number_range.ending_sequence_number.is_none() {
            let start = shard.hash_key_range.start_u128();
            let end = shard.hash_key_range.end_u128();
            if *hash_key >= start && *hash_key <= end {
                let create_time =
                    sequence::parse_sequence(&shard.sequence_number_range.starting_sequence_number)
                        .map(|s| s.shard_create_time)
                        .unwrap_or(0);
                return (i as i64, shard.shard_id.clone(), create_time);
            }
        }
    }
    (0, String::new(), 0)
}

fn throughput_window_key(stream_name: &str, shard_id: &str) -> String {
    format!("{stream_name}:{shard_id}")
}

fn roll_throughput_window(window: &mut ShardThroughputWindow, now_ms: u64) {
    if now_ms.saturating_sub(window.window_start_ms) >= 1000 {
        window.window_start_ms = now_ms;
        window.bytes = 0;
        window.records = 0;
    }
}

fn reserve_throughput_window(
    window: &mut ShardThroughputWindow,
    bytes: u64,
) -> Result<(), KinesisErrorResponse> {
    if window.bytes.saturating_add(bytes) > DEFAULT_SHARD_WRITE_BYTES_PER_SEC
        || window.records.saturating_add(1) > DEFAULT_SHARD_WRITE_RECORDS_PER_SEC
    {
        return Err(KinesisErrorResponse::client_error(
            "ProvisionedThroughputExceededException",
            Some("Rate exceeded for shard."),
        ));
    }

    window.bytes += bytes;
    window.records += 1;
    Ok(())
}
