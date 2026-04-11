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
use crate::types::{
    Consumer, ConsumerStatus, EncryptionType, HashKeyRange, SequenceNumberRange, Shard,
    StoredRecord, Stream, StreamStatus,
};
use crate::util::current_time_ms;
#[cfg(feature = "chaos")]
use crate::{chaos, chaos::ChaosController};
use dashmap::DashMap;
use num_bigint::BigUint;
use num_traits::One;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{Mutex, RwLock};

const DEFAULT_SHARD_WRITE_BYTES_PER_SEC: u64 = 1024 * 1024;
const DEFAULT_SHARD_WRITE_RECORDS_PER_SEC: u64 = 1000;
/// Default snapshot interval, in seconds, for durable mode.
pub const DEFAULT_DURABLE_SNAPSHOT_INTERVAL_SECS: u64 = 30;
/// Upper bound for externally configured durable snapshot intervals.
pub const MAX_DURABLE_SNAPSHOT_INTERVAL_SECS: u64 = 86_400;

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

/// Errors that can occur when validating externally supplied durable settings.
#[derive(Debug, thiserror::Error)]
pub enum DurableSettingsValidationError {
    /// Snapshot intervals must stay within the supported range.
    #[error(
        "snapshot_interval_secs must be between 0 and {MAX_DURABLE_SNAPSHOT_INTERVAL_SECS}, got {0}"
    )]
    SnapshotIntervalSecsOutOfRange(u64),
    /// Zero would make the retained-cap parser silently disable the limit.
    #[error("max_retained_bytes must be greater than 0")]
    MaxRetainedBytesMustBePositive,
}

/// Validate snapshot and retained-byte settings loaded from config/env sources.
pub fn validate_durable_settings(
    snapshot_interval_secs: Option<u64>,
    max_retained_bytes: Option<u64>,
) -> Result<(), DurableSettingsValidationError> {
    if let Some(snapshot_interval_secs) = snapshot_interval_secs
        && snapshot_interval_secs > MAX_DURABLE_SNAPSHOT_INTERVAL_SECS
    {
        return Err(
            DurableSettingsValidationError::SnapshotIntervalSecsOutOfRange(snapshot_interval_secs),
        );
    }
    if max_retained_bytes == Some(0) {
        return Err(DurableSettingsValidationError::MaxRetainedBytesMustBePositive);
    }
    Ok(())
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

/// Internal durable-transition ledger for async state changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PendingTransition {
    CreateStream {
        stream_name: String,
        ready_at_ms: u64,
        shards: Vec<Shard>,
    },
    DeleteStream {
        stream_name: String,
        ready_at_ms: u64,
    },
    RegisterConsumer {
        consumer_arn: String,
        ready_at_ms: u64,
    },
    DeregisterConsumer {
        consumer_arn: String,
        ready_at_ms: u64,
    },
    UpdateShardCount {
        stream_name: String,
        ready_at_ms: u64,
        target_shard_count: u32,
    },
    SplitShard {
        stream_name: String,
        ready_at_ms: u64,
        shard_to_split: String,
        new_starting_hash_key: String,
    },
    MergeShards {
        stream_name: String,
        ready_at_ms: u64,
        shard_to_merge: String,
        adjacent_shard_to_merge: String,
    },
    StartStreamEncryption {
        stream_name: String,
        ready_at_ms: u64,
    },
    StopStreamEncryption {
        stream_name: String,
        ready_at_ms: u64,
    },
}

impl PendingTransition {
    fn key(&self) -> String {
        match self {
            Self::CreateStream { stream_name, .. }
            | Self::DeleteStream { stream_name, .. }
            | Self::UpdateShardCount { stream_name, .. }
            | Self::SplitShard { stream_name, .. }
            | Self::MergeShards { stream_name, .. }
            | Self::StartStreamEncryption { stream_name, .. }
            | Self::StopStreamEncryption { stream_name, .. } => {
                format!("stream:{stream_name}")
            }
            Self::RegisterConsumer { consumer_arn, .. }
            | Self::DeregisterConsumer { consumer_arn, .. } => {
                format!("consumer:{consumer_arn}")
            }
        }
    }

    fn ready_at_ms(&self) -> u64 {
        match self {
            Self::CreateStream { ready_at_ms, .. }
            | Self::DeleteStream { ready_at_ms, .. }
            | Self::RegisterConsumer { ready_at_ms, .. }
            | Self::DeregisterConsumer { ready_at_ms, .. }
            | Self::UpdateShardCount { ready_at_ms, .. }
            | Self::SplitShard { ready_at_ms, .. }
            | Self::MergeShards { ready_at_ms, .. }
            | Self::StartStreamEncryption { ready_at_ms, .. }
            | Self::StopStreamEncryption { ready_at_ms, .. } => *ready_at_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum TransitionMutation {
    None,
    Upsert(PendingTransition),
    Remove(String),
}

/// Runtime configuration passed to [`crate::create_app`].
///
/// All fields have sensible defaults via [`StoreOptions::default`].
///
/// # Examples
///
/// ```rust
/// use ferrokinesis::store::{DurableStateOptions, StoreOptions};
/// use std::path::PathBuf;
///
/// let opts = StoreOptions {
///     shard_limit: 50,
///     aws_region: "eu-west-1".to_string(),
///     durable: Some(DurableStateOptions {
///         state_dir: PathBuf::from("/tmp/ferrokinesis-state"),
///         snapshot_interval_secs: 10,
///         max_retained_bytes: Some(1024),
///     }),
///     ..StoreOptions::default()
/// };
/// ```
/// Configuration for on-disk durable state.
#[derive(Debug, Clone)]
pub struct DurableStateOptions {
    /// Directory used to persist state with WAL + snapshots.
    pub state_dir: PathBuf,
    /// Snapshot interval in seconds when durable mode is enabled.
    pub snapshot_interval_secs: u64,
    /// Optional retained-bytes cap to apply alongside durable mode.
    pub max_retained_bytes: Option<u64>,
}

/// Runtime configuration passed to [`crate::create_app`].
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
    /// Maximum number of records emitted in a single `SubscribeToShard` event.
    /// Defaults to `10_000`.
    pub subscribe_to_shard_event_record_limit: usize,
    /// Maximum lifetime of a single `SubscribeToShard` session in milliseconds.
    /// Defaults to `300_000` (5 minutes).
    pub subscribe_to_shard_session_ms: u64,
    /// Background retention-reaper check interval in seconds.
    /// Set to `0` (default) to disable the reaper.
    pub retention_check_interval_secs: u64,
    /// Enable AWS-like shard write throughput throttling. Disabled by default.
    pub enforce_limits: bool,
    /// Durable persistence settings. When omitted, the store remains in-memory only.
    pub durable: Option<DurableStateOptions>,
    /// Hard cap on retained serialized record bytes.
    pub max_retained_bytes: Option<u64>,
    /// Chaos/fault-injection configuration compiled behind the `chaos` feature.
    #[cfg(feature = "chaos")]
    pub chaos: chaos::ChaosConfig,
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
            subscribe_to_shard_event_record_limit: 10_000,
            subscribe_to_shard_session_ms: 300_000,
            retention_check_interval_secs: 0,
            enforce_limits: false,
            durable: None,
            max_retained_bytes: None,
            #[cfg(feature = "chaos")]
            chaos: chaos::ChaosConfig::default(),
            aws_account_id: "000000000000".to_string(),
            aws_region: "us-east-1".to_string(),
        }
    }
}

impl StoreOptions {
    fn effective_max_retained_bytes(&self) -> Option<u64> {
        self.max_retained_bytes.or(self
            .durable
            .as_ref()
            .and_then(|durable| durable.max_retained_bytes))
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

/// A charged shard-throughput reservation that may be refunded on rollback.
#[derive(Debug, Clone)]
pub struct ThroughputReservation {
    key: Option<String>,
    window_start_ms: u64,
    bytes: u64,
}

impl ThroughputReservation {
    fn disabled() -> Self {
        Self {
            key: None,
            window_start_ms: 0,
            bytes: 0,
        }
    }
}

/// Per-shard record map: shard_hex → sorted records.
type ShardRecords = DashMap<String, Arc<RwLock<BTreeMap<String, Vec<u8>>>>>;
#[cfg(not(target_arch = "wasm32"))]
type RestoredShardRecords = Vec<(String, BTreeMap<String, Vec<u8>>)>;
#[cfg(not(target_arch = "wasm32"))]
type RestoredStreamRecords = Vec<(String, RestoredShardRecords)>;

struct ShardThroughputWindow {
    window_start_ms: u64,
    bytes: u64,
    records: u64,
}

#[cfg(not(target_arch = "wasm32"))]
struct AppliedRecordChange {
    records_arc: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
    key: String,
    previous: Option<Vec<u8>>,
    new_bytes_len: u64,
}

#[cfg(not(target_arch = "wasm32"))]
struct AppliedDeleteChange {
    records_arc: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
    key: String,
    deleted_bytes: Vec<u8>,
}

#[cfg(not(target_arch = "wasm32"))]
struct RestoredState {
    streams: Vec<(String, Stream, Vec<u64>)>,
    records: RestoredStreamRecords,
    consumers: Vec<(String, Vec<u8>)>,
    policies: Vec<(String, String)>,
    resource_tags: Vec<(String, BTreeMap<String, String>)>,
    account_settings: Value,
    pending_transitions: BTreeMap<String, PendingTransition>,
    retained_bytes: u64,
    retained_records: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl RestoredState {
    fn from_snapshot(snapshot: PersistentSnapshot) -> Result<Self, String> {
        let PersistentSnapshot {
            streams,
            records,
            consumers,
            policies,
            resource_tags,
            account_settings_json,
            pending_transitions,
            retained_bytes,
            retained_records,
            ..
        } = snapshot;

        let account_settings = if account_settings_json.is_empty() {
            Value::Object(Default::default())
        } else {
            serde_json::from_slice(&account_settings_json)
                .map_err(|err| format!("failed to decode persisted account settings: {err}"))?
        };

        Ok(Self {
            streams: streams
                .into_iter()
                .map(|stream| {
                    let (name, stream_value, shard_counters) = stream.into_parts();
                    (name, stream_value, shard_counters)
                })
                .collect(),
            records: records
                .into_iter()
                .fold(BTreeMap::new(), |mut acc, shard_records| {
                    acc.entry(shard_records.stream_name)
                        .or_insert_with(Vec::new)
                        .push((
                            shard_records.shard_hex,
                            shard_records.records.into_iter().collect(),
                        ));
                    acc
                })
                .into_iter()
                .collect(),
            consumers,
            policies,
            resource_tags,
            account_settings,
            pending_transitions: pending_transitions
                .into_iter()
                .map(|transition| (transition.key(), transition))
                .collect(),
            retained_bytes,
            retained_records,
        })
    }
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
    pending_transitions: RwLock<BTreeMap<String, PendingTransition>>,
    write_lock: Mutex<()>,
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
    #[cfg(feature = "chaos")]
    chaos: Arc<ChaosController>,
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
        #[cfg(feature = "chaos")]
        let chaos = Arc::new(ChaosController::new(options.chaos.clone()));

        let inner = Arc::new(StoreInner {
            streams: DashMap::new(),
            stream_records: DashMap::new(),
            consumers: DashMap::new(),
            policies: DashMap::new(),
            resource_tags: DashMap::new(),
            account_settings: RwLock::new(Value::Object(Default::default())),
            throughput_windows: DashMap::new(),
            pending_transitions: RwLock::new(BTreeMap::new()),
            write_lock: Mutex::new(()),
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
            #[cfg(feature = "chaos")]
            chaos,
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
        if let Some(durable) = store.options.durable.clone() {
            match Persistence::new(durable.state_dir) {
                Ok(backend) => {
                    let persistence = Arc::new(PersistenceState {
                        backend,
                        io_lock: Mutex::new(()),
                        snapshot_interval_secs: durable.snapshot_interval_secs,
                        last_snapshot_ms: AtomicU64::new(0),
                    });
                    store.persistence = Some(Arc::clone(&persistence));
                    if let Err(err) = store.load_persistent_state(&persistence) {
                        store.mark_unhealthy(err);
                    } else if let Err(err) = store.resume_persistent_transitions() {
                        store.mark_unhealthy(err);
                    }
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

        self.metrics.set_replay_complete(false);
        self.health.replay_complete.store(false, Ordering::Relaxed);

        let mut restored_snapshot = snapshot;
        for entry in entries {
            Self::apply_wal_entry(&mut restored_snapshot, entry)?;
        }

        let created_at_ms = restored_snapshot.created_at_ms;
        let restored_state = RestoredState::from_snapshot(restored_snapshot)?;
        self.install_restored_state(restored_state);
        persistence
            .last_snapshot_ms
            .store(created_at_ms, Ordering::Relaxed);
        self.metrics.set_last_snapshot_ms(created_at_ms);
        self.metrics.set_replay_complete(true);
        self.health.replay_complete.store(true, Ordering::Relaxed);
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn apply_wal_entry(snapshot: &mut PersistentSnapshot, entry: WalEntry) -> Result<(), String> {
        match entry {
            WalEntry::Snapshot(next_snapshot) => {
                *snapshot = next_snapshot;
                Ok(())
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn install_restored_state(&self, restored_state: RestoredState) {
        self.inner.streams.clear();
        self.inner.stream_records.clear();
        self.inner.consumers.clear();
        self.inner.policies.clear();
        self.inner.resource_tags.clear();
        self.inner.throughput_windows.clear();

        for (name, stream_value, shard_counters) in restored_state.streams {
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

        for (stream_name, shards) in restored_state.records {
            let shard_map = self.inner.stream_records.entry(stream_name).or_default();
            for (shard_hex, records) in shards {
                shard_map.insert(shard_hex, Arc::new(RwLock::new(records)));
            }
        }

        for (consumer_arn, bytes) in restored_state.consumers {
            self.inner.consumers.insert(consumer_arn, bytes);
        }
        for (resource_arn, policy) in restored_state.policies {
            self.inner.policies.insert(resource_arn, policy);
        }
        for (resource_arn, tags) in restored_state.resource_tags {
            self.inner.resource_tags.insert(resource_arn, tags);
        }
        if let Ok(mut transitions) = self.inner.pending_transitions.try_write() {
            *transitions = restored_state.pending_transitions;
        }
        if let Ok(mut settings) = self.inner.account_settings.try_write() {
            *settings = restored_state.account_settings;
        }

        self.metrics.set_retained(
            restored_state.retained_bytes,
            restored_state.retained_records,
        );
        self.refresh_topology_metrics_sync();
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn resume_persistent_transitions(&self) -> Result<(), String> {
        let transitions_guard = self
            .inner
            .pending_transitions
            .try_read()
            .map_err(|_| "failed to read pending transitions during recovery".to_string())?;
        let mut transitions: Vec<PendingTransition> = transitions_guard.values().cloned().collect();
        drop(transitions_guard);

        let existing_keys: std::collections::BTreeSet<String> =
            transitions.iter().map(PendingTransition::key).collect();
        let now = current_time_ms();

        for entry in self.inner.streams.iter() {
            let stream = entry
                .value()
                .stream
                .try_read()
                .map_err(|_| format!("failed to inspect recovered stream {}", entry.key()))?;
            let key = format!("stream:{}", entry.key());
            if existing_keys.contains(&key) {
                continue;
            }
            match stream.stream_status {
                StreamStatus::Deleting => transitions.push(PendingTransition::DeleteStream {
                    stream_name: entry.key().clone(),
                    ready_at_ms: now,
                }),
                StreamStatus::Creating => {
                    return Err(format!(
                        "ambiguous persisted stream transition for {}: CREATING without transition metadata",
                        entry.key()
                    ));
                }
                StreamStatus::Updating => {
                    return Err(format!(
                        "ambiguous persisted stream transition for {}: UPDATING without transition metadata",
                        entry.key()
                    ));
                }
                StreamStatus::Active => {}
            }
        }

        for entry in self.inner.consumers.iter() {
            let consumer: Consumer = serde_json::from_slice(entry.value()).map_err(|err| {
                format!("failed to decode recovered consumer {}: {err}", entry.key())
            })?;
            let key = format!("consumer:{}", entry.key());
            if existing_keys.contains(&key) {
                continue;
            }
            match consumer.consumer_status {
                ConsumerStatus::Creating => {
                    transitions.push(PendingTransition::RegisterConsumer {
                        consumer_arn: entry.key().clone(),
                        ready_at_ms: now,
                    });
                }
                ConsumerStatus::Deleting => {
                    transitions.push(PendingTransition::DeregisterConsumer {
                        consumer_arn: entry.key().clone(),
                        ready_at_ms: now,
                    });
                }
                ConsumerStatus::Active => {}
            }
        }

        if let Ok(mut pending) = self.inner.pending_transitions.try_write() {
            for transition in &transitions {
                pending
                    .entry(transition.key())
                    .or_insert_with(|| transition.clone());
            }
        }

        for transition in transitions {
            self.schedule_transition(transition);
        }

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
            pending_transitions: self
                .inner
                .pending_transitions
                .read()
                .await
                .values()
                .cloned()
                .collect(),
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
    async fn persist_current_state_result(&self) -> Result<(), String> {
        if let Some(detail) = self.persistent_mutation_block_reason() {
            return Err(detail);
        }
        let Some(persistence) = &self.persistence else {
            return Ok(());
        };
        let _guard = persistence.io_lock.lock().await;
        if let Some(detail) = self.persistent_mutation_block_reason() {
            return Err(detail);
        }
        let snapshot = match self.export_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(err) => {
                self.mark_unhealthy(err.clone());
                return Err(err);
            }
        };

        self.persist_snapshot(&snapshot).await;
        if !self.health.durable_ok.load(Ordering::Relaxed) {
            let detail = self.durable_state_error_detail();
            return Err(detail);
        }

        if persistence.snapshot_interval_secs == 0 {
            return Ok(());
        }

        let now = snapshot.created_at_ms;
        let last_snapshot = persistence.last_snapshot_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last_snapshot)
            < persistence.snapshot_interval_secs.saturating_mul(1000)
        {
            return Ok(());
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

        Ok(())
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
            .effective_max_retained_bytes()
            .is_some_and(|limit| self.metrics.retained_bytes() > limit)
    }

    fn retained_limit_error(
        &self,
        current_bytes: u64,
        additional_bytes: u64,
    ) -> KinesisErrorResponse {
        self.metrics.increment_rejected_writes();
        let limit = self.options.effective_max_retained_bytes().unwrap();
        KinesisErrorResponse::client_error(
            constants::LIMIT_EXCEEDED,
            Some(&format!(
                "Retained bytes limit exceeded: {} + {} > {}.",
                current_bytes, additional_bytes, limit
            )),
        )
    }

    fn durable_state_requested(&self) -> bool {
        self.options.durable.is_some()
    }

    fn durable_state_error_detail(&self) -> String {
        self.health
            .last_error
            .read()
            .ok()
            .and_then(|guard| guard.clone())
            .unwrap_or_else(|| "durable state is not ready".to_string())
    }

    fn availability_block_reason(&self) -> Option<String> {
        if !self.health.durable_ok.load(Ordering::Relaxed) {
            return Some(self.durable_state_error_detail());
        }
        if !self.health.replay_complete.load(Ordering::Relaxed) {
            return Some("durable replay is not complete".to_string());
        }
        None
    }

    pub(crate) fn check_available(&self) -> Result<(), KinesisErrorResponse> {
        if let Some(detail) = self.availability_block_reason() {
            return Err(KinesisErrorResponse::server_error(None, Some(&detail)));
        }
        Ok(())
    }

    pub(crate) fn check_writable(&self) -> Result<(), KinesisErrorResponse> {
        self.check_available()?;
        if self.retained_capacity_exceeded() {
            return Err(self.retained_limit_error(self.metrics.retained_bytes(), 0));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn persistent_mutation_block_reason(&self) -> Option<String> {
        if !self.durable_state_requested() {
            return None;
        }
        if let Some(detail) = self.availability_block_reason() {
            return Some(detail);
        }
        if self.persistence.is_none() {
            return Some("durable state backend is unavailable".to_string());
        }
        None
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn check_persistent_mutation_allowed(&self) -> Result<(), KinesisErrorResponse> {
        if let Some(detail) = self.persistent_mutation_block_reason() {
            return Err(KinesisErrorResponse::server_error(None, Some(&detail)));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn capture_transition_state(
        &self,
        update: &TransitionMutation,
    ) -> Option<(String, Option<PendingTransition>)> {
        match update {
            TransitionMutation::None => None,
            TransitionMutation::Upsert(transition) => {
                let key = transition.key();
                let previous = self
                    .inner
                    .pending_transitions
                    .read()
                    .await
                    .get(&key)
                    .cloned();
                Some((key, previous))
            }
            TransitionMutation::Remove(key) => {
                let previous = self
                    .inner
                    .pending_transitions
                    .read()
                    .await
                    .get(key)
                    .cloned();
                Some((key.clone(), previous))
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn restore_transition_state(
        &self,
        snapshot: Option<(String, Option<PendingTransition>)>,
    ) {
        let Some((key, previous)) = snapshot else {
            return;
        };

        let mut transitions = self.inner.pending_transitions.write().await;
        match previous {
            Some(transition) => {
                transitions.insert(key, transition);
            }
            None => {
                transitions.remove(&key);
            }
        }
    }

    fn transition_requires_write_lock(transition: &TransitionMutation) -> bool {
        !matches!(transition, TransitionMutation::None)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn lock_transition_mutation<'a>(
        &'a self,
        transition: &TransitionMutation,
        skip_write_lock: bool,
    ) -> Result<Option<tokio::sync::MutexGuard<'a, ()>>, KinesisErrorResponse> {
        if self.durable_state_requested() {
            self.check_persistent_mutation_allowed()?;
        }

        let needs_lock = !skip_write_lock
            && (self.durable_state_requested() || Self::transition_requires_write_lock(transition));
        if needs_lock {
            Ok(Some(self.inner.write_lock.lock().await))
        } else {
            Ok(None)
        }
    }

    #[cfg(target_arch = "wasm32")]
    async fn lock_transition_mutation<'a>(
        &'a self,
        transition: &TransitionMutation,
        skip_write_lock: bool,
    ) -> Result<Option<tokio::sync::MutexGuard<'a, ()>>, KinesisErrorResponse> {
        let needs_lock = !skip_write_lock && Self::transition_requires_write_lock(transition);
        if needs_lock {
            Ok(Some(self.inner.write_lock.lock().await))
        } else {
            Ok(None)
        }
    }

    async fn apply_transition_mutation(&self, update: TransitionMutation) {
        let mut transitions = self.inner.pending_transitions.write().await;
        match update {
            TransitionMutation::None => {}
            TransitionMutation::Upsert(transition) => {
                transitions.insert(transition.key(), transition);
            }
            TransitionMutation::Remove(key) => {
                transitions.remove(&key);
            }
        }
    }

    #[cfg(feature = "chaos")]
    pub(crate) fn chaos_status(&self) -> chaos::ChaosStatus {
        self.chaos.status()
    }

    #[cfg(feature = "chaos")]
    pub(crate) fn set_chaos_enabled(
        &self,
        ids: Option<&[String]>,
        enabled: bool,
    ) -> Result<chaos::ChaosStatus, chaos::ChaosApiError> {
        self.chaos.set_enabled(ids, enabled)
    }

    #[cfg(feature = "chaos")]
    pub(crate) fn chaos_request_plan(
        &self,
        operation: crate::actions::Operation,
    ) -> chaos::RequestPlan {
        self.chaos.request_plan(operation)
    }

    #[cfg(feature = "chaos")]
    pub(crate) fn chaos_put_records_failures(
        &self,
        stream_name: &str,
        allocations: &[SequenceAllocation],
    ) -> Vec<Option<chaos::PutRecordsFailure>> {
        self.chaos
            .put_records_failures(stream_name, &self.aws_account_id, allocations)
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
    pub async fn put_stream(&self, name: &str, stream: Stream) -> Result<(), KinesisErrorResponse> {
        self.put_stream_with_transition(name, stream, TransitionMutation::None)
            .await
    }

    pub(crate) async fn put_stream_with_transition(
        &self,
        name: &str,
        stream: Stream,
        transition: TransitionMutation,
    ) -> Result<(), KinesisErrorResponse> {
        self.put_stream_with_transition_internal(name, stream, transition, false)
            .await
    }

    async fn put_stream_with_transition_internal(
        &self,
        name: &str,
        stream: Stream,
        transition: TransitionMutation,
        skip_write_lock: bool,
    ) -> Result<(), KinesisErrorResponse> {
        let _write_guard = self
            .lock_transition_mutation(&transition, skip_write_lock)
            .await?;

        #[cfg(not(target_arch = "wasm32"))]
        let transition_snapshot = self.capture_transition_state(&transition).await;
        let existing = self
            .inner
            .streams
            .get(name)
            .map(|entry| entry.value().clone());
        #[cfg(not(target_arch = "wasm32"))]
        let previous_stream = if let Some(entry) = existing.as_ref() {
            Some(entry.stream.read().await.clone())
        } else {
            None
        };

        if let Some(entry) = existing.as_ref() {
            let mut guard = entry.stream.write().await;
            *guard = stream.clone();
        } else {
            let shard_seq = build_shard_seq(&stream);
            let entry = Arc::new(StreamEntry {
                stream: RwLock::new(stream),
                shard_seq: RwLock::new(shard_seq),
            });
            self.inner.streams.insert(name.to_string(), entry);
        }
        self.apply_transition_mutation(transition).await;
        self.refresh_topology_metrics().await;

        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            if let Some(entry) = existing {
                let mut guard = entry.stream.write().await;
                *guard = previous_stream.expect("existing stream snapshot");
            } else {
                self.inner.streams.remove(name);
            }
            self.restore_transition_state(transition_snapshot).await;
            self.refresh_topology_metrics().await;
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }

        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;

        Ok(())
    }

    fn shard_limit_error(
        &self,
        current_shards: u32,
        requested_shards: u32,
    ) -> KinesisErrorResponse {
        KinesisErrorResponse::client_error(
            constants::LIMIT_EXCEEDED,
            Some(&format!(
                "This request would exceed the shard limit for the account {} in {}. \
                 Current shard count for the account: {}. Limit: {}. \
                 Number of additional shards that would have resulted from this request: {}. \
                 Refer to the AWS Service Limits page \
                 (http://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) \
                 for current limits and how to request higher limits.",
                self.aws_account_id,
                self.aws_region,
                current_shards,
                self.options.shard_limit,
                requested_shards
            )),
        )
    }

    async fn open_shard_count_for_stream(&self, stream_name: &str) -> u32 {
        let Some(entry) = self.inner.streams.get(stream_name) else {
            return 0;
        };
        let stream = entry.value().stream.read().await;
        stream
            .shards
            .iter()
            .filter(|s| s.sequence_number_range.ending_sequence_number.is_none())
            .count() as u32
    }

    async fn pending_shard_reservation(&self, transition: &PendingTransition) -> u32 {
        match transition {
            PendingTransition::CreateStream {
                stream_name,
                shards,
                ..
            } => {
                let open_shards = self.open_shard_count_for_stream(stream_name).await;
                if open_shards == 0 {
                    shards.len() as u32
                } else {
                    0
                }
            }
            PendingTransition::SplitShard { .. } => 1,
            PendingTransition::UpdateShardCount {
                stream_name,
                target_shard_count,
                ..
            } => target_shard_count
                .saturating_sub(self.open_shard_count_for_stream(stream_name).await),
            PendingTransition::DeleteStream { .. }
            | PendingTransition::RegisterConsumer { .. }
            | PendingTransition::DeregisterConsumer { .. }
            | PendingTransition::MergeShards { .. }
            | PendingTransition::StartStreamEncryption { .. }
            | PendingTransition::StopStreamEncryption { .. } => 0,
        }
    }

    async fn sum_account_shards_including_pending_expansions(&self) -> u32 {
        let mut total = self.sum_open_shards().await;
        let pending = self.inner.pending_transitions.read().await;
        for transition in pending.values() {
            total = total.saturating_add(self.pending_shard_reservation(transition).await);
        }
        total
    }

    pub(crate) async fn create_stream_with_reservation(
        &self,
        name: &str,
        shard_count: u32,
        stream: Stream,
        transition: PendingTransition,
    ) -> Result<(), KinesisErrorResponse> {
        let _write_guard = self.inner.write_lock.lock().await;

        if self.inner.streams.contains_key(name) {
            return Err(KinesisErrorResponse::stream_in_use(
                name,
                &self.aws_account_id,
            ));
        }

        let reserved = self.sum_account_shards_including_pending_expansions().await;
        if reserved.saturating_add(shard_count) > self.options.shard_limit {
            return Err(self.shard_limit_error(reserved, shard_count));
        }

        self.put_stream_with_transition_internal(
            name,
            stream,
            TransitionMutation::Upsert(transition),
            true,
        )
        .await
    }

    pub(crate) async fn split_shard_with_reservation(
        &self,
        stream_name: &str,
        shard_id: &str,
        shard_ix: i64,
        new_starting_hash_key: &str,
        transition: PendingTransition,
    ) -> Result<(), KinesisErrorResponse> {
        let _write_guard = self.inner.write_lock.lock().await;
        let reserved = self.sum_account_shards_including_pending_expansions().await;
        let shard_id = shard_id.to_string();
        let new_starting_hash_key = new_starting_hash_key.to_string();

        self.update_stream_with_transition_internal(
            stream_name,
            TransitionMutation::Upsert(transition),
            true,
            move |stream| {
                if stream.stream_status != StreamStatus::Active {
                    return Err(KinesisErrorResponse::client_error(
                        constants::RESOURCE_IN_USE,
                        Some(&format!(
                            "Stream {} under account {} not ACTIVE, instead in state {}",
                            stream_name, self.aws_account_id, stream.stream_status
                        )),
                    ));
                }

                if shard_ix >= stream.shards.len() as i64 {
                    return Err(KinesisErrorResponse::client_error(
                        constants::RESOURCE_NOT_FOUND,
                        Some(&format!(
                            "Could not find shard {} in stream {} under account {}.",
                            shard_id, stream_name, self.aws_account_id
                        )),
                    ));
                }

                if reserved.saturating_add(1) > self.options.shard_limit {
                    return Err(self.shard_limit_error(reserved, 1));
                }

                let hash_key: u128 = new_starting_hash_key.parse().unwrap_or(0);
                let shard = &stream.shards[shard_ix as usize];
                let shard_start = shard.hash_key_range.start_u128();
                let shard_end = shard.hash_key_range.end_u128();

                if hash_key <= shard_start + 1 || hash_key >= shard_end {
                    return Err(KinesisErrorResponse::client_error(
                        constants::INVALID_ARGUMENT,
                        Some(&format!(
                            "NewStartingHashKey {} used in SplitShard() on shard {} in stream {} under account {} \
                             is not both greater than one plus the shard's StartingHashKey {} and less than the shard's EndingHashKey {}.",
                            new_starting_hash_key,
                            shard_id,
                            stream_name,
                            self.aws_account_id,
                            shard.hash_key_range.starting_hash_key,
                            shard.hash_key_range.ending_hash_key
                        )),
                    ));
                }

                stream.stream_status = StreamStatus::Updating;
                Ok(())
            },
        )
        .await
    }

    pub(crate) async fn update_shard_count_with_reservation(
        &self,
        stream_name: &str,
        target_shard_count: u32,
        transition: PendingTransition,
    ) -> Result<u32, KinesisErrorResponse> {
        let _write_guard = self.inner.write_lock.lock().await;
        let reserved = self.sum_account_shards_including_pending_expansions().await;

        self.update_stream_with_transition_internal(
            stream_name,
            TransitionMutation::Upsert(transition),
            true,
            move |stream| {
                if stream.stream_status != StreamStatus::Active {
                    return Err(KinesisErrorResponse::client_error(
                        constants::RESOURCE_IN_USE,
                        Some(&format!(
                            "Stream {} under account {} not ACTIVE, instead in state {}",
                            stream_name, self.aws_account_id, stream.stream_status
                        )),
                    ));
                }

                let current_count = stream
                    .shards
                    .iter()
                    .filter(|s| s.sequence_number_range.ending_sequence_number.is_none())
                    .count() as u32;

                if target_shard_count == current_count {
                    return Err(KinesisErrorResponse::client_error(
                        constants::INVALID_ARGUMENT,
                        Some(&format!(
                            "TargetShardCount {} is the same as the current shard count {}.",
                            target_shard_count, current_count
                        )),
                    ));
                }

                let additional_shards = target_shard_count.saturating_sub(current_count);
                if additional_shards > 0
                    && reserved.saturating_add(additional_shards) > self.options.shard_limit
                {
                    return Err(self.shard_limit_error(reserved, additional_shards));
                }

                stream.stream_status = StreamStatus::Updating;
                Ok(current_count)
            },
        )
        .await
    }

    /// Deletes a stream and all of its records.
    pub async fn delete_stream(&self, name: &str) -> Result<(), KinesisErrorResponse> {
        self.delete_stream_with_transition(name, TransitionMutation::None)
            .await
    }

    pub(crate) async fn delete_stream_with_transition(
        &self,
        name: &str,
        transition: TransitionMutation,
    ) -> Result<(), KinesisErrorResponse> {
        self.delete_stream_with_transition_internal(name, transition, false)
            .await
    }

    async fn delete_stream_with_transition_internal(
        &self,
        name: &str,
        transition: TransitionMutation,
        skip_write_lock: bool,
    ) -> Result<(), KinesisErrorResponse> {
        let _write_guard = self
            .lock_transition_mutation(&transition, skip_write_lock)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let transition_snapshot = self.capture_transition_state(&transition).await;
        let removed_stream = self.inner.streams.remove(name).map(|(_, entry)| entry);
        let removed_records = self
            .inner
            .stream_records
            .remove(name)
            .map(|(_, records)| records);
        let mut removed_bytes = 0u64;
        let mut removed_count = 0u64;
        if let Some(records) = removed_records.as_ref() {
            for shard in records.iter() {
                let shard_records = shard.value().read().await;
                removed_count += shard_records.len() as u64;
                removed_bytes += shard_records
                    .values()
                    .map(|value| value.len() as u64)
                    .sum::<u64>();
            }
            self.metrics.remove_retained(removed_bytes, removed_count);
        }
        self.apply_transition_mutation(transition).await;
        self.refresh_topology_metrics().await;

        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            if let Some(entry) = removed_stream {
                self.inner.streams.insert(name.to_string(), entry);
            }
            if let Some(records) = removed_records {
                self.inner.stream_records.insert(name.to_string(), records);
            }
            if removed_count > 0 {
                self.metrics.add_retained(removed_bytes, removed_count);
            }
            self.restore_transition_state(transition_snapshot).await;
            self.refresh_topology_metrics().await;
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }

        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;

        self.clear_throughput_windows_for_stream(name);

        Ok(())
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
        self.update_stream_with_transition(name, TransitionMutation::None, f)
            .await
    }

    pub(crate) async fn update_stream_with_transition<F, R>(
        &self,
        name: &str,
        transition: TransitionMutation,
        f: F,
    ) -> Result<R, KinesisErrorResponse>
    where
        F: FnOnce(&mut Stream) -> Result<R, KinesisErrorResponse>,
    {
        self.update_stream_with_transition_internal(name, transition, false, f)
            .await
    }

    async fn update_stream_with_transition_internal<F, R>(
        &self,
        name: &str,
        transition: TransitionMutation,
        skip_write_lock: bool,
        f: F,
    ) -> Result<R, KinesisErrorResponse>
    where
        F: FnOnce(&mut Stream) -> Result<R, KinesisErrorResponse>,
    {
        let _write_guard = self
            .lock_transition_mutation(&transition, skip_write_lock)
            .await?;

        let entry = self
            .inner
            .streams
            .get(name)
            .map(|e| e.value().clone())
            .ok_or_else(|| KinesisErrorResponse::stream_not_found(name, &self.aws_account_id))?;
        let previous_stream = entry.stream.read().await.clone();
        let previous_counters = entry
            .shard_seq
            .read()
            .await
            .iter()
            .map(|state| state.counter.load(Ordering::Relaxed))
            .collect::<Vec<_>>();
        let mut candidate = previous_stream.clone();
        let result = f(&mut candidate)?;

        {
            let mut stream = entry.stream.write().await;
            *stream = candidate.clone();
        }
        sync_shard_seq(&entry, &candidate).await;
        #[cfg(not(target_arch = "wasm32"))]
        let transition_snapshot = self.capture_transition_state(&transition).await;
        self.apply_transition_mutation(transition).await;
        self.refresh_topology_metrics().await;

        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            {
                let mut stream = entry.stream.write().await;
                *stream = previous_stream;
            }
            {
                let mut shard_seq = entry.shard_seq.write().await;
                *shard_seq = build_shard_seq_from_counters(&previous_counters);
            }
            self.restore_transition_state(transition_snapshot).await;
            self.refresh_topology_metrics().await;
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }

        #[cfg(target_arch = "wasm32")]
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
        #[cfg(not(target_arch = "wasm32"))]
        let needs_write_lock =
            self.persistence.is_some() || self.options.effective_max_retained_bytes().is_some();
        #[cfg(target_arch = "wasm32")]
        let needs_write_lock = self.options.effective_max_retained_bytes().is_some();
        let _write_guard = if needs_write_lock {
            Some(self.inner.write_lock.lock().await)
        } else {
            None
        };

        self.check_writable()?;

        let current_bytes = self.metrics.retained_bytes();
        if let Some(limit) = self.options.effective_max_retained_bytes()
            && current_bytes.saturating_add(bytes.len() as u64) > limit
        {
            return Err(self.retained_limit_error(current_bytes, bytes.len() as u64));
        }
        let shard_hex = shard_hex_from_key(key);
        let shard_map = ensure_shard_map(&self.inner.stream_records, stream_name);
        let records_arc = shard_map
            .entry(shard_hex.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())))
            .value()
            .clone();
        drop(shard_map);
        let mut records = records_arc.write().await;
        let previous = records.insert(key.to_string(), bytes.clone());
        if let Some(previous_bytes) = previous.as_ref() {
            self.metrics.remove_retained(previous_bytes.len() as u64, 1);
        }
        self.metrics.add_retained(bytes.len() as u64, 1);
        drop(records);
        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            self.rollback_record_change(AppliedRecordChange {
                records_arc,
                key: key.to_string(),
                previous,
                new_bytes_len: bytes.len() as u64,
            })
            .await;
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }
        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;
        Ok(())
    }

    /// Stores multiple records in a single batch.
    pub async fn put_records_batch<R: Serialize>(
        &self,
        stream_name: &str,
        batch: &[(String, R)],
    ) -> Result<(), KinesisErrorResponse> {
        #[cfg(not(target_arch = "wasm32"))]
        let needs_write_lock =
            self.persistence.is_some() || self.options.effective_max_retained_bytes().is_some();
        #[cfg(target_arch = "wasm32")]
        let needs_write_lock = self.options.effective_max_retained_bytes().is_some();
        let _write_guard = if needs_write_lock {
            Some(self.inner.write_lock.lock().await)
        } else {
            None
        };

        self.check_writable()?;
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

        let additional_bytes: u64 = pending.iter().map(|(_, _, bytes)| bytes.len() as u64).sum();
        let current_bytes = self.metrics.retained_bytes();
        if let Some(limit) = self.options.effective_max_retained_bytes()
            && current_bytes.saturating_add(additional_bytes) > limit
        {
            return Err(self.retained_limit_error(current_bytes, additional_bytes));
        }

        // Phase 2: insert records under per-shard write locks only.
        #[cfg(not(target_arch = "wasm32"))]
        let mut applied = Vec::with_capacity(pending.len());
        for (records_arc, key, bytes) in pending {
            let mut records = records_arc.write().await;
            let previous = records.insert(key.clone(), bytes.clone());
            if let Some(previous_bytes) = previous.as_ref() {
                self.metrics.remove_retained(previous_bytes.len() as u64, 1);
            }
            self.metrics.add_retained(bytes.len() as u64, 1);
            drop(records);
            #[cfg(not(target_arch = "wasm32"))]
            applied.push(AppliedRecordChange {
                records_arc,
                key,
                previous,
                new_bytes_len: bytes.len() as u64,
            });
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            self.rollback_record_changes(&applied).await;
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }
        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;
        Ok(())
    }

    /// Deletes records whose sequence-number–embedded timestamp is older than
    /// the retention cutoff. Returns the number of records removed.
    pub async fn delete_expired_records(&self, stream_name: &str, retention_hours: u32) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        if self.durable_state_requested() && self.check_persistent_mutation_allowed().is_err() {
            return 0;
        }

        #[cfg(not(target_arch = "wasm32"))]
        let _write_guard = if self.durable_state_requested() {
            Some(self.inner.write_lock.lock().await)
        } else {
            None
        };

        let now = crate::util::current_time_ms();
        let cutoff_time = now - (retention_hours as u64 * 60 * 60 * 1000);

        // Phase 1: collect Arc refs while holding the DashMap Ref.
        let shard_arcs: Vec<_> = match self.inner.stream_records.get(stream_name) {
            Some(shard_map) => shard_map.iter().map(|e| e.value().clone()).collect(),
            None => return 0,
        }; // DashMap Ref dropped here.

        // Phase 2: scan and delete under per-shard locks only.
        let mut applied = Vec::new();
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
                        #[cfg(not(target_arch = "wasm32"))]
                        applied.push(AppliedDeleteChange {
                            records_arc: Arc::clone(&records_arc),
                            key: key.clone(),
                            deleted_bytes: bytes,
                        });
                        #[cfg(target_arch = "wasm32")]
                        applied.push((bytes.len() as u64, 1usize));
                    }
                }
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        let (bytes_deleted, total_deleted) = applied.iter().fold((0u64, 0u64), |acc, change| {
            (
                acc.0 + change.deleted_bytes.len() as u64,
                acc.1.saturating_add(1),
            )
        });
        #[cfg(target_arch = "wasm32")]
        let (bytes_deleted, total_deleted) = applied.iter().fold((0u64, 0u64), |acc, change| {
            (acc.0 + change.0, acc.1.saturating_add(change.1 as u64))
        });

        if total_deleted > 0 {
            self.metrics.remove_retained(bytes_deleted, total_deleted);
            #[cfg(not(target_arch = "wasm32"))]
            if let Err(err) = self.persist_current_state_result().await {
                self.rollback_delete_changes(&applied).await;
                tracing::warn!("{err}");
                return 0;
            }
            #[cfg(target_arch = "wasm32")]
            self.persist_current_state().await;
        }
        total_deleted as usize
    }

    /// Deletes records by their shard keys within a stream.
    pub async fn delete_record_keys(&self, stream_name: &str, keys: &[String]) {
        #[cfg(not(target_arch = "wasm32"))]
        if self.durable_state_requested() && self.check_persistent_mutation_allowed().is_err() {
            return;
        }

        #[cfg(not(target_arch = "wasm32"))]
        let _write_guard = if self.durable_state_requested() {
            Some(self.inner.write_lock.lock().await)
        } else {
            None
        };

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
        #[cfg(not(target_arch = "wasm32"))]
        let mut applied = Vec::new();
        #[cfg(target_arch = "wasm32")]
        let mut removed_count = 0u64;
        #[cfg(target_arch = "wasm32")]
        let mut bytes_deleted = 0u64;
        for (records_arc, key) in pending {
            let mut records = records_arc.write().await;
            if let Some(bytes) = records.remove(&key) {
                #[cfg(not(target_arch = "wasm32"))]
                applied.push(AppliedDeleteChange {
                    records_arc: Arc::clone(&records_arc),
                    key,
                    deleted_bytes: bytes,
                });
                #[cfg(target_arch = "wasm32")]
                {
                    bytes_deleted += bytes.len() as u64;
                    removed_count += 1;
                }
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        let (bytes_deleted, removed_count) = applied.iter().fold((0u64, 0u64), |acc, change| {
            (
                acc.0 + change.deleted_bytes.len() as u64,
                acc.1.saturating_add(1),
            )
        });
        if removed_count > 0 {
            self.metrics.remove_retained(bytes_deleted, removed_count);
            #[cfg(not(target_arch = "wasm32"))]
            if let Err(err) = self.persist_current_state_result().await {
                self.rollback_delete_changes(&applied).await;
                tracing::warn!("{err}");
            }
            #[cfg(target_arch = "wasm32")]
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
    pub async fn put_consumer(
        &self,
        consumer_arn: &str,
        consumer: Consumer,
    ) -> Result<(), KinesisErrorResponse> {
        self.put_consumer_with_transition(consumer_arn, consumer, TransitionMutation::None)
            .await
    }

    pub(crate) async fn put_consumer_with_transition(
        &self,
        consumer_arn: &str,
        consumer: Consumer,
        transition: TransitionMutation,
    ) -> Result<(), KinesisErrorResponse> {
        self.put_consumer_with_transition_internal(consumer_arn, consumer, transition, false)
            .await
    }

    async fn put_consumer_with_transition_internal(
        &self,
        consumer_arn: &str,
        consumer: Consumer,
        transition: TransitionMutation,
        skip_write_lock: bool,
    ) -> Result<(), KinesisErrorResponse> {
        let _write_guard = self
            .lock_transition_mutation(&transition, skip_write_lock)
            .await?;
        let bytes = serde_json::to_vec(&consumer).unwrap();
        let previous = self.inner.consumers.insert(consumer_arn.to_string(), bytes);
        #[cfg(not(target_arch = "wasm32"))]
        let transition_snapshot = self.capture_transition_state(&transition).await;
        self.apply_transition_mutation(transition).await;

        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            if let Some(previous) = previous {
                self.inner
                    .consumers
                    .insert(consumer_arn.to_string(), previous);
            } else {
                self.inner.consumers.remove(consumer_arn);
            }
            self.restore_transition_state(transition_snapshot).await;
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }

        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;

        Ok(())
    }

    /// Returns the consumer with the given ARN, or `None` if not found.
    pub async fn get_consumer(&self, consumer_arn: &str) -> Option<Consumer> {
        self.inner
            .consumers
            .get(consumer_arn)
            .map(|entry| serde_json::from_slice(entry.value()).unwrap())
    }

    /// Deletes the consumer with the given ARN.
    pub async fn delete_consumer(&self, consumer_arn: &str) -> Result<(), KinesisErrorResponse> {
        self.delete_consumer_with_transition(consumer_arn, TransitionMutation::None)
            .await
    }

    pub(crate) async fn delete_consumer_with_transition(
        &self,
        consumer_arn: &str,
        transition: TransitionMutation,
    ) -> Result<(), KinesisErrorResponse> {
        self.delete_consumer_with_transition_internal(consumer_arn, transition, false)
            .await
    }

    async fn delete_consumer_with_transition_internal(
        &self,
        consumer_arn: &str,
        transition: TransitionMutation,
        skip_write_lock: bool,
    ) -> Result<(), KinesisErrorResponse> {
        let _write_guard = self
            .lock_transition_mutation(&transition, skip_write_lock)
            .await?;
        let previous = self
            .inner
            .consumers
            .remove(consumer_arn)
            .map(|(_, value)| value);
        #[cfg(not(target_arch = "wasm32"))]
        let transition_snapshot = self.capture_transition_state(&transition).await;
        self.apply_transition_mutation(transition).await;

        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            if let Some(previous) = previous {
                self.inner
                    .consumers
                    .insert(consumer_arn.to_string(), previous);
            }
            self.restore_transition_state(transition_snapshot).await;
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }

        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;

        Ok(())
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
    pub async fn put_policy(
        &self,
        resource_arn: &str,
        policy: &str,
    ) -> Result<(), KinesisErrorResponse> {
        #[cfg(not(target_arch = "wasm32"))]
        let _write_guard = if self.durable_state_requested() {
            self.check_persistent_mutation_allowed()?;
            Some(self.inner.write_lock.lock().await)
        } else {
            None
        };
        let previous = self
            .inner
            .policies
            .insert(resource_arn.to_string(), policy.to_string());

        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            if let Some(previous) = previous {
                self.inner
                    .policies
                    .insert(resource_arn.to_string(), previous);
            } else {
                self.inner.policies.remove(resource_arn);
            }
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }

        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;

        Ok(())
    }

    /// Returns the resource policy for the given ARN, or `None` if none is set.
    pub async fn get_policy(&self, resource_arn: &str) -> Option<String> {
        self.inner
            .policies
            .get(resource_arn)
            .map(|entry| entry.value().clone())
    }

    /// Deletes the resource policy for the given ARN.
    pub async fn delete_policy(&self, resource_arn: &str) -> Result<(), KinesisErrorResponse> {
        #[cfg(not(target_arch = "wasm32"))]
        let _write_guard = if self.durable_state_requested() {
            self.check_persistent_mutation_allowed()?;
            Some(self.inner.write_lock.lock().await)
        } else {
            None
        };
        let previous = self
            .inner
            .policies
            .remove(resource_arn)
            .map(|(_, value)| value);

        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            if let Some(previous) = previous {
                self.inner
                    .policies
                    .insert(resource_arn.to_string(), previous);
            }
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }

        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;

        Ok(())
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
    pub async fn put_resource_tags(
        &self,
        resource_arn: &str,
        tags: &BTreeMap<String, String>,
    ) -> Result<(), KinesisErrorResponse> {
        #[cfg(not(target_arch = "wasm32"))]
        let _write_guard = if self.durable_state_requested() {
            self.check_persistent_mutation_allowed()?;
            Some(self.inner.write_lock.lock().await)
        } else {
            None
        };
        let previous = self
            .inner
            .resource_tags
            .insert(resource_arn.to_string(), tags.clone());

        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            if let Some(previous) = previous {
                self.inner
                    .resource_tags
                    .insert(resource_arn.to_string(), previous);
            } else {
                self.inner.resource_tags.remove(resource_arn);
            }
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }

        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;

        Ok(())
    }

    // --- Account settings operations ---

    /// Returns the account-level settings object, or an empty JSON object if none are set.
    pub async fn get_account_settings(&self) -> Value {
        self.inner.account_settings.read().await.clone()
    }

    /// Stores the account-level settings object.
    pub async fn put_account_settings(&self, settings: &Value) -> Result<(), KinesisErrorResponse> {
        #[cfg(not(target_arch = "wasm32"))]
        let _write_guard = if self.durable_state_requested() {
            self.check_persistent_mutation_allowed()?;
            Some(self.inner.write_lock.lock().await)
        } else {
            None
        };
        let previous = self.inner.account_settings.read().await.clone();
        let mut guard = self.inner.account_settings.write().await;
        *guard = settings.clone();
        drop(guard);

        #[cfg(not(target_arch = "wasm32"))]
        if let Err(err) = self.persist_current_state_result().await {
            let mut guard = self.inner.account_settings.write().await;
            *guard = previous;
            return Err(KinesisErrorResponse::server_error(None, Some(&err)));
        }

        #[cfg(target_arch = "wasm32")]
        self.persist_current_state().await;

        Ok(())
    }

    /// Probes store health. Always succeeds for the in-memory store.
    ///
    /// # Errors
    ///
    /// Returns [`StoreHealthError`] if the store is not ready. With the current
    /// in-memory implementation this never happens, but the signature is kept
    /// for API compatibility.
    pub fn check_ready(&self) -> Result<(), StoreHealthError> {
        if let Some(detail) = self.availability_block_reason() {
            return Err(StoreHealthError::ReadFailed(detail));
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

    #[cfg(not(target_arch = "wasm32"))]
    async fn rollback_record_change(&self, change: AppliedRecordChange) {
        let mut records = change.records_arc.write().await;
        match change.previous {
            Some(previous) => {
                records.insert(change.key, previous.clone());
                self.metrics.remove_retained(change.new_bytes_len, 1);
                self.metrics.add_retained(previous.len() as u64, 1);
            }
            None => {
                records.remove(&change.key);
                self.metrics.remove_retained(change.new_bytes_len, 1);
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn rollback_record_changes(&self, changes: &[AppliedRecordChange]) {
        for change in changes.iter().rev() {
            self.rollback_record_change(AppliedRecordChange {
                records_arc: Arc::clone(&change.records_arc),
                key: change.key.clone(),
                previous: change.previous.clone(),
                new_bytes_len: change.new_bytes_len,
            })
            .await;
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn rollback_delete_changes(&self, changes: &[AppliedDeleteChange]) {
        for change in changes.iter().rev() {
            let mut records = change.records_arc.write().await;
            records.insert(change.key.clone(), change.deleted_bytes.clone());
            self.metrics
                .add_retained(change.deleted_bytes.len() as u64, 1);
        }
    }

    pub(crate) fn schedule_transition(&self, transition: PendingTransition) {
        let store = self.clone();
        crate::runtime::spawn_background(async move {
            let delay = transition.ready_at_ms().saturating_sub(current_time_ms());
            if delay > 0 {
                crate::runtime::sleep_ms(delay).await;
            }
            if let Err(err) = store.finish_transition(transition).await {
                #[cfg(not(target_arch = "wasm32"))]
                store.mark_unhealthy(err.clone());
                tracing::warn!("{err}");
            }
        });
    }

    async fn finish_transition(&self, transition: PendingTransition) -> Result<(), String> {
        let key = transition.key();
        let _write_guard = self.inner.write_lock.lock().await;
        let current_transition = self
            .inner
            .pending_transitions
            .read()
            .await
            .get(&key)
            .cloned();
        if current_transition.as_ref() != Some(&transition) {
            return Ok(());
        }

        match transition {
            PendingTransition::CreateStream {
                stream_name,
                shards,
                ..
            } => self
                .update_stream_with_transition_internal(
                    &stream_name,
                    TransitionMutation::Remove(key),
                    true,
                    move |stream| {
                        if stream.stream_status != StreamStatus::Creating {
                            return Err(KinesisErrorResponse::server_error(
                                None,
                                Some("recovered CreateStream transition found stream in unexpected state"),
                            ));
                        }
                        stream.stream_status = StreamStatus::Active;
                        stream.shards = shards;
                        Ok(())
                    },
                )
                .await
                .map_err(|err| err.to_string()),
            PendingTransition::DeleteStream { stream_name, .. } => {
                let stream = self
                    .get_stream(&stream_name)
                    .await
                    .map_err(|err| err.to_string())?;
                if stream.stream_status != StreamStatus::Deleting {
                    return Err(format!(
                        "recovered DeleteStream transition found {} in unexpected state {}",
                        stream_name, stream.stream_status
                    ));
                }
                self.delete_stream_with_transition_internal(
                    &stream_name,
                    TransitionMutation::Remove(key),
                    true,
                )
                .await
                .map_err(|err| err.to_string())?;
                Ok(())
            }
            PendingTransition::RegisterConsumer { consumer_arn, .. } => {
                let mut consumer = self
                    .get_consumer(&consumer_arn)
                    .await
                    .ok_or_else(|| {
                        format!(
                            "recovered RegisterStreamConsumer transition missing {consumer_arn}"
                        )
                    })?;
                if consumer.consumer_status != ConsumerStatus::Creating {
                    return Err(format!(
                        "recovered RegisterStreamConsumer transition found {} in unexpected state {}",
                        consumer_arn, consumer.consumer_status
                    ));
                }
                consumer.consumer_status = ConsumerStatus::Active;
                self.put_consumer_with_transition_internal(
                    &consumer_arn,
                    consumer,
                    TransitionMutation::Remove(key),
                    true,
                )
                .await
                .map_err(|err| err.to_string())?;
                Ok(())
            }
            PendingTransition::DeregisterConsumer { consumer_arn, .. } => {
                let consumer = self
                    .get_consumer(&consumer_arn)
                    .await
                    .ok_or_else(|| {
                        format!(
                            "recovered DeregisterStreamConsumer transition missing {consumer_arn}"
                        )
                    })?;
                if consumer.consumer_status != ConsumerStatus::Deleting {
                    return Err(format!(
                        "recovered DeregisterStreamConsumer transition found {} in unexpected state {}",
                        consumer_arn, consumer.consumer_status
                    ));
                }
                self.delete_consumer_with_transition_internal(
                    &consumer_arn,
                    TransitionMutation::Remove(key),
                    true,
                )
                .await
                .map_err(|err| err.to_string())?;
                Ok(())
            }
            PendingTransition::UpdateShardCount {
                stream_name,
                target_shard_count,
                ..
            } => {
                let closed_shard_ids = self
                    .update_stream_with_transition_internal(
                        &stream_name,
                        TransitionMutation::Remove(key),
                        true,
                        move |stream| {
                            if stream.stream_status != StreamStatus::Updating {
                                return Err(KinesisErrorResponse::server_error(
                                    None,
                                    Some("recovered UpdateShardCount transition found stream in unexpected state"),
                                ));
                            }

                            let now = current_time_ms();
                            let open_indices: Vec<usize> = stream
                                .shards
                                .iter()
                                .enumerate()
                                .filter(|(_, s)| {
                                    s.sequence_number_range.ending_sequence_number.is_none()
                                })
                                .map(|(i, _)| i)
                                .collect();
                            let closed_shard_ids = open_indices
                                .iter()
                                .map(|&ix| stream.shards[ix].shard_id.clone())
                                .collect::<Vec<_>>();

                            for &ix in &open_indices {
                                let create_time = sequence::parse_sequence(
                                    &stream.shards[ix].sequence_number_range.starting_sequence_number,
                                )
                                .map(|s| s.shard_create_time)
                                .unwrap_or(0);

                                stream.shards[ix].sequence_number_range.ending_sequence_number =
                                    Some(sequence::stringify_sequence(&sequence::SeqObj {
                                        shard_create_time: create_time,
                                        shard_ix: ix as i64,
                                        seq_ix: Some(sequence::MAX_SEQ_IX),
                                        seq_time: Some(now),
                                        byte1: None,
                                        seq_rand: None,
                                        version: 2,
                                    }));
                            }

                            let pow_128 = BigUint::one() << 128;
                            let shard_hash = &pow_128 / BigUint::from(target_shard_count);

                            for i in 0..target_shard_count {
                                let new_ix = stream.shards.len() as i64;
                                let start: BigUint = &shard_hash * BigUint::from(i);
                                let end: BigUint = if i < target_shard_count - 1 {
                                    &shard_hash * BigUint::from(i + 1) - BigUint::one()
                                } else {
                                    &pow_128 - BigUint::one()
                                };

                                stream.shards.push(Shard {
                                    shard_id: sequence::shard_id_name(new_ix),
                                    parent_shard_id: None,
                                    adjacent_parent_shard_id: None,
                                    hash_key_range: HashKeyRange::new(
                                        start.to_string(),
                                        end.to_string(),
                                    ),
                                    sequence_number_range: SequenceNumberRange {
                                        starting_sequence_number: sequence::stringify_sequence(
                                            &sequence::SeqObj {
                                                shard_create_time: now + 1000,
                                                shard_ix: new_ix,
                                                seq_ix: None,
                                                seq_time: None,
                                                byte1: None,
                                                seq_rand: None,
                                                version: 2,
                                            },
                                        ),
                                        ending_sequence_number: None,
                                    },
                                });
                            }

                            stream.stream_status = StreamStatus::Active;
                            Ok(closed_shard_ids)
                        },
                    )
                    .await
                    .map_err(|err| err.to_string())?;
                self.clear_throughput_windows_for_shards(&stream_name, &closed_shard_ids);
                Ok(())
            }
            PendingTransition::SplitShard {
                stream_name,
                shard_to_split,
                new_starting_hash_key,
                ..
            } => {
                let cleared_shard_id = self
                    .update_stream_with_transition_internal(
                        &stream_name,
                        TransitionMutation::Remove(key),
                        true,
                        move |stream| {
                            if stream.stream_status != StreamStatus::Updating {
                                return Err(KinesisErrorResponse::server_error(
                                    None,
                                    Some("recovered SplitShard transition found stream in unexpected state"),
                                ));
                            }

                            let (shard_id, shard_ix) = sequence::resolve_shard_id(&shard_to_split)
                                .map_err(|_| {
                                    KinesisErrorResponse::server_error(
                                        None,
                                        Some("recovered SplitShard transition has invalid shard id"),
                                    )
                                })?;
                            if shard_ix >= stream.shards.len() as i64 {
                                return Err(KinesisErrorResponse::server_error(
                                    None,
                                    Some("recovered SplitShard transition points to missing shard"),
                                ));
                            }

                            let hash_key = new_starting_hash_key.parse::<u128>().map_err(|_| {
                                KinesisErrorResponse::server_error(
                                    None,
                                    Some("recovered SplitShard transition has invalid hash key"),
                                )
                            })?;
                            let shard_start = stream.shards[shard_ix as usize]
                                .hash_key_range
                                .start_u128();
                            let shard_end = stream.shards[shard_ix as usize]
                                .hash_key_range
                                .end_u128();

                            let now = current_time_ms();
                            stream.stream_status = StreamStatus::Active;

                            let shard = &mut stream.shards[shard_ix as usize];
                            let create_time = sequence::parse_sequence(
                                &shard.sequence_number_range.starting_sequence_number,
                            )
                            .map(|s| s.shard_create_time)
                            .unwrap_or(0);

                            shard.sequence_number_range.ending_sequence_number =
                                Some(sequence::stringify_sequence(&sequence::SeqObj {
                                    shard_create_time: create_time,
                                    shard_ix,
                                    seq_ix: Some(sequence::MAX_SEQ_IX),
                                    seq_time: Some(now),
                                    byte1: None,
                                    seq_rand: None,
                                    version: 2,
                                }));

                            let new_ix1 = stream.shards.len() as i64;
                            stream.shards.push(Shard {
                                parent_shard_id: Some(shard_id.clone()),
                                adjacent_parent_shard_id: None,
                                hash_key_range: HashKeyRange::new(
                                    shard_start.to_string(),
                                    (hash_key - 1).to_string(),
                                ),
                                sequence_number_range: SequenceNumberRange {
                                    starting_sequence_number: sequence::stringify_sequence(
                                        &sequence::SeqObj {
                                            shard_create_time: now + 1000,
                                            shard_ix: new_ix1,
                                            seq_ix: None,
                                            seq_time: None,
                                            byte1: None,
                                            seq_rand: None,
                                            version: 2,
                                        },
                                    ),
                                    ending_sequence_number: None,
                                },
                                shard_id: sequence::shard_id_name(new_ix1),
                            });

                            let new_ix2 = stream.shards.len() as i64;
                            stream.shards.push(Shard {
                                parent_shard_id: Some(shard_id),
                                adjacent_parent_shard_id: None,
                                hash_key_range: HashKeyRange::new(
                                    hash_key.to_string(),
                                    shard_end.to_string(),
                                ),
                                sequence_number_range: SequenceNumberRange {
                                    starting_sequence_number: sequence::stringify_sequence(
                                        &sequence::SeqObj {
                                            shard_create_time: now + 1000,
                                            shard_ix: new_ix2,
                                            seq_ix: None,
                                            seq_time: None,
                                            byte1: None,
                                            seq_rand: None,
                                            version: 2,
                                        },
                                    ),
                                    ending_sequence_number: None,
                                },
                                shard_id: sequence::shard_id_name(new_ix2),
                            });

                            Ok(shard_to_split)
                        },
                    )
                    .await
                    .map_err(|err| err.to_string())?;
                self.clear_throughput_windows_for_shards(&stream_name, [cleared_shard_id.as_str()]);
                Ok(())
            }
            PendingTransition::MergeShards {
                stream_name,
                shard_to_merge,
                adjacent_shard_to_merge,
                ..
            } => {
                let cleared_shard_ids = self
                    .update_stream_with_transition_internal(
                        &stream_name,
                        TransitionMutation::Remove(key),
                        true,
                        move |stream| {
                            if stream.stream_status != StreamStatus::Updating {
                                return Err(KinesisErrorResponse::server_error(
                                    None,
                                    Some("recovered MergeShards transition found stream in unexpected state"),
                                ));
                            }

                            let (shard_id0, shard_ix0) =
                                sequence::resolve_shard_id(&shard_to_merge).map_err(|_| {
                                    KinesisErrorResponse::server_error(
                                        None,
                                        Some("recovered MergeShards transition has invalid shard id"),
                                    )
                                })?;
                            let (shard_id1, shard_ix1) =
                                sequence::resolve_shard_id(&adjacent_shard_to_merge).map_err(|_| {
                                    KinesisErrorResponse::server_error(
                                        None,
                                        Some("recovered MergeShards transition has invalid adjacent shard id"),
                                    )
                                })?;

                            let now = current_time_ms();
                            stream.stream_status = StreamStatus::Active;

                            for &ix in &[shard_ix0, shard_ix1] {
                                let shard = &mut stream.shards[ix as usize];
                                let create_time = sequence::parse_sequence(
                                    &shard.sequence_number_range.starting_sequence_number,
                                )
                                .map(|s| s.shard_create_time)
                                .unwrap_or(0);

                                shard.sequence_number_range.ending_sequence_number =
                                    Some(sequence::stringify_sequence(&sequence::SeqObj {
                                        shard_create_time: create_time,
                                        shard_ix: ix,
                                        seq_ix: Some(sequence::MAX_SEQ_IX),
                                        seq_time: Some(now),
                                        byte1: None,
                                        seq_rand: None,
                                        version: 2,
                                    }));
                            }

                            let new_ix = stream.shards.len() as i64;
                            let starting_hash = stream.shards[shard_ix0 as usize]
                                .hash_key_range
                                .starting_hash_key
                                .clone();
                            let ending_hash = stream.shards[shard_ix1 as usize]
                                .hash_key_range
                                .ending_hash_key
                                .clone();

                            stream.shards.push(Shard {
                                parent_shard_id: Some(shard_id0),
                                adjacent_parent_shard_id: Some(shard_id1),
                                hash_key_range: HashKeyRange::new(starting_hash, ending_hash),
                                sequence_number_range: SequenceNumberRange {
                                    starting_sequence_number: sequence::stringify_sequence(
                                        &sequence::SeqObj {
                                            shard_create_time: now + 1000,
                                            shard_ix: new_ix,
                                            seq_ix: None,
                                            seq_time: None,
                                            byte1: None,
                                            seq_rand: None,
                                            version: 2,
                                        },
                                    ),
                                    ending_sequence_number: None,
                                },
                                shard_id: sequence::shard_id_name(new_ix),
                            });

                            Ok([shard_to_merge, adjacent_shard_to_merge])
                        },
                    )
                    .await
                    .map_err(|err| err.to_string())?;
                self.clear_throughput_windows_for_shards(&stream_name, cleared_shard_ids);
                Ok(())
            }
            PendingTransition::StartStreamEncryption { stream_name, .. } => self
                .update_stream_with_transition_internal(
                    &stream_name,
                    TransitionMutation::Remove(key),
                    true,
                    |stream| {
                        if stream.stream_status != StreamStatus::Updating {
                            return Err(KinesisErrorResponse::server_error(
                                None,
                                Some("recovered StartStreamEncryption transition found stream in unexpected state"),
                            ));
                        }
                        stream.stream_status = StreamStatus::Active;
                        stream.encryption_type = EncryptionType::Kms;
                        Ok(())
                    },
                )
                .await
                .map_err(|err| err.to_string()),
            PendingTransition::StopStreamEncryption { stream_name, .. } => self
                .update_stream_with_transition_internal(
                    &stream_name,
                    TransitionMutation::Remove(key),
                    true,
                    |stream| {
                        if stream.stream_status != StreamStatus::Updating {
                            return Err(KinesisErrorResponse::server_error(
                                None,
                                Some("recovered StopStreamEncryption transition found stream in unexpected state"),
                            ));
                        }
                        stream.stream_status = StreamStatus::Active;
                        stream.encryption_type = EncryptionType::None;
                        stream.key_id = None;
                        Ok(())
                    },
                )
                .await
                .map_err(|err| err.to_string()),
        }
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
    ) -> Result<ThroughputReservation, KinesisErrorResponse> {
        if !self.options.enforce_limits {
            return Ok(ThroughputReservation::disabled());
        }

        let key = throughput_window_key(stream_name, shard_id);
        let window = self
            .inner
            .throughput_windows
            .entry(key.clone())
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
        Ok(ThroughputReservation {
            key: Some(key),
            window_start_ms: window.window_start_ms,
            bytes,
        })
    }

    /// Refunds a previously charged shard-throughput reservation.
    ///
    /// Refunds are applied only if the original throughput window is still
    /// current, preventing a rollback from mutating a newer second's quota.
    pub async fn refund_shard_throughput(&self, reservation: ThroughputReservation) {
        let Some(key) = reservation.key else {
            return;
        };

        let Some(window) = self
            .inner
            .throughput_windows
            .get(&key)
            .map(|entry| Arc::clone(entry.value()))
        else {
            return;
        };

        let mut window = window.lock().await;
        if window.window_start_ms != reservation.window_start_ms {
            return;
        }

        window.bytes = window.bytes.saturating_sub(reservation.bytes);
        window.records = window.records.saturating_sub(1);
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
    build_shard_seq_from_counters(&vec![1; stream.shards.len()])
}

fn build_shard_seq_from_counters(counters: &[u64]) -> Vec<ShardSeqState> {
    counters
        .iter()
        .copied()
        .map(|counter| ShardSeqState {
            counter: AtomicU64::new(counter),
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
