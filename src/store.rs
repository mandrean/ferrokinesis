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
use crate::sequence;
use crate::types::{Consumer, StoredRecord, Stream, StreamStatus};
use crate::util::current_time_ms;
use dashmap::DashMap;
use num_bigint::BigUint;
use num_traits::Zero;
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

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
            aws_account_id: "000000000000".to_string(),
            aws_region: "us-east-1".to_string(),
        }
    }
}

/// Per-shard sequence counter for lock-free sequence number generation.
struct ShardSeqState {
    /// Atomic counter — `fetch_add(1)` returns the next seq_ix to use.
    counter: AtomicU64,
    /// Shard creation time in milliseconds (from starting_sequence_number).
    _create_time_ms: u64,
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
    /// Optional capture writer for recording PutRecord/PutRecords calls.
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
        Self::with_capture(options, None)
    }

    /// Creates a new store with an optional [`crate::capture::CaptureWriter`]
    /// to record PutRecord/PutRecords calls to an NDJSON file.
    ///
    /// Strips non-digit characters from `options.aws_account_id` and warns if
    /// the result is not exactly 12 digits.
    pub fn with_capture(
        options: StoreOptions,
        capture_writer: Option<crate::capture::CaptureWriter>,
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

        Self {
            options,
            aws_account_id,
            aws_region,
            inner: Arc::new(StoreInner {
                streams: DashMap::new(),
                stream_records: DashMap::new(),
                consumers: DashMap::new(),
                policies: DashMap::new(),
                resource_tags: DashMap::new(),
                account_settings: RwLock::new(Value::Object(Default::default())),
            }),
            capture_writer,
        }
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
    }

    /// Deletes a stream and all of its records.
    pub async fn delete_stream(&self, name: &str) {
        self.inner.streams.remove(name);
        self.inner.stream_records.remove(name);
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
        Ok(result)
    }

    /// Executes a closure with write access to all streams simultaneously.
    ///
    /// The closure receives a mutable `BTreeMap` of all streams, the current
    /// `StoreOptions`, the account ID, and the region. All changes to the map
    /// are written back atomically when the closure returns.
    pub async fn with_streams_write<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut BTreeMap<String, Stream>, &StoreOptions, &str, &str) -> R,
    {
        // Snapshot all streams.
        let mut snapshot: BTreeMap<String, Stream> = BTreeMap::new();
        for entry in self.inner.streams.iter() {
            let stream = entry.value().stream.read().await.clone();
            snapshot.insert(entry.key().clone(), stream);
        }

        let result = f(
            &mut snapshot,
            &self.options,
            &self.aws_account_id,
            &self.aws_region,
        );

        // Apply deletions.
        let existing_keys: Vec<String> =
            self.inner.streams.iter().map(|e| e.key().clone()).collect();
        for key in &existing_keys {
            if !snapshot.contains_key(key) {
                self.inner.streams.remove(key);
            }
        }

        // Apply updates and inserts.
        for (name, stream) in snapshot {
            if let Some(entry) = self.inner.streams.get(&name) {
                let mut guard = entry.stream.write().await;
                *guard = stream;
            } else {
                let shard_seq = build_shard_seq(&stream);
                let entry = Arc::new(StreamEntry {
                    stream: RwLock::new(stream),
                    shard_seq: RwLock::new(shard_seq),
                });
                self.inner.streams.insert(name, entry);
            }
        }

        result
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
    pub async fn put_record<R: Serialize>(&self, stream_name: &str, key: &str, record: &R) {
        let bytes = postcard::to_allocvec(record).unwrap();
        let shard_hex = shard_hex_from_key(key);
        let shard_map = ensure_shard_map(&self.inner.stream_records, stream_name);
        let records_arc = shard_map
            .entry(shard_hex.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())))
            .value()
            .clone();
        drop(shard_map);
        let mut records = records_arc.write().await;
        records.insert(key.to_string(), bytes);
    }

    /// Stores multiple records in a single batch.
    pub async fn put_records_batch<R: Serialize>(&self, stream_name: &str, batch: &[(String, R)]) {
        // Phase 1: collect Arc refs and serialized bytes while holding the DashMap ref.
        let pending: Vec<_> = {
            let shard_map = ensure_shard_map(&self.inner.stream_records, stream_name);
            batch
                .iter()
                .map(|(key, record)| {
                    let bytes = postcard::to_allocvec(record).unwrap();
                    let shard_hex = shard_hex_from_key(key);
                    let records_arc = shard_map
                        .entry(shard_hex.to_string())
                        .or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())))
                        .value()
                        .clone();
                    (records_arc, key.clone(), bytes)
                })
                .collect()
        }; // shard_map Ref dropped here — no DashMap lock held across await.

        // Phase 2: insert records under per-shard write locks only.
        for (records_arc, key, bytes) in pending {
            let mut records = records_arc.write().await;
            records.insert(key, bytes);
        }
    }

    /// Deletes records whose sequence-number–embedded timestamp is older than
    /// the retention cutoff. Returns the number of records removed.
    pub async fn delete_expired_records(&self, stream_name: &str, retention_hours: u32) -> usize {
        let now = crate::util::current_time_ms();
        let cutoff_time = now - (retention_hours as u64 * 60 * 60 * 1000);

        let shard_map = match self.inner.stream_records.get(stream_name) {
            Some(m) => m,
            None => return 0,
        };

        let mut total_deleted = 0;
        for shard_entry in shard_map.iter() {
            let keys_to_delete: Vec<String> = {
                let records = shard_entry.value().read().await;
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
                let mut records = shard_entry.value().write().await;
                for key in &keys_to_delete {
                    records.remove(key);
                }
                total_deleted += keys_to_delete.len();
            }
        }

        total_deleted
    }

    /// Deletes records by their shard keys within a stream.
    pub async fn delete_record_keys(&self, stream_name: &str, keys: &[String]) {
        let shard_map = match self.inner.stream_records.get(stream_name) {
            Some(m) => m,
            None => return,
        };
        for key in keys {
            let shard_hex = shard_hex_from_key(key);
            if let Some(records_arc) = shard_map.get(shard_hex) {
                let mut records = records_arc.value().write().await;
                records.remove(key);
            }
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
    }

    /// Probes store health. Always succeeds for the in-memory store.
    ///
    /// # Errors
    ///
    /// Returns [`StoreHealthError`] if the store is not ready. With the current
    /// in-memory implementation this never happens, but the signature is kept
    /// for API compatibility.
    pub fn check_ready(&self) -> Result<(), StoreHealthError> {
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
        hash_key: &BigUint,
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
            seq_ix: Some(BigUint::from(current_seq_ix)),
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
        hash_keys: &[BigUint],
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
                seq_ix: Some(BigUint::from(current_seq_ix)),
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
}

// --- Free functions ---

/// Build per-shard sequence state from a stream's shard list.
fn build_shard_seq(stream: &Stream) -> Vec<ShardSeqState> {
    stream
        .shards
        .iter()
        .map(|shard| {
            let create_time_ms =
                sequence::parse_sequence(&shard.sequence_number_range.starting_sequence_number)
                    .map(|s| s.shard_create_time)
                    .unwrap_or(0);
            ShardSeqState {
                // Skip index 0: the shard's starting_sequence_number is generated with
                // seq_ix=0. If the first PutRecord lands in the same millisecond as shard
                // creation, seq_time would also match, so seq_ix must be ≥1 to guarantee
                // every record's sequence number is strictly greater than the starting one.
                counter: AtomicU64::new(1),
                _create_time_ms: create_time_ms,
            }
        })
        .collect()
}

/// Extend shard_seq if the stream has more shards than tracked counters.
async fn sync_shard_seq(entry: &StreamEntry, stream: &Stream) {
    let mut shard_seq = entry.shard_seq.write().await;
    while shard_seq.len() < stream.shards.len() {
        let idx = shard_seq.len();
        let shard = &stream.shards[idx];
        let create_time_ms =
            sequence::parse_sequence(&shard.sequence_number_range.starting_sequence_number)
                .map(|s| s.shard_create_time)
                .unwrap_or(0);
        shard_seq.push(ShardSeqState {
            // Skip index 0: the shard's starting_sequence_number is generated with
            // seq_ix=0. If the first PutRecord lands in the same millisecond as shard
            // creation, seq_time would also match, so seq_ix must be ≥1 to guarantee
            // every record's sequence number is strictly greater than the starting one.
            counter: AtomicU64::new(1),
            _create_time_ms: create_time_ms,
        });
    }
}

/// Route a hash key to the appropriate open shard. Returns `(shard_ix, shard_id, create_time_ms)`.
fn route_hash_to_shard(stream: &Stream, hash_key: &BigUint) -> (i64, String, u64) {
    for (i, shard) in stream.shards.iter().enumerate() {
        if shard.sequence_number_range.ending_sequence_number.is_none() {
            let start: BigUint = shard
                .hash_key_range
                .starting_hash_key
                .parse()
                .unwrap_or_else(|_| BigUint::zero());
            let end: BigUint = shard
                .hash_key_range
                .ending_hash_key
                .parse()
                .unwrap_or_else(|_| BigUint::zero());
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
