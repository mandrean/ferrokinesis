//! In-memory redb-backed store for streams, records, consumers, and policies.
//!
//! [`Store`] is the central state object threaded through all action handlers.
//! It is cheap to clone — all clones share the same underlying [`Arc`]`<`[`Database`]`>`.
//!
//! [`StoreOptions`] controls runtime behaviour such as simulated delays and
//! account identity. Pass it to [`crate::create_app`] to wire everything up.

use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::types::{Consumer, StoredRecord, Stream};
use redb::backends::InMemoryBackend;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

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

const STREAMS: TableDefinition<&str, &[u8]> = TableDefinition::new("streams");
const RECORDS: TableDefinition<&str, &[u8]> = TableDefinition::new("records");
const CONSUMERS: TableDefinition<&str, &[u8]> = TableDefinition::new("consumers");
const POLICIES: TableDefinition<&str, &str> = TableDefinition::new("policies");
const RESOURCE_TAGS: TableDefinition<&str, &[u8]> = TableDefinition::new("resource_tags");
const ACCOUNT_SETTINGS: TableDefinition<&str, &[u8]> = TableDefinition::new("account_settings");

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

/// Handle to the in-memory redb-backed stream store.
///
/// Cheap to clone — all clones share the same underlying `Arc<Database>`.
/// Inject this into Axum handlers via `State<Store>`.
#[derive(Clone)]
pub struct Store {
    /// Runtime configuration used by action handlers.
    pub options: StoreOptions,
    /// The effective AWS account ID (digits only, length-validated at construction).
    pub aws_account_id: String,
    /// The simulated AWS region.
    pub aws_region: String,
    db: Arc<Database>,
}

/// Serialize a Stream for storage, including hidden fields (seq_ix, tags)
fn serialize_stream(stream: &Stream) -> Vec<u8> {
    let mut val = serde_json::to_value(stream).unwrap();
    let obj = val.as_object_mut().unwrap();
    obj.insert(
        "_seq_ix".to_string(),
        serde_json::to_value(&stream.seq_ix).unwrap(),
    );
    obj.insert(
        "_tags".to_string(),
        serde_json::to_value(&stream.tags).unwrap(),
    );
    if let Some(ref key_id) = stream.key_id {
        obj.insert("_key_id".to_string(), serde_json::to_value(key_id).unwrap());
    }
    obj.insert(
        "_warm_throughput_mibps".to_string(),
        serde_json::to_value(stream.warm_throughput_mibps).unwrap(),
    );
    obj.insert(
        "_max_record_size_kib".to_string(),
        serde_json::to_value(stream.max_record_size_kib).unwrap(),
    );
    serde_json::to_vec(&val).unwrap()
}

/// Deserialize a Stream from storage, restoring hidden fields
fn deserialize_stream(bytes: &[u8]) -> Stream {
    let val: Value = serde_json::from_slice(bytes).unwrap();
    let mut stream: Stream = serde_json::from_value(val.clone()).unwrap();
    if let Some(arr) = val.get("_seq_ix") {
        stream.seq_ix = serde_json::from_value(arr.clone()).unwrap_or_default();
    }
    if let Some(obj) = val.get("_tags") {
        stream.tags = serde_json::from_value(obj.clone()).unwrap_or_default();
    }
    if let Some(key_id) = val.get("_key_id") {
        stream.key_id = serde_json::from_value(key_id.clone()).ok();
    }
    if let Some(v) = val.get("_warm_throughput_mibps") {
        stream.warm_throughput_mibps = serde_json::from_value(v.clone()).unwrap_or(0);
    }
    if let Some(v) = val.get("_max_record_size_kib") {
        stream.max_record_size_kib = serde_json::from_value(v.clone()).unwrap_or(1024);
    }
    stream
}

/// Composite key for records: "{stream_name}\0{shard_hex}/{seq_num}"
fn record_key(stream_name: &str, shard_key: &str) -> String {
    format!("{stream_name}\0{shard_key}")
}

/// Range bounds for all records in a stream's shard range
fn record_range(stream_name: &str, start: &str, end: &str) -> (String, String) {
    (
        format!("{stream_name}\0{start}"),
        format!("{stream_name}\0{end}"),
    )
}

impl Store {
    /// Creates a new store, initialising all redb tables.
    ///
    /// Strips non-digit characters from `options.aws_account_id` and warns if
    /// the result is not exactly 12 digits.
    pub fn new(options: StoreOptions) -> Self {
        let aws_account_id: String = options
            .aws_account_id
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect();

        if aws_account_id.len() != 12 {
            eprintln!(
                "warning: AWS account ID has {} digits after stripping non-digits (expected 12)",
                aws_account_id.len()
            );
        }

        let aws_region = options.aws_region.clone();

        let db = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .expect("Failed to create in-memory redb database");

        // Create tables
        let write_txn = db.begin_write().unwrap();
        write_txn.open_table(STREAMS).unwrap();
        write_txn.open_table(RECORDS).unwrap();
        write_txn.open_table(CONSUMERS).unwrap();
        write_txn.open_table(POLICIES).unwrap();
        write_txn.open_table(RESOURCE_TAGS).unwrap();
        write_txn.open_table(ACCOUNT_SETTINGS).unwrap();
        write_txn.commit().unwrap();

        Self {
            options,
            aws_account_id,
            aws_region,
            db: Arc::new(db),
        }
    }

    /// Retrieves a stream by name.
    ///
    /// # Errors
    ///
    /// Returns [`KinesisErrorResponse`] (`ResourceNotFoundException`) if the stream does not exist.
    pub async fn get_stream(&self, name: &str) -> Result<Stream, KinesisErrorResponse> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(STREAMS).unwrap();
        match table.get(name).unwrap() {
            Some(guard) => Ok(deserialize_stream(guard.value())),
            None => Err(KinesisErrorResponse::stream_not_found(
                name,
                &self.aws_account_id,
            )),
        }
    }

    /// Inserts or replaces a stream by name.
    pub async fn put_stream(&self, name: &str, stream: Stream) {
        let bytes = serialize_stream(&stream);
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(STREAMS).unwrap();
            table.insert(name, bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }

    /// Deletes a stream and all of its records.
    pub async fn delete_stream(&self, name: &str) {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut streams = write_txn.open_table(STREAMS).unwrap();
            streams.remove(name).unwrap();

            // Delete all records for this stream
            let mut records = write_txn.open_table(RECORDS).unwrap();
            let prefix = format!("{name}\0");
            let prefix_end = format!("{name}\x01");
            let keys_to_remove: Vec<String> = records
                .range(prefix.as_str()..prefix_end.as_str())
                .unwrap()
                .map(|r| {
                    let (k, _) = r.unwrap();
                    k.value().to_string()
                })
                .collect();
            for key in keys_to_remove {
                records.remove(key.as_str()).unwrap();
            }
        }
        write_txn.commit().unwrap();
    }

    /// Returns `true` if a stream with the given name exists.
    pub async fn contains_stream(&self, name: &str) -> bool {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(STREAMS).unwrap();
        table.get(name).unwrap().is_some()
    }

    /// Returns all stream names in sorted order.
    pub async fn list_stream_names(&self) -> Vec<String> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(STREAMS).unwrap();
        table
            .iter()
            .unwrap()
            .map(|r| {
                let (k, _) = r.unwrap();
                k.value().to_string()
            })
            .collect()
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
        let write_txn = self.db.begin_write().unwrap();
        let result;
        {
            let mut table = write_txn.open_table(STREAMS).unwrap();
            let bytes = table.get(name).unwrap().ok_or_else(|| {
                KinesisErrorResponse::stream_not_found(name, &self.aws_account_id)
            })?;
            let mut stream = deserialize_stream(bytes.value());
            drop(bytes);
            result = f(&mut stream)?;
            let new_bytes = serialize_stream(&stream);
            table.insert(name, new_bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
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
        let write_txn = self.db.begin_write().unwrap();
        let result;
        {
            let mut table = write_txn.open_table(STREAMS).unwrap();
            let mut streams: BTreeMap<String, Stream> = table
                .iter()
                .unwrap()
                .map(|r| {
                    let (k, v) = r.unwrap();
                    (k.value().to_string(), deserialize_stream(v.value()))
                })
                .collect();

            result = f(
                &mut streams,
                &self.options,
                &self.aws_account_id,
                &self.aws_region,
            );

            // Write all streams back
            // First, collect existing keys to detect deletions
            let existing_keys: Vec<String> = table
                .iter()
                .unwrap()
                .map(|r| r.unwrap().0.value().to_string())
                .collect();

            for key in &existing_keys {
                if !streams.contains_key(key) {
                    table.remove(key.as_str()).unwrap();
                }
            }
            for (name, stream) in &streams {
                let bytes = serialize_stream(stream);
                table.insert(name.as_str(), bytes.as_slice()).unwrap();
            }
        }
        write_txn.commit().unwrap();
        result
    }

    /// Returns all records for a stream as a map from shard-key to record.
    ///
    /// The map key is `"{shard_hex}/{seq_num}"` (the composite key with the
    /// stream-name prefix stripped).
    pub async fn get_record_store(&self, stream_name: &str) -> BTreeMap<String, StoredRecord> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(RECORDS).unwrap();
        let prefix = format!("{stream_name}\0");
        let prefix_end = format!("{stream_name}\x01");
        table
            .range(prefix.as_str()..prefix_end.as_str())
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                let full_key = k.value().to_string();
                // Strip the stream_name\0 prefix to get the shard_hex/seq_num key
                let shard_key = full_key
                    .strip_prefix(&prefix)
                    .unwrap_or(&full_key)
                    .to_string();
                let record: StoredRecord = postcard::from_bytes(v.value()).unwrap();
                (shard_key, record)
            })
            .collect()
    }

    /// Stores a single record under the given composite shard key.
    pub async fn put_record(&self, stream_name: &str, key: &str, record: &impl Serialize) {
        let composite_key = record_key(stream_name, key);
        let bytes = postcard::to_allocvec(record).unwrap();
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(RECORDS).unwrap();
            table
                .insert(composite_key.as_str(), bytes.as_slice())
                .unwrap();
        }
        write_txn.commit().unwrap();
    }

    /// Stores multiple records in a single write transaction.
    pub async fn put_records_batch<R: Serialize>(&self, stream_name: &str, batch: &[(String, R)]) {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(RECORDS).unwrap();
            for (key, record) in batch {
                let composite_key = record_key(stream_name, key);
                let bytes = postcard::to_allocvec(record).unwrap();
                table
                    .insert(composite_key.as_str(), bytes.as_slice())
                    .unwrap();
            }
        }
        write_txn.commit().unwrap();
    }

    /// Deletes records whose sequence-number–embedded timestamp is older than
    /// the retention cutoff. Returns the number of records removed.
    pub async fn delete_expired_records(&self, stream_name: &str, retention_hours: u32) -> usize {
        let now = crate::util::current_time_ms();
        let cutoff_time = now - (retention_hours as u64 * 60 * 60 * 1000);

        let prefix = format!("{stream_name}\0");
        let prefix_end = format!("{stream_name}\x01");

        let keys_to_delete: Vec<String> = {
            let read_txn = self.db.begin_read().unwrap();
            let table = read_txn.open_table(RECORDS).unwrap();
            table
                .range(prefix.as_str()..prefix_end.as_str())
                .unwrap()
                .filter_map(|r| {
                    let (k, _) = r.unwrap();
                    let full_key = k.value().to_string();
                    let shard_key = full_key.strip_prefix(&prefix)?;
                    let seq_num = shard_key.split('/').nth(1)?;
                    let seq_obj = crate::sequence::parse_sequence(seq_num).ok()?;
                    if seq_obj.seq_time.unwrap_or(0) < cutoff_time {
                        Some(full_key)
                    } else {
                        None
                    }
                })
                .collect()
        };

        let count = keys_to_delete.len();
        if count > 0 {
            let write_txn = self.db.begin_write().unwrap();
            {
                let mut table = write_txn.open_table(RECORDS).unwrap();
                for key in &keys_to_delete {
                    table.remove(key.as_str()).unwrap();
                }
            }
            write_txn.commit().unwrap();
        }
        count
    }

    /// Deletes records by their shard keys within a stream.
    pub async fn delete_record_keys(&self, stream_name: &str, keys: &[String]) {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(RECORDS).unwrap();
            for key in keys {
                let composite_key = record_key(stream_name, key);
                table.remove(composite_key.as_str()).unwrap();
            }
        }
        write_txn.commit().unwrap();
    }

    /// Returns the total number of open (non-closed) shards across all streams.
    pub async fn sum_open_shards(&self) -> u32 {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(STREAMS).unwrap();
        table
            .iter()
            .unwrap()
            .map(|r| {
                let (_, v) = r.unwrap();
                let stream = deserialize_stream(v.value());
                stream
                    .shards
                    .iter()
                    .filter(|s| s.sequence_number_range.ending_sequence_number.is_none())
                    .count() as u32
            })
            .sum()
    }

    /// Returns records in the given shard-key range for a stream.
    ///
    /// Both `range_start` and `range_end` are shard keys of the form
    /// `"{shard_hex}/{seq_num}"`.
    pub async fn get_records_range(
        &self,
        stream_name: &str,
        range_start: &str,
        range_end: &str,
    ) -> Vec<(String, StoredRecord)> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(RECORDS).unwrap();
        let (start, end) = record_range(stream_name, range_start, range_end);
        table
            .range(start.as_str()..end.as_str())
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                let full_key = k.value().to_string();
                let prefix = format!("{stream_name}\0");
                let shard_key = full_key
                    .strip_prefix(&prefix)
                    .unwrap_or(&full_key)
                    .to_string();
                let record: StoredRecord = postcard::from_bytes(v.value()).unwrap();
                (shard_key, record)
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
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(RECORDS).unwrap();
        let (start, end) = record_range(stream_name, range_start, range_end);
        let prefix_len = stream_name.len() + 1; // stream_name + '\0'
        table
            .range(start.as_str()..end.as_str())
            .unwrap()
            .take(limit)
            .map(|r| {
                let (k, v) = r.unwrap();
                let full_key = k.value().to_string();
                let shard_key = full_key[prefix_len..].to_string();
                let record: StoredRecord = postcard::from_bytes(v.value()).unwrap();
                (shard_key, record)
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
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(RECORDS).unwrap();
        let (start, end) = record_range(stream_name, range_start, range_end);
        let prefix_len = stream_name.len() + 1;
        for r in table.range(start.as_str()..end.as_str()).unwrap() {
            let (k, v) = r.unwrap();
            let record: StoredRecord = postcard::from_bytes(v.value()).unwrap();
            if record.approximate_arrival_timestamp >= timestamp {
                let full_key = k.value().to_string();
                let shard_key = full_key[prefix_len..].to_string();
                return Some((shard_key, record));
            }
        }
        None
    }

    // --- Consumer operations ---

    /// Inserts or replaces a consumer keyed by its ARN.
    pub async fn put_consumer(&self, consumer_arn: &str, consumer: Consumer) {
        let bytes = serde_json::to_vec(&consumer).unwrap();
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(CONSUMERS).unwrap();
            table.insert(consumer_arn, bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }

    /// Returns the consumer with the given ARN, or `None` if not found.
    pub async fn get_consumer(&self, consumer_arn: &str) -> Option<Consumer> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(CONSUMERS).unwrap();
        table
            .get(consumer_arn)
            .unwrap()
            .map(|guard| serde_json::from_slice(guard.value()).unwrap())
    }

    /// Deletes the consumer with the given ARN.
    pub async fn delete_consumer(&self, consumer_arn: &str) {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(CONSUMERS).unwrap();
            table.remove(consumer_arn).unwrap();
        }
        write_txn.commit().unwrap();
    }

    /// Returns all consumers whose ARN belongs to the given stream ARN.
    pub async fn list_consumers_for_stream(&self, stream_arn: &str) -> Vec<Consumer> {
        let prefix = format!("{stream_arn}/consumer/");
        let prefix_end = format!("{stream_arn}/consumer0"); // '0' > '/'
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(CONSUMERS).unwrap();
        table
            .range(prefix.as_str()..prefix_end.as_str())
            .unwrap()
            .map(|r| {
                let (_, v) = r.unwrap();
                serde_json::from_slice(v.value()).unwrap()
            })
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
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(POLICIES).unwrap();
            table.insert(resource_arn, policy).unwrap();
        }
        write_txn.commit().unwrap();
    }

    /// Returns the resource policy for the given ARN, or `None` if none is set.
    pub async fn get_policy(&self, resource_arn: &str) -> Option<String> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(POLICIES).unwrap();
        table
            .get(resource_arn)
            .unwrap()
            .map(|guard| guard.value().to_string())
    }

    /// Deletes the resource policy for the given ARN.
    pub async fn delete_policy(&self, resource_arn: &str) {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(POLICIES).unwrap();
            table.remove(resource_arn).unwrap();
        }
        write_txn.commit().unwrap();
    }

    /// Extracts the stream name from a Kinesis stream ARN.
    ///
    /// ARN format: `arn:aws:kinesis:{region}:{account}:stream/{name}`
    ///
    /// Returns `None` if the ARN does not contain a `/`.
    pub fn stream_name_from_arn(&self, arn: &str) -> Option<String> {
        // Format: arn:aws:kinesis:{region}:{account}:stream/{name}
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
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(RESOURCE_TAGS).unwrap();
        table
            .get(resource_arn)
            .unwrap()
            .map(|guard| serde_json::from_slice(guard.value()).unwrap_or_default())
            .unwrap_or_default()
    }

    /// Replaces the tag map for the given resource ARN.
    pub async fn put_resource_tags(&self, resource_arn: &str, tags: &BTreeMap<String, String>) {
        let bytes = serde_json::to_vec(tags).unwrap();
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(RESOURCE_TAGS).unwrap();
            table.insert(resource_arn, bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }

    // --- Account settings operations ---

    /// Returns the account-level settings object, or an empty JSON object if none are set.
    pub async fn get_account_settings(&self) -> Value {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(ACCOUNT_SETTINGS).unwrap();
        table
            .get("account_settings")
            .unwrap()
            .map(|guard| serde_json::from_slice(guard.value()).unwrap())
            .unwrap_or(Value::Object(Default::default()))
    }

    /// Stores the account-level settings object.
    pub async fn put_account_settings(&self, settings: &Value) {
        let bytes = serde_json::to_vec(settings).unwrap();
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(ACCOUNT_SETTINGS).unwrap();
            table.insert("account_settings", bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }

    /// Probes the database with a read transaction to verify all core tables are accessible.
    ///
    /// # Errors
    ///
    /// Returns [`StoreHealthError::ReadFailed`] if a read transaction cannot be started,
    /// or [`StoreHealthError::TableOpenFailed`] if any core table cannot be opened.
    pub fn check_ready(&self) -> Result<(), StoreHealthError> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| StoreHealthError::ReadFailed(e.to_string()))?;
        for table in [STREAMS, RECORDS, CONSUMERS, RESOURCE_TAGS, ACCOUNT_SETTINGS] {
            txn.open_table(table)
                .map_err(|e| StoreHealthError::TableOpenFailed(e.to_string()))?;
        }
        // POLICIES has a different value type (&str vs &[u8]), so it can't share the loop above.
        txn.open_table(POLICIES)
            .map_err(|e| StoreHealthError::TableOpenFailed(e.to_string()))?;
        Ok(())
    }
}
