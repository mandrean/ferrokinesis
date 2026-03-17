use crate::error::KinesisErrorResponse;
use crate::types::{Consumer, StoredRecord, Stream};
use redb::backends::InMemoryBackend;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

const STREAMS: TableDefinition<&str, &[u8]> = TableDefinition::new("streams");
const RECORDS: TableDefinition<&str, &[u8]> = TableDefinition::new("records");
const CONSUMERS: TableDefinition<&str, &[u8]> = TableDefinition::new("consumers");
const POLICIES: TableDefinition<&str, &str> = TableDefinition::new("policies");
const RESOURCE_TAGS: TableDefinition<&str, &[u8]> = TableDefinition::new("resource_tags");

#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub create_stream_ms: u64,
    pub delete_stream_ms: u64,
    pub update_stream_ms: u64,
    pub shard_limit: u32,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            create_stream_ms: 500,
            delete_stream_ms: 500,
            update_stream_ms: 500,
            shard_limit: 10,
        }
    }
}

#[derive(Clone)]
pub struct Store {
    pub options: StoreOptions,
    pub aws_account_id: String,
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
        obj.insert(
            "_key_id".to_string(),
            serde_json::to_value(key_id).unwrap(),
        );
    }
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
    pub fn new(options: StoreOptions) -> Self {
        let aws_account_id = std::env::var("AWS_ACCOUNT_ID")
            .unwrap_or_else(|_| "0000-0000-0000".to_string())
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect();

        let aws_region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string());

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
        write_txn.commit().unwrap();

        Self {
            options,
            aws_account_id,
            aws_region,
            db: Arc::new(db),
        }
    }

    pub async fn get_stream(&self, name: &str) -> Result<Stream, KinesisErrorResponse> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(STREAMS).unwrap();
        match table.get(name).unwrap() {
            Some(guard) => Ok(deserialize_stream(guard.value())),
            None => Err(KinesisErrorResponse::client_error(
                "ResourceNotFoundException",
                Some(&format!(
                    "Stream {} under account {} not found.",
                    name, self.aws_account_id
                )),
            )),
        }
    }

    pub async fn put_stream(&self, name: &str, stream: Stream) {
        let bytes = serialize_stream(&stream);
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(STREAMS).unwrap();
            table.insert(name, bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }

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

    /// Check if a stream exists
    pub async fn contains_stream(&self, name: &str) -> bool {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(STREAMS).unwrap();
        table.get(name).unwrap().is_some()
    }

    /// List all stream names (sorted)
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

    /// Update a stream in a read-modify-write transaction.
    /// Returns the result of the closure.
    pub async fn update_stream<F, R>(
        &self,
        name: &str,
        f: F,
    ) -> Result<R, KinesisErrorResponse>
    where
        F: FnOnce(&mut Stream) -> Result<R, KinesisErrorResponse>,
    {
        let write_txn = self.db.begin_write().unwrap();
        let result;
        {
            let mut table = write_txn.open_table(STREAMS).unwrap();
            let bytes = table.get(name).unwrap().ok_or_else(|| {
                KinesisErrorResponse::client_error(
                    "ResourceNotFoundException",
                    Some(&format!(
                        "Stream {} under account {} not found.",
                        name, self.aws_account_id
                    )),
                )
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

    /// Execute a closure with write access to all streams.
    /// The closure receives a mutable BTreeMap of all streams.
    /// Changes are written back to the database.
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

    pub async fn get_record_store(
        &self,
        stream_name: &str,
    ) -> BTreeMap<String, StoredRecord> {
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
                let record: StoredRecord = serde_json::from_slice(v.value()).unwrap();
                (shard_key, record)
            })
            .collect()
    }

    pub async fn put_record(&self, stream_name: &str, key: &str, record: StoredRecord) {
        let composite_key = record_key(stream_name, key);
        let bytes = serde_json::to_vec(&record).unwrap();
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(RECORDS).unwrap();
            table.insert(composite_key.as_str(), bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }

    pub async fn put_records_batch(
        &self,
        stream_name: &str,
        batch: Vec<(String, StoredRecord)>,
    ) {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(RECORDS).unwrap();
            for (key, record) in &batch {
                let composite_key = record_key(stream_name, key);
                let bytes = serde_json::to_vec(record).unwrap();
                table.insert(composite_key.as_str(), bytes.as_slice()).unwrap();
            }
        }
        write_txn.commit().unwrap();
    }

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

    /// Get records in a specific range for a stream
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
                let record: StoredRecord = serde_json::from_slice(v.value()).unwrap();
                (shard_key, record)
            })
            .collect()
    }

    // --- Consumer operations ---

    pub async fn put_consumer(&self, consumer_arn: &str, consumer: Consumer) {
        let bytes = serde_json::to_vec(&consumer).unwrap();
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(CONSUMERS).unwrap();
            table.insert(consumer_arn, bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }

    pub async fn get_consumer(&self, consumer_arn: &str) -> Option<Consumer> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(CONSUMERS).unwrap();
        table.get(consumer_arn).unwrap().map(|guard| {
            serde_json::from_slice(guard.value()).unwrap()
        })
    }

    pub async fn delete_consumer(&self, consumer_arn: &str) {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(CONSUMERS).unwrap();
            table.remove(consumer_arn).unwrap();
        }
        write_txn.commit().unwrap();
    }

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

    /// Find a consumer by stream ARN + consumer name
    pub async fn find_consumer(&self, stream_arn: &str, consumer_name: &str) -> Option<Consumer> {
        let consumers = self.list_consumers_for_stream(stream_arn).await;
        consumers.into_iter().find(|c| c.consumer_name == consumer_name)
    }

    // --- Resource policy operations ---

    pub async fn put_policy(&self, resource_arn: &str, policy: &str) {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(POLICIES).unwrap();
            table.insert(resource_arn, policy).unwrap();
        }
        write_txn.commit().unwrap();
    }

    pub async fn get_policy(&self, resource_arn: &str) -> Option<String> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(POLICIES).unwrap();
        table.get(resource_arn).unwrap().map(|guard| guard.value().to_string())
    }

    pub async fn delete_policy(&self, resource_arn: &str) {
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(POLICIES).unwrap();
            table.remove(resource_arn).unwrap();
        }
        write_txn.commit().unwrap();
    }

    /// Resolve a stream name from a stream ARN
    pub fn stream_name_from_arn(&self, arn: &str) -> Option<String> {
        // Format: arn:aws:kinesis:{region}:{account}:stream/{name}
        arn.split("/").nth(1).map(|s| s.to_string())
    }

    // --- Resource tag operations (for non-stream resources like consumers) ---

    pub async fn get_resource_tags(&self, resource_arn: &str) -> BTreeMap<String, String> {
        let read_txn = self.db.begin_read().unwrap();
        let table = read_txn.open_table(RESOURCE_TAGS).unwrap();
        table
            .get(resource_arn)
            .unwrap()
            .map(|guard| serde_json::from_slice(guard.value()).unwrap_or_default())
            .unwrap_or_default()
    }

    pub async fn put_resource_tags(&self, resource_arn: &str, tags: &BTreeMap<String, String>) {
        let bytes = serde_json::to_vec(tags).unwrap();
        let write_txn = self.db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(RESOURCE_TAGS).unwrap();
            table.insert(resource_arn, bytes.as_slice()).unwrap();
        }
        write_txn.commit().unwrap();
    }
}
