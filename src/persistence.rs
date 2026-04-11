use crate::store::PendingTransition;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::types::{
    EncryptionType, EnhancedMonitoring, EpochSeconds, Shard, Stream, StreamBuilder,
    StreamModeDetails, StreamStatus,
};

const SNAPSHOT_FILE: &str = "snapshot.bin";
const SNAPSHOT_TMP_FILE: &str = "snapshot.bin.tmp";
const WAL_FILE: &str = "wal.log";
const WAL_MAGIC: &[u8; 8] = b"FKWALv2\n";

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PersistentSnapshot {
    pub created_at_ms: u64,
    pub streams: Vec<PersistentStream>,
    pub records: Vec<SnapshotShardRecords>,
    pub consumers: Vec<(String, Vec<u8>)>,
    pub policies: Vec<(String, String)>,
    pub resource_tags: Vec<(String, BTreeMap<String, String>)>,
    pub account_settings_json: Vec<u8>,
    #[serde(default)]
    pub(crate) pending_transitions: Vec<PendingTransition>,
    pub retained_bytes: u64,
    pub retained_records: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentStream {
    pub name: String,
    pub retention_period_hours: u32,
    pub enhanced_monitoring: Vec<EnhancedMonitoring>,
    pub encryption_type: EncryptionType,
    pub has_more_shards: bool,
    pub shards: Vec<Shard>,
    pub stream_arn: String,
    pub stream_name: String,
    pub stream_status: StreamStatus,
    pub stream_creation_timestamp: EpochSeconds,
    pub stream_mode_details: StreamModeDetails,
    pub tags: BTreeMap<String, String>,
    pub key_id: Option<String>,
    pub warm_throughput_mibps: u32,
    pub max_record_size_kib: u32,
    pub shard_counters: Vec<u64>,
}

impl PersistentStream {
    pub fn from_stream(name: String, stream: &Stream, shard_counters: Vec<u64>) -> Self {
        Self {
            name,
            retention_period_hours: stream.retention_period_hours,
            enhanced_monitoring: stream.enhanced_monitoring.clone(),
            encryption_type: stream.encryption_type,
            has_more_shards: stream.has_more_shards,
            shards: stream.shards.clone(),
            stream_arn: stream.stream_arn.clone(),
            stream_name: stream.stream_name.clone(),
            stream_status: stream.stream_status,
            stream_creation_timestamp: stream.stream_creation_timestamp,
            stream_mode_details: stream.stream_mode_details.clone(),
            tags: stream.tags.clone(),
            key_id: stream.key_id.clone(),
            warm_throughput_mibps: stream.warm_throughput_mibps,
            max_record_size_kib: stream.max_record_size_kib,
            shard_counters,
        }
    }

    pub fn into_parts(self) -> (String, Stream, Vec<u64>) {
        let stream = StreamBuilder::new(
            self.stream_name,
            self.stream_arn,
            self.stream_status,
            self.stream_creation_timestamp,
            self.shards,
        )
        .retention_period_hours(self.retention_period_hours)
        .enhanced_monitoring(self.enhanced_monitoring)
        .encryption_type(self.encryption_type)
        .has_more_shards(self.has_more_shards)
        .stream_mode_details(self.stream_mode_details)
        .tags(self.tags)
        .key_id(self.key_id)
        .warm_throughput_mibps(self.warm_throughput_mibps)
        .max_record_size_kib(self.max_record_size_kib)
        .build();
        (self.name, stream, self.shard_counters)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotShardRecords {
    pub stream_name: String,
    pub shard_hex: String,
    pub records: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    Snapshot(PersistentSnapshot),
}

#[derive(Debug)]
pub enum LoadError {
    Io(io::Error),
    Snapshot(serde_json::Error),
    Wal(serde_json::Error),
    WalEntryTooLarge(u64),
    TruncatedWal,
    UnsupportedWalFormat,
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Snapshot(err) => write!(f, "failed to decode snapshot: {err}"),
            Self::Wal(err) => write!(f, "failed to decode wal entry: {err}"),
            Self::WalEntryTooLarge(len) => {
                write!(
                    f,
                    "wal entry length {len} exceeds this platform's address space"
                )
            }
            Self::TruncatedWal => write!(f, "wal is truncated"),
            Self::UnsupportedWalFormat => {
                write!(f, "unsupported wal format; recreate state dir")
            }
        }
    }
}

impl std::error::Error for LoadError {}

impl From<io::Error> for LoadError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

#[derive(Debug, Clone)]
pub struct Persistence {
    state_dir: PathBuf,
}

impl Persistence {
    pub fn new(state_dir: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&state_dir)?;
        Ok(Self { state_dir })
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn load(&self) -> Result<Option<(PersistentSnapshot, Vec<WalEntry>)>, LoadError> {
        let snapshot = self.read_snapshot()?;
        let wal_entries = self.read_wal()?;
        if snapshot.is_none() && wal_entries.is_empty() {
            return Ok(None);
        }
        Ok(Some((snapshot.unwrap_or_default(), wal_entries)))
    }

    pub fn append_wal_entry(&self, entry: &WalEntry) -> io::Result<()> {
        let bytes = serde_json::to_vec(entry)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let len = u64::try_from(bytes.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "serialized wal entry exceeds u64 length prefix",
            )
        })?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(self.state_dir.join(WAL_FILE))?;
        if file.metadata()?.len() == 0 {
            file.write_all(WAL_MAGIC)?;
        }
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&bytes)?;
        file.sync_all()
    }

    pub fn write_snapshot(&self, snapshot: &PersistentSnapshot) -> io::Result<()> {
        let bytes = serde_json::to_vec(snapshot)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
        let tmp_path = self.state_dir.join(SNAPSHOT_TMP_FILE);
        let final_path = self.state_dir.join(SNAPSHOT_FILE);

        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }

        fs::rename(&tmp_path, &final_path)?;
        sync_directory(&self.state_dir)?;

        let wal_path = self.state_dir.join(WAL_FILE);
        let mut wal = File::create(wal_path)?;
        wal.write_all(WAL_MAGIC)?;
        wal.flush()?;
        wal.sync_all()?;
        sync_directory(&self.state_dir)
    }

    fn read_snapshot(&self) -> Result<Option<PersistentSnapshot>, LoadError> {
        let path = self.state_dir.join(SNAPSHOT_FILE);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)?;
        let snapshot = serde_json::from_slice(&bytes).map_err(LoadError::Snapshot)?;
        Ok(Some(snapshot))
    }

    fn read_wal(&self) -> Result<Vec<WalEntry>, LoadError> {
        let path = self.state_dir.join(WAL_FILE);
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut magic = [0u8; WAL_MAGIC.len()];

        match reader.read_exact(&mut magic) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(LoadError::UnsupportedWalFormat);
            }
            Err(err) => return Err(LoadError::Io(err)),
        }

        if magic != *WAL_MAGIC {
            return Err(LoadError::UnsupportedWalFormat);
        }

        loop {
            let mut len_buf = [0u8; 8];
            let read = match reader.read(&mut len_buf) {
                Ok(0) => break,
                Ok(read) => read,
                Err(err) => return Err(LoadError::Io(err)),
            };
            if read < len_buf.len() {
                reader
                    .read_exact(&mut len_buf[read..])
                    .map_err(|err| match err.kind() {
                        io::ErrorKind::UnexpectedEof => LoadError::TruncatedWal,
                        _ => LoadError::Io(err),
                    })?;
            }
            let len = decode_wal_entry_len(len_buf)?;
            let mut entry_buf = vec![0u8; len];
            if reader.read_exact(&mut entry_buf).is_err() {
                return Err(LoadError::TruncatedWal);
            }
            let entry = serde_json::from_slice(&entry_buf).map_err(LoadError::Wal)?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

fn decode_wal_entry_len(len_buf: [u8; 8]) -> Result<usize, LoadError> {
    let len = u64::from_le_bytes(len_buf);
    usize::try_from(len).map_err(|_| LoadError::WalEntryTooLarge(len))
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        EncryptionType, EpochSeconds, StreamBuilder, StreamMode, StreamModeDetails, StreamStatus,
    };
    use tempfile::tempdir;

    #[test]
    fn persistent_snapshot_round_trips() {
        let stream = StreamBuilder::new(
            "stream-a".to_string(),
            "arn:aws:kinesis:eu-west-1:123456789012:stream/stream-a".to_string(),
            StreamStatus::Active,
            EpochSeconds(1_700_000_000.0),
            vec![],
        )
        .retention_period_hours(48)
        .encryption_type(EncryptionType::Kms)
        .stream_mode_details(StreamModeDetails {
            stream_mode: StreamMode::OnDemand,
        })
        .tags(BTreeMap::from([("env".to_string(), "prod".to_string())]))
        .key_id(Some("key-123".to_string()))
        .warm_throughput_mibps(9)
        .max_record_size_kib(2048)
        .build();

        let snapshot = PersistentSnapshot {
            created_at_ms: 1,
            streams: vec![PersistentStream::from_stream(
                "stream-a".to_string(),
                &stream,
                vec![7],
            )],
            records: vec![],
            consumers: vec![],
            policies: vec![],
            resource_tags: vec![],
            account_settings_json: vec![],
            pending_transitions: vec![],
            retained_bytes: 0,
            retained_records: 0,
        };

        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let decoded: PersistentSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.streams[0].name, "stream-a");
        assert_eq!(
            decoded.streams[0].tags.get("env").map(String::as_str),
            Some("prod")
        );
        assert_eq!(decoded.streams[0].key_id.as_deref(), Some("key-123"));
        assert_eq!(decoded.streams[0].warm_throughput_mibps, 9);
        assert_eq!(decoded.streams[0].max_record_size_kib, 2048);
        assert_eq!(decoded.streams[0].shard_counters, vec![7]);
    }

    #[test]
    fn wal_round_trips_with_header_and_u64_length_prefix() {
        let dir = tempdir().unwrap();
        let persistence = Persistence::new(dir.path().to_path_buf()).unwrap();
        let snapshot = PersistentSnapshot {
            created_at_ms: 42,
            ..PersistentSnapshot::default()
        };

        persistence
            .append_wal_entry(&WalEntry::Snapshot(snapshot.clone()))
            .unwrap();

        let wal_bytes = fs::read(dir.path().join(WAL_FILE)).unwrap();
        assert!(wal_bytes.starts_with(WAL_MAGIC));

        let (loaded_snapshot, loaded_entries) = persistence.load().unwrap().unwrap();
        assert_eq!(loaded_snapshot.created_at_ms, 0);
        assert_eq!(loaded_entries.len(), 1);
        assert!(matches!(
            &loaded_entries[0],
            WalEntry::Snapshot(entry) if entry.created_at_ms == snapshot.created_at_ms
        ));
    }

    #[test]
    fn legacy_wal_without_header_is_rejected() {
        let dir = tempdir().unwrap();
        let persistence = Persistence::new(dir.path().to_path_buf()).unwrap();

        fs::write(dir.path().join(WAL_FILE), []).unwrap();

        let err = persistence.load().unwrap_err();
        assert!(matches!(err, LoadError::UnsupportedWalFormat));
        assert_eq!(
            err.to_string(),
            "unsupported wal format; recreate state dir"
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn wal_entry_length_helper_supports_lengths_larger_than_u32_max() {
        let len = u64::from(u32::MAX) + 1;
        assert_eq!(
            decode_wal_entry_len(len.to_le_bytes()).unwrap(),
            len as usize
        );
    }

    #[cfg(target_pointer_width = "32")]
    #[test]
    fn wal_entry_length_overflow_is_reported_without_allocating() {
        let err = decode_wal_entry_len(u64::MAX.to_le_bytes()).unwrap_err();
        assert!(matches!(err, LoadError::WalEntryTooLarge(u64::MAX)));
    }
}
