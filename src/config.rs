//! TOML configuration file loading for ferrokinesis.
//!
//! [`FileConfig`] mirrors the TOML configuration file structure. Use [`load_config`]
//! to parse a config file path into a validated [`FileConfig`].

use serde::Deserialize;
use std::path::{Path, PathBuf};

/// Errors that can occur when loading a configuration file.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The config file could not be read from disk.
    #[error("failed to read config file {path}: {source}")]
    Read {
        /// Path to the config file.
        path: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// The config file contents could not be parsed as TOML.
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        /// Path to the config file.
        path: String,
        /// Underlying TOML parse error.
        source: toml::de::Error,
    },
    /// A config value failed semantic validation (e.g. an out-of-range TTL).
    #[error("invalid value in config file {path}: {message}")]
    Validation {
        /// Path to the config file.
        path: String,
        /// Human-readable description of the constraint violation.
        message: String,
    },
}

/// TOML configuration file for ferrokinesis.
///
/// All fields are optional; omitted fields fall back to the defaults defined
/// in [`crate::store::StoreOptions::default`].
///
/// ## Example `ferrokinesis.toml`
///
/// ```toml
/// port = 4567
/// account_id = "000000000000"
/// region = "us-east-1"
/// shard_limit = 50
/// iterator_ttl_seconds = 300
/// ```
#[derive(Deserialize, Default)]
pub struct FileConfig {
    /// TCP port to listen on. Defaults to `4567`.
    pub port: Option<u16>,
    /// Simulated AWS account ID (12 digits). Defaults to `"000000000000"`.
    pub account_id: Option<String>,
    /// Simulated AWS region (e.g. `"us-east-1"`). Defaults to `"us-east-1"`.
    pub region: Option<String>,
    /// Per-account shard limit. Defaults to `10`.
    pub shard_limit: Option<u32>,
    /// Simulated delay for stream creation, in milliseconds. Defaults to `500`.
    pub create_stream_ms: Option<u64>,
    /// Simulated delay for stream deletion, in milliseconds. Defaults to `500`.
    pub delete_stream_ms: Option<u64>,
    /// Simulated delay for stream updates, in milliseconds. Defaults to `500`.
    pub update_stream_ms: Option<u64>,
    /// Shard iterator TTL in seconds. Must be between `1` and `86400`. Defaults to `300`.
    pub iterator_ttl_seconds: Option<u64>,
    /// Background retention-reaper check interval in seconds.
    /// Set to `0` (the default) to disable the reaper entirely.
    /// Must be between `0` and `86400`.
    pub retention_check_interval_secs: Option<u64>,
    /// Enable AWS-like shard write throughput throttling.
    pub enforce_limits: Option<bool>,
    /// Maximum request body size in megabytes. Defaults to `5`.
    pub max_request_body_mb: Option<u64>,
    /// Log level (`off`, `error`, `warn`, `info`, `debug`, `trace`). Defaults to `"info"`.
    pub log_level: Option<String>,
    /// Enable per-request access logging. Defaults to `false`.
    #[cfg(feature = "access-log")]
    pub access_log: Option<bool>,
    /// Path to write captured PutRecord/PutRecords data (NDJSON).
    pub capture: Option<PathBuf>,
    /// Whether to scrub (anonymize) partition keys during capture.
    pub scrub: Option<bool>,
    /// Mirror configuration section.
    #[cfg(feature = "mirror")]
    pub mirror: Option<MirrorConfig>,
    /// Path to the TLS certificate file (PEM). Must be set together with `tls_key`.
    #[cfg(feature = "tls")]
    pub tls_cert: Option<PathBuf>,
    /// Path to the TLS private key file (PEM). Must be set together with `tls_cert`.
    #[cfg(feature = "tls")]
    pub tls_key: Option<PathBuf>,
}

/// Mirror configuration (`[mirror]` TOML section).
#[cfg(feature = "mirror")]
#[derive(Deserialize, Default)]
pub struct MirrorConfig {
    /// Kinesis-compatible endpoint to mirror PutRecord/PutRecords to.
    pub to: Option<String>,
    /// Log response divergences between local and mirror to stderr.
    pub diff: Option<bool>,
    /// Maximum number of concurrent in-flight mirror requests (default: 64).
    pub concurrency: Option<usize>,
    /// Maximum number of retries for failed mirror requests (default: 3, 0 = no retries).
    pub max_retries: Option<usize>,
    /// Initial backoff delay between retries in milliseconds (default: 100).
    pub initial_backoff_ms: Option<u64>,
    /// Maximum backoff delay between retries in milliseconds (default: 5000).
    pub max_backoff_ms: Option<u64>,
}

/// Parse and validate a TOML configuration file.
///
/// # Errors
///
/// Returns [`ConfigError::Read`] if the file cannot be read from disk,
/// [`ConfigError::Parse`] if the file is not valid TOML, or
/// [`ConfigError::Validation`] if a value violates a semantic constraint
/// (e.g. `iterator_ttl_seconds` outside `1..=86400`).
pub fn load_config(path: &Path) -> Result<FileConfig, ConfigError> {
    let content = std::fs::read_to_string(path).map_err(|e| ConfigError::Read {
        path: path.display().to_string(),
        source: e,
    })?;
    let config: FileConfig = toml::from_str(&content).map_err(|e| ConfigError::Parse {
        path: path.display().to_string(),
        source: e,
    })?;
    if let Some(ttl) = config.iterator_ttl_seconds
        && !(1..=86400).contains(&ttl)
    {
        return Err(ConfigError::Validation {
            path: path.display().to_string(),
            message: format!("iterator_ttl_seconds must be between 1 and 86400, got {ttl}"),
        });
    }
    if let Some(v) = config.retention_check_interval_secs
        && v > 86400
    {
        return Err(ConfigError::Validation {
            path: path.display().to_string(),
            message: format!("retention_check_interval_secs must be between 0 and 86400, got {v}"),
        });
    }
    if let Some(ref level) = config.log_level
        && !["off", "error", "warn", "info", "debug", "trace"].contains(&level.as_str())
    {
        return Err(ConfigError::Validation {
            path: path.display().to_string(),
            message: format!(
                "log_level must be one of: off, error, warn, info, debug, trace — got \"{level}\""
            ),
        });
    }
    #[cfg(feature = "mirror")]
    if let Some(ref mirror) = config.mirror
        && let Some(concurrency) = mirror.concurrency
        && concurrency == 0
    {
        return Err(ConfigError::Validation {
            path: path.display().to_string(),
            message: format!("mirror.concurrency must be at least 1, got {concurrency}"),
        });
    }
    #[cfg(feature = "mirror")]
    if let Some(ref mirror) = config.mirror
        && let Some(initial) = mirror.initial_backoff_ms
        && initial == 0
    {
        return Err(ConfigError::Validation {
            path: path.display().to_string(),
            message: format!("mirror.initial_backoff_ms must be at least 1, got {initial}"),
        });
    }
    #[cfg(feature = "mirror")]
    if let Some(ref mirror) = config.mirror
        && let Some(initial) = mirror.initial_backoff_ms
        && let Some(max) = mirror.max_backoff_ms
        && max < initial
    {
        return Err(ConfigError::Validation {
            path: path.display().to_string(),
            message: format!(
                "mirror.max_backoff_ms ({max}) must be >= mirror.initial_backoff_ms ({initial})"
            ),
        });
    }
    #[cfg(feature = "tls")]
    match (&config.tls_cert, &config.tls_key) {
        (Some(_), None) | (None, Some(_)) => {
            return Err(ConfigError::Validation {
                path: path.display().to_string(),
                message: "tls_cert and tls_key must both be set or both be omitted".into(),
            });
        }
        _ => {}
    }
    Ok(config)
}
