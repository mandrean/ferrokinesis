use serde::Deserialize;
use std::path::Path;
#[cfg(feature = "tls")]
use std::path::PathBuf;

/// Errors that can occur when loading a configuration file.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    Parse {
        path: String,
        source: toml::de::Error,
    },
    #[error("invalid value in config file {path}: {message}")]
    Validation { path: String, message: String },
}

#[derive(Deserialize, Default)]
pub struct FileConfig {
    pub port: Option<u16>,
    pub account_id: Option<String>,
    pub region: Option<String>,
    pub shard_limit: Option<u32>,
    pub create_stream_ms: Option<u64>,
    pub delete_stream_ms: Option<u64>,
    pub update_stream_ms: Option<u64>,
    pub iterator_ttl_seconds: Option<u64>,
    pub retention_check_interval_secs: Option<u64>,
    pub max_request_body_mb: Option<u64>,
    #[cfg(feature = "tls")]
    pub tls_cert: Option<PathBuf>,
    #[cfg(feature = "tls")]
    pub tls_key: Option<PathBuf>,
}

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
