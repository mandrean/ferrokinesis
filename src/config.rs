use serde::Deserialize;
use std::path::Path;

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
    pub max_request_body_mb: Option<u64>,
}

pub fn load_config(path: &Path) -> Result<FileConfig, ConfigError> {
    let content = std::fs::read_to_string(path).map_err(|e| ConfigError::Read {
        path: path.display().to_string(),
        source: e,
    })?;
    toml::from_str(&content).map_err(|e| ConfigError::Parse {
        path: path.display().to_string(),
        source: e,
    })
}
