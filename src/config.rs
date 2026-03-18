use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize, Default)]
pub struct FileConfig {
    pub port: Option<u16>,
    pub account_id: Option<String>,
    pub region: Option<String>,
    pub shard_limit: Option<u32>,
    pub create_stream_ms: Option<u64>,
    pub delete_stream_ms: Option<u64>,
    pub update_stream_ms: Option<u64>,
    pub max_request_body_mb: Option<u64>,
}

pub fn load_config(path: &Path) -> FileConfig {
    let content = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read config file {}: {e}", path.display()));
    toml::from_str(&content)
        .unwrap_or_else(|e| panic!("failed to parse config file {}: {e}", path.display()))
}
