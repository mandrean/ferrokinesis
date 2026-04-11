//! Chaos configuration, runtime controller, and operational HTTP endpoints.

use crate::actions::Operation;
use crate::error::KinesisErrorResponse;
use crate::store::{SequenceAllocation, Store};
use crate::util::current_time_ms;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

const LATENCY_SAMPLE_RATE: f64 = 0.01;
const REQUEST_THROTTLE_MESSAGE: &str = "Rate exceeded for shard.";
const INTERNAL_FAILURE_MESSAGE: &str = "Internal service failure.";
const ALL_OPERATIONS: &[Operation] = &[
    Operation::AddTagsToStream,
    Operation::CreateStream,
    Operation::DecreaseStreamRetentionPeriod,
    Operation::DeleteResourcePolicy,
    Operation::DeleteStream,
    Operation::DeregisterStreamConsumer,
    Operation::DescribeAccountSettings,
    Operation::DescribeLimits,
    Operation::DescribeStream,
    Operation::DescribeStreamConsumer,
    Operation::DescribeStreamSummary,
    Operation::DisableEnhancedMonitoring,
    Operation::EnableEnhancedMonitoring,
    Operation::GetRecords,
    Operation::GetResourcePolicy,
    Operation::GetShardIterator,
    Operation::IncreaseStreamRetentionPeriod,
    Operation::ListShards,
    Operation::ListStreamConsumers,
    Operation::ListStreams,
    Operation::ListTagsForResource,
    Operation::ListTagsForStream,
    Operation::MergeShards,
    Operation::PutRecord,
    Operation::PutRecords,
    Operation::PutResourcePolicy,
    Operation::RegisterStreamConsumer,
    Operation::RemoveTagsFromStream,
    Operation::SplitShard,
    Operation::StartStreamEncryption,
    Operation::StopStreamEncryption,
    Operation::SubscribeToShard,
    Operation::TagResource,
    Operation::UntagResource,
    Operation::UpdateAccountSettings,
    Operation::UpdateMaxRecordSize,
    Operation::UpdateShardCount,
    Operation::UpdateStreamMode,
    Operation::UpdateStreamWarmThroughput,
];

/// Errors that can occur while loading a chaos JSON configuration file.
#[derive(Debug, thiserror::Error)]
pub enum ChaosConfigError {
    /// The chaos config file could not be read from disk.
    #[error("failed to read chaos config {path}: {source}")]
    Read {
        /// Path to the chaos config file.
        path: String,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// The chaos config file contents could not be parsed as JSON.
    #[error("failed to parse chaos config {path}: {source}")]
    Parse {
        /// Path to the chaos config file.
        path: String,
        /// Underlying JSON parse error.
        source: serde_json::Error,
    },
    /// The chaos config failed semantic validation.
    #[error("invalid value in chaos config {path}: {message}")]
    Validation {
        /// Path to the chaos config file.
        path: String,
        /// Human-readable description of the constraint violation.
        message: String,
    },
}

/// Validated chaos configuration used to construct the runtime controller.
#[derive(Debug, Clone, Default)]
pub struct ChaosConfig {
    seed: u64,
    scenarios: Vec<ScenarioDefinition>,
}

impl ChaosConfig {
    /// Disable every loaded scenario, preserving the config for runtime toggling.
    pub fn disable_all(&mut self) {
        for scenario in &mut self.scenarios {
            scenario.enabled = false;
        }
    }
}

#[derive(Debug, Clone)]
struct ScenarioDefinition {
    id: String,
    operations: Vec<Operation>,
    enabled: bool,
    kind: ScenarioKind,
}

#[derive(Debug, Clone)]
enum ScenarioKind {
    ErrorRate {
        rate: f64,
        error: RequestChaosError,
    },
    ThroughputBurst {
        burst_duration_ms: u64,
        burst_interval_ms: u64,
    },
    Outage,
    Latency {
        p99_add_ms: u64,
    },
    PartialFailure {
        rate: f64,
        error: PartialFailureError,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum RequestChaosError {
    #[serde(rename = "InternalFailure")]
    InternalFailure,
    #[serde(rename = "ServiceUnavailable")]
    ServiceUnavailable,
}

impl RequestChaosError {
    fn as_str(self) -> &'static str {
        match self {
            Self::InternalFailure => "InternalFailure",
            Self::ServiceUnavailable => "ServiceUnavailable",
        }
    }

    fn to_response(self) -> KinesisErrorResponse {
        match self {
            Self::InternalFailure => KinesisErrorResponse::server_error(Some(self.as_str()), None),
            Self::ServiceUnavailable => KinesisErrorResponse::new(503, self.as_str(), None),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum PartialFailureError {
    #[serde(rename = "InternalFailure")]
    InternalFailure,
    #[serde(rename = "ProvisionedThroughputExceededException")]
    ProvisionedThroughputExceededException,
}

impl PartialFailureError {
    fn as_str(self) -> &'static str {
        match self {
            Self::InternalFailure => "InternalFailure",
            Self::ProvisionedThroughputExceededException => {
                "ProvisionedThroughputExceededException"
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawChaosConfig {
    seed: Option<u64>,
    scenarios: Vec<RawScenario>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RawScenario {
    ErrorRate {
        id: String,
        operations: Option<Vec<String>>,
        enabled: Option<bool>,
        rate: f64,
        #[serde(default)]
        error: Option<RequestChaosError>,
    },
    ThroughputBurst {
        id: String,
        operations: Option<Vec<String>>,
        enabled: Option<bool>,
        burst_duration_ms: u64,
        burst_interval_ms: u64,
    },
    Outage {
        id: String,
        operations: Option<Vec<String>>,
        enabled: Option<bool>,
    },
    Latency {
        id: String,
        operations: Option<Vec<String>>,
        enabled: Option<bool>,
        p99_add_ms: u64,
    },
    PartialFailure {
        id: String,
        operations: Option<Vec<String>>,
        enabled: Option<bool>,
        rate: f64,
        #[serde(default)]
        error: Option<PartialFailureError>,
    },
}

struct RuntimeScenario {
    definition: ScenarioDefinition,
    enabled: AtomicBool,
    enabled_at_ms: AtomicU64,
    salt: u64,
}

/// Request-level chaos decisions for a single Kinesis API call.
#[derive(Debug, Default)]
pub(crate) struct RequestPlan {
    pub(crate) terminal_error: Option<KinesisErrorResponse>,
    pub(crate) latency_ms: u64,
    pub(crate) chaos_affected: bool,
}

/// Per-record chaos failure information for `PutRecords`.
#[derive(Debug, Clone)]
pub(crate) struct PutRecordsFailure {
    pub(crate) error_code: String,
    pub(crate) error_message: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ChaosApiError {
    #[error("unknown scenario ids: {0}")]
    UnknownIds(String),
}

#[derive(Debug, Serialize)]
pub(crate) struct ChaosStatus {
    seed: u64,
    scenarios: Vec<ScenarioStatus>,
}

#[derive(Debug, Serialize)]
struct ScenarioStatus {
    id: String,
    #[serde(rename = "type")]
    scenario_type: &'static str,
    operations: Vec<String>,
    enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    burst_duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    burst_interval_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    p99_add_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct ToggleRequest {
    ids: Option<Vec<String>>,
}

/// Runtime chaos controller shared across handlers.
pub(crate) struct ChaosController {
    seed: u64,
    request_counter: AtomicU64,
    partial_failure_counter: AtomicU64,
    scenarios: Vec<RuntimeScenario>,
}

impl ChaosController {
    pub(crate) fn new(config: ChaosConfig) -> Self {
        let started_at_ms = current_time_ms();
        let scenarios = config
            .scenarios
            .into_iter()
            .map(|definition| RuntimeScenario {
                salt: hash_string(&definition.id),
                enabled: AtomicBool::new(definition.enabled),
                enabled_at_ms: AtomicU64::new(if definition.enabled { started_at_ms } else { 0 }),
                definition,
            })
            .collect();

        Self {
            seed: config.seed,
            request_counter: AtomicU64::new(0),
            partial_failure_counter: AtomicU64::new(0),
            scenarios,
        }
    }

    pub(crate) fn request_plan(&self, operation: Operation) -> RequestPlan {
        let ordinal = self.request_counter.fetch_add(1, Ordering::Relaxed);
        let now_ms = current_time_ms();
        let mut plan = RequestPlan::default();

        for scenario in &self.scenarios {
            if !scenario.enabled.load(Ordering::Relaxed)
                || !scenario.definition.operations.contains(&operation)
            {
                continue;
            }
            if matches!(scenario.definition.kind, ScenarioKind::Outage) {
                plan.terminal_error = Some(RequestChaosError::ServiceUnavailable.to_response());
                plan.chaos_affected = true;
                break;
            }
        }

        if plan.terminal_error.is_none() {
            for scenario in &self.scenarios {
                if !scenario.enabled.load(Ordering::Relaxed)
                    || !scenario.definition.operations.contains(&operation)
                {
                    continue;
                }
                if let ScenarioKind::ThroughputBurst {
                    burst_duration_ms,
                    burst_interval_ms,
                } = scenario.definition.kind
                    && in_burst(
                        scenario.enabled_at_ms.load(Ordering::Relaxed),
                        now_ms,
                        burst_duration_ms,
                        burst_interval_ms,
                    )
                {
                    plan.terminal_error = Some(provisioned_throughput_error());
                    plan.chaos_affected = true;
                    break;
                }
            }
        }

        if plan.terminal_error.is_none() {
            for scenario in &self.scenarios {
                if !scenario.enabled.load(Ordering::Relaxed)
                    || !scenario.definition.operations.contains(&operation)
                {
                    continue;
                }
                if let ScenarioKind::ErrorRate { rate, error } = scenario.definition.kind
                    && sample(self.seed, scenario.salt, ordinal, 0, rate)
                {
                    plan.terminal_error = Some(error.to_response());
                    plan.chaos_affected = true;
                    break;
                }
            }
        }

        for scenario in &self.scenarios {
            if !scenario.enabled.load(Ordering::Relaxed)
                || !scenario.definition.operations.contains(&operation)
            {
                continue;
            }
            if let ScenarioKind::Latency { p99_add_ms } = scenario.definition.kind
                && sample(self.seed, scenario.salt, ordinal, 1, LATENCY_SAMPLE_RATE)
            {
                plan.latency_ms += p99_add_ms;
                plan.chaos_affected = true;
            }
        }

        plan
    }

    pub(crate) fn put_records_failures(
        &self,
        stream_name: &str,
        account_id: &str,
        allocations: &[SequenceAllocation],
    ) -> Vec<Option<PutRecordsFailure>> {
        let ordinal = self.partial_failure_counter.fetch_add(1, Ordering::Relaxed);
        let mut failures = vec![None; allocations.len()];

        for (record_ix, alloc) in allocations.iter().enumerate() {
            for scenario in &self.scenarios {
                if !scenario.enabled.load(Ordering::Relaxed)
                    || !scenario
                        .definition
                        .operations
                        .contains(&Operation::PutRecords)
                {
                    continue;
                }
                if let ScenarioKind::PartialFailure { rate, error } = scenario.definition.kind
                    && sample(self.seed, scenario.salt, ordinal, record_ix as u64, rate)
                {
                    failures[record_ix] = Some(PutRecordsFailure {
                        error_code: error.as_str().to_string(),
                        error_message: match error {
                            PartialFailureError::InternalFailure => {
                                INTERNAL_FAILURE_MESSAGE.to_string()
                            }
                            PartialFailureError::ProvisionedThroughputExceededException => {
                                format!(
                                    "Rate exceeded for shard {} in stream {} under account {}.",
                                    alloc.shard_id, stream_name, account_id
                                )
                            }
                        },
                    });
                    break;
                }
            }
        }

        failures
    }

    pub(crate) fn status(&self) -> ChaosStatus {
        ChaosStatus {
            seed: self.seed,
            scenarios: self
                .scenarios
                .iter()
                .map(|scenario| ScenarioStatus {
                    id: scenario.definition.id.clone(),
                    scenario_type: scenario.definition.kind.type_name(),
                    operations: scenario
                        .definition
                        .operations
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                    enabled: scenario.enabled.load(Ordering::Relaxed),
                    error: scenario.definition.kind.error_name().map(str::to_string),
                    rate: scenario.definition.kind.rate(),
                    burst_duration_ms: scenario.definition.kind.burst_duration_ms(),
                    burst_interval_ms: scenario.definition.kind.burst_interval_ms(),
                    p99_add_ms: scenario.definition.kind.p99_add_ms(),
                })
                .collect(),
        }
    }

    pub(crate) fn set_enabled(
        &self,
        ids: Option<&[String]>,
        enabled: bool,
    ) -> Result<ChaosStatus, ChaosApiError> {
        let now_ms = current_time_ms();
        match ids {
            Some(ids) if !ids.is_empty() => {
                let requested: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
                let known: BTreeSet<&str> = self
                    .scenarios
                    .iter()
                    .map(|scenario| scenario.definition.id.as_str())
                    .collect();
                let unknown: Vec<&str> = requested.difference(&known).copied().collect();
                if !unknown.is_empty() {
                    return Err(ChaosApiError::UnknownIds(unknown.join(", ")));
                }

                for scenario in &self.scenarios {
                    if requested.contains(scenario.definition.id.as_str()) {
                        scenario
                            .enabled_at_ms
                            .store(if enabled { now_ms } else { 0 }, Ordering::Relaxed);
                        scenario.enabled.store(enabled, Ordering::Relaxed);
                    }
                }
            }
            _ => {
                for scenario in &self.scenarios {
                    scenario
                        .enabled_at_ms
                        .store(if enabled { now_ms } else { 0 }, Ordering::Relaxed);
                    scenario.enabled.store(enabled, Ordering::Relaxed);
                }
            }
        }

        Ok(self.status())
    }
}

impl ScenarioKind {
    fn type_name(&self) -> &'static str {
        match self {
            Self::ErrorRate { .. } => "error_rate",
            Self::ThroughputBurst { .. } => "throughput_burst",
            Self::Outage => "outage",
            Self::Latency { .. } => "latency",
            Self::PartialFailure { .. } => "partial_failure",
        }
    }

    fn error_name(&self) -> Option<&'static str> {
        match self {
            Self::ErrorRate { error, .. } => Some(error.as_str()),
            Self::PartialFailure { error, .. } => Some(error.as_str()),
            _ => None,
        }
    }

    fn rate(&self) -> Option<f64> {
        match self {
            Self::ErrorRate { rate, .. } | Self::PartialFailure { rate, .. } => Some(*rate),
            _ => None,
        }
    }

    fn burst_duration_ms(&self) -> Option<u64> {
        match self {
            Self::ThroughputBurst {
                burst_duration_ms, ..
            } => Some(*burst_duration_ms),
            _ => None,
        }
    }

    fn burst_interval_ms(&self) -> Option<u64> {
        match self {
            Self::ThroughputBurst {
                burst_interval_ms, ..
            } => Some(*burst_interval_ms),
            _ => None,
        }
    }

    fn p99_add_ms(&self) -> Option<u64> {
        match self {
            Self::Latency { p99_add_ms } => Some(*p99_add_ms),
            _ => None,
        }
    }
}

/// Load and validate a chaos JSON configuration file.
pub fn load_chaos_config(path: &Path) -> Result<ChaosConfig, ChaosConfigError> {
    let content = std::fs::read_to_string(path).map_err(|source| ChaosConfigError::Read {
        path: path.display().to_string(),
        source,
    })?;
    let raw: RawChaosConfig =
        serde_json::from_str(&content).map_err(|source| ChaosConfigError::Parse {
            path: path.display().to_string(),
            source,
        })?;

    let mut ids = BTreeSet::new();
    let mut scenarios = Vec::with_capacity(raw.scenarios.len());
    for raw_scenario in raw.scenarios {
        let definition = build_scenario_definition(raw_scenario).map_err(|message| {
            ChaosConfigError::Validation {
                path: path.display().to_string(),
                message,
            }
        })?;
        if !ids.insert(definition.id.clone()) {
            return Err(ChaosConfigError::Validation {
                path: path.display().to_string(),
                message: format!("duplicate scenario id {:?}", definition.id),
            });
        }
        scenarios.push(definition);
    }

    Ok(ChaosConfig {
        seed: raw.seed.unwrap_or_default(),
        scenarios,
    })
}

pub(crate) async fn status(State(store): State<Store>) -> Response {
    axum::Json(store.chaos_status()).into_response()
}

pub(crate) async fn enable(State(store): State<Store>, body: Bytes) -> Response {
    toggle(store, body, true).await
}

pub(crate) async fn disable(State(store): State<Store>, body: Bytes) -> Response {
    toggle(store, body, false).await
}

async fn toggle(store: Store, body: Bytes, enabled: bool) -> Response {
    let request = if body.is_empty() {
        ToggleRequest::default()
    } else {
        match serde_json::from_slice::<ToggleRequest>(&body) {
            Ok(request) => request,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(serde_json::json!({
                        "message": format!("invalid chaos toggle request: {err}")
                    })),
                )
                    .into_response();
            }
        }
    };

    match store.set_chaos_enabled(request.ids.as_deref(), enabled) {
        Ok(status) => (StatusCode::OK, axum::Json(status)).into_response(),
        Err(err) => (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({ "message": err.to_string() })),
        )
            .into_response(),
    }
}

fn build_scenario_definition(raw: RawScenario) -> Result<ScenarioDefinition, String> {
    match raw {
        RawScenario::ErrorRate {
            id,
            operations,
            enabled,
            rate,
            error,
        } => Ok(ScenarioDefinition {
            operations: parse_operations(operations, ALL_OPERATIONS, None)?,
            id,
            enabled: enabled.unwrap_or(false),
            kind: ScenarioKind::ErrorRate {
                rate: validate_rate(rate)?,
                error: error.unwrap_or(RequestChaosError::InternalFailure),
            },
        }),
        RawScenario::ThroughputBurst {
            id,
            operations,
            enabled,
            burst_duration_ms,
            burst_interval_ms,
        } => Ok(ScenarioDefinition {
            operations: parse_operations(
                operations,
                &[Operation::PutRecord, Operation::PutRecords],
                Some(&[
                    Operation::PutRecord,
                    Operation::PutRecords,
                    Operation::GetRecords,
                ]),
            )?,
            id,
            enabled: enabled.unwrap_or(false),
            kind: ScenarioKind::ThroughputBurst {
                burst_duration_ms: validate_positive(
                    burst_duration_ms,
                    "burst_duration_ms must be greater than 0",
                )?,
                burst_interval_ms: validate_positive(
                    burst_interval_ms,
                    "burst_interval_ms must be greater than 0",
                )?,
            },
        }),
        RawScenario::Outage {
            id,
            operations,
            enabled,
        } => Ok(ScenarioDefinition {
            operations: parse_operations(operations, ALL_OPERATIONS, None)?,
            id,
            enabled: enabled.unwrap_or(false),
            kind: ScenarioKind::Outage,
        }),
        RawScenario::Latency {
            id,
            operations,
            enabled,
            p99_add_ms,
        } => Ok(ScenarioDefinition {
            operations: parse_operations(operations, ALL_OPERATIONS, None)?,
            id,
            enabled: enabled.unwrap_or(false),
            kind: ScenarioKind::Latency {
                p99_add_ms: validate_positive(p99_add_ms, "p99_add_ms must be greater than 0")?,
            },
        }),
        RawScenario::PartialFailure {
            id,
            operations,
            enabled,
            rate,
            error,
        } => Ok(ScenarioDefinition {
            operations: parse_operations(
                operations,
                &[Operation::PutRecords],
                Some(&[Operation::PutRecords]),
            )?,
            id,
            enabled: enabled.unwrap_or(false),
            kind: ScenarioKind::PartialFailure {
                rate: validate_rate(rate)?,
                error: error.unwrap_or(PartialFailureError::InternalFailure),
            },
        }),
    }
}

fn parse_operations(
    raw: Option<Vec<String>>,
    defaults: &[Operation],
    allowed: Option<&[Operation]>,
) -> Result<Vec<Operation>, String> {
    let parsed = match raw {
        Some(operations) if !operations.is_empty() => operations
            .into_iter()
            .map(|operation| {
                operation
                    .parse::<Operation>()
                    .map_err(|_| format!("unknown operation {:?}", operation))
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => defaults.to_vec(),
    };

    if let Some(allowed) = allowed {
        for operation in &parsed {
            if !allowed.contains(operation) {
                return Err(format!(
                    "operation {operation} is not allowed for this scenario"
                ));
            }
        }
    }

    Ok(parsed)
}

fn validate_positive(value: u64, message: &str) -> Result<u64, String> {
    if value == 0 {
        Err(message.to_string())
    } else {
        Ok(value)
    }
}

fn validate_rate(rate: f64) -> Result<f64, String> {
    if rate > 0.0 && rate <= 1.0 {
        Ok(rate)
    } else {
        Err(format!("rate must be in (0.0, 1.0], got {rate}"))
    }
}

fn provisioned_throughput_error() -> KinesisErrorResponse {
    KinesisErrorResponse::client_error(
        "ProvisionedThroughputExceededException",
        Some(REQUEST_THROTTLE_MESSAGE),
    )
}

fn in_burst(started_at_ms: u64, now_ms: u64, duration_ms: u64, interval_ms: u64) -> bool {
    if started_at_ms == 0 {
        return false;
    }
    let elapsed = now_ms.saturating_sub(started_at_ms);
    let window = elapsed % interval_ms;
    window < duration_ms.min(interval_ms)
}

fn sample(seed: u64, scenario_salt: u64, ordinal: u64, extra: u64, rate: f64) -> bool {
    if rate >= 1.0 {
        return true;
    }

    let mixed =
        splitmix64(seed ^ scenario_salt ^ ordinal.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ extra);
    let threshold = (rate * u64::MAX as f64) as u64;
    mixed <= threshold
}

fn hash_string(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_operations_uses_defaults_for_empty_lists() {
        let operations = parse_operations(Some(vec![]), &[Operation::PutRecords], None).unwrap();
        assert_eq!(operations, vec![Operation::PutRecords]);
    }

    #[test]
    fn validate_rate_rejects_out_of_range_values() {
        assert!(validate_rate(0.0).is_err());
        assert!(validate_rate(1.1).is_err());
    }

    #[test]
    fn throughput_burst_defaults_to_write_operations() {
        let definition = build_scenario_definition(RawScenario::ThroughputBurst {
            id: "burst".into(),
            operations: None,
            enabled: Some(true),
            burst_duration_ms: 100,
            burst_interval_ms: 1_000,
        })
        .unwrap();

        assert_eq!(
            definition.operations,
            vec![Operation::PutRecord, Operation::PutRecords]
        );
    }

    #[test]
    fn partial_failure_rejects_non_put_records_operations() {
        let err = build_scenario_definition(RawScenario::PartialFailure {
            id: "partial".into(),
            operations: Some(vec!["PutRecord".into()]),
            enabled: None,
            rate: 0.5,
            error: None,
        })
        .unwrap_err();

        assert!(err.contains("not allowed"));
    }
}
