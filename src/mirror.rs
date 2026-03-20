//! Transparent traffic mirroring to a real AWS Kinesis (or compatible) endpoint.
//!
//! When configured via `--mirror-to`, [`Mirror`] asynchronously forwards
//! `PutRecord` and `PutRecords` requests to the mirror endpoint after the
//! local response has been sent. Failed requests are retried with configurable
//! exponential backoff. An optional `--mirror-diff` flag logs
//! response divergences for differential validation.

use crate::actions::Operation;
use crate::constants;
use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

/// Captured local response for diff comparison.
///
/// `None` means no body (empty 200), `Some(value)` means a JSON response body.
/// Only successful local dispatches are mirrored — failed dispatches are skipped.
pub type MirrorableResponse = Option<Value>;

/// Retry configuration for mirror forwarding.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts. `0` disables retries.
    pub max_retries: usize,
    /// Initial (minimum) backoff delay between retries.
    pub initial_backoff: Duration,
    /// Maximum backoff delay between retries.
    pub max_backoff: Duration,
}

impl RetryConfig {
    /// Default maximum retry attempts.
    pub const DEFAULT_MAX_RETRIES: usize = 3;
    /// Default initial backoff in milliseconds.
    pub const DEFAULT_INITIAL_BACKOFF_MS: u64 = 100;
    /// Default maximum backoff in milliseconds.
    pub const DEFAULT_MAX_BACKOFF_MS: u64 = 5000;
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: Self::DEFAULT_MAX_RETRIES,
            initial_backoff: Duration::from_millis(Self::DEFAULT_INITIAL_BACKOFF_MS),
            max_backoff: Duration::from_millis(Self::DEFAULT_MAX_BACKOFF_MS),
        }
    }
}

/// Error classification for mirror request forwarding.
#[derive(Debug)]
enum ForwardError {
    /// Transient error (connection failure, timeout, 5xx, 429) — eligible for retry.
    Transient(String),
    /// Permanent error (4xx except 429) — not retried.
    Permanent(String),
}

impl ForwardError {
    fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

impl std::fmt::Display for ForwardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transient(msg) => write!(f, "transient: {msg}"),
            Self::Permanent(msg) => write!(f, "permanent: {msg}"),
        }
    }
}

/// Async traffic mirror that forwards write operations to a remote endpoint.
pub struct Mirror {
    url: String,
    host: String,
    diff: bool,
    client: reqwest::Client,
    provider: Option<aws_credential_types::provider::SharedCredentialsProvider>,
    cached_credentials: tokio::sync::RwLock<Option<aws_credential_types::Credentials>>,
    region: String,
    semaphore: Arc<Semaphore>,
    retry_config: RetryConfig,
}

/// Error during SigV4 request signing.
#[derive(Debug)]
pub enum SignError {
    /// Failed to build signing parameters.
    Build(aws_sigv4::sign::v4::signing_params::BuildError),
    /// Failed to sign the request.
    Signing(aws_sigv4::http_request::SigningError),
    /// Failed to resolve credentials from the provider.
    Credentials(aws_credential_types::provider::error::CredentialsError),
}

impl std::fmt::Display for SignError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Build(e) => write!(f, "failed to build signing params: {e}"),
            Self::Signing(e) => write!(f, "signing failed: {e}"),
            Self::Credentials(e) => write!(f, "failed to resolve credentials: {e}"),
        }
    }
}

impl std::error::Error for SignError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Build(e) => Some(e),
            Self::Signing(e) => Some(e),
            Self::Credentials(e) => Some(e),
        }
    }
}

impl From<aws_sigv4::sign::v4::signing_params::BuildError> for SignError {
    fn from(e: aws_sigv4::sign::v4::signing_params::BuildError) -> Self {
        Self::Build(e)
    }
}

impl From<aws_sigv4::http_request::SigningError> for SignError {
    fn from(e: aws_sigv4::http_request::SigningError) -> Self {
        Self::Signing(e)
    }
}

impl From<aws_credential_types::provider::error::CredentialsError> for SignError {
    fn from(e: aws_credential_types::provider::error::CredentialsError) -> Self {
        Self::Credentials(e)
    }
}

impl Mirror {
    /// Default number of concurrent in-flight mirror requests.
    pub const DEFAULT_CONCURRENCY: usize = 64;

    /// Create a mirror using the AWS default credential provider chain.
    ///
    /// Resolves credentials via `aws-config`'s default chain (env vars,
    /// `~/.aws/credentials`, IMDS, ECS task roles, etc.) with automatic
    /// refresh on every signing call. Logs a warning if no provider is found.
    pub async fn new(
        endpoint: &str,
        diff: bool,
        region: &str,
        concurrency: usize,
        retry_config: RetryConfig,
    ) -> Self {
        let provider = Self::build_credentials_provider().await;
        if provider.is_none() {
            tracing::warn!(
                "no AWS credentials provider found, requests will be forwarded unsigned"
            );
        }
        Self::with_provider(endpoint, diff, region, provider, concurrency, retry_config)
    }

    /// Create a mirror with explicit static credentials (used in tests).
    pub fn with_credentials(
        endpoint: &str,
        diff: bool,
        region: &str,
        credentials: Option<aws_credential_types::Credentials>,
        concurrency: usize,
        retry_config: RetryConfig,
    ) -> Self {
        let provider =
            credentials.map(aws_credential_types::provider::SharedCredentialsProvider::new);
        Self::with_provider(endpoint, diff, region, provider, concurrency, retry_config)
    }

    /// Create a mirror with an explicit credentials provider.
    pub fn with_provider(
        endpoint: &str,
        diff: bool,
        region: &str,
        provider: Option<aws_credential_types::provider::SharedCredentialsProvider>,
        concurrency: usize,
        retry_config: RetryConfig,
    ) -> Self {
        let url = format!("{}/", endpoint.trim_end_matches('/'));
        let host = extract_host(&url);
        Self {
            url,
            host,
            diff,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("failed to build mirror HTTP client"),
            provider,
            cached_credentials: tokio::sync::RwLock::new(None),
            region: region.to_string(),
            semaphore: Arc::new(Semaphore::new(concurrency)),
            retry_config,
        }
    }

    /// Build a credentials provider using the AWS default provider chain.
    ///
    /// Covers env vars, `~/.aws/credentials`, IMDS, ECS task roles — all with
    /// automatic refresh, so STS temporary credentials are never stale.
    async fn build_credentials_provider()
    -> Option<aws_credential_types::provider::SharedCredentialsProvider> {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        config.credentials_provider()
    }

    /// Returns `true` if this operation should be mirrored.
    ///
    /// Only data-write operations (`PutRecord`, `PutRecords`) are mirrored.
    pub fn should_mirror(operation: &Operation) -> bool {
        matches!(operation, Operation::PutRecord | Operation::PutRecords)
    }

    /// Spawn a fire-and-forget task to forward the request to the mirror endpoint.
    pub fn spawn_forward(
        self: &Arc<Self>,
        target: String,
        content_type: String,
        body: Bytes,
        local_result: MirrorableResponse,
    ) {
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                tracing::warn!("backpressure: dropping mirrored request");
                return;
            }
        };
        let mirror = Arc::clone(self);
        tokio::spawn(async move {
            mirror
                .forward(&target, &content_type, body, local_result)
                .await;
            drop(permit);
        });
    }

    async fn forward(
        &self,
        target: &str,
        content_type: &str,
        body: Bytes,
        local_result: MirrorableResponse,
    ) {
        // Resolve credentials (cached with expiry-aware refresh) and sign once
        // before the retry loop. SigV4 signatures are valid for 5 minutes —
        // retries stay well within that window.
        let signed_headers = if let Some(ref provider) = self.provider {
            match self
                .sign_headers(content_type, target, &body, provider)
                .await
            {
                Ok(headers) => Some(headers),
                Err(e) => {
                    tracing::error!(error = %e, "signing failed");
                    return;
                }
            }
        } else {
            None
        };

        let send = || async {
            let mut request = self
                .client
                .post(&self.url)
                .header("Content-Type", content_type)
                .header("X-Amz-Target", target);

            if let Some(ref headers) = signed_headers {
                for (name, value) in headers {
                    request = request.header(name.as_str(), value.as_str());
                }
            }

            // Bytes::clone is cheap (Arc-backed, zero-copy).
            match request.body(body.clone()).send().await {
                Ok(response) => {
                    let status = response.status();
                    if status.is_server_error() || status.as_u16() == 429 {
                        Err(ForwardError::Transient(format!("HTTP {status}")))
                    } else if status.is_client_error() {
                        Err(ForwardError::Permanent(format!("HTTP {status}")))
                    } else {
                        Ok(response)
                    }
                }
                Err(e) => Err(ForwardError::Transient(e.to_string())),
            }
        };

        let result = if self.retry_config.max_retries == 0 {
            send().await
        } else {
            use backon::{ExponentialBuilder, Retryable};
            send.retry(
                ExponentialBuilder::default()
                    .with_min_delay(self.retry_config.initial_backoff)
                    .with_max_delay(self.retry_config.max_backoff)
                    .with_max_times(self.retry_config.max_retries),
            )
            .when(|e| e.is_transient())
            .notify(|e, dur| {
                tracing::warn!(error = %e, delay = ?dur, "retrying mirror request");
            })
            .await
        };

        match result {
            Ok(response) => {
                if self.diff {
                    let status = response.status().as_u16();
                    let response_ct = response
                        .headers()
                        .get("content-type")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("")
                        .to_string();
                    match response.bytes().await {
                        Ok(mirror_body) => {
                            self.diff_responses(
                                target,
                                local_result,
                                status,
                                &response_ct,
                                &mirror_body,
                            );
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "failed to read mirror response body")
                        }
                    }
                }
            }
            Err(ref e) if e.is_transient() => {
                tracing::error!(error = %e, "mirror request failed after retries");
            }
            Err(ref e) => {
                tracing::warn!(error = %e, "mirror request permanently failed");
            }
        }
    }

    /// Resolve credentials, using the cache if they haven't expired yet.
    /// Refreshes proactively 60 seconds before expiry.
    async fn resolve_credentials(
        &self,
        provider: &aws_credential_types::provider::SharedCredentialsProvider,
    ) -> Result<
        aws_credential_types::Credentials,
        aws_credential_types::provider::error::CredentialsError,
    > {
        use aws_credential_types::provider::ProvideCredentials;

        // Fast path: cached credentials still valid
        if let Some(creds) = self.cached_credentials.read().await.as_ref() {
            let near_expiry = creds.expiry().is_some_and(|exp| {
                exp.duration_since(std::time::SystemTime::now())
                    .unwrap_or_default()
                    < std::time::Duration::from_secs(60)
            });
            if !near_expiry {
                return Ok(creds.clone());
            }
        }

        // Slow path: resolve fresh credentials from the provider chain
        let creds = provider.provide_credentials().await?;
        *self.cached_credentials.write().await = Some(creds.clone());
        Ok(creds)
    }

    async fn sign_headers(
        &self,
        content_type: &str,
        target: &str,
        body: &[u8],
        provider: &aws_credential_types::provider::SharedCredentialsProvider,
    ) -> Result<Vec<(String, String)>, SignError> {
        use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
        use aws_sigv4::sign::v4;
        use std::time::SystemTime;

        let credentials = self.resolve_credentials(provider).await?;
        let identity = aws_smithy_runtime_api::client::identity::Identity::from(credentials);

        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name("kinesis")
            .time(SystemTime::now())
            .settings(SigningSettings::default())
            .build()?;

        let headers = [
            ("host", self.host.as_str()),
            ("content-type", content_type),
            ("x-amz-target", target),
        ];

        let signable = SignableRequest::new(
            "POST",
            &self.url,
            headers.iter().copied(),
            SignableBody::Bytes(body),
        )?;

        let (instructions, _) = sign(signable, &params.into())?.into_parts();

        Ok(instructions
            .headers()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect())
    }

    fn diff_responses(
        &self,
        target: &str,
        local: MirrorableResponse,
        mirror_status: u16,
        mirror_ct: &str,
        mirror_body: &[u8],
    ) {
        let operation = target.split('.').nth(1).unwrap_or(target);

        if mirror_status != 200 {
            tracing::warn!(
                operation,
                local = 200u16,
                mirror = mirror_status,
                "status divergence"
            );
        }

        let mirror_value = if mirror_body.is_empty() {
            None
        } else if mirror_ct.contains("cbor") {
            ciborium::from_reader::<ciborium::Value, _>(mirror_body)
                .ok()
                .map(|v| crate::server::cbor_to_json(&v))
        } else {
            serde_json::from_slice(mirror_body).ok()
        };

        let volatile_keys = [constants::SEQUENCE_NUMBER, constants::ENCRYPTION_TYPE];
        let local_stripped = local.map(|mut v| {
            strip_volatile_keys(&mut v, &volatile_keys);
            v
        });
        let mirror_stripped = mirror_value.map(|mut v| {
            strip_volatile_keys(&mut v, &volatile_keys);
            v
        });

        if local_stripped != mirror_stripped {
            let local_str = local_stripped
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default())
                .unwrap_or_else(|| "<empty>".to_string());
            let mirror_str = mirror_stripped
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default())
                .unwrap_or_else(|| "<empty>".to_string());
            tracing::warn!(operation, %local_str, %mirror_str, "body divergence");
        }
    }
}

fn extract_host(url_str: &str) -> String {
    match url::Url::parse(url_str) {
        Ok(parsed) => {
            let raw = parsed.host_str().unwrap_or(url_str);
            let host = if raw.contains(':') && !raw.starts_with('[') {
                format!("[{raw}]")
            } else {
                raw.to_string()
            };
            match parsed.port() {
                Some(port) => format!("{host}:{port}"),
                None => host,
            }
        }
        Err(_) => url_str.to_string(),
    }
}

fn strip_volatile_keys(val: &mut Value, keys: &[&str]) {
    match val {
        Value::Object(map) => {
            for key in keys {
                map.remove(*key);
            }
            for v in map.values_mut() {
                strip_volatile_keys(v, keys);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                strip_volatile_keys(item, keys);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_mirror_put_record() {
        assert!(Mirror::should_mirror(&Operation::PutRecord));
    }

    #[test]
    fn should_mirror_put_records() {
        assert!(Mirror::should_mirror(&Operation::PutRecords));
    }

    #[test]
    fn should_not_mirror_other_operations() {
        assert!(!Mirror::should_mirror(&Operation::DescribeStream));
        assert!(!Mirror::should_mirror(&Operation::CreateStream));
        assert!(!Mirror::should_mirror(&Operation::DeleteStream));
        assert!(!Mirror::should_mirror(&Operation::ListStreams));
        assert!(!Mirror::should_mirror(&Operation::GetRecords));
    }

    #[test]
    fn strip_volatile_keys_removes_sequence_number() {
        let mut val = serde_json::json!({
            "ShardId": "shardId-000000000000",
            "SequenceNumber": "12345"
        });
        strip_volatile_keys(&mut val, &["SequenceNumber"]);
        assert_eq!(
            val,
            serde_json::json!({
                "ShardId": "shardId-000000000000"
            })
        );
    }

    #[test]
    fn strip_volatile_keys_recursive() {
        let mut val = serde_json::json!({
            "Records": [
                {"SequenceNumber": "1", "Data": "abc"},
                {"SequenceNumber": "2", "Data": "def"}
            ]
        });
        strip_volatile_keys(&mut val, &["SequenceNumber"]);
        assert_eq!(
            val,
            serde_json::json!({
                "Records": [
                    {"Data": "abc"},
                    {"Data": "def"}
                ]
            })
        );
    }

    #[test]
    fn strip_volatile_keys_removes_encryption_type() {
        let mut val = serde_json::json!({
            "ShardId": "shardId-000000000000",
            "SequenceNumber": "12345",
            "EncryptionType": "KMS"
        });
        strip_volatile_keys(
            &mut val,
            &[constants::SEQUENCE_NUMBER, constants::ENCRYPTION_TYPE],
        );
        assert_eq!(
            val,
            serde_json::json!({
                "ShardId": "shardId-000000000000"
            })
        );
    }

    #[test]
    fn strip_volatile_keys_removes_encryption_type_in_records() {
        let mut val = serde_json::json!({
            "FailedRecordCount": 0,
            "EncryptionType": "KMS",
            "Records": [
                {"SequenceNumber": "1", "ShardId": "shardId-000000000000", "EncryptionType": "KMS"},
                {"SequenceNumber": "2", "ShardId": "shardId-000000000000", "EncryptionType": "KMS"}
            ]
        });
        strip_volatile_keys(
            &mut val,
            &[constants::SEQUENCE_NUMBER, constants::ENCRYPTION_TYPE],
        );
        assert_eq!(
            val,
            serde_json::json!({
                "FailedRecordCount": 0,
                "Records": [
                    {"ShardId": "shardId-000000000000"},
                    {"ShardId": "shardId-000000000000"}
                ]
            })
        );
    }

    #[test]
    fn extract_host_https() {
        assert_eq!(
            extract_host("https://kinesis.us-east-1.amazonaws.com"),
            "kinesis.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn extract_host_http_with_port() {
        assert_eq!(extract_host("http://localhost:4568"), "localhost:4568");
    }

    #[test]
    fn extract_host_with_path() {
        assert_eq!(
            extract_host("https://kinesis.us-east-1.amazonaws.com/"),
            "kinesis.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn extract_host_ipv6() {
        assert_eq!(extract_host("http://[::1]:4567"), "[::1]:4567");
    }

    #[test]
    fn extract_host_ipv6_no_port() {
        assert_eq!(extract_host("http://[::1]"), "[::1]");
    }

    #[test]
    fn forward_error_is_transient() {
        assert!(ForwardError::Transient("timeout".into()).is_transient());
        assert!(!ForwardError::Permanent("bad request".into()).is_transient());
    }

    #[test]
    fn retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff, Duration::from_millis(100));
        assert_eq!(config.max_backoff, Duration::from_millis(5000));
    }
}
