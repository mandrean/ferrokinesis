//! Transparent traffic mirroring to a real AWS Kinesis (or compatible) endpoint.
//!
//! When configured via `--mirror-to`, [`Mirror`] asynchronously forwards
//! `PutRecord` and `PutRecords` requests to the mirror endpoint after the
//! local response has been sent. An optional `--mirror-diff` flag logs
//! response divergences for differential validation.

use crate::actions::Operation;
use crate::constants;
use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Captured local response for diff comparison.
///
/// `None` means no body (empty 200), `Some(value)` means a JSON response body.
/// Only successful local dispatches are mirrored — failed dispatches are skipped.
pub type MirrorableResponse = Option<Value>;

/// Async traffic mirror that forwards write operations to a remote endpoint.
pub struct Mirror {
    url: String,
    host: String,
    diff: bool,
    client: reqwest::Client,
    credentials: Option<aws_credential_types::Credentials>,
    region: String,
    semaphore: Arc<Semaphore>,
}

/// Error during SigV4 request signing.
#[derive(Debug)]
pub enum SignError {
    /// Failed to build signing parameters.
    Build(aws_sigv4::sign::v4::signing_params::BuildError),
    /// Failed to sign the request.
    Signing(aws_sigv4::http_request::SigningError),
}

impl std::fmt::Display for SignError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Build(e) => write!(f, "failed to build signing params: {e}"),
            Self::Signing(e) => write!(f, "signing failed: {e}"),
        }
    }
}

impl std::error::Error for SignError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Build(e) => Some(e),
            Self::Signing(e) => Some(e),
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

impl Mirror {
    /// Default number of concurrent in-flight mirror requests.
    pub const DEFAULT_CONCURRENCY: usize = 64;

    /// Create a mirror that loads AWS credentials from environment variables.
    ///
    /// Looks for `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optionally
    /// `AWS_SESSION_TOKEN`. Logs a warning if credentials are not found.
    pub fn new(endpoint: &str, diff: bool, region: &str, concurrency: usize) -> Self {
        let credentials = Self::load_credentials();
        if credentials.is_none() {
            tracing::warn!("no AWS credentials found, requests will be forwarded unsigned");
        }
        Self::with_credentials(endpoint, diff, region, credentials, concurrency)
    }

    /// Create a mirror with explicit credentials.
    pub fn with_credentials(
        endpoint: &str,
        diff: bool,
        region: &str,
        credentials: Option<aws_credential_types::Credentials>,
        concurrency: usize,
    ) -> Self {
        let url = format!("{}/", endpoint.trim_end_matches('/'));
        let host = extract_host(&url);
        Self {
            url,
            host,
            diff,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("failed to build mirror HTTP client"),
            credentials,
            region: region.to_string(),
            semaphore: Arc::new(Semaphore::new(concurrency)),
        }
    }

    /// Load AWS credentials from environment variables once at startup.
    ///
    /// Credentials are captured at construction time and not refreshed. If the
    /// underlying env vars change (e.g. STS temporary credentials expire), the
    /// mirror will continue using the original values.
    fn load_credentials() -> Option<aws_credential_types::Credentials> {
        let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok()?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok()?;
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
        Some(aws_credential_types::Credentials::new(
            access_key,
            secret_key,
            session_token,
            None,
            "env",
        ))
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
        let mut request = self
            .client
            .post(&self.url)
            .header("Content-Type", content_type)
            .header("X-Amz-Target", target);

        if let Some(ref credentials) = self.credentials {
            match self.sign_headers(content_type, target, &body, credentials) {
                Ok(headers) => {
                    for (name, value) in headers {
                        request = request.header(name, value);
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "signing failed");
                    return;
                }
            }
        }

        match request.body(body).send().await {
            Ok(response) => {
                let status = response.status();
                if status.is_client_error() || status.is_server_error() {
                    tracing::warn!(status = status.as_u16(), "mirror endpoint returned error");
                }
                if self.diff {
                    let status = status.as_u16();
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
            Err(e) => tracing::warn!(error = %e, "mirror request failed"),
        }
    }

    fn sign_headers(
        &self,
        content_type: &str,
        target: &str,
        body: &[u8],
        credentials: &aws_credential_types::Credentials,
    ) -> Result<Vec<(String, String)>, SignError> {
        use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
        use aws_sigv4::sign::v4;
        use std::time::SystemTime;

        let identity =
            aws_smithy_runtime_api::client::identity::Identity::from(credentials.clone());

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
}
