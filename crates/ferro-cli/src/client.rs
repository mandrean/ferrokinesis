use ferrokinesis_core::constants::{CONTENT_TYPE_JSON, KINESIS_API};
use ferrokinesis_core::operation::Operation;
use reqwest::StatusCode;
use serde_json::Value;
use std::time::Duration;

const AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234";
const AMZ_DATE: &str = "20150101T000000Z";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Api(#[from] ApiError),
    #[error("{0}")]
    InvalidInput(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid endpoint {0:?}: must start with http:// or https://")]
    InvalidEndpoint(String),
    #[error("event stream error: {0}")]
    EventStream(String),
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub error_type: String,
    pub message: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() {
            write!(f, "{} ({})", self.error_type, self.status)
        } else {
            write!(f, "{}: {} ({})", self.error_type, self.message, self.status)
        }
    }
}

impl std::error::Error for ApiError {}

#[derive(Clone, Debug)]
pub struct ApiClient {
    endpoint: String,
    client: reqwest::Client,
}

impl ApiClient {
    pub fn new(endpoint: &str, insecure: bool) -> Result<Self> {
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(Error::InvalidEndpoint(endpoint.to_string()));
        }

        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .danger_accept_invalid_certs(insecure)
            .build()?;

        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_string(),
            client,
        })
    }

    pub async fn call(&self, operation: Operation, body: &Value) -> Result<Value> {
        let response = self
            .call_response(operation, body, Some(Duration::from_secs(30)))
            .await?;
        self.decode_json(response).await
    }

    pub async fn call_response(
        &self,
        operation: Operation,
        body: &Value,
        timeout: Option<Duration>,
    ) -> Result<reqwest::Response> {
        let mut request = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", CONTENT_TYPE_JSON)
            .header(
                "X-Amz-Target",
                format!("{KINESIS_API}.{}", operation.as_str()),
            )
            .header("Authorization", AUTHORIZATION)
            .header("X-Amz-Date", AMZ_DATE)
            .json(body);

        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }

        let response = request.send().await?;
        if response.status().is_success() {
            Ok(response)
        } else {
            Err(self.decode_api_error(response).await.into())
        }
    }

    pub async fn get_health(&self) -> Result<String> {
        let response = self
            .client
            .get(format!("{}/_health/ready", self.endpoint))
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        if response.status().is_success() {
            Ok(response.text().await?)
        } else {
            Err(self.decode_api_error(response).await.into())
        }
    }

    async fn decode_json(&self, response: reqwest::Response) -> Result<Value> {
        let bytes = response.bytes().await?;
        if bytes.is_empty() {
            Ok(Value::Null)
        } else {
            Ok(serde_json::from_slice(&bytes)?)
        }
    }

    async fn decode_api_error(&self, response: reqwest::Response) -> ApiError {
        let status = response.status();
        let bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                return ApiError {
                    status,
                    error_type: "RequestFailed".into(),
                    message: error.to_string(),
                };
            }
        };

        let value = serde_json::from_slice::<Value>(&bytes).ok();
        let error_type = value
            .as_ref()
            .and_then(|value| value.get("__type"))
            .and_then(Value::as_str)
            .unwrap_or("RequestFailed")
            .to_string();
        let message = value
            .as_ref()
            .and_then(|value| value.get("message").or_else(|| value.get("Message")))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| String::from_utf8_lossy(&bytes).trim().to_string());

        ApiError {
            status,
            error_type,
            message,
        }
    }
}
