use crate::constants;
use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Serialize)]
pub struct KinesisError {
    pub __type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "Message")]
    pub message_upper: Option<String>,
}

#[derive(Debug, Clone)]
pub struct KinesisErrorResponse {
    pub status_code: u16,
    pub body: KinesisError,
}

impl fmt::Display for KinesisErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (HTTP {})", self.body.__type, self.status_code)
    }
}

impl std::error::Error for KinesisErrorResponse {}

impl KinesisErrorResponse {
    pub fn client_error(error_type: &str, message: Option<&str>) -> Self {
        Self {
            status_code: 400,
            body: KinesisError {
                __type: error_type.to_string(),
                message: message.map(|s| s.to_string()),
                message_upper: None,
            },
        }
    }

    pub fn server_error(error_type: Option<&str>, message: Option<&str>) -> Self {
        Self {
            status_code: 500,
            body: KinesisError {
                __type: error_type.unwrap_or("InternalFailure").to_string(),
                message: message.map(|s| s.to_string()),
                message_upper: None,
            },
        }
    }

    pub fn serialization_error(msg: &str) -> Self {
        Self {
            status_code: 400,
            body: KinesisError {
                __type: constants::SERIALIZATION_EXCEPTION.to_string(),
                message: None,
                message_upper: Some(msg.to_string()),
            },
        }
    }

    pub fn validation_error(msg: &str) -> Self {
        Self {
            status_code: 400,
            body: KinesisError {
                __type: constants::VALIDATION_EXCEPTION.to_string(),
                message: Some(msg.to_string()),
                message_upper: None,
            },
        }
    }
}
