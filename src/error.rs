use crate::constants;
use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone)]
pub struct KinesisError {
    pub error_type: String,
    pub message: Option<String>,
}

impl Serialize for KinesisError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let has_message = self.message.is_some();
        let len = if has_message { 2 } else { 1 };
        let mut map = serializer.serialize_map(Some(len))?;
        map.serialize_entry("__type", &self.error_type)?;
        if let Some(ref msg) = self.message {
            if self.error_type == constants::SERIALIZATION_EXCEPTION {
                map.serialize_entry("Message", msg)?;
            } else {
                map.serialize_entry("message", msg)?;
            }
        }
        map.end()
    }
}

#[derive(Debug, Clone)]
pub struct KinesisErrorResponse {
    pub status_code: u16,
    pub body: KinesisError,
}

impl fmt::Display for KinesisErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (HTTP {})", self.body.error_type, self.status_code)
    }
}

impl std::error::Error for KinesisErrorResponse {}

impl KinesisErrorResponse {
    pub fn new(status_code: u16, error_type: &str, message: Option<&str>) -> Self {
        Self {
            status_code,
            body: KinesisError {
                error_type: error_type.to_string(),
                message: message.map(|s| s.to_string()),
            },
        }
    }

    pub fn client_error(error_type: &str, message: Option<&str>) -> Self {
        Self::new(400, error_type, message)
    }

    pub fn server_error(error_type: Option<&str>, message: Option<&str>) -> Self {
        Self::new(500, error_type.unwrap_or("InternalFailure"), message)
    }

    pub fn serialization_error(msg: &str) -> Self {
        Self::new(400, constants::SERIALIZATION_EXCEPTION, Some(msg))
    }

    pub fn validation_error(msg: &str) -> Self {
        Self::new(400, constants::VALIDATION_EXCEPTION, Some(msg))
    }

    // --- Convenience message factories ---

    pub fn stream_not_found(name: &str, account_id: &str) -> Self {
        Self::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some(&format!(
                "Stream {name} under account {account_id} not found."
            )),
        )
    }

    pub fn stream_in_use(name: &str, account_id: &str) -> Self {
        Self::client_error(
            constants::RESOURCE_IN_USE,
            Some(&format!(
                "Stream {name} under account {account_id} already exists."
            )),
        )
    }

    pub fn stream_not_active(name: &str, account_id: &str) -> Self {
        Self::client_error(
            constants::RESOURCE_IN_USE,
            Some(&format!(
                "Stream {name} under account {account_id} is not ACTIVE."
            )),
        )
    }

    pub fn shard_not_found(shard_id: &str, stream_name: &str, account_id: &str) -> Self {
        Self::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some(&format!(
                "Shard {shard_id} in stream {stream_name} under account {account_id} does not exist"
            )),
        )
    }
}
