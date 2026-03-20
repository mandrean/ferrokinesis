//! Error types returned by the Kinesis emulator.
//!
//! [`KinesisErrorResponse`] is the primary error type returned by action handlers.
//! It carries an HTTP status code and a [`KinesisError`] body that serializes to
//! the wire format expected by AWS SDK clients.
//!
//! ## Dual message-field casing
//!
//! Kinesis uses **dual casing** for the message field — this is intentional:
//!
//! | Constructor | Wire key | Use case |
//! |---|---|---|
//! | [`KinesisErrorResponse::client_error`] | `"message"` (lowercase) | Action / dispatch errors |
//! | [`KinesisErrorResponse::serialization_error`] | `"Message"` (uppercase) | Type-check errors |
//! | [`KinesisErrorResponse::validation_error`] | `"message"` (lowercase) | Constraint errors |

use crate::constants;
use alloc::format;
use alloc::string::{String, ToString};
use core::fmt;
use serde::Serialize;

/// The JSON body of a Kinesis error response.
///
/// Serialized as `{"__type": "...", "message": "..."}` for most errors, or
/// `{"__type": "...", "Message": "..."}` for [`constants::SERIALIZATION_EXCEPTION`]
/// (see the module-level note on dual casing).
#[derive(Debug, Clone)]
pub struct KinesisError {
    /// The Kinesis error type string (e.g. `"ResourceNotFoundException"`).
    pub error_type: String,
    /// Optional human-readable error message.
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
            // Kinesis uses dual casing for the message field — this is a real AWS quirk,
            // not a typo. SerializationException responses use the uppercase key "Message"
            // while every other error type uses lowercase "message". Clients that parse
            // error bodies must handle both spellings.
            if self.error_type == constants::SERIALIZATION_EXCEPTION {
                map.serialize_entry("Message", msg)?;
            } else {
                map.serialize_entry("message", msg)?;
            }
        }
        map.end()
    }
}

/// An HTTP error response in the Kinesis wire format.
///
/// Carries both the HTTP status code and the [`KinesisError`] body.
/// Implements [`core::error::Error`] so it can be used with `?` in async action handlers.
#[derive(Debug, Clone)]
pub struct KinesisErrorResponse {
    /// HTTP status code (e.g. `400`, `404`, `500`).
    pub status_code: u16,
    /// The error body that will be serialized into the response.
    pub body: KinesisError,
}

impl fmt::Display for KinesisErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (HTTP {})", self.body.error_type, self.status_code)
    }
}

impl core::error::Error for KinesisErrorResponse {}

impl KinesisErrorResponse {
    /// Constructs a new error response with an arbitrary status code, type, and message.
    pub fn new(status_code: u16, error_type: &str, message: Option<&str>) -> Self {
        Self {
            status_code,
            body: KinesisError {
                error_type: error_type.to_string(),
                message: message.map(|s| s.to_string()),
            },
        }
    }

    /// Constructs an HTTP 400 client error with the given type and optional message.
    pub fn client_error(error_type: &str, message: Option<&str>) -> Self {
        Self::new(400, error_type, message)
    }

    /// Constructs an HTTP 500 server error.
    ///
    /// If `error_type` is `None`, defaults to `"InternalFailure"`.
    pub fn server_error(error_type: Option<&str>, message: Option<&str>) -> Self {
        Self::new(500, error_type.unwrap_or("InternalFailure"), message)
    }

    /// Constructs an HTTP 400 `SerializationException`.
    ///
    /// The message is serialized with an uppercase `"Message"` key, matching
    /// the real AWS Kinesis wire format for type-check failures.
    pub fn serialization_error(msg: &str) -> Self {
        Self::new(400, constants::SERIALIZATION_EXCEPTION, Some(msg))
    }

    /// Constructs an HTTP 400 `ValidationException`.
    pub fn validation_error(msg: &str) -> Self {
        Self::new(400, constants::VALIDATION_EXCEPTION, Some(msg))
    }

    // --- Convenience message factories ---

    /// Constructs a `ResourceNotFoundException` for a missing stream.
    pub fn stream_not_found(name: &str, account_id: &str) -> Self {
        Self::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some(&format!(
                "Stream {name} under account {account_id} not found."
            )),
        )
    }

    /// Constructs a `ResourceInUseException` when a stream already exists.
    pub fn stream_in_use(name: &str, account_id: &str) -> Self {
        Self::client_error(
            constants::RESOURCE_IN_USE,
            Some(&format!(
                "Stream {name} under account {account_id} already exists."
            )),
        )
    }

    /// Constructs a `ResourceInUseException` when a stream is not in `ACTIVE` state.
    pub fn stream_not_active(name: &str, account_id: &str) -> Self {
        Self::client_error(
            constants::RESOURCE_IN_USE,
            Some(&format!(
                "Stream {name} under account {account_id} is not ACTIVE."
            )),
        )
    }

    /// Constructs a `ResourceNotFoundException` for a missing shard.
    pub fn shard_not_found(shard_id: &str, stream_name: &str, account_id: &str) -> Self {
        Self::client_error(
            constants::RESOURCE_NOT_FOUND,
            Some(&format!(
                "Shard {shard_id} in stream {stream_name} under account {account_id} does not exist"
            )),
        )
    }
}
