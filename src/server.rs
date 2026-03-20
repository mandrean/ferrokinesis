//! Axum HTTP handler implementing the Kinesis wire protocol.
//!
//! [`handler`] is the Axum fallback handler that accepts all `POST /` requests.
//! It parses the `X-Amz-Target` header to determine the operation, negotiates
//! content type between JSON (`application/x-amz-json-1.1`) and CBOR
//! (`application/x-amz-cbor-1.1`), runs the validation pipeline, and routes
//! to [`crate::actions::dispatch`].
//!
//! [`kinesis_413_middleware`] intercepts bare 413 responses from Axum's body-limit
//! layer and rewraps them as Kinesis-shaped `SerializationException` errors.

use crate::actions::{self, Operation};
use crate::capture::{CaptureOp, CaptureRecordRef, CaptureWriter};
use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::mirror::Mirror;
use crate::store::Store;
use crate::util::current_time_ms;
use crate::validation;
use axum::body::Bytes;
use axum::extract::{Extension, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use serde::Serialize;
use serde_json::{Value, json};
use std::borrow::Cow;
use std::sync::Arc;
use tracing::Instrument;

/// Axum fallback handler implementing the Kinesis wire protocol.
///
/// Accepts all `POST /` requests and processes them as Kinesis API calls.
/// Parses `X-Amz-Target` to determine the operation, negotiates content type
/// (JSON vs CBOR), validates the request body, and dispatches to the appropriate
/// action handler via [`crate::actions::dispatch`].
///
/// `SubscribeToShard` is handled separately via `execute_streaming` to support
/// HTTP/2 event-stream responses.
///
/// # Errors
///
/// - HTTP 400 — client errors (invalid arguments, serialization exceptions, etc.)
/// - HTTP 403 — missing or malformed auth headers
/// - HTTP 404 — unknown operation or service
/// - HTTP 500 — internal server errors
pub async fn handler(
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    State(store): State<Store>,
    mirror: Option<Extension<Arc<Mirror>>>,
    body: Bytes,
) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();

    let mut response_headers = HeaderMap::new();
    response_headers.insert("x-amzn-RequestId", request_id.parse().unwrap());

    let has_origin = headers.get("origin").is_some();

    if method != Method::OPTIONS || !has_origin {
        let id2 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            rand::random::<[u8; 72]>(),
        );
        response_headers.insert("x-amz-id-2", id2.parse().unwrap());
    }

    // CORS handling
    if has_origin {
        response_headers.insert("Access-Control-Allow-Origin", "*".parse().unwrap());

        if method == Method::OPTIONS {
            if let Some(req_headers) = headers.get("access-control-request-headers") {
                response_headers.insert("Access-Control-Allow-Headers", req_headers.clone());
            }
            if let Some(req_method) = headers.get("access-control-request-method") {
                response_headers.insert("Access-Control-Allow-Methods", req_method.clone());
            }
            response_headers.insert("Access-Control-Max-Age", "172800".parse().unwrap());
            response_headers.insert("Content-Length", "0".parse().unwrap());
            return (StatusCode::OK, response_headers, "").into_response();
        }

        response_headers.insert(
            "Access-Control-Expose-Headers",
            "x-amzn-RequestId,x-amzn-ErrorType,x-amz-request-id,x-amz-id-2,x-amzn-ErrorMessage,Date".parse().unwrap(),
        );
    }

    // Non-POST methods
    if method != Method::POST {
        let mut h = response_headers.clone();
        h.insert(
            "x-amzn-ErrorType",
            constants::ACCESS_DENIED.parse().unwrap(),
        );
        return send_xml_error(
            h,
            constants::ACCESS_DENIED,
            "Unable to determine service/operation name to be authorized",
            403,
        );
    }

    // Parse content type
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();

    let content_valid = matches!(
        content_type,
        "application/x-amz-json-1.1" | "application/x-amz-cbor-1.1" | "application/json"
    );

    // Parse target
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    let parts: Vec<&str> = target.splitn(2, '.').collect();
    let service = parts.first().copied().unwrap_or("");
    let operation_str = if parts.len() > 1 { parts[1] } else { "" };

    let service_valid = service == constants::KINESIS_API;
    let operation = operation_str.parse::<Operation>().ok();
    let operation_valid = operation.is_some();

    let response_content_type = if content_type == constants::CONTENT_TYPE_JSON {
        constants::CONTENT_TYPE_JSON
    } else {
        constants::CONTENT_TYPE_CBOR
    };

    // Check body
    if body.is_empty() {
        let error_type = if service_valid && operation_valid {
            constants::SERIALIZATION_EXCEPTION
        } else {
            constants::UNKNOWN_OPERATION
        };
        let err = KinesisErrorResponse::client_error(error_type, None);
        return send_kinesis_error(&response_headers, response_content_type, &err);
    }

    if !content_valid {
        if service.is_empty() || operation_str.is_empty() {
            let mut h = response_headers.clone();
            h.insert(
                "x-amzn-ErrorType",
                constants::ACCESS_DENIED.parse().unwrap(),
            );
            return send_xml_error(
                h,
                constants::ACCESS_DENIED,
                "Unable to determine service/operation name to be authorized",
                403,
            );
        }
        let mut h = response_headers.clone();
        h.insert(
            "x-amzn-ErrorType",
            constants::UNKNOWN_OPERATION.parse().unwrap(),
        );
        return send_xml_error_code(h, constants::UNKNOWN_OPERATION, 404);
    }

    // Parse body
    let data: Option<Value> = if content_type == constants::CONTENT_TYPE_CBOR {
        // Parse via ciborium::Value to handle CBOR byte strings (major type 2),
        // which SDK v2 clients send for Blob fields like Data.
        ciborium::from_reader::<ciborium::Value, _>(&body[..])
            .ok()
            .map(|v| cbor_to_json(&v))
    } else {
        serde_json::from_slice(&body).ok()
    };

    let data = match data {
        Some(Value::Object(map)) => Value::Object(map),
        Some(_) | None => {
            if content_type == "application/json" {
                return send_json_response(
                    response_headers.clone(),
                    "application/json",
                    &json!({
                        "Output": {"__type": "com.amazon.coral.service#SerializationException"},
                        "Version": "1.0",
                    }),
                    400,
                );
            }
            let err = KinesisErrorResponse::client_error(constants::SERIALIZATION_EXCEPTION, None);
            return send_kinesis_error(&response_headers, response_content_type, &err);
        }
    };

    // After this point, application/json doesn't progress further
    if content_type == "application/json" {
        return send_json_response(
            response_headers.clone(),
            "application/json",
            &json!({
                "Output": {"__type": "com.amazon.coral.service#UnknownOperationException"},
                "Version": "1.0",
            }),
            404,
        );
    }

    let Some(operation) = operation else {
        let err = KinesisErrorResponse::client_error(constants::UNKNOWN_OPERATION, None);
        return send_kinesis_error(&response_headers, response_content_type, &err);
    };

    if !service_valid {
        let err = KinesisErrorResponse::client_error(constants::UNKNOWN_OPERATION, None);
        return send_kinesis_error(&response_headers, response_content_type, &err);
    }

    // Auth checking
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let query_string = uri.query().unwrap_or("");
    let auth_query = query_string.contains("X-Amz-Algorithm");

    if auth_header.is_some() && auth_query {
        return send_error_response(
            &response_headers,
            content_valid,
            response_content_type,
            constants::INVALID_SIGNATURE,
            "Found both 'X-Amz-Algorithm' as a query-string param and 'Authorization' as HTTP header.",
            400,
        );
    }

    if auth_header.is_none() && !auth_query {
        return send_error_response(
            &response_headers,
            content_valid,
            response_content_type,
            constants::MISSING_AUTH_TOKEN,
            "Missing Authentication Token",
            400,
        );
    }

    if let Some(auth) = auth_header {
        let mut msg = String::new();
        let auth_params: std::collections::HashMap<String, String> = auth
            .split([',', ' '])
            .skip(1)
            .filter(|s| !s.is_empty())
            .filter_map(|s| {
                let kv: Vec<&str> = s.trim().splitn(2, '=').collect();
                if kv.len() == 2 {
                    Some((kv[0].to_string(), kv[1].to_string()))
                } else {
                    None
                }
            })
            .collect();

        for param in ["Credential", "Signature", "SignedHeaders"] {
            if !auth_params.contains_key(param) {
                msg += &format!("Authorization header requires '{param}' parameter. ");
            }
        }
        if !headers.contains_key("x-amz-date") && !headers.contains_key("date") {
            msg += "Authorization header requires existence of either a 'X-Amz-Date' or a 'Date' header. ";
        }
        if !msg.is_empty() {
            msg += &format!("Authorization={auth}");
            return send_error_response(
                &response_headers,
                content_valid,
                response_content_type,
                constants::INCOMPLETE_SIGNATURE,
                &msg,
                403,
            );
        }
    } else {
        // Query auth
        let query_params: std::collections::HashMap<String, String> = uri
            .query()
            .unwrap_or("")
            .split('&')
            .filter_map(|s| {
                let kv: Vec<&str> = s.splitn(2, '=').collect();
                if kv.len() == 2 {
                    Some((kv[0].to_string(), kv[1].to_string()))
                } else if !kv[0].is_empty() {
                    Some((kv[0].to_string(), String::new()))
                } else {
                    None
                }
            })
            .collect();

        let mut msg = String::new();
        for param in [
            "X-Amz-Algorithm",
            "X-Amz-Credential",
            "X-Amz-Signature",
            "X-Amz-SignedHeaders",
            "X-Amz-Date",
        ] {
            if !query_params.contains_key(param) || query_params[param].is_empty() {
                msg += &format!("AWS query-string parameters must include '{param}'. ");
            }
        }
        if !msg.is_empty() {
            msg += "Re-examine the query-string parameters.";
            return send_error_response(
                &response_headers,
                content_valid,
                response_content_type,
                constants::INCOMPLETE_SIGNATURE,
                &msg,
                403,
            );
        }
    }

    // Validate request data
    let validation_rules = operation.validation_rules();
    let field_refs: Vec<(&str, &validation::FieldDef)> =
        validation_rules.iter().map(|(k, v)| (*k, v)).collect();

    let data = match validation::check_types(&data, &field_refs) {
        Ok(d) => d,
        Err(err) => {
            return send_kinesis_error(&response_headers, response_content_type, &err);
        }
    };

    if let Err(err) = validation::check_validations(&data, &field_refs, None) {
        return send_kinesis_error(&response_headers, response_content_type, &err);
    }

    let span = tracing::info_span!("kinesis", %operation, %request_id);

    // Handle SubscribeToShard separately (streaming response)
    if operation == Operation::SubscribeToShard {
        return match actions::subscribe_to_shard::execute_streaming(
            &store,
            data,
            response_content_type,
        )
        .instrument(span.clone())
        .await
        {
            Ok(body) => {
                tracing::debug!(parent: &span, "ok");
                response_headers.insert(
                    "Content-Type",
                    "application/vnd.amazon.eventstream".parse().unwrap(),
                );
                (StatusCode::OK, response_headers, body).into_response()
            }
            Err(ref err) => {
                log_and_send_error(&span, &response_headers, response_content_type, err)
            }
        };
    }

    // Extract only the fields needed for capture before dispatch() moves data.
    // Uses direct &str extraction (not json!() cloning) to stay on the zero-copy path.
    let capture_ctx = if store.capture_writer.is_some()
        && matches!(operation, Operation::PutRecord | Operation::PutRecords)
    {
        let stream = store.resolve_stream_name(&data).unwrap_or_default();
        let input = match operation {
            Operation::PutRecord => CaptureInput::Single {
                partition_key: data[constants::PARTITION_KEY]
                    .as_str()
                    .unwrap_or("")
                    .to_owned(),
                data: data[constants::DATA].as_str().unwrap_or("").to_owned(),
                explicit_hash_key: data[constants::EXPLICIT_HASH_KEY]
                    .as_str()
                    .map(str::to_owned),
            },
            Operation::PutRecords => {
                let entries = data[constants::RECORDS]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .map(|r| CaptureInputEntry {
                                partition_key: r[constants::PARTITION_KEY]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_owned(),
                                data: r[constants::DATA].as_str().unwrap_or("").to_owned(),
                                explicit_hash_key: r[constants::EXPLICIT_HASH_KEY]
                                    .as_str()
                                    .map(str::to_owned),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                CaptureInput::Batch(entries)
            }
            _ => unreachable!(),
        };
        Some((operation, stream, input))
    } else {
        None
    };

    // Execute action
    let dispatch_result = actions::dispatch(&store, operation, data)
        .instrument(span.clone())
        .await;

    // Build response first (borrows result), then move result into the mirror
    let (response, mirrorable_result) = match dispatch_result {
        Ok(opt_result) => {
            tracing::debug!(parent: &span, "ok");
            let response = match &opt_result {
                Some(result) => {
                    send_json_response(response_headers, response_content_type, result, 200)
                }
                None => {
                    response_headers.insert("Content-Type", response_content_type.parse().unwrap());
                    response_headers.insert("Content-Length", "0".parse().unwrap());
                    (StatusCode::OK, response_headers, "").into_response()
                }
            };
            (response, Ok(opt_result))
        }
        Err(err) => {
            let response =
                log_and_send_error(&span, &response_headers, response_content_type, &err);
            (response, Err(err))
        }
    };

    // Write capture records after successful dispatch
    if let (Some(writer), Some((op, stream, input))) =
        (&store.capture_writer, capture_ctx)
    {
        if let Ok(Some(ref result)) = mirrorable_result {
            write_capture_records(writer, op, &stream, &input, result);
        }
    }

    // Mirror write operations (fire-and-forget) — result moved, not cloned
    if let Some(Extension(ref mirror)) = mirror
        && Mirror::should_mirror(&operation)
    {
        match mirrorable_result {
            Ok(result) => {
                mirror.spawn_forward(target.to_string(), content_type.to_string(), body, result);
            }
            Err(e) => {
                tracing::debug!(
                    parent: &span,
                    error_type = %e.body.error_type,
                    "skipping mirror: local dispatch failed"
                );
            }
        }
    }

    response
}

fn send_kinesis_error(
    extra_headers: &HeaderMap,
    content_type: &str,
    err: &KinesisErrorResponse,
) -> Response {
    let mut headers = extra_headers.clone();
    headers.insert(
        "x-amzn-ErrorType",
        err.body
            .error_type
            .parse()
            .expect("error_type must be valid ASCII"),
    );
    send_json_response(headers, content_type, &err.body, err.status_code)
}

fn log_and_send_error(
    span: &tracing::Span,
    headers: &HeaderMap,
    content_type: &str,
    err: &KinesisErrorResponse,
) -> Response {
    if err.status_code >= 500 {
        tracing::error!(parent: span, error_type = %err.body.error_type, "server error");
    } else {
        tracing::info!(parent: span, error_type = %err.body.error_type, "client error");
    }
    send_kinesis_error(headers, content_type, err)
}

fn send_json_response(
    mut headers: HeaderMap,
    content_type: &str,
    data: &impl Serialize,
    status_code: u16,
) -> Response {
    let body_bytes = if content_type == constants::CONTENT_TYPE_CBOR {
        // Convert to ciborium::Value so Blob fields (Data) become CBOR byte strings
        // (major type 2) instead of text strings, matching real AWS Kinesis behavior.
        let json_val =
            serde_json::to_value(data).expect("response type must be serializable to JSON");
        let cbor_val = json_to_cbor_with_blob_bytes(&json_val);
        let mut buf = Vec::new();
        let _ = ciborium::into_writer(&cbor_val, &mut buf);
        buf
    } else {
        serde_json::to_vec(data).unwrap_or_default()
    };

    headers.insert("Content-Type", content_type.parse().unwrap());
    headers.insert(
        "Content-Length",
        body_bytes.len().to_string().parse().unwrap(),
    );

    (
        StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        headers,
        body_bytes,
    )
        .into_response()
}

fn send_xml_error(
    mut headers: HeaderMap,
    error_type: &str,
    message: &str,
    status_code: u16,
) -> Response {
    let body = format!("<{error_type}>\n  <Message>{message}</Message>\n</{error_type}>\n");
    headers.insert("Content-Length", body.len().to_string().parse().unwrap());

    (
        StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        headers,
        body,
    )
        .into_response()
}

fn send_xml_error_code(mut headers: HeaderMap, error_type: &str, status_code: u16) -> Response {
    let body = format!("<{error_type}/>\n");
    headers.insert("Content-Length", body.len().to_string().parse().unwrap());

    (
        StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        headers,
        body,
    )
        .into_response()
}

/// Middleware that intercepts bare 413 responses from Axum's `DefaultBodyLimit`
/// and replaces them with Kinesis-shaped `SerializationException` error bodies.
pub async fn kinesis_413_middleware(request: Request, next: Next) -> Response {
    let content_type = request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_owned();

    let response = next.run(request).await;

    if response.status() != StatusCode::PAYLOAD_TOO_LARGE {
        return response;
    }

    let error = json!({
        "__type": constants::SERIALIZATION_EXCEPTION,
        "Message": "Request body is too large"
    });

    let response_content_type = if content_type == constants::CONTENT_TYPE_JSON {
        constants::CONTENT_TYPE_JSON
    } else {
        constants::CONTENT_TYPE_CBOR
    };

    let body_bytes = if response_content_type == constants::CONTENT_TYPE_CBOR {
        let mut buf = Vec::new();
        let _ = ciborium::into_writer(&error, &mut buf);
        buf
    } else {
        serde_json::to_vec(&error).unwrap_or_default()
    };

    (
        StatusCode::PAYLOAD_TOO_LARGE,
        [
            ("Content-Type", response_content_type.to_owned()),
            ("Content-Length", body_bytes.len().to_string()),
        ],
        body_bytes,
    )
        .into_response()
}

fn send_error_response(
    extra_headers: &HeaderMap,
    content_valid: bool,
    content_type: &str,
    error_type: &str,
    message: &str,
    status_code: u16,
) -> Response {
    if content_valid {
        let err = KinesisErrorResponse::new(status_code, error_type, Some(message));
        send_kinesis_error(extra_headers, content_type, &err)
    } else {
        let mut headers = extra_headers.clone();
        headers.insert(
            "x-amzn-ErrorType",
            error_type.parse().expect("error_type must be valid ASCII"),
        );
        send_xml_error(headers, error_type, message, status_code)
    }
}

/// Pre-extracted input fields for capture (avoids cloning entire `Value` trees).
enum CaptureInput {
    Single {
        partition_key: String,
        data: String,
        explicit_hash_key: Option<String>,
    },
    Batch(Vec<CaptureInputEntry>),
}

struct CaptureInputEntry {
    partition_key: String,
    data: String,
    explicit_hash_key: Option<String>,
}

/// Construct and write [`CaptureRecordRef`]s from pre-extracted input and dispatch response.
fn write_capture_records(
    writer: &CaptureWriter,
    operation: Operation,
    stream: &str,
    input: &CaptureInput,
    response: &Value,
) {
    let ts = current_time_ms();
    match operation {
        Operation::PutRecord => {
            let CaptureInput::Single {
                partition_key,
                data,
                explicit_hash_key,
            } = input
            else {
                return;
            };
            let record = CaptureRecordRef {
                op: CaptureOp::PutRecord,
                ts,
                stream,
                partition_key: Cow::Borrowed(partition_key),
                data,
                explicit_hash_key: explicit_hash_key.as_deref(),
                sequence_number: response[constants::SEQUENCE_NUMBER].as_str().unwrap_or(""),
                shard_id: response[constants::SHARD_ID].as_str().unwrap_or(""),
            };
            writer.write_record(&record);
        }
        Operation::PutRecords => {
            let CaptureInput::Batch(entries) = input else {
                return;
            };
            let Some(response_records) = response[constants::RECORDS].as_array() else {
                return;
            };
            let records: Vec<CaptureRecordRef<'_>> = entries
                .iter()
                .zip(response_records.iter())
                .filter(|(_, resp)| resp.get(constants::ERROR_CODE).is_none_or(|v| v.is_null()))
                .map(|(entry, resp)| CaptureRecordRef {
                    op: CaptureOp::PutRecords,
                    ts,
                    stream,
                    partition_key: Cow::Borrowed(&entry.partition_key),
                    data: &entry.data,
                    explicit_hash_key: entry.explicit_hash_key.as_deref(),
                    sequence_number: resp[constants::SEQUENCE_NUMBER].as_str().unwrap_or(""),
                    shard_id: resp[constants::SHARD_ID].as_str().unwrap_or(""),
                })
                .collect();
            writer.write_records(&records);
        }
        Operation::AddTagsToStream
        | Operation::CreateStream
        | Operation::DecreaseStreamRetentionPeriod
        | Operation::DeleteResourcePolicy
        | Operation::DeleteStream
        | Operation::DeregisterStreamConsumer
        | Operation::DescribeAccountSettings
        | Operation::DescribeLimits
        | Operation::DescribeStream
        | Operation::DescribeStreamConsumer
        | Operation::DescribeStreamSummary
        | Operation::DisableEnhancedMonitoring
        | Operation::EnableEnhancedMonitoring
        | Operation::GetRecords
        | Operation::GetResourcePolicy
        | Operation::GetShardIterator
        | Operation::IncreaseStreamRetentionPeriod
        | Operation::ListShards
        | Operation::ListStreamConsumers
        | Operation::ListStreams
        | Operation::ListTagsForResource
        | Operation::ListTagsForStream
        | Operation::MergeShards
        | Operation::PutResourcePolicy
        | Operation::RegisterStreamConsumer
        | Operation::RemoveTagsFromStream
        | Operation::SplitShard
        | Operation::StartStreamEncryption
        | Operation::StopStreamEncryption
        | Operation::SubscribeToShard
        | Operation::TagResource
        | Operation::UntagResource
        | Operation::UpdateAccountSettings
        | Operation::UpdateMaxRecordSize
        | Operation::UpdateShardCount
        | Operation::UpdateStreamMode
        | Operation::UpdateStreamWarmThroughput => {}
    }
}

/// Convert ciborium::Value to serde_json::Value.
/// CBOR byte strings (major type 2) are converted to base64-encoded strings,
/// so the rest of the pipeline can treat all data uniformly.
///
/// Exposed for integration tests (`tests/common/mod.rs` needs to decode CBOR
/// responses the same way the server does). Not part of the public API.
#[doc(hidden)]
pub fn cbor_to_json(val: &ciborium::Value) -> Value {
    match val {
        ciborium::Value::Null => Value::Null,
        ciborium::Value::Bool(b) => Value::Bool(*b),
        ciborium::Value::Integer(n) => {
            let n: i128 = (*n).into();
            if let Ok(i) = i64::try_from(n) {
                Value::Number(serde_json::Number::from(i))
            } else {
                // Fallback: i128 values outside i64 range lose precision when cast to f64.
                // Theoretical for Kinesis (all integers fit in i64), but handles CBOR edge cases.
                #[allow(clippy::cast_precision_loss)]
                let f = n as f64;
                serde_json::Number::from_f64(f)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
        }
        ciborium::Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        ciborium::Value::Text(s) => Value::String(s.clone()),
        ciborium::Value::Bytes(b) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        ciborium::Value::Array(arr) => Value::Array(arr.iter().map(cbor_to_json).collect()),
        ciborium::Value::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                let key = match k {
                    ciborium::Value::Text(s) => s.clone(),
                    // Debug format fallback — Kinesis only uses text keys, so this is
                    // a defensive catch-all that avoids panicking on malformed CBOR.
                    _ => format!("{k:?}"),
                };
                obj.insert(key, cbor_to_json(v));
            }
            Value::Object(obj)
        }
        ciborium::Value::Tag(_, inner) => cbor_to_json(inner),
        _ => Value::Null,
    }
}

/// Keys whose values are Blob fields — base64 strings in JSON that must become
/// CBOR byte strings (major type 2) in CBOR responses.
/// Matches at any nesting depth, which is correct for Kinesis where `Data` is
/// always a blob. Would need path-aware matching if non-blob `Data` fields existed.
const BLOB_FIELD_KEYS: &[&str] = &["Data"];

/// Convert serde_json::Value to ciborium::Value for CBOR response serialization.
/// Values under known Blob keys are decoded from base64 and emitted as CBOR byte strings.
///
/// Note: `tests/common/mod.rs` has a similar `json_to_cbor_with_bytes` that uses
/// explicit path-based replacement (e.g. `"Records.*.Data"`) for constructing test
/// requests. This function uses key-name matching because the server doesn't know
/// the request path at serialization time.
pub(crate) fn json_to_cbor_with_blob_bytes(val: &Value) -> ciborium::Value {
    json_to_cbor_impl(val, false)
}

fn json_to_cbor_impl(val: &Value, as_bytes: bool) -> ciborium::Value {
    match val {
        Value::Null => ciborium::Value::Null,
        Value::Bool(b) => ciborium::Value::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ciborium::Value::Integer(i.into())
            } else if let Some(f) = n.as_f64() {
                // Whole-number floats (e.g. epoch-second timestamps) must be
                // emitted as CBOR integers to avoid Java SDK parse failures.
                if crate::types::is_whole_epoch(f) {
                    #[allow(clippy::cast_possible_truncation)]
                    return ciborium::Value::Integer((f as i64).into());
                }
                ciborium::Value::Float(f)
            } else {
                ciborium::Value::Null
            }
        }
        Value::String(s) => {
            if as_bytes {
                // Decode base64 and emit as CBOR byte string
                if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(s) {
                    return ciborium::Value::Bytes(bytes);
                }
            }
            ciborium::Value::Text(s.clone())
        }
        Value::Array(arr) => {
            // as_bytes is not propagated: Kinesis Blob fields are always scalar strings,
            // never arrays, so array elements are always emitted as text.
            ciborium::Value::Array(arr.iter().map(|v| json_to_cbor_impl(v, false)).collect())
        }
        Value::Object(map) => ciborium::Value::Map(
            map.iter()
                .map(|(k, v)| {
                    let is_blob = BLOB_FIELD_KEYS.contains(&k.as_str());
                    (
                        ciborium::Value::Text(k.clone()),
                        json_to_cbor_impl(v, is_blob),
                    )
                })
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::{CaptureOp, CaptureWriter};
    use serde_json::json;
    use tempfile::NamedTempFile;

    /// Verifies that `write_capture_records` filters out PutRecords entries
    /// whose response contains a non-null `ErrorCode`, while keeping entries
    /// with no `ErrorCode` or a null one.
    #[test]
    fn write_capture_records_filters_failed_put_records_entries() {
        let capture_file = NamedTempFile::new().unwrap();
        let writer = CaptureWriter::new(capture_file.path(), false).unwrap();

        let input = CaptureInput::Batch(vec![
            CaptureInputEntry {
                partition_key: "ok-key".into(),
                data: "b2s=".into(),
                explicit_hash_key: None,
            },
            CaptureInputEntry {
                partition_key: "fail-key".into(),
                data: "ZmFpbA==".into(),
                explicit_hash_key: None,
            },
            CaptureInputEntry {
                partition_key: "null-err-key".into(),
                data: "bnVsbA==".into(),
                explicit_hash_key: None,
            },
        ]);

        // Simulate a PutRecords response where the second record failed
        let response = json!({
            "FailedRecordCount": 1,
            "Records": [
                {
                    "SequenceNumber": "seq-1",
                    "ShardId": "shardId-000000000000"
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "Rate exceeded for shard"
                },
                {
                    "SequenceNumber": "seq-3",
                    "ShardId": "shardId-000000000000",
                    "ErrorCode": null
                }
            ]
        });

        write_capture_records(
            &writer,
            Operation::PutRecords,
            "test-stream",
            &input,
            &response,
        );

        let records = crate::capture::read_capture_file(capture_file.path()).unwrap();
        // Only the first and third records should be captured
        assert_eq!(records.len(), 2);

        assert_eq!(records[0].op, CaptureOp::PutRecords);
        assert_eq!(records[0].partition_key, "ok-key");
        assert_eq!(records[0].data, "b2s=");
        assert_eq!(records[0].sequence_number, "seq-1");
        assert_eq!(records[0].shard_id, "shardId-000000000000");

        assert_eq!(records[1].op, CaptureOp::PutRecords);
        assert_eq!(records[1].partition_key, "null-err-key");
        assert_eq!(records[1].data, "bnVsbA==");
        assert_eq!(records[1].sequence_number, "seq-3");
        assert_eq!(records[1].shard_id, "shardId-000000000000");
    }

    /// Verifies that when ALL records in a PutRecords batch fail, no capture
    /// records are written.
    #[test]
    fn write_capture_records_all_failed_writes_nothing() {
        let capture_file = NamedTempFile::new().unwrap();
        let writer = CaptureWriter::new(capture_file.path(), false).unwrap();

        let input = CaptureInput::Batch(vec![
            CaptureInputEntry {
                partition_key: "k1".into(),
                data: "YQ==".into(),
                explicit_hash_key: None,
            },
            CaptureInputEntry {
                partition_key: "k2".into(),
                data: "Yg==".into(),
                explicit_hash_key: None,
            },
        ]);

        let response = json!({
            "FailedRecordCount": 2,
            "Records": [
                {
                    "ErrorCode": "InternalFailure",
                    "ErrorMessage": "Internal error"
                },
                {
                    "ErrorCode": "ProvisionedThroughputExceededException",
                    "ErrorMessage": "Rate exceeded"
                }
            ]
        });

        write_capture_records(
            &writer,
            Operation::PutRecords,
            "test-stream",
            &input,
            &response,
        );

        let records = crate::capture::read_capture_file(capture_file.path()).unwrap();
        assert_eq!(records.len(), 0);
    }
}
