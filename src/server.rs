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
use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::metrics::PreOperationFailureReason;
#[cfg(feature = "mirror")]
use crate::mirror::Mirror;
use crate::store::Store;
use crate::validation;
use axum::body::Bytes;
#[cfg(any(feature = "access-log", feature = "mirror"))]
use axum::extract::Extension;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use serde::Serialize;
use serde_json::{Value, json};
#[cfg(feature = "mirror")]
use std::sync::Arc;
use tracing::Instrument;

#[cfg(feature = "mirror")]
type MirrorExt = Option<Extension<Arc<Mirror>>>;
#[cfg(not(feature = "mirror"))]
type MirrorExt = ();
#[cfg(feature = "access-log")]
type RequestLoggingExt = Option<Extension<RequestLogging>>;
#[cfg(not(feature = "access-log"))]
type RequestLoggingExt = ();

/// Marker extension layered by the binary to enable structured request completion logs.
#[cfg(feature = "access-log")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct RequestLogging;

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
    request_logging: RequestLoggingExt,
    mirror: MirrorExt,
    body: Bytes,
) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let request_started_ms = crate::util::current_time_ms();
    let span = tracing::info_span!(
        "kinesis",
        request_id = %request_id,
        operation = tracing::field::Empty,
        status_code = tracing::field::Empty,
        error_type = tracing::field::Empty,
    );
    let request_span = span.clone();

    async move {
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
                return complete_response(
                    &store,
                    &request_span,
                    &request_id,
                    &request_logging,
                    None,
                    None,
                    request_started_ms,
                    (StatusCode::OK, response_headers, "").into_response(),
                    None,
                );
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
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                None,
                Some(PreOperationFailureReason::AccessDenied),
                request_started_ms,
                send_xml_error(
                    h,
                    constants::ACCESS_DENIED,
                    "Unable to determine service/operation name to be authorized",
                    403,
                ),
                Some(constants::ACCESS_DENIED),
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
        record_operation(&request_span, operation);

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
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                operation,
                operation
                    .is_none()
                    .then_some(PreOperationFailureReason::UnknownOperation),
                request_started_ms,
                send_kinesis_error(&response_headers, response_content_type, &err),
                Some(error_type),
            );
        }

        if !content_valid {
            if service.is_empty() || operation_str.is_empty() {
                let mut h = response_headers.clone();
                h.insert(
                    "x-amzn-ErrorType",
                    constants::ACCESS_DENIED.parse().unwrap(),
                );
                return complete_response(
                    &store,
                    &request_span,
                    &request_id,
                    &request_logging,
                    operation,
                    operation
                        .is_none()
                        .then_some(PreOperationFailureReason::AccessDenied),
                    request_started_ms,
                    send_xml_error(
                        h,
                        constants::ACCESS_DENIED,
                        "Unable to determine service/operation name to be authorized",
                        403,
                    ),
                    Some(constants::ACCESS_DENIED),
                );
            }
            let mut h = response_headers.clone();
            h.insert(
                "x-amzn-ErrorType",
                constants::UNKNOWN_OPERATION.parse().unwrap(),
            );
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                operation,
                operation
                    .is_none()
                    .then_some(PreOperationFailureReason::UnknownOperation),
                request_started_ms,
                send_xml_error_code(h, constants::UNKNOWN_OPERATION, 404),
                Some(constants::UNKNOWN_OPERATION),
            );
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
                    return complete_response(
                        &store,
                        &request_span,
                        &request_id,
                        &request_logging,
                        operation,
                        operation
                            .is_none()
                            .then_some(PreOperationFailureReason::SerializationException),
                        request_started_ms,
                        send_json_response(
                            response_headers.clone(),
                            "application/json",
                            &json!({
                                "Output": {"__type": "com.amazon.coral.service#SerializationException"},
                                "Version": "1.0",
                            }),
                            400,
                        ),
                        Some(constants::SERIALIZATION_EXCEPTION),
                    );
                }
                let err =
                    KinesisErrorResponse::client_error(constants::SERIALIZATION_EXCEPTION, None);
                return complete_response(
                    &store,
                    &request_span,
                    &request_id,
                    &request_logging,
                    operation,
                    operation
                        .is_none()
                        .then_some(PreOperationFailureReason::SerializationException),
                    request_started_ms,
                    send_kinesis_error(&response_headers, response_content_type, &err),
                    Some(constants::SERIALIZATION_EXCEPTION),
                );
            }
        };

        // After this point, application/json doesn't progress further
        if content_type == "application/json" {
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                operation,
                operation
                    .is_none()
                    .then_some(PreOperationFailureReason::UnknownOperation),
                request_started_ms,
                send_json_response(
                    response_headers.clone(),
                    "application/json",
                    &json!({
                        "Output": {"__type": "com.amazon.coral.service#UnknownOperationException"},
                        "Version": "1.0",
                    }),
                    404,
                ),
                Some(constants::UNKNOWN_OPERATION),
            );
        }

        let Some(operation) = operation else {
            let err = KinesisErrorResponse::client_error(constants::UNKNOWN_OPERATION, None);
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                None,
                Some(PreOperationFailureReason::UnknownOperation),
                request_started_ms,
                send_kinesis_error(&response_headers, response_content_type, &err),
                Some(constants::UNKNOWN_OPERATION),
            );
        };
        record_operation(&request_span, Some(operation));

        if let Err(err) = store.check_available() {
            let error_type = err.body.error_type.clone();
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                Some(operation),
                None,
                request_started_ms,
                send_kinesis_error(&response_headers, response_content_type, &err),
                Some(error_type.as_str()),
            );
        }

        if !service_valid {
            let err = KinesisErrorResponse::client_error(constants::UNKNOWN_OPERATION, None);
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                Some(operation),
                None,
                request_started_ms,
                send_kinesis_error(&response_headers, response_content_type, &err),
                Some(constants::UNKNOWN_OPERATION),
            );
        }

        // Auth checking
        let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
        let query_string = uri.query().unwrap_or("");
        let auth_query = query_string.contains("X-Amz-Algorithm");

        if auth_header.is_some() && auth_query {
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                Some(operation),
                Some(PreOperationFailureReason::InvalidSignature),
                request_started_ms,
                send_error_response(
                    &response_headers,
                    content_valid,
                    response_content_type,
                    constants::INVALID_SIGNATURE,
                    "Found both 'X-Amz-Algorithm' as a query-string param and 'Authorization' as HTTP header.",
                    400,
                ),
                Some(constants::INVALID_SIGNATURE),
            );
        }

        if auth_header.is_none() && !auth_query {
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                Some(operation),
                Some(PreOperationFailureReason::MissingAuthToken),
                request_started_ms,
                send_error_response(
                    &response_headers,
                    content_valid,
                    response_content_type,
                    constants::MISSING_AUTH_TOKEN,
                    "Missing Authentication Token",
                    400,
                ),
                Some(constants::MISSING_AUTH_TOKEN),
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
                return complete_response(
                    &store,
                    &request_span,
                    &request_id,
                    &request_logging,
                    Some(operation),
                    Some(PreOperationFailureReason::IncompleteSignature),
                    request_started_ms,
                    send_error_response(
                        &response_headers,
                        content_valid,
                        response_content_type,
                        constants::INCOMPLETE_SIGNATURE,
                        &msg,
                        403,
                    ),
                    Some(constants::INCOMPLETE_SIGNATURE),
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
                return complete_response(
                    &store,
                    &request_span,
                    &request_id,
                    &request_logging,
                    Some(operation),
                    Some(PreOperationFailureReason::IncompleteSignature),
                    request_started_ms,
                    send_error_response(
                        &response_headers,
                        content_valid,
                        response_content_type,
                        constants::INCOMPLETE_SIGNATURE,
                        &msg,
                        403,
                    ),
                    Some(constants::INCOMPLETE_SIGNATURE),
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
                let error_type = err.body.error_type.clone();
                return complete_response(
                    &store,
                    &request_span,
                    &request_id,
                    &request_logging,
                    Some(operation),
                    None,
                    request_started_ms,
                    send_kinesis_error(&response_headers, response_content_type, &err),
                    Some(error_type.as_str()),
                );
            }
        };

        if let Err(err) = validation::check_validations(&data, &field_refs, None) {
            let error_type = err.body.error_type.clone();
            return complete_response(
                &store,
                &request_span,
                &request_id,
                &request_logging,
                Some(operation),
                None,
                request_started_ms,
                send_kinesis_error(&response_headers, response_content_type, &err),
                Some(error_type.as_str()),
            );
        }

        // Handle SubscribeToShard separately (streaming response)
        if operation == Operation::SubscribeToShard {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let response = match store.check_available() {
                    Ok(()) => match actions::subscribe_to_shard::execute_streaming(
                        &store,
                        data,
                        response_content_type,
                    )
                    .instrument(request_span.clone())
                    .await
                    {
                        Ok(body) => {
                            tracing::debug!(parent: &request_span, "ok");
                            response_headers.insert(
                                "Content-Type",
                                "application/vnd.amazon.eventstream".parse().unwrap(),
                            );
                            (StatusCode::OK, response_headers, body).into_response()
                        }
                        Err(ref err) => {
                            log_and_send_error(
                                &request_span,
                                &response_headers,
                                response_content_type,
                                err,
                            )
                        }
                    },
                    Err(err) => {
                        log_and_send_error(
                            &request_span,
                            &response_headers,
                            response_content_type,
                            &err,
                        )
                    }
                };
                let error_type = response
                    .headers()
                    .get("x-amzn-ErrorType")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                return complete_response(
                    &store,
                    &request_span,
                    &request_id,
                    &request_logging,
                    Some(operation),
                    None,
                    request_started_ms,
                    response,
                    error_type.as_deref(),
                );
            }

            #[cfg(target_arch = "wasm32")]
            {
                let err = KinesisErrorResponse::client_error(
                    constants::INVALID_ARGUMENT,
                    Some("SubscribeToShard is not supported in this build."),
                );
                let response =
                    log_and_send_error(&request_span, &response_headers, response_content_type, &err);
                let error_type = err.body.error_type.clone();
                return complete_response(
                    &store,
                    &request_span,
                    &request_id,
                    &request_logging,
                    Some(operation),
                    None,
                    request_started_ms,
                    response,
                    Some(error_type.as_str()),
                );
            }
        }

        // Execute action
        let dispatch_result = actions::dispatch(&store, operation, data)
            .instrument(request_span.clone())
            .await;

        // Build response first (borrows result), then move result into the mirror
        let (response, error_type, mirrorable_result) = match dispatch_result {
            Ok(opt_result) => {
                tracing::debug!(parent: &request_span, "ok");
                let response = match &opt_result {
                    Some(result) => {
                        send_value_response(response_headers, response_content_type, result, 200)
                    }
                    None => {
                        response_headers
                            .insert("Content-Type", response_content_type.parse().unwrap());
                        response_headers.insert("Content-Length", "0".parse().unwrap());
                        (StatusCode::OK, response_headers, "").into_response()
                    }
                };
                (response, None, Ok(opt_result))
            }
            Err(err) => {
                let response =
                    log_and_send_error(&request_span, &response_headers, response_content_type, &err);
                let error_type = err.body.error_type.clone();
                (response, Some(error_type), Err(err))
            }
        };

        // Mirror write operations (fire-and-forget) — result moved, not cloned
        #[cfg(feature = "mirror")]
        if let Some(Extension(ref mirror)) = mirror
            && Mirror::should_mirror(&operation)
        {
            match mirrorable_result {
                Ok(result) => {
                    mirror.spawn_forward(target.to_string(), content_type.to_string(), body, result);
                }
                Err(e) => {
                    tracing::debug!(
                        parent: &request_span,
                        error_type = %e.body.error_type,
                        "skipping mirror: local dispatch failed"
                    );
                }
            }
        }
        #[cfg(not(feature = "mirror"))]
        {
            let _ = (mirror, mirrorable_result);
        }

        complete_response(
            &store,
            &request_span,
            &request_id,
            &request_logging,
            Some(operation),
            None,
            request_started_ms,
            response,
            error_type.as_deref(),
        )
    }
    .instrument(span)
    .await
}

fn elapsed_request_micros(request_started_ms: u64) -> u64 {
    crate::util::current_time_ms()
        .saturating_sub(request_started_ms)
        .saturating_mul(1000)
}

fn finalize_response(
    store: &Store,
    operation: Option<Operation>,
    pre_operation_failure: Option<PreOperationFailureReason>,
    request_started_ms: u64,
    response: Response,
) -> Response {
    let duration_micros = elapsed_request_micros(request_started_ms);
    if let Some(operation) = operation {
        store
            .metrics()
            .record_request(operation, response.status().is_success(), duration_micros);
    }
    if let Some(reason) = pre_operation_failure {
        store
            .metrics()
            .record_pre_operation_failure(reason, duration_micros);
    }
    response
}

fn complete_response(
    store: &Store,
    span: &tracing::Span,
    request_id: &str,
    request_logging: &RequestLoggingExt,
    operation: Option<Operation>,
    pre_operation_failure: Option<PreOperationFailureReason>,
    request_started_ms: u64,
    response: Response,
    error_type: Option<&str>,
) -> Response {
    let response = finalize_response(
        store,
        operation,
        pre_operation_failure,
        request_started_ms,
        response,
    );
    let latency_us = elapsed_request_micros(request_started_ms);
    span.record("status_code", &response.status().as_u16());
    if let Some(error_type) = error_type {
        span.record("error_type", &tracing::field::display(error_type));
    }
    if request_logging_enabled(request_logging) {
        log_request_completion(
            span,
            operation,
            request_id,
            response.status(),
            latency_us,
            error_type,
        );
    }
    response
}

fn record_operation(span: &tracing::Span, operation: Option<Operation>) {
    if let Some(operation) = operation {
        span.record("operation", &tracing::field::display(operation));
    }
}

fn request_logging_enabled(request_logging: &RequestLoggingExt) -> bool {
    #[cfg(feature = "access-log")]
    {
        request_logging.is_some()
    }
    #[cfg(not(feature = "access-log"))]
    {
        let _ = request_logging;
        false
    }
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

fn log_request_completion(
    span: &tracing::Span,
    operation: Option<Operation>,
    request_id: &str,
    status: StatusCode,
    latency_us: u64,
    error_type: Option<&str>,
) {
    if let Some(error_type) = error_type
        && let Some(operation) = operation
    {
        tracing::info!(
            parent: span,
            %operation,
            request_id,
            status_code = status.as_u16(),
            latency_us,
            error_type,
            "request completed"
        );
    } else if let Some(operation) = operation {
        tracing::info!(
            parent: span,
            %operation,
            request_id,
            status_code = status.as_u16(),
            latency_us,
            "request completed"
        );
    } else if let Some(error_type) = error_type {
        tracing::info!(
            parent: span,
            request_id,
            status_code = status.as_u16(),
            latency_us,
            error_type,
            "request completed"
        );
    } else {
        tracing::info!(
            parent: span,
            request_id,
            status_code = status.as_u16(),
            latency_us,
            "request completed"
        );
    }
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
        let mut buf = Vec::new();
        let _ = ciborium::into_writer(data, &mut buf);
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

/// Serialize a `serde_json::Value` action-handler result as either JSON or CBOR.
///
/// For CBOR, wraps the value in [`BlobAwareValue`] so that "Data" fields are
/// emitted as CBOR byte strings (major type 2) rather than text strings,
/// matching real AWS Kinesis CBOR behavior. Avoids the intermediate
/// `serde_json::to_value` clone and `ciborium::Value` tree of the old path.
fn send_value_response(
    mut headers: HeaderMap,
    content_type: &str,
    data: &Value,
    status_code: u16,
) -> Response {
    let body_bytes = if content_type == constants::CONTENT_TYPE_CBOR {
        let mut buf = Vec::new();
        let _ = ciborium::into_writer(&BlobAwareValue::new(data), &mut buf);
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

/// Newtype wrapper around `&serde_json::Value` that serializes "Data" fields as
/// CBOR byte strings (major type 2) rather than text strings.
///
/// Used only in the CBOR response path. When the serializer encounters a key
/// named `"Data"`, the corresponding base64-encoded string value is decoded
/// and emitted via `serialize_bytes`, which ciborium maps to CBOR major type 2.
/// All other values are forwarded to the standard `serde_json::Value` serializer.
///
/// This eliminates the need for an intermediate `ciborium::Value` tree when
/// serializing action-handler responses.
pub(crate) struct BlobAwareValue<'a> {
    val: &'a Value,
    is_blob: bool,
}

impl<'a> BlobAwareValue<'a> {
    pub(crate) fn new(val: &'a Value) -> Self {
        Self {
            val,
            is_blob: false,
        }
    }
}

impl Serialize for BlobAwareValue<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self.val {
            Value::String(st) if self.is_blob => {
                match base64::engine::general_purpose::STANDARD.decode(st) {
                    Ok(bytes) => s.serialize_bytes(&bytes),
                    Err(_) => s.serialize_str(st), // fallback: emit as text
                }
            }
            Value::Object(map) => {
                use serde::ser::SerializeMap;
                let mut m = s.serialize_map(Some(map.len()))?;
                for (k, v) in map {
                    m.serialize_entry(
                        k,
                        &BlobAwareValue {
                            val: v,
                            is_blob: k == constants::DATA,
                        },
                    )?;
                }
                m.end()
            }
            Value::Array(arr) => {
                use serde::ser::SerializeSeq;
                // Blob fields are always scalar in Kinesis — never arrays.
                // is_blob is not propagated into array elements.
                let mut seq = s.serialize_seq(Some(arr.len()))?;
                for v in arr {
                    seq.serialize_element(&BlobAwareValue {
                        val: v,
                        is_blob: false,
                    })?;
                }
                seq.end()
            }
            // Non-blob strings, numbers, bools, nulls — delegate to serde_json::Value.
            other => other.serialize(s),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::json;

    /// Helper: serialize a BlobAwareValue to CBOR bytes, then decode back to
    /// ciborium::Value so we can inspect the CBOR structure.
    fn to_cbor_value(bav: &BlobAwareValue<'_>) -> ciborium::Value {
        let mut buf = Vec::new();
        ciborium::into_writer(bav, &mut buf).expect("CBOR serialization failed");
        ciborium::from_reader(&buf[..]).expect("CBOR deserialization failed")
    }

    #[test]
    fn blob_valid_base64_emits_bytes() {
        let raw = b"hello world";
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        let val = Value::String(b64);
        let bav = BlobAwareValue {
            val: &val,
            is_blob: true,
        };
        let cbor = to_cbor_value(&bav);
        assert_eq!(cbor, ciborium::Value::Bytes(raw.to_vec()));
    }

    #[test]
    fn blob_invalid_base64_falls_back_to_text() {
        let val = Value::String("NOT!VALID!BASE64".to_string());
        let bav = BlobAwareValue {
            val: &val,
            is_blob: true,
        };
        let cbor = to_cbor_value(&bav);
        assert_eq!(cbor, ciborium::Value::Text("NOT!VALID!BASE64".to_string()));
    }

    #[test]
    fn non_blob_string_emits_text() {
        let b64 = base64::engine::general_purpose::STANDARD.encode(b"bytes");
        let val = Value::String(b64.clone());
        let bav = BlobAwareValue {
            val: &val,
            is_blob: false,
        };
        let cbor = to_cbor_value(&bav);
        // Even though it's valid base64, is_blob=false → text string
        assert_eq!(cbor, ciborium::Value::Text(b64));
    }

    #[test]
    fn object_with_data_key_decodes_blob() {
        let raw = b"payload";
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        let val = json!({"Data": b64, "PartitionKey": "pk"});
        let bav = BlobAwareValue::new(&val);
        let cbor = to_cbor_value(&bav);

        // Data should be CBOR bytes, PartitionKey should be CBOR text
        if let ciborium::Value::Map(entries) = cbor {
            for (k, v) in &entries {
                match k {
                    ciborium::Value::Text(key) if key == "Data" => {
                        assert_eq!(v, &ciborium::Value::Bytes(raw.to_vec()));
                    }
                    ciborium::Value::Text(key) if key == "PartitionKey" => {
                        assert_eq!(v, &ciborium::Value::Text("pk".to_string()));
                    }
                    _ => panic!("unexpected key: {k:?}"),
                }
            }
        } else {
            panic!("expected CBOR map, got {cbor:?}");
        }
    }

    #[test]
    fn array_does_not_propagate_is_blob() {
        let b64 = base64::engine::general_purpose::STANDARD.encode(b"data");
        let val = json!([b64]);
        let bav = BlobAwareValue {
            val: &val,
            is_blob: true, // should not propagate into array elements
        };
        let cbor = to_cbor_value(&bav);

        if let ciborium::Value::Array(items) = cbor {
            // Array element should be text, not bytes
            assert_eq!(items[0], ciborium::Value::Text(b64));
        } else {
            panic!("expected CBOR array");
        }
    }

    #[test]
    fn blob_empty_base64_emits_empty_bytes() {
        let val = Value::String(String::new());
        let bav = BlobAwareValue {
            val: &val,
            is_blob: true,
        };
        let cbor = to_cbor_value(&bav);
        assert_eq!(cbor, ciborium::Value::Bytes(vec![]));
    }
}
