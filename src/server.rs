use crate::actions::{self, Operation};
use crate::constants;
use crate::error::KinesisErrorResponse;
use crate::store::Store;
use crate::validation;
use crate::validation::rules;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::{Value, json};

const MAX_REQUEST_BYTES: usize = 7 * 1024 * 1024;

pub async fn handler(
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    State(store): State<Store>,
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
        return send_xml_error(
            &response_headers,
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

    if body.len() > MAX_REQUEST_BYTES {
        response_headers.insert("Transfer-Encoding", "chunked".parse().unwrap());
        return (StatusCode::from_u16(413).unwrap(), response_headers, "").into_response();
    }

    if !content_valid {
        if service.is_empty() || operation_str.is_empty() {
            return send_xml_error(
                &response_headers,
                constants::ACCESS_DENIED,
                "Unable to determine service/operation name to be authorized",
                403,
            );
        }
        return send_xml_error_code(&response_headers, constants::UNKNOWN_OPERATION, 404);
    }

    // Parse body
    let data: Option<Value> = if content_type == constants::CONTENT_TYPE_CBOR {
        // Try CBOR decode
        ciborium::from_reader::<Value, _>(&body[..]).ok()
    } else {
        serde_json::from_slice(&body).ok()
    };

    let data = match data {
        Some(Value::Object(map)) => Value::Object(map),
        Some(_) | None => {
            if content_type == "application/json" {
                return send_json_response(
                    &response_headers,
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
            &response_headers,
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
    let validation_rules = get_validation_rules(operation);
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

    // Handle SubscribeToShard separately (streaming response)
    if operation == Operation::SubscribeToShard {
        return match actions::subscribe_to_shard::execute_streaming(&store, data).await {
            Ok(body) => {
                response_headers.insert(
                    "Content-Type",
                    "application/vnd.amazon.eventstream".parse().unwrap(),
                );
                (StatusCode::OK, response_headers, body).into_response()
            }
            Err(err) => send_kinesis_error(&response_headers, response_content_type, &err),
        };
    }

    // Execute action
    match actions::dispatch(&store, operation, data).await {
        Ok(Some(result)) => {
            send_json_response(&response_headers, response_content_type, &result, 200)
        }
        Ok(None) => {
            response_headers.insert("Content-Type", response_content_type.parse().unwrap());
            response_headers.insert("Content-Length", "0".parse().unwrap());
            (StatusCode::OK, response_headers, "").into_response()
        }
        Err(err) => send_kinesis_error(&response_headers, response_content_type, &err),
    }
}

fn get_validation_rules(operation: Operation) -> Vec<(&'static str, validation::FieldDef)> {
    match operation {
        Operation::AddTagsToStream => rules::add_tags_to_stream(),
        Operation::CreateStream => rules::create_stream(),
        Operation::DecreaseStreamRetentionPeriod => rules::decrease_stream_retention_period(),
        Operation::DeleteResourcePolicy => rules::delete_resource_policy(),
        Operation::DeleteStream => rules::delete_stream(),
        Operation::DeregisterStreamConsumer => rules::deregister_stream_consumer(),
        Operation::DescribeAccountSettings => vec![],
        Operation::DescribeLimits => vec![],
        Operation::DescribeStream => rules::describe_stream(),
        Operation::DescribeStreamConsumer => rules::describe_stream_consumer(),
        Operation::DescribeStreamSummary => rules::describe_stream_summary(),
        Operation::DisableEnhancedMonitoring => rules::disable_enhanced_monitoring(),
        Operation::EnableEnhancedMonitoring => rules::enable_enhanced_monitoring(),
        Operation::GetRecords => rules::get_records(),
        Operation::GetResourcePolicy => rules::get_resource_policy(),
        Operation::GetShardIterator => rules::get_shard_iterator(),
        Operation::IncreaseStreamRetentionPeriod => rules::increase_stream_retention_period(),
        Operation::ListShards => rules::list_shards(),
        Operation::ListStreamConsumers => rules::list_stream_consumers(),
        Operation::ListStreams => rules::list_streams(),
        Operation::ListTagsForResource => rules::list_tags_for_resource(),
        Operation::ListTagsForStream => rules::list_tags_for_stream(),
        Operation::MergeShards => rules::merge_shards(),
        Operation::PutRecord => rules::put_record(),
        Operation::PutRecords => rules::put_records(),
        Operation::PutResourcePolicy => rules::put_resource_policy(),
        Operation::RegisterStreamConsumer => rules::register_stream_consumer(),
        Operation::RemoveTagsFromStream => rules::remove_tags_from_stream(),
        Operation::SplitShard => rules::split_shard(),
        Operation::StartStreamEncryption => rules::start_stream_encryption(),
        Operation::StopStreamEncryption => rules::stop_stream_encryption(),
        Operation::SubscribeToShard => rules::subscribe_to_shard(),
        Operation::TagResource => rules::tag_resource(),
        Operation::UntagResource => rules::untag_resource(),
        Operation::UpdateAccountSettings => rules::update_account_settings(),
        Operation::UpdateMaxRecordSize => rules::update_max_record_size(),
        Operation::UpdateShardCount => rules::update_shard_count(),
        Operation::UpdateStreamMode => rules::update_stream_mode(),
        Operation::UpdateStreamWarmThroughput => rules::update_stream_warm_throughput(),
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
    send_json_response(&headers, content_type, &err.body, err.status_code)
}

fn send_json_response(
    extra_headers: &HeaderMap,
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

    let mut headers = extra_headers.clone();
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
    extra_headers: &HeaderMap,
    error_type: &str,
    message: &str,
    status_code: u16,
) -> Response {
    let body = format!("<{error_type}>\n  <Message>{message}</Message>\n</{error_type}>\n");
    let mut headers = extra_headers.clone();
    headers.insert("Content-Length", body.len().to_string().parse().unwrap());

    (
        StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        headers,
        body,
    )
        .into_response()
}

fn send_xml_error_code(extra_headers: &HeaderMap, error_type: &str, status_code: u16) -> Response {
    let body = format!("<{error_type}/>\n");
    let mut headers = extra_headers.clone();
    headers.insert("Content-Length", body.len().to_string().parse().unwrap());

    (
        StatusCode::from_u16(status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        headers,
        body,
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
    let mut headers = extra_headers.clone();
    headers.insert(
        "x-amzn-ErrorType",
        error_type.parse().expect("error_type must be valid ASCII"),
    );
    if content_valid {
        let err = KinesisErrorResponse::new(status_code, error_type, Some(message));
        send_json_response(&headers, content_type, &err.body, status_code)
    } else {
        send_xml_error(&headers, error_type, message, status_code)
    }
}
