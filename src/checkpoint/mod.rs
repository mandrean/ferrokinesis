use axum::body::Bytes;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub(crate) const DYNAMODB_API: &str = "DynamoDB_20120810";
const DDB_CONTENT_TYPE_JSON: &str = "application/x-amz-json-1.0";

#[derive(Clone)]
pub(crate) struct CheckpointStore {
    aws_account_id: String,
    aws_region: String,
    tables: Arc<RwLock<HashMap<String, TableMeta>>>,
}

impl CheckpointStore {
    pub(crate) fn new(aws_account_id: String, aws_region: String) -> Self {
        Self {
            aws_account_id,
            aws_region,
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) fn export_snapshot_json(&self) -> Result<Vec<u8>, String> {
        let tables = self
            .tables
            .read()
            .map_err(|_| "checkpoint table store lock poisoned".to_string())?;
        let mut snapshot: Vec<TableMeta> = tables.values().cloned().collect();
        snapshot.sort_by(|left, right| left.table_name.cmp(&right.table_name));
        serde_json::to_vec(&snapshot)
            .map_err(|err| format!("failed to encode persisted checkpoint tables: {err}"))
    }

    pub(crate) fn restore_snapshot_json(&self, bytes: &[u8]) -> Result<(), String> {
        let restored: Vec<TableMeta> = if bytes.is_empty() {
            Vec::new()
        } else {
            serde_json::from_slice(bytes)
                .map_err(|err| format!("failed to decode persisted checkpoint tables: {err}"))?
        };
        let mut tables = self
            .tables
            .write()
            .map_err(|_| "checkpoint table store lock poisoned".to_string())?;
        tables.clear();
        for table in restored {
            tables.insert(table.table_name.clone(), table);
        }
        Ok(())
    }

    pub(crate) fn create_table(
        &self,
        input: CreateTableRequest,
    ) -> Result<TableDescription, DdbError> {
        if input.table_name.trim().is_empty() {
            return Err(DdbError::validation(
                "Value null at 'tableName' failed to satisfy constraint: Member must not be null",
            ));
        }
        if input.key_schema.is_empty() {
            return Err(DdbError::validation(
                "One or more parameter values were invalid: KeySchema is required",
            ));
        }

        let mut tables = self
            .tables
            .write()
            .map_err(|_| DdbError::internal("table store lock poisoned"))?;

        if tables.contains_key(&input.table_name) {
            return Err(DdbError::resource_in_use(format!(
                "Table already exists: {}",
                input.table_name
            )));
        }

        let created_at_seconds = crate::util::current_time_ms() as f64 / 1000.0;
        let table_meta = TableMeta {
            table_name: input.table_name.clone(),
            attribute_definitions: input.attribute_definitions,
            key_schema: input.key_schema,
            global_secondary_indexes: input
                .global_secondary_indexes
                .into_iter()
                .map(GlobalSecondaryIndexMeta::from_request)
                .collect(),
            table_id: uuid::Uuid::new_v4().to_string(),
            created_at_seconds,
        };
        let table_description = table_meta.as_description(&self.aws_account_id, &self.aws_region);
        tables.insert(input.table_name, table_meta);
        Ok(table_description)
    }

    pub(crate) fn describe_table(
        &self,
        input: DescribeTableRequest,
    ) -> Result<TableDescription, DdbError> {
        let tables = self
            .tables
            .read()
            .map_err(|_| DdbError::internal("table store lock poisoned"))?;
        let Some(table_meta) = tables.get(&input.table_name) else {
            return Err(DdbError::resource_not_found(format!(
                "Requested resource not found: Table: {} not found",
                input.table_name
            )));
        };
        Ok(table_meta.as_description(&self.aws_account_id, &self.aws_region))
    }

    pub(crate) fn remove_table(&self, table_name: &str) -> Result<(), DdbError> {
        let mut tables = self
            .tables
            .write()
            .map_err(|_| DdbError::internal("table store lock poisoned"))?;
        tables.remove(table_name);
        Ok(())
    }
}

pub(crate) async fn handle_request(
    uri: &Uri,
    headers: &HeaderMap,
    response_headers: &HeaderMap,
    store: &crate::store::Store,
    operation: &str,
    body: &Bytes,
) -> Response {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();

    if content_type != DDB_CONTENT_TYPE_JSON {
        return send_error(
            response_headers,
            DdbError::serialization("Unsupported content type for DynamoDB target"),
        );
    }

    if body.is_empty() {
        return send_error(
            response_headers,
            DdbError::serialization("Request body is empty"),
        );
    }

    if let Err(error) = validate_auth(headers, uri) {
        return send_error(response_headers, error);
    }

    let payload: Value = match serde_json::from_slice(body) {
        Ok(Value::Object(map)) => Value::Object(map),
        Ok(_) | Err(_) => {
            return send_error(
                response_headers,
                DdbError::serialization("Could not parse request body as JSON object"),
            );
        }
    };

    let result = match operation {
        "CreateTable" => {
            let input = serde_json::from_value(payload)
                .map_err(|_| DdbError::serialization("Invalid CreateTable request body"));
            match input {
                Ok(input) => store
                    .checkpoint_create_table(input)
                    .await
                    .map(|table| json!({ "TableDescription": table })),
                Err(err) => Err(err),
            }
        }
        "DescribeTable" => {
            let input = serde_json::from_value(payload)
                .map_err(|_| DdbError::serialization("Invalid DescribeTable request body"));
            match input {
                Ok(input) => store
                    .checkpoint_describe_table(input)
                    .map(|table| json!({ "Table": table })),
                Err(err) => Err(err),
            }
        }
        _ => Err(DdbError::unknown_operation(operation)),
    };

    match result {
        Ok(result) => send_json_response(response_headers, StatusCode::OK, &result),
        Err(error) => send_error(response_headers, error),
    }
}

fn validate_auth(headers: &HeaderMap, uri: &Uri) -> Result<(), DdbError> {
    let auth_header = headers.get("authorization").and_then(|v| v.to_str().ok());
    let query_string = uri.query().unwrap_or("");
    let auth_query = query_string.contains("X-Amz-Algorithm");

    if auth_header.is_some() && auth_query {
        return Err(DdbError::invalid_signature(
            "Found both 'X-Amz-Algorithm' as a query-string param and 'Authorization' as HTTP header.",
        ));
    }

    if auth_header.is_none() && !auth_query {
        return Err(DdbError::missing_auth_token("Missing Authentication Token"));
    }

    if let Some(auth) = auth_header {
        let mut msg = String::new();
        let auth_params: HashMap<String, String> = auth
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
            return Err(DdbError::incomplete_signature(msg));
        }
        return Ok(());
    }

    let query_params: HashMap<String, String> = query_string
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
        return Err(DdbError::incomplete_signature(msg));
    }

    Ok(())
}

fn send_error(extra_headers: &HeaderMap, error: DdbError) -> Response {
    let body = json!({
        "__type": format!("com.amazonaws.dynamodb.v20120810#{}", error.error_type),
        "message": error.message,
    });
    send_json_response(
        extra_headers,
        StatusCode::from_u16(error.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        &body,
    )
}

fn send_json_response(extra_headers: &HeaderMap, status: StatusCode, body: &Value) -> Response {
    let mut headers = extra_headers.clone();
    let body_bytes = serde_json::to_vec(body).unwrap_or_default();
    headers.insert("Content-Type", DDB_CONTENT_TYPE_JSON.parse().unwrap());
    headers.insert(
        "Content-Length",
        body_bytes.len().to_string().parse().unwrap(),
    );
    (status, headers, body_bytes).into_response()
}

#[derive(Debug)]
pub(crate) struct DdbError {
    status_code: u16,
    error_type: &'static str,
    message: String,
}

impl DdbError {
    fn new(status_code: u16, error_type: &'static str, message: impl Into<String>) -> Self {
        Self {
            status_code,
            error_type,
            message: message.into(),
        }
    }

    fn serialization(message: impl Into<String>) -> Self {
        Self::new(400, "SerializationException", message)
    }

    fn unknown_operation(operation: &str) -> Self {
        Self::new(
            400,
            "UnknownOperationException",
            format!("Unknown operation: {operation}"),
        )
    }

    fn missing_auth_token(message: impl Into<String>) -> Self {
        Self::new(400, "MissingAuthenticationTokenException", message)
    }

    fn invalid_signature(message: impl Into<String>) -> Self {
        Self::new(400, "InvalidSignatureException", message)
    }

    fn incomplete_signature(message: impl Into<String>) -> Self {
        Self::new(403, "IncompleteSignatureException", message)
    }

    fn validation(message: impl Into<String>) -> Self {
        Self::new(400, "ValidationException", message)
    }

    fn resource_in_use(message: impl Into<String>) -> Self {
        Self::new(400, "ResourceInUseException", message)
    }

    fn resource_not_found(message: impl Into<String>) -> Self {
        Self::new(400, "ResourceNotFoundException", message)
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self::new(500, "InternalServerError", message)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableMeta {
    table_name: String,
    attribute_definitions: Vec<AttributeDefinition>,
    key_schema: Vec<KeySchemaElement>,
    #[serde(default)]
    global_secondary_indexes: Vec<GlobalSecondaryIndexMeta>,
    table_id: String,
    created_at_seconds: f64,
}

impl TableMeta {
    fn as_description(&self, aws_account_id: &str, aws_region: &str) -> TableDescription {
        TableDescription {
            attribute_definitions: self.attribute_definitions.clone(),
            key_schema: self.key_schema.clone(),
            table_name: self.table_name.clone(),
            table_status: "ACTIVE".to_string(),
            creation_date_time: self.created_at_seconds,
            table_arn: format!(
                "arn:aws:dynamodb:{aws_region}:{aws_account_id}:table/{}",
                self.table_name
            ),
            table_id: self.table_id.clone(),
            global_secondary_indexes: self
                .global_secondary_indexes
                .iter()
                .map(|index| index.as_description(&self.table_name, aws_account_id, aws_region))
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct CreateTableRequest {
    pub(crate) table_name: String,
    #[serde(default)]
    attribute_definitions: Vec<AttributeDefinition>,
    #[serde(default)]
    key_schema: Vec<KeySchemaElement>,
    #[serde(default)]
    global_secondary_indexes: Vec<GlobalSecondaryIndexRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct DescribeTableRequest {
    pub(crate) table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AttributeDefinition {
    attribute_name: String,
    attribute_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct KeySchemaElement {
    attribute_name: String,
    key_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Projection {
    projection_type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    non_key_attributes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GlobalSecondaryIndexRequest {
    index_name: String,
    #[serde(default)]
    key_schema: Vec<KeySchemaElement>,
    projection: Projection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GlobalSecondaryIndexMeta {
    index_name: String,
    key_schema: Vec<KeySchemaElement>,
    projection: Projection,
}

impl GlobalSecondaryIndexMeta {
    fn from_request(request: GlobalSecondaryIndexRequest) -> Self {
        Self {
            index_name: request.index_name,
            key_schema: request.key_schema,
            projection: request.projection,
        }
    }

    fn as_description(
        &self,
        table_name: &str,
        aws_account_id: &str,
        aws_region: &str,
    ) -> GlobalSecondaryIndexDescription {
        GlobalSecondaryIndexDescription {
            index_name: self.index_name.clone(),
            key_schema: self.key_schema.clone(),
            projection: self.projection.clone(),
            index_status: "ACTIVE".to_string(),
            index_arn: format!(
                "arn:aws:dynamodb:{aws_region}:{aws_account_id}:table/{table_name}/index/{}",
                self.index_name
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
struct GlobalSecondaryIndexDescription {
    index_name: String,
    key_schema: Vec<KeySchemaElement>,
    projection: Projection,
    index_status: String,
    index_arn: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct TableDescription {
    attribute_definitions: Vec<AttributeDefinition>,
    key_schema: Vec<KeySchemaElement>,
    table_name: String,
    table_status: String,
    creation_date_time: f64,
    table_arn: String,
    table_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    global_secondary_indexes: Vec<GlobalSecondaryIndexDescription>,
}
