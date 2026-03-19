/// Tests that exercise validation type-checking and constraint error paths.
mod common;

use common::*;
use ferrokinesis::validation::{self, FieldDef, FieldType, check_types, check_validations};
use serde_json::{Value, json};

// -- Blob field type errors (Data in PutRecord) --

#[tokio::test]
async fn blob_field_bool_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-blob-bool", 1).await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "te-blob-bool",
                "PartitionKey": "pk",
                "Data": true,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn blob_field_number_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-blob-num", 1).await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "te-blob-num",
                "PartitionKey": "pk",
                "Data": 42,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn blob_field_array_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-blob-arr", 1).await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "te-blob-arr",
                "PartitionKey": "pk",
                "Data": ["a", "b"],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn blob_field_object_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-blob-obj", 1).await;
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "te-blob-obj",
                "PartitionKey": "pk",
                "Data": {"key": "value"},
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn blob_field_invalid_base64_chars() {
    let server = TestServer::new().await;
    server.create_stream("te-blob-inv", 1).await;
    // '!' is not a valid base64 character
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "te-blob-inv",
                "PartitionKey": "pk",
                "Data": "AAAA!AAA",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn blob_field_not_multiple_of_4() {
    let server = TestServer::new().await;
    server.create_stream("te-blob-len", 1).await;
    // "ABC" is 3 chars, not a multiple of 4
    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "te-blob-len",
                "PartitionKey": "pk",
                "Data": "ABC",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

// -- Integer field type errors (Limit in ListStreams) --

#[tokio::test]
async fn integer_field_string_is_type_error() {
    let server = TestServer::new().await;
    let res = server
        .request("ListStreams", &json!({ "Limit": "not-a-number" }))
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn integer_field_bool_is_type_error() {
    let server = TestServer::new().await;
    let res = server
        .request("ListStreams", &json!({ "Limit": true }))
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn integer_field_array_is_type_error() {
    let server = TestServer::new().await;
    let res = server
        .request("ListStreams", &json!({ "Limit": [1, 2, 3] }))
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn integer_field_object_is_type_error() {
    let server = TestServer::new().await;
    let res = server
        .request("ListStreams", &json!({ "Limit": {"n": 10} }))
        .await;
    assert_eq!(res.status(), 400);
}

// -- String field type errors (StreamName in CreateStream) --

#[tokio::test]
async fn string_field_number_is_type_error() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "CreateStream",
            &json!({ "StreamName": 123, "ShardCount": 1 }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn string_field_bool_is_type_error() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "CreateStream",
            &json!({ "StreamName": true, "ShardCount": 1 }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn string_field_array_is_type_error() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "CreateStream",
            &json!({ "StreamName": ["a", "b"], "ShardCount": 1 }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn string_field_object_is_type_error() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "CreateStream",
            &json!({ "StreamName": {"n": "v"}, "ShardCount": 1 }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

// -- Map field type errors (Tags in AddTagsToStream) --

#[tokio::test]
async fn map_field_string_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-map-str", 1).await;
    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": "te-map-str",
                "Tags": "not-a-map",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn map_field_bool_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-map-bool", 1).await;
    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": "te-map-bool",
                "Tags": true,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn map_field_array_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-map-arr", 1).await;
    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": "te-map-arr",
                "Tags": [{"k": "v"}],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

// -- Structure field type errors (StreamModeDetails in UpdateStreamMode) --

#[tokio::test]
async fn structure_field_string_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-struct-str", 1).await;
    let res = server
        .request(
            "UpdateStreamMode",
            &json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/te-struct-str",
                "StreamModeDetails": "not-a-struct",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn structure_field_number_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-struct-num", 1).await;
    let res = server
        .request(
            "UpdateStreamMode",
            &json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/te-struct-num",
                "StreamModeDetails": 42,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn structure_field_array_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-struct-arr", 1).await;
    let res = server
        .request(
            "UpdateStreamMode",
            &json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/te-struct-arr",
                "StreamModeDetails": ["ON_DEMAND"],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

// -- Timestamp field type errors (Timestamp in GetShardIterator) --

#[tokio::test]
async fn timestamp_field_bool_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-ts-bool", 1).await;
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "te-ts-bool",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": true,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn timestamp_field_string_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-ts-str", 1).await;
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "te-ts-str",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": "2024-01-01T00:00:00Z",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn timestamp_field_array_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-ts-arr", 1).await;
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "te-ts-arr",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": [1000000000.0],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn timestamp_field_object_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-ts-obj", 1).await;
    let res = server
        .request(
            "GetShardIterator",
            &json!({
                "StreamName": "te-ts-obj",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_TIMESTAMP",
                "Timestamp": {"seconds": 1000000000},
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

// -- List field type errors (ShardLevelMetrics in EnableEnhancedMonitoring) --

#[tokio::test]
async fn list_field_string_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-list-str", 1).await;
    let res = server
        .request(
            "EnableEnhancedMonitoring",
            &json!({
                "StreamName": "te-list-str",
                "ShardLevelMetrics": "IncomingBytes",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn list_field_object_is_type_error() {
    let server = TestServer::new().await;
    server.create_stream("te-list-obj", 1).await;
    let res = server
        .request(
            "EnableEnhancedMonitoring",
            &json!({
                "StreamName": "te-list-obj",
                "ShardLevelMetrics": {"metric": "IncomingBytes"},
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

// -- Map child key/value length constraints (Tags in AddTagsToStream) --

#[tokio::test]
async fn map_child_key_too_long() {
    let server = TestServer::new().await;
    server.create_stream("te-key-long", 1).await;
    // Tag key > 128 chars
    let long_key = "k".repeat(129);
    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": "te-key-long",
                "Tags": { long_key: "value" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn map_child_value_too_long() {
    let server = TestServer::new().await;
    server.create_stream("te-val-long", 1).await;
    // Tag value > 256 chars
    let long_val = "v".repeat(257);
    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": "te-val-long",
                "Tags": { "key": long_val },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

// -- List child length constraints (TagKeys in RemoveTagsFromStream has child_lengths(1,128)) --

#[tokio::test]
async fn list_child_item_too_long() {
    let server = TestServer::new().await;
    server.create_stream("te-item-long", 1).await;
    // RemoveTagsFromStream.TagKeys has child_lengths(1, 128); a key > 128 chars fails
    let long_key = "k".repeat(129);
    let res = server
        .request(
            "RemoveTagsFromStream",
            &json!({
                "StreamName": "te-item-long",
                "TagKeys": [long_key],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

// -- Multiple validation errors at once --

#[tokio::test]
async fn multiple_validation_errors() {
    let server = TestServer::new().await;
    server.create_stream("te-multi", 1).await;
    // AddTagsToStream with multiple bad tags to generate multiple errors
    let mut tags = serde_json::Map::new();
    for i in 0..12 {
        // each key is too long (> 128 chars), generating > 10 errors
        let long_key = format!("{}{}", "k".repeat(129), i);
        tags.insert(long_key, serde_json::Value::String("v".to_string()));
    }
    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": "te-multi",
                "Tags": tags,
            }),
        )
        .await;
    // Should get validation error (possibly multiple)
    assert_eq!(res.status(), 400);
}

#[test]
fn check_type_boolean_wrong_types() {
    let bool_field = FieldDef::new(FieldType::Boolean);

    let r = check_types(&json!({ "F": 1 }), &[("F", &bool_field)]);
    assert!(r.is_err(), "Boolean+Number should error");

    let r = check_types(&json!({ "F": "true" }), &[("F", &bool_field)]);
    assert!(r.is_err(), "Boolean+String should error");

    let r = check_types(&json!({ "F": [true] }), &[("F", &bool_field)]);
    assert!(r.is_err(), "Boolean+Array should error");

    let r = check_types(&json!({ "F": {} }), &[("F", &bool_field)]);
    assert!(r.is_err(), "Boolean+Object should error");

    let r = check_types(&json!({ "F": true }), &[("F", &bool_field)]);
    assert!(r.is_ok(), "Boolean+Bool should succeed");
}

#[tokio::test]
async fn blob_embedded_equals_not_at_end() {
    let server = TestServer::new().await;
    server.create_stream("test-blob-eq", 1).await;

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "test-blob-eq",
                "PartitionKey": "pk",
                "Data": "AA=A",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
}

#[tokio::test]
async fn blob_equals_at_start_followed_by_non_equal() {
    let server = TestServer::new().await;
    server.create_stream("test-blob-eq2", 1).await;

    let res = server
        .request(
            "PutRecord",
            &json!({
                "StreamName": "test-blob-eq2",
                "PartitionKey": "pk",
                "Data": "=abc",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "SerializationException");
}

#[tokio::test]
async fn validation_list_field_with_bool() {
    let server = TestServer::new().await;
    server.create_stream("test-val-list-bool", 1).await;

    let res = server
        .request(
            "RemoveTagsFromStream",
            &json!({ "StreamName": "test-val-list-bool", "TagKeys": true }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "SerializationException");
}

#[tokio::test]
async fn validation_map_field_with_bool() {
    let server = TestServer::new().await;
    server.create_stream("test-val-map-bool", 1).await;

    let res = server
        .request(
            "AddTagsToStream",
            &json!({ "StreamName": "test-val-map-bool", "Tags": true }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "SerializationException");
}

// -- Direct validation unit tests for uncovered paths --

#[test]
fn to_lower_first_empty_string() {
    assert_eq!(validation::to_lower_first(""), "");
}

#[test]
fn check_types_non_object_returns_empty() {
    let field = FieldDef::new(FieldType::String);
    let result = check_types(&json!("not-an-object"), &[("F", &field)]).unwrap();
    assert_eq!(result, json!({}));
}

#[test]
fn check_types_null_field_is_skipped() {
    let field = FieldDef::new(FieldType::String);
    let result = check_types(&json!({"F": null}), &[("F", &field)]).unwrap();
    assert!(!result.as_object().unwrap().contains_key("F"));
}

#[test]
fn check_type_short_clamped_to_max() {
    let field = FieldDef::new(FieldType::Short);
    let result = check_types(&json!({"F": 99999}), &[("F", &field)]).unwrap();
    assert_eq!(result["F"], 32767);
}

#[test]
fn check_type_integer_clamped_to_max() {
    let field = FieldDef::new(FieldType::Integer);
    let result = check_types(&json!({"F": 9_999_999_999i64}), &[("F", &field)]).unwrap();
    assert_eq!(result["F"], 2147483647);
}

#[test]
fn check_type_double_passes_through() {
    let field = FieldDef::new(FieldType::Double);
    let result = check_types(&json!({"F": 1.5}), &[("F", &field)]).unwrap();
    assert!((result["F"].as_f64().unwrap() - 1.5).abs() < 0.001);
}

#[test]
fn check_type_double_rejects_bool() {
    let field = FieldDef::new(FieldType::Double);
    assert!(check_types(&json!({"F": true}), &[("F", &field)]).is_err());
}

#[test]
fn check_type_double_rejects_string() {
    let field = FieldDef::new(FieldType::Double);
    assert!(check_types(&json!({"F": "3.14"}), &[("F", &field)]).is_err());
}

#[test]
fn check_type_double_rejects_array() {
    let field = FieldDef::new(FieldType::Double);
    assert!(check_types(&json!({"F": [1.0]}), &[("F", &field)]).is_err());
}

#[test]
fn check_type_double_rejects_object() {
    let field = FieldDef::new(FieldType::Double);
    assert!(check_types(&json!({"F": {"n": 1}}), &[("F", &field)]).is_err());
}

#[test]
fn check_type_long_rejects_bool() {
    let field = FieldDef::new(FieldType::Long);
    assert!(check_types(&json!({"F": true}), &[("F", &field)]).is_err());
}

#[test]
fn check_validations_not_null_rejects_null() {
    let field = FieldDef::new(FieldType::String).not_null();
    let data = json!({"F": null});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(msg.contains("must not be null"), "got: {msg}");
}

#[test]
fn check_validations_blob_length_too_short() {
    // "AAAA" decodes to 3 bytes; require >= 10
    let field = FieldDef::new(FieldType::Blob).len_gte(10);
    let data = json!({"F": "AAAA"});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(
        msg.contains("length greater than or equal to 10"),
        "got: {msg}"
    );
}

#[test]
fn check_validations_blob_length_too_long() {
    // base64 "AAAAAAAAAA==" decodes to 7 bytes; require <= 4
    let field = FieldDef::new(FieldType::Blob).len_lte(4);
    let data = json!({"F": "AAAAAAAAAA=="});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(
        msg.contains("HeapByteBuffer"),
        "blob value_str should use HeapByteBuffer: {msg}"
    );
}

#[test]
fn check_validations_list_child_constraint_triggers_value_str() {
    let child = FieldDef::new(FieldType::String).len_lte(3);
    let field = FieldDef::new(FieldType::List {
        children: Box::new(child),
    });
    let data = json!({"F": ["ab", "toolong"]});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(msg.contains("length less than or equal to 3"), "got: {msg}");
}

#[test]
fn check_validations_map_child_value_constraint_triggers_value_str() {
    let child = FieldDef::new(FieldType::String).len_lte(2);
    let field = FieldDef::new(FieldType::Map {
        children: Box::new(child),
    });
    let data = json!({"F": {"k": "toolong"}});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(msg.contains("length less than or equal to 2"), "got: {msg}");
}

#[test]
fn check_validations_structure_child_constraint() {
    let inner = FieldDef::new(FieldType::String).len_gte(5);
    let field = FieldDef::new(FieldType::Structure {
        children: vec![("Inner".to_string(), inner)],
    });
    let data = json!({"F": {"Inner": "ab"}});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(msg.contains("f.inner"), "structure parent prefix: {msg}");
}

#[test]
fn check_validations_integer_value_too_small() {
    let field = FieldDef::new(FieldType::Integer).gte(10.0);
    let data = json!({"F": 5});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(msg.contains("'5'"), "integer value_str: {msg}");
}

#[test]
fn check_validations_custom_callback() {
    let field = FieldDef::new(FieldType::String);
    let data = json!({"F": "ok"});
    let custom = |_data: &Value| -> Option<String> { Some("custom error".to_string()) };
    let result = check_validations(&data, &[("F", &field)], Some(&custom));
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(msg.contains("custom error"), "got: {msg}");
}

#[test]
fn check_validations_enum_failure() {
    let field = FieldDef::new(FieldType::String).enum_values(vec!["A", "B"]);
    let data = json!({"F": "C"});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(msg.contains("enum value set"), "got: {msg}");
}

#[test]
fn check_validations_lte_failure() {
    let field = FieldDef::new(FieldType::Integer).lte(5.0);
    let data = json!({"F": 10});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(msg.contains("less than or equal to 5"), "got: {msg}");
}

#[test]
fn check_validations_regex_failure() {
    let field = FieldDef::new(FieldType::String).regex("[a-z]+");
    let data = json!({"F": "123"});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
    let msg = result.unwrap_err().body.message.unwrap();
    assert!(msg.contains("regular expression"), "got: {msg}");
}

#[test]
fn get_data_length_array_and_object() {
    // List field: array length is counted
    let field = FieldDef::new(FieldType::List {
        children: Box::new(FieldDef::new(FieldType::String)),
    })
    .len_gte(5);
    let data = json!({"F": ["a", "b"]});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());

    // Map field: object key count is counted
    let field = FieldDef::new(FieldType::Map {
        children: Box::new(FieldDef::new(FieldType::String)),
    })
    .len_gte(3);
    let data = json!({"F": {"a": "1"}});
    let result = check_validations(&data, &[("F", &field)], None);
    assert!(result.is_err());
}
