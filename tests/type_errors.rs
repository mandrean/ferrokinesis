/// Tests that exercise validation type-checking and constraint error paths.
mod common;

use common::*;
use serde_json::json;

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
                "StreamARN": "arn:aws:kinesis:us-east-1:0000-0000-0000:stream/te-struct-str",
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
                "StreamARN": "arn:aws:kinesis:us-east-1:0000-0000-0000:stream/te-struct-num",
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
                "StreamARN": "arn:aws:kinesis:us-east-1:0000-0000-0000:stream/te-struct-arr",
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
