mod common;

use common::*;
use ferrokinesis::store::Store;
use ferrokinesis::store::StoreOptions;
use serde_json::{Value, json};

// -- AddTagsToStream --

#[tokio::test]
async fn add_tags_success() {
    let server = TestServer::new().await;
    let name = "test-add-tags";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": name,
                "Tags": {"key1": "value1", "key2": "value2"},
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    // Verify tags
    let res = server
        .request("ListTagsForStream", &json!({"StreamName": name}))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 2);
    assert_eq!(body["HasMoreTags"], false);
}

#[tokio::test]
async fn add_tags_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": "nonexistent",
                "Tags": {"key1": "value1"},
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn add_tags_over_50_limit() {
    let server = TestServer::new().await;
    let name = "test-tags-limit";
    server.create_stream(name, 1).await;

    // Add 50 tags in batches of 10 (validation limits Tags map to 10 per request)
    for batch in 0..5 {
        let mut tags = serde_json::Map::new();
        for i in 0..10 {
            tags.insert(
                format!("key{:02}", batch * 10 + i),
                json!(format!("val{}", batch * 10 + i)),
            );
        }
        let res = server
            .request(
                "AddTagsToStream",
                &json!({"StreamName": name, "Tags": tags}),
            )
            .await;
        assert_eq!(res.status(), 200);
    }

    // Try to add one more (exceeds 50 tag limit)
    let res = server
        .request(
            "AddTagsToStream",
            &json!({"StreamName": name, "Tags": {"extra": "tag"}}),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn add_tags_update_existing() {
    let server = TestServer::new().await;
    let name = "test-tags-update";
    server.create_stream(name, 1).await;

    server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": name,
                "Tags": {"key1": "value1"},
            }),
        )
        .await;

    // Update existing tag
    server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": name,
                "Tags": {"key1": "updated"},
            }),
        )
        .await;

    let res = server
        .request("ListTagsForStream", &json!({"StreamName": name}))
        .await;
    let body: Value = res.json().await.unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Key"], "key1");
    assert_eq!(tags[0]["Value"], "updated");
}

// -- RemoveTagsFromStream --

#[tokio::test]
async fn remove_tags_success() {
    let server = TestServer::new().await;
    let name = "test-remove-tags";
    server.create_stream(name, 1).await;

    server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": name,
                "Tags": {"key1": "value1", "key2": "value2", "key3": "value3"},
            }),
        )
        .await;

    let res = server
        .request(
            "RemoveTagsFromStream",
            &json!({
                "StreamName": name,
                "TagKeys": ["key1", "key3"],
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let res = server
        .request("ListTagsForStream", &json!({"StreamName": name}))
        .await;
    let body: Value = res.json().await.unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Key"], "key2");
}

#[tokio::test]
async fn remove_tags_nonexistent_key() {
    let server = TestServer::new().await;
    let name = "test-remove-nonexistent";
    server.create_stream(name, 1).await;

    // Should succeed even if key doesn't exist
    let res = server
        .request(
            "RemoveTagsFromStream",
            &json!({
                "StreamName": name,
                "TagKeys": ["nonexistent"],
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

// -- ListTagsForStream --

#[tokio::test]
async fn list_tags_empty() {
    let server = TestServer::new().await;
    let name = "test-list-tags-empty";
    server.create_stream(name, 1).await;

    let res = server
        .request("ListTagsForStream", &json!({"StreamName": name}))
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["Tags"].as_array().unwrap().len(), 0);
    assert_eq!(body["HasMoreTags"], false);
}

#[tokio::test]
async fn list_tags_sorted() {
    let server = TestServer::new().await;
    let name = "test-list-tags-sorted";
    server.create_stream(name, 1).await;

    server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": name,
                "Tags": {"charlie": "c", "alpha": "a", "bravo": "b"},
            }),
        )
        .await;

    let res = server
        .request("ListTagsForStream", &json!({"StreamName": name}))
        .await;
    let body: Value = res.json().await.unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags[0]["Key"], "alpha");
    assert_eq!(tags[1]["Key"], "bravo");
    assert_eq!(tags[2]["Key"], "charlie");
}

#[tokio::test]
async fn list_tags_with_exclusive_start() {
    let server = TestServer::new().await;
    let name = "test-list-tags-start";
    server.create_stream(name, 1).await;

    server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": name,
                "Tags": {"alpha": "a", "bravo": "b", "charlie": "c"},
            }),
        )
        .await;

    let res = server
        .request(
            "ListTagsForStream",
            &json!({
                "StreamName": name,
                "ExclusiveStartTagKey": "alpha",
            }),
        )
        .await;
    let body: Value = res.json().await.unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0]["Key"], "bravo");
    assert_eq!(tags[1]["Key"], "charlie");
}

#[tokio::test]
async fn list_tags_with_limit() {
    let server = TestServer::new().await;
    let name = "test-list-tags-limit";
    server.create_stream(name, 1).await;

    // Add 15 tags in two batches (max 10 per request)
    for batch in 0..2 {
        let mut tags = serde_json::Map::new();
        let count = if batch == 0 { 10 } else { 5 };
        for i in 0..count {
            let idx = batch * 10 + i;
            tags.insert(format!("key{idx:02}"), json!(format!("val{idx}")));
        }
        let res = server
            .request(
                "AddTagsToStream",
                &json!({"StreamName": name, "Tags": tags}),
            )
            .await;
        assert_eq!(res.status(), 200);
    }

    let res = server
        .request(
            "ListTagsForStream",
            &json!({"StreamName": name, "Limit": 5}),
        )
        .await;
    let body: Value = res.json().await.unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 5);
    assert_eq!(body["HasMoreTags"], true);
}

#[tokio::test]
async fn list_tags_stream_not_found() {
    let server = TestServer::new().await;
    let res = server
        .request("ListTagsForStream", &json!({"StreamName": "nonexistent"}))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn add_tags_invalid_char_in_key() {
    let server = TestServer::new().await;
    let name = "test-atts-inv-key";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": name,
                "Tags": {"foo!bar": "valid-value"},
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("invalid characters")
    );
}

#[tokio::test]
async fn add_tags_invalid_char_in_value() {
    let server = TestServer::new().await;
    let name = "test-atts-inv-val";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "AddTagsToStream",
            &json!({
                "StreamName": name,
                "Tags": {"valid-key": "bad!value"},
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn add_tags_percent_in_value_hits_regex() {
    let server = TestServer::new().await;
    let name = "test-atts-pct";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "AddTagsToStream",
            &json!({ "StreamName": name, "Tags": {"key": "val%ue"} }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("invalid characters")
    );
}

#[tokio::test]
async fn remove_tags_percent_in_key_hits_regex() {
    let server = TestServer::new().await;
    let name = "test-rtfs-pct";
    server.create_stream(name, 1).await;

    server
        .request(
            "AddTagsToStream",
            &json!({ "StreamName": name, "Tags": {"valid-key": "value"} }),
        )
        .await;

    let res = server
        .request(
            "RemoveTagsFromStream",
            &json!({ "StreamName": name, "TagKeys": ["key%invalid"] }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("invalid characters")
    );
}

#[tokio::test]
async fn remove_tags_tag_keys_not_array_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::remove_tags_from_stream::execute(
        &store,
        json!({ "StreamName": "test", "TagKeys": "not-an-array" }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.body.__type, "SerializationException");
}

#[tokio::test]
async fn add_tags_tags_not_object_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::add_tags_to_stream::execute(
        &store,
        json!({ "StreamName": "test", "Tags": "not-an-object" }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.body.__type, "SerializationException");
}

#[tokio::test]
async fn remove_tags_invalid_char_in_key() {
    let server = TestServer::new().await;
    let name = "cx-tags-inv-char";
    server.create_stream(name, 1).await;

    server
        .request(
            "AddTagsToStream",
            &json!({"StreamName": name, "Tags": {"valid-key": "value"}}),
        )
        .await;

    let res = server
        .request(
            "RemoveTagsFromStream",
            &json!({
                "StreamName": name,
                "TagKeys": ["invalid!key"],
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}
