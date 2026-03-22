use serde::Deserialize;
use serde_json::json;
use wasm_bindgen::JsValue;
use wasm_bindgen_test::wasm_bindgen_test;

use ferrokinesis_wasm::Kinesis;

#[derive(Deserialize)]
struct Response {
    status: u16,
    body: String,
}

#[wasm_bindgen_test(async)]
async fn create_put_get_records_roundtrip() {
    let kinesis = new_kinesis(json!({
        "createStreamMs": 0,
        "deleteStreamMs": 0,
        "updateStreamMs": 0,
    }));

    let create = request(
        &kinesis,
        "Kinesis_20131202.CreateStream",
        json!({
            "StreamName": "wasm-stream",
            "ShardCount": 1,
        }),
    )
    .await;
    assert_eq!(create.status, 200);

    wait_until_stream_active(&kinesis, "wasm-stream").await;

    let put = request(
        &kinesis,
        "Kinesis_20131202.PutRecord",
        json!({
            "StreamName": "wasm-stream",
            "PartitionKey": "pk-1",
            "Data": "aGVsbG8=",
        }),
    )
    .await;
    assert_eq!(put.status, 200);

    let put_body: serde_json::Value = serde_json::from_str(&put.body).unwrap();
    let shard_id = put_body["ShardId"].as_str().unwrap();

    let iter = request(
        &kinesis,
        "Kinesis_20131202.GetShardIterator",
        json!({
            "StreamName": "wasm-stream",
            "ShardId": shard_id,
            "ShardIteratorType": "TRIM_HORIZON",
        }),
    )
    .await;
    assert_eq!(iter.status, 200);

    let iter_body: serde_json::Value = serde_json::from_str(&iter.body).unwrap();
    let iterator = iter_body["ShardIterator"].as_str().unwrap();

    let records = request(
        &kinesis,
        "Kinesis_20131202.GetRecords",
        json!({
            "ShardIterator": iterator,
        }),
    )
    .await;
    assert_eq!(records.status, 200);

    let records_body: serde_json::Value = serde_json::from_str(&records.body).unwrap();
    let items = records_body["Records"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["Data"].as_str(), Some("aGVsbG8="));
    assert_eq!(items[0]["PartitionKey"].as_str(), Some("pk-1"));
}

#[wasm_bindgen_test(async)]
async fn create_stream_delay_is_preserved() {
    let kinesis = new_kinesis(json!({
        "createStreamMs": 100,
        "deleteStreamMs": 0,
        "updateStreamMs": 0,
    }));

    let create = request(
        &kinesis,
        "Kinesis_20131202.CreateStream",
        json!({
            "StreamName": "delayed-create",
            "ShardCount": 1,
        }),
    )
    .await;
    assert_eq!(create.status, 200);

    assert_eq!(
        describe_stream_status(&kinesis, "delayed-create")
            .await
            .as_deref(),
        Some("CREATING")
    );

    sleep_ms(25).await;
    assert_eq!(
        describe_stream_status(&kinesis, "delayed-create")
            .await
            .as_deref(),
        Some("CREATING")
    );

    wait_until_stream_active(&kinesis, "delayed-create").await;
}

#[wasm_bindgen_test(async)]
async fn delete_stream_delay_is_preserved() {
    let kinesis = new_kinesis(json!({
        "createStreamMs": 0,
        "deleteStreamMs": 100,
        "updateStreamMs": 0,
    }));

    let create = request(
        &kinesis,
        "Kinesis_20131202.CreateStream",
        json!({
            "StreamName": "delayed-delete",
            "ShardCount": 1,
        }),
    )
    .await;
    assert_eq!(create.status, 200);

    wait_until_stream_active(&kinesis, "delayed-delete").await;

    let delete = request(
        &kinesis,
        "Kinesis_20131202.DeleteStream",
        json!({
            "StreamName": "delayed-delete",
        }),
    )
    .await;
    assert_eq!(delete.status, 200);

    assert_eq!(
        describe_stream_status(&kinesis, "delayed-delete")
            .await
            .as_deref(),
        Some("DELETING")
    );

    sleep_ms(25).await;
    assert_eq!(
        describe_stream_status(&kinesis, "delayed-delete")
            .await
            .as_deref(),
        Some("DELETING")
    );

    wait_until_stream_missing(&kinesis, "delayed-delete").await;
}

#[wasm_bindgen_test(async)]
async fn update_stream_delay_is_preserved() {
    let kinesis = new_kinesis(json!({
        "createStreamMs": 0,
        "deleteStreamMs": 0,
        "updateStreamMs": 100,
    }));

    let create = request(
        &kinesis,
        "Kinesis_20131202.CreateStream",
        json!({
            "StreamName": "delayed-update",
            "ShardCount": 1,
        }),
    )
    .await;
    assert_eq!(create.status, 200);

    wait_until_stream_active(&kinesis, "delayed-update").await;

    let start = request(
        &kinesis,
        "Kinesis_20131202.StartStreamEncryption",
        json!({
            "StreamName": "delayed-update",
            "EncryptionType": "KMS",
            "KeyId": "alias/wasm-test",
        }),
    )
    .await;
    assert_eq!(start.status, 200);

    let summary = describe_stream_summary(&kinesis, "delayed-update").await;
    assert_eq!(
        summary["StreamDescriptionSummary"]["StreamStatus"].as_str(),
        Some("UPDATING")
    );

    sleep_ms(25).await;
    let summary = describe_stream_summary(&kinesis, "delayed-update").await;
    assert_eq!(
        summary["StreamDescriptionSummary"]["StreamStatus"].as_str(),
        Some("UPDATING")
    );

    let summary = wait_until_summary_status(&kinesis, "delayed-update", "ACTIVE").await;
    assert_eq!(
        summary["StreamDescriptionSummary"]["EncryptionType"].as_str(),
        Some("KMS")
    );
    assert_eq!(
        summary["StreamDescriptionSummary"]["KeyId"].as_str(),
        Some("alias/wasm-test")
    );
}

async fn wait_until_stream_active(kinesis: &Kinesis, stream_name: &str) {
    wait_until_stream_status(kinesis, stream_name, "ACTIVE").await;
}

async fn wait_until_stream_status(kinesis: &Kinesis, stream_name: &str, expected: &str) {
    for _ in 0..40 {
        if describe_stream_status(kinesis, stream_name)
            .await
            .as_deref()
            == Some(expected)
        {
            return;
        }

        sleep_ms(25).await;
    }

    panic!("stream did not reach expected status {expected}");
}

async fn wait_until_stream_missing(kinesis: &Kinesis, stream_name: &str) {
    for _ in 0..40 {
        let response = request(
            kinesis,
            "Kinesis_20131202.DescribeStream",
            json!({ "StreamName": stream_name }),
        )
        .await;

        if response.status == 400 && response.body.contains("ResourceNotFoundException") {
            return;
        }

        sleep_ms(25).await;
    }

    panic!("stream was not deleted");
}

async fn wait_until_summary_status(
    kinesis: &Kinesis,
    stream_name: &str,
    expected: &str,
) -> serde_json::Value {
    for _ in 0..40 {
        let summary = describe_stream_summary(kinesis, stream_name).await;
        if summary["StreamDescriptionSummary"]["StreamStatus"].as_str() == Some(expected) {
            return summary;
        }

        sleep_ms(25).await;
    }

    panic!("stream summary did not reach expected status {expected}");
}

async fn describe_stream_status(kinesis: &Kinesis, stream_name: &str) -> Option<String> {
    let response = request(
        kinesis,
        "Kinesis_20131202.DescribeStream",
        json!({ "StreamName": stream_name }),
    )
    .await;

    if response.status != 200 {
        return None;
    }

    let body: serde_json::Value = serde_json::from_str(&response.body).unwrap();
    body["StreamDescription"]["StreamStatus"]
        .as_str()
        .map(str::to_string)
}

async fn describe_stream_summary(kinesis: &Kinesis, stream_name: &str) -> serde_json::Value {
    let response = request(
        kinesis,
        "Kinesis_20131202.DescribeStreamSummary",
        json!({ "StreamName": stream_name }),
    )
    .await;
    assert_eq!(response.status, 200);
    serde_json::from_str(&response.body).unwrap()
}

async fn request(kinesis: &Kinesis, target: &str, body: serde_json::Value) -> Response {
    decode(kinesis.request(target, &body.to_string()).await.unwrap())
}

fn new_kinesis(options: serde_json::Value) -> Kinesis {
    let options = serde_wasm_bindgen::to_value(&options).unwrap();
    Kinesis::new(Some(options)).unwrap()
}

async fn sleep_ms(ms: u64) {
    ferrokinesis::runtime::sleep_ms(ms).await;
}

fn decode(value: JsValue) -> Response {
    serde_wasm_bindgen::from_value(value).unwrap()
}
