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
    let options = serde_wasm_bindgen::to_value(&json!({
        "createStreamMs": 0,
        "deleteStreamMs": 0,
        "updateStreamMs": 0,
    }))
    .unwrap();
    let kinesis = Kinesis::new(Some(options)).unwrap();

    let create = decode(
        kinesis
            .request(
                "Kinesis_20131202.CreateStream",
                &json!({
                    "StreamName": "wasm-stream",
                    "ShardCount": 1,
                })
                .to_string(),
            )
            .await
            .unwrap(),
    );
    assert_eq!(create.status, 200);

    wait_until_stream_active(&kinesis, "wasm-stream").await;

    let put = decode(
        kinesis
            .request(
                "Kinesis_20131202.PutRecord",
                &json!({
                    "StreamName": "wasm-stream",
                    "PartitionKey": "pk-1",
                    "Data": "aGVsbG8=",
                })
                .to_string(),
            )
            .await
            .unwrap(),
    );
    assert_eq!(put.status, 200);

    let put_body: serde_json::Value = serde_json::from_str(&put.body).unwrap();
    let shard_id = put_body["ShardId"].as_str().unwrap();

    let iter = decode(
        kinesis
            .request(
                "Kinesis_20131202.GetShardIterator",
                &json!({
                    "StreamName": "wasm-stream",
                    "ShardId": shard_id,
                    "ShardIteratorType": "TRIM_HORIZON",
                })
                .to_string(),
            )
            .await
            .unwrap(),
    );
    assert_eq!(iter.status, 200);

    let iter_body: serde_json::Value = serde_json::from_str(&iter.body).unwrap();
    let iterator = iter_body["ShardIterator"].as_str().unwrap();

    let records = decode(
        kinesis
            .request(
                "Kinesis_20131202.GetRecords",
                &json!({
                    "ShardIterator": iterator,
                })
                .to_string(),
            )
            .await
            .unwrap(),
    );
    assert_eq!(records.status, 200);

    let records_body: serde_json::Value = serde_json::from_str(&records.body).unwrap();
    let items = records_body["Records"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["Data"].as_str(), Some("aGVsbG8="));
    assert_eq!(items[0]["PartitionKey"].as_str(), Some("pk-1"));
}

async fn wait_until_stream_active(kinesis: &Kinesis, stream_name: &str) {
    for _ in 0..10 {
        yield_once().await;
        let response = decode(
            kinesis
                .request(
                    "Kinesis_20131202.DescribeStream",
                    &json!({ "StreamName": stream_name }).to_string(),
                )
                .await
                .unwrap(),
        );

        if response.status == 200 {
            let body: serde_json::Value = serde_json::from_str(&response.body).unwrap();
            if body["StreamDescription"]["StreamStatus"].as_str() == Some("ACTIVE") {
                return;
            }
        }
    }

    panic!("stream did not become ACTIVE");
}

async fn yield_once() {
    let promise = js_sys::Promise::resolve(&JsValue::UNDEFINED);
    wasm_bindgen_futures::JsFuture::from(promise).await.unwrap();
}

fn decode(value: JsValue) -> Response {
    serde_wasm_bindgen::from_value(value).unwrap()
}
