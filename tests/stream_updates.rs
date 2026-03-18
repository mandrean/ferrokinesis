mod common;

use common::*;
use ferrokinesis::store::{Store, StoreOptions};
use serde_json::{Value, json};

const ACCOUNT: &str = "0000-0000-0000";
const REGION: &str = "us-east-1";

fn stream_arn(name: &str) -> String {
    format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{name}")
}

// -- UpdateStreamMode --

#[tokio::test]
async fn update_stream_mode_to_on_demand() {
    let server = TestServer::new().await;
    let name = "usm-on-demand";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "UpdateStreamMode",
            &json!({
                "StreamARN": arn,
                "StreamModeDetails": { "StreamMode": "ON_DEMAND" },
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let body = server.describe_stream(name).await;
    assert_eq!(
        body["StreamDescription"]["StreamModeDetails"]["StreamMode"],
        "ON_DEMAND"
    );
}

#[tokio::test]
async fn update_stream_mode_back_to_provisioned() {
    let server = TestServer::new().await;
    let name = "usm-provisioned";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    server
        .request(
            "UpdateStreamMode",
            &json!({
                "StreamARN": arn,
                "StreamModeDetails": { "StreamMode": "ON_DEMAND" },
            }),
        )
        .await;

    let res = server
        .request(
            "UpdateStreamMode",
            &json!({
                "StreamARN": arn,
                "StreamModeDetails": { "StreamMode": "PROVISIONED" },
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    let body = server.describe_stream(name).await;
    assert_eq!(
        body["StreamDescription"]["StreamModeDetails"]["StreamMode"],
        "PROVISIONED"
    );
}

#[tokio::test]
async fn update_stream_mode_invalid_mode() {
    let server = TestServer::new().await;
    let name = "usm-invalid";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "UpdateStreamMode",
            &json!({
                "StreamARN": stream_arn(name),
                "StreamModeDetails": { "StreamMode": "INVALID" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

// -- UpdateStreamWarmThroughput --

#[tokio::test]
async fn update_warm_throughput_by_name() {
    let server = TestServer::new().await;
    let name = "uwt-by-name";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "UpdateStreamWarmThroughput",
            &json!({ "StreamName": name, "WarmThroughputMiBps": 100 }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["WarmThroughput"]["TargetMiBps"], 100);
    assert_eq!(body["StreamName"], name);
}

#[tokio::test]
async fn update_warm_throughput_by_arn() {
    let server = TestServer::new().await;
    let name = "uwt-by-arn";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "UpdateStreamWarmThroughput",
            &json!({ "StreamARN": stream_arn(name), "WarmThroughputMiBps": 50 }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["WarmThroughput"]["TargetMiBps"], 50);
}

#[tokio::test]
async fn update_warm_throughput_missing_identifier() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "UpdateStreamWarmThroughput",
            &json!({ "WarmThroughputMiBps": 100 }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn update_warm_throughput_missing_value() {
    let server = TestServer::new().await;
    let name = "uwt-no-value";
    server.create_stream(name, 1).await;

    let res = server
        .request("UpdateStreamWarmThroughput", &json!({ "StreamName": name }))
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn update_warm_throughput_returns_current_and_target() {
    let server = TestServer::new().await;
    let name = "uwt-current-target";
    server.create_stream(name, 1).await;

    // First update: current=0, target=10
    let body: Value = server
        .request(
            "UpdateStreamWarmThroughput",
            &json!({ "StreamName": name, "WarmThroughputMiBps": 10 }),
        )
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["WarmThroughput"]["CurrentMiBps"], 0);
    assert_eq!(body["WarmThroughput"]["TargetMiBps"], 10);

    // Second update: current=10, target=20
    let body: Value = server
        .request(
            "UpdateStreamWarmThroughput",
            &json!({ "StreamName": name, "WarmThroughputMiBps": 20 }),
        )
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(body["WarmThroughput"]["CurrentMiBps"], 10);
    assert_eq!(body["WarmThroughput"]["TargetMiBps"], 20);
}

// -- UpdateMaxRecordSize --

#[tokio::test]
async fn update_max_record_size_success() {
    let server = TestServer::new().await;
    let name = "umrs-success";
    server.create_stream(name, 1).await;
    let arn = stream_arn(name);

    let res = server
        .request(
            "UpdateMaxRecordSize",
            &json!({ "StreamARN": arn, "MaxRecordSizeInKiB": 4096 }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn update_max_record_size_min_boundary() {
    let server = TestServer::new().await;
    let name = "umrs-min";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "UpdateMaxRecordSize",
            &json!({ "StreamARN": stream_arn(name), "MaxRecordSizeInKiB": 1024 }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn update_max_record_size_max_boundary() {
    let server = TestServer::new().await;
    let name = "umrs-max";
    server.create_stream(name, 1).await;

    let res = server
        .request(
            "UpdateMaxRecordSize",
            &json!({ "StreamARN": stream_arn(name), "MaxRecordSizeInKiB": 10240 }),
        )
        .await;
    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn update_max_record_size_out_of_range() {
    let server = TestServer::new().await;
    let name = "umrs-oor";
    server.create_stream(name, 1).await;

    for invalid in [0i64, 512, 1023, 10241, 65536] {
        let res = server
            .request(
                "UpdateMaxRecordSize",
                &json!({ "StreamARN": stream_arn(name), "MaxRecordSizeInKiB": invalid }),
            )
            .await;
        assert_eq!(
            res.status(),
            400,
            "Expected 400 for MaxRecordSizeInKiB={invalid}"
        );
        let body: Value = res.json().await.unwrap();
        assert_eq!(body["__type"], "ValidationException");
    }
}

#[tokio::test]
async fn update_max_record_size_missing_arn() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "UpdateMaxRecordSize",
            &json!({ "MaxRecordSizeInKiB": 1024 }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ValidationException");
}

#[tokio::test]
async fn update_max_record_size_unknown_arn() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "UpdateMaxRecordSize",
            &json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:0000-0000-0000:stream/nonexistent",
                "MaxRecordSizeInKiB": 1024,
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

// -- UpdateAccountSettings --

#[tokio::test]
async fn update_account_settings_success() {
    let server = TestServer::new().await;

    let res = server
        .request(
            "UpdateAccountSettings",
            &json!({
                "MinimumThroughputBillingCommitment": {
                    "Status": "ENABLED",
                    "MinimumWriteCapacityUnits": 100,
                    "MinimumReadCapacityUnits": 200,
                }
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    let commitment = &body["MinimumThroughputBillingCommitment"];
    assert_eq!(commitment["Status"], "ENABLED");
    assert_eq!(commitment["MinimumWriteCapacityUnits"], 100);
    assert_eq!(commitment["MinimumReadCapacityUnits"], 200);
}

#[tokio::test]
async fn update_account_settings_missing_commitment() {
    let server = TestServer::new().await;

    let res = server.request("UpdateAccountSettings", &json!({})).await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn update_account_settings_missing_status() {
    let server = TestServer::new().await;

    let res = server
        .request(
            "UpdateAccountSettings",
            &json!({ "MinimumThroughputBillingCommitment": {} }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn update_account_settings_persisted_across_describe() {
    let server = TestServer::new().await;

    server
        .request(
            "UpdateAccountSettings",
            &json!({
                "MinimumThroughputBillingCommitment": { "Status": "ACTIVE" }
            }),
        )
        .await;

    let res = server.request("DescribeAccountSettings", &json!({})).await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(
        body["MinimumThroughputBillingCommitment"]["Status"],
        "ACTIVE"
    );
}

// -- UpdateShardCount --

#[tokio::test]
async fn update_shard_count_scale_up() {
    let server = TestServer::new().await;
    let name = "usc-scale-up";
    server.create_stream(name, 2).await;

    let res = server
        .request(
            "UpdateShardCount",
            &json!({
                "StreamName": name,
                "TargetShardCount": 4,
                "ScalingType": "UNIFORM_SCALING",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["CurrentShardCount"], 2);
    assert_eq!(body["TargetShardCount"], 4);

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();
    let open_count = shards
        .iter()
        .filter(|s| s["SequenceNumberRange"]["EndingSequenceNumber"].is_null())
        .count();
    assert_eq!(open_count, 4);
}

#[tokio::test]
async fn update_shard_count_scale_down() {
    let server = TestServer::new().await;
    let name = "usc-scale-down";
    server.create_stream(name, 4).await;

    let res = server
        .request(
            "UpdateShardCount",
            &json!({
                "StreamName": name,
                "TargetShardCount": 2,
                "ScalingType": "UNIFORM_SCALING",
            }),
        )
        .await;
    assert_eq!(res.status(), 200);

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let desc = server.describe_stream(name).await;
    let shards = desc["StreamDescription"]["Shards"].as_array().unwrap();
    let open_count = shards
        .iter()
        .filter(|s| s["SequenceNumberRange"]["EndingSequenceNumber"].is_null())
        .count();
    assert_eq!(open_count, 2);
}

#[tokio::test]
async fn update_shard_count_same_count_is_error() {
    let server = TestServer::new().await;
    let name = "usc-same";
    server.create_stream(name, 2).await;

    let res = server
        .request(
            "UpdateShardCount",
            &json!({
                "StreamName": name,
                "TargetShardCount": 2,
                "ScalingType": "UNIFORM_SCALING",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "InvalidArgumentException");
}

#[tokio::test]
async fn update_shard_count_stream_not_found() {
    let server = TestServer::new().await;

    let res = server
        .request(
            "UpdateShardCount",
            &json!({
                "StreamName": "nonexistent",
                "TargetShardCount": 2,
                "ScalingType": "UNIFORM_SCALING",
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn update_warm_throughput_arn_without_slash() {
    let server = TestServer::new().await;

    let res = server
        .request(
            "UpdateStreamWarmThroughput",
            &json!({ "StreamARN": "arn-without-slash", "WarmThroughputMiBps": 50 }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn update_stream_mode_invalid_mode_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::update_stream_mode::execute(
        &store,
        json!({
            "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
            "StreamModeDetails": { "StreamMode": "UNKNOWN_MODE" },
        }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "InvalidArgumentException");
}

#[tokio::test]
async fn update_stream_mode_stream_not_found() {
    let server = TestServer::new().await;
    let arn = stream_arn("test-usm-notfound");

    let res = server
        .request(
            "UpdateStreamMode",
            &json!({
                "StreamARN": arn,
                "StreamModeDetails": { "StreamMode": "ON_DEMAND" },
            }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}

#[tokio::test]
async fn update_max_record_size_empty_arn_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::update_max_record_size::execute(
        &store,
        json!({ "StreamARN": "", "MaxRecordSizeInKiB": 4096 }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "InvalidArgumentException");
}

#[tokio::test]
async fn update_max_record_size_kib_not_integer_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::update_max_record_size::execute(
        &store,
        json!({
            "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
            "MaxRecordSizeInKiB": "not-an-integer",
        }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "InvalidArgumentException");
}

#[tokio::test]
async fn update_max_record_size_kib_below_range_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::update_max_record_size::execute(
        &store,
        json!({
            "StreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/test",
            "MaxRecordSizeInKiB": 512,
        }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.body.__type, "InvalidArgumentException");
    assert!(
        err.body
            .message
            .as_deref()
            .unwrap_or("")
            .contains("between 1024 and 10240")
    );
}

#[tokio::test]
async fn update_warm_throughput_both_empty_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::update_stream_warm_throughput::execute(
        &store,
        json!({ "WarmThroughputMiBps": 50 }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "InvalidArgumentException");
}

#[tokio::test]
async fn update_warm_throughput_mibps_not_integer_direct() {
    let store = Store::new(StoreOptions::default());
    let result = ferrokinesis::actions::update_stream_warm_throughput::execute(
        &store,
        json!({
            "StreamName": "test",
            "WarmThroughputMiBps": "not-an-integer",
        }),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().body.__type, "InvalidArgumentException");
}

#[tokio::test]
async fn update_warm_throughput_by_arn_on_creating_stream() {
    let server = TestServer::with_options(StoreOptions {
        create_stream_ms: 500,
        delete_stream_ms: 0,
        update_stream_ms: 0,
        shard_limit: 50,
        ..Default::default()
    })
    .await;
    let name = "cx-uwt-arn-creat";
    let arn = stream_arn(name);

    server
        .request(
            "CreateStream",
            &json!({"StreamName": name, "ShardCount": 1}),
        )
        .await;

    let res = server
        .request(
            "UpdateStreamWarmThroughput",
            &json!({ "StreamARN": arn, "WarmThroughputMiBps": 50 }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceInUseException");
}

#[tokio::test]
async fn update_max_record_size_arn_without_slash() {
    let server = TestServer::new().await;
    let res = server
        .request(
            "UpdateMaxRecordSize",
            &json!({ "StreamARN": "arn-without-any-slash-chars", "MaxRecordSizeInKiB": 4096 }),
        )
        .await;
    assert_eq!(res.status(), 400);
    let body: Value = res.json().await.unwrap();
    assert_eq!(body["__type"], "ResourceNotFoundException");
}
