use std::collections::BTreeMap;

use axum::body::{Body, to_bytes};
use axum::extract::DefaultBodyLimit;
use axum::http::Request;
use ferrokinesis::constants;
use ferrokinesis::store::{Store, StoreOptions};
use serde::{Deserialize, Serialize};
use tower::util::ServiceExt;
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::wasm_bindgen;

const DEFAULT_MAX_REQUEST_BODY_MB: u64 = 7;
const AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234";
const X_AMZ_DATE: &str = "20150101T000000Z";

#[derive(Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct KinesisOptions {
    create_stream_ms: Option<u64>,
    delete_stream_ms: Option<u64>,
    update_stream_ms: Option<u64>,
    shard_limit: Option<u32>,
    iterator_ttl_seconds: Option<u64>,
    retention_check_interval_secs: Option<u64>,
    account_id: Option<String>,
    region: Option<String>,
    max_request_body_mb: Option<u64>,
}

#[derive(Serialize)]
struct KinesisResponse {
    status: u16,
    body: String,
    headers: BTreeMap<String, String>,
}

#[wasm_bindgen]
pub struct Kinesis {
    app: axum::Router,
    _store: Store,
}

#[wasm_bindgen]
impl Kinesis {
    #[wasm_bindgen(constructor)]
    pub fn new(options: Option<JsValue>) -> Result<Kinesis, JsValue> {
        console_error_panic_hook::set_once();

        let options = parse_options(options)?;
        let defaults = StoreOptions::default();
        let store_options = StoreOptions {
            create_stream_ms: options
                .create_stream_ms
                .unwrap_or(defaults.create_stream_ms),
            delete_stream_ms: options
                .delete_stream_ms
                .unwrap_or(defaults.delete_stream_ms),
            update_stream_ms: options
                .update_stream_ms
                .unwrap_or(defaults.update_stream_ms),
            shard_limit: options.shard_limit.unwrap_or(defaults.shard_limit),
            iterator_ttl_seconds: options
                .iterator_ttl_seconds
                .unwrap_or(defaults.iterator_ttl_seconds),
            retention_check_interval_secs: options
                .retention_check_interval_secs
                .unwrap_or(defaults.retention_check_interval_secs),
            aws_account_id: options.account_id.unwrap_or(defaults.aws_account_id),
            aws_region: options.region.unwrap_or(defaults.aws_region),
        };

        let max_bytes: usize = options
            .max_request_body_mb
            .unwrap_or(DEFAULT_MAX_REQUEST_BODY_MB)
            .saturating_mul(1024 * 1024)
            .try_into()
            .map_err(|_| js_error("maxRequestBodyMb overflows usize"))?;

        let store = Store::new(store_options);
        let app =
            ferrokinesis::create_router(store.clone()).layer(DefaultBodyLimit::max(max_bytes));

        Ok(Self { app, _store: store })
    }

    #[wasm_bindgen]
    pub async fn request(&self, target: &str, body: &str) -> Result<JsValue, JsValue> {
        let request = Request::builder()
            .method("POST")
            .uri("/")
            .header("Content-Type", constants::CONTENT_TYPE_JSON)
            .header("X-Amz-Target", target)
            .header("Authorization", AUTHORIZATION)
            .header("X-Amz-Date", X_AMZ_DATE)
            .body(Body::from(body.as_bytes().to_vec()))
            .map_err(|err| js_error(err.to_string()))?;

        let response = self
            .app
            .clone()
            .oneshot(request)
            .await
            .map_err(|err| js_error(err.to_string()))?;

        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    value.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .map_err(|err| js_error(err.to_string()))?;
        let body = String::from_utf8(body.to_vec()).map_err(|err| js_error(err.to_string()))?;

        serde_wasm_bindgen::to_value(&KinesisResponse {
            status,
            body,
            headers,
        })
        .map_err(|err| js_error(err.to_string()))
    }
}

fn parse_options(options: Option<JsValue>) -> Result<KinesisOptions, JsValue> {
    match options {
        Some(value) if !value.is_null() && !value.is_undefined() => {
            serde_wasm_bindgen::from_value(value).map_err(|err| js_error(err.to_string()))
        }
        _ => Ok(KinesisOptions::default()),
    }
}

fn js_error(message: impl Into<String>) -> JsValue {
    JsValue::from_str(&message.into())
}
