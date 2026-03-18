//! Goose load test for comparing Kinesis-compatible server throughput.
//!
//! Build and run:
//! ```sh
//! cargo run --bin loadtest --features loadtest --release -- \
//!     --host http://localhost:4567 --users 10 --run-time 30s
//! ```

use goose::goose::GooseResponse;
use goose::prelude::*;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Atomic counter for unique per-user stream names.
static USER_COUNTER: AtomicUsize = AtomicUsize::new(0);

const CONTENT_TYPE: &str = "application/x-amz-json-1.1";
const AUTH: &str = "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, \
                    SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234";
const AMZ_DATE: &str = "20150101T000000Z";

/// Pre-encoded base64 payload (~60 bytes decoded).
const RECORD_DATA: &str =
    "dGVzdCBkYXRhIGZvciBiZW5jaG1hcmtpbmcgdGVzdCBkYXRhIGZvciBiZW5jaG1hcmtpbmcgdGVzdCBkYXRh";

/// Per-user session state.
#[derive(Debug, Clone)]
struct Session {
    stream_name: String,
    shard_iterator: String,
}

/// Send a Kinesis API request tracked by goose metrics.
async fn kinesis(
    user: &mut GooseUser,
    action: &str,
    body: &Value,
) -> Result<GooseResponse, Box<TransactionError>> {
    let target = format!("Kinesis_20131202.{action}");
    let builder = user
        .get_request_builder(&GooseMethod::Post, "/")?
        .header("Content-Type", CONTENT_TYPE)
        .header("Authorization", AUTH)
        .header("X-Amz-Date", AMZ_DATE)
        .header("X-Amz-Target", target)
        .body(serde_json::to_vec(body).unwrap());

    let request = GooseRequest::builder()
        .set_request_builder(builder)
        .name(action)
        .build();

    user.request(request).await
}

// ── on_start ────────────────────────────────────────────────────────────────

/// Create a per-user stream and initialise the shard iterator.
async fn setup_stream(user: &mut GooseUser) -> TransactionResult {
    let id = USER_COUNTER.fetch_add(1, Ordering::Relaxed);
    let stream_name = format!("bench-stream-{id}");

    kinesis(
        user,
        "CreateStream",
        &json!({"StreamName": &stream_name, "ShardCount": 1}),
    )
    .await?;

    // Poll until the stream reaches ACTIVE (server default: 500 ms transition).
    let mut stream_active = false;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let goose = kinesis(user, "DescribeStream", &json!({"StreamName": &stream_name})).await?;
        if let Ok(resp) = goose.response
            && let Ok(body) = resp.json::<Value>().await
            && body["StreamDescription"]["StreamStatus"].as_str() == Some("ACTIVE")
        {
            stream_active = true;
            break;
        }
    }
    if !stream_active {
        eprintln!(
            "WARNING: stream {stream_name} did not reach ACTIVE within 5 s — \
             subsequent requests may fail"
        );
    }

    // Obtain an initial shard iterator.
    let goose = kinesis(
        user,
        "GetShardIterator",
        &json!({
            "StreamName": &stream_name,
            "ShardId": "shardId-000000000000",
            "ShardIteratorType": "TRIM_HORIZON",
        }),
    )
    .await?;

    let shard_iterator = match goose.response {
        Ok(resp) => resp
            .json::<Value>()
            .await
            .ok()
            .and_then(|v| v["ShardIterator"].as_str().map(String::from))
            .unwrap_or_default(),
        Err(_) => String::new(),
    };

    user.set_session_data(Session {
        stream_name,
        shard_iterator,
    });

    Ok(())
}

// ── Main-loop transactions ──────────────────────────────────────────────────

/// PutRecord with a random partition key (weight 10).
async fn put_record(user: &mut GooseUser) -> TransactionResult {
    let Some(session) = user.get_session_data::<Session>() else {
        return Ok(());
    };
    let stream_name = session.stream_name.clone();

    let pk = format!("pk-{}", rand::random::<u32>());

    kinesis(
        user,
        "PutRecord",
        &json!({
            "StreamName": stream_name,
            "Data": RECORD_DATA,
            "PartitionKey": pk,
        }),
    )
    .await?;

    Ok(())
}

/// GetRecords; refreshes the shard iterator from the response (weight 1).
async fn get_records(user: &mut GooseUser) -> TransactionResult {
    let session = user.get_session_data::<Session>().cloned();
    let Some(session) = session else {
        return Ok(());
    };
    if session.shard_iterator.is_empty() {
        return Ok(());
    }

    let goose = kinesis(
        user,
        "GetRecords",
        &json!({"ShardIterator": &session.shard_iterator}),
    )
    .await?;

    match goose.response {
        Ok(resp) => {
            if let Ok(body) = resp.json::<Value>().await
                && let Some(next) = body["NextShardIterator"].as_str()
                && let Some(s) = user.get_session_data_mut::<Session>()
            {
                s.shard_iterator = next.to_string();
            }
        }
        Err(e) => {
            // Iterator may have expired — re-fetch. Log in case it's something else.
            eprintln!("GetRecords failed (re-fetching iterator): {e}");
            let iter_resp = kinesis(
                user,
                "GetShardIterator",
                &json!({
                    "StreamName": &session.stream_name,
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            )
            .await?;

            if let Ok(resp) = iter_resp.response
                && let Ok(body) = resp.json::<Value>().await
                && let Some(it) = body["ShardIterator"].as_str()
                && let Some(s) = user.get_session_data_mut::<Session>()
            {
                s.shard_iterator = it.to_string();
            }
        }
    }

    Ok(())
}

// ── on_stop ─────────────────────────────────────────────────────────────────

/// Delete the per-user stream.
async fn delete_stream(user: &mut GooseUser) -> TransactionResult {
    let stream_name = user
        .get_session_data::<Session>()
        .map(|s| s.stream_name.clone())
        .unwrap_or_default();

    if !stream_name.is_empty() {
        let _ = kinesis(user, "DeleteStream", &json!({"StreamName": stream_name})).await;
    }

    Ok(())
}

// ── Entry point ─────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    GooseAttack::initialize()?
        .register_scenario(
            scenario!("KinesisWorkload")
                .register_transaction(transaction!(setup_stream).set_on_start())
                .register_transaction(transaction!(put_record).set_weight(10)?)
                .register_transaction(transaction!(get_records).set_weight(1)?)
                .register_transaction(transaction!(delete_stream).set_on_stop()),
        )
        .execute()
        .await?;

    Ok(())
}
