#![cfg(unix)]

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{StatusCode, Uri};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};
use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::process::{Child, Command};
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
struct CapturedRequest {
    path: String,
    body_len: usize,
}

struct OtlpCaptureServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl OtlpCaptureServer {
    async fn spawn() -> Self {
        async fn capture(
            State(requests): State<Arc<Mutex<Vec<CapturedRequest>>>>,
            uri: Uri,
            body: Bytes,
        ) -> (StatusCode, Json<Value>) {
            requests.lock().unwrap().push(CapturedRequest {
                path: uri.path().to_owned(),
                body_len: body.len(),
            });
            (StatusCode::OK, Json(json!({})))
        }

        let requests = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/", post(capture))
            .route("/{*path}", post(capture))
            .with_state(requests.clone());
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        Self {
            addr,
            requests,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        }
    }

    fn snapshot(&self) -> Vec<CapturedRequest> {
        self.requests.lock().unwrap().clone()
    }

    async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.unwrap();
        }
    }
}

fn allocate_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn_server(args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_ferrokinesis"))
        .args(args)
        .env_remove("RUST_LOG")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ferrokinesis")
}

async fn wait_for_ready(port: u16) {
    let client = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/_health/ready");
    for _ in 0..80 {
        if let Ok(response) = client.get(&url).send().await
            && response.status().is_success()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server on port {port} did not become ready");
}

async fn create_stream(port: u16) {
    let client = reqwest::Client::new();
    let response = client
        .post(format!("http://127.0.0.1:{port}/"))
        .header("Content-Type", "application/x-amz-json-1.1")
        .header("X-Amz-Target", "Kinesis_20131202.CreateStream")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
        )
        .header("X-Amz-Date", "20150101T000000Z")
        .json(&json!({"StreamName": "obs-test", "ShardCount": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

async fn terminate_server(child: Child) -> std::process::Output {
    let pid = child.id().expect("child pid");
    let status = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(status.success(), "failed to send SIGTERM to pid {pid}");

    tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .expect("wait for ferrokinesis shutdown")
        .expect("collect ferrokinesis output")
}

fn combined_output(output: &std::process::Output) -> String {
    let mut bytes = output.stdout.clone();
    bytes.extend_from_slice(&output.stderr);
    String::from_utf8_lossy(&bytes).into_owned()
}

fn parse_json_log_lines(output: &std::process::Output) -> Vec<Value> {
    combined_output(output)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).expect("valid json log line"))
        .collect()
}

#[tokio::test]
async fn otlp_http_base_endpoint_flushes_to_v1_traces_on_sigterm() {
    let capture = OtlpCaptureServer::spawn().await;
    let port = allocate_port();
    let endpoint = format!("http://{}", capture.addr);
    let child = spawn_server(&[
        "--port",
        &port.to_string(),
        "--otlp-endpoint",
        &endpoint,
        "--otlp-protocol",
        "http",
        "--log-level",
        "info",
    ]);

    wait_for_ready(port).await;
    create_stream(port).await;

    let output = terminate_server(child).await;
    assert!(
        output.status.success(),
        "server shutdown failed: {}",
        combined_output(&output)
    );

    let requests = capture.snapshot();
    capture.shutdown().await;

    assert!(
        !requests.is_empty(),
        "expected OTLP collector to receive at least one trace export"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.path == "/v1/traces" && request.body_len > 0),
        "expected OTLP HTTP export to POST to /v1/traces, got {requests:?}"
    );
}

#[tokio::test]
async fn json_request_logs_include_operation_and_request_id() {
    let port = allocate_port();
    let child = spawn_server(&[
        "--port",
        &port.to_string(),
        "--log-format",
        "json",
        "--log-level",
        "info",
    ]);

    wait_for_ready(port).await;
    create_stream(port).await;

    let output = terminate_server(child).await;
    assert!(
        output.status.success(),
        "server shutdown failed: {}",
        combined_output(&output)
    );

    let logs = parse_json_log_lines(&output);
    let request_log = logs
        .iter()
        .find(|line| {
            line.get("fields")
                .and_then(Value::as_object)
                .is_some_and(|fields| {
                    fields.get("message") == Some(&Value::String("request completed".into()))
                        && fields.get("operation") == Some(&Value::String("CreateStream".into()))
                        && fields
                            .get("request_id")
                            .and_then(Value::as_str)
                            .is_some_and(|request_id| !request_id.is_empty())
                })
        })
        .cloned();
    assert!(
        request_log.is_some(),
        "expected JSON logs to include operation and request_id, got {}",
        combined_output(&output)
    );
}

#[tokio::test]
async fn otlp_init_warning_does_not_claim_export_enabled() {
    let port = allocate_port();
    let child = spawn_server(&[
        "--port",
        &port.to_string(),
        "--log-format",
        "json",
        "--otlp-endpoint",
        "not-a-url",
        "--otlp-protocol",
        "http",
        "--log-level",
        "info",
    ]);

    wait_for_ready(port).await;

    let output = terminate_server(child).await;
    assert!(
        output.status.success(),
        "server shutdown failed: {}",
        combined_output(&output)
    );

    let stderr = combined_output(&output);
    assert!(
        stderr.contains("failed to initialize OTLP trace exporter"),
        "expected startup warning for OTLP init failure, got {stderr}"
    );
    assert!(
        !stderr.contains("OTLP trace export enabled"),
        "server should not claim OTLP export is enabled after init failure: {stderr}"
    );
}
