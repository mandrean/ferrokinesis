mod common;

use common::TestServer;
use tokio::process::Command;

#[tokio::test]
async fn health_check_default_host() {
    let server = TestServer::new().await;
    let port = server.addr.port();

    let output = Command::new(env!("CARGO_BIN_EXE_ferrokinesis"))
        .args(["health-check", "--port", &port.to_string()])
        .output()
        .await
        .expect("failed to run ferrokinesis");

    assert!(
        output.status.success(),
        "health-check with default host failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn health_check_explicit_ipv4_host() {
    let server = TestServer::new().await;
    let port = server.addr.port();

    let output = Command::new(env!("CARGO_BIN_EXE_ferrokinesis"))
        .args([
            "health-check",
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ])
        .output()
        .await
        .expect("failed to run ferrokinesis");

    assert!(
        output.status.success(),
        "health-check with --host 127.0.0.1 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn health_check_localhost_hostname() {
    let server = TestServer::new().await;
    let port = server.addr.port();

    let output = Command::new(env!("CARGO_BIN_EXE_ferrokinesis"))
        .args([
            "health-check",
            "--host",
            "localhost",
            "--port",
            &port.to_string(),
        ])
        .output()
        .await
        .expect("failed to run ferrokinesis");

    // localhost may resolve to ::1 (IPv6) on some systems while the test server
    // only listens on 127.0.0.1 (IPv4), so we just verify it doesn't panic.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("panicked"),
        "health-check with --host localhost should not panic, stderr: {stderr}"
    );
}

#[tokio::test]
async fn health_check_invalid_host_no_panic() {
    let output = Command::new(env!("CARGO_BIN_EXE_ferrokinesis"))
        .args([
            "health-check",
            "--host",
            "not a valid host!!",
            "--port",
            "1",
        ])
        .output()
        .await
        .expect("failed to run ferrokinesis");

    assert!(
        !output.status.success(),
        "health-check with invalid host should fail"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("panicked"),
        "health-check with invalid host should not panic, stderr: {stderr}"
    );
    assert!(
        stderr.contains("health check failed"),
        "expected graceful error message, stderr: {stderr}"
    );
}

#[tokio::test]
async fn health_check_connection_refused() {
    // Use a port that is almost certainly not listening
    let output = Command::new(env!("CARGO_BIN_EXE_ferrokinesis"))
        .args(["health-check", "--port", "1"])
        .output()
        .await
        .expect("failed to run ferrokinesis");

    assert!(
        !output.status.success(),
        "health-check against unreachable port should fail"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("panicked"),
        "should not panic on connection refused, stderr: {stderr}"
    );
    assert!(
        stderr.contains("health check failed"),
        "expected graceful error message, stderr: {stderr}"
    );
}
