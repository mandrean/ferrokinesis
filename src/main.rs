use axum::extract::DefaultBodyLimit;
use clap::{Args, Parser, Subcommand};
use ferrokinesis::config::{FileConfig, load_config};
use ferrokinesis::store::{
    DEFAULT_DURABLE_SNAPSHOT_INTERVAL_SECS, DurableStateOptions, StoreOptions,
    validate_durable_settings,
};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "ferrokinesis")]
#[command(about = "A local AWS Kinesis mock server for testing")]
#[command(args_conflicts_with_subcommands = true)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    serve_args: ServeArgs,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the mock Kinesis server (default when no subcommand is given)
    Serve(Box<ServeArgs>),

    /// Run a health check against a running server (for Docker HEALTHCHECK)
    HealthCheck(HealthCheckArgs),

    /// Replay captured PutRecord data against a running server
    #[cfg(feature = "replay")]
    Replay(ReplayArgs),

    /// Generate a self-signed TLS certificate and key
    #[cfg(feature = "tls")]
    GenerateCert(GenerateCertArgs),
}

#[derive(Args, Debug)]
struct ServeArgs {
    /// Path to a TOML configuration file
    #[arg(long, env = "FERROKINESIS_CONFIG")]
    config: Option<PathBuf>,

    /// The port to listen on
    #[arg(long, env = "FERROKINESIS_PORT")]
    port: Option<u16>,

    /// AWS account ID used in ARN generation (12-digit numeric)
    #[arg(long, env = "AWS_ACCOUNT_ID")]
    account_id: Option<String>,

    /// AWS region used in ARN generation and responses
    #[arg(long, env = "AWS_REGION")]
    region: Option<String>,

    /// AWS region fallback (used when --region and AWS_REGION are unset)
    #[arg(long, env = "AWS_DEFAULT_REGION", hide = true)]
    default_region: Option<String>,

    /// Amount of time streams stay in CREATING state (ms)
    #[arg(long, env = "FERROKINESIS_CREATE_STREAM_MS")]
    create_stream_ms: Option<u64>,

    /// Amount of time streams stay in DELETING state (ms)
    #[arg(long, env = "FERROKINESIS_DELETE_STREAM_MS")]
    delete_stream_ms: Option<u64>,

    /// Amount of time streams stay in UPDATING state (ms)
    #[arg(long, env = "FERROKINESIS_UPDATE_STREAM_MS")]
    update_stream_ms: Option<u64>,

    /// Shard limit for error reporting
    #[arg(long, env = "FERROKINESIS_SHARD_LIMIT")]
    shard_limit: Option<u32>,

    /// Shard iterator time-to-live in seconds (minimum: 1, maximum: 86400)
    #[arg(long, env = "FERROKINESIS_ITERATOR_TTL_SECONDS", value_parser = clap::value_parser!(u64).range(1..=86400))]
    iterator_ttl_seconds: Option<u64>,

    /// Maximum request body size in megabytes (minimum: 1, maximum: 4096)
    #[arg(long, env = "FERROKINESIS_MAX_REQUEST_BODY_MB", value_parser = clap::value_parser!(u64).range(1..=4096))]
    max_request_body_mb: Option<u64>,

    /// Retention reaper interval in seconds (0 = disabled, maximum: 86400)
    #[arg(long, env = "FERROKINESIS_RETENTION_CHECK_INTERVAL_SECS", value_parser = clap::value_parser!(u64).range(0..=86400))]
    retention_check_interval_secs: Option<u64>,

    /// Enable AWS-like shard write throughput throttling
    #[arg(long, env = "FERROKINESIS_ENFORCE_LIMITS",
          default_missing_value = "true", num_args = 0..=1)]
    enforce_limits: Option<bool>,
    /// Directory used to persist state with WAL + snapshots
    #[arg(long, env = "FERROKINESIS_STATE_DIR")]
    state_dir: Option<PathBuf>,

    /// Snapshot interval in seconds when durable mode is enabled (0 = disabled)
    #[arg(long, env = "FERROKINESIS_SNAPSHOT_INTERVAL_SECS", value_parser = clap::value_parser!(u64).range(0..=86400))]
    snapshot_interval_secs: Option<u64>,

    /// Hard cap on retained serialized record bytes
    #[arg(long, env = "FERROKINESIS_MAX_RETAINED_BYTES", value_parser = clap::value_parser!(u64).range(1..))]
    max_retained_bytes: Option<u64>,

    /// Log level (off, error, warn, info, debug, trace)
    #[arg(long, env = "FERROKINESIS_LOG_LEVEL",
          value_parser = ["off", "error", "warn", "info", "debug", "trace"])]
    log_level: Option<String>,

    /// Enable per-request access logging (controls tower-http traces independently of RUST_LOG)
    #[cfg(feature = "access-log")]
    #[arg(long, env = "FERROKINESIS_ACCESS_LOG",
          default_missing_value = "true", num_args = 0..=1)]
    access_log: Option<bool>,

    /// Path to write captured PutRecord/PutRecords data (NDJSON)
    #[arg(long, env = "FERROKINESIS_CAPTURE")]
    capture: Option<PathBuf>,

    /// Anonymize partition keys in capture output (requires --capture)
    #[arg(long, requires = "capture", env = "FERROKINESIS_SCRUB")]
    scrub: bool,

    /// Forward PutRecord/PutRecords to this Kinesis-compatible endpoint (async, best-effort)
    #[cfg(feature = "mirror")]
    #[arg(long, env = "FERROKINESIS_MIRROR_TO")]
    mirror_to: Option<String>,

    /// Log response divergences between local and mirror to stderr
    #[cfg(feature = "mirror")]
    #[arg(long, env = "FERROKINESIS_MIRROR_DIFF", requires = "mirror_to",
          default_missing_value = "true", num_args = 0..=1)]
    mirror_diff: Option<bool>,

    /// Maximum concurrent in-flight mirror requests (default: 64)
    #[cfg(feature = "mirror")]
    #[arg(long, env = "FERROKINESIS_MIRROR_CONCURRENCY", requires = "mirror_to",
          value_parser = clap::value_parser!(u64).range(1..))]
    mirror_concurrency: Option<u64>,

    /// Maximum retries for failed mirror requests (0 = no retries, default: 3)
    #[cfg(feature = "mirror")]
    #[arg(long, env = "FERROKINESIS_MIRROR_MAX_RETRIES", requires = "mirror_to")]
    mirror_max_retries: Option<usize>,

    /// Initial backoff delay between mirror retries in milliseconds (default: 100)
    #[cfg(feature = "mirror")]
    #[arg(long, env = "FERROKINESIS_MIRROR_INITIAL_BACKOFF_MS", requires = "mirror_to",
          value_parser = clap::value_parser!(u64).range(1..))]
    mirror_initial_backoff_ms: Option<u64>,

    /// Maximum backoff delay between mirror retries in milliseconds (default: 5000)
    #[cfg(feature = "mirror")]
    #[arg(long, env = "FERROKINESIS_MIRROR_MAX_BACKOFF_MS", requires = "mirror_to",
          value_parser = clap::value_parser!(u64).range(1..))]
    mirror_max_backoff_ms: Option<u64>,

    /// Path to TLS certificate PEM file (enables HTTPS)
    #[cfg(feature = "tls")]
    #[arg(long, env = "FERROKINESIS_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// Path to TLS private key PEM file (enables HTTPS)
    #[cfg(feature = "tls")]
    #[arg(long, env = "FERROKINESIS_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<PathBuf>,
}

/// Resolve a value using precedence: CLI/env > config file > default.
fn resolve<T>(cli: Option<T>, file: Option<T>, default: impl FnOnce() -> T) -> T {
    cli.or(file).unwrap_or_else(default)
}

fn resolve_store_options(
    args: &ServeArgs,
    file_cfg: &FileConfig,
    defaults: &StoreOptions,
) -> Result<StoreOptions, String> {
    let max_retained_bytes = args
        .max_retained_bytes
        .or(file_cfg.max_retained_bytes)
        .or(defaults.max_retained_bytes);
    let snapshot_interval_secs = resolve(
        args.snapshot_interval_secs,
        file_cfg.snapshot_interval_secs,
        || {
            defaults
                .durable
                .as_ref()
                .map_or(DEFAULT_DURABLE_SNAPSHOT_INTERVAL_SECS, |durable| {
                    durable.snapshot_interval_secs
                })
        },
    );
    validate_durable_settings(Some(snapshot_interval_secs), max_retained_bytes)
        .map_err(|err| err.to_string())?;
    let durable = args
        .state_dir
        .clone()
        .or(file_cfg.state_dir.clone())
        .or_else(|| {
            defaults
                .durable
                .as_ref()
                .map(|durable| durable.state_dir.clone())
        })
        .map(|state_dir| DurableStateOptions {
            state_dir,
            snapshot_interval_secs,
            max_retained_bytes,
        });

    Ok(StoreOptions {
        create_stream_ms: resolve(args.create_stream_ms, file_cfg.create_stream_ms, || {
            defaults.create_stream_ms
        }),
        delete_stream_ms: resolve(args.delete_stream_ms, file_cfg.delete_stream_ms, || {
            defaults.delete_stream_ms
        }),
        update_stream_ms: resolve(args.update_stream_ms, file_cfg.update_stream_ms, || {
            defaults.update_stream_ms
        }),
        shard_limit: resolve(args.shard_limit, file_cfg.shard_limit, || {
            defaults.shard_limit
        }),
        iterator_ttl_seconds: resolve(
            args.iterator_ttl_seconds,
            file_cfg.iterator_ttl_seconds,
            || defaults.iterator_ttl_seconds,
        ),
        retention_check_interval_secs: resolve(
            args.retention_check_interval_secs,
            file_cfg.retention_check_interval_secs,
            || defaults.retention_check_interval_secs,
        ),
        enforce_limits: resolve(args.enforce_limits, file_cfg.enforce_limits, || {
            defaults.enforce_limits
        }),
        durable,
        max_retained_bytes,
        aws_account_id: resolve(args.account_id.clone(), file_cfg.account_id.clone(), || {
            defaults.aws_account_id.clone()
        }),
        aws_region: resolve(
            args.region.clone().or(args.default_region.clone()),
            file_cfg.region.clone(),
            || defaults.aws_region.clone(),
        ),
    })
}

#[derive(Args, Debug)]
struct HealthCheckArgs {
    /// Host of the server to check
    #[arg(long, env = "FERROKINESIS_HEALTH_HOST", default_value = "127.0.0.1")]
    host: String,

    /// Port of the server to check
    #[arg(long, env = "FERROKINESIS_HEALTH_PORT", default_value_t = 4567)]
    port: u16,

    /// Path to probe
    #[arg(
        long,
        env = "FERROKINESIS_HEALTH_PATH",
        default_value = "/_health/ready"
    )]
    path: String,

    /// Use TLS when connecting (accepts self-signed certificates)
    #[cfg(feature = "tls")]
    #[arg(long, env = "FERROKINESIS_HEALTH_TLS")]
    tls: bool,
}

#[cfg(feature = "replay")]
#[derive(Args, Debug)]
struct ReplayArgs {
    /// Path to the NDJSON capture file
    #[arg(long)]
    file: PathBuf,

    /// Host of the target server
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port of the target server
    #[arg(long, default_value_t = 4567)]
    port: u16,

    /// Replay speed multiplier (e.g. "1x", "10x", "max")
    #[arg(long, default_value = "1x")]
    replay_speed: String,

    /// Use TLS (HTTPS) when connecting to the target server
    #[arg(long)]
    tls: bool,

    /// Skip TLS certificate verification (for self-signed certificates)
    #[arg(long, requires = "tls")]
    tls_insecure: bool,
}

#[cfg(feature = "replay")]
#[derive(Debug)]
enum ReplaySpeed {
    Max,
    Multiplier(f64),
}

#[cfg(feature = "tls")]
#[derive(Args, Debug)]
struct GenerateCertArgs {
    /// Output path for the certificate PEM file
    #[arg(long, default_value = "cert.pem")]
    cert_out: PathBuf,

    /// Output path for the private key PEM file
    #[arg(long, default_value = "key.pem")]
    key_out: PathBuf,

    /// Subject Alternative Names (hostnames/IPs the cert is valid for)
    #[arg(long, default_values_t = ["localhost".to_string(), "127.0.0.1".to_string(), "::1".to_string()])]
    san: Vec<String>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::HealthCheck(args)) => run_health_check(&args),
        Some(Command::Serve(args)) => run_serve(*args),
        #[cfg(feature = "replay")]
        Some(Command::Replay(args)) => run_replay(args),
        #[cfg(feature = "tls")]
        Some(Command::GenerateCert(args)) => run_generate_cert(&args),
        None => run_serve(cli.serve_args),
    }
}

#[cfg(feature = "tls")]
fn run_generate_cert(args: &GenerateCertArgs) -> ExitCode {
    let cert = match rcgen::generate_simple_self_signed(args.san.clone()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to generate certificate: {e}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = std::fs::write(&args.cert_out, cert.cert.pem()) {
        eprintln!("failed to write certificate: {e}");
        return ExitCode::FAILURE;
    }
    // Write the private key with restrictive permissions (0600) on Unix.
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let key_pem = cert.signing_key.serialize_pem();
        let mut file = match std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&args.key_out)
        {
            Ok(f) => f,
            Err(e) => {
                eprintln!("failed to write private key: {e}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(e) = file.write_all(key_pem.as_bytes()) {
            eprintln!("failed to write private key: {e}");
            return ExitCode::FAILURE;
        }
    }
    #[cfg(not(unix))]
    if let Err(e) = std::fs::write(&args.key_out, cert.signing_key.serialize_pem()) {
        eprintln!("failed to write private key: {e}");
        return ExitCode::FAILURE;
    }

    println!("Certificate written to {}", args.cert_out.display());
    println!("Private key written to {}", args.key_out.display());
    ExitCode::SUCCESS
}

fn run_health_check(args: &HealthCheckArgs) -> ExitCode {
    use std::net::ToSocketAddrs;

    let sock_addr = match (args.host.as_str(), args.port).to_socket_addrs() {
        Ok(mut addrs) => match addrs.next() {
            Some(a) => a,
            None => {
                eprintln!(
                    "health check failed: could not resolve host {:?}",
                    args.host
                );
                return ExitCode::FAILURE;
            }
        },
        Err(e) => {
            eprintln!(
                "health check failed: could not resolve host {:?}: {e}",
                args.host
            );
            return ExitCode::FAILURE;
        }
    };

    let stream = match TcpStream::connect_timeout(&sock_addr, Duration::from_secs(3)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("health check failed: connect error: {e}");
            return ExitCode::FAILURE;
        }
    };

    if let Err(e) = stream.set_read_timeout(Some(Duration::from_secs(3))) {
        eprintln!("health check failed: {e}");
        return ExitCode::FAILURE;
    }

    let host_header = format_host_header(&args.host, args.port);

    #[cfg(feature = "tls")]
    if args.tls {
        return run_health_check_tls(stream, &args.path, &host_header, &args.host);
    }

    run_health_check_plain(stream, &args.path, &host_header)
}

fn format_host_header(host: &str, port: u16) -> String {
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn run_health_check_plain(stream: TcpStream, path: &str, host_header: &str) -> ExitCode {
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: {host_header}\r\nConnection: close\r\n\r\n");

    let mut writer = stream.try_clone().expect("failed to clone TcpStream");
    if let Err(e) = writer
        .write_all(request.as_bytes())
        .and_then(|()| writer.flush())
    {
        eprintln!("health check failed: write error: {e}");
        return ExitCode::FAILURE;
    }

    parse_health_response(BufReader::new(stream))
}

#[cfg(feature = "tls")]
fn run_health_check_tls(stream: TcpStream, path: &str, host_header: &str, host: &str) -> ExitCode {
    use std::sync::Arc;

    // Build a rustls config that accepts any certificate (for local/self-signed testing)
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier))
        .with_no_client_auth();

    let server_name = match rustls::pki_types::ServerName::try_from(host.to_owned()) {
        Ok(name) => name,
        Err(e) => {
            eprintln!("health check failed: invalid server name {host:?}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let conn = rustls::ClientConnection::new(Arc::new(config), server_name)
        .expect("failed to create TLS connection");
    let mut tls_stream = rustls::StreamOwned::new(conn, stream);

    let request =
        format!("GET {path} HTTP/1.1\r\nHost: {host_header}\r\nConnection: close\r\n\r\n");
    if let Err(e) = tls_stream
        .write_all(request.as_bytes())
        .and_then(|()| tls_stream.flush())
    {
        eprintln!("health check failed: TLS write error: {e}");
        return ExitCode::FAILURE;
    }

    parse_health_response(BufReader::new(tls_stream))
}

/// A [`ServerCertVerifier`] that accepts any server certificate without validation.
///
/// This is intentionally insecure and is **only** used by the `health-check --tls`
/// CLI subcommand, which connects to the local ferrokinesis server that typically
/// presents a self-signed certificate. This verifier is never used in the server's
/// own TLS stack.
#[cfg(feature = "tls")]
#[derive(Debug)]
struct InsecureCertVerifier;

#[cfg(feature = "tls")]
impl rustls::client::danger::ServerCertVerifier for InsecureCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ED448,
        ]
    }
}

fn parse_health_response(reader: BufReader<impl std::io::Read>) -> ExitCode {
    let status_line = match reader.lines().next() {
        Some(Ok(line)) => line,
        Some(Err(e)) => {
            eprintln!("health check failed: read error: {e}");
            return ExitCode::FAILURE;
        }
        None => {
            eprintln!("health check failed: empty response");
            return ExitCode::FAILURE;
        }
    };

    // Parse "HTTP/1.1 200 OK" -> extract status code.
    // Defaults to 0 if the response isn't valid HTTP, which falls outside
    // 200..300 and correctly fails the health check.
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    if (200..300).contains(&status_code) {
        ExitCode::SUCCESS
    } else {
        eprintln!("health check failed: {status_line}");
        ExitCode::FAILURE
    }
}

#[cfg(feature = "replay")]
#[tokio::main]
async fn run_replay(args: ReplayArgs) -> ExitCode {
    use ferrokinesis::constants;

    let speed = match parse_replay_speed(&args.replay_speed) {
        Ok(s) => s,
        Err(()) => {
            eprintln!(
                "invalid replay speed {:?}: expected \"<N>x\" (e.g. \"1x\", \"10x\") or \"max\"",
                args.replay_speed
            );
            return ExitCode::FAILURE;
        }
    };

    let mut records = match ferrokinesis::capture::read_capture_file(&args.file) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to read capture file: {e}");
            return ExitCode::FAILURE;
        }
    };

    if records.is_empty() {
        println!("capture file is empty, nothing to replay");
        return ExitCode::SUCCESS;
    }

    records.sort_by_key(|r| r.ts);

    let scheme = if args.tls { "https" } else { "http" };
    let base_url = format!("{scheme}://{}:{}", args.host, args.port);
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(args.tls_insecure)
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .expect("failed to build HTTP client");
    let total = records.len();
    let start = std::time::Instant::now();

    // Replay always uses individual PutRecord calls regardless of the original
    // CaptureOp. Batching captured PutRecords back into PutRecords batches
    // could be a future optimization.
    for (i, record) in records.iter().enumerate() {
        // Sleep based on timestamp delta
        if let ReplaySpeed::Multiplier(multiplier) = speed
            && i > 0
        {
            let delta_ms = record.ts.saturating_sub(records[i - 1].ts);
            if delta_ms > 0 {
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                let sleep_ms = (delta_ms as f64 / multiplier) as u64;
                if sleep_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
            }
        }

        let mut body = serde_json::json!({
            constants::STREAM_NAME: record.stream,
            constants::DATA: record.data,
            constants::PARTITION_KEY: record.partition_key,
        });
        if let Some(ref ehk) = record.explicit_hash_key {
            body[constants::EXPLICIT_HASH_KEY] = serde_json::Value::String(ehk.clone());
        }

        let resp = client
            .post(&base_url)
            .header("Content-Type", constants::CONTENT_TYPE_JSON)
            .header("X-Amz-Target", format!("{}.PutRecord", constants::KINESIS_API))
            .header(
                "Authorization",
                "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234",
            )
            .header("X-Amz-Date", "20150101T000000Z")
            .json(&body)
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {}
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                eprintln!("record {}/{total}: HTTP {status}: {body}", i + 1);
            }
            Err(e) => {
                eprintln!("record {}/{total}: request failed: {e}", i + 1);
            }
        }
    }

    let elapsed = start.elapsed();
    println!("replayed {total} records in {:.2}s", elapsed.as_secs_f64());
    ExitCode::SUCCESS
}

/// Parse a replay speed string like "1x", "10x", "0.5x", or "max".
/// Returns `Ok(ReplaySpeed)` on success, `Err(())` on invalid input.
#[cfg(feature = "replay")]
fn parse_replay_speed(s: &str) -> Result<ReplaySpeed, ()> {
    if s == "max" {
        return Ok(ReplaySpeed::Max);
    }
    let Some(s) = s.strip_suffix('x') else {
        return Err(());
    };
    s.parse::<f64>()
        .ok()
        .filter(|v| *v > 0.0)
        .map(ReplaySpeed::Multiplier)
        .ok_or(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
    }
}

#[tokio::main]
async fn run_serve(args: ServeArgs) -> ExitCode {
    let file_cfg = args
        .config
        .as_deref()
        .map(load_config)
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("{e}");
            process::exit(1);
        })
        .unwrap_or_default();

    let defaults = StoreOptions::default();
    let options = resolve_store_options(&args, &file_cfg, &defaults).unwrap_or_else(|err| {
        eprintln!("{err}");
        process::exit(1);
    });
    let port = resolve(args.port, file_cfg.port, || 4567);
    let max_request_body_mb = resolve(args.max_request_body_mb, file_cfg.max_request_body_mb, || 7);
    let log_level: String = resolve(args.log_level, file_cfg.log_level, || "info".into());
    #[cfg(feature = "access-log")]
    let access_log = resolve(args.access_log, file_cfg.access_log, || false);

    // Initialize tracing subscriber.
    // RUST_LOG takes precedence when set; otherwise use the resolved log_level.
    #[cfg_attr(not(feature = "access-log"), allow(unused_mut))]
    let mut env_filter = if std::env::var("RUST_LOG").is_ok_and(|v| !v.is_empty()) {
        tracing_subscriber::EnvFilter::from_default_env()
    } else {
        tracing_subscriber::EnvFilter::new(&log_level)
    };
    // Always apply access-log directive, regardless of RUST_LOG.
    // Later add_directive calls override earlier ones for the same target.
    #[cfg(feature = "access-log")]
    {
        if access_log {
            env_filter = env_filter.add_directive("tower_http::trace=info".parse().unwrap());
        } else {
            env_filter = env_filter.add_directive("tower_http::trace=off".parse().unwrap());
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .init();

    let capture_path = args.capture.or(file_cfg.capture);
    let scrub = args.scrub || file_cfg.scrub.unwrap_or(false);
    let capture_writer = match capture_path {
        Some(ref path) => match ferrokinesis::capture::CaptureWriter::new(path, scrub) {
            Ok(w) => {
                tracing::info!(
                    path = %path.display(),
                    scrub,
                    "capture enabled",
                );
                Some(w)
            }
            Err(e) => {
                tracing::error!(path = %path.display(), "failed to open capture file: {e}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let max_bytes: usize = (max_request_body_mb * 1024 * 1024)
        .try_into()
        .expect("--max-request-body-mb value overflows usize");
    #[cfg(feature = "mirror")]
    let aws_region = options.aws_region.clone();

    let (app, _store) = ferrokinesis::create_app_with_capture(options, capture_writer);
    let app = app.layer(DefaultBodyLimit::max(max_bytes));
    #[cfg(feature = "mirror")]
    let app = {
        let mirror_cfg = file_cfg.mirror.unwrap_or_default();
        let mirror_to = args.mirror_to.or(mirror_cfg.to);
        let mirror_diff = resolve(args.mirror_diff, mirror_cfg.diff, || false);
        let mirror_concurrency = resolve(
            args.mirror_concurrency
                .map(|v| usize::try_from(v).expect("mirror-concurrency overflows usize")),
            mirror_cfg.concurrency,
            || ferrokinesis::mirror::Mirror::DEFAULT_CONCURRENCY,
        );
        let mirror_max_retries = resolve(args.mirror_max_retries, mirror_cfg.max_retries, || {
            ferrokinesis::mirror::RetryConfig::DEFAULT_MAX_RETRIES
        });
        let mirror_initial_backoff_ms = resolve(
            args.mirror_initial_backoff_ms,
            mirror_cfg.initial_backoff_ms,
            || ferrokinesis::mirror::RetryConfig::DEFAULT_INITIAL_BACKOFF_MS,
        );
        let mirror_max_backoff_ms = resolve(
            args.mirror_max_backoff_ms,
            mirror_cfg.max_backoff_ms,
            || ferrokinesis::mirror::RetryConfig::DEFAULT_MAX_BACKOFF_MS,
        );
        let retry_config = ferrokinesis::mirror::RetryConfig {
            max_retries: mirror_max_retries,
            initial_backoff: Duration::from_millis(mirror_initial_backoff_ms),
            max_backoff: Duration::from_millis(mirror_max_backoff_ms),
        };
        if let Some(endpoint) = mirror_to {
            let m = ferrokinesis::mirror::Mirror::new(
                &endpoint,
                mirror_diff,
                &aws_region,
                mirror_concurrency,
                retry_config,
            )
            .await;
            tracing::info!(
                endpoint = %endpoint,
                concurrency = mirror_concurrency,
                max_retries = mirror_max_retries,
                "mirroring PutRecord/PutRecords",
            );
            app.layer(axum::Extension(std::sync::Arc::new(m)))
        } else {
            app
        }
    };

    let addr = format!("0.0.0.0:{port}");

    #[cfg(feature = "tls")]
    {
        let tls_cert = args.tls_cert.or(file_cfg.tls_cert);
        let tls_key = args.tls_key.or(file_cfg.tls_key);

        if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
            let tls_config =
                match axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert, &key).await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("failed to load TLS cert/key: {e}");
                        return ExitCode::FAILURE;
                    }
                };

            tracing::info!("Listening at https://{addr}");

            let handle = axum_server::Handle::new();
            let server_handle = handle.clone();

            let server = tokio::spawn(async move {
                axum_server::bind_rustls(
                    addr.parse::<std::net::SocketAddr>()
                        .expect("constructed addr always parses"),
                    tls_config,
                )
                .handle(server_handle)
                .serve(app.into_make_service())
                .await
            });

            tokio::select! {
                result = server => {
                    match result {
                        Ok(Ok(())) => return ExitCode::SUCCESS,
                        Ok(Err(e)) => {
                            tracing::error!("server error: {e}");
                            return ExitCode::FAILURE;
                        }
                        Err(e) => {
                            tracing::error!("server task panicked: {e}");
                            return ExitCode::FAILURE;
                        }
                    }
                }
                _ = shutdown_signal() => {
                    tracing::info!("shutting down gracefully...");
                    handle.graceful_shutdown(Some(Duration::from_secs(10)));
                    return ExitCode::SUCCESS;
                }
            }
        }
    }

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    tracing::info!("Listening at http://{addr}");

    if let Err(e) = ferrokinesis::serve_plain_http(listener, app, shutdown_signal()).await {
        tracing::error!("server error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrokinesis::store::Store;

    fn serve_args() -> ServeArgs {
        ServeArgs {
            config: None,
            port: None,
            account_id: None,
            region: None,
            default_region: None,
            create_stream_ms: None,
            delete_stream_ms: None,
            update_stream_ms: None,
            shard_limit: None,
            iterator_ttl_seconds: None,
            max_request_body_mb: None,
            retention_check_interval_secs: None,
            enforce_limits: None,
            state_dir: None,
            snapshot_interval_secs: None,
            max_retained_bytes: None,
            log_level: None,
            #[cfg(feature = "access-log")]
            access_log: None,
            capture: None,
            scrub: false,
            #[cfg(feature = "mirror")]
            mirror_to: None,
            #[cfg(feature = "mirror")]
            mirror_diff: None,
            #[cfg(feature = "mirror")]
            mirror_concurrency: None,
            #[cfg(feature = "mirror")]
            mirror_max_retries: None,
            #[cfg(feature = "mirror")]
            mirror_initial_backoff_ms: None,
            #[cfg(feature = "mirror")]
            mirror_max_backoff_ms: None,
            #[cfg(feature = "tls")]
            tls_cert: None,
            #[cfg(feature = "tls")]
            tls_key: None,
        }
    }

    #[tokio::test]
    async fn resolve_store_options_from_config_enables_actual_throttling() {
        let args = serve_args();
        let file_cfg = FileConfig {
            enforce_limits: Some(true),
            ..Default::default()
        };
        let options = resolve_store_options(&args, &file_cfg, &StoreOptions::default()).unwrap();
        let store = Store::new(options);

        let first = store
            .try_reserve_shard_throughput("stream", "shardId-000000000000", 1_048_000, 1_000)
            .await;
        assert!(first.is_ok());

        let second = store
            .try_reserve_shard_throughput("stream", "shardId-000000000000", 1_000, 1_000)
            .await;
        assert!(second.is_err(), "resolved config should enable throttling");
    }

    #[tokio::test]
    async fn cli_args_override_config_for_enforce_limits() {
        let mut args = serve_args();
        args.enforce_limits = Some(false);
        let file_cfg = FileConfig {
            enforce_limits: Some(true),
            ..Default::default()
        };
        let options = resolve_store_options(&args, &file_cfg, &StoreOptions::default()).unwrap();
        let store = Store::new(options);

        let result = store
            .try_reserve_shard_throughput("stream", "shardId-000000000000", 2 * 1024 * 1024, 1_000)
            .await;
        assert!(result.is_ok(), "CLI flag should disable throttling");
    }

    #[test]
    fn resolve_store_options_maps_flat_durable_inputs_into_nested_settings() {
        let mut args = serve_args();
        args.state_dir = Some(PathBuf::from("/tmp/ferrokinesis-state"));
        args.snapshot_interval_secs = Some(17);
        args.max_retained_bytes = Some(2048);

        let options =
            resolve_store_options(&args, &FileConfig::default(), &StoreOptions::default()).unwrap();
        let durable = options.durable.expect("durable settings");
        assert_eq!(durable.state_dir, PathBuf::from("/tmp/ferrokinesis-state"));
        assert_eq!(durable.snapshot_interval_secs, 17);
        assert_eq!(durable.max_retained_bytes, Some(2048));
        assert_eq!(options.max_retained_bytes, Some(2048));
    }
}
