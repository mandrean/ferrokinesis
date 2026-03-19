use axum::extract::DefaultBodyLimit;
use clap::{Args, Parser, Subcommand};
use ferrokinesis::config::load_config;
use ferrokinesis::store::StoreOptions;
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
    Serve(ServeArgs),

    /// Run a health check against a running server (for Docker HEALTHCHECK)
    HealthCheck(HealthCheckArgs),

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
fn resolve<T>(cli: Option<T>, file: Option<T>, default: T) -> T {
    cli.or(file).unwrap_or(default)
}

#[derive(Args, Debug)]
struct HealthCheckArgs {
    /// Port of the server to check
    #[arg(long, default_value_t = 4567)]
    port: u16,

    /// Path to probe
    #[arg(long, default_value = "/_health/ready")]
    path: String,

    /// Use TLS when connecting (accepts self-signed certificates)
    #[cfg(feature = "tls")]
    #[arg(long)]
    tls: bool,
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
        Some(Command::Serve(args)) => run_serve(args),
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
    let addr = format!("127.0.0.1:{}", args.port);

    let stream = match TcpStream::connect_timeout(
        &addr.parse().expect("invalid address"),
        Duration::from_secs(3),
    ) {
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

    #[cfg(feature = "tls")]
    if args.tls {
        return run_health_check_tls(stream, &args.path, &addr);
    }

    run_health_check_plain(stream, &args.path, &addr)
}

fn run_health_check_plain(stream: TcpStream, path: &str, addr: &str) -> ExitCode {
    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");

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
fn run_health_check_tls(stream: TcpStream, path: &str, addr: &str) -> ExitCode {
    use std::sync::Arc;

    // Build a rustls config that accepts any certificate (for local/self-signed testing)
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(InsecureCertVerifier))
        .with_no_client_auth();

    let server_name = rustls::pki_types::ServerName::try_from("localhost")
        .expect("invalid server name")
        .to_owned();
    let conn = rustls::ClientConnection::new(Arc::new(config), server_name)
        .expect("failed to create TLS connection");
    let mut tls_stream = rustls::StreamOwned::new(conn, stream);

    let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
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

#[cfg(feature = "tls")]
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
    let port = resolve(args.port, file_cfg.port, 4567);
    let max_request_body_mb = resolve(args.max_request_body_mb, file_cfg.max_request_body_mb, 7);

    let options = StoreOptions {
        create_stream_ms: resolve(
            args.create_stream_ms,
            file_cfg.create_stream_ms,
            defaults.create_stream_ms,
        ),
        delete_stream_ms: resolve(
            args.delete_stream_ms,
            file_cfg.delete_stream_ms,
            defaults.delete_stream_ms,
        ),
        update_stream_ms: resolve(
            args.update_stream_ms,
            file_cfg.update_stream_ms,
            defaults.update_stream_ms,
        ),
        shard_limit: resolve(args.shard_limit, file_cfg.shard_limit, defaults.shard_limit),
        iterator_ttl_seconds: resolve(
            args.iterator_ttl_seconds,
            file_cfg.iterator_ttl_seconds,
            defaults.iterator_ttl_seconds,
        ),
        retention_check_interval_secs: resolve(
            args.retention_check_interval_secs,
            file_cfg.retention_check_interval_secs,
            defaults.retention_check_interval_secs,
        ),
        aws_account_id: resolve(
            args.account_id,
            file_cfg.account_id,
            defaults.aws_account_id,
        ),
        aws_region: resolve(
            args.region.or(args.default_region),
            file_cfg.region,
            defaults.aws_region,
        ),
    };

    let max_bytes: usize = (max_request_body_mb * 1024 * 1024)
        .try_into()
        .expect("--max-request-body-mb value overflows usize");
    let (app, _store) = ferrokinesis::create_app(options);
    let app = app.layer(DefaultBodyLimit::max(max_bytes));

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
                        eprintln!("failed to load TLS cert/key: {e}");
                        return ExitCode::FAILURE;
                    }
                };

            println!("Listening at https://{addr}");

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
                            eprintln!("server error: {e}");
                            return ExitCode::FAILURE;
                        }
                        Err(e) => {
                            eprintln!("server task panicked: {e}");
                            return ExitCode::FAILURE;
                        }
                    }
                }
                _ = shutdown_signal() => {
                    eprintln!("shutting down gracefully...");
                    handle.graceful_shutdown(Some(Duration::from_secs(10)));
                    return ExitCode::SUCCESS;
                }
            }
        }
    }

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("Listening at http://{addr}");

    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("server error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
