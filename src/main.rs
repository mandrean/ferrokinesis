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
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    match cli.command {
        Some(Command::HealthCheck(args)) => run_health_check(&args),
        // run_serve never returns (axum::serve runs forever or panics on error)
        Some(Command::Serve(args)) => run_serve(args),
        None => run_serve(cli.serve_args),
    }
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

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        args.path, addr
    );

    let mut writer = stream.try_clone().expect("failed to clone TcpStream");
    if let Err(e) = writer
        .write_all(request.as_bytes())
        .and_then(|()| writer.flush())
    {
        eprintln!("health check failed: write error: {e}");
        return ExitCode::FAILURE;
    }

    let reader = BufReader::new(stream);
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

    let port = resolve(args.port, file_cfg.port, 4567);
    let create_stream_ms = resolve(args.create_stream_ms, file_cfg.create_stream_ms, 500);
    let delete_stream_ms = resolve(args.delete_stream_ms, file_cfg.delete_stream_ms, 500);
    let update_stream_ms = resolve(args.update_stream_ms, file_cfg.update_stream_ms, 500);
    let shard_limit = resolve(args.shard_limit, file_cfg.shard_limit, 10);
    let iterator_ttl_seconds = resolve(
        args.iterator_ttl_seconds,
        file_cfg.iterator_ttl_seconds,
        300,
    );
    let max_request_body_mb = resolve(args.max_request_body_mb, file_cfg.max_request_body_mb, 7);
    let aws_account_id = resolve(args.account_id, file_cfg.account_id, "000000000000".into());
    let aws_region = resolve(
        args.region.or(args.default_region),
        file_cfg.region,
        "us-east-1".into(),
    );

    let options = StoreOptions {
        create_stream_ms,
        delete_stream_ms,
        update_stream_ms,
        shard_limit,
        iterator_ttl_seconds,
        aws_account_id,
        aws_region,
    };

    let max_bytes: usize = (max_request_body_mb * 1024 * 1024)
        .try_into()
        .expect("--max-request-body-mb value overflows usize");
    let (app, _store) = ferrokinesis::create_app(options);
    let app = app.layer(DefaultBodyLimit::max(max_bytes));

    let addr = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("Listening at http://{addr}");

    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("server error: {e}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
