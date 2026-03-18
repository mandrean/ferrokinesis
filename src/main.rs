use axum::extract::DefaultBodyLimit;
use clap::Parser;
use ferrokinesis::config::load_config;
use ferrokinesis::store::StoreOptions;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "ferrokinesis")]
#[command(about = "A local AWS Kinesis mock server for testing")]
struct Args {
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

    /// Maximum request body size in megabytes (minimum: 1, maximum: 4096)
    #[arg(long, env = "FERROKINESIS_MAX_REQUEST_BODY_MB", value_parser = clap::value_parser!(u64).range(1..=4096))]
    max_request_body_mb: Option<u64>,
}

/// Resolve a value using precedence: CLI/env > config file > default.
fn resolve<T>(cli: Option<T>, file: Option<T>, default: T) -> T {
    cli.or(file).unwrap_or(default)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let file_cfg = args.config.as_deref().map(load_config).unwrap_or_default();

    let port = resolve(args.port, file_cfg.port, 4567);
    let create_stream_ms = resolve(args.create_stream_ms, file_cfg.create_stream_ms, 500);
    let delete_stream_ms = resolve(args.delete_stream_ms, file_cfg.delete_stream_ms, 500);
    let update_stream_ms = resolve(args.update_stream_ms, file_cfg.update_stream_ms, 500);
    let shard_limit = resolve(args.shard_limit, file_cfg.shard_limit, 10);
    let max_request_body_mb = resolve(args.max_request_body_mb, file_cfg.max_request_body_mb, 7);
    let aws_account_id = args.account_id.or(file_cfg.account_id);
    let aws_region = args.region.or(file_cfg.region);

    let options = StoreOptions {
        create_stream_ms,
        delete_stream_ms,
        update_stream_ms,
        shard_limit,
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

    axum::serve(listener, app).await.unwrap();
}
