use clap::Parser;
use ferrokinesis::store::StoreOptions;

#[derive(Parser, Debug)]
#[command(name = "ferrokinesis")]
#[command(about = "A local AWS Kinesis mock server for testing")]
struct Args {
    /// The port to listen on
    #[arg(long, default_value_t = 4567)]
    port: u16,

    /// Amount of time streams stay in CREATING state (ms)
    #[arg(long, default_value_t = 500)]
    create_stream_ms: u64,

    /// Amount of time streams stay in DELETING state (ms)
    #[arg(long, default_value_t = 500)]
    delete_stream_ms: u64,

    /// Amount of time streams stay in UPDATING state (ms)
    #[arg(long, default_value_t = 500)]
    update_stream_ms: u64,

    /// Shard limit for error reporting
    #[arg(long, default_value_t = 10)]
    shard_limit: u32,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let options = StoreOptions {
        create_stream_ms: args.create_stream_ms,
        delete_stream_ms: args.delete_stream_ms,
        update_stream_ms: args.update_stream_ms,
        shard_limit: args.shard_limit,
    };

    let (app, _store) = ferrokinesis::create_app(options);

    let addr = format!("0.0.0.0:{}", args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("Listening at http://{addr}");

    axum::serve(listener, app).await.unwrap();
}
