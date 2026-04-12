mod client;

use aws_smithy_eventstream::frame::read_message_from;
use aws_smithy_types::event_stream::{HeaderValue, Message};
use base64::Engine;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use clap::{ArgAction, ArgGroup, Args, Parser, Subcommand, ValueEnum};
use client::{ApiClient, Error, Result};
use ferrokinesis_core::capture::read_capture_file;
use ferrokinesis_core::constants;
use ferrokinesis_core::operation::Operation;
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(name = "ferro")]
#[command(about = "Companion CLI for ferrokinesis")]
struct Cli {
    /// ferrokinesis endpoint
    #[arg(
        long,
        global = true,
        env = "FERROKINESIS_ENDPOINT",
        default_value = "http://127.0.0.1:4567"
    )]
    endpoint: String,

    /// Skip TLS certificate validation
    #[arg(long, global = true)]
    insecure: bool,

    /// Print machine-readable JSON
    #[arg(long, global = true, conflicts_with = "ndjson")]
    json: bool,

    /// Print newline-delimited JSON when the command emits multiple records
    #[arg(long, global = true, conflicts_with = "json")]
    ndjson: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Streams(StreamsArgs),
    Shards(ShardsArgs),
    Consumers(ConsumersArgs),
    Put(PutArgs),
    Tail(TailArgs),
    Replay(ReplayArgs),
    Api(ApiArgs),
    Health,
}

#[derive(Args, Debug)]
struct StreamsArgs {
    #[command(subcommand)]
    command: StreamsCommand,
}

#[derive(Subcommand, Debug)]
enum StreamsCommand {
    List(StreamsListArgs),
    Create(StreamsCreateArgs),
    Describe(StreamsDescribeArgs),
    Delete(StreamsDeleteArgs),
}

#[derive(Args, Debug)]
struct StreamsListArgs {
    #[arg(long)]
    limit: Option<usize>,
}

#[derive(Args, Debug)]
struct StreamsCreateArgs {
    stream: String,

    #[arg(long, default_value_t = 1)]
    shards: u32,

    #[arg(long)]
    wait: bool,

    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    timeout: Duration,
}

#[derive(Args, Debug)]
struct StreamsDescribeArgs {
    stream: String,
}

#[derive(Args, Debug)]
struct StreamsDeleteArgs {
    stream: String,

    #[arg(long)]
    wait: bool,

    #[arg(long, value_parser = parse_duration, default_value = "30s")]
    timeout: Duration,
}

#[derive(Args, Debug)]
struct ShardsArgs {
    #[command(subcommand)]
    command: ShardsCommand,
}

#[derive(Subcommand, Debug)]
enum ShardsCommand {
    List(ShardsListArgs),
}

#[derive(Args, Debug)]
struct ShardsListArgs {
    stream: String,

    #[arg(long)]
    limit: Option<usize>,
}

#[derive(Args, Debug)]
struct ConsumersArgs {
    #[command(subcommand)]
    command: ConsumersCommand,
}

#[derive(Subcommand, Debug)]
enum ConsumersCommand {
    List(ConsumersListArgs),
    Register(ConsumerRegisterArgs),
    Describe(ConsumerDescribeArgs),
    Deregister(ConsumerDeregisterArgs),
}

#[derive(Args, Debug)]
struct ConsumersListArgs {
    stream: String,
}

#[derive(Args, Debug)]
struct ConsumerRegisterArgs {
    stream: String,
    consumer: String,
}

#[derive(Args, Debug)]
struct ConsumerDescribeArgs {
    stream: String,
    consumer: String,
}

#[derive(Args, Debug)]
struct ConsumerDeregisterArgs {
    stream: String,
    consumer: String,
}

#[derive(Args, Debug)]
#[command(group(
    ArgGroup::new("input")
        .required(true)
        .args(["data", "file", "stdin"])
))]
struct PutArgs {
    stream: String,

    data: Option<String>,

    #[arg(long)]
    partition_key: String,

    #[arg(long)]
    file: Option<PathBuf>,

    #[arg(long)]
    stdin: bool,

    #[arg(long)]
    base64: bool,

    #[arg(long)]
    explicit_hash_key: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum TailFrom {
    Latest,
    TrimHorizon,
    AtSequence,
    AfterSequence,
    AtTimestamp,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DecodeMode {
    Auto,
    Utf8,
    Base64,
    Hex,
    None,
}

#[derive(Args, Debug)]
struct TailArgs {
    stream: String,

    #[arg(long, value_enum, default_value_t = TailFrom::Latest)]
    from: TailFrom,

    #[arg(long)]
    sequence_number: Option<String>,

    #[arg(long)]
    timestamp: Option<String>,

    #[arg(long = "shard")]
    shard_ids: Vec<String>,

    #[arg(long)]
    limit: Option<NonZeroUsize>,

    #[arg(long, value_parser = parse_duration, default_value = "500ms")]
    poll_interval: Duration,

    #[arg(long, value_enum, default_value_t = DecodeMode::Auto)]
    decode: DecodeMode,

    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "no_follow")]
    follow: bool,

    #[arg(long = "no-follow", action = ArgAction::SetTrue, conflicts_with = "follow")]
    no_follow: bool,

    #[arg(long)]
    consumer: Option<String>,
}

#[derive(Args, Debug)]
struct ReplayArgs {
    file: PathBuf,

    #[arg(long)]
    stream: Option<String>,

    #[arg(long, default_value = "1x")]
    speed: String,

    #[arg(long)]
    limit: Option<usize>,

    #[arg(long)]
    fail_fast: bool,
}

#[derive(Args, Debug)]
struct ApiArgs {
    #[command(subcommand)]
    command: ApiCommand,
}

#[derive(Subcommand, Debug)]
enum ApiCommand {
    Call(ApiCallArgs),
}

#[derive(Args, Debug)]
#[command(group(
    ArgGroup::new("body_input")
        .required(false)
        .args(["body", "body_file", "stdin"])
))]
struct ApiCallArgs {
    operation: String,

    #[arg(long)]
    body: Option<String>,

    #[arg(long = "body-file")]
    body_file: Option<PathBuf>,

    #[arg(long)]
    stdin: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputMode {
    Human,
    Json,
    Ndjson,
}

#[derive(Clone, Debug)]
struct TailRecord {
    stream: String,
    shard_id: String,
    partition_key: String,
    sequence_number: String,
    approximate_arrival_timestamp: Option<f64>,
    data: String,
}

#[derive(Debug)]
enum TailEvent {
    Record(TailRecord),
    Error(String),
}

#[derive(Clone, Debug)]
struct IteratorSpec {
    from: TailFrom,
    sequence_number: Option<String>,
    timestamp_seconds: Option<f64>,
    latest_fallback_timestamp: f64,
}

#[derive(Clone, Debug)]
struct PollShardState {
    shard_id: String,
    iterator: String,
    last_sequence_number: Option<String>,
}

#[derive(Clone, Debug)]
struct TailConfig {
    output_mode: OutputMode,
    stream: String,
    shard_ids: Vec<String>,
    iterator_spec: IteratorSpec,
    follow: bool,
    limit: Option<usize>,
    decode: DecodeMode,
}

#[derive(Clone, Debug)]
struct PollingTailConfig {
    tail: TailConfig,
    poll_interval: Duration,
}

#[derive(Clone, Debug)]
struct EfoTailConfig {
    tail: TailConfig,
    consumer_arn: String,
}

#[derive(Debug)]
struct EfoSessionResult {
    continuation_sequence_number: Option<String>,
    terminal_end_of_shard: bool,
}

#[derive(Clone, Debug)]
enum ReplaySpeed {
    Max,
    Multiplier(f64),
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let output_mode = if cli.json {
        OutputMode::Json
    } else if cli.ndjson {
        OutputMode::Ndjson
    } else {
        OutputMode::Human
    };
    let client = ApiClient::new(&cli.endpoint, cli.insecure)?;

    match cli.command {
        Command::Streams(args) => run_streams(&client, output_mode, args).await,
        Command::Shards(args) => run_shards(&client, output_mode, args).await,
        Command::Consumers(args) => run_consumers(&client, output_mode, args).await,
        Command::Put(args) => run_put(&client, output_mode, args).await,
        Command::Tail(args) => run_tail(&client, output_mode, args).await,
        Command::Replay(args) => run_replay(&client, output_mode, args).await,
        Command::Api(args) => run_api(&client, output_mode, args).await,
        Command::Health => run_health(&client, output_mode).await,
    }
}

async fn run_streams(client: &ApiClient, output_mode: OutputMode, args: StreamsArgs) -> Result<()> {
    match args.command {
        StreamsCommand::List(args) => {
            let mut streams = list_all_streams(client).await?;
            if let Some(limit) = args.limit {
                streams.truncate(limit);
            }

            if output_mode == OutputMode::Human {
                for stream in streams {
                    println!("{stream}");
                }
            } else if output_mode == OutputMode::Ndjson {
                for stream in streams {
                    println!(
                        "{}",
                        serde_json::to_string(&json!({ "StreamName": stream }))?
                    );
                }
            } else {
                println!(
                    "{}",
                    serde_json::to_string(&json!({ "StreamNames": streams }))?
                );
            }
            Ok(())
        }
        StreamsCommand::Create(args) => {
            client
                .call(
                    Operation::CreateStream,
                    &json!({
                        constants::STREAM_NAME: args.stream,
                        constants::SHARD_COUNT: args.shards,
                    }),
                )
                .await?;
            if args.wait {
                wait_for_stream_active(client, &args.stream, args.timeout).await?;
            }

            if output_mode == OutputMode::Human {
                if args.wait {
                    println!("created {}", args.stream);
                } else {
                    println!("create requested for {}", args.stream);
                }
            } else {
                emit_single_value(
                    output_mode,
                    &json!({
                        "StreamName": args.stream,
                        "ShardCount": args.shards,
                        "Waited": args.wait,
                    }),
                )?;
            }
            Ok(())
        }
        StreamsCommand::Describe(args) => {
            let summary = stream_summary(client, &args.stream).await?;
            if output_mode == OutputMode::Human {
                println!(
                    "StreamName: {}",
                    summary["StreamName"].as_str().unwrap_or(&args.stream)
                );
                println!("StreamARN: {}", summary["StreamARN"].as_str().unwrap_or(""));
                println!("Status: {}", summary["StreamStatus"].as_str().unwrap_or(""));
                println!(
                    "Mode: {}",
                    summary["StreamModeDetails"]["StreamMode"]
                        .as_str()
                        .unwrap_or("")
                );
                println!(
                    "RetentionHours: {}",
                    summary["RetentionPeriodHours"].as_i64().unwrap_or_default()
                );
                println!(
                    "Encryption: {}",
                    summary["EncryptionType"].as_str().unwrap_or("")
                );
                println!(
                    "OpenShardCount: {}",
                    summary["OpenShardCount"].as_i64().unwrap_or_default()
                );
                println!(
                    "ConsumerCount: {}",
                    summary["ConsumerCount"].as_i64().unwrap_or_default()
                );
            } else {
                emit_single_value(output_mode, &summary)?;
            }
            Ok(())
        }
        StreamsCommand::Delete(args) => {
            client
                .call(
                    Operation::DeleteStream,
                    &json!({
                        constants::STREAM_NAME: args.stream,
                    }),
                )
                .await?;
            if args.wait {
                wait_for_stream_deleted(client, &args.stream, args.timeout).await?;
            }

            if output_mode == OutputMode::Human {
                if args.wait {
                    println!("deleted {}", args.stream);
                } else {
                    println!("delete requested for {}", args.stream);
                }
            } else {
                emit_single_value(
                    output_mode,
                    &json!({
                        "StreamName": args.stream,
                        "Waited": args.wait,
                    }),
                )?;
            }
            Ok(())
        }
    }
}

async fn run_shards(client: &ApiClient, output_mode: OutputMode, args: ShardsArgs) -> Result<()> {
    match args.command {
        ShardsCommand::List(args) => {
            let mut shards = list_all_shards(client, &args.stream).await?;
            if let Some(limit) = args.limit {
                shards.truncate(limit);
            }

            if output_mode == OutputMode::Human {
                for shard in &shards {
                    let shard_id = shard["ShardId"].as_str().unwrap_or("");
                    let start = shard["SequenceNumberRange"]["StartingSequenceNumber"]
                        .as_str()
                        .unwrap_or("");
                    let end = shard["SequenceNumberRange"]["EndingSequenceNumber"]
                        .as_str()
                        .unwrap_or("OPEN");
                    println!("{shard_id}\t{start}\t{end}");
                }
            } else if output_mode == OutputMode::Ndjson {
                emit_ndjson_values(shards.iter())?;
            } else {
                emit_single_value(output_mode, &json!({ "Shards": shards }))?;
            }
            Ok(())
        }
    }
}

async fn run_consumers(
    client: &ApiClient,
    output_mode: OutputMode,
    args: ConsumersArgs,
) -> Result<()> {
    match args.command {
        ConsumersCommand::List(args) => {
            let stream_arn = stream_arn(client, &args.stream).await?;
            let consumers = list_consumers(client, &stream_arn).await?;

            if output_mode == OutputMode::Human {
                for consumer in &consumers {
                    println!(
                        "{}\t{}\t{}",
                        consumer["ConsumerName"].as_str().unwrap_or(""),
                        consumer["ConsumerStatus"].as_str().unwrap_or(""),
                        consumer["ConsumerARN"].as_str().unwrap_or("")
                    );
                }
            } else if output_mode == OutputMode::Ndjson {
                emit_ndjson_values(consumers.iter())?;
            } else {
                emit_single_value(output_mode, &json!({ "Consumers": consumers }))?;
            }
            Ok(())
        }
        ConsumersCommand::Register(args) => {
            let stream_arn = stream_arn(client, &args.stream).await?;
            let value = client
                .call(
                    Operation::RegisterStreamConsumer,
                    &json!({
                        constants::STREAM_ARN: stream_arn,
                        constants::CONSUMER_NAME: args.consumer,
                    }),
                )
                .await?;
            let consumer = value["Consumer"].clone();

            if output_mode == OutputMode::Human {
                println!(
                    "registered {}\t{}\t{}",
                    consumer["ConsumerName"].as_str().unwrap_or(""),
                    consumer["ConsumerStatus"].as_str().unwrap_or(""),
                    consumer["ConsumerARN"].as_str().unwrap_or("")
                );
            } else {
                emit_single_value(output_mode, &consumer)?;
            }
            Ok(())
        }
        ConsumersCommand::Describe(args) => {
            let stream_arn = stream_arn(client, &args.stream).await?;
            let consumer = describe_consumer(client, &stream_arn, &args.consumer).await?;
            if output_mode == OutputMode::Human {
                println!(
                    "ConsumerName: {}",
                    consumer["ConsumerName"].as_str().unwrap_or("")
                );
                println!(
                    "ConsumerARN: {}",
                    consumer["ConsumerARN"].as_str().unwrap_or("")
                );
                println!(
                    "Status: {}",
                    consumer["ConsumerStatus"].as_str().unwrap_or("")
                );
                println!(
                    "StreamARN: {}",
                    consumer["StreamARN"].as_str().unwrap_or("")
                );
            } else {
                emit_single_value(output_mode, &consumer)?;
            }
            Ok(())
        }
        ConsumersCommand::Deregister(args) => {
            let stream_arn = stream_arn(client, &args.stream).await?;
            client
                .call(
                    Operation::DeregisterStreamConsumer,
                    &json!({
                        constants::STREAM_ARN: stream_arn,
                        constants::CONSUMER_NAME: args.consumer,
                    }),
                )
                .await?;
            if output_mode == OutputMode::Human {
                println!("deregister requested for {}", args.consumer);
            } else {
                emit_single_value(
                    output_mode,
                    &json!({
                        "ConsumerName": args.consumer,
                    }),
                )?;
            }
            Ok(())
        }
    }
}

async fn run_put(client: &ApiClient, output_mode: OutputMode, args: PutArgs) -> Result<()> {
    let trim_base64_whitespace = args.base64 && (args.file.is_some() || args.stdin);
    let bytes = read_bytes_input(args.data, args.file, args.stdin)?;
    let data = if args.base64 {
        let bytes = if trim_base64_whitespace {
            trim_ascii_whitespace(&bytes).to_vec()
        } else {
            bytes
        };
        String::from_utf8(bytes)
            .map_err(|_| Error::InvalidInput("base64 input must be valid UTF-8".into()))?
    } else {
        base64::engine::general_purpose::STANDARD.encode(bytes)
    };

    let mut body = json!({
        constants::STREAM_NAME: args.stream,
        constants::PARTITION_KEY: args.partition_key,
        constants::DATA: data,
    });
    if let Some(explicit_hash_key) = args.explicit_hash_key {
        body[constants::EXPLICIT_HASH_KEY] = Value::String(explicit_hash_key);
    }

    let response = client.call(Operation::PutRecord, &body).await?;
    if output_mode == OutputMode::Human {
        println!(
            "sequence={} shard={}",
            response["SequenceNumber"].as_str().unwrap_or(""),
            response["ShardId"].as_str().unwrap_or("")
        );
    } else {
        emit_single_value(output_mode, &response)?;
    }
    Ok(())
}

async fn run_tail(client: &ApiClient, output_mode: OutputMode, args: TailArgs) -> Result<()> {
    let stream = args.stream.clone();
    let limit = args.limit.map(NonZeroUsize::get);
    let iterator_spec = IteratorSpec {
        from: args.from,
        sequence_number: args.sequence_number.clone(),
        timestamp_seconds: parse_optional_timestamp(args.timestamp.as_deref())?,
        latest_fallback_timestamp: Utc::now().timestamp_millis() as f64 / 1000.0,
    };
    validate_iterator_spec(&iterator_spec)?;

    let follow = match (args.follow, args.no_follow) {
        (true, false) => true,
        (false, true) => false,
        (false, false) => true,
        (true, true) => unreachable!("clap enforces conflicting tail follow flags"),
    };
    validate_tail_output_mode(output_mode, follow, limit)?;
    let shard_ids = resolve_shard_ids(client, &stream, &args.shard_ids).await?;
    let tail_config = TailConfig {
        output_mode,
        stream,
        shard_ids,
        iterator_spec,
        follow,
        limit,
        decode: args.decode,
    };

    if let Some(consumer_name) = args.consumer {
        let stream_arn = stream_arn(client, &tail_config.stream).await?;
        let consumer = describe_consumer_with_hint(client, &stream_arn, &consumer_name).await?;
        tail_efo(
            client,
            EfoTailConfig {
                tail: tail_config,
                consumer_arn: consumer["ConsumerARN"].as_str().unwrap_or("").to_string(),
            },
        )
        .await
    } else {
        tail_polling(
            client,
            PollingTailConfig {
                tail: tail_config,
                poll_interval: args.poll_interval,
            },
        )
        .await
    }
}

async fn run_replay(client: &ApiClient, output_mode: OutputMode, args: ReplayArgs) -> Result<()> {
    let speed = parse_replay_speed(&args.speed)?;
    let mut records = read_capture_file(&args.file)?;
    if records.is_empty() {
        if output_mode == OutputMode::Human {
            println!("capture file is empty, nothing to replay");
        } else {
            emit_single_value(output_mode, &json!({ "ReplayCount": 0 }))?;
        }
        return Ok(());
    }

    records.sort_by_key(|record| record.ts);
    if let Some(limit) = args.limit {
        records.truncate(limit);
    }

    let mut replayed = 0usize;
    let start = Instant::now();
    for (index, record) in records.iter().enumerate() {
        if let ReplaySpeed::Multiplier(multiplier) = speed
            && index > 0
        {
            let delta_ms = record.ts.saturating_sub(records[index - 1].ts);
            if delta_ms > 0 {
                let sleep_ms = (delta_ms as f64 / multiplier) as u64;
                if sleep_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
            }
        }

        let mut body = json!({
            constants::STREAM_NAME: args.stream.as_deref().unwrap_or(&record.stream),
            constants::DATA: record.data,
            constants::PARTITION_KEY: record.partition_key,
        });
        if let Some(explicit_hash_key) = record.explicit_hash_key.as_ref() {
            body[constants::EXPLICIT_HASH_KEY] = Value::String(explicit_hash_key.clone());
        }

        match client.call(Operation::PutRecord, &body).await {
            Ok(_) => replayed += 1,
            Err(error) => {
                eprintln!("record {}/{} failed: {error}", index + 1, records.len());
                if args.fail_fast {
                    return Err(error);
                }
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    if output_mode == OutputMode::Human {
        println!("replayed {replayed} records in {elapsed:.2}s");
    } else {
        emit_single_value(
            output_mode,
            &json!({
                "ReplayCount": replayed,
                "ElapsedSeconds": elapsed,
            }),
        )?;
    }
    Ok(())
}

async fn run_api(client: &ApiClient, output_mode: OutputMode, args: ApiArgs) -> Result<()> {
    match args.command {
        ApiCommand::Call(args) => {
            let operation = Operation::from_str(&args.operation).map_err(|_| {
                Error::InvalidInput(format!("unknown operation {}", args.operation))
            })?;
            if operation == Operation::SubscribeToShard {
                return Err(Error::InvalidInput(
                    "SubscribeToShard is streaming; use `ferro tail --consumer <name>` instead"
                        .into(),
                ));
            }

            let body = read_json_input(args.body, args.body_file, args.stdin)?;
            let response = client.call(operation, &body).await?;
            if output_mode == OutputMode::Human {
                println!("{}", serde_json::to_string_pretty(&response)?);
            } else {
                emit_single_value(output_mode, &response)?;
            }
            Ok(())
        }
    }
}

async fn run_health(client: &ApiClient, output_mode: OutputMode) -> Result<()> {
    let body = client.get_health().await?;
    if output_mode == OutputMode::Human {
        println!("{}", body.trim());
    } else {
        emit_single_value(output_mode, &json!({ "status": body.trim() }))?;
    }
    Ok(())
}

async fn list_all_streams(client: &ApiClient) -> Result<Vec<String>> {
    let mut stream_names = Vec::new();
    let mut start: Option<String> = None;

    loop {
        let mut body = json!({});
        if let Some(start_name) = start.as_ref() {
            body[constants::EXCLUSIVE_START_STREAM_NAME] = Value::String(start_name.clone());
        }
        let response = client.call(Operation::ListStreams, &body).await?;
        let names = response["StreamNames"].as_array().ok_or_else(|| {
            Error::InvalidInput("ListStreams response missing StreamNames".into())
        })?;
        if names.is_empty() {
            break;
        }
        for name in names {
            if let Some(name) = name.as_str() {
                stream_names.push(name.to_string());
            }
        }
        let has_more = response["HasMoreStreams"].as_bool().unwrap_or(false);
        if !has_more {
            break;
        }
        start = stream_names.last().cloned();
    }

    Ok(stream_names)
}

async fn list_all_shards(client: &ApiClient, stream: &str) -> Result<Vec<Value>> {
    let mut shards = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let body = if let Some(token) = next_token.as_ref() {
            json!({
                constants::NEXT_TOKEN: token,
                constants::MAX_RESULTS: 1000,
            })
        } else {
            json!({
                constants::STREAM_NAME: stream,
                constants::MAX_RESULTS: 1000,
            })
        };

        let response = client.call(Operation::ListShards, &body).await?;
        if let Some(items) = response["Shards"].as_array() {
            shards.extend(items.iter().cloned());
        }
        next_token = response[constants::NEXT_TOKEN].as_str().map(str::to_string);
        if next_token.is_none() {
            break;
        }
    }

    Ok(shards)
}

async fn list_consumers(client: &ApiClient, stream_arn: &str) -> Result<Vec<Value>> {
    let response = client
        .call(
            Operation::ListStreamConsumers,
            &json!({
                constants::STREAM_ARN: stream_arn,
                constants::MAX_RESULTS: 10000,
            }),
        )
        .await?;
    Ok(response["Consumers"]
        .as_array()
        .cloned()
        .unwrap_or_default())
}

async fn stream_summary(client: &ApiClient, stream: &str) -> Result<Value> {
    let response = client
        .call(
            Operation::DescribeStreamSummary,
            &json!({
                constants::STREAM_NAME: stream,
            }),
        )
        .await?;
    Ok(response["StreamDescriptionSummary"].clone())
}

async fn stream_arn(client: &ApiClient, stream: &str) -> Result<String> {
    stream_summary(client, stream).await?["StreamARN"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| {
            Error::InvalidInput(format!(
                "DescribeStreamSummary returned no StreamARN for {stream}"
            ))
        })
}

async fn describe_consumer(client: &ApiClient, stream_arn: &str, consumer: &str) -> Result<Value> {
    let response = client
        .call(
            Operation::DescribeStreamConsumer,
            &json!({
                constants::STREAM_ARN: stream_arn,
                constants::CONSUMER_NAME: consumer,
            }),
        )
        .await?;
    Ok(response["ConsumerDescription"].clone())
}

async fn describe_consumer_with_hint(
    client: &ApiClient,
    stream_arn: &str,
    consumer: &str,
) -> Result<Value> {
    match describe_consumer(client, stream_arn, consumer).await {
        Ok(value) => Ok(value),
        Err(Error::Api(api_error)) if api_error.error_type == constants::RESOURCE_NOT_FOUND => {
            Err(Error::InvalidInput(format!(
                "consumer {consumer} not found; run `ferro consumers register <stream> {consumer}` first"
            )))
        }
        Err(error) => Err(error),
    }
}

async fn wait_for_stream_active(client: &ApiClient, stream: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let summary = stream_summary(client, stream).await?;
        if summary["StreamStatus"].as_str() == Some("ACTIVE") {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::InvalidInput(format!(
                "timed out waiting for stream {stream} to become ACTIVE"
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_stream_deleted(
    client: &ApiClient,
    stream: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        match stream_summary(client, stream).await {
            Ok(_) => {}
            Err(Error::Api(api_error)) if api_error.error_type == constants::RESOURCE_NOT_FOUND => {
                return Ok(());
            }
            Err(error) => return Err(error),
        }

        if Instant::now() >= deadline {
            return Err(Error::InvalidInput(format!(
                "timed out waiting for stream {stream} to be deleted"
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn resolve_shard_ids(
    client: &ApiClient,
    stream: &str,
    requested: &[String],
) -> Result<Vec<String>> {
    if !requested.is_empty() {
        return Ok(requested.to_vec());
    }

    let shards = list_all_shards(client, stream).await?;
    let shard_ids = shards
        .iter()
        .filter_map(|shard| shard["ShardId"].as_str().map(str::to_string))
        .collect::<Vec<_>>();
    if shard_ids.is_empty() {
        return Err(Error::InvalidInput(format!(
            "stream {stream} has no shards"
        )));
    }
    Ok(shard_ids)
}

async fn tail_polling(client: &ApiClient, config: PollingTailConfig) -> Result<()> {
    let PollingTailConfig {
        tail:
            TailConfig {
                output_mode,
                stream,
                shard_ids,
                iterator_spec,
                follow,
                limit,
                decode,
            },
        poll_interval,
    } = config;
    let mut shards = Vec::with_capacity(shard_ids.len());
    let mut json_records = Vec::new();
    for shard_id in shard_ids {
        let iterator =
            get_shard_iterator(client, &stream, &shard_id, &iterator_spec, None, false).await?;
        shards.push(PollShardState {
            shard_id,
            iterator,
            last_sequence_number: None,
        });
    }

    let mut printed = 0usize;
    loop {
        let mut saw_records = false;
        for shard in &mut shards {
            let mut refreshed = false;
            let response = loop {
                match client
                    .call(
                        Operation::GetRecords,
                        &json!({
                            constants::SHARD_ITERATOR: shard.iterator,
                        }),
                    )
                    .await
                {
                    Ok(response) => break response,
                    Err(Error::Api(api_error))
                        if api_error.error_type == constants::EXPIRED_ITERATOR && !refreshed =>
                    {
                        shard.iterator = get_shard_iterator(
                            client,
                            &stream,
                            &shard.shard_id,
                            &iterator_spec,
                            shard.last_sequence_number.as_deref(),
                            true,
                        )
                        .await?;
                        refreshed = true;
                    }
                    Err(error) => return Err(error),
                }
            };

            if let Some(iterator) = response["NextShardIterator"].as_str() {
                shard.iterator = iterator.to_string();
            }

            let records = response["Records"].as_array().cloned().unwrap_or_default();
            if !records.is_empty() {
                saw_records = true;
            }
            for record in records {
                let tail_record = TailRecord {
                    stream: stream.clone(),
                    shard_id: shard.shard_id.clone(),
                    partition_key: record["PartitionKey"].as_str().unwrap_or("").to_string(),
                    sequence_number: record["SequenceNumber"].as_str().unwrap_or("").to_string(),
                    approximate_arrival_timestamp: record["ApproximateArrivalTimestamp"].as_f64(),
                    data: record["Data"].as_str().unwrap_or("").to_string(),
                };
                handle_tail_record(output_mode, &mut json_records, &tail_record, decode)?;
                printed += 1;
                shard.last_sequence_number = Some(tail_record.sequence_number.clone());
                if limit.is_some_and(|limit| printed >= limit) {
                    return finish_tail_output(output_mode, json_records);
                }
            }
        }

        if !follow && !saw_records {
            return finish_tail_output(output_mode, json_records);
        }
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return finish_tail_output(output_mode, json_records),
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }
}

async fn tail_efo(client: &ApiClient, config: EfoTailConfig) -> Result<()> {
    let EfoTailConfig {
        tail:
            TailConfig {
                output_mode,
                stream,
                shard_ids,
                iterator_spec,
                follow,
                limit,
                decode,
            },
        consumer_arn,
    } = config;
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut handles = Vec::with_capacity(shard_ids.len());
    let mut json_records = Vec::new();

    for shard_id in shard_ids {
        let tx = tx.clone();
        let error_tx = tx.clone();
        let client = client.clone();
        let stream = stream.clone();
        let consumer_arn = consumer_arn.clone();
        let iterator_spec = iterator_spec.clone();
        handles.push(tokio::spawn(async move {
            if let Err(error) = run_efo_shard_loop(
                client,
                stream,
                shard_id,
                consumer_arn,
                iterator_spec,
                follow,
                tx,
            )
            .await
            {
                let _ = error_tx.send(TailEvent::Error(error.to_string()));
            }
        }));
    }
    drop(tx);

    let mut printed = 0usize;
    while let Some(event) = rx.recv().await {
        match event {
            TailEvent::Record(record) => {
                handle_tail_record(output_mode, &mut json_records, &record, decode)?;
                printed += 1;
                if limit.is_some_and(|limit| printed >= limit) {
                    for handle in &handles {
                        handle.abort();
                    }
                    break;
                }
            }
            TailEvent::Error(error) => {
                for handle in &handles {
                    handle.abort();
                }
                return Err(Error::EventStream(error));
            }
        }
    }

    for handle in handles {
        let _ = handle.await;
    }

    finish_tail_output(output_mode, json_records)
}

async fn run_efo_shard_loop(
    client: ApiClient,
    stream: String,
    shard_id: String,
    consumer_arn: String,
    iterator_spec: IteratorSpec,
    follow: bool,
    tx: mpsc::UnboundedSender<TailEvent>,
) -> Result<()> {
    let mut starting_position = iterator_spec.starting_position();
    loop {
        let result = run_efo_shard_session(
            client.clone(),
            stream.clone(),
            shard_id.clone(),
            consumer_arn.clone(),
            starting_position.clone(),
            follow,
            tx.clone(),
        )
        .await?;
        if !follow || result.terminal_end_of_shard {
            return Ok(());
        }
        starting_position = result
            .continuation_sequence_number
            .map(|sequence_number| {
                json!({
                    "Type": constants::SHARD_ITERATOR_AT_SEQUENCE_NUMBER,
                    "SequenceNumber": sequence_number,
                })
            })
            .unwrap_or_else(|| iterator_spec.starting_position());
    }
}

async fn run_efo_shard_session(
    client: ApiClient,
    stream: String,
    shard_id: String,
    consumer_arn: String,
    starting_position: Value,
    follow: bool,
    tx: mpsc::UnboundedSender<TailEvent>,
) -> Result<EfoSessionResult> {
    let response = client
        .call_response(
            Operation::SubscribeToShard,
            &json!({
                constants::CONSUMER_ARN: consumer_arn,
                constants::SHARD_ID: shard_id,
                constants::STARTING_POSITION: starting_position,
            }),
            None,
        )
        .await?;

    let mut stream_body = response.bytes_stream();
    let mut buffer = BytesMut::new();
    let mut continuation_sequence_number = None;
    let mut terminal_end_of_shard = false;

    while let Some(chunk) = stream_body.next().await {
        let chunk = chunk?;
        buffer.extend_from_slice(&chunk);
        while let Some(message) = pop_event_message(&mut buffer)? {
            match header_value(&message, ":message-type") {
                Some("initial-response") => {}
                Some("event") => {
                    let event_type = header_value(&message, ":event-type").unwrap_or_default();
                    if event_type == "initial-response" {
                        continue;
                    }
                    if event_type != "SubscribeToShardEvent" {
                        return Err(Error::EventStream(format!(
                            "unexpected event type {event_type}"
                        )));
                    }

                    let payload = serde_json::from_slice::<Value>(message.payload())?;
                    continuation_sequence_number = payload["ContinuationSequenceNumber"]
                        .as_str()
                        .map(str::to_string)
                        .or(continuation_sequence_number);
                    let child_shards = payload["ChildShards"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default();
                    let records = payload["Records"].as_array();
                    let saw_records = records.is_some_and(|records| !records.is_empty());
                    if let Some(records) = records {
                        for record in records {
                            let event = TailEvent::Record(TailRecord {
                                stream: stream.clone(),
                                shard_id: shard_id.clone(),
                                partition_key: record["PartitionKey"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string(),
                                sequence_number: record["SequenceNumber"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string(),
                                approximate_arrival_timestamp:
                                    record["ApproximateArrivalTimestamp"].as_f64(),
                                data: record["Data"].as_str().unwrap_or("").to_string(),
                            });
                            let _ = tx.send(event);
                        }
                    }
                    terminal_end_of_shard = !child_shards.is_empty();

                    if !follow && !saw_records {
                        return Ok(EfoSessionResult {
                            continuation_sequence_number,
                            terminal_end_of_shard,
                        });
                    }
                }
                Some("exception") => {
                    return Err(Error::EventStream(format!(
                        "{}: {}",
                        header_value(&message, ":exception-type")
                            .unwrap_or("SubscribeToShardException"),
                        serde_json::from_slice::<Value>(message.payload())
                            .ok()
                            .and_then(|value| value["message"].as_str().map(str::to_string))
                            .unwrap_or_else(|| "stream exception".into())
                    )));
                }
                _ => {}
            }
        }
    }

    Ok(EfoSessionResult {
        continuation_sequence_number,
        terminal_end_of_shard,
    })
}

async fn get_shard_iterator(
    client: &ApiClient,
    stream: &str,
    shard_id: &str,
    iterator_spec: &IteratorSpec,
    last_sequence_number: Option<&str>,
    expired_refresh: bool,
) -> Result<String> {
    let mut body = json!({
        constants::STREAM_NAME: stream,
        constants::SHARD_ID: shard_id,
    });

    if let Some(last_sequence_number) = last_sequence_number {
        body[constants::SHARD_ITERATOR_TYPE] =
            Value::String(constants::SHARD_ITERATOR_AFTER_SEQUENCE_NUMBER.to_string());
        body[constants::SEQUENCE_NUMBER] = Value::String(last_sequence_number.to_string());
    } else {
        let iterator_type = if expired_refresh && iterator_spec.from == TailFrom::Latest {
            constants::SHARD_ITERATOR_AT_TIMESTAMP
        } else {
            iterator_spec.iterator_type()
        };
        body[constants::SHARD_ITERATOR_TYPE] = Value::String(iterator_type.to_string());
        if let Some(sequence_number) = iterator_spec.sequence_number.as_ref()
            && iterator_type != constants::SHARD_ITERATOR_AT_TIMESTAMP
        {
            body[constants::SEQUENCE_NUMBER] = Value::String(sequence_number.clone());
        }
        if iterator_type == constants::SHARD_ITERATOR_AT_TIMESTAMP {
            body[constants::TIMESTAMP] = json!(
                iterator_spec
                    .timestamp_seconds
                    .unwrap_or(iterator_spec.latest_fallback_timestamp)
            );
        } else if let Some(timestamp_seconds) = iterator_spec.timestamp_seconds {
            body[constants::TIMESTAMP] = json!(timestamp_seconds);
        }
    }

    let response = client.call(Operation::GetShardIterator, &body).await?;
    response[constants::SHARD_ITERATOR]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| {
            Error::InvalidInput(format!(
                "GetShardIterator returned no iterator for {shard_id}"
            ))
        })
}

impl IteratorSpec {
    fn iterator_type(&self) -> &'static str {
        match self.from {
            TailFrom::Latest => constants::SHARD_ITERATOR_LATEST,
            TailFrom::TrimHorizon => constants::SHARD_ITERATOR_TRIM_HORIZON,
            TailFrom::AtSequence => constants::SHARD_ITERATOR_AT_SEQUENCE_NUMBER,
            TailFrom::AfterSequence => constants::SHARD_ITERATOR_AFTER_SEQUENCE_NUMBER,
            TailFrom::AtTimestamp => constants::SHARD_ITERATOR_AT_TIMESTAMP,
        }
    }

    fn starting_position(&self) -> Value {
        let mut position = json!({
            "Type": self.iterator_type(),
        });
        if let Some(sequence_number) = self.sequence_number.as_ref() {
            position["SequenceNumber"] = Value::String(sequence_number.clone());
        }
        if let Some(timestamp_seconds) = self.timestamp_seconds {
            position["Timestamp"] = json!(timestamp_seconds);
        }
        position
    }
}

fn validate_iterator_spec(spec: &IteratorSpec) -> Result<()> {
    match spec.from {
        TailFrom::AtSequence | TailFrom::AfterSequence if spec.sequence_number.is_none() => {
            Err(Error::InvalidInput(
                "--sequence-number is required for --from at-sequence and --from after-sequence"
                    .into(),
            ))
        }
        TailFrom::AtTimestamp if spec.timestamp_seconds.is_none() => Err(Error::InvalidInput(
            "--timestamp is required for --from at-timestamp".into(),
        )),
        _ => Ok(()),
    }
}

fn validate_tail_output_mode(
    output_mode: OutputMode,
    follow: bool,
    limit: Option<usize>,
) -> Result<()> {
    if output_mode == OutputMode::Json && follow && limit.is_none() {
        return Err(Error::InvalidInput(
            "--json tail requires a bounded run; use --limit or --no-follow, or use --ndjson for streaming output"
                .into(),
        ));
    }
    Ok(())
}

fn handle_tail_record(
    output_mode: OutputMode,
    json_records: &mut Vec<Value>,
    record: &TailRecord,
    decode: DecodeMode,
) -> Result<()> {
    if output_mode == OutputMode::Json {
        json_records.push(tail_record_value(record, decode));
        Ok(())
    } else {
        emit_tail_record(output_mode, record, decode)
    }
}

fn finish_tail_output(output_mode: OutputMode, json_records: Vec<Value>) -> Result<()> {
    if output_mode == OutputMode::Json {
        emit_single_value(OutputMode::Json, &Value::Array(json_records))?;
    }
    Ok(())
}

fn emit_tail_record(
    output_mode: OutputMode,
    record: &TailRecord,
    decode: DecodeMode,
) -> Result<()> {
    match output_mode {
        OutputMode::Human => {
            let payload = decode_payload(&record.data, decode);
            let timestamp = format_timestamp(record.approximate_arrival_timestamp);
            println!(
                "{}\t{}\t{}\t{}\t{}",
                timestamp, record.shard_id, record.sequence_number, record.partition_key, payload
            );
        }
        OutputMode::Ndjson => {
            emit_single_value(OutputMode::Ndjson, &tail_record_value(record, decode))?
        }
        OutputMode::Json => unreachable!("JSON tail output should be buffered before emission"),
    }
    Ok(())
}

fn tail_record_value(record: &TailRecord, decode: DecodeMode) -> Value {
    json!({
        "StreamName": record.stream,
        "ShardId": record.shard_id,
        "SequenceNumber": record.sequence_number,
        "PartitionKey": record.partition_key,
        "ApproximateArrivalTimestamp": record.approximate_arrival_timestamp,
        "Data": record.data,
        "Decoded": decode_payload(&record.data, decode),
    })
}

fn decode_payload(data: &str, decode: DecodeMode) -> String {
    match decode {
        DecodeMode::None => String::new(),
        DecodeMode::Base64 => data.to_string(),
        DecodeMode::Hex => base64::engine::general_purpose::STANDARD
            .decode(data)
            .map(hex::encode)
            .unwrap_or_else(|_| data.to_string()),
        DecodeMode::Utf8 => base64::engine::general_purpose::STANDARD
            .decode(data)
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_else(|_| data.to_string()),
        DecodeMode::Auto => match base64::engine::general_purpose::STANDARD.decode(data) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(value) => value,
                Err(_) => data.to_string(),
            },
            Err(_) => data.to_string(),
        },
    }
}

fn format_timestamp(timestamp: Option<f64>) -> String {
    timestamp
        .map(|timestamp| (timestamp * 1000.0) as i64)
        .and_then(|millis| Utc.timestamp_millis_opt(millis).single())
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_else(|| "-".into())
}

fn emit_single_value(output_mode: OutputMode, value: &Value) -> Result<()> {
    match output_mode {
        OutputMode::Human => println!("{}", serde_json::to_string_pretty(value)?),
        OutputMode::Json | OutputMode::Ndjson => println!("{}", serde_json::to_string(value)?),
    }
    Ok(())
}

fn emit_ndjson_values<'a>(values: impl IntoIterator<Item = &'a Value>) -> Result<()> {
    for value in values {
        println!("{}", serde_json::to_string(value)?);
    }
    Ok(())
}

fn read_bytes_input(data: Option<String>, file: Option<PathBuf>, stdin: bool) -> Result<Vec<u8>> {
    use std::io::Read as _;

    if let Some(data) = data {
        Ok(data.into_bytes())
    } else if let Some(path) = file {
        Ok(std::fs::read(path)?)
    } else if stdin {
        let mut stdin = std::io::stdin();
        let mut bytes = Vec::new();
        stdin.read_to_end(&mut bytes)?;
        Ok(bytes)
    } else {
        Err(Error::InvalidInput("missing input".into()))
    }
}

fn trim_ascii_whitespace(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|index| index + 1)
        .unwrap_or(start);
    &bytes[start..end]
}

fn read_json_input(body: Option<String>, body_file: Option<PathBuf>, stdin: bool) -> Result<Value> {
    use std::io::Read as _;

    let bytes = if let Some(body) = body {
        body.into_bytes()
    } else if let Some(path) = body_file {
        std::fs::read(path)?
    } else if stdin {
        let mut stdin = std::io::stdin();
        let mut bytes = Vec::new();
        stdin.read_to_end(&mut bytes)?;
        bytes
    } else {
        b"{}".to_vec()
    };
    Ok(serde_json::from_slice(&bytes)?)
}

fn parse_duration(value: &str) -> std::result::Result<Duration, String> {
    humantime::parse_duration(value).map_err(|error| error.to_string())
}

fn parse_optional_timestamp(value: Option<&str>) -> Result<Option<f64>> {
    value
        .map(|value| {
            chrono::DateTime::parse_from_rfc3339(value)
                .map(|timestamp| timestamp.timestamp_millis() as f64 / 1000.0)
                .map_err(|error| {
                    Error::InvalidInput(format!("invalid RFC3339 timestamp {value:?}: {error}"))
                })
        })
        .transpose()
}

fn parse_replay_speed(value: &str) -> Result<ReplaySpeed> {
    if value == "max" {
        return Ok(ReplaySpeed::Max);
    }
    let value = value
        .strip_suffix('x')
        .ok_or_else(|| Error::InvalidInput(format!("invalid replay speed {value:?}")))?;
    let multiplier = value
        .parse::<f64>()
        .map_err(|_| Error::InvalidInput(format!("invalid replay speed {value:?}")))?;
    if multiplier <= 0.0 {
        return Err(Error::InvalidInput(format!(
            "invalid replay speed {:?}: multiplier must be > 0",
            value
        )));
    }
    Ok(ReplaySpeed::Multiplier(multiplier))
}

fn pop_event_message(buffer: &mut BytesMut) -> Result<Option<Message>> {
    if buffer.len() < 4 {
        return Ok(None);
    }
    let total_len = u32::from_be_bytes(buffer[..4].try_into().expect("length prefix")) as usize;
    if buffer.len() < total_len {
        return Ok(None);
    }

    let frame = buffer.split_to(total_len).freeze();
    let mut cursor = Cursor::new(frame.as_ref());
    Ok(Some(
        read_message_from(&mut cursor).map_err(|error| Error::EventStream(error.to_string()))?,
    ))
}

fn header_value<'a>(message: &'a Message, name: &str) -> Option<&'a str> {
    message
        .headers()
        .iter()
        .find(|header| header.name().as_str() == name)
        .and_then(|header| match header.value() {
            HeaderValue::String(value) => Some(value.as_str()),
            _ => None,
        })
}
