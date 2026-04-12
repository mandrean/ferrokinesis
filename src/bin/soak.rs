use clap::{ArgAction, Parser, ValueEnum};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, watch};
use tokio::time::{Instant, MissedTickBehavior};

const CONTENT_TYPE: &str = "application/x-amz-json-1.1";
const VERSION: &str = "Kinesis_20131202";
const AUTH_HEADER: &str = "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234";
const AMZ_DATE: &str = "20150101T000000Z";
const STREAM_COUNT: usize = 2;
const SHARDS_PER_STREAM: u32 = 2;
const RECORD_DATA: &str = "AAAA";
const HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProfileName {
    Local,
    Ci,
}

#[derive(Debug, Clone)]
struct Profile {
    name: &'static str,
    duration: Duration,
    sample_interval: Duration,
    put_interval: Duration,
    get_interval: Duration,
    records_per_put: u32,
    restart_at: Option<Duration>,
}

impl Profile {
    fn from_name(name: ProfileName) -> Self {
        match name {
            ProfileName::Local => Self {
                name: "local",
                duration: Duration::from_secs(15 * 60),
                sample_interval: Duration::from_secs(15),
                put_interval: Duration::from_millis(100),
                get_interval: Duration::from_secs(1),
                records_per_put: 10,
                restart_at: Some(Duration::from_secs(8 * 60)),
            },
            ProfileName::Ci => Self {
                name: "ci",
                duration: Duration::from_secs(60 * 60),
                sample_interval: Duration::from_secs(15),
                put_interval: Duration::from_millis(100),
                get_interval: Duration::from_secs(1),
                records_per_put: 10,
                restart_at: Some(Duration::from_secs(30 * 60)),
            },
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "soak")]
#[command(about = "Production soak harness for ferrokinesis durable mode")]
struct Cli {
    /// Soak profile (`local` ~15m, `ci` ~60m)
    #[arg(long, value_enum, default_value_t = ProfileName::Local)]
    profile: ProfileName,

    /// Override profile duration in seconds
    #[arg(long)]
    duration_secs: Option<u64>,

    /// Override sample interval in seconds
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    sample_interval_secs: Option<u64>,

    /// Restart the server once at this point in the run (0 disables restart)
    #[arg(long)]
    restart_at_secs: Option<u64>,

    /// Host to probe for health and metrics
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port for the ferrokinesis server
    #[arg(long, default_value_t = 4567)]
    port: u16,

    /// Path to ferrokinesis server binary
    #[arg(long)]
    server_bin: Option<PathBuf>,

    /// Build server binary before running soak
    #[arg(long, action = ArgAction::Set, default_value_t = false)]
    build_server: bool,

    /// Output directory for artifacts
    #[arg(long)]
    output_dir: Option<PathBuf>,

    /// Durable state directory (defaults under output dir)
    #[arg(long)]
    state_dir: Option<PathBuf>,

    /// Hard cap on retained serialized record bytes
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    max_retained_bytes: u64,

    /// Snapshot interval in seconds
    #[arg(long, default_value_t = 30)]
    snapshot_interval_secs: u64,

    /// Retention reaper interval in seconds
    #[arg(long, default_value_t = 5)]
    retention_check_interval_secs: u64,

    /// Iterator TTL in seconds
    #[arg(long, default_value_t = 30)]
    iterator_ttl_seconds: u64,
}

#[derive(Debug)]
struct RunConfig {
    profile: Profile,
    port: u16,
    base_url: String,
    output_dir: PathBuf,
    state_dir: PathBuf,
    server_bin: PathBuf,
    restart_at: Option<Duration>,
    max_retained_bytes: u64,
    snapshot_interval_secs: u64,
    retention_check_interval_secs: u64,
    iterator_ttl_seconds: u64,
    build_server: bool,
}

#[derive(Debug, Default, Serialize)]
struct WorkloadStats {
    put_requests_ok: u64,
    put_requests_error: u64,
    put_records_sent: u64,
    put_records_ok: u64,
    put_records_failed: u64,
    get_records_ok: u64,
    get_records_error: u64,
    unexpected_5xx: u64,
    readiness_failures: u64,
    planned_restart_readiness_failures: u64,
    restart_count: u32,
    restart_recovery_ms: Option<u64>,
}

#[derive(Debug, Default, Serialize)]
struct StreamState {
    name: String,
    shard_iterators: BTreeMap<String, String>,
    put_counter: u64,
}

#[derive(Debug, Clone, Default, Serialize)]
struct MetricsSnapshot {
    retained_bytes: Option<u64>,
    retained_records: Option<u64>,
    rejected_writes_total: Option<u64>,
    replay_complete: Option<u64>,
    last_snapshot_timestamp_ms: Option<u64>,
    active_iterators: Option<u64>,
    streams: Option<u64>,
    open_shards: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct Sample {
    elapsed_secs: f64,
    ready_status: u16,
    rss_bytes: Option<u64>,
    open_fds: Option<u64>,
    state_dir_bytes: u64,
    wal_bytes: Option<u64>,
    snapshot_bytes: Option<u64>,
    retained_bytes: Option<u64>,
    retained_records: Option<u64>,
    rejected_writes_total: Option<u64>,
    replay_complete: Option<u64>,
    last_snapshot_timestamp_ms: Option<u64>,
    active_iterators: Option<u64>,
    streams: Option<u64>,
    open_shards: Option<u64>,
}

#[derive(Debug, Serialize)]
struct TimeseriesRow {
    elapsed_secs: String,
    ready_status: u16,
    rss_bytes: Option<u64>,
    open_fds: Option<u64>,
    state_dir_bytes: u64,
    wal_bytes: Option<u64>,
    snapshot_bytes: Option<u64>,
    retained_bytes: Option<u64>,
    retained_records: Option<u64>,
    rejected_writes_total: Option<u64>,
    replay_complete: Option<u64>,
    last_snapshot_timestamp_ms: Option<u64>,
    active_iterators: Option<u64>,
    streams: Option<u64>,
    open_shards: Option<u64>,
}

impl From<&Sample> for TimeseriesRow {
    fn from(sample: &Sample) -> Self {
        Self {
            elapsed_secs: format_elapsed_secs(sample.elapsed_secs),
            ready_status: sample.ready_status,
            rss_bytes: sample.rss_bytes,
            open_fds: sample.open_fds,
            state_dir_bytes: sample.state_dir_bytes,
            wal_bytes: sample.wal_bytes,
            snapshot_bytes: sample.snapshot_bytes,
            retained_bytes: sample.retained_bytes,
            retained_records: sample.retained_records,
            rejected_writes_total: sample.rejected_writes_total,
            replay_complete: sample.replay_complete,
            last_snapshot_timestamp_ms: sample.last_snapshot_timestamp_ms,
            active_iterators: sample.active_iterators,
            streams: sample.streams,
            open_shards: sample.open_shards,
        }
    }
}

#[derive(Debug, Serialize)]
struct StateDirSizeRow {
    elapsed_secs: String,
    state_dir_bytes: u64,
    wal_bytes: Option<u64>,
    snapshot_bytes: Option<u64>,
}

impl From<&Sample> for StateDirSizeRow {
    fn from(sample: &Sample) -> Self {
        Self {
            elapsed_secs: format_elapsed_secs(sample.elapsed_secs),
            state_dir_bytes: sample.state_dir_bytes,
            wal_bytes: sample.wal_bytes,
            snapshot_bytes: sample.snapshot_bytes,
        }
    }
}

#[derive(Debug, Serialize)]
struct Summary {
    profile: String,
    duration_secs: u64,
    sample_interval_secs: u64,
    restart_at_secs: Option<u64>,
    output_dir: String,
    state_dir: String,
    server_bin: String,
    stats: WorkloadStats,
    samples_count: usize,
    rss_peak_bytes: Option<u64>,
    fd_peak: Option<u64>,
    state_dir_peak_bytes: u64,
    retained_bytes_peak: Option<u64>,
    retained_records_peak: Option<u64>,
    rejected_writes_total_final: Option<u64>,
    replay_complete_final: Option<u64>,
}

struct SampleLoopContext {
    base_url: String,
    state_dir: PathBuf,
    pid: Arc<AtomicU32>,
    samples: Arc<Mutex<Vec<Sample>>>,
    start: Instant,
}

struct ServerProcess {
    child: Option<Child>,
}

impl ServerProcess {
    fn spawn(cfg: &RunConfig, log_path: &Path) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            child: Some(spawn_server(cfg, log_path)?),
        })
    }

    fn id(&self) -> u32 {
        self.child
            .as_ref()
            .expect("server process should be present")
            .id()
    }

    fn restart(&mut self, cfg: &RunConfig, log_path: &Path) -> Result<(), Box<dyn Error>> {
        self.stop();
        self.child = Some(spawn_server(cfg, log_path)?);
        Ok(())
    }

    fn take_exit_status(&mut self) -> io::Result<Option<ExitStatus>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };

        let status = child.try_wait()?;
        if status.is_some() {
            self.child = None;
        }
        Ok(status)
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            stop_server(&mut child);
        }
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.stop();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    let mut profile = Profile::from_name(cli.profile);
    if let Some(duration_secs) = cli.duration_secs {
        profile.duration = Duration::from_secs(duration_secs);
    }
    if let Some(sample_interval_secs) = cli.sample_interval_secs {
        profile.sample_interval = Duration::from_secs(sample_interval_secs);
    }

    let restart_at = match cli.restart_at_secs {
        Some(0) => None,
        Some(v) => Some(Duration::from_secs(v)),
        None => profile.restart_at,
    };

    let output_dir = match cli.output_dir {
        Some(path) => path,
        None => default_output_dir(profile.name)?,
    };
    fs::create_dir_all(&output_dir)?;
    let state_dir = cli
        .state_dir
        .unwrap_or_else(|| output_dir.join("state-dir"));
    fs::create_dir_all(&state_dir)?;

    let server_bin = cli
        .server_bin
        .unwrap_or_else(|| PathBuf::from("target/release/ferrokinesis"));
    let base_url = format!("http://{}:{}", cli.host, cli.port);
    let cfg = RunConfig {
        profile,
        port: cli.port,
        base_url,
        output_dir,
        state_dir,
        server_bin,
        restart_at,
        max_retained_bytes: cli.max_retained_bytes,
        snapshot_interval_secs: cli.snapshot_interval_secs,
        retention_check_interval_secs: cli.retention_check_interval_secs,
        iterator_ttl_seconds: cli.iterator_ttl_seconds,
        build_server: cli.build_server,
    };

    if cfg.build_server || !cfg.server_bin.exists() {
        build_server_binary()?;
    }

    run_soak(cfg).await
}

async fn run_soak(cfg: RunConfig) -> Result<(), Box<dyn Error>> {
    let client = build_http_client()?;
    let server_log_path = cfg.output_dir.join("server.log");
    let mut server = ServerProcess::spawn(&cfg, &server_log_path)?;
    let pid_cell = Arc::new(AtomicU32::new(server.id()));
    wait_ready(
        &client,
        &cfg.base_url,
        &mut server,
        &server_log_path,
        Duration::from_secs(90),
    )
    .await?;

    let mut streams = create_streams(&client, &cfg.base_url).await?;

    let start = Instant::now();
    let samples = Arc::new(Mutex::new(Vec::<Sample>::new()));
    let (sampler_stop_tx, sampler_stop_rx) = watch::channel(false);
    let sampler_task = {
        let context = SampleLoopContext {
            base_url: cfg.base_url.clone(),
            state_dir: cfg.state_dir.clone(),
            pid: Arc::clone(&pid_cell),
            samples: Arc::clone(&samples),
            start,
        };
        let interval = cfg.profile.sample_interval;
        let client = client.clone();
        tokio::spawn(async move {
            sample_loop(client, sampler_stop_rx, interval, context).await;
        })
    };

    let mut stats = WorkloadStats::default();
    let mut put_tick = tokio::time::interval(cfg.profile.put_interval);
    put_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut get_tick = tokio::time::interval(cfg.profile.get_interval);
    get_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let deadline = start + cfg.profile.duration;
    let mut restart_done = false;
    let mut restart_window_secs: Option<(f64, f64)> = None;

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }

        if !restart_done
            && let Some(restart_at) = cfg.restart_at
            && now.duration_since(start) >= restart_at
        {
            let restart_started = Instant::now();
            server.restart(&cfg, &server_log_path)?;
            pid_cell.store(server.id(), Ordering::Relaxed);
            wait_ready(
                &client,
                &cfg.base_url,
                &mut server,
                &server_log_path,
                Duration::from_secs(90),
            )
            .await?;
            for stream in &mut streams {
                stream.shard_iterators =
                    get_shard_iterators(&client, &cfg.base_url, &stream.name).await?;
            }
            stats.restart_count += 1;
            stats.restart_recovery_ms = Some(restart_started.elapsed().as_millis() as u64);
            restart_window_secs = Some((
                restart_started.duration_since(start).as_secs_f64(),
                Instant::now().duration_since(start).as_secs_f64(),
            ));
            restart_done = true;
        }

        tokio::select! {
            _ = put_tick.tick() => {
                for stream in &mut streams {
                    run_put_batch(&client, &cfg.base_url, stream, cfg.profile.records_per_put, &mut stats).await?;
                }
            }
            _ = get_tick.tick() => {
                for stream in &mut streams {
                    run_get_records(&client, &cfg.base_url, stream, &mut stats).await?;
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                break;
            }
        }
    }

    let _ = sampler_stop_tx.send(true);
    sampler_task
        .await
        .map_err(|err| format!("sampler task failed: {err}"))?;

    let final_metrics = fetch_metrics_text(&client, &cfg.base_url)
        .await
        .unwrap_or_default();
    server.stop();

    let samples = samples.lock().await;
    let (readiness_failures, planned_restart_readiness_failures) =
        classify_readiness_failures(&samples, restart_window_secs);
    stats.readiness_failures = readiness_failures;
    stats.planned_restart_readiness_failures = planned_restart_readiness_failures;
    write_artifacts(&cfg, &stats, &samples, &final_metrics)?;

    Ok(())
}

fn build_http_client() -> Result<Client, Box<dyn Error>> {
    Ok(Client::builder()
        .connect_timeout(HTTP_CONNECT_TIMEOUT)
        .timeout(HTTP_REQUEST_TIMEOUT)
        .build()?)
}

fn build_server_binary() -> Result<(), Box<dyn Error>> {
    let status = Command::new("cargo")
        .args(["build", "--release", "--bin", "ferrokinesis"])
        .status()?;
    if !status.success() {
        return Err("failed to build release ferrokinesis binary".into());
    }
    Ok(())
}

fn default_output_dir(profile_name: &str) -> io::Result<PathBuf> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_secs();
    Ok(PathBuf::from("artifacts")
        .join("soak")
        .join(format!("{profile_name}-{now}")))
}

fn server_args(cfg: &RunConfig) -> Vec<OsString> {
    vec![
        "--port".into(),
        cfg.port.to_string().into(),
        "--state-dir".into(),
        cfg.state_dir.as_os_str().to_os_string(),
        "--snapshot-interval-secs".into(),
        cfg.snapshot_interval_secs.to_string().into(),
        "--retention-check-interval-secs".into(),
        cfg.retention_check_interval_secs.to_string().into(),
        "--iterator-ttl-seconds".into(),
        cfg.iterator_ttl_seconds.to_string().into(),
        "--max-retained-bytes".into(),
        cfg.max_retained_bytes.to_string().into(),
        "--create-stream-ms".into(),
        "0".into(),
        "--delete-stream-ms".into(),
        "0".into(),
        "--update-stream-ms".into(),
        "0".into(),
        "--log-level".into(),
        "info".into(),
    ]
}

fn spawn_server(cfg: &RunConfig, log_path: &Path) -> Result<Child, Box<dyn Error>> {
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let stderr = stdout.try_clone()?;

    let mut command = Command::new(&cfg.server_bin);
    command
        .args(server_args(cfg))
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));

    let child = command.spawn()?;
    Ok(child)
}

fn stop_server(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

async fn create_streams(
    client: &Client,
    base_url: &str,
) -> Result<Vec<StreamState>, Box<dyn Error>> {
    let mut streams = Vec::with_capacity(STREAM_COUNT);
    for index in 0..STREAM_COUNT {
        let stream_name = format!("soak-stream-{index}");
        let _ = kinesis_request(
            client,
            base_url,
            "DeleteStream",
            &json!({ "StreamName": stream_name, "EnforceConsumerDeletion": true }),
        )
        .await;

        let create = kinesis_request(
            client,
            base_url,
            "CreateStream",
            &json!({ "StreamName": stream_name, "ShardCount": SHARDS_PER_STREAM }),
        )
        .await?;
        if !create.status().is_success() {
            return Err(
                format!("CreateStream failed for {stream_name}: {}", create.status()).into(),
            );
        }

        wait_stream_active(client, base_url, &stream_name, Duration::from_secs(45)).await?;
        let iterators = get_shard_iterators(client, base_url, &stream_name).await?;
        streams.push(StreamState {
            name: stream_name,
            shard_iterators: iterators,
            put_counter: 0,
        });
    }
    Ok(streams)
}

async fn wait_ready(
    client: &Client,
    base_url: &str,
    server: &mut ServerProcess,
    log_path: &Path,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    loop {
        if let Some(status) = server.take_exit_status()? {
            return Err(server_exit_before_ready_error(status, log_path));
        }

        match fetch_ready_status(client, base_url).await {
            Ok(200) => return Ok(()),
            Ok(_) | Err(_) => {
                if started.elapsed() > timeout {
                    return Err("server did not become ready before timeout".into());
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

fn server_exit_before_ready_error(status: ExitStatus, log_path: &Path) -> Box<dyn Error> {
    io::Error::other(format!(
        "server exited before becoming ready with status {status}. See {} for details.",
        log_path.display()
    ))
    .into()
}

async fn wait_stream_active(
    client: &Client,
    base_url: &str,
    stream_name: &str,
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    loop {
        let response = kinesis_request(
            client,
            base_url,
            "DescribeStreamSummary",
            &json!({ "StreamName": stream_name }),
        )
        .await?;

        if response.status().is_success() {
            let body: Value = response.json().await?;
            if body["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE" {
                return Ok(());
            }
        }

        if started.elapsed() > timeout {
            return Err(format!("stream {stream_name} did not become ACTIVE").into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn get_shard_iterators(
    client: &Client,
    base_url: &str,
    stream_name: &str,
) -> Result<BTreeMap<String, String>, Box<dyn Error>> {
    let response = kinesis_request(
        client,
        base_url,
        "ListShards",
        &json!({ "StreamName": stream_name }),
    )
    .await?;
    if !response.status().is_success() {
        return Err(format!("ListShards failed for {stream_name}: {}", response.status()).into());
    }
    let body: Value = response.json().await?;
    let shards = body["Shards"]
        .as_array()
        .ok_or("ListShards response missing Shards array")?;

    let mut iterators = BTreeMap::new();
    for shard in shards {
        let shard_id = shard["ShardId"]
            .as_str()
            .ok_or("ListShards shard missing ShardId")?;
        let iter_resp = kinesis_request(
            client,
            base_url,
            "GetShardIterator",
            &json!({
                "StreamName": stream_name,
                "ShardId": shard_id,
                "ShardIteratorType": "TRIM_HORIZON"
            }),
        )
        .await?;
        if !iter_resp.status().is_success() {
            return Err(format!(
                "GetShardIterator failed for {stream_name}/{shard_id}: {}",
                iter_resp.status()
            )
            .into());
        }
        let iter_body: Value = iter_resp.json().await?;
        let iterator = iter_body["ShardIterator"]
            .as_str()
            .ok_or("GetShardIterator response missing ShardIterator")?
            .to_string();
        iterators.insert(shard_id.to_string(), iterator);
    }
    Ok(iterators)
}

async fn run_put_batch(
    client: &Client,
    base_url: &str,
    stream: &mut StreamState,
    records_per_put: u32,
    stats: &mut WorkloadStats,
) -> Result<(), Box<dyn Error>> {
    let mut records = Vec::with_capacity(records_per_put as usize);
    for _ in 0..records_per_put {
        records.push(json!({
            "Data": RECORD_DATA,
            "PartitionKey": format!("pk-{}-{}", stream.name, stream.put_counter),
        }));
        stream.put_counter += 1;
    }

    let response = kinesis_request(
        client,
        base_url,
        "PutRecords",
        &json!({
            "StreamName": stream.name,
            "Records": records,
        }),
    )
    .await?;
    let attempted_records = u64::from(records_per_put);
    let status = response.status();
    stats.put_records_sent += attempted_records;

    if status.is_success() {
        let body: Value = response.json().await?;
        let (ok_records, failed_records) = parse_put_records_outcomes(&body, records_per_put)?;
        stats.put_requests_ok += 1;
        stats.put_records_ok += ok_records;
        stats.put_records_failed += failed_records;
    } else {
        record_put_records_request_failure(stats, status, attempted_records);
    }
    Ok(())
}

async fn run_get_records(
    client: &Client,
    base_url: &str,
    stream: &mut StreamState,
    stats: &mut WorkloadStats,
) -> Result<(), Box<dyn Error>> {
    let shard_ids: Vec<String> = stream.shard_iterators.keys().cloned().collect();
    for shard_id in shard_ids {
        let Some(iterator) = stream.shard_iterators.get(&shard_id).cloned() else {
            continue;
        };
        let response = kinesis_request(
            client,
            base_url,
            "GetRecords",
            &json!({
                "ShardIterator": iterator,
                "Limit": 100
            }),
        )
        .await?;

        if response.status().is_success() {
            stats.get_records_ok += 1;
            let body: Value = response.json().await?;
            if let Some(next) = body["NextShardIterator"].as_str() {
                stream
                    .shard_iterators
                    .insert(shard_id.clone(), next.to_string());
            } else {
                let iterators = get_shard_iterators(client, base_url, &stream.name).await?;
                stream.shard_iterators = iterators;
                break;
            }
        } else {
            stats.get_records_error += 1;
            if response.status().is_server_error() {
                stats.unexpected_5xx += 1;
            }
        }
    }
    Ok(())
}

async fn sample_loop(
    client: Client,
    mut stop: watch::Receiver<bool>,
    interval: Duration,
    context: SampleLoopContext,
) {
    let mut ticker = tokio::time::interval_at(context.start + interval, interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        let pid_value = context.pid.load(Ordering::Relaxed);
        let ready_status = fetch_ready_status(&client, &context.base_url)
            .await
            .unwrap_or(0);
        let metrics_text = fetch_metrics_text(&client, &context.base_url)
            .await
            .unwrap_or_default();
        let metrics = parse_metrics(&metrics_text);

        let sample = Sample {
            elapsed_secs: context.start.elapsed().as_secs_f64(),
            ready_status,
            rss_bytes: sample_rss_bytes(pid_value),
            open_fds: sample_open_fds(pid_value),
            state_dir_bytes: directory_size_bytes(&context.state_dir),
            wal_bytes: file_size(context.state_dir.join("wal.log")),
            snapshot_bytes: file_size(context.state_dir.join("snapshot.bin")),
            retained_bytes: metrics.retained_bytes,
            retained_records: metrics.retained_records,
            rejected_writes_total: metrics.rejected_writes_total,
            replay_complete: metrics.replay_complete,
            last_snapshot_timestamp_ms: metrics.last_snapshot_timestamp_ms,
            active_iterators: metrics.active_iterators,
            streams: metrics.streams,
            open_shards: metrics.open_shards,
        };

        context.samples.lock().await.push(sample);
        tokio::select! {
            _ = ticker.tick() => {}
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    break;
                }
            }
        }
    }
}

fn write_artifacts(
    cfg: &RunConfig,
    stats: &WorkloadStats,
    samples: &[Sample],
    final_metrics: &str,
) -> Result<(), Box<dyn Error>> {
    let timeseries_path = cfg.output_dir.join("soak-timeseries.csv");
    let summary_path = cfg.output_dir.join("soak-summary.json");
    let metrics_path = cfg.output_dir.join("metrics-final.prom");
    let state_sizes_path = cfg.output_dir.join("state-dir-sizes.csv");

    let mut timeseries_writer = csv::Writer::from_path(&timeseries_path)?;
    for sample in samples {
        timeseries_writer.serialize(TimeseriesRow::from(sample))?;
    }
    timeseries_writer.flush()?;

    let mut state_sizes_writer = csv::Writer::from_path(&state_sizes_path)?;
    for sample in samples {
        state_sizes_writer.serialize(StateDirSizeRow::from(sample))?;
    }
    state_sizes_writer.flush()?;
    fs::write(&metrics_path, final_metrics)?;

    let summary = Summary {
        profile: cfg.profile.name.to_string(),
        duration_secs: cfg.profile.duration.as_secs(),
        sample_interval_secs: cfg.profile.sample_interval.as_secs(),
        restart_at_secs: cfg.restart_at.map(|d| d.as_secs()),
        output_dir: cfg.output_dir.display().to_string(),
        state_dir: cfg.state_dir.display().to_string(),
        server_bin: cfg.server_bin.display().to_string(),
        stats: WorkloadStats {
            put_requests_ok: stats.put_requests_ok,
            put_requests_error: stats.put_requests_error,
            put_records_sent: stats.put_records_sent,
            put_records_ok: stats.put_records_ok,
            put_records_failed: stats.put_records_failed,
            get_records_ok: stats.get_records_ok,
            get_records_error: stats.get_records_error,
            unexpected_5xx: stats.unexpected_5xx,
            readiness_failures: stats.readiness_failures,
            planned_restart_readiness_failures: stats.planned_restart_readiness_failures,
            restart_count: stats.restart_count,
            restart_recovery_ms: stats.restart_recovery_ms,
        },
        samples_count: samples.len(),
        rss_peak_bytes: samples.iter().filter_map(|s| s.rss_bytes).max(),
        fd_peak: samples.iter().filter_map(|s| s.open_fds).max(),
        state_dir_peak_bytes: samples.iter().map(|s| s.state_dir_bytes).max().unwrap_or(0),
        retained_bytes_peak: samples.iter().filter_map(|s| s.retained_bytes).max(),
        retained_records_peak: samples.iter().filter_map(|s| s.retained_records).max(),
        rejected_writes_total_final: samples.last().and_then(|s| s.rejected_writes_total),
        replay_complete_final: samples.last().and_then(|s| s.replay_complete),
    };

    let summary_json = serde_json::to_string_pretty(&summary)?;
    fs::write(summary_path, summary_json)?;

    Ok(())
}

fn record_put_records_request_failure(
    stats: &mut WorkloadStats,
    status: reqwest::StatusCode,
    attempted_records: u64,
) {
    stats.put_requests_error += 1;
    stats.put_records_failed += attempted_records;
    if status.is_server_error() {
        stats.unexpected_5xx += 1;
    }
}

fn format_elapsed_secs(elapsed_secs: f64) -> String {
    format!("{elapsed_secs:.3}")
}

async fn fetch_ready_status(client: &Client, base_url: &str) -> Result<u16, reqwest::Error> {
    let response = client
        .get(format!("{base_url}/_health/ready"))
        .send()
        .await?;
    Ok(response.status().as_u16())
}

async fn fetch_metrics_text(client: &Client, base_url: &str) -> Result<String, reqwest::Error> {
    let response = client.get(format!("{base_url}/metrics")).send().await?;
    response.text().await
}

async fn kinesis_request(
    client: &Client,
    base_url: &str,
    operation: &str,
    body: &Value,
) -> Result<reqwest::Response, reqwest::Error> {
    client
        .post(base_url)
        .header("Content-Type", CONTENT_TYPE)
        .header("X-Amz-Target", format!("{VERSION}.{operation}"))
        .header("Authorization", AUTH_HEADER)
        .header("X-Amz-Date", AMZ_DATE)
        .body(serde_json::to_vec(body).expect("JSON body should serialize"))
        .send()
        .await
}

fn parse_metrics(text: &str) -> MetricsSnapshot {
    MetricsSnapshot {
        retained_bytes: metric_sum_as_u64(text, "ferrokinesis_retained_bytes"),
        retained_records: metric_sum_as_u64(text, "ferrokinesis_retained_records"),
        rejected_writes_total: metric_sum_as_u64(text, "ferrokinesis_rejected_writes_total"),
        replay_complete: metric_sum_as_u64(text, "ferrokinesis_replay_complete"),
        last_snapshot_timestamp_ms: metric_sum_as_u64(
            text,
            "ferrokinesis_last_snapshot_timestamp_ms",
        ),
        active_iterators: metric_sum_as_u64(text, "ferrokinesis_active_iterators"),
        streams: metric_sum_as_u64(text, "ferrokinesis_streams"),
        open_shards: metric_sum_as_u64(text, "ferrokinesis_open_shards"),
    }
}

fn parse_put_records_outcomes(body: &Value, requested_records: u32) -> Result<(u64, u64), String> {
    let requested_records = u64::from(requested_records);
    let failed_record_count = body["FailedRecordCount"].as_u64();

    if let Some(records) = body["Records"].as_array() {
        let failed_from_records = records
            .iter()
            .filter(|record| {
                record
                    .get("ErrorCode")
                    .is_some_and(|value| !value.is_null())
            })
            .count() as u64;
        if let Some(reported_failed) = failed_record_count
            && reported_failed != failed_from_records
        {
            return Err(format!(
                "PutRecords response mismatch: FailedRecordCount={reported_failed}, failed records={failed_from_records}"
            ));
        }
        let total_records = records.len() as u64;
        return Ok((
            total_records.saturating_sub(failed_from_records),
            failed_from_records,
        ));
    }

    if let Some(failed_record_count) = failed_record_count {
        return Ok((
            requested_records.saturating_sub(failed_record_count),
            failed_record_count,
        ));
    }

    Err("PutRecords response missing Records and FailedRecordCount".into())
}

fn classify_readiness_failures(
    samples: &[Sample],
    restart_window_secs: Option<(f64, f64)>,
) -> (u64, u64) {
    let mut unexpected = 0;
    let mut planned = 0;
    for sample in samples.iter().filter(|sample| sample.ready_status != 200) {
        let during_restart = restart_window_secs
            .is_some_and(|(start, end)| sample.elapsed_secs >= start && sample.elapsed_secs <= end);
        if during_restart {
            planned += 1;
        } else {
            unexpected += 1;
        }
    }
    (unexpected, planned)
}

fn metric_sum_as_u64(text: &str, metric_name: &str) -> Option<u64> {
    let mut total = 0.0;
    let mut found = false;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || !line.starts_with(metric_name) {
            continue;
        }

        let Some(rest) = line.get(metric_name.len()..) else {
            continue;
        };
        if !rest.starts_with(' ') && !rest.starts_with('{') {
            continue;
        }

        let Some(value) = line.split_whitespace().last() else {
            continue;
        };
        let Ok(parsed) = value.parse::<f64>() else {
            continue;
        };
        total += parsed;
        found = true;
    }
    found.then_some(total as u64)
}

fn sample_rss_bytes(pid: u32) -> Option<u64> {
    let proc_status = format!("/proc/{pid}/status");
    if let Ok(content) = fs::read_to_string(proc_status) {
        for line in content.lines() {
            if let Some(value) = line.strip_prefix("VmRSS:") {
                let kb = value
                    .split_whitespace()
                    .next()
                    .and_then(|v| v.parse::<u64>().ok())?;
                return Some(kb.saturating_mul(1024));
            }
        }
    }

    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let kb = stdout.trim().parse::<u64>().ok()?;
    Some(kb.saturating_mul(1024))
}

fn sample_open_fds(pid: u32) -> Option<u64> {
    let proc_fd = format!("/proc/{pid}/fd");
    if let Ok(entries) = fs::read_dir(proc_fd) {
        return Some(entries.count() as u64);
    }

    let output = Command::new("lsof")
        .args(["-p", &pid.to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let count = stdout.lines().count();
    count.checked_sub(1).map(|v| v as u64)
}

fn directory_size_bytes(path: &Path) -> u64 {
    let mut total = 0_u64;
    let Ok(entries) = fs::read_dir(path) else {
        return total;
    };
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        if metadata.is_dir() {
            total = total.saturating_add(directory_size_bytes(&entry.path()));
        } else if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    total
}

fn file_size(path: PathBuf) -> Option<u64> {
    fs::metadata(path).ok().map(|meta| meta.len())
}

#[cfg(test)]
mod tests {
    use super::{
        Cli, Profile, ProfileName, RunConfig, Sample, WorkloadStats, classify_readiness_failures,
        directory_size_bytes, format_elapsed_secs, metric_sum_as_u64, parse_put_records_outcomes,
        record_put_records_request_failure, server_args, write_artifacts,
    };
    use clap::Parser;
    use csv::StringRecord;
    use serde_json::json;
    use std::{fs, path::Path};

    #[test]
    fn parses_prometheus_values_with_and_without_labels() {
        let metrics = "\
# TYPE ferrokinesis_retained_bytes gauge
ferrokinesis_retained_bytes 42
ferrokinesis_requests_total{operation=\"PutRecord\",result=\"ok\"} 10
ferrokinesis_requests_total{operation=\"PutRecord\",result=\"error\"} 3
";

        assert_eq!(
            metric_sum_as_u64(metrics, "ferrokinesis_retained_bytes"),
            Some(42)
        );
        assert_eq!(
            metric_sum_as_u64(metrics, "ferrokinesis_requests_total"),
            Some(13)
        );
    }

    #[test]
    fn computes_recursive_directory_size() {
        let temp = tempfile::tempdir().expect("temp dir");
        let file1 = temp.path().join("a.bin");
        let nested = temp.path().join("nested");
        fs::create_dir_all(&nested).expect("nested dir");
        let file2 = nested.join("b.bin");

        fs::write(&file1, vec![0_u8; 10]).expect("write file1");
        fs::write(&file2, vec![0_u8; 7]).expect("write file2");

        assert_eq!(directory_size_bytes(temp.path()), 17);
    }

    #[test]
    fn parses_put_records_partial_failures() {
        let response = json!({
            "FailedRecordCount": 1,
            "Records": [
                {"SequenceNumber": "1", "ShardId": "shardId-000000000000"},
                {"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "Rate exceeded"},
                {"SequenceNumber": "3", "ShardId": "shardId-000000000000"}
            ]
        });

        assert_eq!(parse_put_records_outcomes(&response, 3).unwrap(), (2, 1));
    }

    #[test]
    fn rejects_inconsistent_put_records_response() {
        let response = json!({
            "FailedRecordCount": 2,
            "Records": [
                {"SequenceNumber": "1", "ShardId": "shardId-000000000000"},
                {"ErrorCode": "ProvisionedThroughputExceededException", "ErrorMessage": "Rate exceeded"}
            ]
        });

        assert!(parse_put_records_outcomes(&response, 2).is_err());
    }

    #[test]
    fn counts_failed_put_records_batches_as_failed_records() {
        let mut stats = WorkloadStats::default();

        record_put_records_request_failure(&mut stats, reqwest::StatusCode::BAD_GATEWAY, 3);

        assert_eq!(stats.put_requests_error, 1);
        assert_eq!(stats.put_records_failed, 3);
        assert_eq!(stats.unexpected_5xx, 1);
    }

    #[test]
    fn ignores_planned_restart_window_in_readiness_failures() {
        let samples = vec![
            Sample {
                elapsed_secs: 1.0,
                ready_status: 503,
                rss_bytes: None,
                open_fds: None,
                state_dir_bytes: 0,
                wal_bytes: None,
                snapshot_bytes: None,
                retained_bytes: None,
                retained_records: None,
                rejected_writes_total: None,
                replay_complete: None,
                last_snapshot_timestamp_ms: None,
                active_iterators: None,
                streams: None,
                open_shards: None,
            },
            Sample {
                elapsed_secs: 10.0,
                ready_status: 503,
                rss_bytes: None,
                open_fds: None,
                state_dir_bytes: 0,
                wal_bytes: None,
                snapshot_bytes: None,
                retained_bytes: None,
                retained_records: None,
                rejected_writes_total: None,
                replay_complete: None,
                last_snapshot_timestamp_ms: None,
                active_iterators: None,
                streams: None,
                open_shards: None,
            },
            Sample {
                elapsed_secs: 11.0,
                ready_status: 200,
                rss_bytes: None,
                open_fds: None,
                state_dir_bytes: 0,
                wal_bytes: None,
                snapshot_bytes: None,
                retained_bytes: None,
                retained_records: None,
                rejected_writes_total: None,
                replay_complete: None,
                last_snapshot_timestamp_ms: None,
                active_iterators: None,
                streams: None,
                open_shards: None,
            },
        ];

        assert_eq!(
            classify_readiness_failures(&samples, Some((9.0, 10.5))),
            (1, 1)
        );
    }

    #[test]
    fn cli_rejects_zero_sample_interval() {
        assert!(Cli::try_parse_from(["soak", "--sample-interval-secs", "0"]).is_err());
    }

    #[test]
    fn formats_elapsed_seconds_with_three_decimal_places() {
        assert_eq!(format_elapsed_secs(1.2), "1.200");
    }

    #[test]
    fn builds_server_args_without_access_log_flag() {
        let temp = tempfile::tempdir().expect("temp dir");
        let cfg = test_run_config(temp.path());
        let args: Vec<String> = server_args(&cfg)
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert!(!args.iter().any(|arg| arg == "--access-log=false"));
        assert!(args.iter().any(|arg| arg == "--state-dir"));
    }

    #[test]
    fn writes_csv_artifacts_with_expected_headers_and_values() {
        let temp = tempfile::tempdir().expect("temp dir");
        let output_dir = temp.path().join("artifacts");
        fs::create_dir_all(&output_dir).expect("output dir");
        let cfg = test_run_config(&output_dir);
        let stats = WorkloadStats {
            put_requests_ok: 1,
            ..WorkloadStats::default()
        };
        let samples = vec![Sample {
            elapsed_secs: 1.2345,
            ready_status: 200,
            rss_bytes: Some(11),
            open_fds: None,
            state_dir_bytes: 22,
            wal_bytes: Some(33),
            snapshot_bytes: None,
            retained_bytes: Some(44),
            retained_records: Some(55),
            rejected_writes_total: Some(66),
            replay_complete: Some(1),
            last_snapshot_timestamp_ms: Some(77),
            active_iterators: Some(88),
            streams: Some(2),
            open_shards: Some(3),
        }];

        write_artifacts(&cfg, &stats, &samples, "metric 1\n").expect("artifacts written");

        let mut timeseries_reader =
            csv::Reader::from_path(output_dir.join("soak-timeseries.csv")).expect("timeseries");
        assert_eq!(
            timeseries_reader.headers().expect("headers").clone(),
            StringRecord::from(vec![
                "elapsed_secs",
                "ready_status",
                "rss_bytes",
                "open_fds",
                "state_dir_bytes",
                "wal_bytes",
                "snapshot_bytes",
                "retained_bytes",
                "retained_records",
                "rejected_writes_total",
                "replay_complete",
                "last_snapshot_timestamp_ms",
                "active_iterators",
                "streams",
                "open_shards",
            ])
        );
        let timeseries_rows = timeseries_reader
            .records()
            .collect::<Result<Vec<_>, _>>()
            .expect("timeseries rows");
        assert_eq!(timeseries_rows.len(), 1);
        assert_eq!(timeseries_rows[0].get(0), Some("1.234"));
        assert_eq!(timeseries_rows[0].get(1), Some("200"));
        assert_eq!(timeseries_rows[0].get(2), Some("11"));
        assert_eq!(timeseries_rows[0].get(3), Some(""));

        let mut state_sizes_reader =
            csv::Reader::from_path(output_dir.join("state-dir-sizes.csv")).expect("state sizes");
        assert_eq!(
            state_sizes_reader.headers().expect("headers").clone(),
            StringRecord::from(vec![
                "elapsed_secs",
                "state_dir_bytes",
                "wal_bytes",
                "snapshot_bytes",
            ])
        );
        let state_rows = state_sizes_reader
            .records()
            .collect::<Result<Vec<_>, _>>()
            .expect("state rows");
        assert_eq!(state_rows.len(), 1);
        assert_eq!(state_rows[0].get(0), Some("1.234"));
        assert_eq!(state_rows[0].get(1), Some("22"));
        assert_eq!(state_rows[0].get(2), Some("33"));
        assert_eq!(state_rows[0].get(3), Some(""));
    }

    fn test_run_config(output_dir: &Path) -> RunConfig {
        let profile = Profile::from_name(ProfileName::Local);
        let state_dir = output_dir.join("state-dir");
        fs::create_dir_all(&state_dir).expect("state dir");
        RunConfig {
            profile,
            port: 4567,
            base_url: "http://127.0.0.1:4567".into(),
            output_dir: output_dir.to_path_buf(),
            state_dir,
            server_bin: Path::new("/tmp/ferrokinesis").to_path_buf(),
            restart_at: None,
            max_retained_bytes: 1024,
            snapshot_interval_secs: 30,
            retention_check_interval_secs: 5,
            iterator_ttl_seconds: 30,
            build_server: false,
        }
    }
}
