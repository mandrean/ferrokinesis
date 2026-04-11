use clap::{ArgAction, Parser, ValueEnum};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::{Instant, MissedTickBehavior};

const CONTENT_TYPE: &str = "application/x-amz-json-1.1";
const VERSION: &str = "Kinesis_20131202";
const AUTH_HEADER: &str = "AWS4-HMAC-SHA256 Credential=AKID/20150101/us-east-1/kinesis/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=abcd1234";
const AMZ_DATE: &str = "20150101T000000Z";
const STREAM_COUNT: usize = 2;
const SHARDS_PER_STREAM: u32 = 2;
const RECORD_DATA: &str = "AAAA";

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
    #[arg(long)]
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
    get_records_ok: u64,
    get_records_error: u64,
    unexpected_5xx: u64,
    readiness_failures: u64,
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
    let client = Client::new();
    let server_log_path = cfg.output_dir.join("server.log");
    let mut child = spawn_server(&cfg, &server_log_path)?;
    let pid_cell = Arc::new(AtomicU32::new(child.id()));
    wait_ready(&client, &cfg.base_url, Duration::from_secs(90)).await?;

    let mut streams = create_streams(&client, &cfg.base_url).await?;

    let samples = Arc::new(Mutex::new(Vec::<Sample>::new()));
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler_task = {
        let samples = Arc::clone(&samples);
        let stop = Arc::clone(&sampler_stop);
        let pid = Arc::clone(&pid_cell);
        let base_url = cfg.base_url.clone();
        let state_dir = cfg.state_dir.clone();
        let interval = cfg.profile.sample_interval;
        let client = client.clone();
        tokio::spawn(async move {
            sample_loop(client, base_url, state_dir, pid, stop, samples, interval).await;
        })
    };

    let mut stats = WorkloadStats::default();
    let start = Instant::now();
    let mut put_tick = tokio::time::interval(cfg.profile.put_interval);
    put_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut get_tick = tokio::time::interval(cfg.profile.get_interval);
    get_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let deadline = start + cfg.profile.duration;
    let mut restart_done = false;

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
            stop_server(&mut child);
            child = spawn_server(&cfg, &server_log_path)?;
            pid_cell.store(child.id(), Ordering::Relaxed);
            wait_ready(&client, &cfg.base_url, Duration::from_secs(90)).await?;
            for stream in &mut streams {
                stream.shard_iterators =
                    get_shard_iterators(&client, &cfg.base_url, &stream.name).await?;
            }
            stats.restart_count += 1;
            stats.restart_recovery_ms = Some(restart_started.elapsed().as_millis() as u64);
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

    sampler_stop.store(true, Ordering::Relaxed);
    let _ = sampler_task.await;

    let final_metrics = fetch_metrics_text(&client, &cfg.base_url)
        .await
        .unwrap_or_default();
    stop_server(&mut child);

    let samples = samples.lock().await;
    stats.readiness_failures = samples
        .iter()
        .filter(|sample| sample.ready_status != 200)
        .count() as u64;
    write_artifacts(&cfg, &stats, &samples, &final_metrics)?;

    Ok(())
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

fn spawn_server(cfg: &RunConfig, log_path: &Path) -> Result<Child, Box<dyn Error>> {
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let stderr = stdout.try_clone()?;

    let mut command = Command::new(&cfg.server_bin);
    command
        .arg("--port")
        .arg(cfg.port.to_string())
        .arg("--state-dir")
        .arg(&cfg.state_dir)
        .arg("--snapshot-interval-secs")
        .arg(cfg.snapshot_interval_secs.to_string())
        .arg("--retention-check-interval-secs")
        .arg(cfg.retention_check_interval_secs.to_string())
        .arg("--iterator-ttl-seconds")
        .arg(cfg.iterator_ttl_seconds.to_string())
        .arg("--max-retained-bytes")
        .arg(cfg.max_retained_bytes.to_string())
        .arg("--create-stream-ms")
        .arg("0")
        .arg("--delete-stream-ms")
        .arg("0")
        .arg("--update-stream-ms")
        .arg("0")
        .arg("--log-level")
        .arg("info")
        .arg("--access-log=false")
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
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let started = Instant::now();
    loop {
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

    if response.status().is_success() {
        stats.put_requests_ok += 1;
        stats.put_records_sent += u64::from(records_per_put);
    } else {
        stats.put_requests_error += 1;
    }
    if response.status().is_server_error() {
        stats.unexpected_5xx += 1;
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
    base_url: String,
    state_dir: PathBuf,
    pid: Arc<AtomicU32>,
    stop: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<Sample>>>,
    interval: Duration,
) {
    let start = Instant::now();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        let pid_value = pid.load(Ordering::Relaxed);
        let ready_status = fetch_ready_status(&client, &base_url).await.unwrap_or(0);
        let metrics_text = fetch_metrics_text(&client, &base_url)
            .await
            .unwrap_or_default();
        let metrics = parse_metrics(&metrics_text);

        let sample = Sample {
            elapsed_secs: start.elapsed().as_secs_f64(),
            ready_status,
            rss_bytes: sample_rss_bytes(pid_value),
            open_fds: sample_open_fds(pid_value),
            state_dir_bytes: directory_size_bytes(&state_dir),
            wal_bytes: file_size(state_dir.join("wal.log")),
            snapshot_bytes: file_size(state_dir.join("snapshot.bin")),
            retained_bytes: metrics.retained_bytes,
            retained_records: metrics.retained_records,
            rejected_writes_total: metrics.rejected_writes_total,
            replay_complete: metrics.replay_complete,
            last_snapshot_timestamp_ms: metrics.last_snapshot_timestamp_ms,
            active_iterators: metrics.active_iterators,
            streams: metrics.streams,
            open_shards: metrics.open_shards,
        };

        samples.lock().await.push(sample);
        if stop.load(Ordering::Relaxed) {
            break;
        }
        ticker.tick().await;
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

    let mut csv = String::from(
        "elapsed_secs,ready_status,rss_bytes,open_fds,state_dir_bytes,wal_bytes,snapshot_bytes,retained_bytes,retained_records,rejected_writes_total,replay_complete,last_snapshot_timestamp_ms,active_iterators,streams,open_shards\n",
    );
    for sample in samples {
        csv.push_str(&format!(
            "{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            sample.elapsed_secs,
            sample.ready_status,
            display_option(sample.rss_bytes),
            display_option(sample.open_fds),
            sample.state_dir_bytes,
            display_option(sample.wal_bytes),
            display_option(sample.snapshot_bytes),
            display_option(sample.retained_bytes),
            display_option(sample.retained_records),
            display_option(sample.rejected_writes_total),
            display_option(sample.replay_complete),
            display_option(sample.last_snapshot_timestamp_ms),
            display_option(sample.active_iterators),
            display_option(sample.streams),
            display_option(sample.open_shards),
        ));
    }
    fs::write(&timeseries_path, csv)?;

    let mut state_csv = String::from("elapsed_secs,state_dir_bytes,wal_bytes,snapshot_bytes\n");
    for sample in samples {
        state_csv.push_str(&format!(
            "{:.3},{},{},{}\n",
            sample.elapsed_secs,
            sample.state_dir_bytes,
            display_option(sample.wal_bytes),
            display_option(sample.snapshot_bytes)
        ));
    }
    fs::write(&state_sizes_path, state_csv)?;
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
            get_records_ok: stats.get_records_ok,
            get_records_error: stats.get_records_error,
            unexpected_5xx: stats.unexpected_5xx,
            readiness_failures: stats.readiness_failures,
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

fn display_option(value: Option<u64>) -> String {
    value.map_or_else(String::new, |v| v.to_string())
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
    use super::{directory_size_bytes, metric_sum_as_u64};
    use std::fs;

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
}
