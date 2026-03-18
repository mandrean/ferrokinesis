# Benchmarking

ferrokinesis includes two benchmarking tools:

1. **Criterion micro-benchmarks** — measure individual API operations in isolation.
2. **Goose load test** — measure throughput and latency under concurrent load, with optional comparison against [kinesalite](https://github.com/mhart/kinesalite).

## Criterion Micro-benchmarks

```sh
cargo bench
```

Results are written to `target/criterion/`. Open `target/criterion/report/index.html` for the full report.

## Goose Load Test

The `loadtest` binary is built behind the `loadtest` feature flag so it doesn't affect the main server build.

### Build

```sh
cargo build --release --features loadtest
```

### Run against a single server

Start your server, then:

```sh
# ferrokinesis with instant stream creation (recommended for benchmarks)
./target/release/ferrokinesis --port 4567 --create-stream-ms 0 --delete-stream-ms 0

# In another terminal
./target/release/loadtest \
    --host http://localhost:4567 \
    --users 10 \
    --startup-time 5s \
    --run-time 30s \
    --report-file report.html
```

### Comparison script

The included `scripts/bench-compare.sh` automates running the load test against both ferrokinesis and kinesalite:

```sh
bash scripts/bench-compare.sh
```

The script:

- Builds the loadtest binary in release mode
- Starts ferrokinesis on port 4567
- Runs the load test and prints a metrics summary
- If `npx` is available, starts kinesalite on port 4568 and repeats
- Generates HTML reports: `ferro-report.html` and `kinesalite-report.html`

Environment variables for tuning:

| Variable             | Default | Description                     |
|----------------------|---------|---------------------------------|
| `BENCH_USERS`        | `10`    | Number of concurrent users      |
| `BENCH_STARTUP_TIME` | `5s`    | Time to ramp up all users       |
| `BENCH_RUN_TIME`     | `30s`   | Duration of the sustained phase |

### Workload

Each simulated user runs in a loop:

| Transaction   | Weight | Description                                |
|---------------|--------|--------------------------------------------|
| `PutRecord`   | 10     | Write a record with a random partition key |
| `GetRecords`  | 1      | Read records using a shard iterator        |

Setup (per user): `CreateStream` (1 shard, waits for ACTIVE) + `GetShardIterator`.
Teardown (per user): `DeleteStream`.

### Goose CLI options

The loadtest binary accepts all standard [Goose](https://docs.rs/goose) CLI flags. Common ones:

```
--host <URL>              Target server URL
--users <N>               Number of concurrent users
--startup-time <TIME>     Ramp-up duration (e.g. 5s, 1m)
--run-time <TIME>         Sustained load duration (e.g. 30s, 5m)
--report-file <PATH>      Write an HTML report
--running-metrics <SECS>  Print metrics every N seconds
--no-reset-metrics        Keep startup-phase metrics in the final report
```
