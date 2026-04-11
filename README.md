# ferrokinesis

[![CI](https://github.com/mandrean/ferrokinesis/actions/workflows/ci.yml/badge.svg)](https://github.com/mandrean/ferrokinesis/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mandrean/ferrokinesis/branch/main/graph/badge.svg)](https://codecov.io/gh/mandrean/ferrokinesis)
[![crates.io](https://img.shields.io/crates/v/ferrokinesis.svg)](https://crates.io/crates/ferrokinesis)
[![docs.rs](https://docs.rs/ferrokinesis/badge.svg)](https://docs.rs/ferrokinesis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A local AWS Kinesis mock server for testing, written in Rust, with a companion `ferro` CLI for local stream workflows.

## Browser Demo

Try ferrokinesis without installing anything:

[![Open browser demo](https://img.shields.io/badge/Open-browser_demo-102318?style=for-the-badge&logo=googlechrome&logoColor=CDF564)](https://mandrean.github.io/ferrokinesis/)

The GitHub Pages demo runs the in-process WASM wrapper entirely in your browser. It keeps state in memory only, so refresh or `Reset state` starts you from a clean slate.

Run it locally with:

```sh
npm ci --prefix demo
npm --prefix demo run dev
```

## Features

- Pure Rust implementation
- Companion `ferro` CLI for streams, consumers, puts, tails, replay, and raw API calls
- Fully in-memory storage using [DashMap](https://github.com/xacrimon/dashmap) + per-stream `RwLock` with lock-free per-shard sequence generation
- Implements all 39 Kinesis Data Streams API operations
- Supports both JSON and CBOR content types
- Health check endpoints (`/_health`, `/_health/ready`, `/_health/live`) for Docker/K8s
- TOML configuration file support (`--config`)
- Configurable AWS account ID, region, shard iterator TTL, and request body size limit
- Retention period enforcement with configurable TTL-based record trimming
- Graceful shutdown
- TLS support with built-in certificate generation
- 500+ tests (integration, property-based, unit) with multi-language SDK conformance tests (Go, Python, Node.js, Java v1/v2, KCL)
- Criterion benchmarks with CI regression gate (>10% threshold)

## Installation

### Binary

Download pre-built binaries from [GitHub Releases](https://github.com/mandrean/ferrokinesis/releases):

```sh
# macOS (Apple Silicon)
curl -L https://github.com/mandrean/ferrokinesis/releases/latest/download/ferrokinesis-macos-arm64 -o ferrokinesis
curl -L https://github.com/mandrean/ferrokinesis/releases/latest/download/ferro-macos-arm64 -o ferro
chmod +x ferrokinesis ferro

# macOS (Intel)
curl -L https://github.com/mandrean/ferrokinesis/releases/latest/download/ferrokinesis-macos-amd64 -o ferrokinesis
curl -L https://github.com/mandrean/ferrokinesis/releases/latest/download/ferro-macos-amd64 -o ferro
chmod +x ferrokinesis ferro

# Linux (amd64)
curl -L https://github.com/mandrean/ferrokinesis/releases/latest/download/ferrokinesis-linux-amd64 -o ferrokinesis
curl -L https://github.com/mandrean/ferrokinesis/releases/latest/download/ferro-linux-amd64 -o ferro
chmod +x ferrokinesis ferro
```

### Cargo

```sh
cargo install ferrokinesis
```

The companion CLI ships in GitHub release assets and can also be installed from a checkout of this repository:

```sh
cargo install --path crates/ferro-cli --locked
```

To enable traffic mirroring, install with one of the mirror feature sets:

```sh
# Mirror with static env-var credentials only
cargo install ferrokinesis --features mirror

# Mirror with the AWS provider chain (~/.aws/credentials, ~/.aws/config,
# ECS task roles, EC2 IMDS, and IRSA/web identity)
cargo install ferrokinesis --features mirror-aws-config
```

### WASI Preview 2 (Experimental)

Build the experimental WASI listener binary:

```sh
cargo build --target wasm32-wasip2 --no-default-features --features wasi --bin ferrokinesis-wasi
```

Then run it in a Preview 2-capable runtime with TCP listening enabled, for example:

```sh
wasmtime run --wasi tcp-listen=0.0.0.0:4567 target/wasm32-wasip2/debug/ferrokinesis-wasi.wasm
```

The WASI binary is env-configured only. It currently targets the normal JSON/CBOR request path and health endpoints; `SubscribeToShard` remains unsupported there for now.

### Browser Demo

Build the browser demo and its generated WASM package:

```sh
npm ci --prefix demo
npm --prefix demo run build
```
### Docker

```sh
docker run -p 4567:4567 ghcr.io/mandrean/ferrokinesis
```

The container image includes both `/ferrokinesis` and `/ferro`.

## Quick Start

Start the server:

```sh
docker run -p 4567:4567 ghcr.io/mandrean/ferrokinesis
```

Example using `ferro`:

```sh
# Create a stream with 2 shards and wait until it is ACTIVE
ferro streams create example-stream --shards 2 --wait

# Publish a record
ferro put example-stream "hello world" --partition-key pk1

# Tail from the beginning and stop after one record
ferro tail example-stream --from trim-horizon --limit 1 --no-follow
```

The AWS CLI and SDK examples still work unchanged. See [`examples/aws-cli/quickstart.sh`](examples/aws-cli/quickstart.sh) for the raw AWS CLI flow, or browse the rest of [`examples/`](examples/) for Rust, Python, Node.js, Go, and Java clients.

## Usage

```
$ ferro --help

Companion CLI for ferrokinesis

Usage: ferro [OPTIONS] <COMMAND>

Commands:
  streams
  shards
  consumers
  put
  tail
  replay
  api
  health
  help       Print this message or the help of the given subcommand(s)

Options:
      --endpoint <ENDPOINT>  ferrokinesis endpoint [env: FERROKINESIS_ENDPOINT=] [default: http://127.0.0.1:4567]
      --insecure             Skip TLS certificate validation
      --json                 Print machine-readable JSON
      --ndjson               Print newline-delimited JSON when the command emits multiple records
  -h, --help                 Print help
```

`ferro --json tail ...` is for bounded reads and returns a JSON array. Use `--limit` or `--no-follow` to bound the run, and use `--ndjson` for streaming tails.

```
$ ferrokinesis --help

A local AWS Kinesis mock server for testing

Usage: ferrokinesis [OPTIONS]
       ferrokinesis <COMMAND>

Commands:
  serve         Start the mock Kinesis server (default when no subcommand is given)
  health-check  Run a health check against a running server (for Docker HEALTHCHECK)
  help          Print this message or the help of the given subcommand(s)

Options:
      --config <CONFIG>
          Path to a TOML configuration file [env: FERROKINESIS_CONFIG=]
      --port <PORT>
          The port to listen on [env: FERROKINESIS_PORT=]
      --account-id <ACCOUNT_ID>
          AWS account ID used in ARN generation (12-digit numeric) [env: AWS_ACCOUNT_ID=]
      --region <REGION>
          AWS region used in ARN generation and responses [env: AWS_REGION=]
      --create-stream-ms <CREATE_STREAM_MS>
          Amount of time streams stay in CREATING state (ms) [env: FERROKINESIS_CREATE_STREAM_MS=]
      --delete-stream-ms <DELETE_STREAM_MS>
          Amount of time streams stay in DELETING state (ms) [env: FERROKINESIS_DELETE_STREAM_MS=]
      --update-stream-ms <UPDATE_STREAM_MS>
          Amount of time streams stay in UPDATING state (ms) [env: FERROKINESIS_UPDATE_STREAM_MS=]
      --shard-limit <SHARD_LIMIT>
          Shard limit for error reporting [env: FERROKINESIS_SHARD_LIMIT=]
      --iterator-ttl-seconds <ITERATOR_TTL_SECONDS>
          Shard iterator time-to-live in seconds (minimum: 1, maximum: 86400)
          [env: FERROKINESIS_ITERATOR_TTL_SECONDS=]
      --max-request-body-mb <MAX_REQUEST_BODY_MB>
          Maximum request body size in megabytes (minimum: 1, maximum: 4096)
          [env: FERROKINESIS_MAX_REQUEST_BODY_MB=]
      --retention-check-interval-secs <RETENTION_CHECK_INTERVAL_SECS>
          Retention reaper interval in seconds (0 = disabled, maximum: 86400)
          [env: FERROKINESIS_RETENTION_CHECK_INTERVAL_SECS=]
  -h, --help
          Print help
```

## Capture And Replay

Capture write traffic from the server:

```sh
ferrokinesis --capture capture.ndjson
```

Replay that capture through the companion CLI:

```sh
ferro replay capture.ndjson --stream example-stream --speed max
```

## Mirror Credentials

The mirror CLI and TOML settings stay the same across builds, but credential resolution depends on the feature set you compile in.

- `--features mirror` keeps the lightweight path and reads only `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optional `AWS_SESSION_TOKEN`.
- `--features mirror-aws-config` enables the refreshable AWS credential provider chain for `~/.aws/credentials`, `~/.aws/config`, ECS task roles, EC2 IMDS, and IRSA/web identity.
- If no credentials are available at runtime, mirror requests are still forwarded, but they are sent unsigned.

## Health Check Endpoints

Three endpoints are available for container orchestration and monitoring:

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `GET /_health` | Aggregated health with component breakdown | `{"status":"UP","components":{"store":{"status":"UP"}}}` |
| `GET /_health/live` | Liveness probe — always 200 if the server is running | `OK` |
| `GET /_health/ready` | Readiness probe — checks store connectivity | `OK` or `503 Service Unavailable` |

The built-in `health-check` subcommand can be used for Docker `HEALTHCHECK`:

```dockerfile
HEALTHCHECK CMD ["ferrokinesis", "health-check"]
```

## API & Test Coverage

| Operation                        | Status | Notes                      |
|----------------------------------|--------|----------------------------|
| **Stream Management**            | ✅      |                            |
| CreateStream                     | ✅      |                            |
| DeleteStream                     | ✅      |                            |
| DescribeStream                   | ✅      |                            |
| DescribeStreamSummary            | ✅      |                            |
| ListStreams                      | ✅      |                            |
| UpdateStreamMode                 | ✅      | PROVISIONED / ON_DEMAND    |
| UpdateShardCount                 | ✅      | Uniform scaling            |
| **Data Operations**              | ✅      |                            |
| PutRecord                        | ✅      |                            |
| PutRecords                       | ✅      |                            |
| GetRecords                       | ✅      |                            |
| GetShardIterator                 | ✅      | All 5 iterator types       |
| SubscribeToShard                 | ✅      | Event stream over HTTP/1.1 |
| **Shard Management**             | ✅      |                            |
| ListShards                       | ✅      |                            |
| MergeShards                      | ✅      |                            |
| SplitShard                       | ✅      |                            |
| **Retention**                    | ✅      |                            |
| IncreaseStreamRetentionPeriod    | ✅      |                            |
| DecreaseStreamRetentionPeriod    | ✅      |                            |
| **Enhanced Fan-Out (Consumers)** | ✅      |                            |
| RegisterStreamConsumer           | ✅      |                            |
| DeregisterStreamConsumer         | ✅      |                            |
| DescribeStreamConsumer           | ✅      |                            |
| ListStreamConsumers              | ✅      |                            |
| **Monitoring**                   | ✅      |                            |
| EnableEnhancedMonitoring         | ✅      |                            |
| DisableEnhancedMonitoring        | ✅      |                            |
| DescribeLimits                   | ✅      |                            |
| DescribeAccountSettings          | ✅      |                            |
| UpdateAccountSettings            | ✅      |                            |
| **Encryption**                   | ✅      |                            |
| StartStreamEncryption            | ✅      |                            |
| StopStreamEncryption             | ✅      |                            |
| **Tagging (Stream-name)**        | ✅      |                            |
| AddTagsToStream                  | ✅      |                            |
| RemoveTagsFromStream             | ✅      |                            |
| ListTagsForStream                | ✅      |                            |
| **Tagging (ARN-based)**          | ✅      |                            |
| TagResource                      | ✅      |                            |
| UntagResource                    | ✅      |                            |
| ListTagsForResource              | ✅      |                            |
| **Resource Policies**            | ✅      |                            |
| PutResourcePolicy                | ✅      |                            |
| GetResourcePolicy                | ✅      |                            |
| DeleteResourcePolicy             | ✅      |                            |
| **Other**                        | ✅      |                            |
| UpdateStreamWarmThroughput       | ✅      |                            |
| UpdateMaxRecordSize              | ✅      |                            |

**39/39 operations implemented** (100%)

## TLS / HTTPS

ferrokinesis supports TLS when built with the `tls` feature:

### Install with TLS support

```sh
cargo install ferrokinesis --features tls
```

### Generate a self-signed certificate

```sh
ferrokinesis generate-cert
# Writes cert.pem and key.pem to the current directory

# Custom output paths and SANs:
ferrokinesis generate-cert \
    --cert-out /path/to/cert.pem \
    --key-out /path/to/key.pem \
    --san localhost --san 127.0.0.1 --san my-hostname
```

### Start the server with TLS

```sh
ferrokinesis --tls-cert cert.pem --tls-key key.pem
# Listening at https://0.0.0.0:4567
```

### Connecting with SDK clients

Runnable TLS examples for each SDK are in the [`examples/`](examples/) directory:

| SDK | Example |
|-----|---------|
| AWS CLI | [`examples/aws-cli/quickstart-tls.sh`](examples/aws-cli/quickstart-tls.sh) |
| Python (boto3) | [`examples/python/quickstart_tls.py`](examples/python/quickstart_tls.py) |
| Node.js (v3) | [`examples/node/quickstart-tls.mjs`](examples/node/quickstart-tls.mjs) |
| Go (v2) | [`examples/go/quickstart_tls.go`](examples/go/quickstart_tls.go) |
| Java (v2) | [`examples/java/src/main/java/example/QuickstartTls.java`](examples/java/src/main/java/example/QuickstartTls.java) |

## Building

```sh
cargo build --release
```

## Testing

```sh
cargo test
```

## Benchmarking

See [BENCHMARK.md](BENCHMARK.md) for details on running micro-benchmarks and load tests.

## Acknowledgements

Inspired by [kinesalite](https://github.com/mhart/kinesalite) by Michael Hart.

## License

MIT
