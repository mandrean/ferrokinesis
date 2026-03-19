# ferrokinesis

[![CI](https://github.com/mandrean/ferrokinesis/actions/workflows/ci.yml/badge.svg)](https://github.com/mandrean/ferrokinesis/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mandrean/ferrokinesis/branch/main/graph/badge.svg)](https://codecov.io/gh/mandrean/ferrokinesis)
[![crates.io](https://img.shields.io/crates/v/ferrokinesis.svg)](https://crates.io/crates/ferrokinesis)
[![docs.rs](https://docs.rs/ferrokinesis/badge.svg)](https://docs.rs/ferrokinesis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A local AWS Kinesis mock server for testing, written in Rust.

## Features

- Pure Rust implementation
- Uses [redb](https://github.com/cberner/redb) for in-memory storage (ACID, zero-copy reads)
- Implements all 39 Kinesis Data Streams API operations
- Supports both JSON and CBOR content types
- 125+ integration tests

## Installation

### Binary

Download pre-built binaries from [GitHub Releases](https://github.com/mandrean/ferrokinesis/releases):

```sh
# macOS (Apple Silicon)
curl -L https://github.com/mandrean/ferrokinesis/releases/latest/download/ferrokinesis-macos-arm64 -o ferrokinesis
chmod +x ferrokinesis

# macOS (Intel)
curl -L https://github.com/mandrean/ferrokinesis/releases/latest/download/ferrokinesis-macos-amd64 -o ferrokinesis
chmod +x ferrokinesis

# Linux (amd64)
curl -L https://github.com/mandrean/ferrokinesis/releases/latest/download/ferrokinesis-linux-amd64 -o ferrokinesis
chmod +x ferrokinesis
```

### Cargo

```sh
cargo install ferrokinesis
```

### Docker

```sh
docker run -p 4567:4567 ghcr.io/mandrean/ferrokinesis
```

## Quick Start

Start the server:

```sh
docker run -p 4567:4567 ghcr.io/mandrean/ferrokinesis
```

Example using `aws` CLI:

```sh
# Create a stream with 2 shards
aws kinesis create-stream \
    --stream-name example-stream \
    --shard-count 2 \
    --endpoint-url http://localhost:4567 \
    --region us-east-1

# Publish a record
aws kinesis put-record \
    --stream-name example-stream \
    --partition-key pk1 \
    --data $(echo -n "hello world" | base64) \
    --endpoint-url http://localhost:4567 \
    --region us-east-1
```

See the full example with get-records and cleanup: [`examples/aws-cli/quickstart.sh`](examples/aws-cli/quickstart.sh)

More examples using [Rust](examples/rust/src/main.rs), [Python](examples/python/quickstart.py), [Node.js](examples/node/quickstart.mjs), [Go](examples/go/quickstart.go), and [Java](examples/java/src/main/java/example/Quickstart.java) are available in the [`examples/`](examples/) directory.

## Usage

```
ferrokinesis --help

A local AWS Kinesis mock server for testing

Options:
  -p, --port <PORT>                      Port to listen on [default: 4567]
      --create-stream-ms <MS>            Time streams stay in CREATING state [default: 500]
      --delete-stream-ms <MS>            Time streams stay in DELETING state [default: 500]
      --update-stream-ms <MS>            Time streams stay in UPDATING state [default: 500]
      --shard-limit <LIMIT>              Shard limit for error reporting [default: 10]
  -h, --help                             Print help
  -V, --version                          Print version
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
