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

# Publish a record to the stream
aws kinesis put-record \
    --stream-name example-stream \
    --partition-key pk1 \
    --data $(echo "hello world" | base64) \
    --endpoint-url http://localhost:4567 \
    --region us-east-1

# Read all records across all shards:
aws kinesis list-shards \
    --stream-name example-stream \
    --endpoint-url http://localhost:4567 \
    --region us-east-1 \
  | jq -r '.Shards[].ShardId' \
  | while read shard; do
      iter=$(aws kinesis get-shard-iterator \
        --stream-name example-stream --shard-id "$shard" \
        --shard-iterator-type TRIM_HORIZON \
        --endpoint-url http://localhost:4567 \
        --region us-east-1 \
        | jq -r '.ShardIterator')
      aws kinesis get-records \
        --shard-iterator "$iter" \
        --endpoint-url http://localhost:4567 \
        --region us-east-1
    done \
  | jq -s '[.[].Records[] | {SequenceNumber, Data, DataDecoded: (.Data | @base64d), PartitionKey}]'
```

Outputs:
```json
[
  {
    "SequenceNumber": "49672753465885973963712545055001942108658846086845169682",
    "Data": "aGVsbG8gd29ybGQK",
    "DataDecoded": "hello world\n",
    "PartitionKey": "pk1"
  }
]
```

Example using AWS SDK for Rust:

```rust
let config = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url("http://localhost:4567")
    .load()
    .await;
let client = aws_sdk_kinesis::Client::new(&config);

// Create a stream with 2 shards
client.create_stream()
    .stream_name("example-stream")
    .shard_count(2)
    .send().await?;

// Publish a record
client.put_record()
    .stream_name("example-stream")
    .partition_key("pk1")
    .data(Blob::new("hello world\n"))
    .send().await?;

// Read all records across all shards
let shards = client.list_shards()
    .stream_name("example-stream")
    .send().await?;

for shard in shards.shards() {
    let iter = client.get_shard_iterator()
        .stream_name("example-stream")
        .shard_id(shard.shard_id())
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send().await?;

    let resp = client.get_records()
        .shard_iterator(iter.shard_iterator().unwrap())
        .send().await?;

    for record in resp.records() {
        let data = std::str::from_utf8(record.data().as_ref()).unwrap();
        println!("{}: {}", record.partition_key(), data);
    }
}
// => pk1: hello world
```

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

### Connecting with AWS CLI

```sh
aws kinesis list-streams \
    --endpoint-url https://localhost:4567 \
    --no-verify-ssl \
    --region us-east-1
```

### Connecting with AWS SDK for Python (boto3)

```python
import boto3

client = boto3.client('kinesis',
    endpoint_url='https://localhost:4567',
    verify=False,            # or verify='/path/to/cert.pem'
    region_name='us-east-1',
)
```

### Connecting with AWS SDK for Node.js (v3)

```js
import { KinesisClient } from "@aws-sdk/client-kinesis";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import https from "https";

const client = new KinesisClient({
  endpoint: "https://localhost:4567",
  region: "us-east-1",
  requestHandler: new NodeHttpHandler({
    httpsAgent: new https.Agent({ rejectUnauthorized: false }),
  }),
});
```

### Connecting with AWS SDK for Go (v2)

```go
import (
    "crypto/tls"
    "net/http"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/kinesis"
)

cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithHTTPClient(&http.Client{
        Transport: &http.Transport{
            TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
        },
    }),
)
client := kinesis.NewFromConfig(cfg, func(o *kinesis.Options) {
    o.BaseEndpoint = aws.String("https://localhost:4567")
})
```

### Connecting with AWS SDK for Java (v2)

```java
import software.amazon.awssdk.services.kinesis.KinesisClient;
import java.net.URI;

KinesisClient client = KinesisClient.builder()
    .endpointOverride(URI.create("https://localhost:4567"))
    .region(Region.US_EAST_1)
    // For self-signed certs, configure a custom TrustManager
    .build();
```

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
