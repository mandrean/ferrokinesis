# ferrokinesis

A local AWS Kinesis mock server for testing, written in Rust.

## Features

- Pure Rust implementation
- Uses [redb](https://github.com/cberner/redb) for in-memory storage (ACID, zero-copy reads)
- Implements 38 of 39 Kinesis Data Streams API operations
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

## Usage

```sh
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

Then point your AWS SDK at `http://localhost:4567`:

```rust
let config = aws_config::defaults(BehaviorVersion::latest())
    .endpoint_url("http://localhost:4567")
    .load()
    .await;
let client = aws_sdk_kinesis::Client::new(&config);
client.list_streams().send().await?;
```

## API Coverage

| Operation | Status | Notes |
|-----------|--------|-------|
| **Stream Management** | | |
| CreateStream | ✅ | |
| DeleteStream | ✅ | |
| DescribeStream | ✅ | |
| DescribeStreamSummary | ✅ | |
| ListStreams | ✅ | |
| UpdateStreamMode | ✅ | PROVISIONED / ON_DEMAND |
| UpdateShardCount | ✅ | Uniform scaling |
| **Data Operations** | | |
| PutRecord | ✅ | |
| PutRecords | ✅ | |
| GetRecords | ✅ | |
| GetShardIterator | ✅ | All 5 iterator types |
| SubscribeToShard | ❌ | Requires HTTP/2 push |
| **Shard Management** | | |
| ListShards | ✅ | |
| MergeShards | ✅ | |
| SplitShard | ✅ | |
| **Retention** | | |
| IncreaseStreamRetentionPeriod | ✅ | |
| DecreaseStreamRetentionPeriod | ✅ | |
| **Enhanced Fan-Out (Consumers)** | | |
| RegisterStreamConsumer | ✅ | |
| DeregisterStreamConsumer | ✅ | |
| DescribeStreamConsumer | ✅ | |
| ListStreamConsumers | ✅ | |
| **Monitoring** | | |
| EnableEnhancedMonitoring | ✅ | |
| DisableEnhancedMonitoring | ✅ | |
| DescribeLimits | ✅ | |
| DescribeAccountSettings | ✅ | |
| UpdateAccountSettings | ✅ | |
| **Encryption** | | |
| StartStreamEncryption | ✅ | |
| StopStreamEncryption | ✅ | |
| **Tagging (Stream-name)** | | |
| AddTagsToStream | ✅ | |
| RemoveTagsFromStream | ✅ | |
| ListTagsForStream | ✅ | |
| **Tagging (ARN-based)** | | |
| TagResource | ✅ | |
| UntagResource | ✅ | |
| ListTagsForResource | ✅ | |
| **Resource Policies** | | |
| PutResourcePolicy | ✅ | |
| GetResourcePolicy | ✅ | |
| DeleteResourcePolicy | ✅ | |
| **Other** | | |
| UpdateStreamWarmThroughput | ✅ | |
| UpdateMaxRecordSize | ✅ | |

**38/39 operations implemented** (97%)

## Building

```sh
cargo build --release
```

## Testing

```sh
cargo test
```

## Acknowledgements

Inspired by [kinesalite](https://github.com/mhart/kinesalite) by Michael Hart.

## License

MIT
