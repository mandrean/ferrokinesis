# ferrokinesis

A local AWS Kinesis mock server for testing, written in Rust.

## Features

- Pure Rust implementation
- Uses [redb](https://github.com/cberner/redb) for in-memory storage (ACID, zero-copy reads)
- Implements 17 Kinesis API operations
- Supports both JSON and CBOR content types
- 130+ integration tests

## Supported Operations

- CreateStream / DeleteStream / DescribeStream / DescribeStreamSummary
- ListStreams / ListShards
- PutRecord / PutRecords / GetRecords / GetShardIterator
- SplitShard / MergeShards
- IncreaseStreamRetentionPeriod / DecreaseStreamRetentionPeriod
- AddTagsToStream / RemoveTagsFromStream / ListTagsForStream

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
