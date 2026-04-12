# ferrokinesis examples

Runnable examples showing how to use ferrokinesis with various AWS SDKs.

If you want a lighter local workflow, the release assets also ship the `ferro` companion CLI for creating streams, putting records, tailing traffic, and replaying captures against the same local server.

## Prerequisites

1. A running ferrokinesis instance:
   ```sh
   docker run -p 4567:4567 ghcr.io/mandrean/ferrokinesis
   ```

2. The relevant SDK or runtime for the example you want to run.

## Lockfiles

`Cargo.lock`, `go.sum`, and `package-lock.json` are committed so that `cargo run`, `go run`,
and `npm start` produce reproducible builds without a separate install step.

## Examples

| Language | File | Description |
|----------|------|-------------|
| AWS CLI | [aws-cli/quickstart.sh](aws-cli/quickstart.sh) | Create stream, put/get records |
| AWS CLI | [aws-cli/quickstart-tls.sh](aws-cli/quickstart-tls.sh) | Same flow over TLS |
| Rust | [rust/src/main.rs](rust/src/main.rs) | Full flow with `aws-sdk-kinesis` |
| Python | [python/quickstart.py](python/quickstart.py) | Full flow with `boto3` |
| Python | [python/quickstart_tls.py](python/quickstart_tls.py) | Same flow over TLS |
| Node.js | [node/quickstart.mjs](node/quickstart.mjs) | Full flow with `@aws-sdk/client-kinesis` |
| Node.js | [node/quickstart-tls.mjs](node/quickstart-tls.mjs) | Same flow over TLS |
| Go | [go/quickstart.go](go/quickstart.go) | Full flow with `aws-sdk-go-v2` |
| Go | [go/quickstart_tls.go](go/quickstart_tls.go) | Same flow over TLS |
| Java | [java/src/main/java/example/Quickstart.java](java/src/main/java/example/Quickstart.java) | Full flow with AWS SDK v2 |
| Java | [java/src/main/java/example/QuickstartTls.java](java/src/main/java/example/QuickstartTls.java) | Same flow over TLS |

## TLS examples

The TLS examples require ferrokinesis built with `--features tls` and a self-signed certificate:

```sh
cargo install ferrokinesis --features tls
ferrokinesis generate-cert
ferrokinesis --tls-cert cert.pem --tls-key key.pem
```

See the [TLS section](../README.md#tls--https) in the main README for details.
