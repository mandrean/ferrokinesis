# ferrokinesis

A local AWS Kinesis emulator written in Rust (edition 2024). Aims to exactly match real Kinesis behavior.

## Architecture

HTTP POST → `src/server.rs` parses `X-Amz-Target` header → `Operation` enum → deserialize JSON/CBOR body → `check_types()` validates field types → `check_validations()` validates constraints → `dispatch()` routes to `src/actions/*.rs::execute()` → `Store` (redb) → JSON/CBOR response.

## Action handler contract

All 39 handlers follow the same signature and pattern:

```rust
pub async fn execute(store: &Store, data: Value) -> Result<Option<Value>, KinesisErrorResponse>
```

- Extract fields via `data[constants::FIELD_NAME]` — **never bare string literals**
- `store.resolve_stream_name(&data)?` for operations supporting both StreamName and StreamARN
- `store.update_stream(&name, |stream| { ... Ok(()) })` for mutations
- Return `Ok(None)` for empty 200, `Ok(Some(json!({...})))` for response bodies
- Errors via `KinesisErrorResponse::client_error(constants::ERROR_TYPE, Some("message"))`

## Adding a new operation (four-site checklist)

The compiler enforces exhaustive matches — **never use wildcard `_` arms in `Operation` match blocks**:

1. `Operation` enum variant in `src/actions/mod.rs`
2. `FromStr` mapping in `src/actions/mod.rs`
3. `dispatch()` arm in `src/actions/mod.rs`
4. Validation rules in `src/validation/rules.rs` + `get_validation_rules()` arm in `src/server.rs`

## Constants discipline

All JSON field names, error types, and content types live in `src/constants.rs`. New constants go there — never inline strings in action handlers.

## Type safety

Serde enums in `src/types.rs` (`EncryptionType`, `StreamMode`, `ShardIteratorType`, `StreamStatus`, etc.) — never string comparisons. All domain enum matches must be exhaustive (no wildcards).

## Error message casing

Kinesis uses **dual casing** for the message field — this is intentional, not a bug:

- `KinesisErrorResponse::client_error()` → lowercase `"message"` (action/dispatch errors)
- `KinesisErrorResponse::serialization_error()` → uppercase `"Message"` (type check errors)
- `KinesisErrorResponse::validation_error()` → lowercase `"message"` (constraint errors)

The `KinesisError` struct has both `message` and `message_upper` (serialized as `"Message"`) fields.

## Testing

Tests live in `tests/{operation}.rs`. Pattern:

```rust
mod common;
use common::*;

#[tokio::test]
async fn test_name() {
    let server = TestServer::new().await;
    server.create_stream("name", 3).await;
    let resp = server.request("Kinesis_20131202.DescribeStream", &json!({...})).await;
    assert_eq!(resp.status(), 200);
    let (status, body) = decode_body(resp).await;
    assert_eq!(status, 200);
    assert_eq!(body["StreamDescription"]["StreamName"], "name");
}
```

- `TestServer::new().await` for defaults, `TestServer::with_options(...)` for custom config
- Helpers: `create_stream()`, `request()`, `raw_request()`, `put_record()`, `get_shard_iterator()`, `get_records()`, `get_stream_arn()`, `describe_stream()`
- Assert both status code and body fields; errors assert `body["__type"]`

## Build commands

```sh
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings  # pre-commit (enforced by hook)
cargo test                          # integration + unit tests
cargo bench --bench kinesis_api     # CI gates on >10% regression
cargo cov                           # alias: llvm-cov --summary-only
cargo cov-html                      # alias: llvm-cov --open (HTML report)
```

## Do-not-touch zones

- **`src/sequence.rs`** (324 LOC) — bigint sequence number logic ported from kinesalite. Modify with extreme care.
- **`src/shard_iterator.rs`** (114 LOC) — AES-256-CBC encrypted iterator tokens with hardcoded keys matching kinesalite.

## Conformance tests

Multi-language SDK conformance tests in `tests/conformance/{go-sdk-v2,python-boto3,node-sdk-v3,java-sdk-v1,java-sdk-v2}`. SDK clients serialize differently — always check conformance coverage when modifying an operation.

## Commit messages

Use conventional commit prefixes: `feat:`, `fix:`, `chore:`, `test:`, `docs:`. Include PR number in parentheses when applicable, e.g. `feat: add feature X (#42)`.
