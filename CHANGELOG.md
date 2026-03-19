# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/mandrean/ferrokinesis/compare/v0.1.2...v0.2.0) - 2026-03-19

### Added

- add ARN format regex validation ([#111](https://github.com/mandrean/ferrokinesis/pull/111))
- add TLS/HTTPS support behind `tls` feature flag ([#109](https://github.com/mandrean/ferrokinesis/pull/109))
- add KCL v1 integration test ([#106](https://github.com/mandrean/ferrokinesis/pull/106))
- add SubscribeToShard end-to-end SDK tests ([#107](https://github.com/mandrean/ferrokinesis/pull/107))
- add CBOR ↔ JSON equivalence tests and fix byte string handling ([#88](https://github.com/mandrean/ferrokinesis/pull/88))
- add Goose load test for kinesalite comparison ([#84](https://github.com/mandrean/ferrokinesis/pull/84))
- add retention period enforcement with TTL-based record trimming ([#87](https://github.com/mandrean/ferrokinesis/pull/87))
- add multi-language SDK conformance test suite ([#83](https://github.com/mandrean/ferrokinesis/pull/83))
- enforce 5 MB PutRecords batch limit and add stress tests ([#82](https://github.com/mandrean/ferrokinesis/pull/82))
- add criterion benchmark suite with CI regression gate ([#80](https://github.com/mandrean/ferrokinesis/pull/80))
- add configurable shard iterator TTL ([#40](https://github.com/mandrean/ferrokinesis/pull/40)) ([#77](https://github.com/mandrean/ferrokinesis/pull/77))
- add health check endpoints and CLI subcommand ([#72](https://github.com/mandrean/ferrokinesis/pull/72))
- configurable AWS account ID and region via CLI flags and config file ([#79](https://github.com/mandrean/ferrokinesis/pull/79))
- add configurable request body size limit ([#45](https://github.com/mandrean/ferrokinesis/pull/45)) ([#64](https://github.com/mandrean/ferrokinesis/pull/64))

### Fixed

- assert error before cancelling subscribeToShard subscription ([#118](https://github.com/mandrean/ferrokinesis/pull/118))
- error response conformance audit ([#89](https://github.com/mandrean/ferrokinesis/pull/89))
- return Kinesis-shaped error responses for 413 body size rejections ([#90](https://github.com/mandrean/ferrokinesis/pull/90))
- add fake SigV4 auth headers to Docker smoke test ([#81](https://github.com/mandrean/ferrokinesis/pull/81))

### Other

- document case count rationale and feature gate note for property tests
- address PR #117 review feedback on property tests
- address PR #117 review feedback on property tests
- add property-based tests with proptest ([#21](https://github.com/mandrean/ferrokinesis/pull/21))
- prevent Java SDK v2 subscribeToShard test from hanging ([#116](https://github.com/mandrean/ferrokinesis/pull/116))
- simplify conformance dependency caching ([#115](https://github.com/mandrean/ferrokinesis/pull/115))
- add concurrent stress tests and race condition detection ([#105](https://github.com/mandrean/ferrokinesis/pull/105))
- Add CLAUDE.md with architecture, patterns, and build commands ([#108](https://github.com/mandrean/ferrokinesis/pull/108))
- add shard splitting and merging correctness tests ([#110](https://github.com/mandrean/ferrokinesis/pull/110))
- adopt thiserror for typed error handling ([#78](https://github.com/mandrean/ferrokinesis/pull/78))
- harden Docker image and add Trivy security scanning ([#46](https://github.com/mandrean/ferrokinesis/pull/46)) ([#66](https://github.com/mandrean/ferrokinesis/pull/66))
- add Docker smoke test before image push ([#28](https://github.com/mandrean/ferrokinesis/pull/28)) ([#63](https://github.com/mandrean/ferrokinesis/pull/63))
- add cargo-audit and cargo-deny for supply chain security ([#47](https://github.com/mandrean/ferrokinesis/pull/47)) ([#65](https://github.com/mandrean/ferrokinesis/pull/65))
- pin Rust toolchain and add SHA256 checksums to releases ([#48](https://github.com/mandrean/ferrokinesis/pull/48)) ([#67](https://github.com/mandrean/ferrokinesis/pull/67))

## [0.1.2](https://github.com/mandrean/ferrokinesis/compare/v0.1.1...v0.1.2) - 2026-03-17

### Other

- add version tags to Docker images ([#14](https://github.com/mandrean/ferrokinesis/pull/14))

## [0.1.1](https://github.com/mandrean/ferrokinesis/compare/v0.1.0...v0.1.1) - 2026-03-17

### Added

- Add cross-compiled binary releases for 6 platforms

### Fixed

- remove Cargo.lock from .gitignore

### Other

- replace semantic-rs with release-plz for automated releases ([#12](https://github.com/mandrean/ferrokinesis/pull/12))
- add .clog.toml ([#11](https://github.com/mandrean/ferrokinesis/pull/11))
- use semantic-rs binary for automated releases ([#10](https://github.com/mandrean/ferrokinesis/pull/10))
- replace release-plz with semantic-rs for automated releases ([#9](https://github.com/mandrean/ferrokinesis/pull/9))
- Use more enums, constants ([#8](https://github.com/mandrean/ferrokinesis/pull/8))
- Improve test coverage ([#6](https://github.com/mandrean/ferrokinesis/pull/6))
- Add quick start example to README ([#4](https://github.com/mandrean/ferrokinesis/pull/4))
- Fix clippy warnings (collapsible-if, allows for structural patterns)
