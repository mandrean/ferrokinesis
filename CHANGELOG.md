# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0](https://github.com/mandrean/ferrokinesis/compare/v0.6.0...v0.7.0) - 2026-04-12

### Added

- add ferro companion CLI ([#153](https://github.com/mandrean/ferrokinesis/pull/153)) ([#226](https://github.com/mandrean/ferrokinesis/pull/226))
- *(observability)* bootstrap JSON logs and optional OTLP trace export ([#212](https://github.com/mandrean/ferrokinesis/pull/212))
- add durable single-node state mode ([#203](https://github.com/mandrean/ferrokinesis/pull/203))

### Other

- parallelize check workflow ([#225](https://github.com/mandrean/ferrokinesis/pull/225))
- clarify mirror credential feature flags ([#214](https://github.com/mandrean/ferrokinesis/pull/214))
- clarify mirror credential feature flags

## [0.6.0](https://github.com/mandrean/ferrokinesis/compare/v0.5.0...v0.6.0) - 2026-03-24

### Added

- *(wasm)* Add GitHub Pages browser demo ([#193](https://github.com/mandrean/ferrokinesis/pull/193))
- Add experimental WASI TCP listener binary ([#192](https://github.com/mandrean/ferrokinesis/pull/192))
- Add WASM wrapper and runtime split ([#191](https://github.com/mandrean/ferrokinesis/pull/191))

### Fixed

- exclude ferrokinesis-wasm from release-plz publishing

### Other

- Add cross-format CBOR/JSON property tests ([#194](https://github.com/mandrean/ferrokinesis/pull/194))
- eliminate BigUint heap allocations on write hot path ([#190](https://github.com/mandrean/ferrokinesis/pull/190))
- use v{version} format for GitHub release name ([#188](https://github.com/mandrean/ferrokinesis/pull/188))

## [0.5.0](https://github.com/mandrean/ferrokinesis/compare/v0.4.0...v0.5.0) - 2026-03-20

### Added

- use aws-config credential provider chain in mirror ([#180](https://github.com/mandrean/ferrokinesis/pull/180))

### Fixed

- reorder Docker tags so GHCR shows version in install instruction
- use correct release-plz field name git_tag_name
- move tag_name_template to [workspace] level
- use v{version} tag format for ferrokinesis releases

### Other

- per-shard lock-free concurrent store (replace redb) ([#184](https://github.com/mandrean/ferrokinesis/pull/184))
- single-pass CBOR serialization via BlobAwareValue ([#185](https://github.com/mandrean/ferrokinesis/pull/185))
- move capture write into put_record/put_records handlers ([#179](https://github.com/mandrean/ferrokinesis/pull/179))

## [0.4.0](https://github.com/mandrean/ferrokinesis/compare/ferrokinesis-v0.3.0...ferrokinesis-v0.4.0) - 2026-03-20

### Added

- add transparent traffic mirroring to real AWS ([#165](https://github.com/mandrean/ferrokinesis/pull/165))
- add stream capture and replay ([#163](https://github.com/mandrean/ferrokinesis/pull/163))
- add TRACE-level logging to all action handlers ([#168](https://github.com/mandrean/ferrokinesis/pull/168))

### Fixed

- correct SHA256SUMS generation in release workflow ([#173](https://github.com/mandrean/ferrokinesis/pull/173))
- split release workflow so artifacts only build on actual releases ([#164](https://github.com/mandrean/ferrokinesis/pull/164))

### Other

- add KCL 2.x enhanced fan-out integration test ([#141](https://github.com/mandrean/ferrokinesis/pull/141))
- unify workspace versioning and suppress spurious ferrokinesis-core releases ([#176](https://github.com/mandrean/ferrokinesis/pull/176))
- extract ferrokinesis-core no_std crate ([#166](https://github.com/mandrean/ferrokinesis/pull/166))

### Other

- bump `ferrokinesis-core` from `0.1.0` → `0.3.0` to align with workspace version (intentional jump; no intermediate releases)

## [0.3.0](https://github.com/mandrean/ferrokinesis/compare/v0.2.1...v0.3.0) - 2026-03-20

### Added

- structured & configurable logging via tracing ecosystem ([#159](https://github.com/mandrean/ferrokinesis/pull/159))
- add --host flag to health-check subcommand ([#136](https://github.com/mandrean/ferrokinesis/pull/136))
- add constants enforcement hook for action handlers ([#135](https://github.com/mandrean/ferrokinesis/pull/135))
- add graceful shutdown for plain HTTP serve path ([#126](https://github.com/mandrean/ferrokinesis/pull/126))

### Other

- centralize config defaults and add example config ([#162](https://github.com/mandrean/ferrokinesis/pull/162))
- add `cargo test --doc` to CI ([#161](https://github.com/mandrean/ferrokinesis/pull/161))
- update README to reflect current feature set ([#139](https://github.com/mandrean/ferrokinesis/pull/139))
- zero-copy record path optimization ([#132](https://github.com/mandrean/ferrokinesis/pull/132))
- add inline comments explaining protocol quirks and invariants ([#137](https://github.com/mandrean/ferrokinesis/pull/137))
- add rustdoc coverage and #![warn(missing_docs)] ([#138](https://github.com/mandrean/ferrokinesis/pull/138))
- add property-based tests for sequence roundtrip and error paths ([#130](https://github.com/mandrean/ferrokinesis/pull/130))
- expand conformance tests to cover all supported Kinesis operations ([#128](https://github.com/mandrean/ferrokinesis/pull/128))
- move SDK examples from README to examples/ directory ([#131](https://github.com/mandrean/ferrokinesis/pull/131))
- add CODEOWNERS ([#127](https://github.com/mandrean/ferrokinesis/pull/127))

## [0.2.1](https://github.com/mandrean/ferrokinesis/compare/v0.1.2...v0.2.1) - 2026-03-19

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

- add property-based tests with proptest ([#123](https://github.com/mandrean/ferrokinesis/pull/123))
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
