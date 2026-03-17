# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/mandrean/ferrokinesis/releases/tag/v0.1.0) - 2026-03-17

### Added

- Add cross-compiled binary releases for 6 platforms
- Add SDK integration tests, release profile, CI/CD workflows, and Dockerfile
- Implement SubscribeToShard with event stream encoding
- Add UpdateStreamWarmThroughput and UpdateMaxRecordSize operations
- Add TagResource, UntagResource, ListTagsForResource, DescribeAccountSettings, UpdateAccountSettings
- Add 14 API operations for consumers, encryption, monitoring, and policies
- Implement core Kinesis mock server with 17 operations

### Fixed

- Persist account settings to store

### Other

- Fix clippy warnings (collapsible-if, allows for structural patterns)
- Apply rustfmt formatting and restore bytes dependency
- Extract string literals into constants module
- Init repo
