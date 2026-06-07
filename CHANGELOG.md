# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/manuschillerdev/absurd-rust-sdk/compare/v0.1.0...v0.2.0) - 2026-06-07

### Other

- Align sleep timing tests with SDK parity
- Add high-signal test parity coverage
- Complete Absurd SQL 0.4.0 parity hardening
- Merge pull request #7 from manuschillerdev/docs/readme
- Update README for current SDK API

## [0.1.0](https://github.com/manuschillerdev/absurd-rust-sdk/releases/tag/v0.1.0) - 2026-06-03

### Added

- Initial community Rust SDK for Absurd, built on Tokio and Absurd's Postgres stored procedures.
- Typed task registration and spawning with `serde`-backed parameters, task results, step results, and event payloads.
- Durable task context APIs for checkpointed steps, sleeps, event waits, event emission, and claim heartbeats.
- Queue lifecycle, task cancellation, cleanup, idempotent spawn, retry strategy, and unknown-task deferral APIs.
- Worker execution with configurable concurrency, batch size, claim timeout, polling interval, and graceful shutdown.
- Hello-world and worker examples for common task and workflow patterns.
- CI and release automation with rustfmt, clippy, tests, docs, packaging, cargo-deny, Dependabot, and release-plz.

### Fixed

- Aligned SDK semantics for cancellation mapping, retry/idempotency handling, lease extension, task completion, and worker deferral behavior.

### Tests

- Added integration coverage for queue lifecycle, typed task round trips, checkpoint reuse, failed-step re-execution, repeated step names, sleep resumption, event wakeups, idempotent spawn, retries, retry backoff, unknown tasks, and cancellation.
