# Absurd SQL 0.4.0 SDK parity

## Compatibility statement

The Rust SDK has practical core parity with the Python, TypeScript, and Go SDKs for Absurd SQL `0.4.0`.

Validated target:

- Absurd SQL tag: `0.4.0`
- Absurd SQL commit: `05282a40c8dddc378acdc6933adc4c221583808a`
- Rust crate version: `0.1.0`

## Parity matrix

| Capability | Python SDK | TypeScript SDK | Go SDK | Rust SDK | Rust decision |
|---|---:|---:|---:|---:|---|
| Database schema target | Yes | Yes | Yes | Yes | Tested against Absurd SQL `0.4.0`. |
| Queue create, drop, and list | Yes | Yes | Yes | Yes | Equivalent behavior. |
| Partitioned queue creation | Yes | Yes | Yes | Yes | Equivalent behavior. |
| Queue policy get/set | Yes | Yes | Yes | Yes | Equivalent behavior. |
| Typed task registration | Dynamic | Generic | Generic/context | Generic | Rust uses `Task<P, R>` and typed handlers. |
| Spawn task | Yes | Yes | Yes | Yes | Equivalent behavior. |
| Spawn headers | Yes | Yes | Yes | Yes | Available through `SpawnOptions::headers`. |
| Idempotent spawn | Yes | Yes | Yes | Yes | Equivalent database behavior. |
| Retry strategy | Yes | Yes | Yes | Yes | Fixed, exponential, and none are supported. |
| Cancellation policy | Yes | Yes | Yes | Yes | Max delay and max duration are supported. |
| Worker batch execution | Yes | Yes | Yes | Yes | Rust uses Tokio async execution. |
| Continuous worker | Yes | Yes | Yes | Yes | `Worker::close().await` drains running tasks. |
| Worker error callback | Yes | Yes | Yes | Yes | `WorkerOptions::on_error` reports claim and execution errors. |
| Durable step checkpoint | Yes | Yes | Yes | Yes | `TaskContext::step` persists checkpoint state. |
| Decomposed steps | Yes | Yes | Yes | Yes | `begin_step` / `complete_step` support cached reuse and repeated names. |
| Sleep / scheduled resume | Yes | Yes | Yes | Yes | Equivalent database behavior. |
| Event emit / await | Yes | Yes | Yes | Yes | Equivalent database behavior, including timeout handling. |
| Task result fetch / await | Yes | Yes | Yes | Yes | Same-queue waits are rejected to avoid worker deadlocks. |
| Manual retry task | Yes | Yes | Yes | Yes | Default retry, max-attempt override, and spawn-new retry are supported. |
| Manual cancel task | Yes | Yes | Yes | Yes | Terminal-state races are treated as internal control flow. |
| Cleanup tasks/events | Yes | Yes | Yes | Yes | Equivalent database behavior. |
| Spawn / execution hooks | Yes | Yes | Yes | Yes | `Hooks` supports before-spawn and wrap-execution hooks. |
| Failure payload shape | Yes | Yes | Yes | Yes | Rust persists `name`, `message`, `debug`, and `traceback`. |
| Panic handling | Language-specific | Language-specific | Language-specific | Yes | Rust catches task panics and persists diagnostic payloads. |
| Raw task claim API | Yes | Yes | Yes | No | Intentionally internal in Rust. Use `work_batch` or `Worker` to preserve typed execution, hooks, sentinels, and diagnostics. |
| Sync client API | Yes | No | Blocking/context style | No | Intentionally async-only for Tokio and async Postgres. Use a Tokio runtime from sync applications. |
| Global/decorator task context | Yes | JavaScript idioms | No | No | Intentionally explicit `TaskContext` parameter. |
| Logger injection | Yes | Yes | Go idioms | No | Intentionally uses `tracing` and `WorkerOptions::on_error`. |
| Mandatory client close | Yes | Yes | Context/cancel | No | `Worker::close().await` is explicit; `Drop` requests shutdown through RAII. |
| Captured language traceback | Yes | Yes | Go stack behavior | No | Rust stores diagnostic `debug` detail and `traceback: null`; portable async traceback capture is not implemented. |

## Intentional Rust deviations

### Async-only API

Rust exposes an async Tokio API. This keeps database I/O, worker polling, durable sleeps, event waits, heartbeats, and shutdown in the same async model.

Synchronous applications should create or reuse a Tokio runtime and call `block_on` at their boundary.

### Explicit `TaskContext`

Task handlers receive `TaskContext` explicitly. This avoids global mutable task-local state and keeps helper functions honest about whether they need durable workflow operations.

### `tracing` and callback observability

Rust uses `tracing` for logs and `WorkerOptions::on_error` for claim/execution callbacks instead of logger injection.

### RAII worker lifecycle

`Worker::close().await` performs graceful shutdown and drains running tasks. Dropping a worker requests shutdown through RAII, but explicit close remains the recommended graceful path.

### Internal raw claiming

Raw task claiming is intentionally not public. Exposing low-level claims would let callers bypass registered handler execution, typed deserialization, hooks, internal control-flow sentinels, terminal-race handling, and failure diagnostics.

### Failure tracebacks

Rust failure payloads include `name`, `message`, `debug`, and `traceback`. `traceback` is currently `null`; exact stack/traceback parity is not portable for arbitrary async Rust futures.

## Validation suite

The version target is validated by the ignored database integration suite:

```sh
ABSURD_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/absurd_test cargo test-db
```

The suite covers:

- pending, completed, failed, cancelled, missing, and timeout task-result cases
- same-queue `TaskContext::await_task_result` rejection
- retry-task default, max-attempt override, spawn-new, missing-task, and non-failed-task cases
- decomposed step cached reuse, repeated names, and failed-step non-checkpointing
- worker drain-on-close, claim-error callbacks, execution-error callbacks, invalid headers, terminal races, and lease-timeout configuration
- user failure and panic diagnostic payloads

Final quality gates for this target:

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
ABSURD_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/absurd_test cargo test-db
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo package --allow-dirty
git diff --check
```

## Versioning policy

Rust crate semver describes Rust SDK API compatibility. It does not imply lockstep release numbering with Absurd SQL.

Absurd SQL compatibility is tracked through:

- this versioned parity document
- the CI `ABSURD_SQL_REF`
- release notes for each published crate version

Recommended release-note format:

```text
Absurd SQL compatibility: tested against 0.4.0; minimum supported SQL schema 0.4.0.
```

Future upstream Absurd SQL releases should get their own parity document, for example `docs/parity/absurd-sql-0.5.0.md`.

## Remaining implementation

No required implementation remains for Absurd SQL `0.4.0` core SDK parity.

Future work should track new upstream SQL or SDK surfaces as separate versioned parity targets.
