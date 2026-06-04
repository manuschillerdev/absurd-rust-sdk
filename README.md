# absurd-rust-sdk

Community Rust SDK for [Absurd](https://github.com/earendil-works/absurd), a Postgres-native durable workflow system.

This repository is intentionally independent from upstream while the Rust API is tested and hardened.

## Status

Experimental community SDK. Implemented today:

- Typed task registration and spawning with `serde` parameters and results.
- One-shot batch execution and long-running Tokio workers.
- Durable task context helpers for checkpointed steps, sleeps, event waits, event emission, and claim heartbeats.
- Retry strategies, cancellation policy wiring, idempotent spawn, queue lifecycle, cleanup, and unknown-task handling.
- Database integration tests for queue lifecycle, typed tasks, checkpointing, sleeps, events, idempotency, retries, unknown tasks, and cancellation.

Still hardening:

- Additional parity coverage against upstream SDK behavior.
- Task-result polling APIs for Absurd SQL versions that expose `get_task_result`.
- Optional spawn/execution middleware for tracing and header propagation.
- Broader built-in TLS and pool configuration. Use `Client::from_pool` for custom pool setup today.

## Goals

- Async-only Tokio SDK.
- Typed task parameters, task results, step results, and event payloads through `serde`.
- No `Box::pin` in user code.
- Keep durable semantics in Absurd's Postgres stored procedures.
- Small API surface around `Client`, typed `Task`s, `TaskContext`, workers, and options builders.

## Prerequisites

- Rust 1.85+ and edition 2024.
- A Postgres database initialized with Absurd's SQL schema and stored procedures.
- A database connection through `ABSURD_DATABASE_URL`, `DATABASE_URL`, or `PGDATABASE`.

Add the crate and normal async/serialization dependencies:

```toml
[dependencies]
absurd-rust-sdk = "0.1"
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal", "time"] }
```

`Client::create_queue()` creates the queue tables inside an initialized Absurd schema; it does not install Absurd itself.

## Quick start

```rust
use absurd_rust_sdk::{Client, Result, Task, WorkBatchOptions};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize)]
struct Params {
    name: String,
}

#[derive(Debug, Serialize)]
struct Output {
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::from_env_queue("default").await?;
    client.create_queue().await?;

    let hello = Task::<Params, Output>::new("hello");

    client.register(&hello, |params, mut ctx| async move {
        let message: String = ctx
            .step("greet", || async move { Ok(format!("hello, {}", params.name)) })
            .await?;

        Ok(Output { message })
    })?;

    let spawned = client
        .spawn(
            &hello,
            Params { name: "Absurd".into() },
            Default::default(),
        )
        .await?;

    println!("spawned task {} run {}", spawned.task_id, spawned.run_id);

    client
        .work_batch(WorkBatchOptions::new().claim_timeout(Duration::from_secs(120)))
        .await?;

    Ok(())
}
```

Run the included examples with an initialized Absurd database:

```sh
ABSURD_DATABASE_URL=postgresql://localhost/absurd cargo run --example hello_world
ABSURD_DATABASE_URL=postgresql://localhost/absurd cargo run --example worker
```

## Workers

Use `work_batch` for one-shot processing and `start_worker` for a polling worker:

```rust
use absurd_rust_sdk::{Client, Result, WorkerOptions};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::from_env_queue("orders").await?;

    // Register tasks before starting the worker.

    let worker = client.start_worker(
        WorkerOptions::new()
            .concurrency(8)
            .claim_timeout(Duration::from_secs(120)),
    )?;

    tokio::signal::ctrl_c()
        .await
        .map_err(|err| absurd_rust_sdk::Error::message(err.to_string()))?;

    worker.close().await?;
    Ok(())
}
```

`WorkerOptions` also supports `worker_id`, `batch_size`, `poll_interval`, `unknown_task_policy`, and `fatal_on_lease_timeout`.

## Durable context API

Inside a task handler:

```rust
let task_id = ctx.task_id();
let run_id = ctx.run_id();
let attempt = ctx.attempt();
let headers = ctx.headers();

let value: MyStepResult = ctx
    .step("step-name", || async { Ok(do_side_effect_once().await?) })
    .await?;

ctx.sleep_for("cooldown", Duration::from_secs(60)).await?;
ctx.sleep_until("wake-at", wake_at).await?;

let event: MyEvent = ctx
    .await_event_with_options(
        "payment.confirmed",
        AwaitEventOptions::new()
            .step_name("wait-for-payment")
            .timeout(Duration::from_secs(60 * 60)),
    )
    .await?;

ctx.emit_event("order.done", &serde_json::json!({ "ok": true })).await?;
ctx.heartbeat().await?;
ctx.heartbeat_for(Duration::from_secs(120)).await?;
```

`step`, `sleep_for`/`sleep_until`, and `await_event`/`await_event_with_options` checkpoint their state in Postgres. If a task retries or resumes, completed checkpoints are loaded and not re-executed.

Keep checkpoint names and control flow deterministic. Repeated names in one execution are suffixed automatically (`name`, `name#2`, `name#3`, ...).

## Spawning and options

```rust
use absurd_rust_sdk::{CancellationPolicy, RetryStrategy, SpawnOptions};
use std::time::Duration;

let spawned = client
    .spawn(
        &task,
        params,
        SpawnOptions::new()
            .idempotency_key("daily-report:2026-06-04")
            .max_attempts(3)
            .retry_strategy(RetryStrategy::fixed(Duration::from_secs(30)))
            .cancellation(CancellationPolicy::new().max_duration(Duration::from_secs(300))),
    )
    .await?;

if !spawned.created {
    println!("idempotency key reused existing task {}", spawned.task_id);
}
```

Useful public types:

- `Client` / `Absurd`: connect, queue lifecycle, registration, spawning, events, cancellation, cleanup, and execution.
- `Task<P, R>` and `TaskOptions`: typed task names, queues, default attempts, and default cancellation.
- `SpawnOptions` and `SpawnResult`: queue override, attempts, retry strategy, headers, cancellation, idempotency, task/run IDs, and creation status.
- `TaskContext` and `AwaitEventOptions`: task metadata, checkpointed steps/sleeps/events, event timeouts, event emission, and heartbeats.
- `WorkBatchOptions`, `WorkerOptions`, and `UnknownTaskPolicy`: execution and worker behavior.
- `CleanupResult`, `Error`, `Result`, and `Json`.

## Database contract

The SDK calls Absurd stored procedures directly:

- `absurd.create_queue`
- `absurd.drop_queue`
- `absurd.list_queues`
- `absurd.spawn_task`
- `absurd.claim_task`
- `absurd.complete_run`
- `absurd.fail_run`
- `absurd.schedule_run`
- `absurd.set_task_checkpoint_state`
- `absurd.get_task_checkpoint_state`
- `absurd.get_task_checkpoint_states`
- `absurd.await_event`
- `absurd.emit_event`
- `absurd.extend_claim`
- `absurd.cancel_task`
- `absurd.cleanup_tasks`
- `absurd.cleanup_events`
- `absurd.current_time` for unknown-task deferral

Retry, cancellation, idempotency, event wakeups, leases, and cleanup remain database-owned.

## Configuration

`Client::from_env()` and `Client::from_env_queue()` resolve the database connection in this order:

1. `ABSURD_DATABASE_URL`
2. `DATABASE_URL`
3. `PGDATABASE`
4. `postgresql://localhost/absurd`

A `PGDATABASE` value that is not a URL or keyword connection string is treated as a database name (`dbname=...`). Built-in URL constructors currently use `NoTls`; use `Client::from_pool` to provide a custom `deadpool_postgres::Pool`.

## Development

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo deny check advisories bans licenses sources
cargo package
```

Database integration tests are ignored by default because they require a Postgres database initialized with Absurd SQL:

```sh
ABSURD_DATABASE_URL=postgresql://localhost/absurd_test cargo test --test integration -- --ignored
```

`Cargo.lock` is intentionally not committed because this is a library crate; CI resolves the current compatible dependency graph and Dependabot tracks manifest/action updates.

## License

Apache-2.0
