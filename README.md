# absurd-rust-sdk

Community Rust SDK for [Absurd](https://github.com/earendil-works/absurd), a Postgres-native durable workflow system.

This repository is intentionally independent from upstream while the Rust API is tested and hardened.

## Goals

- Async-only Tokio SDK.
- Typed task parameters, task results, step results, and event payloads through `serde`.
- No `Box::pin` in user code.
- Keep durable semantics in Absurd's Postgres stored procedures.
- Small API surface: client, task registry, worker, task context, spawn options.

## Quick start

```rust
use absurd_rust_sdk::{Client, Result, Task, WorkBatchOptions};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Deserialize, Serialize)]
struct Params {
    name: String,
}

#[derive(Serialize)]
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

    client
        .spawn(
            &hello,
            Params { name: "Absurd".into() },
            Default::default(),
        )
        .await?;

    client
        .work_batch(WorkBatchOptions::new().claim_timeout(Duration::from_secs(120)))
        .await?;
    Ok(())
}
```

## Durable context API

Inside a task handler:

```rust
let value: MyStepResult = ctx.step("step-name", || async {
    Ok(do_side_effect_once().await?)
}).await?;

ctx.sleep_for("cooldown", Duration::from_secs(60)).await?;

let event: MyEvent = ctx.await_event("payment.confirmed").await?;

ctx.emit_event("order.done", &serde_json::json!({ "ok": true })).await?;
ctx.heartbeat().await?;
```

Each step result is checkpointed in Postgres. If the task retries, completed steps are loaded from the checkpoint table and are not re-executed.

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
- `absurd.get_task_checkpoint_state(s)`
- `absurd.await_event`
- `absurd.emit_event`
- `absurd.extend_claim`
- `absurd.cancel_task`
- `absurd.cleanup_tasks`
- `absurd.cleanup_events`
- `absurd.current_time` for unknown-task deferral

Retry, cancellation, idempotency, event wakeups, leases, and cleanup remain database-owned.

## Current status

Experimental community SDK. The core typed API, worker, step/sleep/event helpers, cancellation mapping, idempotent spawn support, cleanup, and unknown-task deferral are implemented.

Next hardening targets:

- Integration tests ported from the Python and Go SDKs.
- Task-result polling APIs when targeting Absurd SQL versions that expose `get_task_result`.
- Optional spawn/execution middleware for tracing/header propagation.
- Broader TLS/pool configuration.

Environment resolution uses `ABSURD_DATABASE_URL`, then `DATABASE_URL`, then `PGDATABASE`, then `postgresql://localhost/absurd`.

## License

Apache-2.0
