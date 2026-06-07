//! Database integration tests for the Absurd Rust SDK.
//!
//! These tests are ignored by default because they require Postgres initialized
//! with Absurd SQL. Run them locally with `ABSURD_DATABASE_URL=... cargo test-db`.

use absurd_rust_sdk::{
    AwaitEventOptions, AwaitTaskResultOptions, CancellationPolicy, Client, CreateQueueOptions,
    Error, Hooks, QueueDetachMode, QueuePolicyOptions, QueueStorageMode, Result, RetryStrategy,
    RetryTaskOptions, SpawnOptions, Task, TaskResultSnapshot, UnknownTaskPolicy, WorkBatchOptions,
    WorkerErrorKind, WorkerOptions,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Params {
    value: i64,
}

#[derive(Debug, Serialize)]
struct Output {
    doubled: i64,
}

#[derive(Debug)]
struct TaskRow {
    state: String,
    attempts: i32,
    max_attempts: Option<i32>,
    completed_payload: Option<Value>,
    cancelled_at: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct RunRow {
    state: String,
    wake_event: Option<String>,
    available_at: Option<DateTime<Utc>>,
    failure_reason: Option<Value>,
}

#[derive(Debug)]
struct TaskSpawnMetadata {
    retry_strategy: Option<Value>,
    cancellation: Option<Value>,
}

fn database_url() -> String {
    std::env::var("ABSURD_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql://localhost/absurd_test".to_string())
}

fn random_queue() -> String {
    format!("rs_{}", uuid::Uuid::new_v4().simple())
}

async fn test_client() -> Result<(String, Client)> {
    let queue = random_queue();
    let client = Client::connect_queue(database_url(), &queue).await?;
    client.create_queue().await?;
    Ok((queue, client))
}

async fn test_client_with_single_connection_pool()
-> Result<(String, Client, deadpool_postgres::Pool)> {
    let queue = random_queue();
    let (client, pool) = connect_queue_with_single_connection_pool(&queue).await?;
    client.create_queue().await?;
    Ok((queue, client, pool))
}

async fn connect_queue_with_single_connection_pool(
    queue: &str,
) -> Result<(Client, deadpool_postgres::Pool)> {
    let mut config = deadpool_postgres::Config::new();
    config.url = Some(database_url());
    config.pool = Some(deadpool_postgres::PoolConfig::new(1));
    let pool = config
        .create_pool(
            Some(deadpool_postgres::Runtime::Tokio1),
            tokio_postgres::NoTls,
        )
        .map_err(|err| Error::Config(format!("failed to create Postgres pool: {err}")))?;

    let connection = pool.get().await?;
    drop(connection);

    Ok((Client::from_pool(pool.clone(), queue)?, pool))
}

fn utc(timestamp: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(timestamp)
        .map_err(|err| Error::message(format!("invalid timestamp {timestamp:?}: {err}")))?
        .with_timezone(&Utc))
}

async fn set_fake_now(
    pool: &deadpool_postgres::Pool,
    timestamp: Option<DateTime<Utc>>,
) -> Result<()> {
    let pg = pool.get().await?;
    let value = timestamp
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_default();
    pg.execute("SELECT set_config('absurd.fake_now', $1, false)", &[&value])
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn queue_create_list_drop_round_trip() -> Result<()> {
    let queue = random_queue();
    let client = Client::connect_queue(database_url(), &queue).await?;

    client.create_queue().await?;
    assert!(client.list_queues().await?.contains(&queue));
    assert_eq!(queue_table_count(&queue).await?, 5);

    client.drop_queue().await?;
    assert!(!client.list_queues().await?.contains(&queue));
    assert_eq!(queue_table_count(&queue).await?, 0);

    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn partitioned_queue_create_round_trip() -> Result<()> {
    let queue = random_queue();
    let client = Client::connect_queue(database_url(), &queue).await?;

    client
        .create_queue_with_options(
            CreateQueueOptions::new().storage_mode(QueueStorageMode::Partitioned),
        )
        .await?;
    assert!(client.list_queues().await?.contains(&queue));

    let relkinds = queue_relation_kinds(&queue).await?;
    for prefix in ["t", "r", "c", "w"] {
        assert_eq!(
            relkinds
                .get(&format!("{prefix}_{queue}"))
                .map(String::as_str),
            Some("p")
        );
    }
    for prefix in ["e", "i"] {
        assert_eq!(
            relkinds
                .get(&format!("{prefix}_{queue}"))
                .map(String::as_str),
            Some("r")
        );
    }

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn queue_policy_round_trip() -> Result<()> {
    let queue = random_queue();
    let client = Client::connect_queue(database_url(), &queue).await?;

    client
        .create_queue_with_options(
            CreateQueueOptions::new()
                .storage_mode(QueueStorageMode::Partitioned)
                .partition_lookahead("35 days")
                .partition_lookback("2 days")
                .cleanup_ttl("12345 seconds")
                .cleanup_limit(77)
                .detach_mode(QueueDetachMode::Empty)
                .detach_min_age("45 days"),
        )
        .await?;

    let policy = client
        .get_queue_policy()
        .await?
        .ok_or_else(|| Error::message("expected queue policy"))?;
    assert_eq!(policy.queue_name, queue);
    assert_eq!(policy.storage_mode, QueueStorageMode::Partitioned);
    assert_eq!(policy.partition_lookahead, "35 days");
    assert_eq!(policy.partition_lookback, "2 days");
    assert!(policy.cleanup_ttl.ends_with("3:25:45"));
    assert_eq!(policy.cleanup_limit, 77);
    assert_eq!(policy.detach_mode, QueueDetachMode::Empty);
    assert_eq!(policy.detach_min_age, "45 days");

    client
        .set_queue_policy(
            QueuePolicyOptions::new()
                .cleanup_ttl("4321 seconds")
                .cleanup_limit(12),
        )
        .await?;

    let updated = client
        .get_queue_policy()
        .await?
        .ok_or_else(|| Error::message("expected updated queue policy"))?;
    assert!(updated.cleanup_ttl.ends_with("1:12:01"));
    assert_eq!(updated.cleanup_limit, 12);

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn basic_typed_task_round_trip() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<Params, Output>::new("double").queue(&queue);
    client.register(&task, |params, mut ctx| async move {
        let doubled = ctx
            .step("double", || async move { Ok(params.value * 2) })
            .await?;
        Ok(Output { doubled })
    })?;

    let spawned = client
        .spawn(&task, Params { value: 21 }, Default::default())
        .await?;
    assert!(spawned.created);

    let worked = client
        .work_batch(WorkBatchOptions::new().claim_timeout(Duration::from_secs(30)))
        .await?;
    assert_eq!(worked, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.completed_payload, Some(json!({ "doubled": 42 })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn spawn_resolution_applies_defaults_overrides_and_rejects_queue_mismatches() -> Result<()> {
    let queue = random_queue();
    let other_queue = random_queue();
    let client = Client::connect_queue(database_url(), &queue)
        .await?
        .default_max_attempts(4);
    client.create_queue().await?;
    client.create_queue_in(&other_queue).await?;

    let client_default_task = Task::<(), Value>::new("client-default-attempts").queue(&queue);
    client.register(&client_default_task, |(), _ctx| async move {
        Ok(json!({ "ok": true }))
    })?;
    let client_default = client
        .spawn(&client_default_task, (), SpawnOptions::new())
        .await?;
    assert_eq!(
        fetch_task(&queue, client_default.task_id)
            .await?
            .max_attempts,
        Some(4)
    );

    let task_default = Task::<(), Value>::new("task-default-attempts")
        .queue(&queue)
        .default_max_attempts(2);
    client.register(&task_default, |(), _ctx| async move {
        Ok(json!({ "ok": true }))
    })?;
    let task_default_spawned = client.spawn(&task_default, (), SpawnOptions::new()).await?;
    assert_eq!(
        fetch_task(&queue, task_default_spawned.task_id)
            .await?
            .max_attempts,
        Some(2)
    );

    let overridden = client
        .spawn(&task_default, (), SpawnOptions::new().max_attempts(3))
        .await?;
    assert_eq!(
        fetch_task(&queue, overridden.task_id).await?.max_attempts,
        Some(3)
    );

    let missing_queue = client
        .spawn_named("unregistered-without-queue", (), SpawnOptions::new())
        .await;
    assert!(
        matches!(missing_queue, Err(Error::Config(message)) if message.contains("is not registered"))
    );

    let mismatched_queue = client
        .spawn(
            &task_default,
            (),
            SpawnOptions::new().queue(other_queue.clone()),
        )
        .await;
    assert!(
        matches!(mismatched_queue, Err(Error::Config(message)) if message.contains("spawn requested queue"))
    );

    client.drop_queue().await?;
    client.drop_queue_in(&other_queue).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn work_batch_handles_mixed_success_and_failure() -> Result<()> {
    let (queue, client) = test_client().await?;

    let ok_a = Task::<(), Value>::new("mixed-ok-a").queue(&queue);
    let ok_b = Task::<(), Value>::new("mixed-ok-b").queue(&queue);
    let fail = Task::<(), Value>::new("mixed-fail")
        .queue(&queue)
        .default_max_attempts(1);

    client.register(&ok_a, |(), _ctx| async move { Ok(json!({ "ok": "a" })) })?;
    client.register(&ok_b, |(), _ctx| async move { Ok(json!({ "ok": "b" })) })?;
    client.register(&fail, |(), _ctx| async move {
        Err::<Value, Error>(Error::message("mixed batch failure"))
    })?;

    let spawned_ok_a = client.spawn(&ok_a, (), SpawnOptions::new()).await?;
    let spawned_fail = client.spawn(&fail, (), SpawnOptions::new()).await?;
    let spawned_ok_b = client.spawn(&ok_b, (), SpawnOptions::new()).await?;

    assert_eq!(
        client
            .work_batch(WorkBatchOptions::new().batch_size(10))
            .await?,
        3
    );

    assert_eq!(
        fetch_task(&queue, spawned_ok_a.task_id).await?.state,
        "completed"
    );
    assert_eq!(
        fetch_task(&queue, spawned_fail.task_id).await?.state,
        "failed"
    );
    assert_eq!(
        fetch_task(&queue, spawned_ok_b.task_id).await?.state,
        "completed"
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn worker_close_waits_for_running_tasks() -> Result<()> {
    let (queue, client) = test_client().await?;
    let task = Task::<(), Value>::new("draining-task").queue(&queue);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));

    client.register(&task, {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        let completed = Arc::clone(&completed);
        move |(), _ctx| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let completed = Arc::clone(&completed);
            async move {
                started.notify_one();
                release.notified().await;
                completed.fetch_add(1, Ordering::SeqCst);
                Ok(json!({ "drained": true }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;
    let worker = client.start_worker(
        WorkerOptions::new()
            .poll_interval(Duration::from_millis(25))
            .fatal_on_lease_timeout(false),
    )?;

    tokio::time::timeout(Duration::from_secs(5), started.notified())
        .await
        .map_err(|_| Error::message("worker did not start task"))?;

    let close = tokio::spawn(worker.close());
    tokio::task::yield_now().await;
    assert!(!close.is_finished());

    release.notify_one();
    let close_result = close.await.map_err(Error::Join)?;
    close_result?;
    assert_eq!(completed.load(Ordering::SeqCst), 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.completed_payload, Some(json!({ "drained": true })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn worker_respects_concurrency_limit() -> Result<()> {
    let (queue, client) = test_client().await?;
    let task = Task::<i64, Value>::new("concurrency-limited").queue(&queue);
    let started = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let release_notify = Arc::new(Notify::new());
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel::<i64>();

    client.register(&task, {
        let started = Arc::clone(&started);
        let release = Arc::clone(&release);
        let release_notify = Arc::clone(&release_notify);
        move |index, _ctx| {
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let release_notify = Arc::clone(&release_notify);
            let started_tx = started_tx.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                let _ = started_tx.send(index);
                while !release.load(Ordering::SeqCst) {
                    release_notify.notified().await;
                }
                Ok(json!({ "index": index }))
            }
        }
    })?;

    let spawned = [
        client.spawn(&task, 1, SpawnOptions::new()).await?,
        client.spawn(&task, 2, SpawnOptions::new()).await?,
        client.spawn(&task, 3, SpawnOptions::new()).await?,
    ];

    let worker = client.start_worker(
        WorkerOptions::new()
            .concurrency(2)
            .batch_size(3)
            .poll_interval(Duration::from_millis(25))
            .fatal_on_lease_timeout(false),
    )?;

    let mut started_indices = Vec::new();
    for _ in 0..2 {
        let index = tokio::time::timeout(Duration::from_secs(5), started_rx.recv())
            .await
            .map_err(|_| Error::message("worker did not start the first two tasks"))?
            .ok_or_else(|| Error::message("started channel closed"))?;
        started_indices.push(index);
    }

    assert_eq!(started.load(Ordering::SeqCst), 2);
    for (offset, item) in spawned.iter().enumerate() {
        let index = i64::try_from(offset + 1)
            .map_err(|err| Error::message(format!("invalid task index: {err}")))?;
        let task = fetch_task(&queue, item.task_id).await?;
        if started_indices.contains(&index) {
            assert_eq!(task.state, "running");
        } else {
            assert_eq!(task.state, "pending");
        }
    }

    release.store(true, Ordering::SeqCst);
    release_notify.notify_waiters();

    tokio::time::timeout(Duration::from_secs(5), started_rx.recv())
        .await
        .map_err(|_| Error::message("worker did not start the third task after capacity freed"))?
        .ok_or_else(|| Error::message("started channel closed"))?;

    worker.close().await?;

    for item in spawned {
        assert_eq!(fetch_task(&queue, item.task_id).await?.state, "completed");
    }

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn worker_on_error_reports_claim_failures() -> Result<()> {
    let (_queue, client) = test_client().await?;
    let (error_tx, mut error_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    let worker = client.start_worker(
        WorkerOptions::new()
            .poll_interval(Duration::from_millis(25))
            .fatal_on_lease_timeout(false)
            .on_error(move |worker_error| {
                if worker_error.kind == WorkerErrorKind::Claim {
                    let _ = error_tx.send(worker_error.error.to_string());
                }
            }),
    )?;

    client.drop_queue().await?;

    let message = tokio::time::timeout(Duration::from_secs(5), error_rx.recv())
        .await
        .map_err(|_| Error::message("worker did not report claim error"))?
        .ok_or_else(|| Error::message("worker error channel closed"))?;
    assert!(!message.is_empty());

    worker.close().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn worker_on_error_reports_execution_failures() -> Result<()> {
    let (queue, client) = test_client().await?;
    let (error_tx, mut error_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    let task = Task::<(), Value>::new("worker-execution-error")
        .queue(&queue)
        .default_max_attempts(1);
    client.register(&task, |(), _ctx| async move {
        Err::<Value, Error>(Error::message("handler execution failed"))
    })?;
    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;

    let worker = client.start_worker(
        WorkerOptions::new()
            .poll_interval(Duration::from_millis(25))
            .fatal_on_lease_timeout(false)
            .on_error(move |worker_error| {
                if worker_error.kind == WorkerErrorKind::Execution {
                    let _ = error_tx.send(worker_error.error.to_string());
                }
            }),
    )?;

    let message = tokio::time::timeout(Duration::from_secs(5), error_rx.recv())
        .await
        .map_err(|_| Error::message("worker did not report execution error"))?
        .ok_or_else(|| Error::message("worker error channel closed"))?;
    assert_eq!(message, "handler execution failed");

    let task = fetch_task(&queue, spawned.task_id).await?;
    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(task.state, "failed");
    assert_eq!(run.state, "failed");
    assert_eq!(
        run.failure_reason,
        Some(json!({
            "name": "Error",
            "message": "handler execution failed",
            "debug": "Message(\"handler execution failed\")",
            "traceback": null,
        }))
    );

    worker.close().await?;
    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn invalid_task_headers_fail_task_and_report_execution_error() -> Result<()> {
    let (queue, client) = test_client().await?;
    let (error_tx, mut error_rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    let task = Task::<(), Value>::new("invalid-headers")
        .queue(&queue)
        .default_max_attempts(1);
    client.register(&task, |(), _ctx| async move {
        Ok(json!({ "unexpected": true }))
    })?;
    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    set_task_headers(&queue, spawned.task_id, json!(["not", "an", "object"])).await?;

    let worker = client.start_worker(
        WorkerOptions::new()
            .poll_interval(Duration::from_millis(25))
            .fatal_on_lease_timeout(false)
            .on_error(move |worker_error| {
                if worker_error.kind == WorkerErrorKind::Execution {
                    let _ = error_tx.send(worker_error.error.to_string());
                }
            }),
    )?;

    let message = tokio::time::timeout(Duration::from_secs(5), error_rx.recv())
        .await
        .map_err(|_| Error::message("worker did not report invalid headers"))?
        .ok_or_else(|| Error::message("worker error channel closed"))?;
    assert_eq!(message, "invalid task headers: expected a JSON object");

    let task = fetch_task(&queue, spawned.task_id).await?;
    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(task.state, "failed");
    assert_eq!(run.state, "failed");
    assert_eq!(
        run.failure_reason,
        Some(json!({
            "name": "InvalidHeaders",
            "message": "invalid task headers: expected a JSON object",
            "debug": "InvalidHeaders",
            "traceback": null,
        }))
    );

    worker.close().await?;
    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn spawn_headers_are_available_to_task_context() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("headers").queue(&queue);
    client.register(&task, |(), ctx| async move {
        Ok(json!({ "headers": ctx.headers().clone() }))
    })?;

    let mut headers = Map::new();
    headers.insert("trace_id".to_string(), json!("trace-123"));
    headers.insert("sampled".to_string(), json!(true));

    let spawned = client
        .spawn(&task, (), SpawnOptions::new().headers(headers.clone()))
        .await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "headers": Value::Object(headers) }))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn before_spawn_hook_can_inject_headers() -> Result<()> {
    let queue = random_queue();
    let hooks = Hooks::new().before_spawn(|task_name, params, mut options| async move {
        assert_eq!(task_name, "hooked-headers");
        assert_eq!(params["user_id"], json!(42));
        options
            .headers
            .get_or_insert_with(Map::new)
            .insert("trace_id".to_string(), json!("trace-hook"));
        Ok(options)
    });
    let client = Client::connect_queue_with_hooks(database_url(), &queue, hooks).await?;
    client.create_queue().await?;

    let task = Task::<Value, Value>::new("hooked-headers").queue(&queue);
    client.register(&task, |_, ctx| async move {
        Ok(json!({ "trace_id": ctx.headers().get("trace_id") }))
    })?;

    let spawned = client
        .spawn(&task, json!({ "user_id": 42 }), SpawnOptions::new())
        .await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "trace_id": "trace-hook" }))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn wrap_task_execution_hook_can_transform_result() -> Result<()> {
    let queue = random_queue();
    let hooks = Hooks::new().wrap_task_execution(|ctx, execute| async move {
        let value = execute(ctx).await?;
        Ok(json!({ "wrapped": value }))
    });
    let client = Client::connect_queue_with_hooks(database_url(), &queue, hooks).await?;
    client.create_queue().await?;

    let task = Task::<(), Value>::new("wrapped-task").queue(&queue);
    client.register(&task, |(), _ctx| async move { Ok(json!({ "ok": true })) })?;

    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "wrapped": { "ok": true } }))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn task_result_fetch_and_await_return_terminal_payload() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("result-source").queue(&queue);
    client.register(&task, |(), _ctx| async move { Ok(json!({ "answer": 42 })) })?;

    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    assert_eq!(
        client.fetch_task_result(spawned.task_id, None).await?,
        Some(TaskResultSnapshot::Pending)
    );

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let expected = TaskResultSnapshot::Completed {
        result: json!({ "answer": 42 }),
    };
    assert_eq!(
        client.fetch_task_result(spawned.task_id, None).await?,
        Some(expected.clone())
    );
    assert_eq!(
        client
            .await_task_result(
                spawned.task_id,
                AwaitTaskResultOptions::new().timeout(Duration::from_secs(1)),
            )
            .await?,
        expected
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn task_result_fetch_reports_failed_cancelled_and_missing_tasks() -> Result<()> {
    let (queue, client) = test_client().await?;

    let failing_task = Task::<(), Value>::new("result-fails")
        .queue(&queue)
        .default_max_attempts(1);
    client.register(&failing_task, |(), _ctx| async move {
        Err::<Value, Error>(Error::message("task result failure"))
    })?;

    let failed = client.spawn(&failing_task, (), SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(
        client.fetch_task_result(failed.task_id, None).await?,
        Some(TaskResultSnapshot::Failed {
            failure: json!({
                "name": "Error",
                "message": "task result failure",
                "debug": "Message(\"task result failure\")",
                "traceback": null,
            })
        })
    );

    let cancelled_task = Task::<(), Value>::new("result-cancelled").queue(&queue);
    client.register(&cancelled_task, |(), _ctx| async move {
        Ok(json!({ "unexpected": true }))
    })?;
    let cancelled = client
        .spawn(&cancelled_task, (), SpawnOptions::new())
        .await?;
    client.cancel_task(cancelled.task_id, None).await?;
    assert_eq!(
        client.fetch_task_result(cancelled.task_id, None).await?,
        Some(TaskResultSnapshot::Cancelled)
    );

    let missing_task_id = uuid::Uuid::new_v4();
    assert_eq!(client.fetch_task_result(missing_task_id, None).await?, None);
    let missing = client
        .await_task_result(
            missing_task_id,
            AwaitTaskResultOptions::new().timeout(Duration::ZERO),
        )
        .await;
    assert!(matches!(missing, Err(Error::TaskNotFound { task_id }) if task_id == missing_task_id));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn task_result_await_times_out_for_pending_task() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("result-timeout").queue(&queue);
    client.register(&task, |(), _ctx| async move { Ok(json!({ "done": true })) })?;
    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;

    let result = client
        .await_task_result(
            spawned.task_id,
            AwaitTaskResultOptions::new().timeout(Duration::ZERO),
        )
        .await;
    assert!(
        matches!(result, Err(Error::TaskResultTimeout { task_id }) if task_id == spawned.task_id)
    );
    assert_eq!(
        client.fetch_task_result(spawned.task_id, None).await?,
        Some(TaskResultSnapshot::Pending)
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn context_rejects_same_queue_task_result_waits() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<uuid::Uuid, Value>::new("same-queue-await-result").queue(&queue);
    client.register(&task, |task_id, mut ctx| async move {
        let message = match ctx
            .await_task_result(task_id, AwaitTaskResultOptions::new())
            .await
        {
            Err(Error::Config(message)) => message,
            Ok(snapshot) => {
                return Err(Error::message(format!(
                    "unexpected same-queue snapshot: {snapshot:?}"
                )));
            }
            Err(err) => return Err(err),
        };
        Ok(json!({ "error": message }))
    })?;

    let target_id = uuid::Uuid::new_v4();
    let spawned = client.spawn(&task, target_id, SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({
            "error": "TaskContext.await_task_result cannot wait on tasks in the same queue because this can deadlock workers. Spawn the child in a different queue and pass options.queue."
        }))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn context_can_await_completed_task_result_from_another_queue() -> Result<()> {
    let parent_queue = random_queue();
    let child_queue = random_queue();
    let parent = Client::connect_queue(database_url(), &parent_queue).await?;
    let child = Client::connect_queue(database_url(), &child_queue).await?;
    parent.create_queue().await?;
    child.create_queue().await?;

    let child_task = Task::<(), Value>::new("child-result").queue(&child_queue);
    child.register(&child_task, |(), _ctx| async move {
        Ok(json!({ "child": "done" }))
    })?;
    let child_spawned = child.spawn(&child_task, (), SpawnOptions::new()).await?;
    assert_eq!(child.work_batch(WorkBatchOptions::new()).await?, 1);

    let parent_task = Task::<uuid::Uuid, Value>::new("parent-await-result").queue(&parent_queue);
    parent.register(&parent_task, {
        let child_queue = child_queue.clone();
        move |task_id, mut ctx| {
            let child_queue = child_queue.clone();
            async move {
                let snapshot = ctx
                    .await_task_result(
                        task_id,
                        AwaitTaskResultOptions::new()
                            .queue(child_queue)
                            .timeout(Duration::from_secs(1)),
                    )
                    .await?;
                Ok(json!({ "snapshot": snapshot }))
            }
        }
    })?;

    let parent_spawned = parent
        .spawn(&parent_task, child_spawned.task_id, SpawnOptions::new())
        .await?;
    assert_eq!(parent.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&parent_queue, parent_spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({
            "snapshot": {
                "state": "completed",
                "result": { "child": "done" },
            }
        }))
    );

    parent.drop_queue().await?;
    child.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn context_await_task_result_waits_for_pending_child_in_other_queue() -> Result<()> {
    let parent_queue = random_queue();
    let child_queue = random_queue();
    let parent = Client::connect_queue(database_url(), &parent_queue).await?;
    let child = Client::connect_queue(database_url(), &child_queue).await?;
    parent.create_queue().await?;
    child.create_queue().await?;

    let child_task = Task::<(), Value>::new("pending-child-result").queue(&child_queue);
    child.register(&child_task, |(), _ctx| async move {
        Ok(json!({ "child": "eventually done" }))
    })?;

    let (waiting_tx, mut waiting_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let parent_task =
        Task::<uuid::Uuid, Value>::new("parent-waits-for-pending-child").queue(&parent_queue);
    parent.register(&parent_task, {
        let child_queue = child_queue.clone();
        move |child_id, mut ctx| {
            let child_queue = child_queue.clone();
            let waiting_tx = waiting_tx.clone();
            async move {
                let _ = waiting_tx.send(());
                let snapshot = ctx
                    .await_task_result(
                        child_id,
                        AwaitTaskResultOptions::new()
                            .queue(child_queue)
                            .timeout(Duration::from_secs(5)),
                    )
                    .await?;
                Ok(json!({ "snapshot": snapshot }))
            }
        }
    })?;

    let child_spawned = child.spawn(&child_task, (), SpawnOptions::new()).await?;
    let parent_spawned = parent
        .spawn(&parent_task, child_spawned.task_id, SpawnOptions::new())
        .await?;

    let parent_worker = {
        let parent = parent.clone();
        tokio::spawn(async move { parent.work_batch(WorkBatchOptions::new()).await })
    };

    tokio::time::timeout(Duration::from_secs(5), waiting_rx.recv())
        .await
        .map_err(|_| Error::message("parent did not begin waiting for child result"))?
        .ok_or_else(|| Error::message("parent wait channel closed"))?;
    assert_eq!(child.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(
        parent_worker.await.map_err(Error::Join)??,
        1,
        "parent task should complete after child result becomes available"
    );

    let task = fetch_task(&parent_queue, parent_spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({
            "snapshot": {
                "state": "completed",
                "result": { "child": "eventually done" },
            }
        }))
    );

    parent.drop_queue().await?;
    child.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn context_await_task_result_checkpoint_survives_parent_retry_after_child_cleanup()
-> Result<()> {
    let parent_queue = random_queue();
    let child_queue = random_queue();
    let parent = Client::connect_queue(database_url(), &parent_queue).await?;
    let (child, child_pool) = connect_queue_with_single_connection_pool(&child_queue).await?;
    parent.create_queue().await?;
    child.create_queue().await?;

    let base = utc("2024-05-01T08:00:00Z")?;
    set_fake_now(&child_pool, Some(base)).await?;

    let child_task = Task::<(), Value>::new("cleanup-child-result").queue(&child_queue);
    child.register(&child_task, |(), _ctx| async move {
        Ok(json!({ "child": "cached" }))
    })?;
    let child_spawned = child.spawn(&child_task, (), SpawnOptions::new()).await?;
    assert_eq!(child.work_batch(WorkBatchOptions::new()).await?, 1);

    let attempts = Arc::new(AtomicUsize::new(0));
    let parent_task = Task::<uuid::Uuid, Value>::new("parent-caches-child-result")
        .queue(&parent_queue)
        .default_max_attempts(2);
    parent.register(&parent_task, {
        let child_queue = child_queue.clone();
        let attempts = Arc::clone(&attempts);
        move |child_id, mut ctx| {
            let child_queue = child_queue.clone();
            let attempts = Arc::clone(&attempts);
            async move {
                let snapshot = ctx
                    .await_task_result(
                        child_id,
                        AwaitTaskResultOptions::new()
                            .queue(child_queue)
                            .timeout(Duration::from_secs(1)),
                    )
                    .await?;
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    return Err(Error::message("retry after child result checkpoint"));
                }
                Ok(json!({ "snapshot": snapshot, "attempt": attempt }))
            }
        }
    })?;

    let parent_spawned = parent
        .spawn(&parent_task, child_spawned.task_id, SpawnOptions::new())
        .await?;
    assert_eq!(parent.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(
        fetch_task(&parent_queue, parent_spawned.task_id)
            .await?
            .state,
        "pending"
    );
    assert_eq!(
        fetch_checkpoints(&parent_queue, parent_spawned.task_id)
            .await?
            .len(),
        1
    );

    set_fake_now(&child_pool, Some(base + ChronoDuration::hours(2))).await?;
    let cleanup = child
        .cleanup_with_limit(Duration::from_secs(3600), 10, None)
        .await?;
    assert_eq!(cleanup.tasks_deleted, 1);
    assert_eq!(
        child.fetch_task_result(child_spawned.task_id, None).await?,
        None
    );

    assert_eq!(parent.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&parent_queue, parent_spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({
            "snapshot": {
                "state": "completed",
                "result": { "child": "cached" },
            },
            "attempt": 2,
        }))
    );

    parent.drop_queue().await?;
    child.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn decomposed_step_handle_reuses_completed_state_after_retry() -> Result<()> {
    let (queue, client) = test_client().await?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("manual-step")
        .queue(&queue)
        .default_max_attempts(2);
    client.register(&task, {
        let attempts = Arc::clone(&attempts);
        move |(), mut ctx| {
            let attempts = Arc::clone(&attempts);
            async move {
                let handle = ctx.begin_step::<i64>("manual").await?;
                let was_done = handle.done;
                let value = ctx.complete_step(handle, 99).await?;
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    return Err(Error::message("retry after decomposed step"));
                }
                Ok(json!({ "value": value, "was_done": was_done }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "value": 99, "was_done": true }))
    );
    assert_eq!(
        fetch_checkpoints(&queue, spawned.task_id).await?,
        vec![("manual".to_string(), json!(99))]
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn failed_decomposed_step_is_not_checkpointed_and_reexecutes() -> Result<()> {
    let (queue, client) = test_client().await?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("manual-fragile-step")
        .queue(&queue)
        .default_max_attempts(2);
    client.register(&task, {
        let attempts = Arc::clone(&attempts);
        move |(), mut ctx| {
            let attempts = Arc::clone(&attempts);
            async move {
                let handle = ctx.begin_step::<i64>("manual-fragile").await?;
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    return Err(Error::message("decomposed step failed before complete"));
                }
                let value = ctx.complete_step(handle, 123).await?;
                Ok(json!({ "value": value, "attempt": attempt }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(fetch_checkpoints(&queue, spawned.task_id).await?.is_empty());

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "value": 123, "attempt": 2 }))
    );
    assert_eq!(
        fetch_checkpoints(&queue, spawned.task_id).await?,
        vec![("manual-fragile".to_string(), json!(123))]
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn repeated_decomposed_step_names_are_numbered() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("manual-loop-steps").queue(&queue);
    client.register(&task, |(), mut ctx| async move {
        let mut results = Vec::new();
        for i in 0_i64..3 {
            let handle = ctx.begin_step::<i64>("manual-loop").await?;
            let value = ctx.complete_step(handle, i * 5).await?;
            results.push(value);
        }
        Ok(json!({ "results": results }))
    })?;

    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "results": [0, 5, 10] }))
    );
    assert_eq!(
        fetch_checkpoints(&queue, spawned.task_id).await?,
        vec![
            ("manual-loop".to_string(), json!(0)),
            ("manual-loop#2".to_string(), json!(5)),
            ("manual-loop#3".to_string(), json!(10)),
        ]
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn step_checkpoint_is_reused_after_retry() -> Result<()> {
    let (queue, client) = test_client().await?;
    let executions = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("cached-step")
        .queue(&queue)
        .default_max_attempts(2);
    client.register(&task, {
        let executions = Arc::clone(&executions);
        let attempts = Arc::clone(&attempts);
        move |(), mut ctx| {
            let executions = Arc::clone(&executions);
            let attempts = Arc::clone(&attempts);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                let value: i64 = ctx
                    .step("expensive", || {
                        let executions = Arc::clone(&executions);
                        async move {
                            executions.fetch_add(1, Ordering::SeqCst);
                            Ok(42)
                        }
                    })
                    .await?;

                if attempt == 1 {
                    return Err(Error::message("retry after checkpoint"));
                }

                Ok(json!({
                    "value": value,
                    "executions": executions.load(Ordering::SeqCst),
                }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.attempts, 2);
    assert_eq!(
        task.completed_payload,
        Some(json!({ "value": 42, "executions": 1 }))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn failed_step_is_not_checkpointed_and_reexecutes() -> Result<()> {
    let (queue, client) = test_client().await?;
    let invocations = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("fragile-step")
        .queue(&queue)
        .default_max_attempts(2);
    client.register(&task, {
        let invocations = Arc::clone(&invocations);
        move |(), mut ctx| {
            let invocations = Arc::clone(&invocations);
            async move {
                let result: String = ctx
                    .step("fragile", || {
                        let invocations = Arc::clone(&invocations);
                        async move {
                            let invocation = invocations.fetch_add(1, Ordering::SeqCst) + 1;
                            if invocation == 1 {
                                Err(Error::message("step failed before checkpoint"))
                            } else {
                                Ok("success".to_string())
                            }
                        }
                    })
                    .await?;
                Ok(json!({ "result": result }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
    assert!(fetch_checkpoints(&queue, spawned.task_id).await?.is_empty());

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(invocations.load(Ordering::SeqCst), 2);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.completed_payload, Some(json!({ "result": "success" })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn multi_step_only_reexecutes_uncompleted_steps() -> Result<()> {
    let (queue, client) = test_client().await?;
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let third_calls = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("multi-step-retry")
        .queue(&queue)
        .default_max_attempts(2);
    client.register(&task, {
        let first_calls = Arc::clone(&first_calls);
        let second_calls = Arc::clone(&second_calls);
        let third_calls = Arc::clone(&third_calls);
        move |(), mut ctx| {
            let first_calls = Arc::clone(&first_calls);
            let second_calls = Arc::clone(&second_calls);
            let third_calls = Arc::clone(&third_calls);
            async move {
                let first: i64 = ctx
                    .step("first", || {
                        let first_calls = Arc::clone(&first_calls);
                        async move {
                            first_calls.fetch_add(1, Ordering::SeqCst);
                            Ok(1)
                        }
                    })
                    .await?;
                let second: i64 = ctx
                    .step("second", || {
                        let second_calls = Arc::clone(&second_calls);
                        async move {
                            second_calls.fetch_add(1, Ordering::SeqCst);
                            Ok(2)
                        }
                    })
                    .await?;
                let third: i64 = ctx
                    .step("third", || {
                        let third_calls = Arc::clone(&third_calls);
                        async move {
                            let call = third_calls.fetch_add(1, Ordering::SeqCst) + 1;
                            if call == 1 {
                                Err(Error::message("third step fails once"))
                            } else {
                                Ok(3)
                            }
                        }
                    })
                    .await?;
                Ok(json!({
                    "sum": first + second + third,
                    "first_calls": first_calls.load(Ordering::SeqCst),
                    "second_calls": second_calls.load(Ordering::SeqCst),
                    "third_calls": third_calls.load(Ordering::SeqCst),
                }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(third_calls.load(Ordering::SeqCst), 2);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({
            "sum": 6,
            "first_calls": 1,
            "second_calls": 1,
            "third_calls": 2,
        }))
    );
    assert_eq!(
        fetch_checkpoints(&queue, spawned.task_id).await?,
        vec![
            ("first".to_string(), json!(1)),
            ("second".to_string(), json!(2)),
            ("third".to_string(), json!(3)),
        ]
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn repeated_step_names_are_numbered() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("loop-steps").queue(&queue);
    client.register(&task, |(), mut ctx| async move {
        let mut results = Vec::new();
        for i in 0_i64..3 {
            let result: i64 = ctx.step("loop-step", || async move { Ok(i * 10) }).await?;
            results.push(result);
        }
        Ok(json!({ "results": results }))
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "results": [0, 10, 20] }))
    );
    assert_eq!(
        fetch_checkpoints(&queue, spawned.task_id).await?,
        vec![
            ("loop-step".to_string(), json!(0)),
            ("loop-step#2".to_string(), json!(10)),
            ("loop-step#3".to_string(), json!(20)),
        ]
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn sleep_for_suspends_until_duration_elapses() -> Result<()> {
    let (queue, client, pool) = test_client_with_single_connection_pool().await?;
    let base = utc("2024-05-05T10:00:00Z")?;
    set_fake_now(&pool, Some(base)).await?;

    let task = Task::<(), Value>::new("sleep-for").queue(&queue);
    client.register(&task, |(), mut ctx| async move {
        ctx.sleep_for("wait-for", Duration::from_secs(60)).await?;
        Ok(json!({ "resumed": true }))
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    let run = fetch_run(&queue, spawned.run_id).await?;
    let wake_at = base + ChronoDuration::seconds(60);
    assert_eq!(task.state, "sleeping");
    assert_eq!(run.state, "sleeping");
    assert_eq!(run.available_at, Some(wake_at));

    set_fake_now(&pool, Some(wake_at + ChronoDuration::seconds(5))).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.completed_payload, Some(json!({ "resumed": true })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn sleep_until_checkpoint_prevents_rescheduling() -> Result<()> {
    let (queue, client, pool) = test_client_with_single_connection_pool().await?;
    let base = utc("2024-05-06T09:00:00Z")?;
    set_fake_now(&pool, Some(base)).await?;
    let wake_at = base + ChronoDuration::minutes(5);
    let executions = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("sleep-until").queue(&queue);
    client.register(&task, {
        let executions = Arc::clone(&executions);
        move |(), mut ctx| {
            let executions = Arc::clone(&executions);
            async move {
                let execution = executions.fetch_add(1, Ordering::SeqCst) + 1;
                ctx.sleep_until("sleep-step", wake_at).await?;
                Ok(json!({ "executions": execution }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let checkpoints = fetch_checkpoints(&queue, spawned.task_id).await?;
    assert_eq!(
        checkpoints,
        vec![("sleep-step".to_string(), serde_json::to_value(wake_at)?)],
    );

    let task = fetch_task(&queue, spawned.task_id).await?;
    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(task.state, "sleeping");
    assert_eq!(run.state, "sleeping");
    assert!(run.wake_event.is_none());
    assert!(run.failure_reason.is_none());
    assert_eq!(run.available_at, Some(wake_at));

    set_fake_now(&pool, Some(wake_at)).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.completed_payload, Some(json!({ "executions": 2 })));
    assert_eq!(executions.load(Ordering::SeqCst), 2);

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn pre_emitted_event_is_available_to_late_waiter() -> Result<()> {
    let (queue, client) = test_client().await?;
    let event_name = format!("pre_emitted_{queue}");
    let payload = json!({ "data": "ready" });

    client.emit_event(&event_name, &payload, None).await?;

    let task = Task::<(), Value>::new("late-waiter").queue(&queue);
    client.register(&task, {
        let event_name = event_name.clone();
        move |(), mut ctx| {
            let event_name = event_name.clone();
            async move {
                let received: Value = ctx.await_event(&event_name).await?;
                Ok(json!({ "received": received }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.completed_payload, Some(json!({ "received": payload })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn event_emit_is_first_write_wins() -> Result<()> {
    let (queue, client) = test_client().await?;
    let event_name = format!("first_write_{queue}");

    client
        .emit_event(&event_name, &json!({ "version": 1 }), None)
        .await?;
    client
        .emit_event(&event_name, &json!({ "version": 2 }), None)
        .await?;

    let task = Task::<(), Value>::new("first-write-waiter").queue(&queue);
    client.register(&task, {
        let event_name = event_name.clone();
        move |(), mut ctx| {
            let event_name = event_name.clone();
            async move {
                let received: Value = ctx.await_event(&event_name).await?;
                Ok(json!({ "received": received }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "received": { "version": 1 } }))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn emitted_event_wakes_all_waiters() -> Result<()> {
    let (queue, client) = test_client().await?;
    let event_name = format!("broadcast_{queue}");

    let task = Task::<Value, Value>::new("multi-waiter").queue(&queue);
    client.register(&task, {
        let event_name = event_name.clone();
        move |params, mut ctx| {
            let event_name = event_name.clone();
            async move {
                let payload: Value = ctx.await_event(&event_name).await?;
                Ok(json!({
                    "task_num": params["task_num"],
                    "received": payload,
                }))
            }
        }
    })?;

    let spawned = [
        client
            .spawn(&task, json!({ "task_num": 1 }), Default::default())
            .await?,
        client
            .spawn(&task, json!({ "task_num": 2 }), Default::default())
            .await?,
        client
            .spawn(&task, json!({ "task_num": 3 }), Default::default())
            .await?,
    ];

    assert_eq!(
        client
            .work_batch(WorkBatchOptions::new().batch_size(10))
            .await?,
        3
    );
    for item in &spawned {
        let task = fetch_task(&queue, item.task_id).await?;
        let run = fetch_run(&queue, item.run_id).await?;
        assert_eq!(task.state, "sleeping");
        assert_eq!(run.state, "sleeping");
        assert_eq!(run.wake_event.as_deref(), Some(event_name.as_str()));
    }

    let payload = json!({ "data": "broadcast" });
    client.emit_event(&event_name, &payload, None).await?;

    assert_eq!(
        client
            .work_batch(WorkBatchOptions::new().batch_size(10))
            .await?,
        3
    );
    for (index, item) in spawned.iter().enumerate() {
        let task = fetch_task(&queue, item.task_id).await?;
        assert_eq!(task.state, "completed");
        assert_eq!(
            task.completed_payload,
            Some(json!({
                "task_num": index + 1,
                "received": payload,
            }))
        );
    }

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn event_timeout_can_be_caught_without_recreating_wait() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("timeout-flow").queue(&queue);
    client.register(&task, |(), mut ctx| async move {
        let options = AwaitEventOptions::new()
            .step_name("wait")
            .timeout(Duration::ZERO);

        match ctx
            .await_event_with_options::<Value>("never", options.clone())
            .await
        {
            Err(Error::EventTimeout { .. }) => {}
            other => return other.map(|_| json!({ "unexpected": true })),
        }

        let payload: Value = ctx.await_event_with_options("never", options).await?;

        Ok(json!({
            "timed_out": true,
            "second_payload": payload,
        }))
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    let first = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(first, 1);
    let second = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(second, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "timed_out": true, "second_payload": null }))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn cleanup_removes_terminal_tasks_and_events_by_ttl() -> Result<()> {
    let (queue, client, pool) = test_client_with_single_connection_pool().await?;
    let base = utc("2024-03-01T08:00:00Z")?;
    set_fake_now(&pool, Some(base)).await?;

    let task = Task::<(), Value>::new("cleanup-target").queue(&queue);
    client.register(
        &task,
        |(), _ctx| async move { Ok(json!({ "status": "done" })) },
    )?;

    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    client
        .emit_event("cleanup-event", &json!({ "kind": "notify" }), None)
        .await?;

    set_fake_now(&pool, Some(base + ChronoDuration::minutes(30))).await?;
    let before_ttl = client.cleanup(Duration::from_secs(3600), None).await?;
    assert_eq!(before_ttl.tasks_deleted, 0);
    assert_eq!(before_ttl.events_deleted, 0);
    assert_eq!(
        fetch_task(&queue, spawned.task_id).await?.state,
        "completed"
    );
    assert_eq!(count_events(&queue).await?, 1);

    set_fake_now(&pool, Some(base + ChronoDuration::hours(2))).await?;
    let cleanup = client
        .cleanup_with_limit(Duration::from_secs(3600), 10, None)
        .await?;
    assert_eq!(cleanup.tasks_deleted, 1);
    assert_eq!(cleanup.events_deleted, 1);
    assert_eq!(count_tasks(&queue).await?, 0);
    assert_eq!(count_events(&queue).await?, 0);

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn idempotent_spawn_creates_one_task_and_executes_once() -> Result<()> {
    let (queue, client) = test_client().await?;
    let executions = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("idempotent-task").queue(&queue);
    client.register(&task, {
        let executions = Arc::clone(&executions);
        move |(), _ctx| {
            let executions = Arc::clone(&executions);
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(json!({ "done": true }))
            }
        }
    })?;

    let options = SpawnOptions::new().idempotency_key("daily-report:2025-01-15");
    let first = client.spawn(&task, (), options.clone()).await?;
    let second = client.spawn(&task, (), options.clone()).await?;
    let third = client.spawn(&task, (), options).await?;

    assert!(first.created);
    assert!(!second.created);
    assert!(!third.created);
    assert_eq!(first.task_id, second.task_id);
    assert_eq!(first.task_id, third.task_id);
    assert_eq!(count_tasks(&queue).await?, 1);

    assert_eq!(
        client
            .work_batch(WorkBatchOptions::new().batch_size(10))
            .await?,
        1
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    let task = fetch_task(&queue, first.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.completed_payload, Some(json!({ "done": true })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn failed_task_retries_immediately_until_success() -> Result<()> {
    let (queue, client) = test_client().await?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("retry-once")
        .queue(&queue)
        .default_max_attempts(2);
    client.register(&task, {
        let attempts = Arc::clone(&attempts);
        move |(), _ctx| {
            let attempts = Arc::clone(&attempts);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    Err(Error::message("first attempt failed"))
                } else {
                    Ok(json!({ "attempts": attempt }))
                }
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "pending");
    assert_eq!(task.attempts, 2);

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.attempts, 2);
    assert_eq!(task.completed_payload, Some(json!({ "attempts": 2 })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn retry_task_defaults_to_one_additional_attempt() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("manual-retry-default")
        .queue(&queue)
        .default_max_attempts(1);
    client.register(&task, |(), _ctx| async move {
        Err::<Value, Error>(Error::message("always fails"))
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "failed");
    assert_eq!(task.attempts, 1);

    let retried = client
        .retry_task(spawned.task_id, RetryTaskOptions::new())
        .await?;

    assert_eq!(retried.task_id, spawned.task_id);
    assert_ne!(retried.run_id, spawned.run_id);
    assert_eq!(retried.attempt, 2);
    assert!(!retried.created);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "pending");
    assert_eq!(task.attempts, 2);
    assert_eq!(task.max_attempts, Some(2));
    assert_eq!(count_runs(&queue, spawned.task_id).await?, 2);

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn retry_task_max_attempt_override_preserves_checkpoints() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("manual-retry-max-attempts")
        .queue(&queue)
        .default_max_attempts(1);
    client.register(&task, |(), mut ctx| async move {
        let value: i64 = ctx.step("preserved", || async move { Ok(7) }).await?;
        Err::<Value, Error>(Error::message(format!("failed after checkpoint {value}")))
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let failed = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(failed.state, "failed");
    assert_eq!(failed.attempts, 1);
    assert_eq!(failed.max_attempts, Some(1));
    assert_eq!(
        fetch_checkpoints(&queue, spawned.task_id).await?,
        vec![("preserved".to_string(), json!(7))]
    );

    let retried = client
        .retry_task(spawned.task_id, RetryTaskOptions::new().max_attempts(3))
        .await?;

    assert_eq!(retried.task_id, spawned.task_id);
    assert_ne!(retried.run_id, spawned.run_id);
    assert_eq!(retried.attempt, 2);
    assert!(!retried.created);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "pending");
    assert_eq!(task.attempts, 2);
    assert_eq!(task.max_attempts, Some(3));
    assert_eq!(count_runs(&queue, spawned.task_id).await?, 2);
    assert_eq!(
        fetch_checkpoints(&queue, spawned.task_id).await?,
        vec![("preserved".to_string(), json!(7))]
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn retry_task_rejects_missing_and_non_failed_tasks() -> Result<()> {
    let (queue, client) = test_client().await?;

    let missing_task_id = uuid::Uuid::new_v4();
    let missing = client
        .retry_task(missing_task_id, RetryTaskOptions::new())
        .await;
    let missing_error = match missing {
        Err(Error::Database(err)) => format!("{err:?}"),
        Ok(_) => {
            return Err(Error::message(
                "retry_task unexpectedly accepted a missing task",
            ));
        }
        Err(err) => {
            return Err(Error::message(format!(
                "unexpected retry_task missing error: {err}"
            )));
        }
    };
    assert!(missing_error.contains("not found"));

    let task = Task::<(), Value>::new("manual-retry-not-failed").queue(&queue);
    client.register(&task, |(), _ctx| async move { Ok(json!({ "ok": true })) })?;
    let spawned = client.spawn(&task, (), Default::default()).await?;

    let not_failed = client
        .retry_task(spawned.task_id, RetryTaskOptions::new())
        .await;
    let not_failed_error = match not_failed {
        Err(Error::Database(err)) => format!("{err:?}"),
        Ok(_) => {
            return Err(Error::message(
                "retry_task unexpectedly accepted a pending task",
            ));
        }
        Err(err) => {
            return Err(Error::message(format!(
                "unexpected retry_task non-failed error: {err}"
            )));
        }
    };
    assert!(not_failed_error.contains("not currently failed"));

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "pending");
    assert_eq!(task.attempts, 1);
    assert_eq!(count_runs(&queue, spawned.task_id).await?, 1);

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn retry_task_can_spawn_new_task_from_original_inputs() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<Value, Value>::new("manual-retry-spawn-new")
        .queue(&queue)
        .default_max_attempts(1);
    client.register(&task, |params, mut ctx| async move {
        let params_for_step = params.clone();
        let _: Value = ctx
            .step("remember-input", || async move { Ok(params_for_step) })
            .await?;
        Err::<Value, Error>(Error::message("always fails"))
    })?;

    let spawned = client
        .spawn(&task, json!({ "payload": 1 }), Default::default())
        .await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "failed");
    assert_eq!(task.attempts, 1);
    assert_eq!(fetch_checkpoints(&queue, spawned.task_id).await?.len(), 1);

    let retried = client
        .retry_task(
            spawned.task_id,
            RetryTaskOptions::new().queue(&queue).spawn_new_task(),
        )
        .await?;

    assert_ne!(retried.task_id, spawned.task_id);
    assert_ne!(retried.run_id, spawned.run_id);
    assert_eq!(retried.attempt, 1);
    assert!(retried.created);
    assert_eq!(count_tasks(&queue).await?, 2);

    let old_task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(old_task.state, "failed");
    assert_eq!(old_task.attempts, 1);
    assert_eq!(fetch_checkpoints(&queue, spawned.task_id).await?.len(), 1);

    let new_task = fetch_task(&queue, retried.task_id).await?;
    assert_eq!(new_task.state, "pending");
    assert_eq!(new_task.attempts, 1);
    assert!(fetch_checkpoints(&queue, retried.task_id).await?.is_empty());

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn fixed_retry_strategy_delays_next_attempt() -> Result<()> {
    let (queue, client, pool) = test_client_with_single_connection_pool().await?;
    let base = utc("2024-05-01T11:00:00Z")?;
    set_fake_now(&pool, Some(base)).await?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("fixed-retry")
        .queue(&queue)
        .default_max_attempts(2);
    client.register(&task, {
        let attempts = Arc::clone(&attempts);
        move |(), _ctx| {
            let attempts = Arc::clone(&attempts);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    Err(Error::message("first attempt failed"))
                } else {
                    Ok(json!({ "attempts": attempt }))
                }
            }
        }
    })?;

    let spawned = client
        .spawn(
            &task,
            (),
            SpawnOptions::new().retry_strategy(RetryStrategy::fixed(Duration::from_secs(1))),
        )
        .await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "sleeping");
    assert_eq!(task.attempts, 2);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 0);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    set_fake_now(&pool, Some(base + ChronoDuration::seconds(1))).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.attempts, 2);
    assert_eq!(task.completed_payload, Some(json!({ "attempts": 2 })));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn retry_strategy_none_requeues_immediately() -> Result<()> {
    let (queue, client, pool) = test_client_with_single_connection_pool().await?;
    let base = utc("2024-05-01T12:00:00Z")?;
    set_fake_now(&pool, Some(base)).await?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("none-retry")
        .queue(&queue)
        .default_max_attempts(2);
    client.register(&task, {
        let attempts = Arc::clone(&attempts);
        move |(), _ctx| {
            let attempts = Arc::clone(&attempts);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    Err(Error::message("retry immediately"))
                } else {
                    Ok(json!({ "attempts": attempt }))
                }
            }
        }
    })?;

    let spawned = client
        .spawn(
            &task,
            (),
            SpawnOptions::new().retry_strategy(RetryStrategy::none()),
        )
        .await?;
    let metadata = fetch_task_spawn_metadata(&queue, spawned.task_id).await?;
    assert_eq!(metadata.retry_strategy, Some(json!({ "kind": "none" })));

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "pending");
    assert_eq!(task.attempts, 2);

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.completed_payload, Some(json!({ "attempts": 2 })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn exponential_retry_strategy_delays_attempts() -> Result<()> {
    let (queue, client, pool) = test_client_with_single_connection_pool().await?;
    let base = utc("2024-05-01T10:00:00Z")?;
    set_fake_now(&pool, Some(base)).await?;
    let attempts = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("exponential-retry")
        .queue(&queue)
        .default_max_attempts(3);
    client.register(&task, {
        let attempts = Arc::clone(&attempts);
        move |(), _ctx| {
            let attempts = Arc::clone(&attempts);
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt < 3 {
                    Err(Error::message(format!("fail-{attempt}")))
                } else {
                    Ok(json!({ "attempts": attempt }))
                }
            }
        }
    })?;

    let spawned = client
        .spawn(
            &task,
            (),
            SpawnOptions::new().retry_strategy(
                RetryStrategy::exponential(Duration::from_secs(1), 3.0)
                    .with_max(Duration::from_secs(2)),
            ),
        )
        .await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "sleeping");
    assert_eq!(task.attempts, 2);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 0);

    set_fake_now(&pool, Some(base + ChronoDuration::seconds(1))).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "sleeping");
    assert_eq!(task.attempts, 3);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 0);

    set_fake_now(&pool, Some(base + ChronoDuration::seconds(3))).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.attempts, 3);
    assert_eq!(task.completed_payload, Some(json!({ "attempts": 3 })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn cancellation_policy_cancels_by_max_delay() -> Result<()> {
    let (queue, client, pool) = test_client_with_single_connection_pool().await?;
    let base = utc("2024-05-01T08:00:00Z")?;
    set_fake_now(&pool, Some(base)).await?;

    let task = Task::<(), Value>::new("max-delay-cancel").queue(&queue);
    client.register(&task, |(), _ctx| async move {
        Ok(json!({ "unexpected": true }))
    })?;

    let spawned = client
        .spawn(
            &task,
            (),
            SpawnOptions::new()
                .cancellation(CancellationPolicy::new().max_delay(Duration::from_secs(60))),
        )
        .await?;

    set_fake_now(&pool, Some(base + ChronoDuration::seconds(61))).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 0);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "cancelled");
    assert!(task.cancelled_at.is_some());

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn cancellation_policy_cancels_by_max_duration() -> Result<()> {
    let (queue, client, pool) = test_client_with_single_connection_pool().await?;
    let base = utc("2024-05-01T09:00:00Z")?;
    set_fake_now(&pool, Some(base)).await?;

    let task = Task::<(), Value>::new("max-duration-cancel")
        .queue(&queue)
        .default_max_attempts(3);
    client.register(&task, |(), _ctx| async move {
        Err::<Value, Error>(Error::message("fail until duration cancellation"))
    })?;

    let spawned = client
        .spawn(
            &task,
            (),
            SpawnOptions::new()
                .retry_strategy(RetryStrategy::fixed(Duration::from_secs(30)))
                .cancellation(CancellationPolicy::new().max_duration(Duration::from_secs(90))),
        )
        .await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "sleeping");
    assert_eq!(count_runs(&queue, spawned.task_id).await?, 2);

    set_fake_now(&pool, Some(base + ChronoDuration::seconds(91))).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 0);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "cancelled");
    assert!(task.cancelled_at.is_some());
    assert_eq!(count_runs(&queue, spawned.task_id).await?, 2);

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn default_task_cancellation_is_applied() -> Result<()> {
    let (queue, client, pool) = test_client_with_single_connection_pool().await?;
    let base = utc("2024-05-01T08:30:00Z")?;
    set_fake_now(&pool, Some(base)).await?;

    let task = Task::<(), Value>::new("default-cancel")
        .queue(&queue)
        .default_cancellation(CancellationPolicy::new().max_delay(Duration::from_secs(60)));
    client.register(&task, |(), _ctx| async move {
        Ok(json!({ "unexpected": true }))
    })?;

    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    let metadata = fetch_task_spawn_metadata(&queue, spawned.task_id).await?;
    assert_eq!(metadata.cancellation, Some(json!({ "max_delay": 60 })));

    set_fake_now(&pool, Some(base + ChronoDuration::seconds(61))).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 0);
    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "cancelled");
    assert!(task.cancelled_at.is_some());

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn unknown_task_is_deferred_by_default() -> Result<()> {
    let (queue, client) = test_client().await?;

    let spawned = client
        .spawn_named(
            "ghost-task",
            json!({ "value": 1 }),
            SpawnOptions::new().queue(&queue).max_attempts(1),
        )
        .await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "sleeping");
    assert_eq!(task.attempts, 1);

    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(run.state, "sleeping");
    assert!(run.failure_reason.is_none());
    assert!(run.available_at.is_some());

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn unknown_task_fail_policy_records_failure_payload() -> Result<()> {
    let (queue, client) = test_client().await?;

    let spawned = client
        .spawn_named(
            "ghost-task-fail",
            json!({ "value": 1 }),
            SpawnOptions::new().queue(&queue).max_attempts(1),
        )
        .await?;

    assert_eq!(
        client
            .work_batch(WorkBatchOptions::new().unknown_task_policy(UnknownTaskPolicy::Fail))
            .await?,
        1
    );

    let task = fetch_task(&queue, spawned.task_id).await?;
    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(task.state, "failed");
    assert_eq!(run.state, "failed");
    assert_eq!(
        run.failure_reason,
        Some(json!({
            "name": "TaskNotRegistered",
            "message": "task \"ghost-task-fail\" is not registered",
            "debug": "TaskNotRegistered(\"ghost-task-fail\")",
            "traceback": null,
        }))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn panic_failure_payload_records_diagnostic_shape() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("panic-payload")
        .queue(&queue)
        .default_max_attempts(1);
    client.register(&task, |(), _ctx| async move {
        std::panic::resume_unwind(Box::new(String::from("panic payload")))
    })?;

    let spawned = client.spawn(&task, (), SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(task.state, "failed");
    assert_eq!(run.state, "failed");
    assert_eq!(
        run.failure_reason,
        Some(json!({
            "name": "panic",
            "message": "panic payload",
            "debug": "task panicked: panic payload",
            "traceback": null,
        }))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn manual_cancel_pending_task_prevents_claim() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("pending-cancel").queue(&queue);
    client.register(&task, |(), _ctx| async move { Ok(json!({ "ok": true })) })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;
    client.cancel_task(spawned.task_id, None).await?;

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "cancelled");
    assert!(task.cancelled_at.is_some());
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 0);

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn manual_cancel_running_sleeping_terminal_and_missing_cases() -> Result<()> {
    let (queue, client) = test_client().await?;

    let running_started = Arc::new(Notify::new());
    let running_release = Arc::new(Notify::new());
    let running_task = Task::<(), Value>::new("running-cancel").queue(&queue);
    client.register(&running_task, {
        let running_started = Arc::clone(&running_started);
        let running_release = Arc::clone(&running_release);
        move |(), _ctx| {
            let running_started = Arc::clone(&running_started);
            let running_release = Arc::clone(&running_release);
            async move {
                running_started.notify_one();
                running_release.notified().await;
                Ok(json!({ "would_complete": true }))
            }
        }
    })?;

    let running = client.spawn(&running_task, (), SpawnOptions::new()).await?;
    let worker = client.start_worker(
        WorkerOptions::new()
            .poll_interval(Duration::from_millis(25))
            .fatal_on_lease_timeout(false),
    )?;
    tokio::time::timeout(Duration::from_secs(5), running_started.notified())
        .await
        .map_err(|_| Error::message("running cancel task did not start"))?;
    client.cancel_task(running.task_id, None).await?;
    running_release.notify_one();
    worker.close().await?;
    let running_row = fetch_task(&queue, running.task_id).await?;
    assert_eq!(running_row.state, "cancelled");
    assert!(running_row.cancelled_at.is_some());

    let sleeping_event = format!("sleeping_cancel_{queue}");
    let sleeping_task = Task::<(), Value>::new("sleeping-cancel").queue(&queue);
    client.register(&sleeping_task, {
        let sleeping_event = sleeping_event.clone();
        move |(), mut ctx| {
            let sleeping_event = sleeping_event.clone();
            async move {
                let payload: Value = ctx.await_event(&sleeping_event).await?;
                Ok(json!({ "payload": payload }))
            }
        }
    })?;
    let sleeping = client
        .spawn(&sleeping_task, (), SpawnOptions::new())
        .await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    assert_eq!(
        fetch_task(&queue, sleeping.task_id).await?.state,
        "sleeping"
    );
    client.cancel_task(sleeping.task_id, None).await?;
    let sleeping_row = fetch_task(&queue, sleeping.task_id).await?;
    assert_eq!(sleeping_row.state, "cancelled");
    assert!(sleeping_row.cancelled_at.is_some());
    client
        .emit_event(&sleeping_event, &json!({ "ignored": true }), None)
        .await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 0);

    let completed_task = Task::<(), Value>::new("completed-cancel-noop").queue(&queue);
    client.register(&completed_task, |(), _ctx| async move {
        Ok(json!({ "done": true }))
    })?;
    let completed = client
        .spawn(&completed_task, (), SpawnOptions::new())
        .await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    client.cancel_task(completed.task_id, None).await?;
    let completed_row = fetch_task(&queue, completed.task_id).await?;
    assert_eq!(completed_row.state, "completed");
    assert!(completed_row.cancelled_at.is_none());

    let failed_task = Task::<(), Value>::new("failed-cancel-noop")
        .queue(&queue)
        .default_max_attempts(1);
    client.register(&failed_task, |(), _ctx| async move {
        Err::<Value, Error>(Error::message("terminal failure"))
    })?;
    let failed = client.spawn(&failed_task, (), SpawnOptions::new()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);
    client.cancel_task(failed.task_id, None).await?;
    let failed_row = fetch_task(&queue, failed.task_id).await?;
    assert_eq!(failed_row.state, "failed");
    assert!(failed_row.cancelled_at.is_none());

    let missing_task_id = uuid::Uuid::new_v4();
    let missing = client.cancel_task(missing_task_id, None).await;
    assert!(
        matches!(missing, Err(Error::Database(err)) if format!("{err:?}").contains("not found"))
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn cancelled_terminal_race_does_not_record_task_failure() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("cancel-race").queue(&queue);
    client.register(&task, {
        let client = client.clone();
        move |(), ctx| {
            let client = client.clone();
            async move {
                client.cancel_task(ctx.task_id(), None).await?;
                Ok(json!({ "would_complete": true }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(task.state, "cancelled");
    assert!(task.cancelled_at.is_some());
    assert_eq!(run.state, "cancelled");
    assert!(run.failure_reason.is_none());
    assert_eq!(
        client.fetch_task_result(spawned.task_id, None).await?,
        Some(TaskResultSnapshot::Cancelled)
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn already_failed_terminal_race_does_not_overwrite_failure_payload() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("already-failed-race")
        .queue(&queue)
        .default_max_attempts(1);
    client.register(&task, {
        let queue = queue.clone();
        move |(), ctx| {
            let queue = queue.clone();
            async move {
                let pg = pg_client().await?;
                let run_id = ctx.run_id();
                let payload = json!({ "name": "ManualFailure", "message": "already failed" });
                let retry_at: Option<DateTime<Utc>> = None;
                pg.execute(
                    "SELECT absurd.fail_run($1, $2, $3, $4)",
                    &[&queue, &run_id, &payload, &retry_at],
                )
                .await?;
                Err::<Value, Error>(Error::message("worker should not overwrite this failure"))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;
    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(task.state, "failed");
    assert_eq!(run.state, "failed");
    assert_eq!(
        run.failure_reason,
        Some(json!({ "name": "ManualFailure", "message": "already failed" }))
    );
    assert_eq!(count_runs(&queue, spawned.task_id).await?, 1);

    client.drop_queue().await?;
    Ok(())
}

async fn pg_client() -> Result<tokio_postgres::Client> {
    let (pg, connection) = tokio_postgres::connect(&database_url(), tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(pg)
}

async fn fetch_task(queue: &str, task_id: uuid::Uuid) -> Result<TaskRow> {
    let pg = pg_client().await?;
    let query = format!(
        "SELECT state, attempts, max_attempts, completed_payload, cancelled_at FROM absurd.t_{queue} WHERE task_id = $1"
    );
    let row = pg.query_one(&query, &[&task_id]).await?;
    Ok(TaskRow {
        state: row.get(0),
        attempts: row.get(1),
        max_attempts: row.get(2),
        completed_payload: row.get(3),
        cancelled_at: row.get(4),
    })
}

async fn fetch_task_spawn_metadata(queue: &str, task_id: uuid::Uuid) -> Result<TaskSpawnMetadata> {
    let pg = pg_client().await?;
    let query =
        format!("SELECT retry_strategy, cancellation FROM absurd.t_{queue} WHERE task_id = $1");
    let row = pg.query_one(&query, &[&task_id]).await?;
    Ok(TaskSpawnMetadata {
        retry_strategy: row.get(0),
        cancellation: row.get(1),
    })
}

async fn fetch_run(queue: &str, run_id: uuid::Uuid) -> Result<RunRow> {
    let pg = pg_client().await?;
    let query = format!(
        "SELECT state, wake_event, nullif(available_at, 'infinity'::timestamptz), failure_reason FROM absurd.r_{queue} WHERE run_id = $1"
    );
    let row = pg.query_one(&query, &[&run_id]).await?;
    Ok(RunRow {
        state: row.get(0),
        wake_event: row.get(1),
        available_at: row.get(2),
        failure_reason: row.get(3),
    })
}

async fn fetch_checkpoints(queue: &str, task_id: uuid::Uuid) -> Result<Vec<(String, Value)>> {
    let pg = pg_client().await?;
    let query = format!(
        "SELECT checkpoint_name, state FROM absurd.c_{queue} WHERE task_id = $1 ORDER BY checkpoint_name"
    );
    let rows = pg.query(&query, &[&task_id]).await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}

async fn set_task_headers(queue: &str, task_id: uuid::Uuid, headers: Value) -> Result<()> {
    let pg = pg_client().await?;
    let query = format!("UPDATE absurd.t_{queue} SET headers = $2 WHERE task_id = $1");
    pg.execute(&query, &[&task_id, &headers]).await?;
    Ok(())
}

async fn count_tasks(queue: &str) -> Result<i64> {
    let pg = pg_client().await?;
    let query = format!("SELECT count(*) FROM absurd.t_{queue}");
    Ok(pg.query_one(&query, &[]).await?.get(0))
}

async fn count_events(queue: &str) -> Result<i64> {
    let pg = pg_client().await?;
    let query = format!("SELECT count(*) FROM absurd.e_{queue}");
    Ok(pg.query_one(&query, &[]).await?.get(0))
}

async fn count_runs(queue: &str, task_id: uuid::Uuid) -> Result<i64> {
    let pg = pg_client().await?;
    let query = format!("SELECT count(*) FROM absurd.r_{queue} WHERE task_id = $1");
    Ok(pg.query_one(&query, &[&task_id]).await?.get(0))
}

async fn queue_table_count(queue: &str) -> Result<i64> {
    let pg = pg_client().await?;
    let table_names = vec![
        format!("c_{queue}"),
        format!("e_{queue}"),
        format!("r_{queue}"),
        format!("t_{queue}"),
        format!("w_{queue}"),
    ];
    Ok(pg
        .query_one(
            "SELECT count(*) FROM pg_tables WHERE schemaname = 'absurd' AND tablename = ANY($1::text[])",
            &[&table_names],
        )
        .await?
        .get(0))
}

async fn queue_relation_kinds(queue: &str) -> Result<HashMap<String, String>> {
    let pg = pg_client().await?;
    let relation_names = vec![
        format!("c_{queue}"),
        format!("e_{queue}"),
        format!("i_{queue}"),
        format!("r_{queue}"),
        format!("t_{queue}"),
        format!("w_{queue}"),
    ];
    let rows = pg
        .query(
            "SELECT c.relname, c.relkind::text
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'absurd' AND c.relname = ANY($1::text[])",
            &[&relation_names],
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect())
}
