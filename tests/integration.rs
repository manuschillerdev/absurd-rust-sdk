use absurd_rust_sdk::{
    AwaitEventOptions, Client, Error, Result, RetryStrategy, SpawnOptions, Task, WorkBatchOptions,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

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
async fn sleep_until_suspends_then_resumes_from_checkpoint() -> Result<()> {
    let (queue, client) = test_client().await?;
    let executions = Arc::new(AtomicUsize::new(0));

    let task = Task::<(), Value>::new("sleepy").queue(&queue);
    client.register(&task, {
        let executions = Arc::clone(&executions);
        move |(), mut ctx| {
            let executions = Arc::clone(&executions);
            async move {
                let execution = executions.fetch_add(1, Ordering::SeqCst) + 1;
                let wake_at = Utc::now() + ChronoDuration::seconds(1);
                ctx.sleep_until("pause", wake_at).await?;
                Ok(json!({ "executions": execution }))
            }
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(task.state, "sleeping");
    assert_eq!(run.state, "sleeping");
    assert!(run.wake_event.is_none());

    let checkpoints = fetch_checkpoints(&queue, spawned.task_id).await?;
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].0, "pause");
    let checkpoint_wake: DateTime<Utc> = serde_json::from_value(checkpoints[0].1.clone())?;
    assert_eq!(run.available_at, Some(checkpoint_wake));

    assert_eq!(client.work_batch(WorkBatchOptions::new()).await?, 0);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
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
async fn event_timeout_resume_returns_event_timeout() -> Result<()> {
    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("timeout-flow").queue(&queue);
    client.register(&task, |(), mut ctx| async move {
        let options = AwaitEventOptions::new()
            .step_name("wait")
            .timeout(Duration::ZERO);

        match ctx
            .await_event_with_options::<Value>("never", options)
            .await
        {
            Err(Error::EventTimeout { event }) if event == "never" => {
                Ok(json!({ "timed_out": true }))
            }
            Err(Error::EventTimeout { event }) => Err(Error::message(format!(
                "unexpected timeout event {event:?}"
            ))),
            Err(err) => Err(err),
            Ok(payload) => Err(Error::message(format!(
                "expected event timeout, got payload {payload}"
            ))),
        }
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    let first = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(first, 1);
    let second = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(second, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(task.completed_payload, Some(json!({ "timed_out": true })));

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Absurd SQL with await_event timed_out checkpoint support"]
async fn event_timeout_checkpoint_preserves_progress_across_multiple_awaits() -> Result<()> {
    if !await_event_exposes_timed_out().await? {
        eprintln!("skipping: absurd.await_event does not expose timed_out yet");
        return Ok(());
    }

    let (queue, client) = test_client().await?;

    let task = Task::<(), Value>::new("timeout-loop").queue(&queue);
    client.register(&task, |(), mut ctx| async move {
        let mut stages = Vec::with_capacity(2);

        for cycle in 0..2 {
            let event_name = format!("wake:{cycle}");
            let options = AwaitEventOptions::new()
                .step_name(format!("await-{cycle}"))
                .timeout(Duration::ZERO);

            match ctx
                .await_event_with_options::<Value>(&event_name, options)
                .await
            {
                Err(Error::EventTimeout { .. }) => stages.push(format!("timeout-{cycle}")),
                Err(err) => return Err(err),
                Ok(_) => stages.push(format!("event-{cycle}")),
            }
        }

        Ok(json!({ "stages": stages }))
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    let first = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(first, 1);

    let second = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(second, 1);

    let run = fetch_run(&queue, spawned.run_id).await?;
    assert_eq!(run.state, "sleeping");
    assert_eq!(run.wake_event.as_deref(), Some("wake:1"));

    let third = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(third, 1);

    let task = fetch_task(&queue, spawned.task_id).await?;
    assert_eq!(task.state, "completed");
    assert_eq!(
        task.completed_payload,
        Some(json!({ "stages": ["timeout-0", "timeout-1"] }))
    );

    assert_eq!(fetch_wait_count(&queue).await?, 0);

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
async fn fixed_retry_strategy_delays_next_attempt() -> Result<()> {
    let (queue, client) = test_client().await?;
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

    tokio::time::sleep(Duration::from_millis(1_100)).await;
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
        "SELECT state, attempts, completed_payload, cancelled_at FROM absurd.t_{queue} WHERE task_id = $1"
    );
    let row = pg.query_one(&query, &[&task_id]).await?;
    Ok(TaskRow {
        state: row.get(0),
        attempts: row.get(1),
        completed_payload: row.get(2),
        cancelled_at: row.get(3),
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

async fn count_tasks(queue: &str) -> Result<i64> {
    let pg = pg_client().await?;
    let query = format!("SELECT count(*) FROM absurd.t_{queue}");
    Ok(pg.query_one(&query, &[]).await?.get(0))
}

async fn fetch_wait_count(queue: &str) -> Result<i64> {
    let pg = pg_client().await?;
    let query = format!("SELECT count(*) FROM absurd.w_{queue}");
    Ok(pg.query_one(&query, &[]).await?.get(0))
}

async fn await_event_exposes_timed_out() -> Result<bool> {
    let pg = pg_client().await?;
    let row = pg
        .query_one(
            "SELECT pg_get_function_result(
                'absurd.await_event(text, uuid, uuid, text, text, integer)'::regprocedure
            )",
            &[],
        )
        .await?;
    let result: String = row.get(0);
    Ok(result.contains("timed_out"))
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
