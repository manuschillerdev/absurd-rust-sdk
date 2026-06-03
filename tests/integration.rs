use absurd_rust_sdk::{AwaitEventOptions, Client, Error, Result, Task, WorkBatchOptions};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Params {
    value: i64,
}

#[derive(Debug, Serialize)]
struct Output {
    doubled: i64,
}

fn database_url() -> String {
    std::env::var("ABSURD_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql://localhost/absurd_test".to_string())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn basic_typed_task_round_trip() -> Result<()> {
    let queue = format!("rust_sdk_{}", uuid::Uuid::new_v4().simple());
    let client = Client::connect_queue(database_url(), &queue).await?;
    client.create_queue().await?;

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

    let payload = fetch_task_payload(&queue, spawned.task_id).await?;
    assert_eq!(payload.state, "completed");
    assert_eq!(
        payload.completed_payload,
        serde_json::json!({ "doubled": 42 })
    );

    client.drop_queue().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires a Postgres database initialized with Absurd SQL"]
async fn event_timeout_resume_returns_event_timeout() -> Result<()> {
    let queue = format!("rust_sdk_{}", uuid::Uuid::new_v4().simple());
    let client = Client::connect_queue(database_url(), &queue).await?;
    client.create_queue().await?;

    let task = Task::<(), serde_json::Value>::new("timeout-flow").queue(&queue);
    client.register(&task, |(), mut ctx| async move {
        let options = AwaitEventOptions::new()
            .step_name("wait")
            .timeout(Duration::ZERO);

        match ctx
            .await_event_with_options::<serde_json::Value>("never", options)
            .await
        {
            Err(Error::EventTimeout { event }) if event == "never" => {
                Ok(serde_json::json!({ "timed_out": true }))
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

    let payload = fetch_task_payload(&queue, spawned.task_id).await?;
    assert_eq!(payload.state, "completed");
    assert_eq!(
        payload.completed_payload,
        serde_json::json!({ "timed_out": true })
    );

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

    let queue = format!("rust_sdk_{}", uuid::Uuid::new_v4().simple());
    let client = Client::connect_queue(database_url(), &queue).await?;
    client.create_queue().await?;

    let task = Task::<(), serde_json::Value>::new("timeout-loop").queue(&queue);
    client.register(&task, |(), mut ctx| async move {
        let mut stages = Vec::with_capacity(2);

        for cycle in 0..2 {
            let event_name = format!("wake:{cycle}");
            let options = AwaitEventOptions::new()
                .step_name(format!("await-{cycle}"))
                .timeout(Duration::ZERO);

            match ctx
                .await_event_with_options::<serde_json::Value>(&event_name, options)
                .await
            {
                Err(Error::EventTimeout { .. }) => stages.push(format!("timeout-{cycle}")),
                Err(err) => return Err(err),
                Ok(_) => stages.push(format!("event-{cycle}")),
            }
        }

        Ok(serde_json::json!({ "stages": stages }))
    })?;

    let spawned = client.spawn(&task, (), Default::default()).await?;

    let first = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(first, 1);

    let second = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(second, 1);

    let run = fetch_run_state(&queue, spawned.run_id).await?;
    assert_eq!(run.state, "sleeping");
    assert_eq!(run.wake_event.as_deref(), Some("wake:1"));

    let third = client.work_batch(WorkBatchOptions::new()).await?;
    assert_eq!(third, 1);

    let payload = fetch_task_payload(&queue, spawned.task_id).await?;
    assert_eq!(payload.state, "completed");
    assert_eq!(
        payload.completed_payload,
        serde_json::json!({ "stages": ["timeout-0", "timeout-1"] })
    );

    assert_eq!(fetch_wait_count(&queue).await?, 0);

    client.drop_queue().await?;
    Ok(())
}

struct TaskPayload {
    state: String,
    completed_payload: serde_json::Value,
}

struct RunState {
    state: String,
    wake_event: Option<String>,
}

async fn fetch_task_payload(queue: &str, task_id: uuid::Uuid) -> Result<TaskPayload> {
    let pg = connect_pg().await?;
    let query = format!("SELECT state, completed_payload FROM absurd.t_{queue} WHERE task_id = $1");
    let row = pg.query_one(&query, &[&task_id]).await?;
    Ok(TaskPayload {
        state: row.get(0),
        completed_payload: row.get(1),
    })
}

async fn fetch_run_state(queue: &str, run_id: uuid::Uuid) -> Result<RunState> {
    let pg = connect_pg().await?;
    let query = format!("SELECT state, wake_event FROM absurd.r_{queue} WHERE run_id = $1");
    let row = pg.query_one(&query, &[&run_id]).await?;
    Ok(RunState {
        state: row.get(0),
        wake_event: row.get(1),
    })
}

async fn fetch_wait_count(queue: &str) -> Result<i64> {
    let pg = connect_pg().await?;
    let query = format!("SELECT COUNT(*) FROM absurd.w_{queue}");
    let row = pg.query_one(&query, &[]).await?;
    Ok(row.get(0))
}

async fn await_event_exposes_timed_out() -> Result<bool> {
    let pg = connect_pg().await?;
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

async fn connect_pg() -> Result<tokio_postgres::Client> {
    let (pg, connection) = tokio_postgres::connect(&database_url(), tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(pg)
}
