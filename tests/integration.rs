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
async fn event_timeout_can_be_caught_without_recreating_wait() -> Result<()> {
    let queue = format!("rust_sdk_{}", uuid::Uuid::new_v4().simple());
    let client = Client::connect_queue(database_url(), &queue).await?;
    client.create_queue().await?;

    let task = Task::<(), serde_json::Value>::new("timeout-flow").queue(&queue);
    client.register(&task, |(), mut ctx| async move {
        let options = AwaitEventOptions::new()
            .step_name("wait")
            .timeout(Duration::ZERO);

        match ctx
            .await_event_with_options::<serde_json::Value>("never", options.clone())
            .await
        {
            Err(Error::EventTimeout { .. }) => {}
            other => return other.map(|_| serde_json::json!({ "unexpected": true })),
        }

        let payload: serde_json::Value = ctx.await_event_with_options("never", options).await?;

        Ok(serde_json::json!({
            "timed_out": true,
            "second_payload": payload,
        }))
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
        serde_json::json!({ "timed_out": true, "second_payload": null })
    );

    client.drop_queue().await?;
    Ok(())
}

struct TaskPayload {
    state: String,
    completed_payload: serde_json::Value,
}

async fn fetch_task_payload(queue: &str, task_id: uuid::Uuid) -> Result<TaskPayload> {
    let (pg, connection) = tokio_postgres::connect(&database_url(), tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let query = format!("SELECT state, completed_payload FROM absurd.t_{queue} WHERE task_id = $1");
    let row = pg.query_one(&query, &[&task_id]).await?;
    Ok(TaskPayload {
        state: row.get(0),
        completed_payload: row.get(1),
    })
}
