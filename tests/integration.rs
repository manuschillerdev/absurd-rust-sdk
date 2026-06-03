use absurd_rust_sdk::{Client, Result, Task, WorkBatchOptions};
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
    client.create_queue(None).await?;

    let task = Task::<Params, Output>::new("double").queue(&queue);
    client.register(task.clone(), |params, mut ctx| async move {
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

    client.drop_queue(None).await?;
    Ok(())
}
