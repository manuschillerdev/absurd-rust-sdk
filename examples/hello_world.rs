use absurd_rust_sdk::{Client, Result, Task, WorkBatchOptions};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Deserialize, Serialize)]
struct HelloParams {
    name: String,
}

#[derive(Debug, Serialize)]
struct HelloResult {
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let client = Client::from_env_queue("default").await?;
    client.create_queue(None).await?;

    let hello = Task::<HelloParams, HelloResult>::new("hello-world");

    client.register(hello.clone(), |params, mut ctx| async move {
        let greeting: String = ctx
            .step("build-greeting", || async move {
                Ok(format!("hello, {}", params.name))
            })
            .await?;

        Ok(HelloResult { message: greeting })
    })?;

    let spawned = client
        .spawn(
            &hello,
            HelloParams {
                name: "Absurd".to_string(),
            },
            Default::default(),
        )
        .await?;

    println!("spawned task {} run {}", spawned.task_id, spawned.run_id);

    client
        .work_batch(WorkBatchOptions::new().claim_timeout(Duration::from_secs(30)))
        .await?;

    Ok(())
}
