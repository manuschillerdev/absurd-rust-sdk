use crate::client::{RegisteredTask, TaskHandler};
use crate::context::TaskContext;
use crate::error::{Error, Result, map_database_error};
use crate::types::{ClaimedTask, Json, UnknownTaskPolicy};
use deadpool_postgres::Pool;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

pub(crate) async fn execute_claimed_catching(
    pool: Pool,
    worker_queue: String,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
    task: ClaimedTask,
    claim_timeout: Duration,
    unknown_task_policy: UnknownTaskPolicy,
    fatal_on_lease_timeout: bool,
) -> Result<()> {
    let run_id = task.run_id;
    let task_name = task.task_name.clone();
    let task_id = task.task_id;
    let panic_pool = pool.clone();
    let panic_queue = worker_queue.clone();

    let inner = tokio::spawn(execute_claimed(
        pool,
        worker_queue,
        registry,
        task,
        claim_timeout,
        unknown_task_policy,
        fatal_on_lease_timeout,
    ));

    match inner.await {
        Ok(result) => result,
        Err(join_error) if join_error.is_panic() => {
            let message = panic_message(join_error);
            tracing::error!(%task_id, %run_id, %task_name, "task panicked: {message}");
            fail_run_payload(
                &panic_pool,
                &panic_queue,
                &run_id,
                json!({
                    "name": "panic",
                    "message": message,
                    "traceback": null,
                }),
            )
            .await
        }
        Err(join_error) => Err(Error::Join(join_error)),
    }
}

async fn execute_claimed(
    pool: Pool,
    worker_queue: String,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
    task: ClaimedTask,
    claim_timeout: Duration,
    unknown_task_policy: UnknownTaskPolicy,
    fatal_on_lease_timeout: bool,
) -> Result<()> {
    let registration = registry
        .read()
        .map_err(|_| Error::Config("task registry lock poisoned".to_string()))?
        .get(&task.task_name)
        .cloned();

    let Some(registration) = registration else {
        return handle_unknown_task(&pool, &worker_queue, &task, unknown_task_policy).await;
    };

    if registration.queue_name != worker_queue {
        let err = Error::Config(format!(
            "task {:?} is registered for queue {:?}, but was claimed by worker queue {:?}",
            task.task_name, registration.queue_name, worker_queue
        ));
        return fail_run_error(&pool, &worker_queue, &task.run_id, &err).await;
    }

    let (lease_tx, lease_rx) = watch::channel(claim_timeout);
    let watchdog = tokio::spawn(lease_watchdog(
        lease_rx,
        format!("{} ({})", task.task_name, task.task_id),
        fatal_on_lease_timeout,
    ));

    let ctx = TaskContext::new(
        pool.clone(),
        registration.queue_name.clone(),
        task.clone(),
        claim_timeout,
        Some(lease_tx),
    )
    .await;

    let result = match ctx {
        Ok(ctx) => run_handler(&registration.handler, task.params.clone(), ctx).await,
        Err(err) => Err(err),
    };

    watchdog.abort();

    match result {
        Ok(value) => complete_run(&pool, &worker_queue, &task.run_id, value).await,
        Err(Error::Suspended | Error::Cancelled | Error::AlreadyFailed) => Ok(()),
        Err(err) => fail_run_error(&pool, &worker_queue, &task.run_id, &err).await,
    }
}

async fn run_handler(handler: &TaskHandler, params: Json, ctx: TaskContext) -> Result<Json> {
    handler(params, ctx).await
}

async fn handle_unknown_task(
    pool: &Pool,
    worker_queue: &str,
    task: &ClaimedTask,
    policy: UnknownTaskPolicy,
) -> Result<()> {
    match policy {
        UnknownTaskPolicy::Defer => {
            let delay = unknown_task_defer_delay(task.run_id);
            let seconds = delay.as_secs().min(i32::MAX as u64) as i32;
            let client = pool.get().await?;
            client
                .execute(
                    "SELECT absurd.schedule_run($1, $2, absurd.current_time() + make_interval(secs => $3))",
                    &[&worker_queue, &task.run_id, &seconds],
                )
                .await
                .map_err(map_database_error)?;
            tracing::warn!(
                task_name = %task.task_name,
                task_id = %task.task_id,
                run_id = %task.run_id,
                ?delay,
                "claimed unknown task; deferred run"
            );
            Ok(())
        }
        UnknownTaskPolicy::Fail => {
            fail_run_payload(
                pool,
                worker_queue,
                &task.run_id,
                json!({
                    "name": "TaskNotRegistered",
                    "message": format!("task {:?} is not registered", task.task_name),
                    "traceback": null,
                }),
            )
            .await
        }
    }
}

async fn complete_run(pool: &Pool, queue_name: &str, run_id: &Uuid, result: Json) -> Result<()> {
    let client = pool.get().await?;
    match client
        .execute(
            "SELECT absurd.complete_run($1, $2, $3)",
            &[&queue_name, run_id, &result],
        )
        .await
        .map_err(map_database_error)
    {
        Err(err) if err.is_terminal_state_race() => Ok(()),
        other => other.map(|_| ()),
    }
}

async fn fail_run_error(pool: &Pool, queue_name: &str, run_id: &Uuid, err: &Error) -> Result<()> {
    fail_run_payload(
        pool,
        queue_name,
        run_id,
        json!({
            "name": error_name(err),
            "message": err.to_string(),
            "traceback": null,
        }),
    )
    .await
}

async fn fail_run_payload(
    pool: &Pool,
    queue_name: &str,
    run_id: &Uuid,
    payload: Json,
) -> Result<()> {
    let client = pool.get().await?;
    match client
        .execute(
            "SELECT absurd.fail_run($1, $2, $3, $4)",
            &[&queue_name, run_id, &payload, &Option::<String>::None],
        )
        .await
        .map_err(map_database_error)
    {
        Err(err) if err.is_terminal_state_race() => Ok(()),
        other => other.map(|_| ()),
    }
}

async fn lease_watchdog(
    mut rx: watch::Receiver<Duration>,
    task_label: String,
    fatal_on_lease_timeout: bool,
) {
    loop {
        let lease = *rx.borrow();
        tokio::select! {
            changed = rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep(lease) => {
                tracing::warn!(task = %task_label, ?lease, "task exceeded claim timeout without checkpoint or heartbeat");
                if fatal_on_lease_timeout {
                    tokio::select! {
                        changed = rx.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                        _ = tokio::time::sleep(lease) => {
                            tracing::error!(task = %task_label, ?lease, "task exceeded claim timeout by more than 100%; terminating process");
                            std::process::exit(1);
                        }
                    }
                }
            }
        }
    }
}

fn unknown_task_defer_delay(run_id: Uuid) -> Duration {
    let mut hash = 0x811c9dc5u32;
    for byte in run_id.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    Duration::from_secs(15 + u64::from(hash % 16))
}

fn error_name(err: &Error) -> &'static str {
    match err {
        Error::Database(_) => "Database",
        Error::Pool(_) => "Pool",
        Error::Serialization(_) => "Serialization",
        Error::Config(_) => "Config",
        Error::TaskNotRegistered(_) => "TaskNotRegistered",
        Error::TaskAlreadyRegistered(_) => "TaskAlreadyRegistered",
        Error::Suspended => "Suspended",
        Error::Cancelled => "Cancelled",
        Error::AlreadyFailed => "AlreadyFailed",
        Error::EventTimeout { .. } => "EventTimeout",
        Error::Join(_) => "Join",
        Error::InvalidHeaders => "InvalidHeaders",
        Error::Message(_) => "Error",
    }
}

fn panic_message(join_error: tokio::task::JoinError) -> String {
    let panic = join_error.into_panic();
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "task panicked".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_defer_delay_is_bounded() {
        let delay = unknown_task_defer_delay(Uuid::nil());
        assert!(delay >= Duration::from_secs(15));
        assert!(delay <= Duration::from_secs(30));
    }
}
