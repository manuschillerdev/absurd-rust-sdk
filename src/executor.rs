use crate::client::{RegisteredTask, TaskHandler};
use crate::context::TaskContext;
use crate::error::{Error, Result, map_database_error};
use crate::hooks::{Hooks, TaskExecution};
use crate::types::{
    ClaimedTask, Json, UnknownTaskPolicy, WorkerError, WorkerErrorHandler, WorkerErrorKind,
};
use deadpool_postgres::Pool;
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct TaskExecutionConfig {
    pub hooks: Hooks,
    pub claim_timeout: Duration,
    pub unknown_task_policy: UnknownTaskPolicy,
    pub fatal_on_lease_timeout: bool,
    pub on_error: Option<WorkerErrorHandler>,
}

pub(crate) async fn execute_claimed_catching(
    pool: Pool,
    worker_queue: String,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
    task: ClaimedTask,
    config: TaskExecutionConfig,
) -> Result<()> {
    let run_id = task.run_id;
    let task_name = task.task_name.clone();
    let task_id = task.task_id;
    let panic_pool = pool.clone();
    let panic_queue = worker_queue.clone();
    let panic_on_error = config.on_error.clone();

    let inner = tokio::spawn(execute_claimed(pool, worker_queue, registry, task, config));

    match inner.await {
        Ok(result) => result,
        Err(join_error) if join_error.is_panic() => {
            let message = panic_message(join_error);
            tracing::error!(%task_id, %run_id, %task_name, "task panicked: {message}");
            let result = fail_run_payload(
                &panic_pool,
                &panic_queue,
                &run_id,
                failure_payload(
                    "panic",
                    message.clone(),
                    format!("task panicked: {message}"),
                ),
            )
            .await;
            let err = Error::message(format!("task panicked: {message}"));
            report_execution_error(panic_on_error.as_ref(), &err);
            result
        }
        Err(join_error) => Err(Error::Join(join_error)),
    }
}

async fn execute_claimed(
    pool: Pool,
    worker_queue: String,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
    mut task: ClaimedTask,
    config: TaskExecutionConfig,
) -> Result<()> {
    let registration = registry
        .read()
        .map_err(|_| Error::Config("task registry lock poisoned".to_string()))?
        .get(&task.task_name)
        .cloned();

    let Some(registration) = registration else {
        return handle_unknown_task(&pool, &worker_queue, &task, config.unknown_task_policy).await;
    };

    if registration.queue_name != worker_queue {
        let err = Error::Config(format!(
            "task {:?} is registered for queue {:?}, but was claimed by worker queue {:?}",
            task.task_name, registration.queue_name, worker_queue
        ));
        return fail_run_error(&pool, &worker_queue, &task.run_id, &err).await;
    }

    let run_id = task.run_id;
    let task_label = format!("{} ({})", task.task_name, task.task_id);
    let params = std::mem::take(&mut task.params);

    let (lease_tx, lease_rx) = watch::channel(config.claim_timeout);
    let watchdog = tokio::spawn(lease_watchdog(
        lease_rx,
        task_label,
        config.fatal_on_lease_timeout,
    ));

    let ctx = TaskContext::new(
        pool.clone(),
        registration.queue_name.clone(),
        task,
        config.claim_timeout,
        Some(lease_tx),
    )
    .await;

    let result = match ctx {
        Ok(ctx) => run_handler(&registration.handler, params, ctx, &config.hooks).await,
        Err(err) => Err(err),
    };

    watchdog.abort();

    match result {
        Ok(value) => complete_run(&pool, &worker_queue, &run_id, value).await,
        Err(err) if err.is_control_flow() => Ok(()),
        Err(err) => {
            let result = fail_run_error(&pool, &worker_queue, &run_id, &err).await;
            report_execution_error(config.on_error.as_ref(), &err);
            result
        }
    }
}

async fn run_handler(
    handler: &TaskHandler,
    params: Json,
    ctx: TaskContext,
    hooks: &Hooks,
) -> Result<Json> {
    let handler = Arc::clone(handler);
    let execute: TaskExecution = Box::new(move |ctx| handler(params, ctx));

    if let Some(wrap) = hooks.wrap_task_execution_hook() {
        wrap(ctx, execute).await
    } else {
        execute(ctx).await
    }
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
            let seconds = delay.as_secs_f64();
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
            let message = format!("task {:?} is not registered", task.task_name);
            let debug = format!("TaskNotRegistered({:?})", task.task_name);
            fail_run_payload(
                pool,
                worker_queue,
                &task.run_id,
                failure_payload("TaskNotRegistered", message, debug),
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
    fail_run_payload(pool, queue_name, run_id, error_failure_payload(err)).await
}

fn error_failure_payload(err: &Error) -> Json {
    failure_payload(error_name(err), err.to_string(), format!("{err:?}"))
}

fn failure_payload(
    name: impl Into<String>,
    message: impl Into<String>,
    debug: impl Into<String>,
) -> Json {
    json!({
        "name": name.into(),
        "message": message.into(),
        "debug": debug.into(),
        "traceback": null,
    })
}

async fn fail_run_payload(
    pool: &Pool,
    queue_name: &str,
    run_id: &Uuid,
    payload: Json,
) -> Result<()> {
    let client = pool.get().await?;
    let retry_at: Option<chrono::DateTime<chrono::Utc>> = None;
    match client
        .execute(
            "SELECT absurd.fail_run($1, $2, $3, $4)",
            &[&queue_name, run_id, &payload, &retry_at],
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

fn report_execution_error(on_error: Option<&WorkerErrorHandler>, error: &Error) {
    if let Some(on_error) = on_error {
        on_error(WorkerError {
            kind: WorkerErrorKind::Execution,
            error,
        });
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
        Error::TaskNotFound { .. } => "TaskNotFound",
        Error::TaskResultTimeout { .. } => "TaskResultTimeout",
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

    #[test]
    fn error_failure_payload_includes_diagnostics() {
        let err = Error::message("boom");
        let payload = error_failure_payload(&err);

        assert_eq!(payload["name"], json!("Error"));
        assert_eq!(payload["message"], json!("boom"));
        assert_eq!(payload["traceback"], Json::Null);
        assert!(
            payload["debug"]
                .as_str()
                .is_some_and(|debug| { debug.contains("Message") && debug.contains("boom") })
        );
    }

    #[test]
    fn manual_failure_payload_preserves_sql_shape_with_debug() {
        let payload = failure_payload("TaskNotRegistered", "missing", "debug details");

        assert_eq!(payload["name"], json!("TaskNotRegistered"));
        assert_eq!(payload["message"], json!("missing"));
        assert_eq!(payload["debug"], json!("debug details"));
        assert_eq!(payload["traceback"], Json::Null);
    }
}
