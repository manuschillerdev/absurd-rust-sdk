use crate::client::{RegisteredTask, claim_tasks};
use crate::error::{Error, Result};
use crate::executor::{TaskExecutionConfig, execute_claimed_catching};
use crate::hooks::Hooks;
use crate::types::{WorkerError, WorkerErrorHandler, WorkerErrorKind, WorkerOptions};
use deadpool_postgres::Pool;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::watch;
use tokio::task::JoinSet;

pub struct Worker {
    shutdown_tx: watch::Sender<bool>,
    handle: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl Worker {
    pub(crate) fn start(
        pool: Pool,
        queue_name: String,
        registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
        hooks: Hooks,
        options: WorkerOptions,
    ) -> Result<Self> {
        let options = options.normalized()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(worker_loop(
            pool,
            queue_name,
            registry,
            hooks,
            options,
            shutdown_rx,
        ));
        Ok(Self {
            shutdown_tx,
            handle: Some(handle),
        })
    }

    pub async fn close(mut self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        if let Some(handle) = self.handle.take() {
            handle.await.map_err(Error::Join)?
        } else {
            Ok(())
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

async fn worker_loop(
    pool: Pool,
    queue_name: String,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
    hooks: Hooks,
    options: crate::types::NormalizedWorkerOptions,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut running = JoinSet::new();

    loop {
        drain_finished(&mut running, &options.on_error).await?;

        if *shutdown_rx.borrow() {
            break;
        }

        let available = options.concurrency.saturating_sub(running.len());
        if available == 0 {
            if wait_for_capacity_or_shutdown(&mut running, &mut shutdown_rx, &options.on_error)
                .await?
            {
                break;
            }
            continue;
        }

        let to_claim = available.min(options.batch_size).min(i32::MAX as usize) as i32;
        let tasks = match claim_tasks(
            &pool,
            &queue_name,
            &options.worker_id,
            options.claim_timeout_seconds,
            to_claim,
        )
        .await
        {
            Ok(tasks) => tasks,
            Err(err) => {
                report_worker_error(&options.on_error, WorkerErrorKind::Claim, &err);
                if wait_poll_interval_or_shutdown(
                    &mut running,
                    &mut shutdown_rx,
                    options.poll_interval,
                    &options.on_error,
                )
                .await?
                {
                    break;
                }
                continue;
            }
        };

        if tasks.is_empty() {
            if wait_poll_interval_or_shutdown(
                &mut running,
                &mut shutdown_rx,
                options.poll_interval,
                &options.on_error,
            )
            .await?
            {
                break;
            }
            continue;
        }

        for task in tasks {
            let pool = pool.clone();
            let queue_name = queue_name.clone();
            let registry = registry.clone();
            let hooks = hooks.clone();
            let config = TaskExecutionConfig {
                hooks,
                claim_timeout: options.claim_timeout,
                unknown_task_policy: options.unknown_task_policy,
                fatal_on_lease_timeout: options.fatal_on_lease_timeout,
                on_error: Some(Arc::clone(&options.on_error)),
            };
            running.spawn(async move {
                execute_claimed_catching(pool, queue_name, registry, task, config).await
            });
        }
    }

    while let Some(result) = running.join_next().await {
        report_task_result(&options.on_error, result)?;
    }

    Ok(())
}

async fn drain_finished(
    running: &mut JoinSet<Result<()>>,
    on_error: &WorkerErrorHandler,
) -> Result<()> {
    while let Some(result) = running.try_join_next() {
        report_task_result(on_error, result)?;
    }
    Ok(())
}

async fn wait_for_capacity_or_shutdown(
    running: &mut JoinSet<Result<()>>,
    shutdown_rx: &mut watch::Receiver<bool>,
    on_error: &WorkerErrorHandler,
) -> Result<bool> {
    tokio::select! {
        changed = shutdown_rx.changed() => Ok(changed.is_err() || *shutdown_rx.borrow()),
        result = running.join_next(), if !running.is_empty() => {
            if let Some(result) = result {
                report_task_result(on_error, result)?;
            }
            Ok(false)
        }
    }
}

async fn wait_poll_interval_or_shutdown(
    running: &mut JoinSet<Result<()>>,
    shutdown_rx: &mut watch::Receiver<bool>,
    poll_interval: std::time::Duration,
    on_error: &WorkerErrorHandler,
) -> Result<bool> {
    tokio::select! {
        changed = shutdown_rx.changed() => Ok(changed.is_err() || *shutdown_rx.borrow()),
        _ = tokio::time::sleep(poll_interval) => Ok(false),
        result = running.join_next(), if !running.is_empty() => {
            if let Some(result) = result {
                report_task_result(on_error, result)?;
            }
            Ok(false)
        }
    }
}

fn report_task_result(
    on_error: &WorkerErrorHandler,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => {
            report_worker_error(on_error, WorkerErrorKind::Execution, &err);
            Ok(())
        }
        Err(err) => {
            let err = Error::Join(err);
            report_worker_error(on_error, WorkerErrorKind::Execution, &err);
            Err(err)
        }
    }
}

fn report_worker_error(on_error: &WorkerErrorHandler, kind: WorkerErrorKind, error: &Error) {
    on_error(WorkerError { kind, error });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn report_worker_error_invokes_handler_for_claim_failures() {
        let seen = Arc::new(AtomicUsize::new(0));
        let handler_seen = Arc::clone(&seen);
        let handler: WorkerErrorHandler = Arc::new(move |worker_error| {
            if worker_error.kind == WorkerErrorKind::Claim {
                handler_seen.fetch_add(1, Ordering::SeqCst);
            }
        });
        let err = Error::message("claim failed");

        report_worker_error(&handler, WorkerErrorKind::Claim, &err);

        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn report_task_result_invokes_handler_for_execution_failures() -> Result<()> {
        let seen = Arc::new(AtomicUsize::new(0));
        let handler_seen = Arc::clone(&seen);
        let handler: WorkerErrorHandler = Arc::new(move |worker_error| {
            if worker_error.kind == WorkerErrorKind::Execution {
                handler_seen.fetch_add(1, Ordering::SeqCst);
            }
        });

        report_task_result(&handler, Ok(Err(Error::message("execution failed"))))?;

        assert_eq!(seen.load(Ordering::SeqCst), 1);
        Ok(())
    }
}
