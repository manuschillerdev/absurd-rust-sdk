use crate::client::{RegisteredTask, claim_tasks};
use crate::error::{Error, Result};
use crate::executor::execute_claimed_catching;
use crate::types::WorkerOptions;
use deadpool_postgres::Pool;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::watch;
use tokio::task::JoinSet;

pub struct Worker {
    shutdown_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<Result<()>>,
}

impl Worker {
    pub(crate) fn start(
        pool: Pool,
        queue_name: String,
        registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
        options: WorkerOptions,
    ) -> Self {
        let options = options.normalized();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(worker_loop(
            pool,
            queue_name,
            registry,
            options,
            shutdown_rx,
        ));
        Self {
            shutdown_tx,
            handle,
        }
    }

    pub async fn close(self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);
        self.handle.await.map_err(Error::Join)?
    }
}

async fn worker_loop(
    pool: Pool,
    queue_name: String,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
    options: crate::types::NormalizedWorkerOptions,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let mut running = JoinSet::new();

    loop {
        drain_finished(&mut running).await?;

        if *shutdown_rx.borrow() {
            break;
        }

        let available = options.concurrency.saturating_sub(running.len());
        if available == 0 {
            wait_for_capacity_or_shutdown(&mut running, &mut shutdown_rx).await?;
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
                tracing::error!(error = %err, "failed to claim tasks");
                wait_poll_interval_or_shutdown(
                    &mut running,
                    &mut shutdown_rx,
                    options.poll_interval,
                )
                .await?;
                continue;
            }
        };

        if tasks.is_empty() {
            wait_poll_interval_or_shutdown(&mut running, &mut shutdown_rx, options.poll_interval)
                .await?;
            continue;
        }

        for task in tasks {
            let pool = pool.clone();
            let queue_name = queue_name.clone();
            let registry = registry.clone();
            let claim_timeout = options.claim_timeout;
            let unknown_task_policy = options.unknown_task_policy;
            let fatal_on_lease_timeout = options.fatal_on_lease_timeout;
            running.spawn(async move {
                execute_claimed_catching(
                    pool,
                    queue_name,
                    registry,
                    task,
                    claim_timeout,
                    unknown_task_policy,
                    fatal_on_lease_timeout,
                )
                .await
            });
        }
    }

    while let Some(result) = running.join_next().await {
        report_task_result(result)?;
    }

    Ok(())
}

async fn drain_finished(running: &mut JoinSet<Result<()>>) -> Result<()> {
    while let Some(result) = running.try_join_next() {
        report_task_result(result)?;
    }
    Ok(())
}

async fn wait_for_capacity_or_shutdown(
    running: &mut JoinSet<Result<()>>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<()> {
    tokio::select! {
        _ = shutdown_rx.changed() => Ok(()),
        result = running.join_next(), if !running.is_empty() => {
            if let Some(result) = result {
                report_task_result(result)?;
            }
            Ok(())
        }
    }
}

async fn wait_poll_interval_or_shutdown(
    running: &mut JoinSet<Result<()>>,
    shutdown_rx: &mut watch::Receiver<bool>,
    poll_interval: std::time::Duration,
) -> Result<()> {
    tokio::select! {
        _ = shutdown_rx.changed() => Ok(()),
        _ = tokio::time::sleep(poll_interval) => Ok(()),
        result = running.join_next(), if !running.is_empty() => {
            if let Some(result) = result {
                report_task_result(result)?;
            }
            Ok(())
        }
    }
}

fn report_task_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => {
            tracing::error!(error = %err, "task execution failed");
            Ok(())
        }
        Err(err) => Err(Error::Join(err)),
    }
}
