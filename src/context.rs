use crate::error::{Error, Result, map_database_error};
use crate::task_result::{
    AwaitTaskResultOptions, TaskResultSnapshot, await_task_result_with_backoff,
    fetch_task_result_snapshot,
};
use crate::types::{ClaimedTask, Json, duration_seconds_ceil, validate_queue_name};
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
use uuid::Uuid;

/// Handle returned by [`TaskContext::begin_step`] for decomposed step execution.
///
/// A handle represents one concrete checkpoint slot, including automatic
/// numbering for repeated step names.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct StepHandle<T = Json> {
    pub name: String,
    pub checkpoint_name: String,
    pub done: bool,
    pub state: Option<T>,
}

impl<T> StepHandle<T> {
    fn into_cached_state(self) -> Result<T> {
        self.state
            .ok_or_else(|| completed_step_missing_state(&self.checkpoint_name))
    }
}

fn completed_step_missing_state(checkpoint_name: &str) -> Error {
    Error::Config(format!(
        "completed step handle {checkpoint_name:?} is missing cached state"
    ))
}

#[derive(Debug)]
pub struct TaskContext {
    pool: Pool,
    queue_name: String,
    task: ClaimedTask,
    headers: Map<String, Value>,
    checkpoint_cache: HashMap<String, Value>,
    step_name_counter: HashMap<String, usize>,
    reported_timeouts: HashSet<String>,
    claim_timeout: Duration,
    claim_timeout_seconds: i32,
    lease_tx: Option<watch::Sender<Duration>>,
}

impl TaskContext {
    pub(crate) async fn new(
        pool: Pool,
        queue_name: String,
        mut task: ClaimedTask,
        claim_timeout: Duration,
        lease_tx: Option<watch::Sender<Duration>>,
    ) -> Result<Self> {
        let client = pool.get().await?;
        let rows = client
            .query(
                "SELECT checkpoint_name, state
                 FROM absurd.get_task_checkpoint_states($1, $2, $3)",
                &[&queue_name, &task.task_id, &task.run_id],
            )
            .await
            .map_err(map_database_error)?;

        let mut checkpoint_cache = HashMap::with_capacity(rows.len());
        for row in rows {
            let name: String = row.get(0);
            let state: Value = row.get(1);
            checkpoint_cache.insert(name, state);
        }

        let headers = match task.headers.take() {
            None | Some(Value::Null) => Map::new(),
            Some(Value::Object(headers)) => headers,
            Some(_) => return Err(Error::InvalidHeaders),
        };

        Ok(Self {
            pool,
            queue_name,
            task,
            headers,
            checkpoint_cache,
            step_name_counter: HashMap::new(),
            reported_timeouts: HashSet::new(),
            claim_timeout,
            claim_timeout_seconds: duration_seconds_ceil(claim_timeout),
            lease_tx,
        })
    }

    #[cfg(test)]
    pub(crate) fn from_parts_for_test(
        pool: Pool,
        queue_name: String,
        task: ClaimedTask,
        headers: Map<String, Value>,
        claim_timeout: Duration,
    ) -> Self {
        Self {
            pool,
            queue_name,
            task,
            headers,
            checkpoint_cache: HashMap::new(),
            step_name_counter: HashMap::new(),
            reported_timeouts: HashSet::new(),
            claim_timeout,
            claim_timeout_seconds: duration_seconds_ceil(claim_timeout),
            lease_tx: None,
        }
    }

    pub fn task_id(&self) -> Uuid {
        self.task.task_id
    }

    pub fn run_id(&self) -> Uuid {
        self.task.run_id
    }

    pub fn task_name(&self) -> &str {
        &self.task.task_name
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub fn attempt(&self) -> i32 {
        self.task.attempt
    }

    pub fn headers(&self) -> &Map<String, Value> {
        &self.headers
    }

    pub async fn step<T, F, Fut>(&mut self, name: impl AsRef<str>, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let handle = self.begin_step::<T>(name).await?;
        if handle.done {
            return handle.into_cached_state();
        }

        let result = f().await?;
        self.complete_step(handle, result).await
    }

    /// Starts a step and checks whether its checkpoint already exists.
    pub async fn begin_step<T>(&mut self, name: impl AsRef<str>) -> Result<StepHandle<T>>
    where
        T: DeserializeOwned,
    {
        let name = name.as_ref().to_string();
        let checkpoint_name = self.next_checkpoint_name(&name);
        if let Some(cached) = self.lookup_checkpoint(&checkpoint_name).await? {
            return Ok(StepHandle {
                name,
                checkpoint_name,
                done: true,
                state: Some(serde_json::from_value(cached)?),
            });
        }

        Ok(StepHandle {
            name,
            checkpoint_name,
            done: false,
            state: None,
        })
    }

    /// Completes a step started with [`TaskContext::begin_step`] by persisting its state.
    ///
    /// If the handle is already done, this returns the cached state.
    pub async fn complete_step<T>(&mut self, handle: StepHandle<T>, value: T) -> Result<T>
    where
        T: Serialize,
    {
        if handle.done {
            return handle.into_cached_state();
        }

        let checkpoint_name = handle.checkpoint_name;
        let value_json = serde_json::to_value(&value)?;
        self.persist_checkpoint(&checkpoint_name, &value_json)
            .await?;
        Ok(value)
    }

    pub async fn sleep_for(&mut self, name: impl AsRef<str>, duration: Duration) -> Result<()> {
        self.sleep_until(
            name,
            Utc::now()
                + chrono::Duration::from_std(duration).map_err(|_| {
                    Error::Config("sleep duration is too large for chrono".to_string())
                })?,
        )
        .await
    }

    pub async fn sleep_until(
        &mut self,
        name: impl AsRef<str>,
        wake_at: DateTime<Utc>,
    ) -> Result<()> {
        let checkpoint_name = self.next_checkpoint_name(name.as_ref());
        let actual_wake_at = if let Some(cached) = self.lookup_checkpoint(&checkpoint_name).await? {
            serde_json::from_value(cached)?
        } else {
            let value = serde_json::to_value(wake_at)?;
            self.persist_checkpoint(&checkpoint_name, &value).await?;
            wake_at
        };

        if Utc::now() < actual_wake_at {
            self.schedule_run(actual_wake_at).await?;
            return Err(Error::Suspended);
        }

        Ok(())
    }

    pub async fn await_event<T>(&mut self, event_name: impl AsRef<str>) -> Result<T>
    where
        T: DeserializeOwned,
    {
        self.await_event_with_options(event_name, AwaitEventOptions::default())
            .await
    }

    pub async fn await_event_with_options<T>(
        &mut self,
        event_name: impl AsRef<str>,
        options: AwaitEventOptions,
    ) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let event_name = event_name.as_ref();
        if event_name.trim().is_empty() {
            return Err(Error::Config("event name must be provided".to_string()));
        }

        let default_step_name = format!("$awaitEvent:{event_name}");
        let step_name = options
            .step_name
            .as_deref()
            .unwrap_or(default_step_name.as_str());
        let checkpoint_name = self.next_checkpoint_name(step_name);

        if let Some(cached) = self.lookup_checkpoint(&checkpoint_name).await? {
            return Ok(serde_json::from_value(cached)?);
        }

        if self.task.wake_event.as_deref() == Some(event_name) && self.task.event_payload.is_none()
        {
            self.task.wake_event = None;
            self.task.event_payload = None;
            self.reported_timeouts.insert(event_name.to_string());
            return Err(Error::EventTimeout {
                event: event_name.to_string(),
            });
        }

        let timeout_seconds = options.timeout.map(duration_seconds_ceil);
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "SELECT should_suspend, payload
                 FROM absurd.await_event($1, $2, $3, $4, $5, $6)",
                &[
                    &self.queue_name,
                    &self.task.task_id,
                    &self.task.run_id,
                    &checkpoint_name,
                    &event_name,
                    &timeout_seconds,
                ],
            )
            .await
            .map_err(map_database_error)?;

        let should_suspend: bool = row.get(0);
        let payload: Option<Value> = row.get(1);

        if should_suspend {
            return Err(Error::Suspended);
        }

        let payload = match payload {
            Some(payload) => {
                self.reported_timeouts.remove(event_name);
                payload
            }
            None if self.reported_timeouts.remove(event_name) => Value::Null,
            None => {
                return Err(Error::EventTimeout {
                    event: event_name.to_string(),
                });
            }
        };

        self.checkpoint_cache
            .insert(checkpoint_name, payload.clone());
        Ok(serde_json::from_value(payload)?)
    }

    pub async fn await_task_result(
        &mut self,
        task_id: Uuid,
        options: AwaitTaskResultOptions,
    ) -> Result<TaskResultSnapshot> {
        let queue_name = options.queue.as_deref().unwrap_or(&self.queue_name);
        validate_queue_name(queue_name)?;
        if queue_name == self.queue_name.as_str() {
            return Err(Error::Config(
                "TaskContext.await_task_result cannot wait on tasks in the same queue because this can deadlock workers. Spawn the child in a different queue and pass options.queue."
                    .to_string(),
            ));
        }
        let queue_name = queue_name.to_string();

        let default_step_name = format!("$awaitTaskResult:{task_id}");
        let step_name = options
            .step_name
            .as_deref()
            .unwrap_or(default_step_name.as_str());
        let checkpoint_name = self.next_checkpoint_name(step_name);

        if let Some(cached) = self.lookup_checkpoint(&checkpoint_name).await? {
            return Ok(serde_json::from_value(cached)?);
        }

        let heartbeat_interval = (self.claim_timeout / 2).max(Duration::from_millis(500));
        let mut next_heartbeat_at = Instant::now() + heartbeat_interval;
        let fetch_pool = self.pool.clone();
        let heartbeat_pool = self.pool.clone();
        let heartbeat_queue_name = self.queue_name.clone();
        let run_id = self.task.run_id;
        let claim_timeout = self.claim_timeout;
        let lease_tx = self.lease_tx.clone();

        let snapshot = await_task_result_with_backoff(
            || {
                let pool = fetch_pool.clone();
                let queue_name = queue_name.clone();
                async move { fetch_task_result_snapshot(&pool, &queue_name, task_id).await }
            },
            task_id,
            options.timeout,
            || {
                let should_heartbeat = Instant::now() >= next_heartbeat_at;
                if should_heartbeat {
                    next_heartbeat_at = Instant::now() + heartbeat_interval;
                }
                let pool = heartbeat_pool.clone();
                let queue_name = heartbeat_queue_name.clone();
                let lease_tx = lease_tx.clone();
                async move {
                    if should_heartbeat {
                        extend_claim(&pool, &queue_name, run_id, claim_timeout).await?;
                        if let Some(tx) = lease_tx {
                            let _ = tx.send(claim_timeout);
                        }
                    }
                    Ok(())
                }
            },
        )
        .await?;

        let value = serde_json::to_value(&snapshot)?;
        self.persist_checkpoint(&checkpoint_name, &value).await?;
        Ok(snapshot)
    }

    pub async fn emit_event<T>(&self, event_name: impl AsRef<str>, payload: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        let event_name = event_name.as_ref();
        if event_name.trim().is_empty() {
            return Err(Error::Config("event name must be provided".to_string()));
        }
        let payload = serde_json::to_value(payload)?;
        let client = self.pool.get().await?;
        client
            .execute(
                "SELECT absurd.emit_event($1, $2, $3)",
                &[&self.queue_name, &event_name, &payload],
            )
            .await
            .map_err(map_database_error)?;
        Ok(())
    }

    pub async fn heartbeat(&self) -> Result<()> {
        self.heartbeat_for(self.claim_timeout).await
    }

    pub async fn heartbeat_for(&self, duration: Duration) -> Result<()> {
        extend_claim(&self.pool, &self.queue_name, self.task.run_id, duration).await?;
        self.notify_lease_extended(duration);
        Ok(())
    }

    fn next_checkpoint_name(&mut self, name: &str) -> String {
        let count = self.step_name_counter.entry(name.to_string()).or_insert(0);
        *count += 1;
        if *count == 1 {
            name.to_string()
        } else {
            format!("{name}#{count}")
        }
    }

    async fn lookup_checkpoint(&mut self, checkpoint_name: &str) -> Result<Option<Json>> {
        if let Some(value) = self.checkpoint_cache.get(checkpoint_name) {
            return Ok(Some(value.clone()));
        }

        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT state
                 FROM absurd.get_task_checkpoint_state($1, $2, $3)",
                &[&self.queue_name, &self.task.task_id, &checkpoint_name],
            )
            .await
            .map_err(map_database_error)?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };

        let value: Value = row.get(0);
        self.checkpoint_cache
            .insert(checkpoint_name.to_string(), value.clone());
        Ok(Some(value))
    }

    async fn persist_checkpoint(&mut self, checkpoint_name: &str, value: &Value) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "SELECT absurd.set_task_checkpoint_state($1, $2, $3, $4, $5, $6)",
                &[
                    &self.queue_name,
                    &self.task.task_id,
                    &checkpoint_name,
                    &value,
                    &self.task.run_id,
                    &self.claim_timeout_seconds,
                ],
            )
            .await
            .map_err(map_database_error)?;

        self.checkpoint_cache
            .insert(checkpoint_name.to_string(), value.clone());
        self.notify_lease_extended(self.claim_timeout);
        Ok(())
    }

    async fn schedule_run(&self, wake_at: DateTime<Utc>) -> Result<()> {
        let client = self.pool.get().await?;
        client
            .execute(
                "SELECT absurd.schedule_run($1, $2, $3)",
                &[&self.queue_name, &self.task.run_id, &wake_at],
            )
            .await
            .map_err(map_database_error)?;
        Ok(())
    }

    fn notify_lease_extended(&self, duration: Duration) {
        if let Some(tx) = &self.lease_tx {
            let _ = tx.send(duration);
        }
    }
}

async fn extend_claim(
    pool: &Pool,
    queue_name: &str,
    run_id: Uuid,
    duration: Duration,
) -> Result<()> {
    let seconds = duration_seconds_ceil(duration);
    let client = pool.get().await?;
    client
        .execute(
            "SELECT absurd.extend_claim($1, $2, $3)",
            &[&queue_name, &run_id, &seconds],
        )
        .await
        .map_err(map_database_error)?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct AwaitEventOptions {
    pub step_name: Option<String>,
    pub timeout: Option<Duration>,
}

impl AwaitEventOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn step_name(mut self, step_name: impl Into<String>) -> Self {
        self.step_name = Some(step_name.into());
        self
    }

    /// Explicit `Duration::ZERO` means immediate timeout. `None` means no timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
