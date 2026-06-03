use crate::context::TaskContext;
use crate::error::{Error, Result, map_database_error};
use crate::executor::execute_claimed_catching;
use crate::task::Task;
use crate::types::{
    CancellationPolicy, ClaimedTask, CleanupResult, Json, SpawnOptions, SpawnResult, TaskOptions,
    WorkBatchOptions, WorkerOptions, duration_seconds_ceil, validate_queue_name,
};
use crate::worker::Worker;
use deadpool_postgres::{Config as PoolConfig, Pool, Runtime};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio_postgres::NoTls;
use uuid::Uuid;

pub(crate) type HandlerFuture = Pin<Box<dyn Future<Output = Result<Json>> + Send + 'static>>;
pub(crate) type TaskHandler = Arc<dyn Fn(Json, TaskContext) -> HandlerFuture + Send + Sync>;

#[derive(Clone)]
pub struct Client {
    pub(crate) pool: Pool,
    queue_name: String,
    default_max_attempts: i32,
    registry: Arc<RwLock<HashMap<String, RegisteredTask>>>,
}

#[derive(Clone)]
pub(crate) struct RegisteredTask {
    pub queue_name: String,
    pub default_max_attempts: Option<i32>,
    pub default_cancellation: Option<CancellationPolicy>,
    pub handler: TaskHandler,
}

impl Client {
    pub async fn connect(database_url: impl AsRef<str>) -> Result<Self> {
        Self::connect_queue(database_url, "default").await
    }

    pub async fn connect_queue(
        database_url: impl AsRef<str>,
        queue_name: impl Into<String>,
    ) -> Result<Self> {
        let queue_name = queue_name.into();
        validate_queue_name(&queue_name)?;

        let mut cfg = PoolConfig::new();
        cfg.url = Some(database_url.as_ref().to_string());
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|err| Error::Config(format!("failed to create Postgres pool: {err}")))?;

        // Fail fast on bad configuration.
        let _ = pool.get().await?;
        Ok(Self::from_pool(pool, queue_name))
    }

    pub async fn from_env() -> Result<Self> {
        Self::connect(resolve_database_url(None)).await
    }

    pub async fn from_env_queue(queue_name: impl Into<String>) -> Result<Self> {
        Self::connect_queue(resolve_database_url(None), queue_name).await
    }

    pub fn from_pool(pool: Pool, queue_name: impl Into<String>) -> Self {
        Self {
            pool,
            queue_name: queue_name.into(),
            default_max_attempts: 5,
            registry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    pub fn default_max_attempts(mut self, value: i32) -> Self {
        self.default_max_attempts = value;
        self
    }

    pub fn register<P, R, F, Fut>(&self, task: Task<P, R>, handler: F) -> Result<()>
    where
        P: DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(P, TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        self.register_with_options(task.options.clone(), handler)
    }

    pub fn register_with_options<P, R, F, Fut>(
        &self,
        options: TaskOptions,
        handler: F,
    ) -> Result<()>
    where
        P: DeserializeOwned + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(P, TaskContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
    {
        if options.name.trim().is_empty() {
            return Err(Error::Config("task name must be provided".to_string()));
        }
        if matches!(options.default_max_attempts, Some(value) if value < 1) {
            return Err(Error::Config(
                "default max attempts must be at least 1".to_string(),
            ));
        }

        let queue_name = options
            .queue
            .clone()
            .unwrap_or_else(|| self.queue_name.clone());
        validate_queue_name(&queue_name)?;

        let handler = Arc::new(handler);
        let erased: TaskHandler = Arc::new(move |params: Value, ctx: TaskContext| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                let params = serde_json::from_value(params)?;
                let result = handler(params, ctx).await?;
                Ok(serde_json::to_value(result)?)
            })
        });

        let registered = RegisteredTask {
            queue_name,
            default_max_attempts: options.default_max_attempts,
            default_cancellation: options.default_cancellation,
            handler: erased,
        };

        self.registry
            .write()
            .map_err(|_| Error::Config("task registry lock poisoned".to_string()))?
            .insert(options.name, registered);
        Ok(())
    }

    pub async fn spawn<P, R>(
        &self,
        task: &Task<P, R>,
        params: P,
        options: SpawnOptions,
    ) -> Result<SpawnResult>
    where
        P: Serialize,
    {
        self.spawn_named(task.name(), params, options).await
    }

    pub async fn spawn_named<P>(
        &self,
        task_name: impl AsRef<str>,
        params: P,
        options: SpawnOptions,
    ) -> Result<SpawnResult>
    where
        P: Serialize,
    {
        let task_name = task_name.as_ref();
        if task_name.trim().is_empty() {
            return Err(Error::Config("task name must be provided".to_string()));
        }

        let (queue_name, max_attempts, cancellation) = self.resolve_spawn(task_name, &options)?;
        let params = serde_json::to_value(params)?;
        let options_json = options.to_sql_json(max_attempts, cancellation);

        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "SELECT task_id, run_id, attempt, created
                 FROM absurd.spawn_task($1, $2, $3, $4)",
                &[&queue_name, &task_name, &params, &options_json],
            )
            .await
            .map_err(map_database_error)?;

        Ok(SpawnResult {
            task_id: row.get(0),
            run_id: row.get(1),
            attempt: row.get(2),
            created: row.get(3),
        })
    }

    pub async fn create_queue(&self, queue_name: Option<&str>) -> Result<()> {
        let queue_name = queue_name.unwrap_or(&self.queue_name);
        validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        client
            .execute("SELECT absurd.create_queue($1)", &[&queue_name])
            .await
            .map_err(map_database_error)?;
        Ok(())
    }

    pub async fn drop_queue(&self, queue_name: Option<&str>) -> Result<()> {
        let queue_name = queue_name.unwrap_or(&self.queue_name);
        validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        client
            .execute("SELECT absurd.drop_queue($1)", &[&queue_name])
            .await
            .map_err(map_database_error)?;
        Ok(())
    }

    pub async fn list_queues(&self) -> Result<Vec<String>> {
        let client = self.pool.get().await?;
        let rows = client
            .query("SELECT queue_name FROM absurd.list_queues()", &[])
            .await
            .map_err(map_database_error)?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    pub async fn emit_event<T>(
        &self,
        event_name: impl AsRef<str>,
        payload: &T,
        queue_name: Option<&str>,
    ) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        let event_name = event_name.as_ref();
        if event_name.trim().is_empty() {
            return Err(Error::Config("event name must be provided".to_string()));
        }
        let queue_name = queue_name.unwrap_or(&self.queue_name);
        validate_queue_name(queue_name)?;
        let payload = serde_json::to_value(payload)?;
        let client = self.pool.get().await?;
        client
            .execute(
                "SELECT absurd.emit_event($1, $2, $3)",
                &[&queue_name, &event_name, &payload],
            )
            .await
            .map_err(map_database_error)?;
        Ok(())
    }

    pub async fn cancel_task(&self, task_id: Uuid, queue_name: Option<&str>) -> Result<()> {
        let queue_name = queue_name.unwrap_or(&self.queue_name);
        validate_queue_name(queue_name)?;
        let client = self.pool.get().await?;
        client
            .execute(
                "SELECT absurd.cancel_task($1, $2)",
                &[&queue_name, &task_id],
            )
            .await
            .map_err(map_database_error)?;
        Ok(())
    }

    pub async fn cleanup(&self, ttl: Duration, queue_name: Option<&str>) -> Result<CleanupResult> {
        self.cleanup_with_limit(ttl, 1000, queue_name).await
    }

    pub async fn cleanup_with_limit(
        &self,
        ttl: Duration,
        limit: i32,
        queue_name: Option<&str>,
    ) -> Result<CleanupResult> {
        let queue_name = queue_name.unwrap_or(&self.queue_name);
        validate_queue_name(queue_name)?;
        let ttl_seconds = duration_seconds_ceil(ttl);
        let client = self.pool.get().await?;
        let tasks_deleted = client
            .query_one(
                "SELECT absurd.cleanup_tasks($1, $2, $3)",
                &[&queue_name, &ttl_seconds, &limit],
            )
            .await
            .map_err(map_database_error)?
            .get(0);
        let events_deleted = client
            .query_one(
                "SELECT absurd.cleanup_events($1, $2, $3)",
                &[&queue_name, &ttl_seconds, &limit],
            )
            .await
            .map_err(map_database_error)?
            .get(0);
        Ok(CleanupResult {
            tasks_deleted,
            events_deleted,
        })
    }

    pub async fn work_batch(&self, options: WorkBatchOptions) -> Result<usize> {
        let worker_id = options.worker_id_value();
        let claim_timeout_seconds = options.claim_timeout_seconds();
        let batch_size = options.batch_size.max(1).min(i32::MAX as usize) as i32;
        let tasks = self
            .claim_tasks(&worker_id, claim_timeout_seconds, batch_size)
            .await?;
        let count = tasks.len();

        for task in tasks {
            execute_claimed_catching(
                self.pool.clone(),
                self.queue_name.clone(),
                self.registry.clone(),
                task,
                options.claim_timeout,
                options.unknown_task_policy,
                false,
            )
            .await?;
        }

        Ok(count)
    }

    pub fn start_worker(&self, options: WorkerOptions) -> Worker {
        Worker::start(
            self.pool.clone(),
            self.queue_name.clone(),
            self.registry.clone(),
            options,
        )
    }

    pub(crate) async fn claim_tasks(
        &self,
        worker_id: &str,
        claim_timeout_seconds: i32,
        batch_size: i32,
    ) -> Result<Vec<ClaimedTask>> {
        claim_tasks(
            &self.pool,
            &self.queue_name,
            worker_id,
            claim_timeout_seconds,
            batch_size,
        )
        .await
    }

    pub(crate) fn get_registration(&self, task_name: &str) -> Result<Option<RegisteredTask>> {
        Ok(self
            .registry
            .read()
            .map_err(|_| Error::Config("task registry lock poisoned".to_string()))?
            .get(task_name)
            .cloned())
    }

    fn resolve_spawn(
        &self,
        task_name: &str,
        options: &SpawnOptions,
    ) -> Result<(String, i32, Option<CancellationPolicy>)> {
        let registration = self.get_registration(task_name)?;

        let queue_name = if let Some(registration) = &registration {
            if let Some(requested) = &options.queue {
                if requested != &registration.queue_name {
                    return Err(Error::Config(format!(
                        "task {task_name:?} is registered for queue {:?}, but spawn requested queue {requested:?}",
                        registration.queue_name
                    )));
                }
            }
            registration.queue_name.clone()
        } else {
            options.queue.clone().ok_or_else(|| {
                Error::Config(format!(
                    "task {task_name:?} is not registered; provide SpawnOptions::queue for unregistered tasks"
                ))
            })?
        };
        validate_queue_name(&queue_name)?;

        let max_attempts = options
            .max_attempts
            .or_else(|| registration.as_ref().and_then(|r| r.default_max_attempts))
            .unwrap_or(self.default_max_attempts);
        if max_attempts < 1 {
            return Err(Error::Config("max attempts must be at least 1".to_string()));
        }

        let cancellation = options
            .cancellation
            .clone()
            .or_else(|| registration.and_then(|r| r.default_cancellation));

        Ok((queue_name, max_attempts, cancellation))
    }
}

pub(crate) async fn claim_tasks(
    pool: &Pool,
    queue_name: &str,
    worker_id: &str,
    claim_timeout_seconds: i32,
    batch_size: i32,
) -> Result<Vec<ClaimedTask>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT run_id, task_id, attempt, task_name, params, retry_strategy,
                    max_attempts, headers, wake_event, event_payload
             FROM absurd.claim_task($1, $2, $3, $4)",
            &[&queue_name, &worker_id, &claim_timeout_seconds, &batch_size],
        )
        .await
        .map_err(map_database_error)?;

    Ok(rows
        .into_iter()
        .map(|row| ClaimedTask {
            run_id: row.get(0),
            task_id: row.get(1),
            attempt: row.get(2),
            task_name: row.get(3),
            params: row.get(4),
            retry_strategy: row.get(5),
            max_attempts: row.get(6),
            headers: row.get(7),
            wake_event: row.get(8),
            event_payload: row.get(9),
        })
        .collect())
}

fn resolve_database_url(explicit: Option<&str>) -> String {
    if let Some(explicit) = explicit.filter(|value| !value.trim().is_empty()) {
        return explicit.to_string();
    }
    if let Ok(url) = std::env::var("ABSURD_DATABASE_URL") {
        if !url.trim().is_empty() {
            return url;
        }
    }
    if let Ok(pgdatabase) = std::env::var("PGDATABASE") {
        if !pgdatabase.trim().is_empty() {
            if pgdatabase.contains("://") || pgdatabase.contains('=') {
                return pgdatabase;
            }
            return format!("dbname={pgdatabase}");
        }
    }
    "postgresql://localhost/absurd".to_string()
}
