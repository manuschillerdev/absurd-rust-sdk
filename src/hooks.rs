use crate::context::TaskContext;
use crate::error::Result;
use crate::types::{Json, SpawnOptions};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub type BeforeSpawnFuture = Pin<Box<dyn Future<Output = Result<SpawnOptions>> + Send + 'static>>;
pub type BeforeSpawnHook =
    Arc<dyn Fn(String, Json, SpawnOptions) -> BeforeSpawnFuture + Send + Sync + 'static>;
pub type TaskExecutionFuture = Pin<Box<dyn Future<Output = Result<Json>> + Send + 'static>>;
pub type TaskExecution = Box<dyn FnOnce(TaskContext) -> TaskExecutionFuture + Send + 'static>;
pub type WrapTaskExecutionHook =
    Arc<dyn Fn(TaskContext, TaskExecution) -> TaskExecutionFuture + Send + Sync + 'static>;

#[derive(Clone, Default)]
#[non_exhaustive]
pub struct Hooks {
    before_spawn: Option<BeforeSpawnHook>,
    wrap_task_execution: Option<WrapTaskExecutionHook>,
}

/// Compatibility alias matching the naming used by other Absurd SDKs.
pub type AbsurdHooks = Hooks;

impl Hooks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn before_spawn<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(String, Json, SpawnOptions) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<SpawnOptions>> + Send + 'static,
    {
        self.before_spawn = Some(Arc::new(move |task_name, params, options| {
            Box::pin(hook(task_name, params, options))
        }));
        self
    }

    pub fn wrap_task_execution<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(TaskContext, TaskExecution) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Json>> + Send + 'static,
    {
        self.wrap_task_execution = Some(Arc::new(move |ctx, execute| Box::pin(hook(ctx, execute))));
        self
    }

    pub(crate) async fn apply_before_spawn(
        &self,
        task_name: &str,
        params: Json,
        options: SpawnOptions,
    ) -> Result<SpawnOptions> {
        if let Some(hook) = &self.before_spawn {
            hook(task_name.to_string(), params, options).await
        } else {
            Ok(options)
        }
    }

    pub(crate) fn wrap_task_execution_hook(&self) -> Option<WrapTaskExecutionHook> {
        self.wrap_task_execution.clone()
    }
}

impl std::fmt::Debug for Hooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hooks")
            .field("before_spawn", &self.before_spawn.is_some())
            .field("wrap_task_execution", &self.wrap_task_execution.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ClaimedTask;
    use serde_json::{Map, json};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[tokio::test]
    async fn before_spawn_can_inject_headers() -> Result<()> {
        let hooks = Hooks::new().before_spawn(|task_name, params, mut options| async move {
            assert_eq!(task_name, "send-email");
            assert_eq!(params["user_id"], json!(42));
            options
                .headers
                .get_or_insert_with(Map::new)
                .insert("trace_id".to_string(), json!("trace-123"));
            Ok(options)
        });

        let options = hooks
            .apply_before_spawn("send-email", json!({ "user_id": 42 }), SpawnOptions::new())
            .await?;

        assert_eq!(
            options
                .headers
                .and_then(|headers| headers.get("trace_id").cloned()),
            Some(json!("trace-123"))
        );
        Ok(())
    }

    #[tokio::test]
    async fn before_spawn_can_preserve_existing_headers() -> Result<()> {
        let hooks = Hooks::new().before_spawn(|_, _, mut options| async move {
            options
                .headers
                .get_or_insert_with(Map::new)
                .insert("trace_id".to_string(), json!("trace-123"));
            Ok(options)
        });

        let mut headers = Map::new();
        headers.insert("request_id".to_string(), json!("req-456"));
        let options = hooks
            .apply_before_spawn(
                "send-email",
                json!({}),
                SpawnOptions::new().headers(headers),
            )
            .await?;
        let headers = options.headers.unwrap_or_default();

        assert_eq!(headers.get("request_id"), Some(&json!("req-456")));
        assert_eq!(headers.get("trace_id"), Some(&json!("trace-123")));
        Ok(())
    }

    #[tokio::test]
    async fn wrap_task_execution_wraps_handler() -> Result<()> {
        let order = Arc::new(Mutex::new(Vec::new()));
        let hooks = Hooks::new().wrap_task_execution({
            let order = Arc::clone(&order);
            move |ctx, execute| {
                let order = Arc::clone(&order);
                async move {
                    order
                        .lock()
                        .map_err(|_| crate::Error::message("order lock poisoned"))?
                        .push("before");
                    let result = execute(ctx).await?;
                    order
                        .lock()
                        .map_err(|_| crate::Error::message("order lock poisoned"))?
                        .push("after");
                    Ok(result)
                }
            }
        });

        let ctx = test_context("wrapped", Map::new())?;
        let execute: TaskExecution = Box::new(|_ctx| Box::pin(async move { Ok(json!("done")) }));
        let wrap = hooks
            .wrap_task_execution_hook()
            .ok_or_else(|| crate::Error::message("hook not registered"))?;
        let result = wrap(ctx, execute).await?;

        assert_eq!(result, json!("done"));
        let order = order
            .lock()
            .map_err(|_| crate::Error::message("order lock poisoned"))?;
        assert_eq!(order.as_slice(), ["before", "after"]);
        Ok(())
    }

    #[tokio::test]
    async fn wrap_task_execution_can_access_task_context() -> Result<()> {
        let mut headers = Map::new();
        headers.insert("trace_id".to_string(), json!("trace-123"));
        let captured = Arc::new(Mutex::new(None));
        let hooks = Hooks::new().wrap_task_execution({
            let captured = Arc::clone(&captured);
            move |ctx, execute| {
                let captured = Arc::clone(&captured);
                async move {
                    let snapshot = json!({
                        "task_name": ctx.task_name(),
                        "queue_name": ctx.queue_name(),
                        "attempt": ctx.attempt(),
                        "trace_id": ctx.headers().get("trace_id"),
                    });
                    *captured
                        .lock()
                        .map_err(|_| crate::Error::message("captured lock poisoned"))? =
                        Some(snapshot);
                    execute(ctx).await
                }
            }
        });

        let ctx = test_context("context-task", headers)?;
        let execute: TaskExecution = Box::new(|_ctx| Box::pin(async move { Ok(json!("ok")) }));
        let wrap = hooks
            .wrap_task_execution_hook()
            .ok_or_else(|| crate::Error::message("hook not registered"))?;
        wrap(ctx, execute).await?;

        let captured = captured
            .lock()
            .map_err(|_| crate::Error::message("captured lock poisoned"))?
            .clone();
        assert_eq!(
            captured,
            Some(json!({
                "task_name": "context-task",
                "queue_name": "test-queue",
                "attempt": 2,
                "trace_id": "trace-123",
            }))
        );
        Ok(())
    }

    fn test_context(
        task_name: impl Into<String>,
        headers: Map<String, serde_json::Value>,
    ) -> Result<TaskContext> {
        let mut pool_config = deadpool_postgres::Config::new();
        pool_config.url = Some("postgresql://localhost/absurd_test".to_string());
        let pool = pool_config
            .create_pool(
                Some(deadpool_postgres::Runtime::Tokio1),
                tokio_postgres::NoTls,
            )
            .map_err(|err| crate::Error::Config(format!("failed to create test pool: {err}")))?;

        Ok(TaskContext::from_parts_for_test(
            pool,
            "test-queue".to_string(),
            ClaimedTask {
                run_id: Uuid::new_v4(),
                task_id: Uuid::new_v4(),
                attempt: 2,
                task_name: task_name.into(),
                params: json!({}),
                retry_strategy: None,
                max_attempts: Some(3),
                headers: Some(serde_json::Value::Object(headers.clone())),
                wake_event: None,
                event_payload: None,
            },
            headers,
            std::time::Duration::from_secs(30),
        ))
    }
}
