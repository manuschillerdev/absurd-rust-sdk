use crate::error::{Error, Result, map_database_error};
use crate::types::Json;
use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

pub(crate) const INITIAL_TASK_RESULT_BACKOFF: Duration = Duration::from_millis(50);
pub(crate) const MAX_TASK_RESULT_BACKOFF: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum TaskResultState {
    Pending,
    Running,
    Sleeping,
    Completed,
    Failed,
    Cancelled,
}

impl TaskResultState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Sleeping => "sleeping",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

impl TryFrom<&str> for TaskResultState {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "sleeping" => Ok(Self::Sleeping),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(Error::Config(format!(
                "unknown task result state {other:?}"
            ))),
        }
    }
}

impl std::fmt::Display for TaskResultState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[non_exhaustive]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum TaskResultSnapshot {
    Pending,
    Running,
    Sleeping,
    Completed {
        #[serde(default)]
        result: Json,
    },
    Failed {
        #[serde(default)]
        failure: Json,
    },
    Cancelled,
}

impl TaskResultSnapshot {
    pub fn state(&self) -> TaskResultState {
        match self {
            Self::Pending => TaskResultState::Pending,
            Self::Running => TaskResultState::Running,
            Self::Sleeping => TaskResultState::Sleeping,
            Self::Completed { .. } => TaskResultState::Completed,
            Self::Failed { .. } => TaskResultState::Failed,
            Self::Cancelled => TaskResultState::Cancelled,
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.state().is_terminal()
    }

    pub fn result(&self) -> Option<&Json> {
        match self {
            Self::Completed { result } => Some(result),
            _ => None,
        }
    }

    pub fn failure(&self) -> Option<&Json> {
        match self {
            Self::Failed { failure } => Some(failure),
            _ => None,
        }
    }

    pub fn deserialize_result<T>(&self) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.result()
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
    }

    pub fn deserialize_failure<T>(&self) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        self.failure()
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(Error::from)
    }

    pub(crate) fn from_sql_parts(
        state: &str,
        result: Option<Json>,
        failure: Option<Json>,
    ) -> Result<Self> {
        match TaskResultState::try_from(state)? {
            TaskResultState::Pending => Ok(Self::Pending),
            TaskResultState::Running => Ok(Self::Running),
            TaskResultState::Sleeping => Ok(Self::Sleeping),
            TaskResultState::Completed => Ok(Self::Completed {
                result: result.unwrap_or(Json::Null),
            }),
            TaskResultState::Failed => Ok(Self::Failed {
                failure: failure.unwrap_or(Json::Null),
            }),
            TaskResultState::Cancelled => Ok(Self::Cancelled),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct AwaitTaskResultOptions {
    pub queue: Option<String>,
    pub timeout: Option<Duration>,
    pub step_name: Option<String>,
}

impl AwaitTaskResultOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Explicit `Duration::ZERO` means immediate timeout. `None` means no timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn step_name(mut self, step_name: impl Into<String>) -> Self {
        self.step_name = Some(step_name.into());
        self
    }
}

pub(crate) async fn fetch_task_result_snapshot(
    pool: &Pool,
    queue_name: &str,
    task_id: Uuid,
) -> Result<Option<TaskResultSnapshot>> {
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT state, result, failure_reason
             FROM absurd.get_task_result($1, $2)",
            &[&queue_name, &task_id],
        )
        .await
        .map_err(map_database_error)?;

    let Some(row) = rows.first() else {
        return Ok(None);
    };

    let state: String = row.get(0);
    let result: Option<Json> = row.get(1);
    let failure: Option<Json> = row.get(2);
    TaskResultSnapshot::from_sql_parts(&state, result, failure).map(Some)
}

pub(crate) async fn await_task_result_with_backoff<
    Fetch,
    FetchFuture,
    BeforeSleep,
    BeforeSleepFuture,
>(
    mut fetch_snapshot: Fetch,
    task_id: Uuid,
    timeout: Option<Duration>,
    mut before_sleep: BeforeSleep,
) -> Result<TaskResultSnapshot>
where
    Fetch: FnMut() -> FetchFuture,
    FetchFuture: Future<Output = Result<Option<TaskResultSnapshot>>>,
    BeforeSleep: FnMut() -> BeforeSleepFuture,
    BeforeSleepFuture: Future<Output = Result<()>>,
{
    let started_at = Instant::now();
    let mut delay = INITIAL_TASK_RESULT_BACKOFF;

    loop {
        let snapshot = fetch_snapshot().await?;
        let Some(snapshot) = snapshot else {
            return Err(Error::TaskNotFound { task_id });
        };
        if snapshot.is_terminal() {
            return Ok(snapshot);
        }

        let sleep_for = next_sleep_duration(started_at, timeout, delay, task_id)?;
        before_sleep().await?;
        tokio::time::sleep(sleep_for).await;
        delay = next_task_result_backoff(delay);
    }
}

pub(crate) fn next_task_result_backoff(delay: Duration) -> Duration {
    delay.saturating_mul(2).min(MAX_TASK_RESULT_BACKOFF)
}

fn next_sleep_duration(
    started_at: Instant,
    timeout: Option<Duration>,
    delay: Duration,
    task_id: Uuid,
) -> Result<Duration> {
    let Some(timeout) = timeout else {
        return Ok(delay);
    };

    let elapsed = started_at.elapsed();
    if elapsed >= timeout {
        return Err(Error::TaskResultTimeout { task_id });
    }

    Ok(delay.min(timeout - elapsed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn task_result_state_parses_known_sql_values() -> Result<()> {
        assert_eq!(
            TaskResultState::try_from("pending")?,
            TaskResultState::Pending
        );
        assert_eq!(
            TaskResultState::try_from("completed")?,
            TaskResultState::Completed
        );
        assert!(TaskResultState::Completed.is_terminal());
        assert!(!TaskResultState::Running.is_terminal());
        assert_eq!(TaskResultState::Failed.as_str(), "failed");
        Ok(())
    }

    #[test]
    fn task_result_snapshot_uses_tagged_json_shape() -> std::result::Result<(), serde_json::Error> {
        let completed = TaskResultSnapshot::Completed {
            result: json!({ "ok": true }),
        };
        assert_eq!(
            serde_json::to_value(&completed)?,
            json!({ "state": "completed", "result": { "ok": true } })
        );

        let failed = TaskResultSnapshot::Failed {
            failure: Json::Null,
        };
        assert_eq!(
            serde_json::to_value(&failed)?,
            json!({ "state": "failed", "failure": null })
        );

        assert_eq!(
            serde_json::to_value(TaskResultSnapshot::Sleeping)?,
            json!({ "state": "sleeping" })
        );
        Ok(())
    }

    #[test]
    fn sql_parts_only_expose_terminal_payloads() -> Result<()> {
        assert_eq!(
            TaskResultSnapshot::from_sql_parts("pending", Some(json!(1)), Some(json!(2)))?,
            TaskResultSnapshot::Pending
        );
        assert_eq!(
            TaskResultSnapshot::from_sql_parts("completed", Some(json!(42)), None)?,
            TaskResultSnapshot::Completed { result: json!(42) }
        );
        assert_eq!(
            TaskResultSnapshot::from_sql_parts("failed", None, Some(json!({ "error": true })))?,
            TaskResultSnapshot::Failed {
                failure: json!({ "error": true })
            }
        );
        Ok(())
    }

    #[test]
    fn task_result_backoff_doubles_until_cap() {
        let first = INITIAL_TASK_RESULT_BACKOFF;
        let second = next_task_result_backoff(first);
        let third = next_task_result_backoff(second);

        assert_eq!(second, Duration::from_millis(100));
        assert_eq!(third, Duration::from_millis(200));
        assert_eq!(
            next_task_result_backoff(Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }
}
