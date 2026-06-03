use crate::error::{Error, Result};
use serde_json::{Map, Value, json};
use std::time::Duration;
use uuid::Uuid;

pub type Json = Value;

#[derive(Clone, Debug, PartialEq)]
pub enum RetryStrategy {
    None,
    Fixed {
        base: Duration,
    },
    Exponential {
        base: Duration,
        factor: f64,
        max: Option<Duration>,
    },
}

impl RetryStrategy {
    pub fn none() -> Self {
        Self::None
    }

    pub fn fixed(base: Duration) -> Self {
        Self::Fixed { base }
    }

    pub fn exponential(base: Duration, factor: f64) -> Self {
        Self::Exponential {
            base,
            factor,
            max: None,
        }
    }

    pub fn with_max(mut self, max: Duration) -> Self {
        if let Self::Exponential { max: slot, .. } = &mut self {
            *slot = Some(max);
        }
        self
    }

    pub(crate) fn to_json(&self) -> Value {
        match self {
            Self::None => json!({ "kind": "none" }),
            Self::Fixed { base } => json!({
                "kind": "fixed",
                "base_seconds": duration_seconds_f64(*base),
            }),
            Self::Exponential { base, factor, max } => {
                let mut obj = Map::new();
                obj.insert("kind".to_string(), Value::String("exponential".to_string()));
                obj.insert(
                    "base_seconds".to_string(),
                    json!(duration_seconds_f64(*base)),
                );
                obj.insert("factor".to_string(), json!(factor));
                if let Some(max) = max {
                    obj.insert("max_seconds".to_string(), json!(duration_seconds_f64(*max)));
                }
                Value::Object(obj)
            }
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct CancellationPolicy {
    pub max_duration: Option<Duration>,
    pub max_delay: Option<Duration>,
}

impl CancellationPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_duration(mut self, value: Duration) -> Self {
        self.max_duration = Some(value);
        self
    }

    pub fn max_delay(mut self, value: Duration) -> Self {
        self.max_delay = Some(value);
        self
    }

    pub(crate) fn to_json(&self) -> Option<Value> {
        let mut obj = Map::new();
        if let Some(max_duration) = self.max_duration {
            obj.insert(
                "max_duration".to_string(),
                json!(duration_seconds_ceil(max_duration)),
            );
        }
        if let Some(max_delay) = self.max_delay {
            obj.insert(
                "max_delay".to_string(),
                json!(duration_seconds_ceil(max_delay)),
            );
        }
        (!obj.is_empty()).then_some(Value::Object(obj))
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SpawnOptions {
    pub queue: Option<String>,
    pub max_attempts: Option<i32>,
    pub retry_strategy: Option<RetryStrategy>,
    pub headers: Option<Map<String, Value>>,
    pub cancellation: Option<CancellationPolicy>,
    pub idempotency_key: Option<String>,
}

impl SpawnOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: RetryStrategy) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    pub fn headers(mut self, headers: Map<String, Value>) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn cancellation(mut self, cancellation: CancellationPolicy) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    pub fn idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }

    pub(crate) fn to_sql_json(
        &self,
        effective_max_attempts: i32,
        effective_cancellation: Option<CancellationPolicy>,
    ) -> Value {
        let mut obj = Map::new();
        obj.insert("max_attempts".to_string(), json!(effective_max_attempts));

        if let Some(headers) = &self.headers {
            obj.insert("headers".to_string(), Value::Object(headers.clone()));
        }
        if let Some(retry_strategy) = &self.retry_strategy {
            obj.insert("retry_strategy".to_string(), retry_strategy.to_json());
        }
        if let Some(cancellation) = effective_cancellation.and_then(|c| c.to_json()) {
            obj.insert("cancellation".to_string(), cancellation);
        }
        if let Some(key) = &self.idempotency_key {
            obj.insert("idempotency_key".to_string(), Value::String(key.clone()));
        }
        Value::Object(obj)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SpawnResult {
    pub task_id: Uuid,
    pub run_id: Uuid,
    pub attempt: i32,
    pub created: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClaimedTask {
    pub run_id: Uuid,
    pub task_id: Uuid,
    pub attempt: i32,
    pub task_name: String,
    pub params: Value,
    pub retry_strategy: Option<Value>,
    pub max_attempts: Option<i32>,
    pub headers: Option<Value>,
    pub wake_event: Option<String>,
    pub event_payload: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TaskOptions {
    pub name: String,
    pub queue: Option<String>,
    pub default_max_attempts: Option<i32>,
    pub default_cancellation: Option<CancellationPolicy>,
}

impl TaskOptions {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            queue: None,
            default_max_attempts: None,
            default_cancellation: None,
        }
    }

    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    pub fn default_max_attempts(mut self, default_max_attempts: i32) -> Self {
        self.default_max_attempts = Some(default_max_attempts);
        self
    }

    pub fn default_cancellation(mut self, default_cancellation: CancellationPolicy) -> Self {
        self.default_cancellation = Some(default_cancellation);
        self
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum UnknownTaskPolicy {
    /// Defer unknown claimed tasks briefly so another worker version can pick them up.
    #[default]
    Defer,
    /// Mark unknown claimed tasks failed immediately.
    Fail,
}

#[derive(Clone, Debug)]
pub struct WorkerOptions {
    pub worker_id: Option<String>,
    pub claim_timeout: Duration,
    pub concurrency: usize,
    pub batch_size: Option<usize>,
    pub poll_interval: Duration,
    pub unknown_task_policy: UnknownTaskPolicy,
    pub fatal_on_lease_timeout: bool,
}

impl Default for WorkerOptions {
    fn default() -> Self {
        Self {
            worker_id: None,
            claim_timeout: Duration::from_secs(120),
            concurrency: 1,
            batch_size: None,
            poll_interval: Duration::from_millis(250),
            unknown_task_policy: UnknownTaskPolicy::Defer,
            fatal_on_lease_timeout: false,
        }
    }
}

impl WorkerOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    pub fn claim_timeout(mut self, claim_timeout: Duration) -> Self {
        self.claim_timeout = claim_timeout;
        self
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn unknown_task_policy(mut self, policy: UnknownTaskPolicy) -> Self {
        self.unknown_task_policy = policy;
        self
    }

    pub fn fatal_on_lease_timeout(mut self, value: bool) -> Self {
        self.fatal_on_lease_timeout = value;
        self
    }

    pub(crate) fn normalized(&self) -> NormalizedWorkerOptions {
        let concurrency = self.concurrency.max(1);
        NormalizedWorkerOptions {
            worker_id: self.worker_id.clone().unwrap_or_else(default_worker_id),
            claim_timeout: self.claim_timeout,
            claim_timeout_seconds: duration_seconds_ceil(self.claim_timeout),
            concurrency,
            batch_size: self.batch_size.unwrap_or(concurrency).max(1),
            poll_interval: self.poll_interval,
            unknown_task_policy: self.unknown_task_policy,
            fatal_on_lease_timeout: self.fatal_on_lease_timeout,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NormalizedWorkerOptions {
    pub worker_id: String,
    pub claim_timeout: Duration,
    pub claim_timeout_seconds: i32,
    pub concurrency: usize,
    pub batch_size: usize,
    pub poll_interval: Duration,
    pub unknown_task_policy: UnknownTaskPolicy,
    pub fatal_on_lease_timeout: bool,
}

#[derive(Clone, Debug)]
pub struct WorkBatchOptions {
    pub worker_id: Option<String>,
    pub claim_timeout: Duration,
    pub batch_size: usize,
    pub unknown_task_policy: UnknownTaskPolicy,
}

impl Default for WorkBatchOptions {
    fn default() -> Self {
        Self {
            worker_id: Some("worker".to_string()),
            claim_timeout: Duration::from_secs(120),
            batch_size: 1,
            unknown_task_policy: UnknownTaskPolicy::Defer,
        }
    }
}

impl WorkBatchOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    pub fn claim_timeout(mut self, claim_timeout: Duration) -> Self {
        self.claim_timeout = claim_timeout;
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn unknown_task_policy(mut self, policy: UnknownTaskPolicy) -> Self {
        self.unknown_task_policy = policy;
        self
    }

    pub(crate) fn worker_id_value(&self) -> String {
        self.worker_id
            .clone()
            .unwrap_or_else(|| "worker".to_string())
    }

    pub(crate) fn claim_timeout_seconds(&self) -> i32 {
        duration_seconds_ceil(self.claim_timeout)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CleanupResult {
    pub tasks_deleted: i32,
    pub events_deleted: i32,
}

pub(crate) fn validate_queue_name(queue_name: &str) -> Result<()> {
    if queue_name.trim().is_empty() {
        return Err(Error::Config("queue name must be provided".to_string()));
    }
    if queue_name.len() > 57 {
        return Err(Error::Config(format!(
            "queue name {queue_name:?} is too long (max 57 bytes)"
        )));
    }
    Ok(())
}

pub(crate) fn duration_seconds_ceil(duration: Duration) -> i32 {
    let seconds = duration.as_secs();
    let nanos = duration.subsec_nanos();
    let rounded = seconds.saturating_add(u64::from(nanos > 0));
    rounded.min(i32::MAX as u64) as i32
}

fn duration_seconds_f64(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

fn default_worker_id() -> String {
    let host = hostname::get()
        .ok()
        .and_then(|value| value.into_string().ok())
        .unwrap_or_else(|| "host".to_string());
    format!("{}:{}", host, std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_seconds_rounds_up() {
        assert_eq!(duration_seconds_ceil(Duration::from_millis(1)), 1);
        assert_eq!(duration_seconds_ceil(Duration::from_secs(2)), 2);
        assert_eq!(duration_seconds_ceil(Duration::ZERO), 0);
    }

    #[test]
    fn spawn_options_use_sql_wire_keys() {
        let options = SpawnOptions::new()
            .retry_strategy(RetryStrategy::fixed(Duration::from_secs(30)))
            .cancellation(CancellationPolicy::new().max_delay(Duration::from_secs(60)))
            .idempotency_key("idem");

        let value = options.to_sql_json(3, options.cancellation.clone());
        assert_eq!(value["max_attempts"], json!(3));
        assert_eq!(value["retry_strategy"]["kind"], json!("fixed"));
        assert_eq!(value["retry_strategy"]["base_seconds"], json!(30.0));
        assert_eq!(value["cancellation"]["max_delay"], json!(60));
        assert_eq!(value["idempotency_key"], json!("idem"));
    }
}
