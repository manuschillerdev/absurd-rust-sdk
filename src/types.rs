use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{fmt, sync::Arc, time::Duration};
use uuid::Uuid;

pub type Json = Value;

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
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
#[non_exhaustive]
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

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueueStorageMode {
    #[default]
    Unpartitioned,
    Partitioned,
}

impl QueueStorageMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unpartitioned => "unpartitioned",
            Self::Partitioned => "partitioned",
        }
    }
}

impl std::fmt::Display for QueueStorageMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for QueueStorageMode {
    type Err = Error;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "unpartitioned" => Ok(Self::Unpartitioned),
            "partitioned" => Ok(Self::Partitioned),
            _ => Err(Error::Config(format!(
                "invalid queue storage mode: {value}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueueDetachMode {
    #[default]
    None,
    Empty,
}

impl QueueDetachMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Empty => "empty",
        }
    }
}

impl std::fmt::Display for QueueDetachMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for QueueDetachMode {
    type Err = Error;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Ok(Self::None),
            "empty" => Ok(Self::Empty),
            _ => Err(Error::Config(format!("invalid queue detach mode: {value}"))),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, rename_all = "snake_case")]
#[non_exhaustive]
pub struct QueuePolicyOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_lookahead: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_lookback: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_ttl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_mode: Option<QueueDetachMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_min_age: Option<String>,
}

impl QueuePolicyOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn partition_lookahead(mut self, value: impl Into<String>) -> Self {
        self.partition_lookahead = Some(value.into());
        self
    }

    pub fn partition_lookback(mut self, value: impl Into<String>) -> Self {
        self.partition_lookback = Some(value.into());
        self
    }

    pub fn cleanup_ttl(mut self, value: impl Into<String>) -> Self {
        self.cleanup_ttl = Some(value.into());
        self
    }

    pub fn cleanup_limit(mut self, value: i32) -> Self {
        self.cleanup_limit = Some(value);
        self
    }

    pub fn detach_mode(mut self, value: QueueDetachMode) -> Self {
        self.detach_mode = Some(value);
        self
    }

    pub fn detach_min_age(mut self, value: impl Into<String>) -> Self {
        self.detach_min_age = Some(value.into());
        self
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.partition_lookahead.is_none()
            && self.partition_lookback.is_none()
            && self.cleanup_ttl.is_none()
            && self.cleanup_limit.is_none()
            && self.detach_mode.is_none()
            && self.detach_min_age.is_none()
    }

    pub(crate) fn to_sql_json(&self) -> Value {
        let mut obj = Map::new();
        if let Some(value) = &self.partition_lookahead {
            obj.insert(
                "partition_lookahead".to_string(),
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.partition_lookback {
            obj.insert(
                "partition_lookback".to_string(),
                Value::String(value.clone()),
            );
        }
        if let Some(value) = &self.cleanup_ttl {
            obj.insert("cleanup_ttl".to_string(), Value::String(value.clone()));
        }
        if let Some(value) = self.cleanup_limit {
            obj.insert("cleanup_limit".to_string(), json!(value));
        }
        if let Some(value) = self.detach_mode {
            obj.insert(
                "detach_mode".to_string(),
                Value::String(value.as_str().to_string()),
            );
        }
        if let Some(value) = &self.detach_min_age {
            obj.insert("detach_min_age".to_string(), Value::String(value.clone()));
        }
        Value::Object(obj)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default, rename_all = "snake_case")]
#[non_exhaustive]
pub struct CreateQueueOptions {
    #[serde(skip_serializing_if = "is_default_queue_storage_mode")]
    pub storage_mode: QueueStorageMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_lookahead: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_lookback: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_ttl: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleanup_limit: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_mode: Option<QueueDetachMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detach_min_age: Option<String>,
}

impl Default for CreateQueueOptions {
    fn default() -> Self {
        Self {
            storage_mode: QueueStorageMode::Unpartitioned,
            partition_lookahead: None,
            partition_lookback: None,
            cleanup_ttl: None,
            cleanup_limit: None,
            detach_mode: None,
            detach_min_age: None,
        }
    }
}

impl CreateQueueOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn storage_mode(mut self, value: QueueStorageMode) -> Self {
        self.storage_mode = value;
        self
    }

    pub fn partition_lookahead(mut self, value: impl Into<String>) -> Self {
        self.partition_lookahead = Some(value.into());
        self
    }

    pub fn partition_lookback(mut self, value: impl Into<String>) -> Self {
        self.partition_lookback = Some(value.into());
        self
    }

    pub fn cleanup_ttl(mut self, value: impl Into<String>) -> Self {
        self.cleanup_ttl = Some(value.into());
        self
    }

    pub fn cleanup_limit(mut self, value: i32) -> Self {
        self.cleanup_limit = Some(value);
        self
    }

    pub fn detach_mode(mut self, value: QueueDetachMode) -> Self {
        self.detach_mode = Some(value);
        self
    }

    pub fn detach_min_age(mut self, value: impl Into<String>) -> Self {
        self.detach_min_age = Some(value.into());
        self
    }

    pub(crate) fn policy_options(&self) -> QueuePolicyOptions {
        QueuePolicyOptions {
            partition_lookahead: self.partition_lookahead.clone(),
            partition_lookback: self.partition_lookback.clone(),
            cleanup_ttl: self.cleanup_ttl.clone(),
            cleanup_limit: self.cleanup_limit,
            detach_mode: self.detach_mode,
            detach_min_age: self.detach_min_age.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub struct QueuePolicy {
    pub queue_name: String,
    pub storage_mode: QueueStorageMode,
    pub partition_lookahead: String,
    pub partition_lookback: String,
    pub cleanup_ttl: String,
    pub cleanup_limit: i32,
    pub detach_mode: QueueDetachMode,
    pub detach_min_age: String,
}

fn is_default_queue_storage_mode(value: &QueueStorageMode) -> bool {
    *value == QueueStorageMode::Unpartitioned
}

#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
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

#[derive(Clone, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct RetryTaskOptions {
    pub queue: Option<String>,
    pub max_attempts: Option<i32>,
    pub spawn_new: bool,
}

impl RetryTaskOptions {
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

    pub fn spawn_new(mut self) -> Self {
        self.spawn_new = true;
        self
    }

    pub fn spawn_new_task(self) -> Self {
        self.spawn_new()
    }

    pub(crate) fn to_sql_json(&self) -> Result<Value> {
        let mut obj = Map::new();
        if let Some(max_attempts) = self.max_attempts {
            if max_attempts < 1 {
                return Err(Error::Config("max attempts must be at least 1".to_string()));
            }
            obj.insert("max_attempts".to_string(), json!(max_attempts));
        }
        if self.spawn_new {
            obj.insert("spawn_new".to_string(), json!(true));
        }
        Ok(Value::Object(obj))
    }
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct SpawnResult {
    pub task_id: Uuid,
    pub run_id: Uuid,
    pub attempt: i32,
    pub created: bool,
}

#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
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
#[non_exhaustive]
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
#[non_exhaustive]
pub enum UnknownTaskPolicy {
    /// Defer unknown claimed tasks briefly so another worker version can pick them up.
    #[default]
    Defer,
    /// Mark unknown claimed tasks failed immediately.
    Fail,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum WorkerErrorKind {
    Claim,
    Execution,
}

#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct WorkerError<'a> {
    pub kind: WorkerErrorKind,
    pub error: &'a Error,
}

pub type WorkerErrorHandler = Arc<dyn for<'a> Fn(WorkerError<'a>) + Send + Sync + 'static>;

#[derive(Clone)]
#[non_exhaustive]
pub struct WorkerOptions {
    pub worker_id: Option<String>,
    pub claim_timeout: Duration,
    pub concurrency: usize,
    pub batch_size: Option<usize>,
    pub poll_interval: Duration,
    pub unknown_task_policy: UnknownTaskPolicy,
    pub on_error: Option<WorkerErrorHandler>,
    pub fatal_on_lease_timeout: bool,
}

impl fmt::Debug for WorkerOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerOptions")
            .field("worker_id", &self.worker_id)
            .field("claim_timeout", &self.claim_timeout)
            .field("concurrency", &self.concurrency)
            .field("batch_size", &self.batch_size)
            .field("poll_interval", &self.poll_interval)
            .field("unknown_task_policy", &self.unknown_task_policy)
            .field("on_error", &self.on_error.as_ref().map(|_| "<callback>"))
            .field("fatal_on_lease_timeout", &self.fatal_on_lease_timeout)
            .finish()
    }
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
            on_error: None,
            fatal_on_lease_timeout: true,
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

    pub fn on_error<F>(mut self, handler: F) -> Self
    where
        F: for<'a> Fn(WorkerError<'a>) + Send + Sync + 'static,
    {
        self.on_error = Some(Arc::new(handler));
        self
    }

    pub fn fatal_on_lease_timeout(mut self, value: bool) -> Self {
        self.fatal_on_lease_timeout = value;
        self
    }

    pub(crate) fn normalized(&self) -> Result<NormalizedWorkerOptions> {
        if self.claim_timeout.is_zero() {
            return Err(Error::Config(
                "claim timeout must be greater than zero".to_string(),
            ));
        }
        if self.poll_interval.is_zero() {
            return Err(Error::Config(
                "poll interval must be greater than zero".to_string(),
            ));
        }

        let concurrency = self.concurrency.max(1);
        Ok(NormalizedWorkerOptions {
            worker_id: self.worker_id.clone().unwrap_or_else(default_worker_id),
            claim_timeout: self.claim_timeout,
            claim_timeout_seconds: duration_seconds_ceil(self.claim_timeout),
            concurrency,
            batch_size: self.batch_size.unwrap_or(concurrency).max(1),
            poll_interval: self.poll_interval,
            unknown_task_policy: self.unknown_task_policy,
            on_error: self
                .on_error
                .clone()
                .unwrap_or_else(default_worker_error_handler),
            fatal_on_lease_timeout: self.fatal_on_lease_timeout,
        })
    }
}

fn default_worker_error_handler() -> WorkerErrorHandler {
    Arc::new(|worker_error| match worker_error.kind {
        WorkerErrorKind::Claim => {
            tracing::error!(error = %worker_error.error, "failed to claim tasks");
        }
        WorkerErrorKind::Execution => {
            tracing::error!(error = %worker_error.error, "task execution failed");
        }
    })
}

#[derive(Clone)]
pub(crate) struct NormalizedWorkerOptions {
    pub worker_id: String,
    pub claim_timeout: Duration,
    pub claim_timeout_seconds: i32,
    pub concurrency: usize,
    pub batch_size: usize,
    pub poll_interval: Duration,
    pub unknown_task_policy: UnknownTaskPolicy,
    pub on_error: WorkerErrorHandler,
    pub fatal_on_lease_timeout: bool,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
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

    pub(crate) fn claim_timeout_seconds(&self) -> Result<i32> {
        if self.claim_timeout.is_zero() {
            return Err(Error::Config(
                "claim timeout must be greater than zero".to_string(),
            ));
        }
        Ok(duration_seconds_ceil(self.claim_timeout))
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CleanupResult {
    pub tasks_deleted: i32,
    pub events_deleted: i32,
}

pub(crate) fn validate_queue_name(queue_name: &str) -> Result<()> {
    if queue_name.is_empty() {
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
    fn queue_name_validation_rejects_empty() {
        assert!(validate_queue_name("").is_err());
    }

    #[test]
    fn queue_name_validation_accepts_whitespace_only() {
        assert!(validate_queue_name("   \t\n").is_ok());
    }

    #[test]
    fn queue_name_validation_accepts_exactly_57_utf8_bytes() {
        let queue_name = "a".repeat(57);

        assert_eq!(queue_name.len(), 57);
        assert!(validate_queue_name(&queue_name).is_ok());
    }

    #[test]
    fn queue_name_validation_rejects_58_utf8_bytes() {
        let queue_name = "a".repeat(58);

        assert_eq!(queue_name.len(), 58);
        assert!(validate_queue_name(&queue_name).is_err());
    }

    #[test]
    fn queue_name_validation_counts_utf8_bytes() {
        let queue_name = format!("{}a", "é".repeat(28));

        assert_eq!(queue_name.chars().count(), 29);
        assert_eq!(queue_name.len(), 57);
        assert!(validate_queue_name(&queue_name).is_ok());

        let queue_name = "é".repeat(29);

        assert_eq!(queue_name.chars().count(), 29);
        assert_eq!(queue_name.len(), 58);
        assert!(validate_queue_name(&queue_name).is_err());
    }

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

    #[test]
    fn retry_task_options_use_sql_wire_keys() -> Result<()> {
        let options = RetryTaskOptions::new()
            .queue("other")
            .max_attempts(3)
            .spawn_new_task();

        let value = options.to_sql_json()?;
        assert_eq!(value["max_attempts"], json!(3));
        assert_eq!(value["spawn_new"], json!(true));
        assert!(value.get("queue").is_none());
        Ok(())
    }

    #[test]
    fn retry_task_options_validate_max_attempts() -> Result<()> {
        let err = match RetryTaskOptions::new().max_attempts(0).to_sql_json() {
            Ok(_) => {
                return Err(Error::Config(
                    "invalid max attempts should fail".to_string(),
                ));
            }
            Err(err) => err,
        };

        assert!(
            matches!(err, Error::Config(message) if message == "max attempts must be at least 1")
        );
        Ok(())
    }

    #[test]
    fn queue_modes_serialize_to_wire_strings() -> std::result::Result<(), serde_json::Error> {
        assert_eq!(
            serde_json::to_value(QueueStorageMode::Partitioned)?,
            json!("partitioned")
        );
        assert_eq!(
            serde_json::to_value(QueueDetachMode::Empty)?,
            json!("empty")
        );
        assert_eq!(
            serde_json::from_value::<QueueStorageMode>(json!("unpartitioned"))?,
            QueueStorageMode::Unpartitioned
        );
        assert_eq!(
            serde_json::from_value::<QueueDetachMode>(json!("none"))?,
            QueueDetachMode::None
        );
        Ok(())
    }

    #[test]
    fn queue_policy_options_use_sql_wire_keys() {
        let options = QueuePolicyOptions::new()
            .partition_lookahead("35 days")
            .partition_lookback("2 days")
            .cleanup_ttl("12345 seconds")
            .cleanup_limit(77)
            .detach_mode(QueueDetachMode::Empty)
            .detach_min_age("45 days");

        assert_eq!(
            options.to_sql_json(),
            json!({
                "partition_lookahead": "35 days",
                "partition_lookback": "2 days",
                "cleanup_ttl": "12345 seconds",
                "cleanup_limit": 77,
                "detach_mode": "empty",
                "detach_min_age": "45 days",
            })
        );
    }

    #[test]
    fn create_queue_options_serialize_with_upstream_keys()
    -> std::result::Result<(), serde_json::Error> {
        let options = CreateQueueOptions::new()
            .storage_mode(QueueStorageMode::Partitioned)
            .cleanup_ttl("12345 seconds")
            .detach_mode(QueueDetachMode::Empty);

        assert_eq!(
            serde_json::to_value(&options)?,
            json!({
                "storage_mode": "partitioned",
                "cleanup_ttl": "12345 seconds",
                "detach_mode": "empty",
            })
        );
        assert_eq!(
            options.policy_options().to_sql_json(),
            json!({
                "cleanup_ttl": "12345 seconds",
                "detach_mode": "empty",
            })
        );
        Ok(())
    }

    #[test]
    fn worker_options_default_to_fatal_lease_timeout() {
        let options = WorkerOptions::default();
        assert!(options.fatal_on_lease_timeout);
        assert!(options.on_error.is_none());
    }

    #[test]
    fn worker_options_can_disable_fatal_lease_timeout() -> Result<()> {
        let options = WorkerOptions::new().fatal_on_lease_timeout(false);
        let normalized = options.normalized()?;

        assert!(!normalized.fatal_on_lease_timeout);
        Ok(())
    }

    #[test]
    fn worker_options_normalize_preserves_custom_error_handler() -> Result<()> {
        let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let handler_seen = Arc::clone(&seen);
        let options = WorkerOptions::new().on_error(move |worker_error| {
            if worker_error.kind == WorkerErrorKind::Claim {
                handler_seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        });

        let normalized = options.normalized()?;
        let err = Error::message("claim failed");
        (normalized.on_error)(WorkerError {
            kind: WorkerErrorKind::Claim,
            error: &err,
        });

        assert_eq!(seen.load(std::sync::atomic::Ordering::SeqCst), 1);
        Ok(())
    }
}
