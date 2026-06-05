//! Community Rust SDK for Absurd.
//!
//! This crate is intentionally small: typed async Rust wrappers around Absurd's
//! Postgres stored procedures, plus a Tokio worker and durable task context.

mod client;
mod context;
mod error;
mod executor;
mod hooks;
mod task;
mod task_result;
mod types;
mod worker;

pub use client::Client;
pub use context::{AwaitEventOptions, StepHandle, TaskContext};
pub use error::{Error, Result};
pub use hooks::{
    AbsurdHooks, BeforeSpawnFuture, BeforeSpawnHook, Hooks, TaskExecution, TaskExecutionFuture,
    WrapTaskExecutionHook,
};
pub use task::Task;
pub use task_result::{AwaitTaskResultOptions, TaskResultSnapshot, TaskResultState};
pub use types::{
    CancellationPolicy, CleanupResult, CreateQueueOptions, Json, QueueDetachMode, QueuePolicy,
    QueuePolicyOptions, QueueStorageMode, RetryStrategy, RetryTaskOptions, SpawnOptions,
    SpawnResult, TaskOptions, UnknownTaskPolicy, WorkBatchOptions, WorkerError, WorkerErrorHandler,
    WorkerErrorKind, WorkerOptions,
};
pub use worker::Worker;

/// Compatibility alias for people who prefer the project name as the client type.
pub type Absurd = Client;

pub mod prelude {
    pub use crate::{
        Absurd, AwaitEventOptions, AwaitTaskResultOptions, CancellationPolicy, Client,
        CreateQueueOptions, Error, Hooks, QueueDetachMode, QueuePolicy, QueuePolicyOptions,
        QueueStorageMode, Result, RetryStrategy, RetryTaskOptions, SpawnOptions, StepHandle, Task,
        TaskContext, TaskOptions, TaskResultSnapshot, TaskResultState, UnknownTaskPolicy,
        WorkBatchOptions, Worker, WorkerError, WorkerErrorHandler, WorkerErrorKind, WorkerOptions,
    };
}
