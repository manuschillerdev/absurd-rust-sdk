//! Community Rust SDK for Absurd.
//!
//! This crate is intentionally small: typed async Rust wrappers around Absurd's
//! Postgres stored procedures, plus a Tokio worker and durable task context.

mod client;
mod context;
mod error;
mod executor;
mod task;
mod types;
mod worker;

pub use client::Client;
pub use context::{AwaitEventOptions, TaskContext};
pub use error::{Error, Result};
pub use task::Task;
pub use types::{
    CancellationPolicy, ClaimedTask, CleanupResult, Json, RetryStrategy, SpawnOptions, SpawnResult,
    TaskOptions, UnknownTaskPolicy, WorkBatchOptions, WorkerOptions,
};
pub use worker::Worker;

/// Compatibility alias for people who prefer the project name as the client type.
pub type Absurd = Client;

pub mod prelude {
    pub use crate::{
        Absurd, AwaitEventOptions, CancellationPolicy, Client, Error, Result, RetryStrategy,
        SpawnOptions, Task, TaskContext, TaskOptions, UnknownTaskPolicy, WorkBatchOptions, Worker,
        WorkerOptions,
    };
}
