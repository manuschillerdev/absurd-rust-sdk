use thiserror::Error as ThisError;
use uuid::Uuid;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, ThisError)]
#[non_exhaustive]
pub enum Error {
    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("database pool error: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("task {0:?} is not registered")]
    TaskNotRegistered(String),

    #[error("task {0:?} is already registered")]
    TaskAlreadyRegistered(String),

    #[error("task {task_id} not found")]
    TaskNotFound { task_id: Uuid },

    #[error("timed out waiting for task {task_id}")]
    TaskResultTimeout { task_id: Uuid },

    /// Internal worker control-flow sentinel for a run that was durably suspended.
    ///
    /// Task code should not construct this directly. Propagate it with `?` from
    /// [`TaskContext`](crate::TaskContext) operations such as sleep or event waits;
    /// the worker consumes it instead of recording a task failure.
    #[error("task suspended")]
    Suspended,

    /// Internal worker control-flow sentinel for a task cancelled in Absurd.
    ///
    /// This is produced from Absurd SQLSTATE `AB001`. User code should call
    /// [`Client::cancel_task`](crate::Client::cancel_task) rather than returning
    /// this variant directly; the worker treats it as a terminal-state race.
    #[error("task cancelled")]
    Cancelled,

    /// Internal worker control-flow sentinel for a run that already failed.
    ///
    /// This is produced from Absurd SQLSTATE `AB002`, for example when another
    /// worker or claim-timeout sweep failed the run first. The worker consumes it
    /// instead of trying to fail the run again.
    #[error("task run is already failed")]
    AlreadyFailed,

    #[error("timed out waiting for event {event:?}")]
    EventTimeout { event: String },

    #[error("worker task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("invalid task headers: expected a JSON object")]
    InvalidHeaders,

    #[error("{0}")]
    Message(String),
}

impl Error {
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }

    pub fn is_suspended(&self) -> bool {
        matches!(self, Self::Suspended)
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    /// Returns true for internal worker control-flow sentinels.
    ///
    /// Task handlers should normally only propagate these with `?`; worker
    /// execution consumes them instead of recording a task failure.
    pub fn is_control_flow(&self) -> bool {
        matches!(
            self,
            Self::Suspended | Self::Cancelled | Self::AlreadyFailed
        )
    }

    pub fn is_terminal_state_race(&self) -> bool {
        matches!(self, Self::Cancelled | Self::AlreadyFailed)
    }
}

pub(crate) fn map_database_error(err: tokio_postgres::Error) -> Error {
    if let Some(db_error) = err.as_db_error() {
        match db_error.code().code() {
            "AB001" => return Error::Cancelled,
            "AB002" => return Error::AlreadyFailed,
            _ => {}
        }
    }
    Error::Database(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_flow_sentinels_are_classified() {
        assert!(Error::Suspended.is_control_flow());
        assert!(Error::Cancelled.is_control_flow());
        assert!(Error::AlreadyFailed.is_control_flow());
        assert!(!Error::message("boom").is_control_flow());
    }

    #[test]
    fn only_terminal_races_are_terminal_state_races() {
        assert!(Error::Cancelled.is_terminal_state_race());
        assert!(Error::AlreadyFailed.is_terminal_state_race());
        assert!(!Error::Suspended.is_terminal_state_race());
        assert!(!Error::message("boom").is_terminal_state_race());
    }
}
