use thiserror::Error as ThisError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, ThisError)]
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

    #[error("task suspended")]
    Suspended,

    #[error("task cancelled")]
    Cancelled,

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
