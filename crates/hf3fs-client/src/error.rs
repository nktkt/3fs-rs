//! Client error types.

use hf3fs_net::SocketError;
use hf3fs_types::Status;

/// Errors that can occur during client operations.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// A network-level error (connection, timeout, etc.).
    #[error("network error: {0}")]
    Net(#[from] SocketError),

    /// A service returned a non-OK status.
    #[error("status error: {0}")]
    Status(#[from] Status),

    /// No server is available to handle the request.
    #[error("no server available: {0}")]
    NoServerAvailable(String),

    /// Retry budget exhausted.
    #[error("retry exhausted after {attempts} attempts: {message}")]
    RetryExhausted {
        attempts: u32,
        message: String,
    },

    /// The client has not been started or has been stopped.
    #[error("client not running")]
    NotRunning,

    /// Routing info is not yet available from mgmtd.
    #[error("routing info not ready")]
    RoutingInfoNotReady,

    /// Configuration error.
    #[error("config error: {0}")]
    Config(String),

    /// An internal / unexpected error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<anyhow::Error> for ClientError {
    fn from(err: anyhow::Error) -> Self {
        ClientError::Internal(err.to_string())
    }
}

/// Convenience result type.
pub type ClientResult<T> = std::result::Result<T, ClientError>;
