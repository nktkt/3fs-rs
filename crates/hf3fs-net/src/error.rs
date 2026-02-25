use thiserror::Error;

/// Errors that can occur in the networking layer.
#[derive(Debug, Error)]
pub enum NetError {
    /// The connection was closed by the remote peer.
    #[error("connection closed")]
    ConnectionClosed,

    /// The connection was refused by the remote peer.
    #[error("connection refused")]
    ConnectionRefused,

    /// An operation timed out.
    #[error("timeout")]
    Timeout,

    /// Sending data failed.
    #[error("send failed: {0}")]
    SendFailed(String),

    /// An I/O error from the underlying transport.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The message checksum did not match the computed CRC32C.
    #[error("checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    /// The received message is not a valid serde message (bad magic number).
    #[error("invalid message: not a serde message (checksum low byte: {0:#04x})")]
    InvalidMagic(u8),

    /// The message size exceeds the maximum allowed.
    #[error("message too large: {size} bytes (max {max})")]
    MessageTooLarge { size: usize, max: usize },

    /// The message header is incomplete (not enough bytes for the 8-byte header).
    #[error("incomplete header: need {need} bytes, have {have}")]
    IncompleteHeader { need: usize, have: usize },

    /// The requested service was not found in the registry.
    #[error("service not found: service_id={0}")]
    ServiceNotFound(u16),

    /// The server is shutting down and not accepting new requests.
    #[error("server shutting down")]
    ShuttingDown,

    /// A serialization/deserialization error from the wire format.
    #[error("wire error: {0}")]
    WireError(#[from] hf3fs_serde::WireError),

    /// An error propagated from a service handler (carries an hf3fs Status).
    #[error("service error: {0}")]
    ServiceError(#[from] hf3fs_types::Status),
}

/// Convenience alias kept for backward compatibility with existing code that
/// references `SocketError`.
pub type SocketError = NetError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_connection_closed() {
        let err = NetError::ConnectionClosed;
        assert_eq!(err.to_string(), "connection closed");
    }

    #[test]
    fn test_display_checksum_mismatch() {
        let err = NetError::ChecksumMismatch {
            expected: 0xAABBCC86,
            actual: 0x11223386,
        };
        let s = err.to_string();
        assert!(s.contains("checksum mismatch"));
        assert!(s.contains("0xaabbcc86"));
        assert!(s.contains("0x11223386"));
    }

    #[test]
    fn test_display_message_too_large() {
        let err = NetError::MessageTooLarge {
            size: 1_000_000_000,
            max: 512 * 1024 * 1024,
        };
        assert!(err.to_string().contains("message too large"));
    }

    #[test]
    fn test_display_service_not_found() {
        let err = NetError::ServiceNotFound(42);
        assert!(err.to_string().contains("42"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broke");
        let net_err: NetError = io_err.into();
        assert!(matches!(net_err, NetError::Io(_)));
        assert!(net_err.to_string().contains("pipe broke"));
    }

    #[test]
    fn test_wire_error_conversion() {
        let wire_err = hf3fs_serde::WireError::InsufficientData { need: 8, have: 2 };
        let net_err: NetError = wire_err.into();
        assert!(matches!(net_err, NetError::WireError(_)));
    }
}
