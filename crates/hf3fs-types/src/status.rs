use std::fmt;

use crate::status_code::{self, StatusCode, status_code_t};

/// A status value carrying a code and optional message.
///
/// Mirrors the C++ `Status` class. The `#[must_use]` attribute ensures
/// callers do not silently ignore error statuses.
#[derive(Debug, Clone)]
#[must_use]
pub struct Status {
    code: status_code_t,
    message: Option<String>,
}

impl Status {
    /// Create a status with just a code.
    pub fn new(code: status_code_t) -> Self {
        Self {
            code,
            message: None,
        }
    }

    /// Create a status with a code and a descriptive message.
    pub fn with_message(code: status_code_t, msg: impl Into<String>) -> Self {
        Self {
            code,
            message: Some(msg.into()),
        }
    }

    /// Return the numeric status code.
    pub fn code(&self) -> status_code_t {
        self.code
    }

    /// Return the optional message.
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Whether this status represents success (code == OK).
    pub fn is_ok(&self) -> bool {
        self.code == StatusCode::OK
    }

    /// Produce a human-readable description like `"NotFound(3000) file missing"`.
    pub fn describe(&self) -> String {
        let name = status_code::to_string(self.code);
        match &self.message {
            Some(msg) => format!("{}({}) {}", name, self.code, msg),
            None => format!("{}({})", name, self.code),
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.describe())
    }
}

impl std::error::Error for Status {}

impl From<status_code_t> for Status {
    fn from(code: status_code_t) -> Self {
        Self::new(code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status_code::{MetaCode, RPCCode};

    #[test]
    fn test_status_ok() {
        let s = Status::new(StatusCode::OK);
        assert!(s.is_ok());
        assert_eq!(s.code(), 0);
        assert!(s.message().is_none());
        assert_eq!(s.describe(), "OK(0)");
    }

    #[test]
    fn test_status_with_message() {
        let s = Status::with_message(MetaCode::NOT_FOUND, "file not found");
        assert!(!s.is_ok());
        assert_eq!(s.code(), 3000);
        assert_eq!(s.message(), Some("file not found"));
        assert_eq!(s.describe(), "Meta::NotFound(3000) file not found");
    }

    #[test]
    fn test_status_display() {
        let s = Status::new(RPCCode::TIMEOUT);
        assert_eq!(format!("{}", s), "RPC::Timeout(2005)");
    }

    #[test]
    fn test_status_from_code() {
        let s: Status = StatusCode::INVALID_ARG.into();
        assert_eq!(s.code(), 3);
    }

    #[test]
    fn test_status_is_error() {
        // Status implements std::error::Error
        let s = Status::new(StatusCode::UNKNOWN);
        let e: &dyn std::error::Error = &s;
        assert!(e.to_string().contains("Unknown"));
    }
}
