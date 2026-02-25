use crate::status::Status;
use crate::status_code::status_code_t;

/// The standard result type used throughout hf3fs, with `Status` as the error.
pub type Result<T> = std::result::Result<T, Status>;

/// Alias for the unit type, mirroring C++ `Void = folly::Unit`.
pub type Void = ();

/// Create an error result from a status code.
pub fn make_error<T>(code: status_code_t) -> Result<T> {
    Err(Status::new(code))
}

/// Create an error result from a status code and message.
pub fn make_error_msg<T>(code: status_code_t, msg: impl Into<String>) -> Result<T> {
    Err(Status::with_message(code, msg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status_code::{MetaCode, StatusCode};

    #[test]
    fn test_make_error() {
        let r: Result<i32> = make_error(MetaCode::NOT_FOUND);
        assert!(r.is_err());
        assert_eq!(r.unwrap_err().code(), 3000);
    }

    #[test]
    fn test_make_error_msg() {
        let r: Result<i32> = make_error_msg(StatusCode::INVALID_ARG, "bad param");
        let err = r.unwrap_err();
        assert_eq!(err.code(), 3);
        assert_eq!(err.message(), Some("bad param"));
    }

    #[test]
    fn test_ok_result() {
        let r: Result<i32> = Ok(42);
        assert_eq!(r.unwrap(), 42);
    }

    #[test]
    fn test_void_type() {
        let v: Void = ();
        assert_eq!(v, ());
    }
}
