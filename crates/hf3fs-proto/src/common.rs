use hf3fs_serde::{WireDeserialize, WireSerialize};
use serde::{Deserialize, Serialize};

/// Protocol version information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
    pub hash: u32,
}

/// Timestamp tracking for request/response lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct Timestamp {
    pub client_called: i64,
    pub client_serialized: i64,
    pub server_received: i64,
    pub server_waked: i64,
    pub server_processed: i64,
    pub server_serialized: i64,
    pub client_received: i64,
    pub client_waked: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_serde::{WireDeserialize, WireSerialize};

    fn roundtrip<T: WireSerialize + WireDeserialize + std::fmt::Debug + PartialEq>(val: &T) -> T {
        let mut buf = Vec::new();
        val.wire_serialize(&mut buf).unwrap();
        let mut offset = 0;
        let result = T::wire_deserialize(&buf, &mut offset).unwrap();
        assert_eq!(offset, buf.len());
        result
    }

    #[test]
    fn test_version_roundtrip() {
        let v = Version {
            major: 1,
            minor: 2,
            patch: 3,
            hash: 0xDEADBEEF,
        };
        assert_eq!(roundtrip(&v), v);
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let ts = Timestamp {
            client_called: 1000,
            client_serialized: 1001,
            server_received: 2000,
            server_waked: 2001,
            server_processed: 2500,
            server_serialized: 2501,
            client_received: 3000,
            client_waked: 3001,
        };
        assert_eq!(roundtrip(&ts), ts);
    }

    #[test]
    fn test_timestamp_default() {
        let ts = Timestamp::default();
        assert_eq!(ts.client_called, 0);
        assert_eq!(ts.server_received, 0);
    }
}
