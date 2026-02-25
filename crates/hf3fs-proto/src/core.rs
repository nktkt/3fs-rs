//! Core service RPC messages.
//!
//! Based on 3FS/src/fbs/core/service/Rpc.h

use hf3fs_serde::{WireDeserialize, WireSerialize};
use serde::{Deserialize, Serialize};

/// Echo request/response - mirrors `EchoMessage` in C++.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct EchoReq {
    pub str: String,
}

/// Echo response (same shape as request).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct EchoRsp {
    pub str: String,
}

/// Request to retrieve the current configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct GetConfigReq {
    pub config_key: String,
}

/// Response containing the current configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct GetConfigRsp {
    pub config: String,
}

/// Request to render a configuration template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct RenderConfigReq {
    pub config_template: String,
    pub test_update: bool,
    pub is_hot_update: bool,
}

/// Response with the rendered configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct RenderConfigRsp {
    pub config_after_render: String,
    pub update_status: i32,
    pub config_after_update: String,
}

/// Request to hot-update the running configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct HotUpdateConfigReq {
    pub update: String,
    pub render: bool,
}

/// Response acknowledging a hot config update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct HotUpdateConfigRsp {}

/// Request to retrieve the last config update record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct GetLastConfigUpdateRecordReq {}

/// Response with the last config update record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct GetLastConfigUpdateRecordRsp {
    pub record: Option<String>,
}

/// Request to shut down the service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct ShutdownReq {
    pub graceful: bool,
}

/// Response acknowledging the shutdown request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct ShutdownRsp {}

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
    fn test_echo_roundtrip() {
        let req = EchoReq {
            str: "hello".to_string(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = EchoRsp {
            str: "world".to_string(),
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_get_config_roundtrip() {
        let req = GetConfigReq {
            config_key: "my.key".to_string(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = GetConfigRsp {
            config: "value = 42".to_string(),
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_render_config_roundtrip() {
        let req = RenderConfigReq {
            config_template: "template".to_string(),
            test_update: true,
            is_hot_update: false,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = RenderConfigRsp {
            config_after_render: "rendered".to_string(),
            update_status: 0,
            config_after_update: "updated".to_string(),
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_hot_update_config_roundtrip() {
        let req = HotUpdateConfigReq {
            update: "new_config".to_string(),
            render: true,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = HotUpdateConfigRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_get_last_config_update_record_roundtrip() {
        let req = GetLastConfigUpdateRecordReq {};
        assert_eq!(roundtrip(&req), req);

        let rsp = GetLastConfigUpdateRecordRsp {
            record: Some("record_data".to_string()),
        };
        assert_eq!(roundtrip(&rsp), rsp);

        let rsp_none = GetLastConfigUpdateRecordRsp { record: None };
        assert_eq!(roundtrip(&rsp_none), rsp_none);
    }

    #[test]
    fn test_shutdown_roundtrip() {
        let req = ShutdownReq { graceful: true };
        assert_eq!(roundtrip(&req), req);

        let req_false = ShutdownReq { graceful: false };
        assert_eq!(roundtrip(&req_false), req_false);

        let rsp = ShutdownRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }
}
