//! Management daemon (mgmtd) RPC types.
//!
//! Stubs for now - will be expanded when mgmtd service is implemented.

use hf3fs_serde::{WireDeserialize, WireSerialize};
use serde::{Deserialize, Serialize};

/// Node type in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeType {
    Meta = 0,
    Storage = 1,
    Client = 2,
    Mgmtd = 3,
}

impl TryFrom<u8> for NodeType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Meta),
            1 => Ok(Self::Storage),
            2 => Ok(Self::Client),
            3 => Ok(Self::Mgmtd),
            _ => Err(()),
        }
    }
}

impl WireSerialize for NodeType {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for NodeType {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "NodeType",
            value: v as u64,
        })
    }
}

/// Heartbeat request from a node to mgmtd.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct HeartbeatReq {
    pub node_id: u32,
    pub node_type: u8,
}

/// Heartbeat response from mgmtd.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct HeartbeatRsp {}

/// Request to get cluster info.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct GetClusterInfoReq {}

/// Cluster info response.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct GetClusterInfoRsp {
    pub cluster_id: String,
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
    fn test_node_type_roundtrip() {
        assert_eq!(roundtrip(&NodeType::Meta), NodeType::Meta);
        assert_eq!(roundtrip(&NodeType::Storage), NodeType::Storage);
        assert_eq!(roundtrip(&NodeType::Client), NodeType::Client);
        assert_eq!(roundtrip(&NodeType::Mgmtd), NodeType::Mgmtd);
    }

    #[test]
    fn test_heartbeat_roundtrip() {
        let req = HeartbeatReq {
            node_id: 42,
            node_type: NodeType::Storage as u8,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = HeartbeatRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }
}
