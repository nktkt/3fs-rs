//! Management daemon (mgmtd) RPC types.
//!
//! Based on 3FS/src/fbs/mgmtd/Rpc.h and MgmtdServiceDef.h
//!
//! Service methods (from C++ MgmtdServiceDef.h):
//!   1  - getPrimaryMgmtd(GetPrimaryMgmtdReq, GetPrimaryMgmtdRsp)
//!   3  - heartbeat(HeartbeatReq, HeartbeatRsp)
//!   4  - registerNode(RegisterNodeReq, RegisterNodeRsp)
//!   5  - getRoutingInfo(GetRoutingInfoReq, GetRoutingInfoRsp)
//!   6  - setConfig(SetConfigReq, SetConfigRsp)
//!   7  - getConfig(GetConfigReq, GetConfigRsp)
//!   8  - setChainTable(SetChainTableReq, SetChainTableRsp)
//!   9  - enableNode(EnableNodeReq, EnableNodeRsp)
//!   10 - disableNode(DisableNodeReq, DisableNodeRsp)
//!   11 - extendClientSession(ExtendClientSessionReq, ExtendClientSessionRsp)
//!   12 - listClientSessions(ListClientSessionsReq, ListClientSessionsRsp)
//!   13 - setNodeTags(SetNodeTagsReq, SetNodeTagsRsp)
//!   14 - unregisterNode(UnregisterNodeReq, UnregisterNodeRsp)
//!   15 - setChains(SetChainsReq, SetChainsRsp)
//!   16 - setUniversalTags(SetUniversalTagsReq, SetUniversalTagsRsp)
//!   17 - getUniversalTags(GetUniversalTagsReq, GetUniversalTagsRsp)
//!   18 - getConfigVersions(GetConfigVersionsReq, GetConfigVersionsRsp)
//!   19 - getClientSession(GetClientSessionReq, GetClientSessionRsp)
//!   20 - rotateLastSrv(RotateLastSrvReq, RotateLastSrvRsp)
//!   21 - listOrphanTargets(ListOrphanTargetsReq, ListOrphanTargetsRsp)
//!   22 - setPreferredTargetOrder(SetPreferredTargetOrderReq, SetPreferredTargetOrderRsp)
//!   23 - rotateAsPreferredOrder(RotateAsPreferredOrderReq, RotateAsPreferredOrderRsp)
//!   24 - updateChain(UpdateChainReq, UpdateChainRsp)

use hf3fs_serde::{WireDeserialize, WireSerialize};
use serde::{Deserialize, Serialize};

use crate::common::*;

// Re-export NodeType so users of this module can access it directly.
pub use crate::common::NodeType;

// ---------------------------------------------------------------------------
// UpdateChainMode
// ---------------------------------------------------------------------------

/// Mode for UpdateChain operation.
///
/// Mirrors `mgmtd::UpdateChainReq::Mode` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum UpdateChainMode {
    Add = 0,
    Remove = 1,
}

impl Default for UpdateChainMode {
    fn default() -> Self {
        Self::Add
    }
}

impl TryFrom<u8> for UpdateChainMode {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Add),
            1 => Ok(Self::Remove),
            _ => Err(()),
        }
    }
}

impl WireSerialize for UpdateChainMode {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for UpdateChainMode {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "UpdateChainMode",
            value: v as u64,
        })
    }
}

// ---------------------------------------------------------------------------
// RPC Request / Response types
// ---------------------------------------------------------------------------

// ---- GetPrimaryMgmtd ----

/// Request to get the primary mgmtd node.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetPrimaryMgmtdReq {
    pub cluster_id: String,
    pub user: UserInfo,
}

/// Response with the primary mgmtd node.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetPrimaryMgmtdRsp {
    pub primary: Option<PersistentNodeInfo>,
}

// ---- Heartbeat ----

/// Heartbeat request from a node to mgmtd.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct HeartbeatReq {
    pub cluster_id: String,
    pub app: AppInfo,
    pub heartbeat_version: u64,
    pub config_version: u64,
    pub config_status: u8,
    pub timestamp: i64,
    pub user: UserInfo,
    /// Storage heartbeat: local target infos. Empty for non-storage nodes.
    pub local_targets: Vec<LocalTargetInfo>,
}

/// Heartbeat response from mgmtd.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct HeartbeatRsp {
    pub config: Option<ConfigInfo>,
}

// ---- RegisterNode ----

/// Request to register a node with mgmtd.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RegisterNodeReq {
    pub cluster_id: String,
    pub node_id: u32,
    pub node_type: u8,
    pub user: UserInfo,
}

/// Response acknowledging node registration.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RegisterNodeRsp {}

// ---- UnregisterNode ----

/// Request to unregister a node.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct UnregisterNodeReq {
    pub cluster_id: String,
    pub node_id: u32,
    pub user: UserInfo,
}

/// Response acknowledging node unregistration.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct UnregisterNodeRsp {}

// ---- GetRoutingInfo ----

/// Request to get routing info.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetRoutingInfoReq {
    pub cluster_id: String,
    pub routing_info_version: u64,
    pub user: UserInfo,
}

/// Response with routing info.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetRoutingInfoRsp {
    pub info: Option<RoutingInfo>,
}

// ---- SetConfig ----

/// Request to set config for a node type.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetConfigReq {
    pub cluster_id: String,
    pub node_type: u8,
    pub content: String,
    pub desc: String,
    pub user: UserInfo,
}

/// Response with the new config version.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetConfigRsp {
    pub config_version: u64,
}

// ---- GetConfig ----

/// Request to get config for a node type.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetConfigReq {
    pub cluster_id: String,
    pub node_type: u8,
    pub config_version: u64,
    pub exact_version: bool,
    pub user: UserInfo,
}

/// Response with config info.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetConfigRsp {
    pub info: Option<ConfigInfo>,
}

// ---- GetConfigVersions ----

/// Request to get config versions for all node types.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetConfigVersionsReq {
    pub cluster_id: String,
    pub user: UserInfo,
}

/// Response with config versions per node type name.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetConfigVersionsRsp {
    pub versions: Vec<(String, u64)>,
}

// ---- SetChainTable ----

/// Request to set a chain table.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetChainTableReq {
    pub cluster_id: String,
    pub chain_table_id: u32,
    pub chains: Vec<u32>,
    pub desc: String,
    pub user: UserInfo,
}

/// Response with the new chain table version.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetChainTableRsp {
    pub chain_table_version: u32,
}

// ---- EnableNode ----

/// Request to enable a node.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct EnableNodeReq {
    pub cluster_id: String,
    pub node_id: u32,
    pub user: UserInfo,
}

/// Response acknowledging node enable.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct EnableNodeRsp {}

// ---- DisableNode ----

/// Request to disable a node.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct DisableNodeReq {
    pub cluster_id: String,
    pub node_id: u32,
    pub user: UserInfo,
}

/// Response acknowledging node disable.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct DisableNodeRsp {}

// ---- ExtendClientSession ----

/// Request to extend a client session.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ExtendClientSessionReq {
    pub cluster_id: String,
    pub client_id: String,
    pub client_session_version: u64,
    pub config_version: u64,
    pub data: ClientSessionData,
    pub config_status: u8,
    pub node_type: u8,
    pub user: UserInfo,
    pub client_start: i64,
}

/// Response with updated config info and tags.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ExtendClientSessionRsp {
    pub config: Option<ConfigInfo>,
    pub tags: Vec<TagPair>,
}

// ---- ListClientSessions ----

/// Request to list all client sessions.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ListClientSessionsReq {
    pub cluster_id: String,
    pub user: UserInfo,
}

/// Response with all client sessions.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ListClientSessionsRsp {
    pub bootstrapping: bool,
    pub sessions: Vec<ClientSession>,
}

// ---- GetClientSession ----

/// Request to get a specific client session.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetClientSessionReq {
    pub cluster_id: String,
    pub client_id: String,
    pub user: UserInfo,
}

/// Response with the client session.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetClientSessionRsp {
    pub bootstrapping: bool,
    pub session: Option<ClientSession>,
    pub referenced_tags: Vec<TagPair>,
}

// ---- SetNodeTags ----

/// Request to set tags on a node.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetNodeTagsReq {
    pub cluster_id: String,
    pub node_id: u32,
    pub tags: Vec<TagPair>,
    pub mode: u8,
    pub user: UserInfo,
}

/// Response with updated node info.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetNodeTagsRsp {
    pub info: NodeInfo,
}

// ---- SetChains ----

/// Request to configure chains.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetChainsReq {
    pub cluster_id: String,
    pub chains: Vec<ChainSetting>,
    pub user: UserInfo,
}

/// Response acknowledging chain configuration.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetChainsRsp {}

// ---- SetUniversalTags ----

/// Request to set universal tags.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetUniversalTagsReq {
    pub cluster_id: String,
    pub universal_id: String,
    pub tags: Vec<TagPair>,
    pub mode: u8,
    pub user: UserInfo,
}

/// Response with updated tags.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetUniversalTagsRsp {
    pub tags: Vec<TagPair>,
}

// ---- GetUniversalTags ----

/// Request to get universal tags.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetUniversalTagsReq {
    pub cluster_id: String,
    pub universal_id: String,
    pub user: UserInfo,
}

/// Response with universal tags.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetUniversalTagsRsp {
    pub tags: Vec<TagPair>,
}

// ---- RotateLastSrv ----

/// Request to rotate the last-server in a chain.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RotateLastSrvReq {
    pub cluster_id: String,
    pub chain_id: u32,
    pub user: UserInfo,
}

/// Response with the updated chain info.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RotateLastSrvRsp {
    pub chain: ChainInfo,
}

// ---- ListOrphanTargets ----

/// Request to list orphan targets.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ListOrphanTargetsReq {
    pub cluster_id: String,
    pub user: UserInfo,
}

/// Response with orphan targets.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ListOrphanTargetsRsp {
    pub targets: Vec<TargetInfo>,
}

// ---- SetPreferredTargetOrder ----

/// Request to set preferred target order for a chain.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetPreferredTargetOrderReq {
    pub cluster_id: String,
    pub chain_id: u32,
    pub preferred_target_order: Vec<u64>,
    pub user: UserInfo,
}

/// Response with the updated chain info.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SetPreferredTargetOrderRsp {
    pub chain: ChainInfo,
}

// ---- RotateAsPreferredOrder ----

/// Request to rotate chain targets to match preferred order.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RotateAsPreferredOrderReq {
    pub cluster_id: String,
    pub chain_id: u32,
    pub user: UserInfo,
}

/// Response with the updated chain info.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RotateAsPreferredOrderRsp {
    pub chain: ChainInfo,
}

// ---- UpdateChain ----

/// Request to add or remove a target from a chain.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct UpdateChainReq {
    pub cluster_id: String,
    pub user: UserInfo,
    pub chain_id: u32,
    pub target_id: u64,
    pub mode: u8,
}

/// Response with the updated chain info.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct UpdateChainRsp {
    pub chain: ChainInfo,
}

// ---------------------------------------------------------------------------
// Compatibility aliases
// ---------------------------------------------------------------------------

pub type GetClusterInfoReq = GetRoutingInfoReq;
pub type GetClusterInfoRsp = GetRoutingInfoRsp;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
    fn test_get_primary_mgmtd_roundtrip() {
        let req = GetPrimaryMgmtdReq {
            cluster_id: "cluster-1".to_string(),
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = GetPrimaryMgmtdRsp {
            primary: Some(PersistentNodeInfo {
                node_id: 1,
                node_type: NodeType::Mgmtd as u8,
                service_groups: vec![],
                tags: vec![],
                hostname: "mgmtd01".to_string(),
            }),
        };
        assert_eq!(roundtrip(&rsp), rsp);

        let rsp_none = GetPrimaryMgmtdRsp { primary: None };
        assert_eq!(roundtrip(&rsp_none), rsp_none);
    }

    #[test]
    fn test_heartbeat_roundtrip() {
        let req = HeartbeatReq {
            cluster_id: "cluster-1".to_string(),
            app: AppInfo {
                node_id: 5,
                node_type: 2,
                hostname: "storage01".to_string(),
                pid: 1234,
                service_groups: vec![],
                release_version: ReleaseVersion::default(),
                podname: String::new(),
            },
            heartbeat_version: 1,
            config_version: 0,
            config_status: ConfigStatus::Normal as u8,
            timestamp: 1234567890,
            user: UserInfo::default(),
            local_targets: vec![LocalTargetInfo {
                target_id: 100,
                local_state: LocalTargetState::UpToDate as u8,
                disk_index: Some(0),
                used_size: 1024,
                chain_version: 1,
                low_space: false,
            }],
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = HeartbeatRsp {
            config: Some(ConfigInfo {
                config_version: 1,
                content: "cfg".to_string(),
                desc: "desc".to_string(),
            }),
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_register_node_roundtrip() {
        let req = RegisterNodeReq {
            cluster_id: "c1".to_string(),
            node_id: 10,
            node_type: NodeType::Storage as u8,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = RegisterNodeRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_unregister_node_roundtrip() {
        let req = UnregisterNodeReq {
            cluster_id: "c1".to_string(),
            node_id: 10,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = UnregisterNodeRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_get_routing_info_roundtrip() {
        let req = GetRoutingInfoReq {
            cluster_id: "c1".to_string(),
            routing_info_version: 0,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = GetRoutingInfoRsp { info: None };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_set_config_roundtrip() {
        let req = SetConfigReq {
            cluster_id: "c1".to_string(),
            node_type: NodeType::Meta as u8,
            content: "key=value".to_string(),
            desc: "test".to_string(),
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = SetConfigRsp {
            config_version: 42,
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_get_config_roundtrip() {
        let req = GetConfigReq {
            cluster_id: "c1".to_string(),
            node_type: NodeType::Storage as u8,
            config_version: 0,
            exact_version: false,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = GetConfigRsp {
            info: Some(ConfigInfo {
                config_version: 1,
                content: "data".to_string(),
                desc: "v1".to_string(),
            }),
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_get_config_versions_roundtrip() {
        let req = GetConfigVersionsReq {
            cluster_id: "c1".to_string(),
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = GetConfigVersionsRsp {
            versions: vec![("meta".to_string(), 3), ("storage".to_string(), 5)],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_set_chain_table_roundtrip() {
        let req = SetChainTableReq {
            cluster_id: "c1".to_string(),
            chain_table_id: 1,
            chains: vec![10, 20, 30],
            desc: "default".to_string(),
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = SetChainTableRsp {
            chain_table_version: 2,
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_enable_disable_node_roundtrip() {
        let enable_req = EnableNodeReq {
            cluster_id: "c1".to_string(),
            node_id: 5,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&enable_req), enable_req);
        assert_eq!(roundtrip(&EnableNodeRsp {}), EnableNodeRsp {});

        let disable_req = DisableNodeReq {
            cluster_id: "c1".to_string(),
            node_id: 5,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&disable_req), disable_req);
        assert_eq!(roundtrip(&DisableNodeRsp {}), DisableNodeRsp {});
    }

    #[test]
    fn test_extend_client_session_roundtrip() {
        let req = ExtendClientSessionReq {
            cluster_id: "c1".to_string(),
            client_id: "client-001".to_string(),
            client_session_version: 1,
            config_version: 3,
            data: ClientSessionData {
                universal_id: "uid".to_string(),
                description: "test".to_string(),
                service_groups: vec![],
                release_version: ReleaseVersion::default(),
            },
            config_status: ConfigStatus::Normal as u8,
            node_type: NodeType::Client as u8,
            user: UserInfo::default(),
            client_start: 1000,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = ExtendClientSessionRsp {
            config: None,
            tags: vec![TagPair {
                key: "k".to_string(),
                value: "v".to_string(),
            }],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_list_client_sessions_roundtrip() {
        let req = ListClientSessionsReq {
            cluster_id: "c1".to_string(),
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = ListClientSessionsRsp {
            bootstrapping: false,
            sessions: vec![],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_get_client_session_roundtrip() {
        let req = GetClientSessionReq {
            cluster_id: "c1".to_string(),
            client_id: "client-001".to_string(),
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = GetClientSessionRsp {
            bootstrapping: false,
            session: None,
            referenced_tags: vec![],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_set_node_tags_roundtrip() {
        let req = SetNodeTagsReq {
            cluster_id: "c1".to_string(),
            node_id: 5,
            tags: vec![TagPair {
                key: "zone".to_string(),
                value: "a".to_string(),
            }],
            mode: SetTagMode::Replace as u8,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_set_chains_roundtrip() {
        let req = SetChainsReq {
            cluster_id: "c1".to_string(),
            chains: vec![ChainSetting {
                chain_id: 1,
                targets: vec![ChainTargetSetting { target_id: 100 }],
                set_preferred_target_order: false,
            }],
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = SetChainsRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_set_universal_tags_roundtrip() {
        let req = SetUniversalTagsReq {
            cluster_id: "c1".to_string(),
            universal_id: "uid".to_string(),
            tags: vec![],
            mode: SetTagMode::Upsert as u8,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = SetUniversalTagsRsp { tags: vec![] };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_get_universal_tags_roundtrip() {
        let req = GetUniversalTagsReq {
            cluster_id: "c1".to_string(),
            universal_id: "uid".to_string(),
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = GetUniversalTagsRsp { tags: vec![] };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_rotate_last_srv_roundtrip() {
        let req = RotateLastSrvReq {
            cluster_id: "c1".to_string(),
            chain_id: 5,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = RotateLastSrvRsp {
            chain: ChainInfo::default(),
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_list_orphan_targets_roundtrip() {
        let req = ListOrphanTargetsReq {
            cluster_id: "c1".to_string(),
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = ListOrphanTargetsRsp {
            targets: vec![TargetInfo::default()],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_set_preferred_target_order_roundtrip() {
        let req = SetPreferredTargetOrderReq {
            cluster_id: "c1".to_string(),
            chain_id: 3,
            preferred_target_order: vec![200, 100],
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_rotate_as_preferred_order_roundtrip() {
        let req = RotateAsPreferredOrderReq {
            cluster_id: "c1".to_string(),
            chain_id: 3,
            user: UserInfo::default(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_update_chain_roundtrip() {
        let req = UpdateChainReq {
            cluster_id: "c1".to_string(),
            user: UserInfo::default(),
            chain_id: 5,
            target_id: 100,
            mode: UpdateChainMode::Add as u8,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = UpdateChainRsp {
            chain: ChainInfo {
                chain_id: 5,
                chain_version: 2,
                targets: vec![ChainTargetInfo {
                    target_id: 100,
                    public_state: PublicTargetState::Serving as u8,
                }],
                preferred_target_order: vec![],
            },
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_update_chain_mode_roundtrip() {
        assert_eq!(roundtrip(&UpdateChainMode::Add), UpdateChainMode::Add);
        assert_eq!(
            roundtrip(&UpdateChainMode::Remove),
            UpdateChainMode::Remove
        );
    }

    #[test]
    fn test_update_chain_mode_invalid() {
        let mut buf = Vec::new();
        99u8.wire_serialize(&mut buf).unwrap();
        let mut offset = 0;
        assert!(UpdateChainMode::wire_deserialize(&buf, &mut offset).is_err());
    }
}
