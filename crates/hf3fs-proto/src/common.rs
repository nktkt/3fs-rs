//! Common protocol types shared across all services.
//!
//! Based on:
//! - 3FS/src/fbs/core/user/User.h (UserInfo, UserAttr)
//! - 3FS/src/fbs/mgmtd/MgmtdTypes.h (NodeType, PublicTargetState, etc.)
//! - 3FS/src/fbs/mgmtd/NodeInfo.h (NodeInfo)
//! - 3FS/src/fbs/mgmtd/RoutingInfo.h (RoutingInfo)
//! - 3FS/src/common/app/AppInfo.h (AppInfo, TagPair, ServiceGroupInfo, ReleaseVersion)

use hf3fs_serde::{WireDeserialize, WireSerialize};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Protocol version information.
// ---------------------------------------------------------------------------

/// Protocol version information.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
    pub hash: u32,
}

// ---------------------------------------------------------------------------
// Timestamp tracking for request/response lifecycle.
// ---------------------------------------------------------------------------

/// Timestamp tracking for request/response lifecycle.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
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

// ---------------------------------------------------------------------------
// Enums mirroring C++ flat:: enums from MgmtdTypes.h
// ---------------------------------------------------------------------------

/// Node type in the cluster.
///
/// Mirrors `flat::NodeType` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeType {
    Mgmtd = 0,
    Meta = 1,
    Storage = 2,
    Client = 3,
    Fuse = 4,
}

impl Default for NodeType {
    fn default() -> Self {
        Self::Mgmtd
    }
}

impl TryFrom<u8> for NodeType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Mgmtd),
            1 => Ok(Self::Meta),
            2 => Ok(Self::Storage),
            3 => Ok(Self::Client),
            4 => Ok(Self::Fuse),
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

/// Public state of a storage target, as tracked by mgmtd.
///
/// Mirrors `flat::PublicTargetState` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum PublicTargetState {
    Invalid = 0,
    Serving = 1,
    LastSrv = 2,
    Syncing = 4,
    Waiting = 8,
    Offline = 16,
}

impl Default for PublicTargetState {
    fn default() -> Self {
        Self::Invalid
    }
}

impl TryFrom<u8> for PublicTargetState {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Invalid),
            1 => Ok(Self::Serving),
            2 => Ok(Self::LastSrv),
            4 => Ok(Self::Syncing),
            8 => Ok(Self::Waiting),
            16 => Ok(Self::Offline),
            _ => Err(()),
        }
    }
}

impl WireSerialize for PublicTargetState {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for PublicTargetState {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "PublicTargetState",
            value: v as u64,
        })
    }
}

/// Local state of a storage target, as reported by the storage node.
///
/// Mirrors `flat::LocalTargetState` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum LocalTargetState {
    Invalid = 0,
    UpToDate = 1,
    Online = 2,
    Offline = 4,
}

impl Default for LocalTargetState {
    fn default() -> Self {
        Self::Invalid
    }
}

impl TryFrom<u8> for LocalTargetState {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Invalid),
            1 => Ok(Self::UpToDate),
            2 => Ok(Self::Online),
            4 => Ok(Self::Offline),
            _ => Err(()),
        }
    }
}

impl WireSerialize for LocalTargetState {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for LocalTargetState {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "LocalTargetState",
            value: v as u64,
        })
    }
}

/// Node status tracked by mgmtd.
///
/// Mirrors `flat::NodeStatus` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum NodeStatus {
    PrimaryMgmtd = 0,
    HeartbeatConnected = 1,
    HeartbeatConnecting = 2,
    HeartbeatFailed = 3,
    Disabled = 4,
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self::HeartbeatConnecting
    }
}

impl TryFrom<u8> for NodeStatus {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::PrimaryMgmtd),
            1 => Ok(Self::HeartbeatConnected),
            2 => Ok(Self::HeartbeatConnecting),
            3 => Ok(Self::HeartbeatFailed),
            4 => Ok(Self::Disabled),
            _ => Err(()),
        }
    }
}

impl WireSerialize for NodeStatus {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for NodeStatus {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "NodeStatus",
            value: v as u64,
        })
    }
}

/// Tag mode for SetNodeTags / SetUniversalTags.
///
/// Mirrors `flat::SetTagMode` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum SetTagMode {
    Replace = 0,
    Upsert = 1,
    Remove = 2,
}

impl Default for SetTagMode {
    fn default() -> Self {
        Self::Replace
    }
}

impl TryFrom<u8> for SetTagMode {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Replace),
            1 => Ok(Self::Upsert),
            2 => Ok(Self::Remove),
            _ => Err(()),
        }
    }
}

impl WireSerialize for SetTagMode {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for SetTagMode {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "SetTagMode",
            value: v as u64,
        })
    }
}

/// Config status reported in heartbeat / extend session.
///
/// Mirrors `ConfigStatus` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ConfigStatus {
    Normal = 0,
    Updating = 1,
    Failed = 2,
}

impl Default for ConfigStatus {
    fn default() -> Self {
        Self::Normal
    }
}

impl TryFrom<u8> for ConfigStatus {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Normal),
            1 => Ok(Self::Updating),
            2 => Ok(Self::Failed),
            _ => Err(()),
        }
    }
}

impl WireSerialize for ConfigStatus {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for ConfigStatus {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "ConfigStatus",
            value: v as u64,
        })
    }
}

// ---------------------------------------------------------------------------
// AppInfo-related types from 3FS/src/common/app/AppInfo.h
// ---------------------------------------------------------------------------

/// A key-value tag pair.
///
/// Mirrors `flat::TagPair` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct TagPair {
    pub key: String,
    pub value: String,
}

/// Release version tracking.
///
/// Mirrors `flat::ReleaseVersion` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ReleaseVersion {
    pub major_version: u8,
    pub minor_version: u8,
    pub patch_version: u8,
    pub commit_hash_short: u32,
    pub build_time_in_seconds: u64,
    pub build_pipeline_id: u32,
}

/// Service group: a set of service names and their network endpoints.
///
/// Mirrors `flat::ServiceGroupInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ServiceGroupInfo {
    pub services: Vec<String>,
    pub endpoints: Vec<String>,
}

/// Full application info broadcast via heartbeat.
///
/// Mirrors `flat::FbsAppInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct AppInfo {
    pub node_id: u32,
    pub node_type: u8,
    pub hostname: String,
    pub pid: u32,
    pub service_groups: Vec<ServiceGroupInfo>,
    pub release_version: ReleaseVersion,
    pub podname: String,
}

// ---------------------------------------------------------------------------
// User types from 3FS/src/fbs/core/user/User.h
// ---------------------------------------------------------------------------

/// User information for authentication / authorization.
///
/// Mirrors `flat::UserInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct UserInfo {
    pub uid: u32,
    pub gid: u32,
    pub groups: Vec<u32>,
    pub token: String,
}

impl UserInfo {
    pub fn is_root(&self) -> bool {
        self.uid == 0
    }

    pub fn in_group(&self, gid: u32) -> bool {
        self.gid == gid || self.groups.contains(&gid)
    }
}

// ---------------------------------------------------------------------------
// Mgmtd cluster types
// ---------------------------------------------------------------------------

/// Config information managed by mgmtd.
///
/// Mirrors `flat::ConfigInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ConfigInfo {
    pub config_version: u64,
    pub content: String,
    pub desc: String,
}

/// Information about a single chain target within a chain.
///
/// Mirrors `flat::ChainTargetInfo` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct ChainTargetInfo {
    pub target_id: u64,
    pub public_state: u8,
}

/// Chain setting used in SetChains requests.
///
/// Mirrors `flat::ChainSetting` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ChainSetting {
    pub chain_id: u32,
    pub targets: Vec<ChainTargetSetting>,
    pub set_preferred_target_order: bool,
}

/// Target setting within a chain setting.
///
/// Mirrors `flat::ChainTargetSetting` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct ChainTargetSetting {
    pub target_id: u64,
}

/// Information about a replication chain.
///
/// Mirrors `flat::ChainInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ChainInfo {
    pub chain_id: u32,
    pub chain_version: u32,
    pub targets: Vec<ChainTargetInfo>,
    pub preferred_target_order: Vec<u64>,
}

/// Chain table - maps a table ID to a set of chain IDs.
///
/// Mirrors `flat::ChainTable` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ChainTable {
    pub chain_table_id: u32,
    pub chain_table_version: u32,
    pub chains: Vec<u32>,
    pub desc: String,
}

/// Information about a storage target.
///
/// Mirrors `flat::TargetInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct TargetInfo {
    pub target_id: u64,
    pub public_state: u8,
    pub local_state: u8,
    pub chain_id: u32,
    pub node_id: Option<u32>,
    pub disk_index: Option<u32>,
    pub used_size: u64,
}

/// Local target info reported in heartbeat.
///
/// Mirrors `flat::LocalTargetInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct LocalTargetInfo {
    pub target_id: u64,
    pub local_state: u8,
    pub disk_index: Option<u32>,
    pub used_size: u64,
    pub chain_version: u32,
    pub low_space: bool,
}

/// Full node information tracked by mgmtd.
///
/// Mirrors `flat::NodeInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct NodeInfo {
    pub app: AppInfo,
    pub node_type: u8,
    pub status: u8,
    pub last_heartbeat_ts: i64,
    pub tags: Vec<TagPair>,
    pub config_version: u64,
    pub config_status: u8,
}

/// Persistent node info stored in KV for recovery.
///
/// Mirrors `flat::PersistentNodeInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct PersistentNodeInfo {
    pub node_id: u32,
    pub node_type: u8,
    pub service_groups: Vec<ServiceGroupInfo>,
    pub tags: Vec<TagPair>,
    pub hostname: String,
}

/// Client session data sent during session extend.
///
/// Mirrors `flat::ClientSessionData` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ClientSessionData {
    pub universal_id: String,
    pub description: String,
    pub service_groups: Vec<ServiceGroupInfo>,
    pub release_version: ReleaseVersion,
}

/// Client session tracked by mgmtd.
///
/// Mirrors `flat::ClientSession` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ClientSession {
    pub client_id: String,
    pub config_version: u64,
    pub start: i64,
    pub last_extend: i64,
    pub universal_id: String,
    pub description: String,
    pub service_groups: Vec<ServiceGroupInfo>,
    pub release_version: ReleaseVersion,
    pub config_status: u8,
    pub node_type: u8,
    pub client_start: i64,
}

/// Routing info distributed by mgmtd to all nodes.
///
/// Mirrors `flat::RoutingInfo` in C++.
/// Note: in Rust we use Vec-of-pairs for maps to keep wire serialization simple.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RoutingInfo {
    pub routing_info_version: u64,
    pub bootstrapping: bool,
    pub nodes: Vec<(u32, NodeInfo)>,
    pub chain_tables: Vec<(u32, Vec<(u32, ChainTable)>)>,
    pub chains: Vec<(u32, ChainInfo)>,
    pub targets: Vec<(u64, TargetInfo)>,
}

/// Mgmtd lease information.
///
/// Mirrors `flat::MgmtdLeaseInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct MgmtdLeaseInfo {
    pub primary: PersistentNodeInfo,
    pub lease_start: i64,
    pub lease_end: i64,
    pub release_version: ReleaseVersion,
}

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

    #[test]
    fn test_node_type_roundtrip() {
        assert_eq!(roundtrip(&NodeType::Mgmtd), NodeType::Mgmtd);
        assert_eq!(roundtrip(&NodeType::Meta), NodeType::Meta);
        assert_eq!(roundtrip(&NodeType::Storage), NodeType::Storage);
        assert_eq!(roundtrip(&NodeType::Client), NodeType::Client);
        assert_eq!(roundtrip(&NodeType::Fuse), NodeType::Fuse);
    }

    #[test]
    fn test_node_type_invalid() {
        let mut buf = Vec::new();
        99u8.wire_serialize(&mut buf).unwrap();
        let mut offset = 0;
        let result = NodeType::wire_deserialize(&buf, &mut offset);
        assert!(result.is_err());
    }

    #[test]
    fn test_public_target_state_roundtrip() {
        assert_eq!(
            roundtrip(&PublicTargetState::Invalid),
            PublicTargetState::Invalid
        );
        assert_eq!(
            roundtrip(&PublicTargetState::Serving),
            PublicTargetState::Serving
        );
        assert_eq!(
            roundtrip(&PublicTargetState::Offline),
            PublicTargetState::Offline
        );
    }

    #[test]
    fn test_local_target_state_roundtrip() {
        assert_eq!(
            roundtrip(&LocalTargetState::Invalid),
            LocalTargetState::Invalid
        );
        assert_eq!(
            roundtrip(&LocalTargetState::UpToDate),
            LocalTargetState::UpToDate
        );
    }

    #[test]
    fn test_node_status_roundtrip() {
        assert_eq!(
            roundtrip(&NodeStatus::PrimaryMgmtd),
            NodeStatus::PrimaryMgmtd
        );
        assert_eq!(
            roundtrip(&NodeStatus::HeartbeatConnected),
            NodeStatus::HeartbeatConnected
        );
        assert_eq!(roundtrip(&NodeStatus::Disabled), NodeStatus::Disabled);
    }

    #[test]
    fn test_set_tag_mode_roundtrip() {
        assert_eq!(roundtrip(&SetTagMode::Replace), SetTagMode::Replace);
        assert_eq!(roundtrip(&SetTagMode::Upsert), SetTagMode::Upsert);
        assert_eq!(roundtrip(&SetTagMode::Remove), SetTagMode::Remove);
    }

    #[test]
    fn test_config_status_roundtrip() {
        assert_eq!(roundtrip(&ConfigStatus::Normal), ConfigStatus::Normal);
        assert_eq!(roundtrip(&ConfigStatus::Updating), ConfigStatus::Updating);
        assert_eq!(roundtrip(&ConfigStatus::Failed), ConfigStatus::Failed);
    }

    #[test]
    fn test_tag_pair_roundtrip() {
        let t = TagPair {
            key: "TrafficZone".to_string(),
            value: "zone-a".to_string(),
        };
        assert_eq!(roundtrip(&t), t);
    }

    #[test]
    fn test_release_version_roundtrip() {
        let rv = ReleaseVersion {
            major_version: 1,
            minor_version: 0,
            patch_version: 3,
            commit_hash_short: 0xABCD,
            build_time_in_seconds: 1700000000,
            build_pipeline_id: 42,
        };
        assert_eq!(roundtrip(&rv), rv);
    }

    #[test]
    fn test_service_group_info_roundtrip() {
        let sg = ServiceGroupInfo {
            services: vec!["meta".to_string(), "storage".to_string()],
            endpoints: vec!["192.168.1.1:9000".to_string()],
        };
        assert_eq!(roundtrip(&sg), sg);
    }

    #[test]
    fn test_app_info_roundtrip() {
        let ai = AppInfo {
            node_id: 42,
            node_type: 0,
            hostname: "node01".to_string(),
            pid: 1234,
            service_groups: vec![],
            release_version: ReleaseVersion::default(),
            podname: "pod01".to_string(),
        };
        assert_eq!(roundtrip(&ai), ai);
    }

    #[test]
    fn test_user_info_roundtrip() {
        let u = UserInfo {
            uid: 1000,
            gid: 100,
            groups: vec![100, 200, 300],
            token: "secret".to_string(),
        };
        assert_eq!(roundtrip(&u), u);
    }

    #[test]
    fn test_user_info_is_root() {
        let root = UserInfo {
            uid: 0,
            ..Default::default()
        };
        assert!(root.is_root());

        let normal = UserInfo {
            uid: 1000,
            ..Default::default()
        };
        assert!(!normal.is_root());
    }

    #[test]
    fn test_user_info_in_group() {
        let u = UserInfo {
            uid: 1000,
            gid: 100,
            groups: vec![200, 300],
            token: String::new(),
        };
        assert!(u.in_group(100)); // primary gid
        assert!(u.in_group(200)); // supplementary
        assert!(u.in_group(300));
        assert!(!u.in_group(400));
    }

    #[test]
    fn test_config_info_roundtrip() {
        let ci = ConfigInfo {
            config_version: 5,
            content: "key=value".to_string(),
            desc: "test config".to_string(),
        };
        assert_eq!(roundtrip(&ci), ci);
    }

    #[test]
    fn test_chain_target_info_roundtrip() {
        let cti = ChainTargetInfo {
            target_id: 100,
            public_state: PublicTargetState::Serving as u8,
        };
        assert_eq!(roundtrip(&cti), cti);
    }

    #[test]
    fn test_chain_info_roundtrip() {
        let ci = ChainInfo {
            chain_id: 1,
            chain_version: 10,
            targets: vec![
                ChainTargetInfo {
                    target_id: 100,
                    public_state: 1,
                },
                ChainTargetInfo {
                    target_id: 200,
                    public_state: 1,
                },
            ],
            preferred_target_order: vec![200, 100],
        };
        assert_eq!(roundtrip(&ci), ci);
    }

    #[test]
    fn test_chain_table_roundtrip() {
        let ct = ChainTable {
            chain_table_id: 1,
            chain_table_version: 2,
            chains: vec![10, 20, 30],
            desc: "default".to_string(),
        };
        assert_eq!(roundtrip(&ct), ct);
    }

    #[test]
    fn test_target_info_roundtrip() {
        let ti = TargetInfo {
            target_id: 42,
            public_state: PublicTargetState::Serving as u8,
            local_state: LocalTargetState::UpToDate as u8,
            chain_id: 10,
            node_id: Some(5),
            disk_index: Some(2),
            used_size: 1024 * 1024,
        };
        assert_eq!(roundtrip(&ti), ti);
    }

    #[test]
    fn test_local_target_info_roundtrip() {
        let lti = LocalTargetInfo {
            target_id: 100,
            local_state: LocalTargetState::UpToDate as u8,
            disk_index: Some(0),
            used_size: 2048,
            chain_version: 5,
            low_space: false,
        };
        assert_eq!(roundtrip(&lti), lti);
    }

    #[test]
    fn test_node_info_roundtrip() {
        let ni = NodeInfo {
            app: AppInfo {
                node_id: 1,
                node_type: NodeType::Storage as u8,
                hostname: "host".to_string(),
                pid: 999,
                service_groups: vec![],
                release_version: ReleaseVersion::default(),
                podname: String::new(),
            },
            node_type: NodeType::Storage as u8,
            status: NodeStatus::HeartbeatConnected as u8,
            last_heartbeat_ts: 1234567890,
            tags: vec![TagPair {
                key: "zone".to_string(),
                value: "a".to_string(),
            }],
            config_version: 3,
            config_status: ConfigStatus::Normal as u8,
        };
        assert_eq!(roundtrip(&ni), ni);
    }

    #[test]
    fn test_persistent_node_info_roundtrip() {
        let pni = PersistentNodeInfo {
            node_id: 1,
            node_type: NodeType::Meta as u8,
            service_groups: vec![],
            tags: vec![],
            hostname: "meta01".to_string(),
        };
        assert_eq!(roundtrip(&pni), pni);
    }

    #[test]
    fn test_client_session_data_roundtrip() {
        let csd = ClientSessionData {
            universal_id: "uid123".to_string(),
            description: "test client".to_string(),
            service_groups: vec![],
            release_version: ReleaseVersion::default(),
        };
        assert_eq!(roundtrip(&csd), csd);
    }

    #[test]
    fn test_client_session_roundtrip() {
        let cs = ClientSession {
            client_id: "client-001".to_string(),
            config_version: 5,
            start: 1000,
            last_extend: 2000,
            universal_id: "uid".to_string(),
            description: "desc".to_string(),
            service_groups: vec![],
            release_version: ReleaseVersion::default(),
            config_status: ConfigStatus::Normal as u8,
            node_type: NodeType::Client as u8,
            client_start: 500,
        };
        assert_eq!(roundtrip(&cs), cs);
    }

    #[test]
    fn test_routing_info_roundtrip() {
        let ri = RoutingInfo {
            routing_info_version: 42,
            bootstrapping: false,
            nodes: vec![],
            chain_tables: vec![],
            chains: vec![],
            targets: vec![],
        };
        assert_eq!(roundtrip(&ri), ri);
    }

    #[test]
    fn test_mgmtd_lease_info_roundtrip() {
        let mli = MgmtdLeaseInfo {
            primary: PersistentNodeInfo::default(),
            lease_start: 1000,
            lease_end: 2000,
            release_version: ReleaseVersion::default(),
        };
        assert_eq!(roundtrip(&mli), mli);
    }

    #[test]
    fn test_chain_setting_roundtrip() {
        let cs = ChainSetting {
            chain_id: 10,
            targets: vec![
                ChainTargetSetting { target_id: 100 },
                ChainTargetSetting { target_id: 200 },
            ],
            set_preferred_target_order: true,
        };
        assert_eq!(roundtrip(&cs), cs);
    }
}
