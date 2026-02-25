//! Cluster state tracked by the management daemon.

use dashmap::DashMap;
use hf3fs_proto::mgmtd::NodeType;
use hf3fs_types::UtcTime;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Information about a single registered node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique identifier for this node.
    pub node_id: u32,
    /// The type of node (Meta, Storage, Client, Mgmtd).
    pub node_type: NodeType,
    /// Timestamp of the most recent heartbeat from this node.
    pub last_heartbeat: UtcTime,
    /// Network address the node is reachable at.
    pub address: String,
}

/// Shared cluster state managed by the mgmtd service.
///
/// All fields are safe for concurrent access. The `DashMap` allows lock-free
/// reads and fine-grained locking for writes on the node registry.
#[derive(Debug)]
pub struct MgmtdState {
    /// Map from `node_id` to its metadata.
    pub nodes: DashMap<u32, NodeInfo>,
    /// Identifier of this cluster.
    pub cluster_id: String,
}

impl MgmtdState {
    /// Create a new `MgmtdState` with the given cluster identifier.
    pub fn new(cluster_id: impl Into<String>) -> Self {
        Self {
            nodes: DashMap::new(),
            cluster_id: cluster_id.into(),
        }
    }

    /// Wrap the state in an `Arc` for shared ownership.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    /// Return the number of currently registered nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Register a new node or update an existing one.
    pub fn upsert_node(&self, node_id: u32, node_type: NodeType, address: String) {
        let now = UtcTime::now();
        self.nodes.insert(
            node_id,
            NodeInfo {
                node_id,
                node_type,
                last_heartbeat: now,
                address,
            },
        );
    }

    /// Update the last-heartbeat timestamp for an existing node.
    ///
    /// Returns `true` if the node was found and updated, `false` otherwise.
    pub fn touch_heartbeat(&self, node_id: u32) -> bool {
        if let Some(mut entry) = self.nodes.get_mut(&node_id) {
            entry.last_heartbeat = UtcTime::now();
            true
        } else {
            false
        }
    }

    /// Look up a node by its id.
    pub fn get_node(&self, node_id: u32) -> Option<NodeInfo> {
        self.nodes.get(&node_id).map(|r| r.clone())
    }

    /// Remove a node from the registry.
    pub fn remove_node(&self, node_id: u32) -> Option<NodeInfo> {
        self.nodes.remove(&node_id).map(|(_, v)| v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_state() {
        let state = MgmtdState::new("test-cluster");
        assert_eq!(state.cluster_id, "test-cluster");
        assert_eq!(state.node_count(), 0);
    }

    #[test]
    fn test_upsert_and_get_node() {
        let state = MgmtdState::new("c1");
        state.upsert_node(1, NodeType::Storage, "10.0.0.1:9000".into());

        let info = state.get_node(1).expect("node should exist");
        assert_eq!(info.node_id, 1);
        assert_eq!(info.node_type, NodeType::Storage);
        assert_eq!(info.address, "10.0.0.1:9000");
    }

    #[test]
    fn test_upsert_overwrites() {
        let state = MgmtdState::new("c1");
        state.upsert_node(1, NodeType::Storage, "10.0.0.1:9000".into());
        state.upsert_node(1, NodeType::Storage, "10.0.0.2:9000".into());

        let info = state.get_node(1).unwrap();
        assert_eq!(info.address, "10.0.0.2:9000");
        assert_eq!(state.node_count(), 1);
    }

    #[test]
    fn test_touch_heartbeat() {
        let state = MgmtdState::new("c1");
        assert!(!state.touch_heartbeat(999), "missing node returns false");

        state.upsert_node(1, NodeType::Meta, "10.0.0.1:8000".into());
        let before = state.get_node(1).unwrap().last_heartbeat;
        // Touch should succeed.
        assert!(state.touch_heartbeat(1));
        let after = state.get_node(1).unwrap().last_heartbeat;
        assert!(after >= before);
    }

    #[test]
    fn test_remove_node() {
        let state = MgmtdState::new("c1");
        state.upsert_node(1, NodeType::Client, "10.0.0.3:7000".into());
        assert_eq!(state.node_count(), 1);

        let removed = state.remove_node(1);
        assert!(removed.is_some());
        assert_eq!(state.node_count(), 0);
        assert!(state.get_node(1).is_none());
    }

    #[test]
    fn test_into_arc() {
        let state = MgmtdState::new("c1");
        let arc = state.into_arc();
        assert_eq!(arc.cluster_id, "c1");
    }
}
