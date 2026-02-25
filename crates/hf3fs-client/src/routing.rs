//! Routing information for locating services in the cluster.
//!
//! Mirrors the C++ `client::RoutingInfo` that wraps `flat::RoutingInfo` and
//! provides lookups for chains, targets, and nodes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use hf3fs_types::{Address, ChainId, NodeId, TargetId};
use parking_lot::RwLock;

/// Information about a single node in the cluster.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Unique identifier for the node.
    pub node_id: NodeId,
    /// Network address for this node.
    pub address: Address,
    /// Human-readable hostname (or pod name).
    pub hostname: String,
    /// Whether the node is currently considered healthy.
    pub is_healthy: bool,
}

/// Information about a chain (replication group).
#[derive(Debug, Clone)]
pub struct ChainInfo {
    /// Chain identifier.
    pub chain_id: ChainId,
    /// Ordered list of target IDs in the chain.
    pub targets: Vec<TargetId>,
}

/// Information about a storage target.
#[derive(Debug, Clone)]
pub struct TargetInfo {
    /// Target identifier.
    pub target_id: TargetId,
    /// Node hosting this target.
    pub node_id: NodeId,
}

/// Snapshot of routing information, refreshed periodically from mgmtd.
///
/// Corresponds to the C++ `client::RoutingInfo` class. All lookup methods
/// operate on an immutable snapshot; callers obtain a new `Arc<RoutingInfo>`
/// when routing is refreshed.
#[derive(Debug, Clone)]
pub struct RoutingInfo {
    /// Map of node ID to node info.
    pub nodes: HashMap<NodeId, NodeInfo>,
    /// Map of chain ID to chain info.
    pub chains: HashMap<ChainId, ChainInfo>,
    /// Map of target ID to target info.
    pub targets: HashMap<TargetId, TargetInfo>,
    /// When this snapshot was last refreshed.
    pub last_refresh: Instant,
}

impl RoutingInfo {
    /// Create an empty routing info (no nodes / chains / targets known).
    pub fn empty() -> Self {
        Self {
            nodes: HashMap::new(),
            chains: HashMap::new(),
            targets: HashMap::new(),
            last_refresh: Instant::now(),
        }
    }

    /// Look up a node by ID.
    pub fn get_node(&self, id: NodeId) -> Option<&NodeInfo> {
        self.nodes.get(&id)
    }

    /// Look up a chain by ID.
    pub fn get_chain(&self, id: ChainId) -> Option<&ChainInfo> {
        self.chains.get(&id)
    }

    /// Look up a target by ID.
    pub fn get_target(&self, id: TargetId) -> Option<&TargetInfo> {
        self.targets.get(&id)
    }

    /// Return all nodes matching a predicate.
    pub fn nodes_where(&self, pred: impl Fn(&NodeInfo) -> bool) -> Vec<&NodeInfo> {
        self.nodes.values().filter(|n| pred(n)).collect()
    }

    /// Return all healthy node IDs.
    pub fn healthy_node_ids(&self) -> Vec<NodeId> {
        self.nodes
            .values()
            .filter(|n| n.is_healthy)
            .map(|n| n.node_id)
            .collect()
    }
}

/// Thread-safe handle to the current routing info.
///
/// Writers (the mgmtd client) call `update` with a new snapshot.
/// Readers clone the `Arc` for lock-free access to an immutable snapshot.
#[derive(Debug, Clone)]
pub struct RoutingInfoHandle {
    inner: Arc<RwLock<Arc<RoutingInfo>>>,
}

impl RoutingInfoHandle {
    /// Create a handle with empty routing info.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Arc::new(RoutingInfo::empty()))),
        }
    }

    /// Get a snapshot of the current routing info.
    pub fn get(&self) -> Arc<RoutingInfo> {
        Arc::clone(&*self.inner.read())
    }

    /// Replace the current routing info with a new snapshot.
    pub fn update(&self, info: RoutingInfo) {
        *self.inner.write() = Arc::new(info);
    }
}

impl Default for RoutingInfoHandle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_types::AddressType;

    fn make_node(id: u32) -> NodeInfo {
        NodeInfo {
            node_id: NodeId(id),
            address: Address::from_octets(10, 0, 0, id as u8, 8080, AddressType::TCP),
            hostname: format!("node-{}", id),
            is_healthy: true,
        }
    }

    #[test]
    fn test_routing_info_empty() {
        let ri = RoutingInfo::empty();
        assert!(ri.nodes.is_empty());
        assert!(ri.chains.is_empty());
        assert!(ri.targets.is_empty());
    }

    #[test]
    fn test_routing_info_lookup() {
        let mut ri = RoutingInfo::empty();
        ri.nodes.insert(NodeId(1), make_node(1));
        ri.nodes.insert(NodeId(2), make_node(2));

        assert!(ri.get_node(NodeId(1)).is_some());
        assert!(ri.get_node(NodeId(99)).is_none());
    }

    #[test]
    fn test_routing_info_healthy_nodes() {
        let mut ri = RoutingInfo::empty();
        ri.nodes.insert(NodeId(1), make_node(1));
        let mut unhealthy = make_node(2);
        unhealthy.is_healthy = false;
        ri.nodes.insert(NodeId(2), unhealthy);
        ri.nodes.insert(NodeId(3), make_node(3));

        let healthy = ri.healthy_node_ids();
        assert_eq!(healthy.len(), 2);
        assert!(healthy.contains(&NodeId(1)));
        assert!(healthy.contains(&NodeId(3)));
    }

    #[test]
    fn test_routing_info_handle() {
        let handle = RoutingInfoHandle::new();
        let snap1 = handle.get();
        assert!(snap1.nodes.is_empty());

        let mut ri = RoutingInfo::empty();
        ri.nodes.insert(NodeId(5), make_node(5));
        handle.update(ri);

        let snap2 = handle.get();
        assert_eq!(snap2.nodes.len(), 1);
        // snap1 is still valid (old snapshot).
        assert!(snap1.nodes.is_empty());
    }

    #[test]
    fn test_nodes_where() {
        let mut ri = RoutingInfo::empty();
        for id in 1..=5 {
            let mut node = make_node(id);
            node.is_healthy = id % 2 == 0;
            ri.nodes.insert(NodeId(id), node);
        }

        let even = ri.nodes_where(|n| n.is_healthy);
        assert_eq!(even.len(), 2);
    }

    #[test]
    fn test_chain_and_target_lookup() {
        let mut ri = RoutingInfo::empty();
        ri.chains.insert(
            ChainId(100),
            ChainInfo {
                chain_id: ChainId(100),
                targets: vec![TargetId(1), TargetId(2)],
            },
        );
        ri.targets.insert(
            TargetId(1),
            TargetInfo {
                target_id: TargetId(1),
                node_id: NodeId(10),
            },
        );

        assert_eq!(ri.get_chain(ChainId(100)).unwrap().targets.len(), 2);
        assert_eq!(ri.get_target(TargetId(1)).unwrap().node_id, NodeId(10));
        assert!(ri.get_chain(ChainId(999)).is_none());
        assert!(ri.get_target(TargetId(999)).is_none());
    }
}
