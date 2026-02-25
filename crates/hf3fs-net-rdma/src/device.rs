//! RDMA device abstraction.
//!
//! Corresponds to the C++ `IBDevice` and `IBManager` classes from
//! `src/common/net/ib/IBDevice.h`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Link layer type for an IB port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LinkLayer {
    /// InfiniBand.
    Infiniband,
    /// RoCE (RDMA over Converged Ethernet).
    Ethernet,
    /// Unknown or unspecified.
    Unspecified,
}

impl std::fmt::Display for LinkLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LinkLayer::Infiniband => write!(f, "INFINIBAND"),
            LinkLayer::Ethernet => write!(f, "ETHERNET"),
            LinkLayer::Unspecified => write!(f, "UNSPECIFIED"),
        }
    }
}

/// Port state for an IB port.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PortState {
    Nop,
    Down,
    Init,
    Armed,
    Active,
    ActiveDefer,
    Unknown,
}

impl std::fmt::Display for PortState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PortState::Nop => write!(f, "NOP"),
            PortState::Down => write!(f, "DOWN"),
            PortState::Init => write!(f, "INIT"),
            PortState::Armed => write!(f, "ARMED"),
            PortState::Active => write!(f, "ACTIVE"),
            PortState::ActiveDefer => write!(f, "ACTIVE_DEFER"),
            PortState::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// Information about an IB port.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortInfo {
    /// Port number.
    pub port_num: u8,
    /// Link layer type.
    pub link_layer: LinkLayer,
    /// Port state.
    pub state: PortState,
    /// Local ID (LID) for InfiniBand.
    pub lid: u16,
    /// Network zones this port belongs to.
    pub zones: Vec<String>,
}

impl PortInfo {
    /// Whether this port uses RoCE.
    pub fn is_roce(&self) -> bool {
        self.link_layer == LinkLayer::Ethernet
    }

    /// Whether this port uses InfiniBand.
    pub fn is_infiniband(&self) -> bool {
        self.link_layer == LinkLayer::Infiniband
    }

    /// Whether this port is in the active state.
    pub fn is_active(&self) -> bool {
        self.state == PortState::Active || self.state == PortState::ActiveDefer
    }
}

/// Represents an RDMA-capable device.
///
/// Corresponds to the C++ `IBDevice`. Without the `rdma` feature, this is a
/// stub that holds device metadata without actual hardware interaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdmaDevice {
    /// Device ID (index in the device list).
    pub id: u8,
    /// Device name (e.g., "mlx5_0").
    pub name: String,
    /// Ports on this device.
    pub ports: HashMap<u8, PortInfo>,
}

impl RdmaDevice {
    /// Create a new device description.
    pub fn new(id: u8, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            ports: HashMap::new(),
        }
    }

    /// Add a port to this device.
    pub fn add_port(&mut self, port: PortInfo) {
        self.ports.insert(port.port_num, port);
    }

    /// Get information about a specific port.
    pub fn port(&self, num: u8) -> Option<&PortInfo> {
        self.ports.get(&num)
    }

    /// Return all active ports.
    pub fn active_ports(&self) -> Vec<&PortInfo> {
        self.ports.values().filter(|p| p.is_active()).collect()
    }

    /// Discover all available RDMA devices on this system.
    ///
    /// Without the `rdma` feature, this always returns an empty list.
    /// With `rdma`, it would call `ibv_get_device_list` and enumerate
    /// all devices and their ports.
    pub fn discover_all() -> Vec<RdmaDevice> {
        #[cfg(feature = "rdma")]
        {
            // Would enumerate via ibv_get_device_list.
            tracing::warn!("RDMA device discovery not yet fully implemented");
            Vec::new()
        }
        #[cfg(not(feature = "rdma"))]
        {
            tracing::debug!("RDMA feature not enabled; no devices to discover");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_device_creation() {
        let mut dev = RdmaDevice::new(0, "mlx5_0");
        dev.add_port(PortInfo {
            port_num: 1,
            link_layer: LinkLayer::Infiniband,
            state: PortState::Active,
            lid: 42,
            zones: vec!["zone1".to_string()],
        });
        dev.add_port(PortInfo {
            port_num: 2,
            link_layer: LinkLayer::Ethernet,
            state: PortState::Down,
            lid: 0,
            zones: vec![],
        });

        assert_eq!(dev.name, "mlx5_0");
        assert_eq!(dev.ports.len(), 2);

        let p1 = dev.port(1).unwrap();
        assert!(p1.is_infiniband());
        assert!(p1.is_active());

        let p2 = dev.port(2).unwrap();
        assert!(p2.is_roce());
        assert!(!p2.is_active());

        let active = dev.active_ports();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].port_num, 1);
    }

    #[test]
    fn test_discover_no_rdma() {
        let devices = RdmaDevice::discover_all();
        // Without rdma feature, should be empty.
        assert!(devices.is_empty());
    }

    #[test]
    fn test_link_layer_display() {
        assert_eq!(LinkLayer::Infiniband.to_string(), "INFINIBAND");
        assert_eq!(LinkLayer::Ethernet.to_string(), "ETHERNET");
        assert_eq!(LinkLayer::Unspecified.to_string(), "UNSPECIFIED");
    }

    #[test]
    fn test_port_state_display() {
        assert_eq!(PortState::Active.to_string(), "ACTIVE");
        assert_eq!(PortState::Down.to_string(), "DOWN");
    }
}
