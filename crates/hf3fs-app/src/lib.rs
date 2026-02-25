use async_trait::async_trait;
use hf3fs_types::{Address, NodeId, Result};
use serde::{Deserialize, Serialize};

/// Application metadata describing a running service instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppInfo {
    pub node_id: NodeId,
    pub cluster_id: String,
    pub hostname: String,
    pub pid: u32,
    pub addresses: Vec<Address>,
}

/// Core trait for 3FS service applications.
#[async_trait]
pub trait Application: Send + Sync + 'static {
    type Config: hf3fs_config::Config + Clone;

    fn name(&self) -> &str;
    async fn init(&mut self, config: &Self::Config) -> Result<()>;
    async fn start(&mut self) -> Result<()>;
    async fn stop(&mut self) -> Result<()>;
}

/// Wait for a shutdown signal (CTRL+C or SIGTERM).
pub async fn wait_for_shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");
    tokio::select! {
        _ = ctrl_c => { tracing::info!("Received CTRL+C"); }
        _ = sigterm.recv() => { tracing::info!("Received SIGTERM"); }
    }
}

/// Run an application through its full lifecycle: init, start, wait for shutdown, stop.
pub async fn run_application<A: Application>(mut app: A, config: A::Config) -> Result<()> {
    app.init(&config).await?;
    app.start().await?;
    wait_for_shutdown_signal().await;
    app.stop().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_types::AddressType;

    #[test]
    fn test_app_info_creation() {
        let info = AppInfo {
            node_id: NodeId(1),
            cluster_id: "test-cluster".into(),
            hostname: "host-01".into(),
            pid: 12345,
            addresses: vec![Address::from_octets(127, 0, 0, 1, 8080, AddressType::TCP)],
        };
        assert_eq!(*info.node_id, 1u32);
        assert_eq!(info.cluster_id, "test-cluster");
        assert_eq!(info.hostname, "host-01");
        assert_eq!(info.pid, 12345);
        assert_eq!(info.addresses.len(), 1);
    }

    #[test]
    fn test_app_info_serde_roundtrip() {
        let info = AppInfo {
            node_id: NodeId(42),
            cluster_id: "prod".into(),
            hostname: "node-42.internal".into(),
            pid: 9999,
            addresses: vec![
                Address::from_octets(10, 0, 0, 1, 3000, AddressType::TCP),
                Address::from_octets(10, 0, 0, 1, 3001, AddressType::RDMA),
            ],
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: AppInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.node_id, info.node_id);
        assert_eq!(parsed.cluster_id, info.cluster_id);
        assert_eq!(parsed.hostname, info.hostname);
        assert_eq!(parsed.pid, info.pid);
        assert_eq!(parsed.addresses.len(), 2);
    }

    #[test]
    fn test_app_info_empty_addresses() {
        let info = AppInfo {
            node_id: NodeId(0),
            cluster_id: String::new(),
            hostname: String::new(),
            pid: 0,
            addresses: vec![],
        };
        assert!(info.addresses.is_empty());
    }

    #[test]
    fn test_app_info_clone() {
        let info = AppInfo {
            node_id: NodeId(5),
            cluster_id: "cluster-a".into(),
            hostname: "host".into(),
            pid: 1,
            addresses: vec![],
        };
        let cloned = info.clone();
        assert_eq!(cloned.node_id, info.node_id);
        assert_eq!(cloned.cluster_id, info.cluster_id);
    }
}
