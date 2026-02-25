//! Cluster management commands.
//!
//! Mirrors the C++ cluster/node management admin commands:
//! - `list-nodes`        -> `3FS/src/client/cli/admin/ListNodes.cc`
//! - `register-node`     -> `3FS/src/client/cli/admin/RegisterNode.cc`
//! - `unregister-node`   -> `3FS/src/client/cli/admin/UnregisterNode.cc`
//! - `set-node-tags`     -> `3FS/src/client/cli/admin/SetNodeTags.cc`
//! - `init-cluster`      -> `3FS/src/client/cli/admin/InitCluster.cc`
//! - `list-clients`      -> `3FS/src/client/cli/admin/ListClients.cc`
//! - `refresh-routing-info` -> `3FS/src/client/cli/admin/RefreshRoutingInfo.cc`
//! - `shutdown-all-chains`  -> `3FS/src/client/cli/admin/ShutdownAllChains.cc`
//! - `remote-call`          -> `3FS/src/client/cli/admin/RemoteCall.cc`

use clap::Args;
use clap::Subcommand;

use crate::connection::{node_type_name, parse_node_type, AdminEnv};
use crate::output::{kv_row, table_with_header, OutputTable};

/// Cluster management subcommands.
#[derive(Debug, Subcommand)]
pub enum ClusterCommands {
    /// List all nodes in the cluster with their status.
    ///
    /// Mirrors `list-nodes` from C++.
    ListNodes(ListNodes),

    /// Register a new node in the cluster.
    ///
    /// Mirrors `register-node` from C++.
    RegisterNode(RegisterNode),

    /// Unregister (remove) a node from the cluster.
    ///
    /// Mirrors `unregister-node` from C++.
    UnregisterNode(UnregisterNode),

    /// Set tags on a node.
    ///
    /// Mirrors `set-node-tags` from C++.
    SetNodeTags(SetNodeTags),

    /// Initialize a new cluster.
    ///
    /// Mirrors `init-cluster` from C++.
    InitCluster(InitCluster),

    /// List connected clients.
    ///
    /// Mirrors `list-clients` from C++.
    ListClients(ListClients),

    /// Force-refresh routing information from mgmtd.
    ///
    /// Mirrors `refresh-routing-info` from C++.
    RefreshRoutingInfo(RefreshRoutingInfo),

    /// Show cluster status summary.
    Status(ClusterStatus),
}

/// Arguments for `cluster list-nodes`.
#[derive(Debug, Args)]
pub struct ListNodes {}

/// Arguments for `cluster register-node`.
#[derive(Debug, Args)]
pub struct RegisterNode {
    /// Node ID to register.
    pub node_id: u32,

    /// Node type (META, STORAGE, CLIENT, MGMTD).
    pub node_type: String,
}

/// Arguments for `cluster unregister-node`.
#[derive(Debug, Args)]
pub struct UnregisterNode {
    /// Node ID to unregister.
    pub node_id: u32,

    /// Node type for verification.
    pub node_type: String,
}

/// Arguments for `cluster set-node-tags`.
#[derive(Debug, Args)]
pub struct SetNodeTags {
    /// Node ID.
    #[arg(short = 'n', long)]
    pub node_id: u32,

    /// Tags as key=value pairs, comma-separated.
    #[arg(short = 't', long)]
    pub tags: String,
}

/// Arguments for `cluster init-cluster`.
#[derive(Debug, Args)]
pub struct InitCluster {
    /// Cluster ID to initialize.
    #[arg(long)]
    pub cluster_id: String,
}

/// Arguments for `cluster list-clients`.
#[derive(Debug, Args)]
pub struct ListClients {}

/// Arguments for `cluster refresh-routing-info`.
#[derive(Debug, Args)]
pub struct RefreshRoutingInfo {}

/// Arguments for `cluster status`.
#[derive(Debug, Args)]
pub struct ClusterStatus {}

impl ClusterCommands {
    /// Execute the cluster subcommand and return an output table.
    pub async fn execute(&self, env: &mut AdminEnv) -> anyhow::Result<OutputTable> {
        match self {
            Self::ListNodes(args) => execute_list_nodes(env, args).await,
            Self::RegisterNode(args) => execute_register_node(env, args).await,
            Self::UnregisterNode(args) => execute_unregister_node(env, args).await,
            Self::SetNodeTags(args) => execute_set_node_tags(env, args).await,
            Self::InitCluster(args) => execute_init_cluster(env, args).await,
            Self::ListClients(args) => execute_list_clients(env, args).await,
            Self::RefreshRoutingInfo(args) => execute_refresh_routing_info(env, args).await,
            Self::Status(args) => execute_status(env, args).await,
        }
    }
}

/// Execute `cluster list-nodes` -- list all nodes with status.
///
/// Mirrors the C++ `handleListNodes` in `ListNodes.cc`.
/// Output columns: Id, Type, Status, Hostname, Pid, Tags, LastHeartbeatTime,
/// ConfigVersion, ReleaseVersion.
async fn execute_list_nodes(_env: &AdminEnv, _args: &ListNodes) -> anyhow::Result<OutputTable> {
    let table = table_with_header(&[
        "Id",
        "Type",
        "Status",
        "Hostname",
        "Pid",
        "Tags",
        "LastHeartbeatTime",
        "ConfigVersion",
        "ReleaseVersion",
    ]);

    // TODO: connect to mgmtd, refresh routing info, and iterate over nodes.
    // The C++ implementation sorts by (type, status, nodeId).

    tracing::debug!("cluster list-nodes executed");

    Ok(table)
}

/// Execute `cluster register-node`.
///
/// Mirrors the C++ `handle` in `RegisterNode.cc`.
async fn execute_register_node(
    _env: &AdminEnv,
    args: &RegisterNode,
) -> anyhow::Result<OutputTable> {
    let node_type = parse_node_type(&args.node_type)
        .ok_or_else(|| anyhow::anyhow!("invalid node type: {}", args.node_type))?;

    let mut table = OutputTable::new();
    table.push(kv_row("NodeId", args.node_id));
    table.push(kv_row("NodeType", node_type_name(node_type)));

    // TODO: connect to mgmtd and call register_node.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(
        node_id = args.node_id,
        node_type = %args.node_type,
        "cluster register-node executed"
    );

    Ok(table)
}

/// Execute `cluster unregister-node`.
///
/// Mirrors the C++ `handle` in `UnregisterNode.cc`.
async fn execute_unregister_node(
    _env: &AdminEnv,
    args: &UnregisterNode,
) -> anyhow::Result<OutputTable> {
    let node_type = parse_node_type(&args.node_type)
        .ok_or_else(|| anyhow::anyhow!("invalid node type: {}", args.node_type))?;

    let mut table = OutputTable::new();
    table.push(kv_row("NodeId", args.node_id));
    table.push(kv_row("NodeType", node_type_name(node_type)));

    // TODO: connect to mgmtd, verify node type matches, then unregister.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(
        node_id = args.node_id,
        node_type = %args.node_type,
        "cluster unregister-node executed"
    );

    Ok(table)
}

/// Execute `cluster set-node-tags`.
///
/// Mirrors the C++ `SetNodeTags.cc`.
async fn execute_set_node_tags(
    _env: &AdminEnv,
    args: &SetNodeTags,
) -> anyhow::Result<OutputTable> {
    // Parse tags from comma-separated key=value pairs.
    let tags: Vec<(String, String)> = args
        .tags
        .split(',')
        .map(|t| {
            let mut parts = t.splitn(2, '=');
            let key = parts.next().unwrap_or("").trim().to_string();
            let value = parts.next().unwrap_or("").trim().to_string();
            (key, value)
        })
        .collect();

    let mut table = OutputTable::new();
    table.push(kv_row("NodeId", args.node_id));
    table.push(kv_row(
        "Tags",
        format!(
            "[{}]",
            tags.iter()
                .map(|(k, v)| if v.is_empty() {
                    k.clone()
                } else {
                    format!("{}={}", k, v)
                })
                .collect::<Vec<_>>()
                .join(",")
        ),
    ));

    // TODO: connect to mgmtd and set tags.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(node_id = args.node_id, tags = %args.tags, "cluster set-node-tags executed");

    Ok(table)
}

/// Execute `cluster init-cluster`.
///
/// Mirrors the C++ `InitCluster.cc`.
async fn execute_init_cluster(
    _env: &AdminEnv,
    args: &InitCluster,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("ClusterId", &args.cluster_id));

    // TODO: connect to mgmtd and initialize cluster.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(cluster_id = %args.cluster_id, "cluster init-cluster executed");

    Ok(table)
}

/// Execute `cluster list-clients`.
///
/// Mirrors the C++ `ListClients.cc`.
async fn execute_list_clients(
    _env: &AdminEnv,
    _args: &ListClients,
) -> anyhow::Result<OutputTable> {
    let table = table_with_header(&[
        "ClientId",
        "Hostname",
        "Pid",
        "LastHeartbeatTime",
        "MountPoint",
    ]);

    // TODO: connect to mgmtd and list client sessions.

    tracing::debug!("cluster list-clients executed");

    Ok(table)
}

/// Execute `cluster refresh-routing-info`.
///
/// Mirrors the C++ `RefreshRoutingInfo.cc`.
async fn execute_refresh_routing_info(
    _env: &AdminEnv,
    _args: &RefreshRoutingInfo,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();

    // TODO: connect to mgmtd and force-refresh routing info.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!("cluster refresh-routing-info executed");

    Ok(table)
}

/// Execute `cluster status` -- show cluster status summary.
async fn execute_status(_env: &AdminEnv, _args: &ClusterStatus) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();

    // TODO: aggregate node/chain/target status from routing info.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!("cluster status executed");

    Ok(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionOptions;

    fn test_env() -> AdminEnv {
        AdminEnv::new(ConnectionOptions {
            config: "test.toml".to_string(),
            cluster_id: None,
            mgmtd_addr: None,
            as_super: true,
            timeout_secs: 5,
        })
    }

    #[tokio::test]
    async fn test_list_nodes() {
        let env = test_env();
        let args = ListNodes {};
        let result = execute_list_nodes(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        // Should have header row.
        assert_eq!(table.len(), 1);
        assert_eq!(table[0][0], "Id");
    }

    #[tokio::test]
    async fn test_register_node_invalid_type() {
        let env = test_env();
        let args = RegisterNode {
            node_id: 1,
            node_type: "INVALID".to_string(),
        };
        let result = execute_register_node(&env, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_register_node_valid() {
        let env = test_env();
        let args = RegisterNode {
            node_id: 1,
            node_type: "META".to_string(),
        };
        let result = execute_register_node(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        let keys: Vec<&str> = table.iter().map(|r| r[0].as_str()).collect();
        assert!(keys.contains(&"NodeId"));
        assert!(keys.contains(&"NodeType"));
    }

    #[tokio::test]
    async fn test_unregister_node() {
        let env = test_env();
        let args = UnregisterNode {
            node_id: 1,
            node_type: "STORAGE".to_string(),
        };
        let result = execute_unregister_node(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_node_tags() {
        let env = test_env();
        let args = SetNodeTags {
            node_id: 1,
            tags: "zone=us-east-1,gpu".to_string(),
        };
        let result = execute_set_node_tags(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        // Check that tags are formatted correctly.
        let tags_row = table.iter().find(|r| r[0] == "Tags").unwrap();
        assert!(tags_row[1].contains("zone=us-east-1"));
        assert!(tags_row[1].contains("gpu"));
    }

    #[tokio::test]
    async fn test_init_cluster() {
        let env = test_env();
        let args = InitCluster {
            cluster_id: "test-cluster".to_string(),
        };
        let result = execute_init_cluster(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_list_clients() {
        let env = test_env();
        let args = ListClients {};
        let result = execute_list_clients(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table.len(), 1); // Header only.
    }

    #[tokio::test]
    async fn test_refresh_routing_info() {
        let env = test_env();
        let args = RefreshRoutingInfo {};
        let result = execute_refresh_routing_info(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cluster_status() {
        let env = test_env();
        let args = ClusterStatus {};
        let result = execute_status(&env, &args).await;
        assert!(result.is_ok());
    }
}
