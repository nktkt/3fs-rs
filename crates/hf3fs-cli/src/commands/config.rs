//! Configuration management commands.
//!
//! Mirrors the C++ config-related admin commands:
//! - `get-config`   -> `3FS/src/client/cli/admin/GetConfig.cc`
//! - `set-config`   -> `3FS/src/client/cli/admin/SetConfig.cc`
//! - `verify-config` -> `3FS/src/client/cli/admin/VerifyConfig.cc`
//! - `render-config` -> `3FS/src/client/cli/admin/RenderConfig.cc`
//! - `hot-update-config` -> `3FS/src/client/cli/admin/HotUpdateConfig.cc`
//! - `get-last-config-update-record` -> `3FS/src/client/cli/admin/GetLastConfigUpdateRecord.cc`

use clap::Args;
use clap::Subcommand;

use crate::connection::{node_type_name, parse_node_type, AdminEnv};
use crate::output::{kv_row, table_with_header, OutputTable};

/// Configuration management subcommands.
#[derive(Debug, Subcommand)]
pub enum ConfigCommands {
    /// Show (get) configuration for a node type or specific node.
    ///
    /// Mirrors `get-config` from C++.
    Show(ShowConfig),

    /// Upload (set) configuration for a node type.
    ///
    /// Mirrors `set-config` from C++.
    Update(UpdateConfig),

    /// Validate a configuration template against running nodes.
    ///
    /// Mirrors `verify-config` from C++.
    Validate(ValidateConfig),

    /// Hot-update running configuration on a node or all nodes of a type.
    ///
    /// Mirrors `hot-update-config` from C++.
    HotUpdate(HotUpdateConfig),

    /// Render a configuration template on a specific node.
    ///
    /// Mirrors `render-config` from C++.
    Render(RenderConfig),

    /// List configuration versions for all node types.
    ///
    /// Mirrors `get-config -l` from C++.
    ListVersions(ListConfigVersions),

    /// Get the last configuration update record.
    ///
    /// Mirrors `get-last-config-update-record` from C++.
    LastUpdateRecord(GetLastUpdateRecord),
}

/// Arguments for `config show`.
#[derive(Debug, Args)]
pub struct ShowConfig {
    /// Node type (META, STORAGE, CLIENT, MGMTD).
    #[arg(short = 't', long)]
    pub node_type: String,

    /// Specific node ID to fetch config from (via core RPC).
    #[arg(short = 'n', long)]
    pub node_id: Option<u32>,

    /// Output file path to save the configuration.
    #[arg(short = 'o', long)]
    pub output_file: Option<String>,
}

/// Arguments for `config update`.
#[derive(Debug, Args)]
pub struct UpdateConfig {
    /// Node type (META, STORAGE, CLIENT, MGMTD).
    #[arg(short = 't', long)]
    pub node_type: String,

    /// Path to the configuration file to upload.
    #[arg(short = 'f', long)]
    pub file: String,

    /// Description of the config update.
    #[arg(long, default_value = "")]
    pub desc: String,
}

/// Arguments for `config validate`.
#[derive(Debug, Args)]
pub struct ValidateConfig {
    /// Node type (META, STORAGE, MGMTD).
    #[arg(short = 't', long)]
    pub node_type: String,

    /// Path to the template configuration file.
    #[arg(short = 'f', long)]
    pub template_file: String,

    /// Show per-node details.
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

/// Arguments for `config hot-update`.
#[derive(Debug, Args)]
pub struct HotUpdateConfig {
    /// Node ID to update.
    #[arg(short = 'n', long, group = "target")]
    pub node_id: Option<u32>,

    /// Node type -- update all active nodes of this type.
    #[arg(short = 't', long, group = "target")]
    pub node_type: Option<String>,

    /// Client ID to update.
    #[arg(short = 'c', long, group = "target")]
    pub client_id: Option<String>,

    /// Direct address to update.
    #[arg(short = 'a', long, group = "target")]
    pub addr: Option<String>,

    /// Configuration update as a string.
    #[arg(short = 's', long, group = "source")]
    pub string: Option<String>,

    /// Path to configuration update file.
    #[arg(short = 'f', long, group = "source")]
    pub file: Option<String>,

    /// Whether to render the config before applying.
    #[arg(long, default_value_t = false)]
    pub render: bool,
}

/// Arguments for `config render`.
#[derive(Debug, Args)]
pub struct RenderConfig {
    /// Node ID to render config on.
    #[arg(short = 'n', long)]
    pub node_id: Option<u32>,

    /// Direct address.
    #[arg(short = 'a', long)]
    pub addr: Option<String>,

    /// Path to the template configuration file.
    #[arg(short = 'f', long)]
    pub file: String,

    /// Test update (dry run).
    #[arg(long, default_value_t = false)]
    pub test_update: bool,

    /// Hot update mode.
    #[arg(long, default_value_t = false)]
    pub hot_update: bool,
}

/// Arguments for `config list-versions`.
#[derive(Debug, Args)]
pub struct ListConfigVersions {}

/// Arguments for `config last-update-record`.
#[derive(Debug, Args)]
pub struct GetLastUpdateRecord {
    /// Node ID to query.
    #[arg(short = 'n', long)]
    pub node_id: Option<u32>,

    /// Direct address.
    #[arg(short = 'a', long)]
    pub addr: Option<String>,
}

impl ConfigCommands {
    /// Execute the config subcommand and return an output table.
    pub async fn execute(&self, env: &mut AdminEnv) -> anyhow::Result<OutputTable> {
        match self {
            Self::Show(args) => execute_show(env, args).await,
            Self::Update(args) => execute_update(env, args).await,
            Self::Validate(args) => execute_validate(env, args).await,
            Self::HotUpdate(args) => execute_hot_update(env, args).await,
            Self::Render(args) => execute_render(env, args).await,
            Self::ListVersions(args) => execute_list_versions(env, args).await,
            Self::LastUpdateRecord(args) => execute_last_update_record(env, args).await,
        }
    }
}

/// Execute `config show` -- retrieve configuration for a node type or specific node.
///
/// Mirrors the C++ `handle` in `GetConfig.cc`.
async fn execute_show(_env: &AdminEnv, args: &ShowConfig) -> anyhow::Result<OutputTable> {
    let node_type = parse_node_type(&args.node_type)
        .ok_or_else(|| anyhow::anyhow!("invalid node type: {}", args.node_type))?;

    let mut table = OutputTable::new();
    table.push(kv_row("NodeType", node_type_name(node_type)));

    if let Some(node_id) = args.node_id {
        // Fetch config from a specific node via core RPC.
        table.push(kv_row("NodeId", node_id));
        // TODO: connect to node and fetch config via CoreClient::get_config.
        table.push(kv_row("Status", "not yet implemented (client crate is stub)"));
    } else {
        // Fetch config from mgmtd for the node type.
        // TODO: connect to mgmtd and fetch config.
        table.push(kv_row("Status", "not yet implemented (client crate is stub)"));
    }

    if let Some(ref output_file) = args.output_file {
        table.push(kv_row("OutputFile", output_file));
    }

    tracing::debug!(
        node_type = %args.node_type,
        node_id = ?args.node_id,
        "config show executed"
    );

    Ok(table)
}

/// Execute `config update` -- upload a new configuration for a node type.
///
/// Mirrors the C++ `handle` in `SetConfig.cc`.
async fn execute_update(_env: &AdminEnv, args: &UpdateConfig) -> anyhow::Result<OutputTable> {
    let node_type = parse_node_type(&args.node_type)
        .ok_or_else(|| anyhow::anyhow!("invalid node type: {}", args.node_type))?;

    // Read the config file and validate it as TOML.
    let content = tokio::fs::read_to_string(&args.file)
        .await
        .map_err(|e| anyhow::anyhow!("failed to read config file '{}': {}", args.file, e))?;

    // Validate TOML parse.
    let _parsed: toml::Value = content
        .parse()
        .map_err(|e: toml::de::Error| anyhow::anyhow!("invalid TOML in '{}': {}", args.file, e))?;

    let mut table = OutputTable::new();
    table.push(kv_row("NodeType", node_type_name(node_type)));
    table.push(kv_row("File", &args.file));

    // TODO: connect to mgmtd and call set_config.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(
        node_type = %args.node_type,
        file = %args.file,
        "config update executed"
    );

    Ok(table)
}

/// Execute `config validate` -- validate a config template against running nodes.
///
/// Mirrors the C++ `handle` in `VerifyConfig.cc`.
async fn execute_validate(_env: &AdminEnv, args: &ValidateConfig) -> anyhow::Result<OutputTable> {
    let node_type = parse_node_type(&args.node_type)
        .ok_or_else(|| anyhow::anyhow!("invalid node type: {}", args.node_type))?;

    // Read the template file and validate it as TOML.
    let content = tokio::fs::read_to_string(&args.template_file)
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "failed to read template file '{}': {}",
                args.template_file,
                e
            )
        })?;

    let _parsed: toml::Value = content.parse().map_err(|e: toml::de::Error| {
        anyhow::anyhow!("invalid TOML in '{}': {}", args.template_file, e)
    })?;

    let mut table = OutputTable::new();
    table.push(kv_row("NodeType", node_type_name(node_type)));
    table.push(kv_row("TemplateFile", &args.template_file));

    // TODO: iterate over active nodes of this type, render + compare config.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    if args.verbose {
        table.push(kv_row("Verbose", "enabled"));
    }

    tracing::debug!(
        node_type = %args.node_type,
        template_file = %args.template_file,
        verbose = args.verbose,
        "config validate executed"
    );

    Ok(table)
}

/// Execute `config hot-update` -- push a config hot-update to a running node.
///
/// Mirrors the C++ `handle` in `HotUpdateConfig.cc`.
async fn execute_hot_update(_env: &AdminEnv, args: &HotUpdateConfig) -> anyhow::Result<OutputTable> {
    // Determine the update content.
    let update_content = if let Some(ref s) = args.string {
        s.clone()
    } else if let Some(ref f) = args.file {
        tokio::fs::read_to_string(f)
            .await
            .map_err(|e| anyhow::anyhow!("failed to read file '{}': {}", f, e))?
    } else {
        anyhow::bail!("must specify one of --string or --file");
    };

    // Validate TOML.
    let _parsed: toml::Value = update_content
        .parse()
        .map_err(|e: toml::de::Error| anyhow::anyhow!("invalid TOML: {}", e))?;

    let mut table = OutputTable::new();

    if let Some(node_id) = args.node_id {
        table.push(kv_row("NodeId", node_id));
    } else if let Some(ref node_type) = args.node_type {
        let nt = parse_node_type(node_type)
            .ok_or_else(|| anyhow::anyhow!("invalid node type: {}", node_type))?;
        table.push(kv_row("NodeType", node_type_name(nt)));
    } else if let Some(ref client_id) = args.client_id {
        table.push(kv_row("ClientId", client_id));
    } else if let Some(ref addr) = args.addr {
        table.push(kv_row("Address", addr));
    } else {
        anyhow::bail!("must specify one of --node-id, --node-type, --client-id, or --addr");
    }

    // TODO: connect and send hot-update via CoreClient.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!("config hot-update executed");

    Ok(table)
}

/// Execute `config render` -- render a config template on a node.
///
/// Mirrors the C++ `RenderConfig.cc`.
async fn execute_render(_env: &AdminEnv, args: &RenderConfig) -> anyhow::Result<OutputTable> {
    let content = tokio::fs::read_to_string(&args.file)
        .await
        .map_err(|e| anyhow::anyhow!("failed to read file '{}': {}", args.file, e))?;

    let _parsed: toml::Value = content
        .parse()
        .map_err(|e: toml::de::Error| anyhow::anyhow!("invalid TOML in '{}': {}", args.file, e))?;

    let mut table = OutputTable::new();
    table.push(kv_row("File", &args.file));
    table.push(kv_row("TestUpdate", args.test_update));
    table.push(kv_row("HotUpdate", args.hot_update));

    // TODO: connect and call CoreClient::render_config.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(file = %args.file, "config render executed");

    Ok(table)
}

/// Execute `config list-versions` -- list config versions for all node types.
///
/// Mirrors the `-l` / `--list-versions` flag from `GetConfig.cc`.
async fn execute_list_versions(
    _env: &AdminEnv,
    _args: &ListConfigVersions,
) -> anyhow::Result<OutputTable> {
    let mut table = table_with_header(&["Type", "ConfigVersion"]);

    // TODO: connect to mgmtd and fetch config versions.
    table.push(vec!["META".to_string(), "N/A".to_string()]);
    table.push(vec!["STORAGE".to_string(), "N/A".to_string()]);
    table.push(vec!["MGMTD".to_string(), "N/A".to_string()]);
    table.push(vec!["CLIENT".to_string(), "N/A".to_string()]);

    tracing::debug!("config list-versions executed");

    Ok(table)
}

/// Execute `config last-update-record` -- get the last config update record.
///
/// Mirrors `GetLastConfigUpdateRecord.cc`.
async fn execute_last_update_record(
    _env: &AdminEnv,
    args: &GetLastUpdateRecord,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();

    if let Some(node_id) = args.node_id {
        table.push(kv_row("NodeId", node_id));
    } else if let Some(ref addr) = args.addr {
        table.push(kv_row("Address", addr));
    }

    // TODO: connect and fetch last update record.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!("config last-update-record executed");

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
    async fn test_show_config_invalid_type() {
        let env = test_env();
        let args = ShowConfig {
            node_type: "INVALID".to_string(),
            node_id: None,
            output_file: None,
        };
        let result = execute_show(&env, &args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid node type"));
    }

    #[tokio::test]
    async fn test_show_config_valid_type() {
        let env = test_env();
        let args = ShowConfig {
            node_type: "META".to_string(),
            node_id: None,
            output_file: None,
        };
        let result = execute_show(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        assert!(!table.is_empty());
        assert_eq!(table[0][0], "NodeType");
        assert_eq!(table[0][1], "META");
    }

    #[tokio::test]
    async fn test_show_config_with_node_id() {
        let env = test_env();
        let args = ShowConfig {
            node_type: "STORAGE".to_string(),
            node_id: Some(42),
            output_file: Some("/tmp/out.toml".to_string()),
        };
        let result = execute_show(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        // Should contain NodeType, NodeId, OutputFile rows.
        let keys: Vec<&str> = table.iter().map(|r| r[0].as_str()).collect();
        assert!(keys.contains(&"NodeType"));
        assert!(keys.contains(&"NodeId"));
        assert!(keys.contains(&"OutputFile"));
    }

    #[tokio::test]
    async fn test_update_config_missing_file() {
        let env = test_env();
        let args = UpdateConfig {
            node_type: "META".to_string(),
            file: "/nonexistent/file.toml".to_string(),
            desc: "test".to_string(),
        };
        let result = execute_update(&env, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_versions() {
        let env = test_env();
        let args = ListConfigVersions {};
        let result = execute_list_versions(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        // Header + 4 node types.
        assert_eq!(table.len(), 5);
        assert_eq!(table[0][0], "Type");
    }

    #[tokio::test]
    async fn test_hot_update_no_target() {
        let env = test_env();
        let args = HotUpdateConfig {
            node_id: None,
            node_type: None,
            client_id: None,
            addr: None,
            string: Some("key = \"value\"".to_string()),
            file: None,
            render: false,
        };
        let result = execute_hot_update(&env, &args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must specify one of"));
    }

    #[tokio::test]
    async fn test_hot_update_no_source() {
        let env = test_env();
        let args = HotUpdateConfig {
            node_id: Some(1),
            node_type: None,
            client_id: None,
            addr: None,
            string: None,
            file: None,
            render: false,
        };
        let result = execute_hot_update(&env, &args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must specify one of --string or --file"));
    }

    #[tokio::test]
    async fn test_hot_update_invalid_toml() {
        let env = test_env();
        let args = HotUpdateConfig {
            node_id: Some(1),
            node_type: None,
            client_id: None,
            addr: None,
            string: Some("this is not [[valid toml".to_string()),
            file: None,
            render: false,
        };
        let result = execute_hot_update(&env, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_hot_update_with_string() {
        let env = test_env();
        let args = HotUpdateConfig {
            node_id: Some(1),
            node_type: None,
            client_id: None,
            addr: None,
            string: Some("key = \"value\"".to_string()),
            file: None,
            render: false,
        };
        let result = execute_hot_update(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_last_update_record() {
        let env = test_env();
        let args = GetLastUpdateRecord {
            node_id: Some(1),
            addr: None,
        };
        let result = execute_last_update_record(&env, &args).await;
        assert!(result.is_ok());
    }
}
