//! Top-level admin CLI command enum.
//!
//! Mirrors the full set of admin commands registered in
//! `3FS/src/client/cli/admin/registerAdminCommands.cc`, grouped into
//! logical subcommand categories.
//!
//! The C++ version uses a flat dispatcher with commands like `stat`, `ls`,
//! `get-config`, `list-nodes`, etc. In Rust we organize these into nested
//! subcommand groups (config, cluster, storage, meta) for better discoverability,
//! while keeping the flat names available as aliases.

use clap::Subcommand;

use super::cluster::ClusterCommands;
use super::config::ConfigCommands;
use super::meta::MetaCommands;
use super::storage::StorageCommands;
use crate::connection::AdminEnv;
use crate::output::{OutputFormat, OutputTable, Printer};

/// Top-level admin command set for the 3FS CLI.
///
/// Groups all admin operations into logical subcommand categories.
/// Each variant dispatches to the corresponding subcommand group.
#[derive(Debug, Subcommand)]
pub enum AdminCommands {
    /// Configuration management commands (get-config, set-config, verify-config, etc.).
    #[command(subcommand)]
    Config(ConfigCommands),

    /// Cluster management commands (list-nodes, register-node, init-cluster, etc.).
    #[command(subcommand)]
    Cluster(ClusterCommands),

    /// Storage management commands (list-targets, query-chunk, list-chains, etc.).
    #[command(subcommand)]
    Storage(StorageCommands),

    /// Metadata / filesystem commands (stat, ls, mkdir, rm, rename, etc.).
    #[command(subcommand)]
    Meta(MetaCommands),
}

impl AdminCommands {
    /// Execute the command and return the output table.
    ///
    /// Each subcommand group dispatches to its own `execute` method which
    /// connects to the cluster via `AdminEnv` and performs the operation.
    pub async fn execute(&self, env: &mut AdminEnv) -> anyhow::Result<OutputTable> {
        match self {
            Self::Config(cmd) => cmd.execute(env).await,
            Self::Cluster(cmd) => cmd.execute(env).await,
            Self::Storage(cmd) => cmd.execute(env).await,
            Self::Meta(cmd) => cmd.execute(env).await,
        }
    }

    /// Execute and print the result using the given output format.
    pub async fn run(&self, env: &mut AdminEnv, output_format: OutputFormat) -> anyhow::Result<()> {
        let mut printer = Printer::stdout(output_format);
        match self.execute(env).await {
            Ok(table) => {
                printer.print_table(&table)?;
                Ok(())
            }
            Err(e) => {
                printer.print_error(&format!("{:#}", e))?;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that all subcommand variants can be constructed.
    #[test]
    fn test_admin_commands_variants() {
        // Config variant
        let _config = AdminCommands::Config(ConfigCommands::Show(super::super::config::ShowConfig {
            node_type: "META".to_string(),
            node_id: None,
            output_file: None,
        }));

        // Cluster variant
        let _cluster = AdminCommands::Cluster(ClusterCommands::ListNodes(
            super::super::cluster::ListNodes {},
        ));

        // Storage variant
        let _storage = AdminCommands::Storage(StorageCommands::ListTargets(
            super::super::storage::ListTargets {
                chain_id: None,
                orphan: false,
            },
        ));

        // Meta variant
        let _meta = AdminCommands::Meta(MetaCommands::Stat(super::super::meta::StatPath {
            path: "/".to_string(),
            follow_link: false,
            display_layout: false,
            inode: false,
        }));
    }
}
