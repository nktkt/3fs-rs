//! Storage management commands.
//!
//! Mirrors the C++ storage-related admin commands:
//! - `list-targets`       -> `3FS/src/client/cli/admin/ListTargets.cc`
//! - `list-chains`        -> `3FS/src/client/cli/admin/ListChains.cc`
//! - `list-chain-tables`  -> `3FS/src/client/cli/admin/ListChainTables.cc`
//! - `query-chunk`        -> `3FS/src/client/cli/admin/QueryChunk.cc`
//! - `create-target`      -> `3FS/src/client/cli/admin/CreateTarget.cc`
//! - `create-targets`     -> `3FS/src/client/cli/admin/CreateTargets.cc`
//! - `offline-target`     -> `3FS/src/client/cli/admin/OfflineTarget.cc`
//! - `remove-target`      -> `3FS/src/client/cli/admin/RemoveTarget.cc`
//! - `upload-chain-table` -> `3FS/src/client/cli/admin/UploadChainTable.cc`
//! - `upload-chains`      -> `3FS/src/client/cli/admin/UploadChains.cc`
//! - `dump-chains`        -> `3FS/src/client/cli/admin/DumpChains.cc`
//! - `dump-chain-table`   -> `3FS/src/client/cli/admin/DumpChainTable.cc`
//! - `dump-chunk-meta`    -> `3FS/src/client/cli/admin/DumpChunkMeta.cc`
//! - `update-chain`       -> `3FS/src/client/cli/admin/UpdateChain.cc`
//! - `checksum`           -> `3FS/src/client/cli/admin/Checksum.cc`
//! - `find-orphaned-chunks` -> `3FS/src/client/cli/admin/FindOrphanedChunks.cc`
//! - `remove-chunks`      -> `3FS/src/client/cli/admin/RemoveChunks.cc`

use clap::Args;
use clap::Subcommand;

use crate::connection::AdminEnv;
use crate::output::{kv_row, table_with_header, OutputTable};

/// Storage management subcommands.
#[derive(Debug, Subcommand)]
pub enum StorageCommands {
    /// List storage targets, optionally filtered by chain.
    ///
    /// Mirrors `list-targets` from C++.
    ListTargets(ListTargets),

    /// List storage chains.
    ///
    /// Mirrors `list-chains` from C++.
    ListChains(ListChains),

    /// List chain tables.
    ///
    /// Mirrors `list-chain-tables` from C++.
    ListChainTables(ListChainTables),

    /// Query a specific chunk's metadata and replicas.
    ///
    /// Mirrors `query-chunk` from C++.
    QueryChunk(QueryChunk),

    /// Create a new storage target.
    ///
    /// Mirrors `create-target` from C++.
    CreateTarget(CreateTarget),

    /// Take a storage target offline.
    ///
    /// Mirrors `offline-target` from C++.
    OfflineTarget(OfflineTarget),

    /// Remove a storage target.
    ///
    /// Mirrors `remove-target` from C++.
    RemoveTarget(RemoveTarget),

    /// Upload a chain table definition.
    ///
    /// Mirrors `upload-chain-table` from C++.
    UploadChainTable(UploadChainTable),

    /// Upload chain definitions.
    ///
    /// Mirrors `upload-chains` from C++.
    UploadChains(UploadChains),

    /// Dump chain table information.
    ///
    /// Mirrors `dump-chain-table` from C++.
    DumpChainTable(DumpChainTable),

    /// Dump chain information.
    ///
    /// Mirrors `dump-chains` from C++.
    DumpChains(DumpChains),

    /// Dump chunk metadata.
    ///
    /// Mirrors `dump-chunk-meta` from C++.
    DumpChunkMeta(DumpChunkMeta),

    /// Update a chain configuration.
    ///
    /// Mirrors `update-chain` from C++.
    UpdateChain(UpdateChain),

    /// Compute or verify checksums on stored data.
    ///
    /// Mirrors `checksum` from C++.
    Checksum(ChecksumCmd),

    /// Find orphaned chunks not referenced by any inode.
    ///
    /// Mirrors `find-orphaned-chunks` from C++.
    FindOrphanedChunks(FindOrphanedChunks),

    /// Remove orphaned or specified chunks.
    ///
    /// Mirrors `remove-chunks` from C++.
    RemoveChunks(RemoveChunks),

    /// Set the preferred target order for a chain.
    ///
    /// Mirrors `set-preferred-target-order` from C++.
    SetPreferredTargetOrder(SetPreferredTargetOrder),

    /// Rotate chain to use preferred target order.
    ///
    /// Mirrors `rotate-as-preferred-order` from C++.
    RotateAsPreferredOrder(RotateAsPreferredOrder),

    /// Rotate the last server in a chain.
    ///
    /// Mirrors `rotate-last-srv` from C++.
    RotateLastSrv(RotateLastSrv),

    /// Shut down all chains (emergency operation).
    ///
    /// Mirrors `shutdown-all-chains` from C++.
    ShutdownAllChains(ShutdownAllChains),
}

// ---- Argument structs ----

/// Arguments for `storage list-targets`.
#[derive(Debug, Args)]
pub struct ListTargets {
    /// Filter by chain ID.
    #[arg(short = 'c', long)]
    pub chain_id: Option<u32>,

    /// List orphan targets (not assigned to any chain).
    #[arg(long, default_value_t = false)]
    pub orphan: bool,
}

/// Arguments for `storage list-chains`.
#[derive(Debug, Args)]
pub struct ListChains {
    /// Filter by chain table ID.
    #[arg(short = 't', long)]
    pub table_id: Option<u32>,
}

/// Arguments for `storage list-chain-tables`.
#[derive(Debug, Args)]
pub struct ListChainTables {}

/// Arguments for `storage query-chunk`.
#[derive(Debug, Args)]
pub struct QueryChunk {
    /// Chain ID (if not specified, derived from chunk's inode).
    #[arg(short = 'c', long)]
    pub chain_id: Option<u32>,

    /// Chunk ID string.
    #[arg(long)]
    pub chunk: String,

    /// Also perform a read operation on the chunk.
    #[arg(long, default_value_t = false)]
    pub read: bool,

    /// Target replica index for read (default 0).
    #[arg(long, default_value_t = 0)]
    pub index: u32,
}

/// Arguments for `storage create-target`.
#[derive(Debug, Args)]
pub struct CreateTarget {
    /// Node ID to create the target on.
    #[arg(short = 'n', long)]
    pub node_id: u32,

    /// Disk index on the node.
    #[arg(short = 'd', long)]
    pub disk_index: u32,
}

/// Arguments for `storage offline-target`.
#[derive(Debug, Args)]
pub struct OfflineTarget {
    /// Target ID to take offline.
    pub target_id: u32,
}

/// Arguments for `storage remove-target`.
#[derive(Debug, Args)]
pub struct RemoveTarget {
    /// Target ID to remove.
    pub target_id: u32,
}

/// Arguments for `storage upload-chain-table`.
#[derive(Debug, Args)]
pub struct UploadChainTable {
    /// Path to the chain table definition file.
    #[arg(short = 'f', long)]
    pub file: String,
}

/// Arguments for `storage upload-chains`.
#[derive(Debug, Args)]
pub struct UploadChains {
    /// Path to the chains definition file.
    #[arg(short = 'f', long)]
    pub file: String,
}

/// Arguments for `storage dump-chain-table`.
#[derive(Debug, Args)]
pub struct DumpChainTable {
    /// Chain table ID.
    pub table_id: u32,
}

/// Arguments for `storage dump-chains`.
#[derive(Debug, Args)]
pub struct DumpChains {
    /// Chain table ID.
    #[arg(short = 't', long)]
    pub table_id: Option<u32>,
}

/// Arguments for `storage dump-chunk-meta`.
#[derive(Debug, Args)]
pub struct DumpChunkMeta {
    /// Chain ID.
    #[arg(short = 'c', long)]
    pub chain_id: u32,

    /// Chunk ID.
    #[arg(long)]
    pub chunk: String,
}

/// Arguments for `storage update-chain`.
#[derive(Debug, Args)]
pub struct UpdateChain {
    /// Chain ID.
    pub chain_id: u32,

    /// New chain version.
    #[arg(long)]
    pub version: Option<u32>,
}

/// Arguments for `storage checksum`.
#[derive(Debug, Args)]
pub struct ChecksumCmd {
    /// File path to checksum.
    pub path: String,

    /// Verify existing checksum instead of computing.
    #[arg(long, default_value_t = false)]
    pub verify: bool,
}

/// Arguments for `storage find-orphaned-chunks`.
#[derive(Debug, Args)]
pub struct FindOrphanedChunks {
    /// Chain ID to scan.
    #[arg(short = 'c', long)]
    pub chain_id: Option<u32>,

    /// Output file path for results.
    #[arg(short = 'o', long)]
    pub output: Option<String>,
}

/// Arguments for `storage remove-chunks`.
#[derive(Debug, Args)]
pub struct RemoveChunks {
    /// Input file with chunk IDs to remove.
    #[arg(short = 'f', long)]
    pub file: String,

    /// Dry run -- show what would be removed.
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

/// Arguments for `storage set-preferred-target-order`.
#[derive(Debug, Args)]
pub struct SetPreferredTargetOrder {
    /// Chain ID.
    pub chain_id: u32,

    /// Comma-separated target IDs in preferred order.
    #[arg(long)]
    pub order: String,
}

/// Arguments for `storage rotate-as-preferred-order`.
#[derive(Debug, Args)]
pub struct RotateAsPreferredOrder {
    /// Chain ID.
    pub chain_id: u32,
}

/// Arguments for `storage rotate-last-srv`.
#[derive(Debug, Args)]
pub struct RotateLastSrv {
    /// Chain ID.
    pub chain_id: u32,
}

/// Arguments for `storage shutdown-all-chains`.
#[derive(Debug, Args)]
pub struct ShutdownAllChains {
    /// Confirmation flag -- must be set to actually execute.
    #[arg(long, default_value_t = false)]
    pub confirm: bool,
}

impl StorageCommands {
    /// Execute the storage subcommand and return an output table.
    pub async fn execute(&self, env: &mut AdminEnv) -> anyhow::Result<OutputTable> {
        match self {
            Self::ListTargets(args) => execute_list_targets(env, args).await,
            Self::ListChains(args) => execute_list_chains(env, args).await,
            Self::ListChainTables(args) => execute_list_chain_tables(env, args).await,
            Self::QueryChunk(args) => execute_query_chunk(env, args).await,
            Self::CreateTarget(args) => execute_create_target(env, args).await,
            Self::OfflineTarget(args) => execute_offline_target(env, args).await,
            Self::RemoveTarget(args) => execute_remove_target(env, args).await,
            Self::UploadChainTable(args) => execute_upload_chain_table(env, args).await,
            Self::UploadChains(args) => execute_upload_chains(env, args).await,
            Self::DumpChainTable(args) => execute_dump_chain_table(env, args).await,
            Self::DumpChains(args) => execute_dump_chains(env, args).await,
            Self::DumpChunkMeta(args) => execute_dump_chunk_meta(env, args).await,
            Self::UpdateChain(args) => execute_update_chain(env, args).await,
            Self::Checksum(args) => execute_checksum(env, args).await,
            Self::FindOrphanedChunks(args) => execute_find_orphaned_chunks(env, args).await,
            Self::RemoveChunks(args) => execute_remove_chunks(env, args).await,
            Self::SetPreferredTargetOrder(args) => {
                execute_set_preferred_target_order(env, args).await
            }
            Self::RotateAsPreferredOrder(args) => {
                execute_rotate_as_preferred_order(env, args).await
            }
            Self::RotateLastSrv(args) => execute_rotate_last_srv(env, args).await,
            Self::ShutdownAllChains(args) => execute_shutdown_all_chains(env, args).await,
        }
    }
}

// ---- Command implementations ----

/// Execute `storage list-targets`.
///
/// Mirrors `handleListTargets` in `ListTargets.cc`.
/// Output columns: TargetId, ChainId, Role, PublicState, LocalState, NodeId, DiskIndex, UsedSize.
async fn execute_list_targets(
    _env: &AdminEnv,
    args: &ListTargets,
) -> anyhow::Result<OutputTable> {
    if args.orphan {
        let table = table_with_header(&[
            "TargetId",
            "LocalState",
            "NodeId",
            "DiskIndex",
            "UsedSize",
        ]);

        // TODO: connect to mgmtd and list orphan targets.
        tracing::debug!("storage list-targets --orphan executed");
        return Ok(table);
    }

    let mut table = table_with_header(&[
        "TargetId",
        "ChainId",
        "Role",
        "PublicState",
        "LocalState",
        "NodeId",
        "DiskIndex",
        "UsedSize",
    ]);

    if let Some(chain_id) = args.chain_id {
        tracing::debug!(chain_id, "storage list-targets filtered by chain");
    }

    // TODO: connect to mgmtd, refresh routing info, and list targets.

    tracing::debug!("storage list-targets executed");

    Ok(table)
}

/// Execute `storage list-chains`.
async fn execute_list_chains(
    _env: &AdminEnv,
    args: &ListChains,
) -> anyhow::Result<OutputTable> {
    let table = table_with_header(&[
        "ChainId",
        "ChainVersion",
        "Targets",
        "PreferredOrder",
    ]);

    if let Some(table_id) = args.table_id {
        tracing::debug!(table_id, "storage list-chains filtered by table");
    }

    // TODO: connect to mgmtd and list chains.

    tracing::debug!("storage list-chains executed");

    Ok(table)
}

/// Execute `storage list-chain-tables`.
async fn execute_list_chain_tables(
    _env: &AdminEnv,
    _args: &ListChainTables,
) -> anyhow::Result<OutputTable> {
    let table = table_with_header(&["TableId", "Version", "NumChains"]);

    // TODO: connect to mgmtd and list chain tables.

    tracing::debug!("storage list-chain-tables executed");

    Ok(table)
}

/// Execute `storage query-chunk`.
///
/// Mirrors `handleQueryChunk` in `QueryChunk.cc`.
async fn execute_query_chunk(
    _env: &AdminEnv,
    args: &QueryChunk,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Chunk", &args.chunk));

    if let Some(chain_id) = args.chain_id {
        table.push(kv_row("ChainId", chain_id));
    }

    if args.read {
        table.push(kv_row("ReadIndex", args.index));
    }

    // TODO: connect to storage client and query the chunk.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(chunk = %args.chunk, "storage query-chunk executed");

    Ok(table)
}

/// Execute `storage create-target`.
async fn execute_create_target(
    _env: &AdminEnv,
    args: &CreateTarget,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("NodeId", args.node_id));
    table.push(kv_row("DiskIndex", args.disk_index));

    // TODO: connect to mgmtd and create target.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(node_id = args.node_id, disk_index = args.disk_index, "storage create-target executed");

    Ok(table)
}

/// Execute `storage offline-target`.
async fn execute_offline_target(
    _env: &AdminEnv,
    args: &OfflineTarget,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("TargetId", args.target_id));

    // TODO: connect to mgmtd and offline target.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(target_id = args.target_id, "storage offline-target executed");

    Ok(table)
}

/// Execute `storage remove-target`.
async fn execute_remove_target(
    _env: &AdminEnv,
    args: &RemoveTarget,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("TargetId", args.target_id));

    // TODO: connect to mgmtd and remove target.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(target_id = args.target_id, "storage remove-target executed");

    Ok(table)
}

/// Execute `storage upload-chain-table`.
async fn execute_upload_chain_table(
    _env: &AdminEnv,
    args: &UploadChainTable,
) -> anyhow::Result<OutputTable> {
    // Verify file exists.
    if !tokio::fs::try_exists(&args.file).await.unwrap_or(false) {
        anyhow::bail!("file not found: {}", args.file);
    }

    let mut table = OutputTable::new();
    table.push(kv_row("File", &args.file));

    // TODO: read file and upload to mgmtd.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(file = %args.file, "storage upload-chain-table executed");

    Ok(table)
}

/// Execute `storage upload-chains`.
async fn execute_upload_chains(
    _env: &AdminEnv,
    args: &UploadChains,
) -> anyhow::Result<OutputTable> {
    if !tokio::fs::try_exists(&args.file).await.unwrap_or(false) {
        anyhow::bail!("file not found: {}", args.file);
    }

    let mut table = OutputTable::new();
    table.push(kv_row("File", &args.file));

    // TODO: read file and upload to mgmtd.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(file = %args.file, "storage upload-chains executed");

    Ok(table)
}

/// Execute `storage dump-chain-table`.
async fn execute_dump_chain_table(
    _env: &AdminEnv,
    args: &DumpChainTable,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("TableId", args.table_id));

    // TODO: connect to mgmtd and dump chain table.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(table_id = args.table_id, "storage dump-chain-table executed");

    Ok(table)
}

/// Execute `storage dump-chains`.
async fn execute_dump_chains(
    _env: &AdminEnv,
    args: &DumpChains,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();

    if let Some(table_id) = args.table_id {
        table.push(kv_row("TableId", table_id));
    }

    // TODO: connect to mgmtd and dump chains.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!("storage dump-chains executed");

    Ok(table)
}

/// Execute `storage dump-chunk-meta`.
async fn execute_dump_chunk_meta(
    _env: &AdminEnv,
    args: &DumpChunkMeta,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("ChainId", args.chain_id));
    table.push(kv_row("Chunk", &args.chunk));

    // TODO: connect to storage and dump chunk metadata.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(chain_id = args.chain_id, chunk = %args.chunk, "storage dump-chunk-meta executed");

    Ok(table)
}

/// Execute `storage update-chain`.
async fn execute_update_chain(
    _env: &AdminEnv,
    args: &UpdateChain,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("ChainId", args.chain_id));

    if let Some(version) = args.version {
        table.push(kv_row("Version", version));
    }

    // TODO: connect to mgmtd and update chain.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(chain_id = args.chain_id, "storage update-chain executed");

    Ok(table)
}

/// Execute `storage checksum`.
async fn execute_checksum(_env: &AdminEnv, args: &ChecksumCmd) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));
    table.push(kv_row("Mode", if args.verify { "verify" } else { "compute" }));

    // TODO: connect to storage and compute/verify checksum.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, verify = args.verify, "storage checksum executed");

    Ok(table)
}

/// Execute `storage find-orphaned-chunks`.
async fn execute_find_orphaned_chunks(
    _env: &AdminEnv,
    args: &FindOrphanedChunks,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();

    if let Some(chain_id) = args.chain_id {
        table.push(kv_row("ChainId", chain_id));
    }

    if let Some(ref output) = args.output {
        table.push(kv_row("Output", output));
    }

    // TODO: scan chunks and find orphans.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!("storage find-orphaned-chunks executed");

    Ok(table)
}

/// Execute `storage remove-chunks`.
async fn execute_remove_chunks(
    _env: &AdminEnv,
    args: &RemoveChunks,
) -> anyhow::Result<OutputTable> {
    if !tokio::fs::try_exists(&args.file).await.unwrap_or(false) {
        anyhow::bail!("file not found: {}", args.file);
    }

    let mut table = OutputTable::new();
    table.push(kv_row("File", &args.file));
    table.push(kv_row("DryRun", args.dry_run));

    // TODO: read chunk list and remove them.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(file = %args.file, dry_run = args.dry_run, "storage remove-chunks executed");

    Ok(table)
}

/// Execute `storage set-preferred-target-order`.
async fn execute_set_preferred_target_order(
    _env: &AdminEnv,
    args: &SetPreferredTargetOrder,
) -> anyhow::Result<OutputTable> {
    let order: Vec<u32> = args
        .order
        .split(',')
        .map(|s| {
            s.trim()
                .parse::<u32>()
                .map_err(|e| anyhow::anyhow!("invalid target ID '{}': {}", s, e))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut table = OutputTable::new();
    table.push(kv_row("ChainId", args.chain_id));
    table.push(kv_row(
        "Order",
        format!(
            "[{}]",
            order
                .iter()
                .map(|t| t.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ),
    ));

    // TODO: connect to mgmtd and set preferred order.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(chain_id = args.chain_id, "storage set-preferred-target-order executed");

    Ok(table)
}

/// Execute `storage rotate-as-preferred-order`.
async fn execute_rotate_as_preferred_order(
    _env: &AdminEnv,
    args: &RotateAsPreferredOrder,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("ChainId", args.chain_id));

    // TODO: connect to mgmtd and rotate.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(chain_id = args.chain_id, "storage rotate-as-preferred-order executed");

    Ok(table)
}

/// Execute `storage rotate-last-srv`.
async fn execute_rotate_last_srv(
    _env: &AdminEnv,
    args: &RotateLastSrv,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("ChainId", args.chain_id));

    // TODO: connect to mgmtd and rotate last server.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(chain_id = args.chain_id, "storage rotate-last-srv executed");

    Ok(table)
}

/// Execute `storage shutdown-all-chains`.
async fn execute_shutdown_all_chains(
    _env: &AdminEnv,
    args: &ShutdownAllChains,
) -> anyhow::Result<OutputTable> {
    if !args.confirm {
        anyhow::bail!(
            "shutdown-all-chains is a dangerous operation. \
             Pass --confirm to proceed."
        );
    }

    let mut table = OutputTable::new();

    // TODO: connect to mgmtd and shut down all chains.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!("storage shutdown-all-chains executed");

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
    async fn test_list_targets() {
        let env = test_env();
        let args = ListTargets {
            chain_id: None,
            orphan: false,
        };
        let result = execute_list_targets(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table[0][0], "TargetId");
    }

    #[tokio::test]
    async fn test_list_targets_orphan() {
        let env = test_env();
        let args = ListTargets {
            chain_id: None,
            orphan: true,
        };
        let result = execute_list_targets(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table[0][0], "TargetId");
        // Orphan table has fewer columns.
        assert_eq!(table[0].len(), 5);
    }

    #[tokio::test]
    async fn test_list_chains() {
        let env = test_env();
        let args = ListChains { table_id: None };
        let result = execute_list_chains(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_query_chunk() {
        let env = test_env();
        let args = QueryChunk {
            chain_id: Some(1),
            chunk: "test-chunk-id".to_string(),
            read: false,
            index: 0,
        };
        let result = execute_query_chunk(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        let keys: Vec<&str> = table.iter().map(|r| r[0].as_str()).collect();
        assert!(keys.contains(&"Chunk"));
        assert!(keys.contains(&"ChainId"));
    }

    #[tokio::test]
    async fn test_shutdown_all_chains_without_confirm() {
        let env = test_env();
        let args = ShutdownAllChains { confirm: false };
        let result = execute_shutdown_all_chains(&env, &args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("confirm"));
    }

    #[tokio::test]
    async fn test_shutdown_all_chains_with_confirm() {
        let env = test_env();
        let args = ShutdownAllChains { confirm: true };
        let result = execute_shutdown_all_chains(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_preferred_target_order() {
        let env = test_env();
        let args = SetPreferredTargetOrder {
            chain_id: 1,
            order: "10,20,30".to_string(),
        };
        let result = execute_set_preferred_target_order(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        let order_row = table.iter().find(|r| r[0] == "Order").unwrap();
        assert_eq!(order_row[1], "[10,20,30]");
    }

    #[tokio::test]
    async fn test_set_preferred_target_order_invalid() {
        let env = test_env();
        let args = SetPreferredTargetOrder {
            chain_id: 1,
            order: "10,abc,30".to_string(),
        };
        let result = execute_set_preferred_target_order(&env, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_target() {
        let env = test_env();
        let args = CreateTarget {
            node_id: 1,
            disk_index: 0,
        };
        let result = execute_create_target(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_list_chain_tables() {
        let env = test_env();
        let args = ListChainTables {};
        let result = execute_list_chain_tables(&env, &args).await;
        assert!(result.is_ok());
    }
}
