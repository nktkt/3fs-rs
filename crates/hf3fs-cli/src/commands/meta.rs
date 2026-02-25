//! Metadata / filesystem operation commands.
//!
//! Mirrors the C++ filesystem admin commands:
//! - `stat`           -> `3FS/src/client/cli/admin/Stat.cc`
//! - `ls`             -> `3FS/src/client/cli/admin/List.cc`
//! - `mkdir`          -> `3FS/src/client/cli/admin/Mkdir.cc`
//! - `rm`             -> `3FS/src/client/cli/admin/Remove.cc`
//! - `rename`         -> `3FS/src/client/cli/admin/Rename.cc`
//! - `create`         -> `3FS/src/client/cli/admin/Create.cc`
//! - `symlink`        -> `3FS/src/client/cli/admin/Symlink.cc`
//! - `set-permission` -> `3FS/src/client/cli/admin/SetPermission.cc`
//! - `chdir`          -> `3FS/src/client/cli/admin/Chdir.cc`
//! - `get-real-path`  -> `3FS/src/client/cli/admin/GetRealPath.cc`
//! - `set-layout`     -> `3FS/src/client/cli/admin/SetLayout.cc`
//! - `read-file`      -> `3FS/src/client/cli/admin/ReadFile.cc`
//! - `write-file`     -> `3FS/src/client/cli/admin/WriteFile.cc`
//! - `scan-tree`      -> `3FS/src/client/cli/admin/ScanTree.cc`
//! - `dump-inodes`    -> `3FS/src/client/cli/admin/DumpInodes.cc`
//! - `dump-dir-entries` -> `3FS/src/client/cli/admin/DumpDirEntries.cc`
//! - `dump-session`   -> `3FS/src/client/cli/admin/DumpSession.cc`
//! - `prune-session`  -> `3FS/src/client/cli/admin/PruneSession.cc`
//! - `recursive-chown` -> `3FS/src/client/cli/admin/RecursiveChown.cc`

use clap::Args;
use clap::Subcommand;

use crate::connection::AdminEnv;
use crate::output::{format_permission, kv_row, table_with_header, OutputTable};

/// Metadata / filesystem operation subcommands.
#[derive(Debug, Subcommand)]
pub enum MetaCommands {
    /// Display file/directory/symlink metadata.
    ///
    /// Mirrors `stat` from C++.
    Stat(StatPath),

    /// List directory contents.
    ///
    /// Mirrors `ls` from C++.
    Ls(ListDir),

    /// Create a directory.
    ///
    /// Mirrors `mkdir` from C++.
    Mkdir(MakeDir),

    /// Remove a file or directory.
    ///
    /// Mirrors `rm` from C++.
    Rm(Remove),

    /// Rename (move) a file or directory.
    ///
    /// Mirrors `rename` from C++.
    Rename(RenamePath),

    /// Create a file.
    ///
    /// Mirrors `create` from C++.
    Create(CreateFile),

    /// Create a symbolic link.
    ///
    /// Mirrors `symlink` from C++.
    Symlink(CreateSymlink),

    /// Set file/directory permissions.
    ///
    /// Mirrors `set-permission` from C++.
    SetPermission(SetPermission),

    /// Change the current working directory (interactive mode).
    ///
    /// Mirrors `chdir` from C++.
    Chdir(ChangeDir),

    /// Resolve a path to its real (absolute) path.
    ///
    /// Mirrors `get-real-path` from C++.
    GetRealPath(GetRealPath),

    /// Set the storage layout for a file or directory.
    ///
    /// Mirrors `set-layout` from C++.
    SetLayout(SetLayout),

    /// Read a file and output to stdout or a local file.
    ///
    /// Mirrors `read-file` from C++.
    ReadFile(ReadFile),

    /// Write data from stdin or a local file to a 3FS file.
    ///
    /// Mirrors `write-file` from C++.
    WriteFile(WriteFile),

    /// Recursively scan a directory tree.
    ///
    /// Mirrors `scan-tree` from C++.
    ScanTree(ScanTree),

    /// Dump inode metadata from the KV store.
    ///
    /// Mirrors `dump-inodes` from C++.
    DumpInodes(DumpInodes),

    /// Dump directory entries from the KV store.
    ///
    /// Mirrors `dump-dir-entries` from C++.
    DumpDirEntries(DumpDirEntries),

    /// Dump session information.
    ///
    /// Mirrors `dump-session` from C++.
    DumpSession(DumpSession),

    /// Prune stale sessions.
    ///
    /// Mirrors `prune-session` from C++.
    PruneSession(PruneSession),

    /// Recursively change ownership of a directory tree.
    ///
    /// Mirrors `recursive-chown` from C++.
    RecursiveChown(RecursiveChown),
}

// ---- Argument structs ----

/// Arguments for `meta stat`.
#[derive(Debug, Args)]
pub struct StatPath {
    /// Path to stat (relative to current directory).
    pub path: String,

    /// Follow symbolic links.
    #[arg(short = 'L', long, default_value_t = false)]
    pub follow_link: bool,

    /// Display the storage layout.
    #[arg(long, default_value_t = false)]
    pub display_layout: bool,

    /// Stat by inode ID instead of path.
    #[arg(long, default_value_t = false)]
    pub inode: bool,
}

/// Arguments for `meta ls`.
#[derive(Debug, Args)]
pub struct ListDir {
    /// Path to list (defaults to current directory).
    #[arg(default_value = ".")]
    pub path: String,

    /// Maximum number of entries to return.
    #[arg(short = 'l', long)]
    pub limit: Option<i32>,

    /// Include full inode status (mtime, size, perm) for each entry.
    #[arg(short = 's', long, default_value_t = false)]
    pub status: bool,
}

/// Arguments for `meta mkdir`.
#[derive(Debug, Args)]
pub struct MakeDir {
    /// Path of the directory to create.
    pub path: String,

    /// Create parent directories as needed (recursive).
    #[arg(short = 'p', long, default_value_t = false)]
    pub recursive: bool,

    /// Permission mode (octal, e.g. "0755").
    #[arg(long)]
    pub perm: Option<String>,
}

/// Arguments for `meta rm`.
#[derive(Debug, Args)]
pub struct Remove {
    /// Path to remove.
    pub path: String,

    /// Remove directories and their contents recursively.
    #[arg(short = 'r', long, default_value_t = false)]
    pub recursive: bool,
}

/// Arguments for `meta rename`.
#[derive(Debug, Args)]
pub struct RenamePath {
    /// Source path.
    pub src: String,

    /// Destination path.
    pub dest: String,
}

/// Arguments for `meta create`.
#[derive(Debug, Args)]
pub struct CreateFile {
    /// Path for the new file.
    pub path: String,

    /// Permission mode (octal, e.g. "0644").
    #[arg(long)]
    pub perm: Option<String>,
}

/// Arguments for `meta symlink`.
#[derive(Debug, Args)]
pub struct CreateSymlink {
    /// Path for the symbolic link.
    pub path: String,

    /// Target of the symbolic link.
    pub target: String,
}

/// Arguments for `meta set-permission`.
#[derive(Debug, Args)]
pub struct SetPermission {
    /// Path to modify.
    pub path: String,

    /// Permission mode (octal, e.g. "0755").
    pub perm: String,

    /// Also set owner UID.
    #[arg(long)]
    pub uid: Option<u32>,

    /// Also set owner GID.
    #[arg(long)]
    pub gid: Option<u32>,
}

/// Arguments for `meta chdir`.
#[derive(Debug, Args)]
pub struct ChangeDir {
    /// Path to change to.
    pub path: String,
}

/// Arguments for `meta get-real-path`.
#[derive(Debug, Args)]
pub struct GetRealPath {
    /// Path to resolve.
    pub path: String,
}

/// Arguments for `meta set-layout`.
#[derive(Debug, Args)]
pub struct SetLayout {
    /// Path to set layout on.
    pub path: String,

    /// Chain table ID.
    #[arg(long)]
    pub chain_table: Option<u64>,

    /// Stripe size.
    #[arg(long)]
    pub stripe_size: Option<u32>,

    /// Number of stripes.
    #[arg(long)]
    pub num_stripes: Option<u32>,
}

/// Arguments for `meta read-file`.
#[derive(Debug, Args)]
pub struct ReadFile {
    /// 3FS file path to read.
    pub path: String,

    /// Local file path to write output to (defaults to stdout).
    #[arg(short = 'o', long)]
    pub output: Option<String>,

    /// Offset to start reading from.
    #[arg(long, default_value_t = 0)]
    pub offset: u64,

    /// Maximum bytes to read (0 = entire file).
    #[arg(long, default_value_t = 0)]
    pub length: u64,
}

/// Arguments for `meta write-file`.
#[derive(Debug, Args)]
pub struct WriteFile {
    /// 3FS file path to write to.
    pub path: String,

    /// Local file path to read input from (defaults to stdin).
    #[arg(short = 'i', long)]
    pub input: Option<String>,

    /// Offset to start writing at.
    #[arg(long, default_value_t = 0)]
    pub offset: u64,
}

/// Arguments for `meta scan-tree`.
#[derive(Debug, Args)]
pub struct ScanTree {
    /// Root path to scan.
    #[arg(default_value = "/")]
    pub path: String,

    /// Maximum depth to scan (0 = unlimited).
    #[arg(long, default_value_t = 0)]
    pub max_depth: u32,
}

/// Arguments for `meta dump-inodes`.
#[derive(Debug, Args)]
pub struct DumpInodes {
    /// Start inode ID.
    #[arg(long)]
    pub start: Option<u64>,

    /// Maximum number of inodes to dump.
    #[arg(long)]
    pub limit: Option<u32>,
}

/// Arguments for `meta dump-dir-entries`.
#[derive(Debug, Args)]
pub struct DumpDirEntries {
    /// Parent inode ID.
    pub parent_inode: u64,
}

/// Arguments for `meta dump-session`.
#[derive(Debug, Args)]
pub struct DumpSession {
    /// Client ID to dump sessions for.
    #[arg(long)]
    pub client_id: Option<String>,
}

/// Arguments for `meta prune-session`.
#[derive(Debug, Args)]
pub struct PruneSession {
    /// Client ID whose sessions to prune.
    pub client_id: String,

    /// Specific session IDs to prune (comma-separated).
    #[arg(long)]
    pub sessions: Option<String>,
}

/// Arguments for `meta recursive-chown`.
#[derive(Debug, Args)]
pub struct RecursiveChown {
    /// Path to chown recursively.
    pub path: String,

    /// New owner UID.
    #[arg(long)]
    pub uid: Option<u32>,

    /// New owner GID.
    #[arg(long)]
    pub gid: Option<u32>,
}

impl MetaCommands {
    /// Execute the meta subcommand and return an output table.
    pub async fn execute(&self, env: &mut AdminEnv) -> anyhow::Result<OutputTable> {
        match self {
            Self::Stat(args) => execute_stat(env, args).await,
            Self::Ls(args) => execute_ls(env, args).await,
            Self::Mkdir(args) => execute_mkdir(env, args).await,
            Self::Rm(args) => execute_rm(env, args).await,
            Self::Rename(args) => execute_rename(env, args).await,
            Self::Create(args) => execute_create(env, args).await,
            Self::Symlink(args) => execute_symlink(env, args).await,
            Self::SetPermission(args) => execute_set_permission(env, args).await,
            Self::Chdir(args) => execute_chdir(env, args).await,
            Self::GetRealPath(args) => execute_get_real_path(env, args).await,
            Self::SetLayout(args) => execute_set_layout(env, args).await,
            Self::ReadFile(args) => execute_read_file(env, args).await,
            Self::WriteFile(args) => execute_write_file(env, args).await,
            Self::ScanTree(args) => execute_scan_tree(env, args).await,
            Self::DumpInodes(args) => execute_dump_inodes(env, args).await,
            Self::DumpDirEntries(args) => execute_dump_dir_entries(env, args).await,
            Self::DumpSession(args) => execute_dump_session(env, args).await,
            Self::PruneSession(args) => execute_prune_session(env, args).await,
            Self::RecursiveChown(args) => execute_recursive_chown(env, args).await,
        }
    }
}

// ---- Command implementations ----

/// Execute `meta stat`.
///
/// Mirrors `handleStat` in `Stat.cc`.
/// Output: key-value pairs for Type, Inode, NLink, Permission, IFlags, Uid, Gid,
/// Access, Change, Modified, and type-specific fields (Length, ParentInode, Target).
async fn execute_stat(_env: &AdminEnv, args: &StatPath) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();

    if args.inode {
        // Parse as inode ID.
        let inode_id: u64 = args
            .path
            .parse()
            .or_else(|_| u64::from_str_radix(args.path.trim_start_matches("0x"), 16))
            .map_err(|_| anyhow::anyhow!("invalid inode ID: {}", args.path))?;
        table.push(kv_row("InodeId", format!("0x{:x}", inode_id)));
    } else {
        table.push(kv_row("Path", &args.path));
    }

    // TODO: connect to meta service and perform stat.
    // The C++ implementation outputs:
    // Type, Inode (hex), NLink, Permission (octal), IFlags (hex),
    // Uid, Gid, Access (time), Change (time), Modified (time),
    // and for File: Length@TruncateVer, DynamicStripe, optional layout.
    // for Directory: ParentInode (hex), optional layout.
    // for Symlink: Target.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, follow_link = args.follow_link, "meta stat executed");

    Ok(table)
}

/// Execute `meta ls`.
///
/// Mirrors `handleList` in `List.cc`.
/// Output columns: Name, Type, InodeId (hex). With --status: also Mtime, Size, Perm.
async fn execute_ls(_env: &AdminEnv, args: &ListDir) -> anyhow::Result<OutputTable> {
    if args.status {
        let table = table_with_header(&["Name", "Type", "InodeId", "Mtime", "Size", "Perm"]);

        // TODO: connect to meta service and list with status.

        tracing::debug!(path = %args.path, limit = ?args.limit, status = true, "meta ls executed");

        return Ok(table);
    }

    let table = table_with_header(&["Name", "Type", "InodeId"]);

    // TODO: connect to meta service and list entries.
    // The C++ implementation paginates using prev/limit.

    tracing::debug!(path = %args.path, limit = ?args.limit, "meta ls executed");

    Ok(table)
}

/// Execute `meta mkdir`.
///
/// Mirrors `handleMkdir` in `Mkdir.cc`.
async fn execute_mkdir(_env: &AdminEnv, args: &MakeDir) -> anyhow::Result<OutputTable> {
    let perm = if let Some(ref p) = args.perm {
        let val = u32::from_str_radix(p.trim_start_matches("0o").trim_start_matches("0"), 8)
            .map_err(|_| anyhow::anyhow!("invalid permission format: {}", p))?;
        val
    } else {
        0o755
    };

    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));
    table.push(kv_row("Permission", format_permission(perm)));
    table.push(kv_row("Recursive", args.recursive));

    // TODO: connect to meta service and create directory.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, recursive = args.recursive, "meta mkdir executed");

    Ok(table)
}

/// Execute `meta rm`.
///
/// Mirrors `handleRemove` in `Remove.cc`.
async fn execute_rm(_env: &AdminEnv, args: &Remove) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));
    table.push(kv_row("Recursive", args.recursive));

    // TODO: connect to meta service and remove.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, recursive = args.recursive, "meta rm executed");

    Ok(table)
}

/// Execute `meta rename`.
///
/// Mirrors the C++ `Rename.cc`.
async fn execute_rename(_env: &AdminEnv, args: &RenamePath) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Source", &args.src));
    table.push(kv_row("Destination", &args.dest));

    // TODO: connect to meta service and rename.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(src = %args.src, dest = %args.dest, "meta rename executed");

    Ok(table)
}

/// Execute `meta create`.
///
/// Mirrors the C++ `Create.cc`.
async fn execute_create(_env: &AdminEnv, args: &CreateFile) -> anyhow::Result<OutputTable> {
    let perm = if let Some(ref p) = args.perm {
        u32::from_str_radix(p.trim_start_matches("0o").trim_start_matches("0"), 8)
            .map_err(|_| anyhow::anyhow!("invalid permission format: {}", p))?
    } else {
        0o644
    };

    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));
    table.push(kv_row("Permission", format_permission(perm)));

    // TODO: connect to meta service and create file.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, "meta create executed");

    Ok(table)
}

/// Execute `meta symlink`.
async fn execute_symlink(_env: &AdminEnv, args: &CreateSymlink) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));
    table.push(kv_row("Target", &args.target));

    // TODO: connect to meta service and create symlink.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, target = %args.target, "meta symlink executed");

    Ok(table)
}

/// Execute `meta set-permission`.
async fn execute_set_permission(
    _env: &AdminEnv,
    args: &SetPermission,
) -> anyhow::Result<OutputTable> {
    let perm = u32::from_str_radix(
        args.perm.trim_start_matches("0o").trim_start_matches("0"),
        8,
    )
    .map_err(|_| anyhow::anyhow!("invalid permission format: {}", args.perm))?;

    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));
    table.push(kv_row("Permission", format_permission(perm)));

    if let Some(uid) = args.uid {
        table.push(kv_row("Uid", uid));
    }
    if let Some(gid) = args.gid {
        table.push(kv_row("Gid", gid));
    }

    // TODO: connect to meta service and set attributes.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, perm = %args.perm, "meta set-permission executed");

    Ok(table)
}

/// Execute `meta chdir`.
///
/// Updates the `AdminEnv` current directory. In full implementation this
/// would stat the target path to get the inode ID.
async fn execute_chdir(env: &mut AdminEnv, args: &ChangeDir) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();

    // TODO: stat the path to get inode ID, then update env.
    // For now, just update the path string.
    let new_path = if args.path.starts_with('/') {
        args.path.clone()
    } else {
        format!(
            "{}/{}",
            env.current_dir.trim_end_matches('/'),
            args.path
        )
    };
    env.set_current_dir(env.current_dir_id, new_path.clone());

    table.push(kv_row("CurrentDir", &new_path));

    tracing::debug!(path = %args.path, "meta chdir executed");

    Ok(table)
}

/// Execute `meta get-real-path`.
async fn execute_get_real_path(
    _env: &AdminEnv,
    args: &GetRealPath,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));

    // TODO: connect to meta service and resolve path.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, "meta get-real-path executed");

    Ok(table)
}

/// Execute `meta set-layout`.
async fn execute_set_layout(_env: &AdminEnv, args: &SetLayout) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));

    if let Some(ct) = args.chain_table {
        table.push(kv_row("ChainTable", ct));
    }
    if let Some(ss) = args.stripe_size {
        table.push(kv_row("StripeSize", ss));
    }
    if let Some(ns) = args.num_stripes {
        table.push(kv_row("NumStripes", ns));
    }

    // TODO: connect to meta service and set layout.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, "meta set-layout executed");

    Ok(table)
}

/// Execute `meta read-file`.
async fn execute_read_file(_env: &AdminEnv, args: &ReadFile) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));
    table.push(kv_row("Offset", args.offset));
    table.push(kv_row("Length", if args.length == 0 { "all".to_string() } else { args.length.to_string() }));

    if let Some(ref output) = args.output {
        table.push(kv_row("Output", output));
    }

    // TODO: connect to storage service and read file data.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, "meta read-file executed");

    Ok(table)
}

/// Execute `meta write-file`.
async fn execute_write_file(_env: &AdminEnv, args: &WriteFile) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));
    table.push(kv_row("Offset", args.offset));

    if let Some(ref input) = args.input {
        table.push(kv_row("Input", input));
    }

    // TODO: connect to storage service and write file data.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, "meta write-file executed");

    Ok(table)
}

/// Execute `meta scan-tree`.
async fn execute_scan_tree(_env: &AdminEnv, args: &ScanTree) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));
    table.push(kv_row(
        "MaxDepth",
        if args.max_depth == 0 {
            "unlimited".to_string()
        } else {
            args.max_depth.to_string()
        },
    ));

    // TODO: recursively scan directory tree.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, max_depth = args.max_depth, "meta scan-tree executed");

    Ok(table)
}

/// Execute `meta dump-inodes`.
async fn execute_dump_inodes(
    _env: &AdminEnv,
    args: &DumpInodes,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();

    if let Some(start) = args.start {
        table.push(kv_row("Start", format!("0x{:x}", start)));
    }
    if let Some(limit) = args.limit {
        table.push(kv_row("Limit", limit));
    }

    // TODO: connect to KV engine and dump inodes.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!("meta dump-inodes executed");

    Ok(table)
}

/// Execute `meta dump-dir-entries`.
async fn execute_dump_dir_entries(
    _env: &AdminEnv,
    args: &DumpDirEntries,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row(
        "ParentInode",
        format!("0x{:x}", args.parent_inode),
    ));

    // TODO: connect to KV engine and dump directory entries.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(parent_inode = args.parent_inode, "meta dump-dir-entries executed");

    Ok(table)
}

/// Execute `meta dump-session`.
async fn execute_dump_session(
    _env: &AdminEnv,
    args: &DumpSession,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();

    if let Some(ref client_id) = args.client_id {
        table.push(kv_row("ClientId", client_id));
    }

    // TODO: connect to meta service and dump session info.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!("meta dump-session executed");

    Ok(table)
}

/// Execute `meta prune-session`.
async fn execute_prune_session(
    _env: &AdminEnv,
    args: &PruneSession,
) -> anyhow::Result<OutputTable> {
    let mut table = OutputTable::new();
    table.push(kv_row("ClientId", &args.client_id));

    if let Some(ref sessions) = args.sessions {
        table.push(kv_row("Sessions", sessions));
    }

    // TODO: connect to meta service and prune sessions.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(client_id = %args.client_id, "meta prune-session executed");

    Ok(table)
}

/// Execute `meta recursive-chown`.
async fn execute_recursive_chown(
    _env: &AdminEnv,
    args: &RecursiveChown,
) -> anyhow::Result<OutputTable> {
    if args.uid.is_none() && args.gid.is_none() {
        anyhow::bail!("must specify at least one of --uid or --gid");
    }

    let mut table = OutputTable::new();
    table.push(kv_row("Path", &args.path));

    if let Some(uid) = args.uid {
        table.push(kv_row("Uid", uid));
    }
    if let Some(gid) = args.gid {
        table.push(kv_row("Gid", gid));
    }

    // TODO: recursively change ownership.
    table.push(kv_row("Status", "not yet implemented (client crate is stub)"));

    tracing::debug!(path = %args.path, uid = ?args.uid, gid = ?args.gid, "meta recursive-chown executed");

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
    async fn test_stat_by_path() {
        let env = test_env();
        let args = StatPath {
            path: "/data/test.txt".to_string(),
            follow_link: false,
            display_layout: false,
            inode: false,
        };
        let result = execute_stat(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table[0][0], "Path");
        assert_eq!(table[0][1], "/data/test.txt");
    }

    #[tokio::test]
    async fn test_stat_by_inode() {
        let env = test_env();
        let args = StatPath {
            path: "0xff".to_string(),
            follow_link: false,
            display_layout: false,
            inode: true,
        };
        let result = execute_stat(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table[0][0], "InodeId");
        assert_eq!(table[0][1], "0xff");
    }

    #[tokio::test]
    async fn test_stat_by_inode_decimal() {
        let env = test_env();
        let args = StatPath {
            path: "42".to_string(),
            follow_link: false,
            display_layout: false,
            inode: true,
        };
        let result = execute_stat(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_stat_by_inode_invalid() {
        let env = test_env();
        let args = StatPath {
            path: "not_a_number".to_string(),
            follow_link: false,
            display_layout: false,
            inode: true,
        };
        let result = execute_stat(&env, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ls_default() {
        let env = test_env();
        let args = ListDir {
            path: ".".to_string(),
            limit: None,
            status: false,
        };
        let result = execute_ls(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table[0], vec!["Name", "Type", "InodeId"]);
    }

    #[tokio::test]
    async fn test_ls_with_status() {
        let env = test_env();
        let args = ListDir {
            path: "/".to_string(),
            limit: Some(10),
            status: true,
        };
        let result = execute_ls(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table[0].len(), 6); // Name, Type, InodeId, Mtime, Size, Perm
    }

    #[tokio::test]
    async fn test_mkdir() {
        let env = test_env();
        let args = MakeDir {
            path: "/data/new_dir".to_string(),
            recursive: true,
            perm: Some("0755".to_string()),
        };
        let result = execute_mkdir(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        let perm_row = table.iter().find(|r| r[0] == "Permission").unwrap();
        assert_eq!(perm_row[1], "0755");
    }

    #[tokio::test]
    async fn test_mkdir_invalid_perm() {
        let env = test_env();
        let args = MakeDir {
            path: "/test".to_string(),
            recursive: false,
            perm: Some("xyz".to_string()),
        };
        let result = execute_mkdir(&env, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_rm() {
        let env = test_env();
        let args = Remove {
            path: "/data/old.txt".to_string(),
            recursive: false,
        };
        let result = execute_rm(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rename() {
        let env = test_env();
        let args = RenamePath {
            src: "/data/old.txt".to_string(),
            dest: "/data/new.txt".to_string(),
        };
        let result = execute_rename(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_symlink() {
        let env = test_env();
        let args = CreateSymlink {
            path: "/data/link".to_string(),
            target: "/data/target".to_string(),
        };
        let result = execute_symlink(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_permission() {
        let env = test_env();
        let args = SetPermission {
            path: "/data/file.txt".to_string(),
            perm: "0644".to_string(),
            uid: Some(1000),
            gid: None,
        };
        let result = execute_set_permission(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_set_permission_invalid() {
        let env = test_env();
        let args = SetPermission {
            path: "/data/file.txt".to_string(),
            perm: "zzz".to_string(),
            uid: None,
            gid: None,
        };
        let result = execute_set_permission(&env, &args).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_chdir() {
        let mut env = test_env();
        assert_eq!(env.current_dir, "/");

        let args = ChangeDir {
            path: "/data".to_string(),
        };
        let result = execute_chdir(&mut env, &args).await;
        assert!(result.is_ok());
        assert_eq!(env.current_dir, "/data");
    }

    #[tokio::test]
    async fn test_chdir_relative() {
        let mut env = test_env();
        env.set_current_dir(1, "/data".to_string());

        let args = ChangeDir {
            path: "subdir".to_string(),
        };
        let result = execute_chdir(&mut env, &args).await;
        assert!(result.is_ok());
        assert_eq!(env.current_dir, "/data/subdir");
    }

    #[tokio::test]
    async fn test_recursive_chown_no_uid_gid() {
        let env = test_env();
        let args = RecursiveChown {
            path: "/data".to_string(),
            uid: None,
            gid: None,
        };
        let result = execute_recursive_chown(&env, &args).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must specify"));
    }

    #[tokio::test]
    async fn test_recursive_chown() {
        let env = test_env();
        let args = RecursiveChown {
            path: "/data".to_string(),
            uid: Some(1000),
            gid: Some(100),
        };
        let result = execute_recursive_chown(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_file() {
        let env = test_env();
        let args = CreateFile {
            path: "/data/new.txt".to_string(),
            perm: None,
        };
        let result = execute_create(&env, &args).await;
        assert!(result.is_ok());
        let table = result.unwrap();
        let perm_row = table.iter().find(|r| r[0] == "Permission").unwrap();
        assert_eq!(perm_row[1], "0644"); // Default
    }

    #[tokio::test]
    async fn test_scan_tree() {
        let env = test_env();
        let args = ScanTree {
            path: "/".to_string(),
            max_depth: 3,
        };
        let result = execute_scan_tree(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_dump_inodes() {
        let env = test_env();
        let args = DumpInodes {
            start: Some(1),
            limit: Some(100),
        };
        let result = execute_dump_inodes(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_dump_dir_entries() {
        let env = test_env();
        let args = DumpDirEntries { parent_inode: 1 };
        let result = execute_dump_dir_entries(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_real_path() {
        let env = test_env();
        let args = GetRealPath {
            path: "/data/../data/file.txt".to_string(),
        };
        let result = execute_get_real_path(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_read_file() {
        let env = test_env();
        let args = ReadFile {
            path: "/data/test.txt".to_string(),
            output: None,
            offset: 0,
            length: 0,
        };
        let result = execute_read_file(&env, &args).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_write_file() {
        let env = test_env();
        let args = WriteFile {
            path: "/data/test.txt".to_string(),
            input: None,
            offset: 0,
        };
        let result = execute_write_file(&env, &args).await;
        assert!(result.is_ok());
    }
}
