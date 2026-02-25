//! Main FUSE filesystem implementation.
//!
//! `FuseFileSystem` is the primary struct that implements the `FuseOps` trait.
//! It holds the client connection (via `Arc<dyn FuseClient>`), inode table,
//! handle table, and configuration.
//!
//! This mirrors the C++ `FuseClients` struct and the operation
//! implementations in `FuseOps.cc`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::Mutex;
use tracing::{debug, info};

use hf3fs_types::{Gid, InodeId, Uid};

use crate::config::FuseConfig;
use crate::inode::*;
use crate::ops::*;
use crate::reply::*;
use crate::types::*;

// ── Client trait ────────────────────────────────────────────────────────────

/// Abstraction over the meta/storage client layer.
///
/// The FUSE filesystem delegates all metadata and data operations to this
/// trait. In production it will be backed by the real `MetaClient` and
/// `StorageClient`; in tests it can be mocked.
#[async_trait::async_trait]
pub trait FuseClient: Send + Sync + 'static {
    /// Look up a child entry in a directory.
    async fn stat(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        parent: InodeId,
        name: &str,
    ) -> Result<Inode, FuseError>;

    /// Get inode attributes by ID.
    async fn stat_by_id(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        ino: InodeId,
    ) -> Result<Inode, FuseError>;

    /// Create a regular file.
    async fn create(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        parent: InodeId,
        name: &str,
        perm: u16,
        flags: i32,
        session: Option<u64>,
    ) -> Result<Inode, FuseError>;

    /// Create a directory.
    async fn mkdirs(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        parent: InodeId,
        name: &str,
        perm: u16,
        recursive: bool,
    ) -> Result<Inode, FuseError>;

    /// Remove a file or directory entry.
    async fn remove(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        parent: InodeId,
        name: &str,
        recursive: bool,
    ) -> Result<(), FuseError>;

    /// Rename an entry.
    async fn rename(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        parent: InodeId,
        name: &str,
        new_parent: InodeId,
        new_name: &str,
    ) -> Result<Inode, FuseError>;

    /// Create a hard link.
    async fn hard_link(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        ino: InodeId,
        new_parent: InodeId,
        new_name: &str,
    ) -> Result<Inode, FuseError>;

    /// Create a symbolic link.
    async fn symlink(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        parent: InodeId,
        name: &str,
        target: &str,
    ) -> Result<Inode, FuseError>;

    /// Open a file for writing (establishes a session).
    async fn open(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        ino: InodeId,
        session: u64,
        flags: i32,
    ) -> Result<Inode, FuseError>;

    /// Close a file (ends a write session).
    async fn close(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        ino: InodeId,
        session: u64,
        update_length: bool,
    ) -> Result<Inode, FuseError>;

    /// Sync file metadata (length, timestamps).
    async fn sync(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        ino: InodeId,
        update_length: bool,
    ) -> Result<Inode, FuseError>;

    /// Truncate a file to the given length.
    async fn truncate(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        ino: InodeId,
        length: u64,
    ) -> Result<Inode, FuseError>;

    /// Set permissions/ownership.
    async fn set_permission(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        ino: InodeId,
        owner_uid: Option<Uid>,
        owner_gid: Option<Gid>,
        perm: Option<u16>,
    ) -> Result<Inode, FuseError>;

    /// Set access and modification times.
    async fn utimes(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        ino: InodeId,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> Result<Inode, FuseError>;

    /// Read data from a file.
    async fn read(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        inode: &Inode,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, FuseError>;

    /// Write data to a file.
    async fn write(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        inode: &Inode,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, FuseError>;

    /// Read directory entries.
    async fn readdir(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
        ino: InodeId,
    ) -> Result<Vec<DirEntry>, FuseError>;

    /// Get filesystem statistics.
    async fn statfs(
        &self,
        uid: Uid,
        gid: Gid,
        token: &str,
    ) -> Result<FsStats, FuseError>;
}

/// A single directory entry from the metadata server.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Inode ID of the entry.
    pub ino: InodeId,
    /// Entry name.
    pub name: String,
    /// Inode type.
    pub inode_type: InodeType,
}

/// Filesystem capacity statistics.
#[derive(Debug, Clone, Default)]
pub struct FsStats {
    /// Total capacity in bytes.
    pub capacity: u64,
    /// Free space in bytes.
    pub free: u64,
}

/// Error type for FuseClient operations.
#[derive(Debug, Clone)]
pub struct FuseError {
    /// The errno value to return to FUSE.
    pub errno: i32,
    /// A human-readable error message.
    pub message: String,
}

impl FuseError {
    pub fn new(errno: i32, message: impl Into<String>) -> Self {
        Self {
            errno,
            message: message.into(),
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(libc::ENOENT, msg)
    }

    pub fn permission_denied(msg: impl Into<String>) -> Self {
        Self::new(libc::EACCES, msg)
    }

    pub fn read_only() -> Self {
        Self::new(libc::EROFS, "read-only filesystem")
    }

    pub fn not_implemented() -> Self {
        Self::new(libc::ENOSYS, "not implemented")
    }

    pub fn io_error(msg: impl Into<String>) -> Self {
        Self::new(libc::EIO, msg)
    }
}

impl std::fmt::Display for FuseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FuseError(errno={}, {})", self.errno, self.message)
    }
}

impl std::error::Error for FuseError {}

// ── UserInfo helper ─────────────────────────────────────────────────────────

/// User context for meta client calls.
#[derive(Debug, Clone)]
struct UserInfo {
    uid: Uid,
    gid: Gid,
    token: String,
}

impl UserInfo {
    fn from_ctx(ctx: &FuseRequestContext, token: &str) -> Self {
        Self {
            uid: ctx.uid,
            gid: ctx.gid,
            token: token.to_string(),
        }
    }
}

// ── SyncType ────────────────────────────────────────────────────────────────

/// The context in which a sync is triggered, affecting behavior.
///
/// Mirrors the C++ `SyncType` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncType {
    Lookup,
    GetAttr,
    PeriodSync,
    Fsync,
    ForceFsync,
}

// ── FuseFileSystem ──────────────────────────────────────────────────────────

/// The main FUSE filesystem implementation.
///
/// Holds the client connection, configuration, and all in-memory state
/// (inode table, handle table, dirty inode set).
pub struct FuseFileSystem {
    /// Client for metadata and storage operations.
    client: Arc<dyn FuseClient>,

    /// Current configuration.
    config: Arc<FuseConfig>,

    /// Authentication token.
    token: String,

    /// Mount point path.
    mountpoint: PathBuf,

    /// Optional remount prefix for bind mounts.
    #[allow(dead_code)]
    remount_prefix: Option<PathBuf>,

    /// Reference-counted inode table.
    inode_table: InodeTable,

    /// File and directory handle table.
    handle_table: HandleTable,

    /// Monotonically increasing directory handle counter.
    dir_handle_counter: AtomicU64,

    /// Set of inode IDs with un-synced writes (for periodic sync).
    dirty_inodes: Mutex<HashSet<InodeId>>,

    /// Whether the kernel supports writeback cache.
    writeback_cache_enabled: AtomicBool,
}

impl FuseFileSystem {
    /// Create a new FUSE filesystem.
    ///
    /// # Arguments
    ///
    /// * `client` - The meta/storage client to delegate operations to.
    /// * `config` - FUSE configuration.
    /// * `token` - Authentication token string.
    pub fn new(
        client: Arc<dyn FuseClient>,
        config: FuseConfig,
        token: String,
    ) -> Self {
        let mountpoint = PathBuf::from(&config.mountpoint);
        let remount_prefix = config.remount_prefix_opt().map(PathBuf::from);

        Self {
            client,
            config: Arc::new(config),
            token,
            mountpoint,
            remount_prefix,
            inode_table: InodeTable::new(),
            handle_table: HandleTable::new(),
            dir_handle_counter: AtomicU64::new(0),
            dirty_inodes: Mutex::new(HashSet::new()),
            writeback_cache_enabled: AtomicBool::new(false),
        }
    }

    /// Returns the current configuration.
    pub fn config(&self) -> &FuseConfig {
        &self.config
    }

    /// Returns the mount point path.
    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }

    /// Returns the number of tracked inodes.
    pub fn inode_count(&self) -> usize {
        self.inode_table.len()
    }

    /// Returns the number of dirty inodes pending sync.
    pub fn dirty_inode_count(&self) -> usize {
        self.dirty_inodes.lock().len()
    }

    /// Mark an inode as dirty (has un-synced writes).
    pub fn mark_dirty(&self, id: InodeId) {
        self.dirty_inodes.lock().insert(id);
    }

    /// Remove an inode from the dirty set.
    pub fn clear_dirty(&self, id: InodeId) {
        self.dirty_inodes.lock().remove(&id);
    }

    /// Fill a FileAttr from an Inode.
    fn fill_attr(&self, inode: &Inode) -> FileAttr {
        let ino = inode_id_to_fuse_ino(inode.id);
        let file_type = match inode.inode_type() {
            InodeType::File => S_IFREG,
            InodeType::Directory => S_IFDIR,
            InodeType::Symlink => S_IFLNK,
        };
        let size = inode.file_length();
        let blocks = if inode.is_file() {
            (size + 511) / 512
        } else {
            0
        };
        let blksize = if inode.is_symlink() {
            0
        } else {
            inode.chunk_size()
        };

        FileAttr {
            ino,
            size,
            blocks,
            atime: inode.atime,
            mtime: inode.mtime,
            ctime: inode.ctime,
            mode: (inode.acl.perm as u32 & 0o7777) | file_type,
            nlink: inode.nlink,
            uid: inode.acl.uid,
            gid: inode.acl.gid,
            rdev: 0,
            blksize,
        }
    }

    /// Create a FuseEntryParam from an Inode using current config timeouts.
    fn make_entry(&self, inode: &Inode) -> FuseEntryParam {
        let attr = self.fill_attr(inode);
        let timeout = if inode.is_symlink() {
            self.config.symlink_timeout_duration()
        } else {
            self.config.attr_timeout_duration()
        };
        let entry_timeout = if inode.is_symlink() {
            self.config.symlink_timeout_duration()
        } else {
            self.config.entry_timeout_duration()
        };
        FuseEntryParam {
            ino: attr.ino,
            generation: 0,
            attr,
            attr_timeout: timeout,
            entry_timeout,
        }
    }

    /// Register an inode from a lookup/create result.
    fn add_inode_entry(&self, inode: &Inode) -> Arc<RcInode> {
        self.inode_table.add_entry(inode.clone())
    }

    /// Get a user info from a request context.
    fn user_info(&self, ctx: &FuseRequestContext) -> UserInfo {
        UserInfo::from_ctx(ctx, &self.token)
    }

    /// Check if the filesystem is read-only and return EROFS if so.
    fn check_readonly(&self) -> FuseResult<()> {
        if self.config.readonly {
            Err(libc::EROFS)
        } else {
            Ok(())
        }
    }

    /// Get the RcInode for an inode number, optionally using a file handle.
    fn get_rcinode(&self, ino: u64, fh: Option<u64>) -> Option<Arc<RcInode>> {
        // Try file handle first
        if let Some(fh_id) = fh {
            if let Some(handle) = self.handle_table.get_file(fh_id) {
                return Some(handle.rcinode.clone());
            }
        }
        // Fall back to inode table
        let inode_id = fuse_ino_to_inode_id(ino);
        self.inode_table.get(inode_id)
    }

    /// Generate a random session ID for write-opened files.
    fn random_session_id() -> u64 {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        let s = RandomState::new();
        let mut h = s.build_hasher();
        h.write_u64(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64);
        h.finish()
    }
}

#[async_trait::async_trait]
impl FuseOps for FuseFileSystem {
    async fn init(&self, conn_info: &mut FuseConnInfo) -> FuseResult<()> {
        info!("hf3fs_init()");

        // Negotiate capabilities
        let max_buf = self.config.io_bufs.max_buf_size as u32;
        conn_info.max_readahead = max_buf;
        conn_info.max_read = max_buf;
        conn_info.max_write = max_buf;
        conn_info.max_background = self.config.max_background;
        conn_info.time_gran = (self.config.time_granularity_us * 1000) as u32;

        if self.config.enable_writeback_cache {
            info!("FUSE writeback cache: ON");
            self.writeback_cache_enabled.store(true, Ordering::Relaxed);
        }

        Ok(())
    }

    async fn destroy(&self) {
        info!("hf3fs_destroy()");
    }

    async fn lookup(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
    ) -> FuseResult<ReplyEntry> {
        let parent_id = fuse_ino_to_inode_id(parent);
        debug!(parent = %parent_id, name, pid = ctx.pid, "lookup");

        let user = self.user_info(&ctx);
        let result = self
            .client
            .stat(user.uid, user.gid, &user.token, parent_id, name)
            .await
            .map_err(|e| e.errno)?;

        let entry = self.make_entry(&result);
        self.add_inode_entry(&result);

        Ok(ReplyEntry { entry })
    }

    async fn forget(&self, ino: u64, nlookup: u64) {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, nlookup, "forget");
        self.inode_table.forget(inode_id, nlookup);
    }

    async fn getattr(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: Option<u64>,
    ) -> FuseResult<ReplyAttr> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, pid = ctx.pid, "getattr");

        let user = self.user_info(&ctx);
        let result = self
            .client
            .stat_by_id(user.uid, user.gid, &user.token, inode_id)
            .await
            .map_err(|e| e.errno)?;

        let attr = self.fill_attr(&result);

        // Update cached inode if present
        if let Some(rc) = self.get_rcinode(ino, fh) {
            rc.update(&result, 0, false);
        }

        Ok(ReplyAttr {
            attr,
            attr_timeout: self.config.attr_timeout_duration(),
        })
    }

    async fn setattr(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        attrs: &SetAttrValues,
        _fh: Option<u64>,
    ) -> FuseResult<ReplyAttr> {
        self.check_readonly()?;
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, pid = ctx.pid, "setattr");

        let user = self.user_info(&ctx);
        let mut last_inode: Option<Inode> = None;

        // Handle permission/ownership changes
        if attrs.mode.is_some() || attrs.uid.is_some() || attrs.gid.is_some() {
            let result = self
                .client
                .set_permission(
                    user.uid,
                    user.gid,
                    &user.token,
                    inode_id,
                    attrs.uid.map(Uid),
                    attrs.gid.map(Gid),
                    attrs.mode.map(|m| (m & 0o7777) as u16),
                )
                .await
                .map_err(|e| e.errno)?;
            last_inode = Some(result);
        }

        // Handle truncation
        if let Some(size) = attrs.size {
            let result = self
                .client
                .truncate(user.uid, user.gid, &user.token, inode_id, size)
                .await
                .map_err(|e| e.errno)?;
            last_inode = Some(result);
        }

        // Handle time changes
        if attrs.atime.is_some() || attrs.mtime.is_some() {
            let atime = attrs.atime.as_ref().map(|t| match t {
                SetAttrTime::Now => SystemTime::now(),
                SetAttrTime::Specific(t) => *t,
            });
            let mtime = attrs.mtime.as_ref().map(|t| match t {
                SetAttrTime::Now => SystemTime::now(),
                SetAttrTime::Specific(t) => *t,
            });
            let result = self
                .client
                .utimes(user.uid, user.gid, &user.token, inode_id, atime, mtime)
                .await
                .map_err(|e| e.errno)?;
            last_inode = Some(result);
        }

        // If we got a result, use it; otherwise re-fetch
        let inode = match last_inode {
            Some(inode) => inode,
            None => {
                // No attributes were set; just return current attributes
                self.client
                    .stat_by_id(user.uid, user.gid, &user.token, inode_id)
                    .await
                    .map_err(|e| e.errno)?
            }
        };

        let attr = self.fill_attr(&inode);
        Ok(ReplyAttr {
            attr,
            attr_timeout: self.config.attr_timeout_duration(),
        })
    }

    async fn readlink(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
    ) -> FuseResult<ReplyReadlink> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, pid = ctx.pid, "readlink");

        // Try cached inode first
        if let Some(rc) = self.inode_table.get(inode_id) {
            if let Some(target) = rc.inode.symlink_target() {
                return Ok(ReplyReadlink {
                    target: target.to_string(),
                });
            }
        }

        // Fall back to server
        let user = self.user_info(&ctx);
        let result = self
            .client
            .stat_by_id(user.uid, user.gid, &user.token, inode_id)
            .await
            .map_err(|e| e.errno)?;

        match result.symlink_target() {
            Some(target) => Ok(ReplyReadlink {
                target: target.to_string(),
            }),
            None => Err(libc::EINVAL),
        }
    }

    async fn mknod(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        mode: u32,
        _rdev: u32,
    ) -> FuseResult<ReplyEntry> {
        self.check_readonly()?;
        let parent_id = fuse_ino_to_inode_id(parent);
        debug!(parent = %parent_id, name, mode, pid = ctx.pid, "mknod");

        // Only support regular files, directories, symlinks
        if (mode & S_IFREG == 0) && (mode & S_IFDIR == 0) && (mode & S_IFLNK == 0) {
            return Err(libc::ENOSYS);
        }

        let user = self.user_info(&ctx);
        let perm = (mode & 0o7777) as u16;
        let result = self
            .client
            .create(
                user.uid,
                user.gid,
                &user.token,
                parent_id,
                name,
                perm,
                libc::O_RDONLY,
                None,
            )
            .await
            .map_err(|e| e.errno)?;

        let entry = self.make_entry(&result);
        self.add_inode_entry(&result);

        Ok(ReplyEntry { entry })
    }

    async fn mkdir(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        mode: u32,
    ) -> FuseResult<ReplyEntry> {
        self.check_readonly()?;
        let parent_id = fuse_ino_to_inode_id(parent);
        debug!(parent = %parent_id, name, mode, pid = ctx.pid, "mkdir");

        let user = self.user_info(&ctx);
        let perm = (mode & 0o7777) as u16;
        let result = self
            .client
            .mkdirs(user.uid, user.gid, &user.token, parent_id, name, perm, false)
            .await
            .map_err(|e| e.errno)?;

        let entry = self.make_entry(&result);
        self.add_inode_entry(&result);

        Ok(ReplyEntry { entry })
    }

    async fn unlink(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
    ) -> FuseResult<()> {
        self.check_readonly()?;
        let parent_id = fuse_ino_to_inode_id(parent);
        debug!(parent = %parent_id, name, pid = ctx.pid, "unlink");

        let user = self.user_info(&ctx);
        self.client
            .remove(user.uid, user.gid, &user.token, parent_id, name, false)
            .await
            .map_err(|e| e.errno)?;

        Ok(())
    }

    async fn rmdir(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
    ) -> FuseResult<()> {
        self.check_readonly()?;
        let parent_id = fuse_ino_to_inode_id(parent);
        debug!(parent = %parent_id, name, pid = ctx.pid, "rmdir");

        let user = self.user_info(&ctx);
        self.client
            .remove(user.uid, user.gid, &user.token, parent_id, name, false)
            .await
            .map_err(|e| e.errno)?;

        Ok(())
    }

    async fn symlink(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        link: &str,
    ) -> FuseResult<ReplyEntry> {
        self.check_readonly()?;
        let parent_id = fuse_ino_to_inode_id(parent);
        debug!(parent = %parent_id, name, link, pid = ctx.pid, "symlink");

        let user = self.user_info(&ctx);
        let result = self
            .client
            .symlink(user.uid, user.gid, &user.token, parent_id, name, link)
            .await
            .map_err(|e| e.errno)?;

        let entry = self.make_entry(&result);
        self.add_inode_entry(&result);

        Ok(ReplyEntry { entry })
    }

    async fn rename(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
        flags: u32,
    ) -> FuseResult<()> {
        self.check_readonly()?;
        let parent_id = fuse_ino_to_inode_id(parent);
        let new_parent_id = fuse_ino_to_inode_id(new_parent);
        debug!(
            parent = %parent_id, name, new_parent = %new_parent_id,
            new_name, flags, pid = ctx.pid, "rename"
        );

        if flags != 0 {
            return Err(libc::ENOSYS);
        }

        let user = self.user_info(&ctx);
        self.client
            .rename(
                user.uid,
                user.gid,
                &user.token,
                parent_id,
                name,
                new_parent_id,
                new_name,
            )
            .await
            .map_err(|e| e.errno)?;

        Ok(())
    }

    async fn link(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        new_parent: u64,
        new_name: &str,
    ) -> FuseResult<ReplyEntry> {
        self.check_readonly()?;
        let inode_id = fuse_ino_to_inode_id(ino);
        let new_parent_id = fuse_ino_to_inode_id(new_parent);
        debug!(
            ino = %inode_id, new_parent = %new_parent_id,
            new_name, pid = ctx.pid, "link"
        );

        let user = self.user_info(&ctx);
        let result = self
            .client
            .hard_link(
                user.uid,
                user.gid,
                &user.token,
                inode_id,
                new_parent_id,
                new_name,
            )
            .await
            .map_err(|e| e.errno)?;

        let entry = self.make_entry(&result);
        self.add_inode_entry(&result);

        Ok(ReplyEntry { entry })
    }

    async fn open(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        flags: i32,
    ) -> FuseResult<ReplyOpen> {
        let inode_id = fuse_ino_to_inode_id(ino);
        let parsed_flags = OpenFlags::from_raw(flags);
        debug!(ino = %inode_id, pid = ctx.pid, "open");

        // Check readonly for write operations
        if parsed_flags.is_writable() || parsed_flags.create || parsed_flags.truncate || parsed_flags.append {
            self.check_readonly()?;
        }

        let rc = self.inode_table.get(inode_id).ok_or(libc::ENOENT)?;

        // Open a write session if needed
        let session_id = if parsed_flags.is_writable() {
            let session = Self::random_session_id();
            let user = self.user_info(&ctx);
            self.client
                .open(user.uid, user.gid, &user.token, inode_id, session, flags)
                .await
                .map_err(|e| e.errno)?;
            session
        } else {
            0
        };

        // Determine direct_io mode
        let direct_io = parsed_flags.direct
            || (!self.config.enable_read_cache && !parsed_flags.nonblock);

        let file_handle = FileHandle {
            rcinode: rc,
            direct_io,
            session_id,
            flags,
        };

        let fh = self.handle_table.insert_file(file_handle);

        let reply_flags = if direct_io { 1 } else { 0 }; // FOPEN_DIRECT_IO = 1

        Ok(ReplyOpen {
            fh,
            flags: reply_flags,
        })
    }

    async fn read(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, size, offset, pid = ctx.pid, "read");

        let rc = self.get_rcinode(ino, Some(fh)).ok_or(libc::ENOENT)?;

        // Update atime
        rc.dynamic_attr.write().atime = Some(SystemTime::now());

        let user = self.user_info(&ctx);

        if self.config.dryrun_bench_mode {
            return Ok(ReplyData {
                data: vec![0u8; size as usize],
            });
        }

        let data = self
            .client
            .read(
                user.uid,
                user.gid,
                &user.token,
                &rc.inode,
                offset as u64,
                size,
            )
            .await
            .map_err(|e| e.errno)?;

        Ok(ReplyData { data })
    }

    async fn write(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _flags: i32,
    ) -> FuseResult<ReplyWrite> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, size = data.len(), offset, pid = ctx.pid, "write");

        self.check_readonly()?;

        if self.config.dryrun_bench_mode {
            return Ok(ReplyWrite {
                written: data.len() as u32,
            });
        }

        let rc = self.get_rcinode(ino, Some(fh)).ok_or(libc::ENOENT)?;

        // Update mtime
        rc.dynamic_attr.write().mtime = Some(SystemTime::now());

        let user = self.user_info(&ctx);
        let written = self
            .client
            .write(
                user.uid,
                user.gid,
                &user.token,
                &rc.inode,
                offset as u64,
                data,
            )
            .await
            .map_err(|e| e.errno)?;

        // Track dirty writes
        {
            let mut dyn_attr = rc.dynamic_attr.write();
            dyn_attr.written += 1;
        }
        self.mark_dirty(inode_id);

        Ok(ReplyWrite { written })
    }

    async fn flush(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
    ) -> FuseResult<()> {
        // Flush delegates to fsync in the C++ implementation
        self.fsync(ctx, ino, fh, false).await
    }

    async fn release(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        _flags: i32,
    ) -> FuseResult<()> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, pid = ctx.pid, "release");

        self.clear_dirty(inode_id);

        let handle = self.handle_table.remove_file(fh);
        if let Some(handle) = handle {
            let parsed = OpenFlags::from_raw(handle.flags);
            if parsed.is_writable() && handle.session_id != 0 {
                let user = self.user_info(&ctx);
                let has_dirty = handle.rcinode.dynamic_attr.read().has_unfsynced_writes();
                self.client
                    .close(
                        user.uid,
                        user.gid,
                        &user.token,
                        inode_id,
                        handle.session_id,
                        has_dirty,
                    )
                    .await
                    .map_err(|e| e.errno)?;
            }
        }

        Ok(())
    }

    async fn fsync(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        datasync: bool,
    ) -> FuseResult<()> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, datasync, pid = ctx.pid, "fsync");

        let rc = match self.get_rcinode(ino, Some(fh)) {
            Some(rc) if rc.inode.is_file() => rc,
            _ => return Ok(()),
        };

        let user = self.user_info(&ctx);
        let flush_only = datasync && !self.config.fdatasync_update_length;

        if !flush_only {
            self.clear_dirty(inode_id);
            let has_dirty = rc.dynamic_attr.read().has_dirty_writes();
            if has_dirty {
                let result = self
                    .client
                    .sync(user.uid, user.gid, &user.token, inode_id, true)
                    .await
                    .map_err(|e| e.errno)?;

                let written = rc.dynamic_attr.read().written;
                rc.update(&result, written, true);
            }
        }

        Ok(())
    }

    async fn opendir(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
    ) -> FuseResult<ReplyOpen> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, pid = ctx.pid, "opendir");

        let dir_id = self.dir_handle_counter.fetch_add(1, Ordering::Relaxed);
        let dir_handle = DirHandle {
            dir_id,
            pid: ctx.pid,
            iov_dir: false,
        };

        let fh = self.handle_table.insert_dir(dir_handle);
        Ok(ReplyOpen { fh, flags: 0 })
    }

    async fn readdir(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        _fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, offset, pid = ctx.pid, "readdir");

        let user = self.user_info(&ctx);
        let entries = self
            .client
            .readdir(user.uid, user.gid, &user.token, inode_id)
            .await
            .map_err(|e| e.errno)?;

        let fuse_entries: Vec<FuseDirEntry> = entries
            .into_iter()
            .enumerate()
            .skip(offset as usize)
            .map(|(idx, entry)| {
                let file_type = match entry.inode_type {
                    InodeType::File => libc::DT_REG as u32,
                    InodeType::Directory => libc::DT_DIR as u32,
                    InodeType::Symlink => libc::DT_LNK as u32,
                };
                FuseDirEntry {
                    ino: inode_id_to_fuse_ino(entry.ino),
                    offset: (idx + 1) as i64,
                    file_type,
                    name: entry.name,
                }
            })
            .collect();

        Ok(ReplyDirectory {
            entries: fuse_entries,
        })
    }

    async fn readdirplus(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        _fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectoryPlus> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, offset, pid = ctx.pid, "readdirplus");

        let user = self.user_info(&ctx);
        let entries = self
            .client
            .readdir(user.uid, user.gid, &user.token, inode_id)
            .await
            .map_err(|e| e.errno)?;

        let mut result = Vec::new();
        for (idx, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            let file_type = match entry.inode_type {
                InodeType::File => libc::DT_REG as u32,
                InodeType::Directory => libc::DT_DIR as u32,
                InodeType::Symlink => libc::DT_LNK as u32,
            };

            // Try to get full attributes from the server
            let entry_param = match self
                .client
                .stat_by_id(user.uid, user.gid, &user.token, entry.ino)
                .await
            {
                Ok(inode) => {
                    self.add_inode_entry(&inode);
                    self.make_entry(&inode)
                }
                Err(_) => {
                    // Minimal entry if we can't get attributes
                    let mut attr = FileAttr::new(inode_id_to_fuse_ino(entry.ino));
                    attr.mode = match entry.inode_type {
                        InodeType::File => S_IFREG | 0o644,
                        InodeType::Directory => S_IFDIR | 0o755,
                        InodeType::Symlink => S_IFLNK | 0o777,
                    };
                    FuseEntryParam {
                        ino: attr.ino,
                        generation: 0,
                        attr,
                        attr_timeout: self.config.attr_timeout_duration(),
                        entry_timeout: self.config.entry_timeout_duration(),
                    }
                }
            };

            result.push(FuseDirEntryPlus {
                entry: FuseDirEntry {
                    ino: inode_id_to_fuse_ino(entry.ino),
                    offset: (idx + 1) as i64,
                    file_type,
                    name: entry.name,
                },
                entry_param,
            });
        }

        Ok(ReplyDirectoryPlus { entries: result })
    }

    async fn releasedir(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
    ) -> FuseResult<()> {
        let inode_id = fuse_ino_to_inode_id(ino);
        debug!(ino = %inode_id, pid = ctx.pid, "releasedir");
        self.handle_table.remove_dir(fh);
        Ok(())
    }

    async fn create(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        mode: u32,
        flags: i32,
    ) -> FuseResult<ReplyCreate> {
        self.check_readonly()?;
        let parent_id = fuse_ino_to_inode_id(parent);
        debug!(parent = %parent_id, name, mode, pid = ctx.pid, "create");

        if (mode & S_IFREG == 0) && (mode & S_IFDIR == 0) && (mode & S_IFLNK == 0) {
            return Err(libc::ENOSYS);
        }

        let session = Self::random_session_id();
        let user = self.user_info(&ctx);
        let perm = (mode & 0o7777) as u16;
        let result = self
            .client
            .create(
                user.uid,
                user.gid,
                &user.token,
                parent_id,
                name,
                perm,
                flags,
                Some(session),
            )
            .await
            .map_err(|e| e.errno)?;

        let entry = self.make_entry(&result);
        let rc = self.add_inode_entry(&result);

        let parsed_flags = OpenFlags::from_raw(flags);
        let direct_io = parsed_flags.direct
            || (!self.config.enable_read_cache && !parsed_flags.nonblock);

        let file_handle = FileHandle {
            rcinode: rc,
            direct_io,
            session_id: session,
            flags,
        };

        let fh = self.handle_table.insert_file(file_handle);
        let reply_flags = if direct_io { 1 } else { 0 };

        Ok(ReplyCreate {
            entry,
            fh,
            flags: reply_flags,
        })
    }

    async fn statfs(
        &self,
        ctx: FuseRequestContext,
        _ino: u64,
    ) -> FuseResult<ReplyStatFs> {
        debug!(pid = ctx.pid, "statfs");

        let user = self.user_info(&ctx);
        let stats = self
            .client
            .statfs(user.uid, user.gid, &user.token)
            .await
            .map_err(|e| e.errno)?;

        let bsize = 2u64 << 20; // 2 MiB, same as C++
        Ok(ReplyStatFs {
            stat: StatFs {
                bsize,
                blocks: stats.capacity / bsize,
                bfree: stats.free / bsize,
                bavail: stats.free / bsize,
                files: 0,
                ffree: 0,
                namelen: 255,
            },
        })
    }
}

impl std::fmt::Debug for FuseFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuseFileSystem")
            .field("mountpoint", &self.mountpoint)
            .field("inode_count", &self.inode_table.len())
            .field("handle_table", &self.handle_table)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::UNIX_EPOCH;

    /// A mock client for testing.
    struct MockClient {
        inodes: Mutex<HashMap<InodeId, Inode>>,
        children: Mutex<HashMap<(InodeId, String), InodeId>>,
    }

    impl MockClient {
        fn new() -> Self {
            let mut inodes = HashMap::new();
            let root = Inode {
                id: InodeId(ROOT_INODE_ID),
                acl: Acl {
                    uid: 0,
                    gid: 0,
                    perm: 0o755,
                },
                nlink: 2,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                data: InodeData::Directory {
                    parent: InodeId(ROOT_INODE_ID),
                    layout: Layout::default(),
                },
            };
            inodes.insert(root.id, root);
            Self {
                inodes: Mutex::new(inodes),
                children: Mutex::new(HashMap::new()),
            }
        }

        fn add_file(&self, parent: InodeId, name: &str, id: u64) {
            let inode = Inode {
                id: InodeId(id),
                acl: Acl {
                    uid: 1000,
                    gid: 1000,
                    perm: 0o644,
                },
                nlink: 1,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                data: InodeData::File {
                    length: 100,
                    truncate_ver: 1,
                    dyn_stripe: 1,
                    layout: Layout {
                        chunk_size: 65536,
                        stripe_size: 1,
                    },
                },
            };
            self.inodes.lock().insert(inode.id, inode);
            self.children
                .lock()
                .insert((parent, name.to_string()), InodeId(id));
        }

        fn add_dir(&self, parent: InodeId, name: &str, id: u64) {
            let inode = Inode {
                id: InodeId(id),
                acl: Acl {
                    uid: 0,
                    gid: 0,
                    perm: 0o755,
                },
                nlink: 2,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                data: InodeData::Directory {
                    parent,
                    layout: Layout::default(),
                },
            };
            self.inodes.lock().insert(inode.id, inode);
            self.children
                .lock()
                .insert((parent, name.to_string()), InodeId(id));
        }
    }

    #[async_trait::async_trait]
    impl FuseClient for MockClient {
        async fn stat(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            parent: InodeId,
            name: &str,
        ) -> Result<Inode, FuseError> {
            let children = self.children.lock();
            let id = children
                .get(&(parent, name.to_string()))
                .ok_or_else(|| FuseError::not_found(format!("{name} not found")))?;
            let inodes = self.inodes.lock();
            inodes
                .get(id)
                .cloned()
                .ok_or_else(|| FuseError::not_found("inode not found"))
        }

        async fn stat_by_id(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            ino: InodeId,
        ) -> Result<Inode, FuseError> {
            self.inodes
                .lock()
                .get(&ino)
                .cloned()
                .ok_or_else(|| FuseError::not_found("inode not found"))
        }

        async fn create(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            parent: InodeId,
            name: &str,
            perm: u16,
            _flags: i32,
            _session: Option<u64>,
        ) -> Result<Inode, FuseError> {
            let id = InodeId(1000 + self.inodes.lock().len() as u64);
            let inode = Inode {
                id,
                acl: Acl {
                    uid: 1000,
                    gid: 1000,
                    perm,
                },
                nlink: 1,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                data: InodeData::File {
                    length: 0,
                    truncate_ver: 0,
                    dyn_stripe: 1,
                    layout: Layout {
                        chunk_size: 65536,
                        stripe_size: 1,
                    },
                },
            };
            self.inodes.lock().insert(id, inode.clone());
            self.children
                .lock()
                .insert((parent, name.to_string()), id);
            Ok(inode)
        }

        async fn mkdirs(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            parent: InodeId,
            name: &str,
            perm: u16,
            _recursive: bool,
        ) -> Result<Inode, FuseError> {
            let id = InodeId(2000 + self.inodes.lock().len() as u64);
            let inode = Inode {
                id,
                acl: Acl {
                    uid: 1000,
                    gid: 1000,
                    perm,
                },
                nlink: 2,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                data: InodeData::Directory {
                    parent,
                    layout: Layout::default(),
                },
            };
            self.inodes.lock().insert(id, inode.clone());
            self.children
                .lock()
                .insert((parent, name.to_string()), id);
            Ok(inode)
        }

        async fn remove(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            parent: InodeId,
            name: &str,
            _recursive: bool,
        ) -> Result<(), FuseError> {
            let removed = self.children.lock().remove(&(parent, name.to_string()));
            if let Some(id) = removed {
                self.inodes.lock().remove(&id);
                Ok(())
            } else {
                Err(FuseError::not_found("not found"))
            }
        }

        async fn rename(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            parent: InodeId,
            name: &str,
            new_parent: InodeId,
            new_name: &str,
        ) -> Result<Inode, FuseError> {
            let mut children = self.children.lock();
            let id = children
                .remove(&(parent, name.to_string()))
                .ok_or_else(|| FuseError::not_found("not found"))?;
            children.insert((new_parent, new_name.to_string()), id);
            let inodes = self.inodes.lock();
            inodes
                .get(&id)
                .cloned()
                .ok_or_else(|| FuseError::not_found("inode not found"))
        }

        async fn hard_link(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            ino: InodeId,
            new_parent: InodeId,
            new_name: &str,
        ) -> Result<Inode, FuseError> {
            self.children
                .lock()
                .insert((new_parent, new_name.to_string()), ino);
            self.inodes
                .lock()
                .get(&ino)
                .cloned()
                .ok_or_else(|| FuseError::not_found("inode not found"))
        }

        async fn symlink(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            parent: InodeId,
            name: &str,
            target: &str,
        ) -> Result<Inode, FuseError> {
            let id = InodeId(3000 + self.inodes.lock().len() as u64);
            let inode = Inode {
                id,
                acl: Acl {
                    uid: 1000,
                    gid: 1000,
                    perm: 0o777,
                },
                nlink: 1,
                atime: SystemTime::now(),
                mtime: SystemTime::now(),
                ctime: SystemTime::now(),
                data: InodeData::Symlink {
                    target: target.to_string(),
                },
            };
            self.inodes.lock().insert(id, inode.clone());
            self.children
                .lock()
                .insert((parent, name.to_string()), id);
            Ok(inode)
        }

        async fn open(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            ino: InodeId,
            _session: u64,
            _flags: i32,
        ) -> Result<Inode, FuseError> {
            self.inodes
                .lock()
                .get(&ino)
                .cloned()
                .ok_or_else(|| FuseError::not_found("inode not found"))
        }

        async fn close(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            _ino: InodeId,
            _session: u64,
            _update_length: bool,
        ) -> Result<Inode, FuseError> {
            Ok(Inode::default())
        }

        async fn sync(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            ino: InodeId,
            _update_length: bool,
        ) -> Result<Inode, FuseError> {
            self.inodes
                .lock()
                .get(&ino)
                .cloned()
                .ok_or_else(|| FuseError::not_found("inode not found"))
        }

        async fn truncate(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            ino: InodeId,
            length: u64,
        ) -> Result<Inode, FuseError> {
            let mut inodes = self.inodes.lock();
            let inode = inodes
                .get_mut(&ino)
                .ok_or_else(|| FuseError::not_found("inode not found"))?;
            if let InodeData::File { length: ref mut l, .. } = inode.data {
                *l = length;
            }
            Ok(inode.clone())
        }

        async fn set_permission(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            ino: InodeId,
            owner_uid: Option<Uid>,
            owner_gid: Option<Gid>,
            perm: Option<u16>,
        ) -> Result<Inode, FuseError> {
            let mut inodes = self.inodes.lock();
            let inode = inodes
                .get_mut(&ino)
                .ok_or_else(|| FuseError::not_found("inode not found"))?;
            if let Some(uid) = owner_uid {
                inode.acl.uid = uid.0;
            }
            if let Some(gid) = owner_gid {
                inode.acl.gid = gid.0;
            }
            if let Some(perm) = perm {
                inode.acl.perm = perm;
            }
            Ok(inode.clone())
        }

        async fn utimes(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            ino: InodeId,
            atime: Option<SystemTime>,
            mtime: Option<SystemTime>,
        ) -> Result<Inode, FuseError> {
            let mut inodes = self.inodes.lock();
            let inode = inodes
                .get_mut(&ino)
                .ok_or_else(|| FuseError::not_found("inode not found"))?;
            if let Some(atime) = atime {
                inode.atime = atime;
            }
            if let Some(mtime) = mtime {
                inode.mtime = mtime;
            }
            Ok(inode.clone())
        }

        async fn read(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            _inode: &Inode,
            _offset: u64,
            size: u32,
        ) -> Result<Vec<u8>, FuseError> {
            Ok(vec![0u8; size as usize])
        }

        async fn write(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            _inode: &Inode,
            _offset: u64,
            data: &[u8],
        ) -> Result<u32, FuseError> {
            Ok(data.len() as u32)
        }

        async fn readdir(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
            ino: InodeId,
        ) -> Result<Vec<DirEntry>, FuseError> {
            let children: &HashMap<(InodeId, String), InodeId> = &self.children.lock();
            let inodes: &HashMap<InodeId, Inode> = &self.inodes.lock();
            let mut entries: Vec<DirEntry> = Vec::new();
            for (key, child_id) in children.iter() {
                let parent = &key.0;
                let name = &key.1;
                if *parent == ino {
                    if let Some(child) = inodes.get(child_id) {
                        entries.push(DirEntry {
                            ino: *child_id,
                            name: name.clone(),
                            inode_type: child.inode_type(),
                        });
                    }
                }
            }
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(entries)
        }

        async fn statfs(
            &self,
            _uid: Uid,
            _gid: Gid,
            _token: &str,
        ) -> Result<FsStats, FuseError> {
            Ok(FsStats {
                capacity: 1024 * 1024 * 1024 * 100, // 100 GiB
                free: 1024 * 1024 * 1024 * 50,       // 50 GiB
            })
        }
    }

    fn make_ctx() -> FuseRequestContext {
        FuseRequestContext {
            uid: Uid(1000),
            gid: Gid(1000),
            pid: 1234,
        }
    }

    fn make_fs() -> (FuseFileSystem, Arc<MockClient>) {
        let client = Arc::new(MockClient::new());
        let config = FuseConfig::default();
        let fs = FuseFileSystem::new(client.clone(), config, "test-token".to_string());
        (fs, client)
    }

    #[tokio::test]
    async fn test_init() {
        let (fs, _) = make_fs();
        let mut conn = FuseConnInfo::default();
        let result = fs.init(&mut conn).await;
        assert!(result.is_ok());
        assert_eq!(conn.max_read, fs.config().io_bufs.max_buf_size as u32);
    }

    #[tokio::test]
    async fn test_lookup_existing() {
        let (fs, client) = make_fs();
        client.add_file(InodeId(ROOT_INODE_ID), "hello.txt", 100);

        let ctx = make_ctx();
        let result = fs.lookup(ctx, FUSE_ROOT_ID, "hello.txt").await;
        assert!(result.is_ok());
        let entry = result.unwrap();
        assert_eq!(entry.entry.ino, 100);
    }

    #[tokio::test]
    async fn test_lookup_not_found() {
        let (fs, _) = make_fs();
        let ctx = make_ctx();
        let result = fs.lookup(ctx, FUSE_ROOT_ID, "nonexistent").await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), libc::ENOENT);
    }

    #[tokio::test]
    async fn test_getattr_root() {
        let (fs, _) = make_fs();
        let ctx = make_ctx();
        let result = fs.getattr(ctx, FUSE_ROOT_ID, None).await;
        assert!(result.is_ok());
        let reply = result.unwrap();
        assert_eq!(reply.attr.ino, FUSE_ROOT_ID);
        assert_ne!(reply.attr.mode & S_IFDIR, 0);
    }

    #[tokio::test]
    async fn test_mkdir_and_readdir() {
        let (fs, _) = make_fs();
        let ctx = make_ctx();

        let result = fs.mkdir(ctx, FUSE_ROOT_ID, "subdir", 0o755).await;
        assert!(result.is_ok());

        let open_result = fs.opendir(ctx, FUSE_ROOT_ID).await;
        assert!(open_result.is_ok());
        let fh = open_result.unwrap().fh;

        let readdir_result = fs.readdir(ctx, FUSE_ROOT_ID, fh, 0).await;
        assert!(readdir_result.is_ok());
        let entries = readdir_result.unwrap().entries;
        assert!(entries.iter().any(|e| e.name == "subdir"));
    }

    #[tokio::test]
    async fn test_create_and_open() {
        let (fs, _) = make_fs();
        let ctx = make_ctx();

        let result = fs
            .create(ctx, FUSE_ROOT_ID, "newfile.txt", S_IFREG | 0o644, libc::O_RDWR)
            .await;
        assert!(result.is_ok());
        let reply = result.unwrap();
        assert_ne!(reply.fh, 0);
        assert_ne!(reply.entry.ino, 0);
    }

    #[tokio::test]
    async fn test_unlink() {
        let (fs, client) = make_fs();
        client.add_file(InodeId(ROOT_INODE_ID), "to_delete.txt", 101);

        let ctx = make_ctx();
        let result = fs.unlink(ctx, FUSE_ROOT_ID, "to_delete.txt").await;
        assert!(result.is_ok());

        let lookup_result = fs.lookup(ctx, FUSE_ROOT_ID, "to_delete.txt").await;
        assert!(lookup_result.is_err());
    }

    #[tokio::test]
    async fn test_symlink_and_readlink() {
        let (fs, _) = make_fs();
        let ctx = make_ctx();

        let result = fs
            .symlink(ctx, FUSE_ROOT_ID, "mylink", "/some/target")
            .await;
        assert!(result.is_ok());
        let ino = result.unwrap().entry.ino;

        let readlink_result = fs.readlink(ctx, ino).await;
        assert!(readlink_result.is_ok());
        assert_eq!(readlink_result.unwrap().target, "/some/target");
    }

    #[tokio::test]
    async fn test_rename() {
        let (fs, client) = make_fs();
        client.add_file(InodeId(ROOT_INODE_ID), "old_name.txt", 102);

        let ctx = make_ctx();
        let result = fs
            .rename(ctx, FUSE_ROOT_ID, "old_name.txt", FUSE_ROOT_ID, "new_name.txt", 0)
            .await;
        assert!(result.is_ok());

        // Old name gone
        assert!(fs.lookup(ctx, FUSE_ROOT_ID, "old_name.txt").await.is_err());
        // New name exists
        assert!(fs.lookup(ctx, FUSE_ROOT_ID, "new_name.txt").await.is_ok());
    }

    #[tokio::test]
    async fn test_statfs() {
        let (fs, _) = make_fs();
        let ctx = make_ctx();
        let result = fs.statfs(ctx, FUSE_ROOT_ID).await;
        assert!(result.is_ok());
        let stat = result.unwrap().stat;
        assert!(stat.blocks > 0);
        assert!(stat.bfree > 0);
        assert_eq!(stat.namelen, 255);
    }

    #[tokio::test]
    async fn test_readonly_rejects_writes() {
        let client = Arc::new(MockClient::new());
        let mut config = FuseConfig::default();
        config.readonly = true;
        let fs = FuseFileSystem::new(client, config, "test-token".to_string());
        let ctx = make_ctx();

        assert_eq!(
            fs.mkdir(ctx, FUSE_ROOT_ID, "dir", 0o755)
                .await
                .unwrap_err(),
            libc::EROFS
        );
        assert_eq!(
            fs.unlink(ctx, FUSE_ROOT_ID, "file").await.unwrap_err(),
            libc::EROFS
        );
        assert_eq!(
            fs.symlink(ctx, FUSE_ROOT_ID, "link", "/target")
                .await
                .unwrap_err(),
            libc::EROFS
        );
    }

    #[tokio::test]
    async fn test_forget_removes_inode() {
        let (fs, client) = make_fs();
        client.add_file(InodeId(ROOT_INODE_ID), "file.txt", 200);

        let ctx = make_ctx();
        let result = fs.lookup(ctx, FUSE_ROOT_ID, "file.txt").await.unwrap();
        let ino = result.entry.ino;
        let initial_count = fs.inode_count();

        fs.forget(ino, 1).await;
        assert_eq!(fs.inode_count(), initial_count - 1);
    }

    #[tokio::test]
    async fn test_link() {
        let (fs, client) = make_fs();
        client.add_file(InodeId(ROOT_INODE_ID), "original.txt", 300);
        client.add_dir(InodeId(ROOT_INODE_ID), "subdir", 301);

        let ctx = make_ctx();
        let lookup = fs.lookup(ctx, FUSE_ROOT_ID, "original.txt").await.unwrap();
        let ino = lookup.entry.ino;

        let sub_lookup = fs.lookup(ctx, FUSE_ROOT_ID, "subdir").await.unwrap();
        let sub_ino = sub_lookup.entry.ino;

        let result = fs.link(ctx, ino, sub_ino, "hardlink.txt").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_read_write() {
        let (fs, client) = make_fs();
        client.add_file(InodeId(ROOT_INODE_ID), "data.txt", 400);

        let ctx = make_ctx();
        // Lookup to populate inode table
        let lookup = fs.lookup(ctx, FUSE_ROOT_ID, "data.txt").await.unwrap();
        let ino = lookup.entry.ino;

        // Open for writing
        let open_result = fs.open(ctx, ino, libc::O_RDWR).await.unwrap();
        let fh = open_result.fh;

        // Write
        let data = b"hello world";
        let write_result = fs.write(ctx, ino, fh, 0, data, 0).await;
        assert!(write_result.is_ok());
        assert_eq!(write_result.unwrap().written, data.len() as u32);

        // Read
        let read_result = fs.read(ctx, ino, fh, 0, 100).await;
        assert!(read_result.is_ok());

        // Release
        let release_result = fs.release(ctx, ino, fh, libc::O_RDWR).await;
        assert!(release_result.is_ok());
    }
}
