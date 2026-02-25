//! FUSE operations trait.
//!
//! Defines the `FuseOps` trait with all FUSE low-level operations. This is
//! an async trait that can later be backed by the `fuser` crate or any other
//! FUSE implementation.
//!
//! The operations mirror the `fuse_lowlevel_ops` struct from the FUSE3 C API,
//! which is the interface used by the C++ implementation.

use crate::reply::*;
use crate::types::*;

/// Trait defining all FUSE filesystem operations.
///
/// Each method corresponds to a FUSE low-level operation. Default
/// implementations return `ENOSYS` (function not implemented).
///
/// The `ctx` parameter provides the UID/GID/PID of the calling process.
///
/// # Error handling
///
/// Operations return `FuseResult<T>` where the error value is an errno
/// (positive integer, e.g., `libc::ENOENT`).
#[async_trait::async_trait]
pub trait FuseOps: Send + Sync + 'static {
    // ── Lifecycle ───────────────────────────────────────────────────────

    /// Called when the filesystem is mounted.
    ///
    /// The implementation should negotiate capabilities via `conn_info`
    /// and perform any initialization.
    async fn init(&self, conn_info: &mut FuseConnInfo) -> FuseResult<()> {
        let _ = conn_info;
        Ok(())
    }

    /// Called when the filesystem is unmounted.
    async fn destroy(&self) {
        // Default: no-op
    }

    // ── Name lookup ─────────────────────────────────────────────────────

    /// Look up a directory entry by name and return its attributes.
    ///
    /// This is the most frequently called operation and must be efficient.
    /// The kernel caches the result for `entry_timeout` duration.
    async fn lookup(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
    ) -> FuseResult<ReplyEntry> {
        let _ = (ctx, parent, name);
        Err(libc::ENOSYS)
    }

    /// Forget about an inode, decrementing its lookup count.
    ///
    /// This is called when the kernel removes cached entries. It does not
    /// necessarily mean the file was deleted. `nlookup` is the number of
    /// lookups to forget.
    async fn forget(&self, ino: u64, nlookup: u64) {
        let _ = (ino, nlookup);
    }

    // ── Attribute operations ────────────────────────────────────────────

    /// Get file attributes.
    async fn getattr(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: Option<u64>,
    ) -> FuseResult<ReplyAttr> {
        let _ = (ctx, ino, fh);
        Err(libc::ENOSYS)
    }

    /// Set file attributes (chmod, chown, truncate, utimes).
    ///
    /// `to_set` indicates which attributes to modify.
    async fn setattr(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        attrs: &SetAttrValues,
        fh: Option<u64>,
    ) -> FuseResult<ReplyAttr> {
        let _ = (ctx, ino, attrs, fh);
        Err(libc::ENOSYS)
    }

    // ── Symlink operations ──────────────────────────────────────────────

    /// Read the target of a symbolic link.
    async fn readlink(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
    ) -> FuseResult<ReplyReadlink> {
        let _ = (ctx, ino);
        Err(libc::ENOSYS)
    }

    /// Create a symbolic link.
    async fn symlink(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        link: &str,
    ) -> FuseResult<ReplyEntry> {
        let _ = (ctx, parent, name, link);
        Err(libc::ENOSYS)
    }

    // ── File creation / deletion ────────────────────────────────────────

    /// Create a regular file node.
    async fn mknod(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        mode: u32,
        rdev: u32,
    ) -> FuseResult<ReplyEntry> {
        let _ = (ctx, parent, name, mode, rdev);
        Err(libc::ENOSYS)
    }

    /// Create a directory.
    async fn mkdir(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        mode: u32,
    ) -> FuseResult<ReplyEntry> {
        let _ = (ctx, parent, name, mode);
        Err(libc::ENOSYS)
    }

    /// Remove a file.
    async fn unlink(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
    ) -> FuseResult<()> {
        let _ = (ctx, parent, name);
        Err(libc::ENOSYS)
    }

    /// Remove a directory.
    async fn rmdir(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
    ) -> FuseResult<()> {
        let _ = (ctx, parent, name);
        Err(libc::ENOSYS)
    }

    // ── Rename / Link ───────────────────────────────────────────────────

    /// Rename a file or directory.
    async fn rename(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        new_parent: u64,
        new_name: &str,
        flags: u32,
    ) -> FuseResult<()> {
        let _ = (ctx, parent, name, new_parent, new_name, flags);
        Err(libc::ENOSYS)
    }

    /// Create a hard link.
    async fn link(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        new_parent: u64,
        new_name: &str,
    ) -> FuseResult<ReplyEntry> {
        let _ = (ctx, ino, new_parent, new_name);
        Err(libc::ENOSYS)
    }

    // ── File I/O ────────────────────────────────────────────────────────

    /// Open a file.
    ///
    /// Returns a file handle and flags (direct_io, keep_cache).
    async fn open(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        flags: i32,
    ) -> FuseResult<ReplyOpen> {
        let _ = (ctx, ino, flags);
        Err(libc::ENOSYS)
    }

    /// Read data from an open file.
    async fn read(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        let _ = (ctx, ino, fh, offset, size);
        Err(libc::ENOSYS)
    }

    /// Write data to an open file.
    async fn write(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        flags: i32,
    ) -> FuseResult<ReplyWrite> {
        let _ = (ctx, ino, fh, offset, data, flags);
        Err(libc::ENOSYS)
    }

    /// Flush any buffered data for an open file.
    ///
    /// Called on each `close()` of a file descriptor (there may be multiple
    /// if the fd was dup'd).
    async fn flush(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
    ) -> FuseResult<()> {
        let _ = (ctx, ino, fh);
        Ok(())
    }

    /// Release (close) an open file.
    ///
    /// Called once per open, when all file descriptors are closed.
    async fn release(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        flags: i32,
    ) -> FuseResult<()> {
        let _ = (ctx, ino, fh, flags);
        Ok(())
    }

    /// Synchronize file contents to storage.
    ///
    /// If `datasync` is true, only the file data should be synced,
    /// not the metadata.
    async fn fsync(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        datasync: bool,
    ) -> FuseResult<()> {
        let _ = (ctx, ino, fh, datasync);
        Err(libc::ENOSYS)
    }

    // ── Directory I/O ───────────────────────────────────────────────────

    /// Open a directory for reading.
    async fn opendir(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
    ) -> FuseResult<ReplyOpen> {
        let _ = (ctx, ino);
        Err(libc::ENOSYS)
    }

    /// Read directory entries.
    ///
    /// `offset` is an opaque cursor; 0 means start from the beginning.
    /// Returns entries until the buffer is full or no more entries.
    async fn readdir(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory> {
        let _ = (ctx, ino, fh, offset);
        Err(libc::ENOSYS)
    }

    /// Read directory entries with attributes (readdirplus).
    ///
    /// Like readdir, but also returns file attributes for each entry,
    /// avoiding a subsequent lookup call.
    async fn readdirplus(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectoryPlus> {
        let _ = (ctx, ino, fh, offset);
        Err(libc::ENOSYS)
    }

    /// Release (close) an open directory.
    async fn releasedir(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
    ) -> FuseResult<()> {
        let _ = (ctx, ino, fh);
        Ok(())
    }

    // ── File creation with open ─────────────────────────────────────────

    /// Atomically create and open a file.
    ///
    /// This combines mknod + open into a single operation, which is more
    /// efficient and avoids race conditions.
    async fn create(
        &self,
        ctx: FuseRequestContext,
        parent: u64,
        name: &str,
        mode: u32,
        flags: i32,
    ) -> FuseResult<ReplyCreate> {
        let _ = (ctx, parent, name, mode, flags);
        Err(libc::ENOSYS)
    }

    // ── Filesystem info ─────────────────────────────────────────────────

    /// Get filesystem statistics.
    async fn statfs(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
    ) -> FuseResult<ReplyStatFs> {
        let _ = (ctx, ino);
        Err(libc::ENOSYS)
    }

    // ── Extended attributes ─────────────────────────────────────────────

    /// Set an extended attribute.
    async fn setxattr(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        name: &str,
        value: &[u8],
        flags: i32,
    ) -> FuseResult<()> {
        let _ = (ctx, ino, name, value, flags);
        Err(libc::ENOSYS)
    }

    /// Get an extended attribute.
    async fn getxattr(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        name: &str,
        size: u32,
    ) -> FuseResult<ReplyXattr> {
        let _ = (ctx, ino, name, size);
        Err(libc::ENOSYS)
    }

    /// List extended attribute names.
    async fn listxattr(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        size: u32,
    ) -> FuseResult<ReplyXattr> {
        let _ = (ctx, ino, size);
        Err(libc::ENOSYS)
    }

    /// Remove an extended attribute.
    async fn removexattr(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        name: &str,
    ) -> FuseResult<()> {
        let _ = (ctx, ino, name);
        Err(libc::ENOSYS)
    }

    // ── ioctl ───────────────────────────────────────────────────────────

    /// Handle an ioctl request.
    async fn ioctl(
        &self,
        ctx: FuseRequestContext,
        ino: u64,
        fh: u64,
        cmd: u32,
        arg: u64,
        in_data: &[u8],
        out_size: u32,
    ) -> FuseResult<Vec<u8>> {
        let _ = (ctx, ino, fh, cmd, arg, in_data, out_size);
        Err(libc::ENOSYS)
    }
}

// ── SetAttrValues ───────────────────────────────────────────────────────────

/// Values to set in a setattr operation.
///
/// Each field is `Some` if that attribute should be changed.
#[derive(Debug, Clone, Default)]
pub struct SetAttrValues {
    /// New file mode (permission bits only).
    pub mode: Option<u32>,
    /// New owner UID.
    pub uid: Option<u32>,
    /// New owner GID.
    pub gid: Option<u32>,
    /// New file size (truncate).
    pub size: Option<u64>,
    /// New access time. `None` inside the Option means "set to now".
    pub atime: Option<SetAttrTime>,
    /// New modification time. `None` inside the Option means "set to now".
    pub mtime: Option<SetAttrTime>,
}

/// A time value for setattr, which can be an explicit time or "now".
#[derive(Debug, Clone)]
pub enum SetAttrTime {
    /// Set to the current time.
    Now,
    /// Set to a specific time.
    Specific(std::time::SystemTime),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal no-op implementation for testing default methods.
    struct NoopFs;

    #[async_trait::async_trait]
    impl FuseOps for NoopFs {}

    #[tokio::test]
    async fn test_default_lookup_returns_enosys() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(1000),
            gid: hf3fs_types::Gid(1000),
            pid: 1234,
        };
        let result = fs.lookup(ctx, 1, "test").await;
        assert_eq!(result.unwrap_err(), libc::ENOSYS);
    }

    #[tokio::test]
    async fn test_default_init_succeeds() {
        let fs = NoopFs;
        let mut conn = FuseConnInfo::default();
        let result = fs.init(&mut conn).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_default_flush_succeeds() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert!(fs.flush(ctx, 1, 0).await.is_ok());
    }

    #[tokio::test]
    async fn test_default_release_succeeds() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert!(fs.release(ctx, 1, 0, 0).await.is_ok());
    }

    #[tokio::test]
    async fn test_default_getattr_returns_enosys() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert_eq!(fs.getattr(ctx, 1, None).await.unwrap_err(), libc::ENOSYS);
    }

    #[tokio::test]
    async fn test_default_read_returns_enosys() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert_eq!(
            fs.read(ctx, 1, 0, 0, 4096).await.unwrap_err(),
            libc::ENOSYS
        );
    }

    #[tokio::test]
    async fn test_default_write_returns_enosys() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert_eq!(
            fs.write(ctx, 1, 0, 0, &[0u8; 4], 0).await.unwrap_err(),
            libc::ENOSYS
        );
    }

    #[tokio::test]
    async fn test_default_statfs_returns_enosys() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert_eq!(fs.statfs(ctx, 1).await.unwrap_err(), libc::ENOSYS);
    }

    #[tokio::test]
    async fn test_default_readdir_returns_enosys() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert_eq!(
            fs.readdir(ctx, 1, 0, 0).await.unwrap_err(),
            libc::ENOSYS
        );
    }

    #[tokio::test]
    async fn test_default_create_returns_enosys() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert_eq!(
            fs.create(ctx, 1, "test", 0o644, libc::O_RDWR)
                .await
                .unwrap_err(),
            libc::ENOSYS
        );
    }

    #[tokio::test]
    async fn test_default_symlink_returns_enosys() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert_eq!(
            fs.symlink(ctx, 1, "link", "/target").await.unwrap_err(),
            libc::ENOSYS
        );
    }

    #[tokio::test]
    async fn test_default_rename_returns_enosys() {
        let fs = NoopFs;
        let ctx = FuseRequestContext {
            uid: hf3fs_types::Uid(0),
            gid: hf3fs_types::Gid(0),
            pid: 1,
        };
        assert_eq!(
            fs.rename(ctx, 1, "old", 1, "new", 0).await.unwrap_err(),
            libc::ENOSYS
        );
    }

    #[test]
    fn test_set_attr_values_default() {
        let vals = SetAttrValues::default();
        assert!(vals.mode.is_none());
        assert!(vals.uid.is_none());
        assert!(vals.gid.is_none());
        assert!(vals.size.is_none());
        assert!(vals.atime.is_none());
        assert!(vals.mtime.is_none());
    }
}
