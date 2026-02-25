//! FUSE-specific types that mirror the FUSE kernel protocol structures.
//!
//! These types abstract away the raw FUSE protocol so the implementation
//! can later be backed by the `fuser` crate or a custom kernel binding.

use hf3fs_types::{Gid, InodeId, Uid};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// O_DIRECT is Linux-specific; on macOS we define it as 0 (no-op).
#[cfg(target_os = "linux")]
const O_DIRECT: i32 = O_DIRECT;
#[cfg(not(target_os = "linux"))]
const O_DIRECT: i32 = 0;

// ── Inode number constants ──────────────────────────────────────────────────

/// The FUSE root inode number (always 1 in the kernel protocol).
pub const FUSE_ROOT_ID: u64 = 1;

/// Special inode ID for the GC root (root + 1 in the C++ code).
pub const FUSE_GC_ROOT_ID: u64 = 2;

// ── File type bits (matching libc S_IF* constants) ──────────────────────────

/// Regular file.
pub const S_IFREG: u32 = libc::S_IFREG as u32;
/// Directory.
pub const S_IFDIR: u32 = libc::S_IFDIR as u32;
/// Symbolic link.
pub const S_IFLNK: u32 = libc::S_IFLNK as u32;

// ── File attribute struct ───────────────────────────────────────────────────

/// File attributes returned by getattr/lookup operations.
///
/// Mirrors the kernel `struct stat` fields that FUSE cares about.
#[derive(Debug, Clone)]
pub struct FileAttr {
    /// Inode number.
    pub ino: u64,
    /// File size in bytes.
    pub size: u64,
    /// Number of 512-byte blocks allocated.
    pub blocks: u64,
    /// Last access time.
    pub atime: SystemTime,
    /// Last modification time.
    pub mtime: SystemTime,
    /// Last status change time.
    pub ctime: SystemTime,
    /// File mode (type + permission bits).
    pub mode: u32,
    /// Number of hard links.
    pub nlink: u32,
    /// Owner UID.
    pub uid: u32,
    /// Owner GID.
    pub gid: u32,
    /// Device number (unused, always 0).
    pub rdev: u32,
    /// Preferred I/O block size.
    pub blksize: u32,
}

impl Default for FileAttr {
    fn default() -> Self {
        Self {
            ino: 0,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            mode: 0,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
        }
    }
}

impl FileAttr {
    /// Create a new `FileAttr` with the given inode and zero/default values.
    pub fn new(ino: u64) -> Self {
        Self {
            ino,
            ..Default::default()
        }
    }
}

// ── FUSE entry param ────────────────────────────────────────────────────────

/// Entry returned by lookup and create operations.
///
/// Mirrors `struct fuse_entry_param` from the FUSE protocol.
#[derive(Debug, Clone)]
pub struct FuseEntryParam {
    /// Inode number of the entry.
    pub ino: u64,
    /// Generation number for the entry (0 for most implementations).
    pub generation: u64,
    /// File attributes.
    pub attr: FileAttr,
    /// Attribute cache validity duration.
    pub attr_timeout: Duration,
    /// Entry (name lookup) cache validity duration.
    pub entry_timeout: Duration,
}

impl FuseEntryParam {
    /// Create a new entry with default timeouts.
    pub fn new(attr_timeout: Duration, entry_timeout: Duration) -> Self {
        Self {
            ino: 0,
            generation: 0,
            attr: FileAttr::default(),
            attr_timeout,
            entry_timeout,
        }
    }

    /// Fill in the entry from a FileAttr, setting `ino` to match `attr.ino`.
    pub fn set_attr(&mut self, attr: FileAttr) {
        self.ino = attr.ino;
        self.attr = attr;
    }
}

// ── Filesystem statistics ───────────────────────────────────────────────────

/// Filesystem statistics returned by statfs.
///
/// Mirrors `struct statvfs`.
#[derive(Debug, Clone, Default)]
pub struct StatFs {
    /// Filesystem block size (also used as fragment size).
    pub bsize: u64,
    /// Total blocks in the filesystem.
    pub blocks: u64,
    /// Free blocks available.
    pub bfree: u64,
    /// Free blocks available to unprivileged users.
    pub bavail: u64,
    /// Total inodes (0 = unknown).
    pub files: u64,
    /// Free inodes.
    pub ffree: u64,
    /// Maximum filename length.
    pub namelen: u32,
}

// ── Open file flags ─────────────────────────────────────────────────────────

/// Parsed open flags for file operations.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenFlags {
    /// File was opened read-only.
    pub read_only: bool,
    /// File was opened write-only.
    pub write_only: bool,
    /// File was opened read-write.
    pub read_write: bool,
    /// O_CREAT was specified.
    pub create: bool,
    /// O_EXCL was specified (must create new file).
    pub exclusive: bool,
    /// O_TRUNC was specified.
    pub truncate: bool,
    /// O_APPEND was specified.
    pub append: bool,
    /// O_DIRECT was specified (bypass page cache).
    pub direct: bool,
    /// O_NONBLOCK was specified.
    pub nonblock: bool,
}

impl OpenFlags {
    /// Parse raw POSIX open flags into structured form.
    pub fn from_raw(flags: i32) -> Self {
        let access_mode = flags & libc::O_ACCMODE;
        Self {
            read_only: access_mode == libc::O_RDONLY,
            write_only: access_mode == libc::O_WRONLY,
            read_write: access_mode == libc::O_RDWR,
            create: flags & libc::O_CREAT != 0,
            exclusive: flags & libc::O_EXCL != 0,
            truncate: flags & libc::O_TRUNC != 0,
            append: flags & libc::O_APPEND != 0,
            direct: flags & O_DIRECT != 0,
            nonblock: flags & libc::O_NONBLOCK != 0,
        }
    }

    /// Returns true if the file is opened for writing (write-only or read-write).
    pub fn is_writable(&self) -> bool {
        self.write_only || self.read_write
    }

    /// Convert back to raw POSIX flags.
    pub fn to_raw(&self) -> i32 {
        let mut flags = if self.read_write {
            libc::O_RDWR
        } else if self.write_only {
            libc::O_WRONLY
        } else {
            libc::O_RDONLY
        };
        if self.create {
            flags |= libc::O_CREAT;
        }
        if self.exclusive {
            flags |= libc::O_EXCL;
        }
        if self.truncate {
            flags |= libc::O_TRUNC;
        }
        if self.append {
            flags |= libc::O_APPEND;
        }
        if self.direct {
            flags |= O_DIRECT;
        }
        if self.nonblock {
            flags |= libc::O_NONBLOCK;
        }
        flags
    }
}

// ── SetAttr flags ───────────────────────────────────────────────────────────

/// Bitmask flags for setattr indicating which attributes to set.
///
/// These mirror FUSE_SET_ATTR_* from the FUSE protocol.
#[derive(Debug, Clone, Copy, Default)]
pub struct SetAttrFlags {
    /// Set file mode/permissions.
    pub mode: bool,
    /// Set owner UID.
    pub uid: bool,
    /// Set owner GID.
    pub gid: bool,
    /// Set file size (truncate).
    pub size: bool,
    /// Set access time.
    pub atime: bool,
    /// Set access time to current time.
    pub atime_now: bool,
    /// Set modification time.
    pub mtime: bool,
    /// Set modification time to current time.
    pub mtime_now: bool,
}

// ── FUSE request context ────────────────────────────────────────────────────

/// Context information about the caller making a FUSE request.
///
/// Mirrors `fuse_req_ctx` from the FUSE kernel protocol.
#[derive(Debug, Clone, Copy)]
pub struct FuseRequestContext {
    /// UID of the calling process.
    pub uid: Uid,
    /// GID of the calling process.
    pub gid: Gid,
    /// PID of the calling process.
    pub pid: u32,
}

// ── Dir entry for readdir ───────────────────────────────────────────────────

/// A single directory entry returned by readdir.
#[derive(Debug, Clone)]
pub struct FuseDirEntry {
    /// Inode number.
    pub ino: u64,
    /// Offset for the next entry (opaque cursor).
    pub offset: i64,
    /// File type (DT_REG, DT_DIR, DT_LNK, etc).
    pub file_type: u32,
    /// Entry name.
    pub name: String,
}

/// A directory entry with full attributes, for readdirplus.
#[derive(Debug, Clone)]
pub struct FuseDirEntryPlus {
    /// The directory entry.
    pub entry: FuseDirEntry,
    /// Full entry params (attributes + timeouts).
    pub entry_param: FuseEntryParam,
}

// ── File info for open/create ───────────────────────────────────────────────

/// Information about an opened file, passed between open/create and read/write.
#[derive(Debug, Clone)]
pub struct FuseFileInfo {
    /// File handle (opaque to FUSE, we use it to index into our handle table).
    pub fh: u64,
    /// Raw open flags.
    pub flags: i32,
    /// Whether direct I/O should be used for this file.
    pub direct_io: bool,
    /// Whether the file is seekable.
    pub keep_cache: bool,
    /// Whether the page cache should be flushed on open.
    pub nonseekable: bool,
}

// ── Connection info ─────────────────────────────────────────────────────────

/// FUSE connection capabilities and parameters, set during init.
///
/// Mirrors `struct fuse_conn_info`.
#[derive(Debug, Clone)]
pub struct FuseConnInfo {
    /// Protocol major version.
    pub proto_major: u32,
    /// Protocol minor version.
    pub proto_minor: u32,
    /// Maximum readahead.
    pub max_readahead: u32,
    /// Maximum read size.
    pub max_read: u32,
    /// Maximum write size.
    pub max_write: u32,
    /// Maximum background requests.
    pub max_background: u32,
    /// Time granularity in nanoseconds.
    pub time_gran: u32,
    /// Capabilities of the kernel FUSE module.
    pub capable: u32,
    /// Capabilities requested by the filesystem.
    pub want: u32,
}

impl Default for FuseConnInfo {
    fn default() -> Self {
        Self {
            proto_major: 7,
            proto_minor: 0,
            max_readahead: 0,
            max_read: 0,
            max_write: 0,
            max_background: 0,
            time_gran: 1_000_000_000, // 1 second
            capable: 0,
            want: 0,
        }
    }
}

// ── Inode ID helpers ────────────────────────────────────────────────────────

/// Convert a FUSE inode number to our internal InodeId.
///
/// FUSE root (1) maps to InodeId::root, and FUSE_GC_ROOT_ID (2) maps to GC root.
pub fn fuse_ino_to_inode_id(fuse_ino: u64) -> InodeId {
    match fuse_ino {
        FUSE_ROOT_ID => InodeId(ROOT_INODE_ID),
        FUSE_GC_ROOT_ID => InodeId(GC_ROOT_INODE_ID),
        other => InodeId(other),
    }
}

/// Convert our internal InodeId to a FUSE inode number.
pub fn inode_id_to_fuse_ino(id: InodeId) -> u64 {
    match id.0 {
        ROOT_INODE_ID => FUSE_ROOT_ID,
        GC_ROOT_INODE_ID => FUSE_GC_ROOT_ID,
        other => other,
    }
}

/// The internal inode ID used for the root directory.
pub const ROOT_INODE_ID: u64 = 0x0000_0000_0000_0001;

/// The internal inode ID used for the garbage-collection root.
pub const GC_ROOT_INODE_ID: u64 = 0x0000_0000_0000_0002;

/// Virtual inode ID for the `/3fs-virt` directory.
pub const VIRT_DIR_INODE_ID: u64 = 0xF000_0000_0000_0001;

/// Check if an inode ID belongs to the virtual inode space.
pub fn is_virtual_inode(id: InodeId) -> bool {
    (id.0 & 0xF000_0000_0000_0000) != 0
}

// ── Utility: SystemTime from microseconds ───────────────────────────────────

/// Convert microseconds since epoch to SystemTime.
pub fn system_time_from_us(us: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_micros(us)
}

/// Convert SystemTime to timespec-style (seconds, nanoseconds).
pub fn system_time_to_timespec(t: SystemTime) -> (i64, u32) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos()),
        Err(_) => (0, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fuse_ino_conversion_root() {
        let id = fuse_ino_to_inode_id(FUSE_ROOT_ID);
        assert_eq!(id.0, ROOT_INODE_ID);
        assert_eq!(inode_id_to_fuse_ino(id), FUSE_ROOT_ID);
    }

    #[test]
    fn test_fuse_ino_conversion_regular() {
        let id = fuse_ino_to_inode_id(12345);
        assert_eq!(id.0, 12345);
        assert_eq!(inode_id_to_fuse_ino(id), 12345);
    }

    #[test]
    fn test_is_virtual_inode() {
        assert!(is_virtual_inode(InodeId(VIRT_DIR_INODE_ID)));
        assert!(is_virtual_inode(InodeId(0xF000_0000_0000_0000)));
        assert!(!is_virtual_inode(InodeId(1)));
        assert!(!is_virtual_inode(InodeId(0)));
    }

    #[test]
    fn test_open_flags_roundtrip() {
        let raw = libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC;
        let parsed = OpenFlags::from_raw(raw);
        assert!(parsed.read_write);
        assert!(parsed.create);
        assert!(parsed.truncate);
        assert!(parsed.is_writable());
        assert!(!parsed.append);
        assert!(!parsed.direct);

        let round = parsed.to_raw();
        assert_eq!(round & libc::O_ACCMODE, libc::O_RDWR);
        assert_ne!(round & libc::O_CREAT, 0);
        assert_ne!(round & libc::O_TRUNC, 0);
    }

    #[test]
    fn test_open_flags_read_only() {
        let flags = OpenFlags::from_raw(libc::O_RDONLY);
        assert!(flags.read_only);
        assert!(!flags.is_writable());
    }

    #[test]
    fn test_system_time_from_us() {
        let t = system_time_from_us(1_000_000); // 1 second after epoch
        let dur = t.duration_since(UNIX_EPOCH).unwrap();
        assert_eq!(dur.as_secs(), 1);
        assert_eq!(dur.subsec_micros(), 0);
    }

    #[test]
    fn test_system_time_to_timespec() {
        let t = UNIX_EPOCH + Duration::from_nanos(1_500_000_000);
        let (sec, nsec) = system_time_to_timespec(t);
        assert_eq!(sec, 1);
        assert_eq!(nsec, 500_000_000);
    }

    #[test]
    fn test_file_attr_new() {
        let attr = FileAttr::new(42);
        assert_eq!(attr.ino, 42);
        assert_eq!(attr.size, 0);
        assert_eq!(attr.nlink, 0);
    }

    #[test]
    fn test_fuse_entry_param_set_attr() {
        let mut entry = FuseEntryParam::new(
            Duration::from_secs(30),
            Duration::from_secs(30),
        );
        let attr = FileAttr {
            ino: 100,
            size: 4096,
            mode: S_IFREG | 0o644,
            ..Default::default()
        };
        entry.set_attr(attr.clone());
        assert_eq!(entry.ino, 100);
        assert_eq!(entry.attr.size, 4096);
    }

    #[test]
    fn test_statfs_default() {
        let st = StatFs::default();
        assert_eq!(st.bsize, 0);
        assert_eq!(st.blocks, 0);
        assert_eq!(st.namelen, 0);
    }

    #[test]
    fn test_set_attr_flags_default() {
        let flags = SetAttrFlags::default();
        assert!(!flags.mode);
        assert!(!flags.uid);
        assert!(!flags.size);
    }

    #[test]
    fn test_fuse_conn_info_default() {
        let info = FuseConnInfo::default();
        assert_eq!(info.proto_major, 7);
        assert_eq!(info.time_gran, 1_000_000_000);
    }
}
