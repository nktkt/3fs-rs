//! Inode state management for the FUSE filesystem.
//!
//! Mirrors the C++ `RcInode`, `FileHandle`, `DirHandle` structs from
//! `FuseClients.h`. Tracks reference-counted inodes, open file handles,
//! and directory handles.

use hf3fs_types::{InodeId, Uid};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

// ── Inode type enumeration ──────────────────────────────────────────────────

/// The type of filesystem object an inode represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeType {
    /// Regular file.
    File,
    /// Directory.
    Directory,
    /// Symbolic link.
    Symlink,
}

// ── Layout information ──────────────────────────────────────────────────────

/// Storage layout information for files and directories.
#[derive(Debug, Clone, Default)]
pub struct Layout {
    /// Size of each chunk in bytes.
    pub chunk_size: u32,
    /// Number of stripes.
    pub stripe_size: u32,
}

// ── ACL / permissions ───────────────────────────────────────────────────────

/// Access control information for an inode.
#[derive(Debug, Clone, Default)]
pub struct Acl {
    pub uid: u32,
    pub gid: u32,
    pub perm: u16,
}

// ── Inode data variants ─────────────────────────────────────────────────────

/// Data specific to each inode type.
#[derive(Debug, Clone)]
pub enum InodeData {
    File {
        length: u64,
        truncate_ver: u64,
        dyn_stripe: u32,
        layout: Layout,
    },
    Directory {
        parent: InodeId,
        layout: Layout,
    },
    Symlink {
        target: String,
    },
}

impl Default for InodeData {
    fn default() -> Self {
        InodeData::Directory {
            parent: InodeId(0),
            layout: Layout::default(),
        }
    }
}

// ── Inode ───────────────────────────────────────────────────────────────────

/// An in-memory inode, representing a filesystem object.
///
/// Mirrors the C++ `Inode` struct with fields for identification,
/// permissions, timestamps, and type-specific data.
#[derive(Debug, Clone)]
pub struct Inode {
    /// Unique inode identifier.
    pub id: InodeId,
    /// Access control.
    pub acl: Acl,
    /// Number of hard links.
    pub nlink: u32,
    /// Last access time.
    pub atime: SystemTime,
    /// Last modification time.
    pub mtime: SystemTime,
    /// Last status change time.
    pub ctime: SystemTime,
    /// Type-specific data.
    pub data: InodeData,
}

impl Default for Inode {
    fn default() -> Self {
        Self {
            id: InodeId(0),
            acl: Acl::default(),
            nlink: 0,
            atime: std::time::UNIX_EPOCH,
            mtime: std::time::UNIX_EPOCH,
            ctime: std::time::UNIX_EPOCH,
            data: InodeData::default(),
        }
    }
}

impl Inode {
    /// Returns true if this is a regular file.
    pub fn is_file(&self) -> bool {
        matches!(self.data, InodeData::File { .. })
    }

    /// Returns true if this is a directory.
    pub fn is_directory(&self) -> bool {
        matches!(self.data, InodeData::Directory { .. })
    }

    /// Returns true if this is a symbolic link.
    pub fn is_symlink(&self) -> bool {
        matches!(self.data, InodeData::Symlink { .. })
    }

    /// Returns the inode type.
    pub fn inode_type(&self) -> InodeType {
        match &self.data {
            InodeData::File { .. } => InodeType::File,
            InodeData::Directory { .. } => InodeType::Directory,
            InodeData::Symlink { .. } => InodeType::Symlink,
        }
    }

    /// Returns file length if this is a file, 0 otherwise.
    pub fn file_length(&self) -> u64 {
        match &self.data {
            InodeData::File { length, .. } => *length,
            InodeData::Symlink { target } => target.len() as u64,
            _ => 0,
        }
    }

    /// Returns the layout chunk size.
    pub fn chunk_size(&self) -> u32 {
        match &self.data {
            InodeData::File { layout, .. } => layout.chunk_size,
            InodeData::Directory { layout, .. } => layout.chunk_size,
            InodeData::Symlink { .. } => 0,
        }
    }

    /// Returns the symlink target, if this is a symlink.
    pub fn symlink_target(&self) -> Option<&str> {
        match &self.data {
            InodeData::Symlink { target } => Some(target),
            _ => None,
        }
    }
}

// ── Dynamic attributes (for write tracking) ─────────────────────────────────

/// Tracks dynamic state that changes during file writes.
///
/// This mirrors the C++ `RcInode::DynamicAttr` struct.
#[derive(Debug, Clone)]
pub struct DynamicAttr {
    /// Monotonically increasing write version counter.
    pub written: u64,
    /// Last periodic-synced write version.
    pub synced: u64,
    /// Last fsync/close-synced write version.
    pub fsynced: u64,
    /// UID of the last writer.
    pub writer: Uid,
    /// Dynamic stripe count for the file.
    pub dyn_stripe: u32,
    /// Largest known truncate version.
    pub truncate_ver: u64,
    /// Local access time (only tracked for write-opened files).
    pub atime: Option<SystemTime>,
    /// Local modification time.
    pub mtime: Option<SystemTime>,
}

impl Default for DynamicAttr {
    fn default() -> Self {
        Self {
            written: 0,
            synced: 0,
            fsynced: 0,
            writer: Uid(0),
            dyn_stripe: 1,
            truncate_ver: 0,
            atime: None,
            mtime: None,
        }
    }
}

impl DynamicAttr {
    /// Update dynamic attributes from an inode received from the metadata server.
    pub fn update(&mut self, inode: &Inode, syncver: u64, fsync: bool) {
        if !inode.is_file() {
            return;
        }

        self.synced = self.synced.max(syncver);
        if fsync {
            self.fsynced = self.fsynced.max(syncver);
        }

        if let InodeData::File {
            truncate_ver,
            dyn_stripe,
            ..
        } = &inode.data
        {
            self.truncate_ver = self.truncate_ver.max(*truncate_ver);
            self.dyn_stripe = *dyn_stripe;
        }
    }

    /// Returns true if there are un-synced writes.
    pub fn has_dirty_writes(&self) -> bool {
        self.written > self.synced
    }

    /// Returns true if there are un-fsynced writes.
    pub fn has_unfsynced_writes(&self) -> bool {
        self.written > self.fsynced
    }
}

// ── Reference-counted inode ─────────────────────────────────────────────────

/// A reference-counted inode with dynamic write-tracking state.
///
/// Mirrors the C++ `RcInode` struct. Each entry in the inode table
/// holds one of these, protected by the table lock. The dynamic
/// attributes are separately locked for concurrent read/write access.
pub struct RcInode {
    /// The underlying inode metadata.
    pub inode: Inode,
    /// FUSE lookup reference count (atomic for concurrent access).
    pub refcount: AtomicI64,
    /// Dynamic write-tracking attributes (separate lock for writers).
    pub dynamic_attr: RwLock<DynamicAttr>,
    /// Write buffer mutex (serializes buffer flushes).
    pub write_buf_lock: Mutex<()>,
}

impl RcInode {
    /// Create a new reference-counted inode with the given lookup refcount.
    pub fn new(inode: Inode, refcount: i64) -> Self {
        let mut dyn_attr = DynamicAttr::default();
        if let InodeData::File {
            truncate_ver,
            dyn_stripe,
            ..
        } = &inode.data
        {
            dyn_attr.truncate_ver = *truncate_ver;
            dyn_attr.dyn_stripe = *dyn_stripe;
        }

        Self {
            inode,
            refcount: AtomicI64::new(refcount),
            dynamic_attr: RwLock::new(dyn_attr),
            write_buf_lock: Mutex::new(()),
        }
    }

    /// Get the current truncate version.
    pub fn truncate_ver(&self) -> u64 {
        self.dynamic_attr.read().truncate_ver
    }

    /// Update the inode from a metadata server response.
    pub fn update(&self, inode: &Inode, syncver: u64, fsync: bool) {
        if !inode.is_file() {
            return;
        }
        self.dynamic_attr.write().update(inode, syncver, fsync);
    }

    /// Clear the hint length, forcing recalculation on next sync.
    pub fn clear_hint_length(&self) {
        // This is a simplified version; the C++ code manages a VersionedLength.
        // For now we just note that the hint needs to be recalculated.
        let guard = self.dynamic_attr.write();
        let _ = &guard.synced; // no-op placeholder; real impl would clear hintLength
    }
}

impl std::fmt::Debug for RcInode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RcInode")
            .field("inode_id", &self.inode.id)
            .field("refcount", &self.refcount.load(Ordering::Relaxed))
            .finish()
    }
}

// ── File handle ─────────────────────────────────────────────────────────────

/// State associated with an open file descriptor.
///
/// Mirrors the C++ `FileHandle` struct.
#[derive(Debug)]
pub struct FileHandle {
    /// Reference to the inode.
    pub rcinode: Arc<RcInode>,
    /// Whether O_DIRECT was used when opening.
    pub direct_io: bool,
    /// Session ID for write-opened files (for close/sync protocol).
    pub session_id: u64,
    /// Raw open flags.
    pub flags: i32,
}

// ── Directory handle ────────────────────────────────────────────────────────

/// State associated with an open directory.
///
/// Mirrors the C++ `DirHandle` struct.
#[derive(Debug)]
pub struct DirHandle {
    /// Unique directory handle ID.
    pub dir_id: u64,
    /// PID of the process that opened the directory.
    pub pid: u32,
    /// Whether this is an IOV directory (virtual).
    pub iov_dir: bool,
}

// ── Handle table ────────────────────────────────────────────────────────────

/// Thread-safe table mapping file handle IDs to FileHandle state.
pub struct HandleTable {
    next_id: AtomicU64,
    file_handles: dashmap::DashMap<u64, FileHandle>,
    dir_handles: dashmap::DashMap<u64, DirHandle>,
}

impl HandleTable {
    /// Create a new empty handle table.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            file_handles: dashmap::DashMap::new(),
            dir_handles: dashmap::DashMap::new(),
        }
    }

    /// Allocate a new file handle ID and store the associated state.
    pub fn insert_file(&self, handle: FileHandle) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.file_handles.insert(id, handle);
        id
    }

    /// Look up a file handle by ID.
    pub fn get_file(&self, id: u64) -> Option<dashmap::mapref::one::Ref<'_, u64, FileHandle>> {
        self.file_handles.get(&id)
    }

    /// Remove and return a file handle.
    pub fn remove_file(&self, id: u64) -> Option<FileHandle> {
        self.file_handles.remove(&id).map(|(_, v)| v)
    }

    /// Allocate a new directory handle ID and store the associated state.
    pub fn insert_dir(&self, handle: DirHandle) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.dir_handles.insert(id, handle);
        id
    }

    /// Look up a directory handle by ID.
    pub fn get_dir(&self, id: u64) -> Option<dashmap::mapref::one::Ref<'_, u64, DirHandle>> {
        self.dir_handles.get(&id)
    }

    /// Remove and return a directory handle.
    pub fn remove_dir(&self, id: u64) -> Option<DirHandle> {
        self.dir_handles.remove(&id).map(|(_, v)| v)
    }

    /// Number of open file handles.
    pub fn file_count(&self) -> usize {
        self.file_handles.len()
    }

    /// Number of open directory handles.
    pub fn dir_count(&self) -> usize {
        self.dir_handles.len()
    }
}

impl Default for HandleTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HandleTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandleTable")
            .field("file_count", &self.file_handles.len())
            .field("dir_count", &self.dir_handles.len())
            .finish()
    }
}

// ── Inode table ─────────────────────────────────────────────────────────────

/// Thread-safe table tracking all known inodes by their InodeId.
///
/// Mirrors the C++ `std::unordered_map<InodeId, std::shared_ptr<RcInode>>`.
pub struct InodeTable {
    inodes: Mutex<HashMap<InodeId, Arc<RcInode>>>,
}

impl InodeTable {
    /// Create a new inode table with the root inode pre-populated.
    pub fn new() -> Self {
        let mut map = HashMap::new();
        let root = Inode {
            id: InodeId(crate::types::ROOT_INODE_ID),
            nlink: 2,
            data: InodeData::Directory {
                parent: InodeId(crate::types::ROOT_INODE_ID),
                layout: Layout::default(),
            },
            ..Default::default()
        };
        map.insert(root.id, Arc::new(RcInode::new(root, 2)));
        Self {
            inodes: Mutex::new(map),
        }
    }

    /// Add or update an inode entry. If the inode already exists, increment
    /// its refcount; otherwise insert a new entry with refcount 1.
    /// Returns the `Arc<RcInode>`.
    pub fn add_entry(&self, inode: Inode) -> Arc<RcInode> {
        let mut map = self.inodes.lock();
        let id = inode.id;
        if let Some(existing) = map.get(&id) {
            existing.refcount.fetch_add(1, Ordering::Relaxed);
            existing.update(&inode, 0, false);
            existing.clone()
        } else {
            let rc = Arc::new(RcInode::new(inode, 1));
            map.insert(id, rc.clone());
            rc
        }
    }

    /// Decrease the lookup refcount for an inode. If it reaches 0, remove
    /// the entry from the table.
    pub fn forget(&self, id: InodeId, nlookup: u64) {
        let mut map = self.inodes.lock();
        if let Some(entry) = map.get(&id) {
            let prev = entry.refcount.fetch_sub(nlookup as i64, Ordering::Relaxed);
            if prev - nlookup as i64 <= 0 {
                map.remove(&id);
            }
        }
    }

    /// Look up an inode by ID.
    pub fn get(&self, id: InodeId) -> Option<Arc<RcInode>> {
        self.inodes.lock().get(&id).cloned()
    }

    /// Number of tracked inodes.
    pub fn len(&self) -> usize {
        self.inodes.lock().len()
    }

    /// Whether the table is empty.
    pub fn is_empty(&self) -> bool {
        self.inodes.lock().is_empty()
    }
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InodeTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InodeTable")
            .field("count", &self.inodes.lock().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    fn make_file_inode(id: u64) -> Inode {
        Inode {
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
                length: 4096,
                truncate_ver: 1,
                dyn_stripe: 1,
                layout: Layout {
                    chunk_size: 65536,
                    stripe_size: 1,
                },
            },
        }
    }

    fn make_dir_inode(id: u64, parent: u64) -> Inode {
        Inode {
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
                parent: InodeId(parent),
                layout: Layout::default(),
            },
        }
    }

    fn make_symlink_inode(id: u64, target: &str) -> Inode {
        Inode {
            id: InodeId(id),
            acl: Acl {
                uid: 1000,
                gid: 1000,
                perm: 0o777,
            },
            nlink: 1,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            data: InodeData::Symlink {
                target: target.to_string(),
            },
        }
    }

    #[test]
    fn test_inode_type_checks() {
        let file = make_file_inode(100);
        assert!(file.is_file());
        assert!(!file.is_directory());
        assert!(!file.is_symlink());
        assert_eq!(file.inode_type(), InodeType::File);

        let dir = make_dir_inode(200, 1);
        assert!(!dir.is_file());
        assert!(dir.is_directory());
        assert_eq!(dir.inode_type(), InodeType::Directory);

        let sym = make_symlink_inode(300, "/target");
        assert!(sym.is_symlink());
        assert_eq!(sym.symlink_target(), Some("/target"));
        assert_eq!(sym.inode_type(), InodeType::Symlink);
    }

    #[test]
    fn test_inode_file_length() {
        let file = make_file_inode(100);
        assert_eq!(file.file_length(), 4096);

        let dir = make_dir_inode(200, 1);
        assert_eq!(dir.file_length(), 0);

        let sym = make_symlink_inode(300, "/target");
        assert_eq!(sym.file_length(), 7); // "/target".len()
    }

    #[test]
    fn test_inode_chunk_size() {
        let file = make_file_inode(100);
        assert_eq!(file.chunk_size(), 65536);

        let sym = make_symlink_inode(300, "/target");
        assert_eq!(sym.chunk_size(), 0);
    }

    #[test]
    fn test_dynamic_attr_update() {
        let mut dyn_attr = DynamicAttr::default();
        let inode = make_file_inode(100);

        dyn_attr.written = 5;
        dyn_attr.update(&inode, 3, false);
        assert_eq!(dyn_attr.synced, 3);
        assert_eq!(dyn_attr.fsynced, 0);
        assert!(dyn_attr.has_dirty_writes());

        dyn_attr.update(&inode, 5, true);
        assert_eq!(dyn_attr.synced, 5);
        assert_eq!(dyn_attr.fsynced, 5);
        assert!(!dyn_attr.has_dirty_writes());
    }

    #[test]
    fn test_dynamic_attr_skip_non_file() {
        let mut dyn_attr = DynamicAttr::default();
        dyn_attr.written = 5;
        let dir = make_dir_inode(200, 1);
        dyn_attr.update(&dir, 3, false);
        // Should not have changed
        assert_eq!(dyn_attr.synced, 0);
    }

    #[test]
    fn test_rcinode_new() {
        let inode = make_file_inode(100);
        let rc = RcInode::new(inode, 1);
        assert_eq!(rc.refcount.load(Ordering::Relaxed), 1);
        assert_eq!(rc.truncate_ver(), 1);
        assert_eq!(rc.inode.id.0, 100);
    }

    #[test]
    fn test_handle_table_file() {
        let table = HandleTable::new();
        let inode = make_file_inode(100);
        let rc = Arc::new(RcInode::new(inode, 1));

        let fh = FileHandle {
            rcinode: rc,
            direct_io: false,
            session_id: 42,
            flags: libc::O_RDWR,
        };
        let id = table.insert_file(fh);
        assert_eq!(table.file_count(), 1);

        {
            let handle = table.get_file(id).unwrap();
            assert_eq!(handle.session_id, 42);
            assert!(!handle.direct_io);
        }

        let removed = table.remove_file(id);
        assert!(removed.is_some());
        assert_eq!(table.file_count(), 0);
    }

    #[test]
    fn test_handle_table_dir() {
        let table = HandleTable::new();
        let dh = DirHandle {
            dir_id: 1,
            pid: 1234,
            iov_dir: false,
        };
        let id = table.insert_dir(dh);
        assert_eq!(table.dir_count(), 1);

        {
            let handle = table.get_dir(id).unwrap();
            assert_eq!(handle.pid, 1234);
        }

        let removed = table.remove_dir(id);
        assert!(removed.is_some());
        assert_eq!(table.dir_count(), 0);
    }

    #[test]
    fn test_inode_table_new_has_root() {
        let table = InodeTable::new();
        assert_eq!(table.len(), 1);

        let root = table.get(InodeId(crate::types::ROOT_INODE_ID));
        assert!(root.is_some());
    }

    #[test]
    fn test_inode_table_add_and_forget() {
        let table = InodeTable::new();
        let inode = make_file_inode(100);
        let _rc = table.add_entry(inode);
        assert_eq!(table.len(), 2); // root + our inode

        let found = table.get(InodeId(100));
        assert!(found.is_some());

        table.forget(InodeId(100), 1);
        assert_eq!(table.len(), 1); // only root left
        assert!(table.get(InodeId(100)).is_none());
    }

    #[test]
    fn test_inode_table_add_twice_increments_refcount() {
        let table = InodeTable::new();
        let inode1 = make_file_inode(100);
        let inode2 = make_file_inode(100);
        let _rc1 = table.add_entry(inode1);
        let _rc2 = table.add_entry(inode2);
        assert_eq!(table.len(), 2); // root + inode 100

        // Need to forget twice (refcount was 1 + 1 = 2)
        table.forget(InodeId(100), 1);
        assert_eq!(table.len(), 2); // still there
        table.forget(InodeId(100), 1);
        assert_eq!(table.len(), 1); // now gone
    }
}
