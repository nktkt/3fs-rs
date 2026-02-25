//! Server-side inode implementation with KV store operations.
//!
//! This module wraps the proto `Inode` type with methods for
//! serializing/deserializing to the KV store, including key packing
//! and load/store/remove operations within transactions.
//!
//! Based on 3FS/src/meta/store/Inode.h and Inode.cc

use hf3fs_kv::{ReadOnlyTransaction, ReadWriteTransaction};
use hf3fs_proto::meta::{self, InodeType};
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::{MetaCode, StatusCode};
use hf3fs_types::Result;

use crate::key_prefix;

/// Inode ID type (u64), matching C++ `InodeId`.
pub type InodeId = u64;

/// The root inode ID.
pub const ROOT_INODE_ID: InodeId = 0;

/// The GC root inode ID.
pub const GC_ROOT_INODE_ID: InodeId = 1;

/// Check if an inode ID is a tree root (root or GC root).
pub fn is_tree_root(id: InodeId) -> bool {
    id == ROOT_INODE_ID || id == GC_ROOT_INODE_ID
}

/// ACL (Access Control List) for an inode.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Acl {
    pub uid: u32,
    pub gid: u32,
    pub perm: u32,
    pub iflags: u32,
}

impl Acl {
    pub fn new(uid: u32, gid: u32, perm: u32) -> Self {
        Self {
            uid,
            gid,
            perm,
            iflags: 0,
        }
    }

    /// Root ACL (uid=0, gid=0, perm=0755, immutable).
    pub fn root() -> Self {
        Self {
            uid: 0,
            gid: 0,
            perm: 0o755,
            iflags: FS_IMMUTABLE_FL,
        }
    }

    /// GC root ACL (uid=0, gid=0, perm=0700, immutable).
    pub fn gc_root() -> Self {
        Self {
            uid: 0,
            gid: 0,
            perm: 0o700,
            iflags: FS_IMMUTABLE_FL,
        }
    }

    /// Check if the given user has the requested access type.
    pub fn check_permission(&self, user: &meta::UserInfo, access: AccessType) -> Result<()> {
        // root can do anything
        if user.uid == 0 {
            return Ok(());
        }

        // owner check
        if user.uid == self.uid {
            let owner_bits = (self.perm >> 6) & 0o7;
            if has_permission(owner_bits, access) {
                return Ok(());
            }
        }

        // group check
        if user.gid == self.gid || user.gids.contains(&self.gid) {
            let group_bits = (self.perm >> 3) & 0o7;
            if has_permission(group_bits, access) {
                return Ok(());
            }
        }

        // other check
        let other_bits = self.perm & 0o7;
        if has_permission(other_bits, access) {
            return Ok(());
        }

        make_error(MetaCode::NO_PERMISSION)
    }
}

/// Access type bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    Read,
    Write,
    Exec,
    ReadWrite,
}

/// FS_IMMUTABLE_FL flag value from Linux.
pub const FS_IMMUTABLE_FL: u32 = 0x00000010;

/// FS_INDEX_FL reused for chain allocation.
pub const FS_CHAIN_ALLOCATION_FL: u32 = 0x00001000;

/// FS_SECRM_FL reused for new chunk engine.
pub const FS_NEW_CHUNK_ENGINE: u32 = 0x00000001;

/// Inheritable iflags mask.
pub const FS_FL_INHERITABLE: u32 = FS_CHAIN_ALLOCATION_FL | FS_NEW_CHUNK_ENGINE;

/// Check if permission bits satisfy the requested access.
fn has_permission(bits: u32, access: AccessType) -> bool {
    match access {
        AccessType::Read => bits & 0o4 != 0,
        AccessType::Write => bits & 0o2 != 0,
        AccessType::Exec => bits & 0o1 != 0,
        AccessType::ReadWrite => (bits & 0o4 != 0) && (bits & 0o2 != 0),
    }
}

/// Server-side inode that wraps the proto Inode with KV operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Inode {
    /// The underlying proto inode.
    pub inner: meta::Inode,
}

impl Inode {
    /// Create a new file inode.
    pub fn new_file(
        id: InodeId,
        acl: &Acl,
        layout: Option<meta::Layout>,
        now_ns: i64,
    ) -> Self {
        Self {
            inner: meta::Inode {
                id,
                inode_type: InodeType::File as u8,
                permission: acl.perm,
                uid: acl.uid,
                gid: acl.gid,
                nlink: 1,
                length: 0,
                atime_ns: now_ns,
                mtime_ns: now_ns,
                ctime_ns: now_ns,
                iflags: acl.iflags,
                layout,
                symlink_target: None,
            },
        }
    }

    /// Create a new directory inode.
    pub fn new_directory(
        id: InodeId,
        _parent: InodeId,
        _name: &str,
        acl: &Acl,
        layout: Option<meta::Layout>,
        now_ns: i64,
    ) -> Self {
        Self {
            inner: meta::Inode {
                id,
                inode_type: InodeType::Directory as u8,
                permission: acl.perm,
                uid: acl.uid,
                gid: acl.gid,
                nlink: 1,
                length: 0,
                atime_ns: now_ns,
                mtime_ns: now_ns,
                ctime_ns: now_ns,
                iflags: acl.iflags,
                layout,
                symlink_target: None,
            },
        }
    }

    /// Create a new symlink inode.
    pub fn new_symlink(
        id: InodeId,
        target: String,
        uid: u32,
        gid: u32,
        now_ns: i64,
    ) -> Self {
        Self {
            inner: meta::Inode {
                id,
                inode_type: InodeType::Symlink as u8,
                permission: 0o777,
                uid,
                gid,
                nlink: 1,
                length: 0,
                atime_ns: now_ns,
                mtime_ns: now_ns,
                ctime_ns: now_ns,
                iflags: 0,
                layout: None,
                symlink_target: Some(target),
            },
        }
    }

    /// Get the inode ID.
    pub fn id(&self) -> InodeId {
        self.inner.id
    }

    /// Get the inode type.
    pub fn inode_type(&self) -> Option<InodeType> {
        InodeType::try_from(self.inner.inode_type).ok()
    }

    /// Whether this is a file.
    pub fn is_file(&self) -> bool {
        self.inner.inode_type == InodeType::File as u8
    }

    /// Whether this is a directory.
    pub fn is_directory(&self) -> bool {
        self.inner.inode_type == InodeType::Directory as u8
    }

    /// Whether this is a symlink.
    pub fn is_symlink(&self) -> bool {
        self.inner.inode_type == InodeType::Symlink as u8
    }

    /// Get the ACL for this inode.
    pub fn acl(&self) -> Acl {
        Acl {
            uid: self.inner.uid,
            gid: self.inner.gid,
            perm: self.inner.permission,
            iflags: self.inner.iflags,
        }
    }

    /// Set the ACL on this inode.
    pub fn set_acl(&mut self, acl: &Acl) {
        self.inner.uid = acl.uid;
        self.inner.gid = acl.gid;
        self.inner.permission = acl.perm;
        self.inner.iflags = acl.iflags;
    }

    /// Pack the KV key for this inode.
    pub fn pack_key(&self) -> Vec<u8> {
        Self::pack_key_for(self.inner.id)
    }

    /// Pack the KV key for a given inode ID.
    ///
    /// Key format: `[INODE_PREFIX] + [InodeId as little-endian 8 bytes]`
    pub fn pack_key_for(id: InodeId) -> Vec<u8> {
        let mut key = Vec::with_capacity(9);
        key.push(key_prefix::INODE_PREFIX);
        key.extend_from_slice(&id.to_le_bytes());
        key
    }

    /// Serialize the inode data to bytes for KV value storage.
    pub fn pack_value(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.inner)
            .map_err(|e| hf3fs_types::Status::with_message(
                StatusCode::DATA_CORRUPTION,
                format!("failed to serialize inode: {}", e),
            ))
    }

    /// Deserialize an inode from KV key and value bytes.
    pub fn unpack(key: &[u8], value: &[u8]) -> Result<Self> {
        if key.len() != 9 || key[0] != key_prefix::INODE_PREFIX {
            return make_error_msg(
                StatusCode::DATA_CORRUPTION,
                "invalid inode key format",
            );
        }
        let id = u64::from_le_bytes(
            key[1..9]
                .try_into()
                .map_err(|_| hf3fs_types::Status::new(StatusCode::DATA_CORRUPTION))?,
        );
        let mut inner: meta::Inode = serde_json::from_slice(value)
            .map_err(|e| hf3fs_types::Status::with_message(
                StatusCode::DATA_CORRUPTION,
                format!("failed to deserialize inode {}: {}", id, e),
            ))?;
        inner.id = id;
        Ok(Self { inner })
    }

    /// Load an inode from the KV store (snapshot read, no read conflict).
    pub async fn snapshot_load<T: ReadOnlyTransaction + ?Sized>(
        txn: &T,
        id: InodeId,
    ) -> Result<Option<Self>> {
        let key = Self::pack_key_for(id);
        let value = txn.snapshot_get(&key).await?;
        match value {
            Some(v) => {
                let inode = Self::unpack(&key, &v)?;
                Ok(Some(inode))
            }
            None => Ok(None),
        }
    }

    /// Load an inode from the KV store (normal read, adds to read conflict set).
    pub async fn load<T: ReadOnlyTransaction + ?Sized>(
        txn: &T,
        id: InodeId,
    ) -> Result<Option<Self>> {
        let key = Self::pack_key_for(id);
        let value = txn.get(&key).await?;
        match value {
            Some(v) => {
                let inode = Self::unpack(&key, &v)?;
                Ok(Some(inode))
            }
            None => Ok(None),
        }
    }

    /// Store this inode into the KV store.
    pub async fn store(
        &self,
        txn: &mut dyn ReadWriteTransaction,
    ) -> Result<()> {
        // Validate tree roots
        if is_tree_root(self.id()) {
            if !self.is_directory() {
                return make_error_msg(
                    MetaCode::FOUND_BUG,
                    format!("store invalid root inode {}", self.id()),
                );
            }
        }
        // Validate directory names
        if self.is_directory() {
            // We don't have direct access to the "name" field in the proto Inode,
            // but the name is stored in the DirEntry and the parent inode.
            // Validation is done at the operation level.
        }

        let key = self.pack_key();
        let value = self.pack_value()?;
        txn.set(&key, &value).await
    }

    /// Remove this inode from the KV store.
    pub async fn remove(
        &self,
        txn: &mut dyn ReadWriteTransaction,
    ) -> Result<()> {
        if is_tree_root(self.id()) {
            return make_error_msg(
                MetaCode::FOUND_BUG,
                "cannot remove tree root",
            );
        }
        if self.inner.iflags & FS_IMMUTABLE_FL != 0 {
            return make_error_msg(
                MetaCode::FOUND_BUG,
                format!("cannot remove inode {} with FS_IMMUTABLE_FL", self.id()),
            );
        }
        let key = self.pack_key();
        txn.clear(&key).await
    }

    /// Add this inode's key to the read conflict set.
    pub async fn add_read_conflict(
        &self,
        txn: &mut dyn ReadWriteTransaction,
    ) -> Result<()> {
        let key = self.pack_key();
        txn.add_read_conflict(&key).await
    }

    /// Convert to proto Inode.
    pub fn into_proto(self) -> meta::Inode {
        self.inner
    }

    /// Borrow the inner proto Inode.
    pub fn as_proto(&self) -> &meta::Inode {
        &self.inner
    }
}

impl From<meta::Inode> for Inode {
    fn from(inner: meta::Inode) -> Self {
        Self { inner }
    }
}

impl From<Inode> for meta::Inode {
    fn from(inode: Inode) -> Self {
        inode.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_key() {
        let key = Inode::pack_key_for(0);
        assert_eq!(key.len(), 9);
        assert_eq!(key[0], key_prefix::INODE_PREFIX);
        assert_eq!(&key[1..], &0u64.to_le_bytes());
    }

    #[test]
    fn test_pack_key_roundtrip() {
        let id = 0x0123456789ABCDEFu64;
        let key = Inode::pack_key_for(id);
        assert_eq!(key[0], key_prefix::INODE_PREFIX);
        let unpacked = u64::from_le_bytes(key[1..9].try_into().unwrap());
        assert_eq!(unpacked, id);
    }

    #[test]
    fn test_new_file() {
        let acl = Acl::new(1000, 100, 0o644);
        let inode = Inode::new_file(42, &acl, None, 1_000_000);
        assert!(inode.is_file());
        assert!(!inode.is_directory());
        assert!(!inode.is_symlink());
        assert_eq!(inode.id(), 42);
        assert_eq!(inode.inner.uid, 1000);
        assert_eq!(inode.inner.gid, 100);
        assert_eq!(inode.inner.permission, 0o644);
        assert_eq!(inode.inner.nlink, 1);
    }

    #[test]
    fn test_new_directory() {
        let acl = Acl::new(0, 0, 0o755);
        let inode = Inode::new_directory(1, 0, "test", &acl, None, 2_000_000);
        assert!(inode.is_directory());
        assert!(!inode.is_file());
        assert_eq!(inode.id(), 1);
    }

    #[test]
    fn test_new_symlink() {
        let inode = Inode::new_symlink(99, "/target".to_string(), 1000, 100, 3_000_000);
        assert!(inode.is_symlink());
        assert_eq!(inode.inner.symlink_target.as_deref(), Some("/target"));
    }

    #[test]
    fn test_serialize_roundtrip() {
        let acl = Acl::new(1000, 100, 0o644);
        let inode = Inode::new_file(42, &acl, None, 1_000_000);
        let key = inode.pack_key();
        let value = inode.pack_value().unwrap();
        let restored = Inode::unpack(&key, &value).unwrap();
        assert_eq!(inode, restored);
    }

    #[test]
    fn test_acl_root_permissions() {
        let acl = Acl::new(1000, 100, 0o000);
        let root_user = meta::UserInfo {
            uid: 0,
            gid: 0,
            gids: vec![],
        };
        // root can do anything
        assert!(acl.check_permission(&root_user, AccessType::Read).is_ok());
        assert!(acl.check_permission(&root_user, AccessType::Write).is_ok());
    }

    #[test]
    fn test_acl_owner_permissions() {
        let acl = Acl::new(1000, 100, 0o700);
        let owner = meta::UserInfo {
            uid: 1000,
            gid: 100,
            gids: vec![],
        };
        assert!(acl.check_permission(&owner, AccessType::Read).is_ok());
        assert!(acl.check_permission(&owner, AccessType::Write).is_ok());
        assert!(acl.check_permission(&owner, AccessType::Exec).is_ok());

        let other = meta::UserInfo {
            uid: 2000,
            gid: 200,
            gids: vec![],
        };
        assert!(acl.check_permission(&other, AccessType::Read).is_err());
    }

    #[test]
    fn test_acl_group_permissions() {
        let acl = Acl::new(1000, 100, 0o070);
        let group_user = meta::UserInfo {
            uid: 2000,
            gid: 100,
            gids: vec![],
        };
        assert!(acl.check_permission(&group_user, AccessType::Read).is_ok());

        let supplementary = meta::UserInfo {
            uid: 3000,
            gid: 200,
            gids: vec![100],
        };
        assert!(supplementary
            .clone()
            .gids
            .contains(&100));
        assert!(acl
            .check_permission(&supplementary, AccessType::Read)
            .is_ok());
    }

    #[test]
    fn test_acl_other_permissions() {
        let acl = Acl::new(1000, 100, 0o004);
        let other = meta::UserInfo {
            uid: 2000,
            gid: 200,
            gids: vec![],
        };
        assert!(acl.check_permission(&other, AccessType::Read).is_ok());
        assert!(acl.check_permission(&other, AccessType::Write).is_err());
    }

    #[test]
    fn test_is_tree_root() {
        assert!(is_tree_root(ROOT_INODE_ID));
        assert!(is_tree_root(GC_ROOT_INODE_ID));
        assert!(!is_tree_root(42));
    }
}
