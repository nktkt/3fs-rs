//! Server-side directory entry implementation with KV store operations.
//!
//! Directory entries map a (parent_inode_id, name) pair to a child inode.
//! They are stored in the KV store with keys that enable efficient prefix-based
//! listing of directory contents.
//!
//! Based on 3FS/src/meta/store/DirEntry.h and DirEntry.cc

use hf3fs_kv::{
    GetRangeResult, KeySelector, ReadOnlyTransaction, ReadWriteTransaction,
};
use hf3fs_proto::meta::{self, InodeType};
use hf3fs_types::result::make_error_msg;
use hf3fs_types::status_code::{MetaCode, StatusCode};
use hf3fs_types::Result;

use crate::inode::{Acl, Inode, InodeId, ROOT_INODE_ID};
use crate::key_prefix;

/// Server-side directory entry with KV store operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    /// Parent directory inode ID.
    pub parent: InodeId,
    /// Entry name within the parent directory.
    pub name: String,
    /// Child inode ID this entry points to.
    pub inode_id: InodeId,
    /// Type of the child inode.
    pub inode_type: InodeType,
    /// For directory entries, cache the directory's ACL.
    pub dir_acl: Option<Acl>,
}

impl DirEntry {
    /// Create a directory entry for a file.
    pub fn new_file(parent: InodeId, name: String, inode_id: InodeId) -> Self {
        Self {
            parent,
            name,
            inode_id,
            inode_type: InodeType::File,
            dir_acl: None,
        }
    }

    /// Create a directory entry for a symlink.
    pub fn new_symlink(parent: InodeId, name: String, inode_id: InodeId) -> Self {
        Self {
            parent,
            name,
            inode_id,
            inode_type: InodeType::Symlink,
            dir_acl: None,
        }
    }

    /// Create a directory entry for a directory, with the directory's ACL cached.
    pub fn new_directory(
        parent: InodeId,
        name: String,
        inode_id: InodeId,
        acl: Acl,
    ) -> Self {
        Self {
            parent,
            name,
            inode_id,
            inode_type: InodeType::Directory,
            dir_acl: Some(acl),
        }
    }

    /// Create the root directory entry.
    pub fn root() -> Self {
        Self {
            parent: ROOT_INODE_ID,
            name: ".".to_string(),
            inode_id: ROOT_INODE_ID,
            inode_type: InodeType::Directory,
            dir_acl: Some(Acl::root()),
        }
    }

    /// Whether this entry is a file.
    pub fn is_file(&self) -> bool {
        self.inode_type == InodeType::File
    }

    /// Whether this entry is a directory.
    pub fn is_directory(&self) -> bool {
        self.inode_type == InodeType::Directory
    }

    /// Whether this entry is a symlink.
    pub fn is_symlink(&self) -> bool {
        self.inode_type == InodeType::Symlink
    }

    /// Pack the KV key for this directory entry.
    ///
    /// Key format: `[DIR_ENTRY_PREFIX] + [parent InodeId LE 8 bytes] + [name bytes]`
    pub fn pack_key(&self) -> Vec<u8> {
        Self::pack_key_for(self.parent, &self.name)
    }

    /// Pack the KV key for a given (parent, name) pair.
    pub fn pack_key_for(parent: InodeId, name: &str) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let mut key = Vec::with_capacity(1 + 8 + name_bytes.len());
        key.push(key_prefix::DIR_ENTRY_PREFIX);
        key.extend_from_slice(&parent.to_le_bytes());
        key.extend_from_slice(name_bytes);
        key
    }

    /// Pack the prefix key for listing entries under a parent directory.
    pub fn pack_prefix(parent: InodeId) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(9);
        prefix.push(key_prefix::DIR_ENTRY_PREFIX);
        prefix.extend_from_slice(&parent.to_le_bytes());
        prefix
    }

    /// Serialize the entry value for KV storage.
    fn pack_value(&self) -> Result<Vec<u8>> {
        let data = DirEntryData {
            inode_id: self.inode_id,
            inode_type: self.inode_type as u8,
            dir_acl: self.dir_acl.clone(),
        };
        serde_json::to_vec(&data).map_err(|e| {
            hf3fs_types::Status::with_message(
                StatusCode::DATA_CORRUPTION,
                format!("failed to serialize dir entry: {}", e),
            )
        })
    }

    /// Deserialize a directory entry from KV key and value.
    pub fn unpack(key: &[u8], value: &[u8]) -> Result<Self> {
        if key.len() < 9 || key[0] != key_prefix::DIR_ENTRY_PREFIX {
            return make_error_msg(
                StatusCode::DATA_CORRUPTION,
                "invalid dir entry key format",
            );
        }
        let parent = u64::from_le_bytes(
            key[1..9]
                .try_into()
                .map_err(|_| hf3fs_types::Status::new(StatusCode::DATA_CORRUPTION))?,
        );
        let name = std::str::from_utf8(&key[9..])
            .map_err(|_| {
                hf3fs_types::Status::with_message(
                    StatusCode::DATA_CORRUPTION,
                    "dir entry name is not valid UTF-8",
                )
            })?
            .to_string();

        let data: DirEntryData = serde_json::from_slice(value).map_err(|e| {
            hf3fs_types::Status::with_message(
                StatusCode::DATA_CORRUPTION,
                format!(
                    "failed to deserialize dir entry ({}, {}): {}",
                    parent, name, e
                ),
            )
        })?;

        let inode_type = InodeType::try_from(data.inode_type).map_err(|_| {
            hf3fs_types::Status::with_message(
                StatusCode::DATA_CORRUPTION,
                format!("invalid inode type {}", data.inode_type),
            )
        })?;

        Ok(Self {
            parent,
            name,
            inode_id: data.inode_id,
            inode_type,
            dir_acl: data.dir_acl,
        })
    }

    /// Load a directory entry from the KV store (snapshot, no conflict).
    pub async fn snapshot_load<T: ReadOnlyTransaction + ?Sized>(
        txn: &T,
        parent: InodeId,
        name: &str,
    ) -> Result<Option<Self>> {
        let key = Self::pack_key_for(parent, name);
        let value = txn.snapshot_get(&key).await?;
        match value {
            Some(v) => Ok(Some(Self::unpack(&key, &v)?)),
            None => Ok(None),
        }
    }

    /// Load a directory entry from the KV store (normal read).
    pub async fn load<T: ReadOnlyTransaction + ?Sized>(
        txn: &T,
        parent: InodeId,
        name: &str,
    ) -> Result<Option<Self>> {
        let key = Self::pack_key_for(parent, name);
        let value = txn.get(&key).await?;
        match value {
            Some(v) => Ok(Some(Self::unpack(&key, &v)?)),
            None => Ok(None),
        }
    }

    /// Store this directory entry into the KV store.
    pub async fn store(
        &self,
        txn: &mut dyn ReadWriteTransaction,
    ) -> Result<()> {
        let key = self.pack_key();
        let value = self.pack_value()?;
        txn.set(&key, &value).await
    }

    /// Remove this directory entry from the KV store.
    pub async fn remove(
        &self,
        txn: &mut dyn ReadWriteTransaction,
    ) -> Result<()> {
        let key = self.pack_key();
        txn.clear(&key).await
    }

    /// Add this entry's key to the read conflict set.
    pub async fn add_read_conflict(
        &self,
        txn: &mut dyn ReadWriteTransaction,
    ) -> Result<()> {
        let key = self.pack_key();
        txn.add_read_conflict(&key).await
    }

    /// Load the inode that this entry points to (snapshot read).
    pub async fn snapshot_load_inode<T: ReadOnlyTransaction + ?Sized>(
        &self,
        txn: &T,
    ) -> Result<Inode> {
        Inode::snapshot_load(txn, self.inode_id)
            .await?
            .ok_or_else(|| {
                hf3fs_types::Status::with_message(
                    MetaCode::NOT_FOUND,
                    format!("inode {} not found for entry {}", self.inode_id, self.name),
                )
            })
    }

    /// Convert to proto DirEntry.
    pub fn into_proto(self) -> meta::DirEntry {
        meta::DirEntry {
            name: self.name,
            inode_id: self.inode_id,
            inode_type: self.inode_type as u8,
        }
    }
}

/// Internal serialization format for directory entry values.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct DirEntryData {
    inode_id: u64,
    inode_type: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    dir_acl: Option<Acl>,
}

/// Result of listing directory entries.
#[derive(Debug, Clone)]
pub struct DirEntryList {
    /// The directory entries.
    pub entries: Vec<DirEntry>,
    /// Optionally loaded inodes corresponding to entries.
    pub inodes: Vec<Inode>,
    /// Whether there are more entries after this batch.
    pub more: bool,
}

impl DirEntryList {
    /// Load a batch of directory entries starting after `prev` name.
    ///
    /// If `prev` is empty, starts from the beginning.
    /// If `load_inodes` is true, also loads the inode for each entry.
    pub async fn snapshot_load<T: ReadOnlyTransaction + ?Sized>(
        txn: &T,
        parent: InodeId,
        prev: &str,
        limit: i32,
        load_inodes: bool,
    ) -> Result<Self> {
        let prefix = DirEntry::pack_prefix(parent);
        let begin_key = if prev.is_empty() {
            prefix.clone()
        } else {
            let mut k = DirEntry::pack_key_for(parent, prev);
            // Start after the prev key by appending a zero byte
            k.push(0);
            k
        };
        let end_key = hf3fs_kv::prefix_list_end_key(&prefix);

        let begin = KeySelector::new(begin_key, true);
        let end = KeySelector::new(end_key, false);

        let actual_limit = if limit <= 0 { 128 } else { limit };
        // Fetch one extra to know if there are more
        let fetch_limit = actual_limit + 1;

        let result: GetRangeResult = txn
            .snapshot_get_range(&begin, &end, fetch_limit)
            .await?;

        let more = result.kvs.len() > actual_limit as usize;
        let entries_count = std::cmp::min(result.kvs.len(), actual_limit as usize);
        let mut entries = Vec::with_capacity(entries_count);
        for kv in result.kvs.iter().take(entries_count) {
            entries.push(DirEntry::unpack(&kv.key, &kv.value)?);
        }

        let inodes = if load_inodes {
            let mut inodes = Vec::with_capacity(entries.len());
            for entry in &entries {
                match Inode::snapshot_load(txn, entry.inode_id).await? {
                    Some(inode) => inodes.push(inode),
                    None => {
                        return make_error_msg(
                            MetaCode::NOT_FOUND,
                            format!(
                                "inode {} not found for dir entry {}",
                                entry.inode_id, entry.name
                            ),
                        );
                    }
                }
            }
            inodes
        } else {
            Vec::new()
        };

        Ok(DirEntryList {
            entries,
            inodes,
            more,
        })
    }

    /// Check whether a directory is empty.
    pub async fn check_empty<T: ReadOnlyTransaction + ?Sized>(
        txn: &T,
        parent: InodeId,
    ) -> Result<bool> {
        let prefix = DirEntry::pack_prefix(parent);
        let end_key = hf3fs_kv::prefix_list_end_key(&prefix);
        let begin = KeySelector::new(prefix, true);
        let end = KeySelector::new(end_key, false);
        let result = txn.get_range(&begin, &end, 1).await?;
        Ok(result.kvs.is_empty())
    }

    /// Convert to proto ListRsp format.
    pub fn into_proto(self) -> meta::ListRsp {
        let entries = self
            .entries
            .into_iter()
            .map(|e| e.into_proto())
            .collect();
        let inodes = self
            .inodes
            .into_iter()
            .map(|i| i.into_proto())
            .collect();
        meta::ListRsp {
            entries,
            inodes,
            more: self.more,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_key() {
        let entry = DirEntry::new_file(42, "hello.txt".to_string(), 100);
        let key = entry.pack_key();
        assert_eq!(key[0], key_prefix::DIR_ENTRY_PREFIX);
        let parent = u64::from_le_bytes(key[1..9].try_into().unwrap());
        assert_eq!(parent, 42);
        assert_eq!(&key[9..], b"hello.txt");
    }

    #[test]
    fn test_serialize_roundtrip() {
        let entry = DirEntry::new_file(42, "test.txt".to_string(), 100);
        let key = entry.pack_key();
        let value = entry.pack_value().unwrap();
        let restored = DirEntry::unpack(&key, &value).unwrap();
        assert_eq!(entry, restored);
    }

    #[test]
    fn test_directory_entry_with_acl() {
        let acl = Acl::new(1000, 100, 0o755);
        let entry = DirEntry::new_directory(0, "subdir".to_string(), 50, acl.clone());
        let key = entry.pack_key();
        let value = entry.pack_value().unwrap();
        let restored = DirEntry::unpack(&key, &value).unwrap();
        assert_eq!(restored.dir_acl, Some(acl));
        assert!(restored.is_directory());
    }

    #[test]
    fn test_root_entry() {
        let root = DirEntry::root();
        assert_eq!(root.parent, ROOT_INODE_ID);
        assert_eq!(root.name, ".");
        assert_eq!(root.inode_id, ROOT_INODE_ID);
        assert!(root.is_directory());
    }

    #[test]
    fn test_prefix() {
        let prefix = DirEntry::pack_prefix(42);
        assert_eq!(prefix.len(), 9);
        assert_eq!(prefix[0], key_prefix::DIR_ENTRY_PREFIX);
        let parent = u64::from_le_bytes(prefix[1..9].try_into().unwrap());
        assert_eq!(parent, 42);
    }

    #[test]
    fn test_into_proto() {
        let entry = DirEntry::new_file(0, "foo.txt".to_string(), 123);
        let proto = entry.into_proto();
        assert_eq!(proto.name, "foo.txt");
        assert_eq!(proto.inode_id, 123);
        assert_eq!(proto.inode_type, InodeType::File as u8);
    }
}
