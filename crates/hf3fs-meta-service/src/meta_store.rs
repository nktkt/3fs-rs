//! MetaStore: the low-level metadata storage layer.
//!
//! This module provides the `MetaStore` struct which encapsulates all metadata
//! operations that run within KV transactions. Each operation creates the
//! appropriate objects and executes within the provided transaction.
//!
//! Based on 3FS/src/meta/store/MetaStore.h and MetaStore.cc

use std::sync::atomic::{AtomicU64, Ordering};

use hf3fs_kv::{ReadOnlyTransaction, ReadWriteTransaction};
use hf3fs_proto::meta;
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::inode::{Acl, Inode, InodeId, GC_ROOT_INODE_ID, ROOT_INODE_ID};
use crate::ops;

/// MetaStore provides all metadata operations that run within KV transactions.
///
/// It maintains a reference to configuration and an inode ID allocator counter.
/// In a full implementation, this would also hold references to components like
/// the Distributor, ChainAllocator, FileHelper, SessionManager, and GcManager.
pub struct MetaStore {
    config: MetaServiceConfig,
    /// Simple atomic counter for inode ID allocation. In production this would
    /// use a distributed allocator backed by the KV store.
    next_inode_id: AtomicU64,
}

impl MetaStore {
    /// Create a new MetaStore with the given configuration.
    pub fn new(config: MetaServiceConfig) -> Self {
        Self {
            config,
            // Start inode IDs after the reserved ones (0=root, 1=gc_root)
            next_inode_id: AtomicU64::new(2),
        }
    }

    /// Get a reference to the configuration.
    pub fn config(&self) -> &MetaServiceConfig {
        &self.config
    }

    /// Allocate a new unique inode ID.
    ///
    /// In production, this would use a distributed allocator. For now, uses
    /// a simple atomic counter.
    pub fn alloc_inode_id(&self) -> InodeId {
        self.next_inode_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the current timestamp in nanoseconds.
    fn now_ns(&self) -> i64 {
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(0)
    }

    // ---- File System Initialization ----

    /// Initialize the file system by creating the root and GC root inodes
    /// if they do not already exist.
    pub async fn init_fs(
        &self,
        txn: &mut dyn ReadWriteTransaction,
        root_layout: Option<meta::Layout>,
    ) -> Result<()> {
        let now_ns = self.now_ns();

        {
            // Check if root exists (use read-only view of the transaction)
            let root = Inode::load(&*txn as &dyn ReadOnlyTransaction, ROOT_INODE_ID).await?;
            if root.is_none() {
                let root_inode = Inode::new_directory(
                    ROOT_INODE_ID,
                    ROOT_INODE_ID, // root's parent is itself
                    "/",
                    &Acl::root(),
                    root_layout.clone(),
                    now_ns,
                );
                root_inode.store(txn).await?;
                tracing::info!("init_fs: created root inode");
            }
        }

        {
            // Check if GC root exists
            let gc_root = Inode::load(&*txn as &dyn ReadOnlyTransaction, GC_ROOT_INODE_ID).await?;
            if gc_root.is_none() {
                let gc_root_inode = Inode::new_directory(
                    GC_ROOT_INODE_ID,
                    GC_ROOT_INODE_ID, // GC root's parent is itself
                    "/",
                    &Acl::gc_root(),
                    None,
                    now_ns,
                );
                gc_root_inode.store(txn).await?;
                tracing::info!("init_fs: created GC root inode");
            }
        }

        Ok(())
    }

    // ---- Read-Only Operations ----

    /// Stat: retrieve inode metadata by path or inode ID.
    pub async fn stat(
        &self,
        txn: &dyn ReadOnlyTransaction,
        req: &meta::StatReq,
    ) -> Result<meta::StatRsp> {
        ops::stat::stat(txn, &self.config, req).await
    }

    /// Batch stat: retrieve multiple inodes by ID.
    pub async fn batch_stat(
        &self,
        txn: &dyn ReadOnlyTransaction,
        req: &meta::BatchStatReq,
    ) -> Result<meta::BatchStatRsp> {
        ops::stat::batch_stat(txn, &self.config, req).await
    }

    /// List: read directory entries.
    pub async fn list(
        &self,
        txn: &dyn ReadOnlyTransaction,
        req: &meta::ListReq,
    ) -> Result<meta::ListRsp> {
        ops::list::list(txn, &self.config, req).await
    }

    /// Open: open an existing file.
    pub async fn open(
        &self,
        txn: &dyn ReadOnlyTransaction,
        req: &meta::OpenReq,
    ) -> Result<meta::OpenRsp> {
        ops::open::open(txn, &self.config, req).await
    }

    // ---- Read-Write Operations ----

    /// Create: create a new file.
    pub async fn create(
        &self,
        txn: &mut dyn ReadWriteTransaction,
        req: &meta::CreateReq,
    ) -> Result<meta::CreateRsp> {
        let now_ns = self.now_ns();
        ops::create::create(txn, &self.config, req, now_ns, || self.alloc_inode_id())
            .await
    }

    /// Mkdirs: create one or more directories along a path.
    pub async fn mkdirs(
        &self,
        txn: &mut dyn ReadWriteTransaction,
        req: &meta::MkdirsReq,
    ) -> Result<meta::MkdirsRsp> {
        let now_ns = self.now_ns();
        ops::mkdirs::mkdirs(txn, &self.config, req, now_ns, || self.alloc_inode_id())
            .await
    }

    /// Symlink: create a symbolic link.
    pub async fn symlink(
        &self,
        txn: &mut dyn ReadWriteTransaction,
        req: &meta::SymlinkReq,
    ) -> Result<meta::SymlinkRsp> {
        let now_ns = self.now_ns();
        ops::symlink::symlink(txn, &self.config, req, now_ns, || self.alloc_inode_id())
            .await
    }

    /// Hard link: create an additional name for an existing file.
    pub async fn hard_link(
        &self,
        txn: &mut dyn ReadWriteTransaction,
        req: &meta::HardLinkReq,
    ) -> Result<meta::HardLinkRsp> {
        ops::hard_link::hard_link(txn, &self.config, req).await
    }

    /// Remove: unlink a file or remove a directory.
    pub async fn remove(
        &self,
        txn: &mut dyn ReadWriteTransaction,
        req: &meta::RemoveReq,
    ) -> Result<meta::RemoveRsp> {
        ops::remove::remove(txn, &self.config, req).await
    }

    /// Rename: move/rename a file or directory.
    pub async fn rename(
        &self,
        txn: &mut dyn ReadWriteTransaction,
        req: &meta::RenameReq,
    ) -> Result<meta::RenameRsp> {
        ops::rename::rename(txn, &self.config, req).await
    }

    /// SetAttr: modify inode attributes.
    pub async fn set_attr(
        &self,
        txn: &mut dyn ReadWriteTransaction,
        req: &meta::SetAttrReq,
    ) -> Result<meta::SetAttrRsp> {
        ops::set_attr::set_attr(txn, &self.config, req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alloc_inode_id() {
        let store = MetaStore::new(MetaServiceConfig::default());
        let id1 = store.alloc_inode_id();
        let id2 = store.alloc_inode_id();
        assert_eq!(id1, 2); // starts at 2 (0=root, 1=gc_root)
        assert_eq!(id2, 3);
    }

    #[test]
    fn test_default_config() {
        let store = MetaStore::new(MetaServiceConfig::default());
        assert_eq!(store.config().list_default_limit, 128);
        assert!(!store.config().readonly);
    }
}
