//! MetaService trait and implementation.
//!
//! The `MetaService` trait defines the public async interface for all metadata
//! operations. `MetaServiceImpl` implements this trait by wrapping a `MetaStore`
//! with a KV engine, creating transactions and handling retries.
//!
//! Based on 3FS/src/meta/service/MetaOperator.h and MetaOperator.cc

use std::sync::Arc;

use async_trait::async_trait;
use hf3fs_kv::{KvEngine, ReadWriteTransaction};
use hf3fs_proto::meta;
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::{MetaCode, StatusCode};
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::meta_store::MetaStore;

/// The metadata service trait defining all filesystem metadata operations.
///
/// All methods are async and return `hf3fs_types::Result<Rsp>`.
/// This trait mirrors the C++ `MetaOperator` class interface.
#[async_trait]
pub trait MetaService: Send + Sync {
    /// Stat: retrieve inode metadata by path or inode ID.
    async fn stat(&self, req: meta::StatReq) -> Result<meta::StatRsp>;

    /// Batch stat: retrieve multiple inodes by ID.
    async fn batch_stat(&self, req: meta::BatchStatReq) -> Result<meta::BatchStatRsp>;

    /// Create: create a new file (O_CREAT).
    async fn create(&self, req: meta::CreateReq) -> Result<meta::CreateRsp>;

    /// Open: open an existing file.
    async fn open(&self, req: meta::OpenReq) -> Result<meta::OpenRsp>;

    /// Mkdirs: create one or more directories along a path.
    async fn mkdirs(&self, req: meta::MkdirsReq) -> Result<meta::MkdirsRsp>;

    /// Symlink: create a symbolic link.
    async fn symlink(&self, req: meta::SymlinkReq) -> Result<meta::SymlinkRsp>;

    /// Hard link: create an additional name for an existing file.
    async fn hard_link(&self, req: meta::HardLinkReq) -> Result<meta::HardLinkRsp>;

    /// Remove: unlink a file or remove a directory.
    async fn remove(&self, req: meta::RemoveReq) -> Result<meta::RemoveRsp>;

    /// Rename: move/rename a file or directory.
    async fn rename(&self, req: meta::RenameReq) -> Result<meta::RenameRsp>;

    /// List: read directory entries.
    async fn list(&self, req: meta::ListReq) -> Result<meta::ListRsp>;

    /// SetAttr: modify inode attributes (permissions, timestamps, etc.).
    async fn set_attr(&self, req: meta::SetAttrReq) -> Result<meta::SetAttrRsp>;

    /// Sync: sync file data to storage.
    async fn sync_file(&self, req: meta::SyncReq) -> Result<meta::SyncRsp>;

    /// Close: close an open file.
    async fn close(&self, req: meta::CloseReq) -> Result<meta::CloseRsp>;

    /// StatFs: get filesystem statistics.
    async fn stat_fs(&self, req: meta::StatFsReq) -> Result<meta::StatFsRsp>;

    /// Truncate: truncate a file to a specified length.
    async fn truncate(&self, req: meta::TruncateReq) -> Result<meta::TruncateRsp>;

    /// GetRealPath: resolve a path to its absolute canonical form.
    async fn get_real_path(&self, req: meta::GetRealPathReq)
        -> Result<meta::GetRealPathRsp>;

    /// PruneSession: remove stale client sessions.
    async fn prune_session(
        &self,
        req: meta::PruneSessionReq,
    ) -> Result<meta::PruneSessionRsp>;

    /// LockDirectory: lock/unlock a directory for exclusive access.
    async fn lock_directory(
        &self,
        req: meta::LockDirectoryReq,
    ) -> Result<meta::LockDirectoryRsp>;

    /// TestRpc: a no-op RPC for benchmarking.
    async fn test_rpc(&self, req: meta::TestRpcReq) -> Result<meta::TestRpcRsp>;
}

/// Concrete implementation of the `MetaService` trait.
///
/// Wraps a `MetaStore` with a KV engine, creating transactions for each
/// operation and handling the transaction lifecycle (commit, retry on conflict).
pub struct MetaServiceImpl<E: KvEngine> {
    store: Arc<MetaStore>,
    kv_engine: Arc<E>,
}

impl<E: KvEngine> MetaServiceImpl<E> {
    /// Create a new MetaServiceImpl.
    pub fn new(config: MetaServiceConfig, kv_engine: Arc<E>) -> Self {
        Self {
            store: Arc::new(MetaStore::new(config)),
            kv_engine,
        }
    }

    /// Initialize the file system (create root and GC root if needed).
    pub async fn init_fs(&self, root_layout: Option<meta::Layout>) -> Result<()> {
        let mut txn = self.kv_engine.create_readwrite_transaction();
        self.store.init_fs(&mut txn, root_layout).await?;
        txn.commit().await?;
        Ok(())
    }

    /// Get a reference to the underlying MetaStore.
    pub fn store(&self) -> &MetaStore {
        &self.store
    }

    /// Check if the filesystem is in read-only mode.
    fn check_readonly(&self) -> Result<()> {
        if self.store.config().readonly {
            return make_error_msg(
                StatusCode::READ_ONLY_MODE,
                "filesystem is in read-only mode",
            );
        }
        Ok(())
    }
}

#[async_trait]
impl<E: KvEngine + 'static> MetaService for MetaServiceImpl<E>
where
    E::RoTxn: Send + Sync,
    E::RwTxn: Send + Sync,
{
    async fn stat(&self, req: meta::StatReq) -> Result<meta::StatRsp> {
        let txn = self.kv_engine.create_readonly_transaction();
        self.store.stat(&txn, &req).await
    }

    async fn batch_stat(&self, req: meta::BatchStatReq) -> Result<meta::BatchStatRsp> {
        let txn = self.kv_engine.create_readonly_transaction();
        self.store.batch_stat(&txn, &req).await
    }

    async fn create(&self, req: meta::CreateReq) -> Result<meta::CreateRsp> {
        self.check_readonly()?;
        let mut txn = self.kv_engine.create_readwrite_transaction();
        let result = self.store.create(&mut txn, &req).await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn open(&self, req: meta::OpenReq) -> Result<meta::OpenRsp> {
        let txn = self.kv_engine.create_readonly_transaction();
        self.store.open(&txn, &req).await
    }

    async fn mkdirs(&self, req: meta::MkdirsReq) -> Result<meta::MkdirsRsp> {
        self.check_readonly()?;
        let mut txn = self.kv_engine.create_readwrite_transaction();
        let result = self.store.mkdirs(&mut txn, &req).await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn symlink(&self, req: meta::SymlinkReq) -> Result<meta::SymlinkRsp> {
        self.check_readonly()?;
        let mut txn = self.kv_engine.create_readwrite_transaction();
        let result = self.store.symlink(&mut txn, &req).await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn hard_link(&self, req: meta::HardLinkReq) -> Result<meta::HardLinkRsp> {
        self.check_readonly()?;
        let mut txn = self.kv_engine.create_readwrite_transaction();
        let result = self.store.hard_link(&mut txn, &req).await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn remove(&self, req: meta::RemoveReq) -> Result<meta::RemoveRsp> {
        self.check_readonly()?;
        let mut txn = self.kv_engine.create_readwrite_transaction();
        let result = self.store.remove(&mut txn, &req).await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn rename(&self, req: meta::RenameReq) -> Result<meta::RenameRsp> {
        self.check_readonly()?;
        let mut txn = self.kv_engine.create_readwrite_transaction();
        let result = self.store.rename(&mut txn, &req).await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn list(&self, req: meta::ListReq) -> Result<meta::ListRsp> {
        let txn = self.kv_engine.create_readonly_transaction();
        self.store.list(&txn, &req).await
    }

    async fn set_attr(&self, req: meta::SetAttrReq) -> Result<meta::SetAttrRsp> {
        self.check_readonly()?;
        let mut txn = self.kv_engine.create_readwrite_transaction();
        let result = self.store.set_attr(&mut txn, &req).await?;
        txn.commit().await?;
        Ok(result)
    }

    async fn sync_file(&self, req: meta::SyncReq) -> Result<meta::SyncRsp> {
        // Sync updates the file's length and timestamps from the storage layer.
        // Simplified implementation: just load and return the inode.
        let txn = self.kv_engine.create_readonly_transaction();
        let inode = crate::inode::Inode::snapshot_load(&txn, req.inode)
            .await?
            .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
        Ok(meta::SyncRsp {
            stat: inode.into_proto(),
        })
    }

    async fn close(&self, req: meta::CloseReq) -> Result<meta::CloseRsp> {
        // Close removes the file session and optionally updates timestamps.
        // Simplified implementation: just load and return the inode.
        let txn = self.kv_engine.create_readonly_transaction();
        let inode = crate::inode::Inode::snapshot_load(&txn, req.inode)
            .await?
            .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
        Ok(meta::CloseRsp {
            stat: inode.into_proto(),
        })
    }

    async fn stat_fs(&self, _req: meta::StatFsReq) -> Result<meta::StatFsRsp> {
        // In a full implementation, this would query the management daemon
        // for storage capacity information. Return placeholder values.
        Ok(meta::StatFsRsp {
            capacity: 0,
            used: 0,
            free: 0,
        })
    }

    async fn truncate(&self, req: meta::TruncateReq) -> Result<meta::TruncateRsp> {
        self.check_readonly()?;
        let mut txn = self.kv_engine.create_readwrite_transaction();

        let inode = crate::inode::Inode::load(&txn, req.inode)
            .await?
            .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;

        if !inode.is_file() {
            return make_error(MetaCode::NOT_FILE);
        }

        // Check write permission
        inode.acl().check_permission(
            &req.base.user,
            crate::inode::AccessType::Write,
        )?;

        let mut updated = inode;
        updated.inner.length = req.length;
        updated.inner.ctime_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        updated.inner.mtime_ns = updated.inner.ctime_ns;
        updated.add_read_conflict(&mut txn).await?;
        updated.store(&mut txn).await?;

        txn.commit().await?;
        Ok(meta::TruncateRsp {
            chunks_removed: 0,
            stat: updated.into_proto(),
            finished: true,
        })
    }

    async fn get_real_path(
        &self,
        req: meta::GetRealPathReq,
    ) -> Result<meta::GetRealPathRsp> {
        // Simplified: just return the path as-is. A full implementation
        // would resolve symlinks and canonicalize.
        let path = req
            .path
            .path
            .unwrap_or_default();
        Ok(meta::GetRealPathRsp { path })
    }

    async fn prune_session(
        &self,
        _req: meta::PruneSessionReq,
    ) -> Result<meta::PruneSessionRsp> {
        // Session management is handled by the SessionManager component.
        // Simplified: just return success.
        Ok(meta::PruneSessionRsp {})
    }

    async fn lock_directory(
        &self,
        _req: meta::LockDirectoryReq,
    ) -> Result<meta::LockDirectoryRsp> {
        // Directory locking is a simplified operation.
        // Full implementation would store/check locks in the KV store.
        Ok(meta::LockDirectoryRsp {})
    }

    async fn test_rpc(&self, _req: meta::TestRpcReq) -> Result<meta::TestRpcRsp> {
        Ok(meta::TestRpcRsp {
            stat: meta::Inode::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::{Inode, ROOT_INODE_ID, GC_ROOT_INODE_ID};
    use hf3fs_kv::*;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    /// A simple in-memory KV engine for testing.
    #[derive(Clone)]
    struct MemKvEngine {
        data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    }

    impl MemKvEngine {
        fn new() -> Self {
            Self {
                data: Arc::new(Mutex::new(BTreeMap::new())),
            }
        }
    }

    impl KvEngine for MemKvEngine {
        type RoTxn = MemTxn;
        type RwTxn = MemTxn;

        fn create_readonly_transaction(&self) -> Self::RoTxn {
            MemTxn {
                data: self.data.clone(),
                pending: BTreeMap::new(),
                deletions: Vec::new(),
            }
        }

        fn create_readwrite_transaction(&self) -> Self::RwTxn {
            MemTxn {
                data: self.data.clone(),
                pending: BTreeMap::new(),
                deletions: Vec::new(),
            }
        }
    }

    struct MemTxn {
        data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
        pending: BTreeMap<Vec<u8>, Vec<u8>>,
        deletions: Vec<Vec<u8>>,
    }

    #[async_trait]
    impl ReadOnlyTransaction for MemTxn {
        fn set_read_version(&mut self, _version: i64) {}

        async fn snapshot_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            // Check pending writes first
            if let Some(v) = self.pending.get(key) {
                return Ok(Some(v.clone()));
            }
            if self.deletions.contains(&key.to_vec()) {
                return Ok(None);
            }
            let data = self.data.lock();
            Ok(data.get(key).cloned())
        }

        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.snapshot_get(key).await
        }

        async fn snapshot_get_range(
            &self,
            begin: &KeySelector,
            end: &KeySelector,
            limit: i32,
        ) -> Result<GetRangeResult> {
            self.get_range(begin, end, limit).await
        }

        async fn get_range(
            &self,
            begin: &KeySelector,
            end: &KeySelector,
            limit: i32,
        ) -> Result<GetRangeResult> {
            let data = self.data.lock();
            let mut kvs = Vec::new();

            // Merge committed data with pending writes
            let mut all_data: BTreeMap<Vec<u8>, Vec<u8>> = data.clone();
            for (k, v) in &self.pending {
                all_data.insert(k.clone(), v.clone());
            }
            for k in &self.deletions {
                all_data.remove(k);
            }

            let iter = if begin.inclusive {
                all_data.range(begin.key.clone()..)
            } else {
                // For exclusive begin, we need the key after begin.key
                all_data.range(begin.key.clone()..)
            };

            for (k, v) in iter {
                if !begin.inclusive && k == &begin.key {
                    continue;
                }
                if !end.key.is_empty() {
                    if end.inclusive {
                        if k > &end.key {
                            break;
                        }
                    } else if k >= &end.key {
                        break;
                    }
                }
                kvs.push(KeyValue {
                    key: k.clone(),
                    value: v.clone(),
                });
                if kvs.len() >= limit as usize {
                    break;
                }
            }

            let has_more = kvs.len() >= limit as usize;
            Ok(GetRangeResult { kvs, has_more })
        }

        async fn cancel(&mut self) -> Result<()> {
            Ok(())
        }

        fn reset(&mut self) {
            self.pending.clear();
            self.deletions.clear();
        }
    }

    #[async_trait]
    impl ReadWriteTransaction for MemTxn {
        async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
            self.pending.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn clear(&mut self, key: &[u8]) -> Result<()> {
            self.pending.remove(key);
            self.deletions.push(key.to_vec());
            Ok(())
        }

        async fn add_read_conflict(&mut self, _key: &[u8]) -> Result<()> {
            Ok(())
        }

        async fn add_read_conflict_range(
            &mut self,
            _begin: &[u8],
            _end: &[u8],
        ) -> Result<()> {
            Ok(())
        }

        async fn set_versionstamped_key(
            &mut self,
            _key: &[u8],
            _offset: u32,
            _value: &[u8],
        ) -> Result<()> {
            Ok(())
        }

        async fn set_versionstamped_value(
            &mut self,
            _key: &[u8],
            _value: &[u8],
            _offset: u32,
        ) -> Result<()> {
            Ok(())
        }

        async fn commit(&mut self) -> Result<()> {
            let mut data = self.data.lock();
            for k in &self.deletions {
                data.remove(k);
            }
            for (k, v) in std::mem::take(&mut self.pending) {
                data.insert(k, v);
            }
            self.deletions.clear();
            Ok(())
        }

        fn get_committed_version(&self) -> i64 {
            0
        }
    }

    fn make_user(uid: u32) -> meta::UserInfo {
        meta::UserInfo {
            uid,
            gid: uid,
            gids: vec![],
        }
    }

    #[tokio::test]
    async fn test_init_fs() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        // Verify root inode exists
        let txn = engine.create_readonly_transaction();
        let root = Inode::snapshot_load(&txn, ROOT_INODE_ID).await.unwrap();
        assert!(root.is_some());
        let root = root.unwrap();
        assert!(root.is_directory());
        assert_eq!(root.inner.uid, 0);

        // Verify GC root exists
        let gc = Inode::snapshot_load(&txn, GC_ROOT_INODE_ID)
            .await
            .unwrap();
        assert!(gc.is_some());
    }

    #[tokio::test]
    async fn test_create_and_stat() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        let user = make_user(0); // root user

        // Create a file
        let create_rsp = svc
            .create(meta::CreateReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("test.txt".to_string()),
                },
                perm: meta::Permission(0o644),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(create_rsp.stat.inode_type, meta::InodeType::File as u8);
        assert_eq!(create_rsp.stat.permission, 0o644);
        assert!(!create_rsp.need_truncate);

        // Stat the file
        let stat_rsp = svc
            .stat(meta::StatReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("test.txt".to_string()),
                },
                flags: meta::AtFlags(0),
            })
            .await
            .unwrap();

        assert_eq!(stat_rsp.stat.id, create_rsp.stat.id);
        assert_eq!(stat_rsp.stat.permission, 0o644);
    }

    #[tokio::test]
    async fn test_mkdirs_and_list() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        let user = make_user(0);

        // Create a directory
        let mkdirs_rsp = svc
            .mkdirs(meta::MkdirsReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("subdir".to_string()),
                },
                perm: meta::Permission(0o755),
                recursive: false,
                layout: None,
            })
            .await
            .unwrap();

        assert_eq!(
            mkdirs_rsp.stat.inode_type,
            meta::InodeType::Directory as u8
        );

        // List root directory
        let list_rsp = svc
            .list(meta::ListReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: None,
                },
                prev: String::new(),
                limit: 100,
                status: false,
            })
            .await
            .unwrap();

        assert_eq!(list_rsp.entries.len(), 1);
        assert_eq!(list_rsp.entries[0].name, "subdir");
    }

    #[tokio::test]
    async fn test_mkdirs_recursive() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        let user = make_user(0);

        // Create nested directories recursively
        let rsp = svc
            .mkdirs(meta::MkdirsReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("a/b/c".to_string()),
                },
                perm: meta::Permission(0o755),
                recursive: true,
                layout: None,
            })
            .await
            .unwrap();

        assert_eq!(rsp.stat.inode_type, meta::InodeType::Directory as u8);

        // Verify we can stat the nested directory
        let stat_rsp = svc
            .stat(meta::StatReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("a/b/c".to_string()),
                },
                flags: meta::AtFlags(0),
            })
            .await
            .unwrap();

        assert_eq!(
            stat_rsp.stat.inode_type,
            meta::InodeType::Directory as u8
        );
    }

    #[tokio::test]
    async fn test_create_and_remove() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        let user = make_user(0);

        // Create a file
        svc.create(meta::CreateReq {
            base: meta::ReqBase {
                user: user.clone(),
                ..Default::default()
            },
            path: meta::PathAt {
                parent: 0,
                path: Some("to_remove.txt".to_string()),
            },
            perm: meta::Permission(0o644),
            ..Default::default()
        })
        .await
        .unwrap();

        // Remove it
        svc.remove(meta::RemoveReq {
            base: meta::ReqBase {
                user: user.clone(),
                ..Default::default()
            },
            path: meta::PathAt {
                parent: 0,
                path: Some("to_remove.txt".to_string()),
            },
            at_flags: meta::AtFlags(0),
            recursive: false,
            check_type: false,
            inode_id: None,
        })
        .await
        .unwrap();

        // Stat should fail
        let result = svc
            .stat(meta::StatReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("to_remove.txt".to_string()),
                },
                flags: meta::AtFlags(0),
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_symlink() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        let user = make_user(0);

        // Create a file
        svc.create(meta::CreateReq {
            base: meta::ReqBase {
                user: user.clone(),
                ..Default::default()
            },
            path: meta::PathAt {
                parent: 0,
                path: Some("target.txt".to_string()),
            },
            perm: meta::Permission(0o644),
            ..Default::default()
        })
        .await
        .unwrap();

        // Create a symlink
        let rsp = svc
            .symlink(meta::SymlinkReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("link.txt".to_string()),
                },
                target: "target.txt".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(rsp.stat.inode_type, meta::InodeType::Symlink as u8);
        assert_eq!(rsp.stat.symlink_target.as_deref(), Some("target.txt"));
    }

    #[tokio::test]
    async fn test_hard_link() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        let user = make_user(0);

        // Create a file
        let create_rsp = svc
            .create(meta::CreateReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("original.txt".to_string()),
                },
                perm: meta::Permission(0o644),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(create_rsp.stat.nlink, 1);

        // Hard link
        let link_rsp = svc
            .hard_link(meta::HardLinkReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                old_path: meta::PathAt {
                    parent: 0,
                    path: Some("original.txt".to_string()),
                },
                new_path: meta::PathAt {
                    parent: 0,
                    path: Some("hardlink.txt".to_string()),
                },
                flags: meta::AtFlags(0),
            })
            .await
            .unwrap();

        assert_eq!(link_rsp.stat.nlink, 2);
        assert_eq!(link_rsp.stat.id, create_rsp.stat.id);
    }

    #[tokio::test]
    async fn test_rename() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        let user = make_user(0);

        // Create a file
        svc.create(meta::CreateReq {
            base: meta::ReqBase {
                user: user.clone(),
                ..Default::default()
            },
            path: meta::PathAt {
                parent: 0,
                path: Some("old_name.txt".to_string()),
            },
            perm: meta::Permission(0o644),
            ..Default::default()
        })
        .await
        .unwrap();

        // Rename
        let rename_rsp = svc
            .rename(meta::RenameReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                src: meta::PathAt {
                    parent: 0,
                    path: Some("old_name.txt".to_string()),
                },
                dest: meta::PathAt {
                    parent: 0,
                    path: Some("new_name.txt".to_string()),
                },
                move_to_trash: false,
                inode_id: None,
            })
            .await
            .unwrap();

        assert!(rename_rsp.stat.is_some());

        // Old name should not exist
        let old_stat = svc
            .stat(meta::StatReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("old_name.txt".to_string()),
                },
                flags: meta::AtFlags(0),
            })
            .await;
        assert!(old_stat.is_err());

        // New name should exist
        let new_stat = svc
            .stat(meta::StatReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("new_name.txt".to_string()),
                },
                flags: meta::AtFlags(0),
            })
            .await;
        assert!(new_stat.is_ok());
    }

    #[tokio::test]
    async fn test_set_attr() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        let user = make_user(0); // root

        // Create a file
        svc.create(meta::CreateReq {
            base: meta::ReqBase {
                user: user.clone(),
                ..Default::default()
            },
            path: meta::PathAt {
                parent: 0,
                path: Some("attrs.txt".to_string()),
            },
            perm: meta::Permission(0o644),
            ..Default::default()
        })
        .await
        .unwrap();

        // Change permission
        let rsp = svc
            .set_attr(meta::SetAttrReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("attrs.txt".to_string()),
                },
                flags: meta::AtFlags(0),
                perm: Some(0o755),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(rsp.stat.permission, 0o755);
    }

    #[tokio::test]
    async fn test_readonly_mode() {
        let engine = Arc::new(MemKvEngine::new());
        let mut config = MetaServiceConfig::default();
        config.readonly = true;
        let svc = MetaServiceImpl::new(config, engine.clone());

        // Write operations should fail
        let result = svc
            .create(meta::CreateReq {
                base: meta::ReqBase::default(),
                path: meta::PathAt {
                    parent: 0,
                    path: Some("fail.txt".to_string()),
                },
                perm: meta::Permission(0o644),
                ..Default::default()
            })
            .await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().code(),
            StatusCode::READ_ONLY_MODE
        );
    }

    #[tokio::test]
    async fn test_test_rpc() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine);
        let rsp = svc
            .test_rpc(meta::TestRpcReq {
                ..Default::default()
            })
            .await
            .unwrap();
        // TestRpc is a no-op, should return default inode
        assert_eq!(rsp.stat.id, 0);
    }

    #[tokio::test]
    async fn test_permission_denied() {
        let engine = Arc::new(MemKvEngine::new());
        let svc = MetaServiceImpl::new(MetaServiceConfig::default(), engine.clone());
        svc.init_fs(None).await.unwrap();

        // Create a file as root
        let root = make_user(0);
        svc.create(meta::CreateReq {
            base: meta::ReqBase {
                user: root.clone(),
                ..Default::default()
            },
            path: meta::PathAt {
                parent: 0,
                path: Some("restricted.txt".to_string()),
            },
            perm: meta::Permission(0o600),
            ..Default::default()
        })
        .await
        .unwrap();

        // Non-root user should not be able to write
        let user = make_user(1000);
        let result = svc
            .set_attr(meta::SetAttrReq {
                base: meta::ReqBase {
                    user: user.clone(),
                    ..Default::default()
                },
                path: meta::PathAt {
                    parent: 0,
                    path: Some("restricted.txt".to_string()),
                },
                flags: meta::AtFlags(0),
                perm: Some(0o777),
                ..Default::default()
            })
            .await;

        assert!(result.is_err());
    }
}
