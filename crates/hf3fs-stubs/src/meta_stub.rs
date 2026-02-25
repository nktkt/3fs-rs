//! Meta service stub trait and mock implementation.

use async_trait::async_trait;
use hf3fs_proto::meta::{
    AuthReq, AuthRsp, BatchStatByPathReq, BatchStatByPathRsp, BatchStatReq, BatchStatRsp,
    CloseReq, CloseRsp, CreateReq, CreateRsp, DropUserCacheReq, DropUserCacheRsp, GetRealPathReq,
    GetRealPathRsp, HardLinkReq, HardLinkRsp, ListReq, ListRsp, LockDirectoryReq,
    LockDirectoryRsp, MkdirsReq, MkdirsRsp, OpenReq, OpenRsp, PruneSessionReq, PruneSessionRsp,
    RemoveReq, RemoveRsp, RenameReq, RenameRsp, SetAttrReq, SetAttrRsp, StatFsReq, StatFsRsp,
    StatReq, StatRsp, SymlinkReq, SymlinkRsp, SyncReq, SyncRsp, TestRpcReq, TestRpcRsp,
    TruncateReq, TruncateRsp,
};
use hf3fs_proto::meta::types::Inode;
use hf3fs_types::Result;
use parking_lot::Mutex;
use std::sync::Arc;

/// Client-side stub for calling the meta service.
///
/// The meta service is responsible for all namespace (inode/directory)
/// operations in the distributed file system.
#[async_trait]
pub trait IMetaServiceStub: Send + Sync {
    async fn auth(&self, req: AuthReq) -> Result<AuthRsp>;
    async fn stat_fs(&self, req: StatFsReq) -> Result<StatFsRsp>;
    async fn stat(&self, req: StatReq) -> Result<StatRsp>;
    async fn batch_stat(&self, req: BatchStatReq) -> Result<BatchStatRsp>;
    async fn batch_stat_by_path(&self, req: BatchStatByPathReq) -> Result<BatchStatByPathRsp>;
    async fn create(&self, req: CreateReq) -> Result<CreateRsp>;
    async fn mkdirs(&self, req: MkdirsReq) -> Result<MkdirsRsp>;
    async fn symlink(&self, req: SymlinkReq) -> Result<SymlinkRsp>;
    async fn hard_link(&self, req: HardLinkReq) -> Result<HardLinkRsp>;
    async fn remove(&self, req: RemoveReq) -> Result<RemoveRsp>;
    async fn open(&self, req: OpenReq) -> Result<OpenRsp>;
    async fn sync(&self, req: SyncReq) -> Result<SyncRsp>;
    async fn close(&self, req: CloseReq) -> Result<CloseRsp>;
    async fn rename(&self, req: RenameReq) -> Result<RenameRsp>;
    async fn list(&self, req: ListReq) -> Result<ListRsp>;
    async fn truncate(&self, req: TruncateReq) -> Result<TruncateRsp>;
    async fn get_real_path(&self, req: GetRealPathReq) -> Result<GetRealPathRsp>;
    async fn set_attr(&self, req: SetAttrReq) -> Result<SetAttrRsp>;
    async fn prune_session(&self, req: PruneSessionReq) -> Result<PruneSessionRsp>;
    async fn drop_user_cache(&self, req: DropUserCacheReq) -> Result<DropUserCacheRsp>;
    async fn lock_directory(&self, req: LockDirectoryReq) -> Result<LockDirectoryRsp>;
    async fn test_rpc(&self, req: TestRpcReq) -> Result<TestRpcRsp>;
}

// ---------------------------------------------------------------------------
// Mock implementation
// ---------------------------------------------------------------------------

type Handler<Req, Rsp> = Box<dyn Fn(Req) -> Result<Rsp> + Send + Sync>;

/// A configurable mock for [`IMetaServiceStub`].
///
/// Each RPC method can be overridden with a closure. If no handler is
/// installed the mock returns a default (success) response.
pub struct MockMetaServiceStub {
    pub auth_handler: Mutex<Option<Handler<AuthReq, AuthRsp>>>,
    pub stat_fs_handler: Mutex<Option<Handler<StatFsReq, StatFsRsp>>>,
    pub stat_handler: Mutex<Option<Handler<StatReq, StatRsp>>>,
    pub batch_stat_handler: Mutex<Option<Handler<BatchStatReq, BatchStatRsp>>>,
    pub batch_stat_by_path_handler: Mutex<Option<Handler<BatchStatByPathReq, BatchStatByPathRsp>>>,
    pub create_handler: Mutex<Option<Handler<CreateReq, CreateRsp>>>,
    pub mkdirs_handler: Mutex<Option<Handler<MkdirsReq, MkdirsRsp>>>,
    pub symlink_handler: Mutex<Option<Handler<SymlinkReq, SymlinkRsp>>>,
    pub hard_link_handler: Mutex<Option<Handler<HardLinkReq, HardLinkRsp>>>,
    pub remove_handler: Mutex<Option<Handler<RemoveReq, RemoveRsp>>>,
    pub open_handler: Mutex<Option<Handler<OpenReq, OpenRsp>>>,
    pub sync_handler: Mutex<Option<Handler<SyncReq, SyncRsp>>>,
    pub close_handler: Mutex<Option<Handler<CloseReq, CloseRsp>>>,
    pub rename_handler: Mutex<Option<Handler<RenameReq, RenameRsp>>>,
    pub list_handler: Mutex<Option<Handler<ListReq, ListRsp>>>,
    pub truncate_handler: Mutex<Option<Handler<TruncateReq, TruncateRsp>>>,
    pub get_real_path_handler: Mutex<Option<Handler<GetRealPathReq, GetRealPathRsp>>>,
    pub set_attr_handler: Mutex<Option<Handler<SetAttrReq, SetAttrRsp>>>,
    pub prune_session_handler: Mutex<Option<Handler<PruneSessionReq, PruneSessionRsp>>>,
    pub drop_user_cache_handler: Mutex<Option<Handler<DropUserCacheReq, DropUserCacheRsp>>>,
    pub lock_directory_handler: Mutex<Option<Handler<LockDirectoryReq, LockDirectoryRsp>>>,
    pub test_rpc_handler: Mutex<Option<Handler<TestRpcReq, TestRpcRsp>>>,
}

impl MockMetaServiceStub {
    pub fn new() -> Self {
        Self {
            auth_handler: Mutex::new(None),
            stat_fs_handler: Mutex::new(None),
            stat_handler: Mutex::new(None),
            batch_stat_handler: Mutex::new(None),
            batch_stat_by_path_handler: Mutex::new(None),
            create_handler: Mutex::new(None),
            mkdirs_handler: Mutex::new(None),
            symlink_handler: Mutex::new(None),
            hard_link_handler: Mutex::new(None),
            remove_handler: Mutex::new(None),
            open_handler: Mutex::new(None),
            sync_handler: Mutex::new(None),
            close_handler: Mutex::new(None),
            rename_handler: Mutex::new(None),
            list_handler: Mutex::new(None),
            truncate_handler: Mutex::new(None),
            get_real_path_handler: Mutex::new(None),
            set_attr_handler: Mutex::new(None),
            prune_session_handler: Mutex::new(None),
            drop_user_cache_handler: Mutex::new(None),
            lock_directory_handler: Mutex::new(None),
            test_rpc_handler: Mutex::new(None),
        }
    }

    /// Wrap in an `Arc` for convenient sharing.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn on_auth(&self, f: impl Fn(AuthReq) -> Result<AuthRsp> + Send + Sync + 'static) {
        *self.auth_handler.lock() = Some(Box::new(f));
    }

    pub fn on_stat_fs(
        &self,
        f: impl Fn(StatFsReq) -> Result<StatFsRsp> + Send + Sync + 'static,
    ) {
        *self.stat_fs_handler.lock() = Some(Box::new(f));
    }

    pub fn on_stat(&self, f: impl Fn(StatReq) -> Result<StatRsp> + Send + Sync + 'static) {
        *self.stat_handler.lock() = Some(Box::new(f));
    }

    pub fn on_batch_stat(
        &self,
        f: impl Fn(BatchStatReq) -> Result<BatchStatRsp> + Send + Sync + 'static,
    ) {
        *self.batch_stat_handler.lock() = Some(Box::new(f));
    }

    pub fn on_batch_stat_by_path(
        &self,
        f: impl Fn(BatchStatByPathReq) -> Result<BatchStatByPathRsp> + Send + Sync + 'static,
    ) {
        *self.batch_stat_by_path_handler.lock() = Some(Box::new(f));
    }

    pub fn on_create(
        &self,
        f: impl Fn(CreateReq) -> Result<CreateRsp> + Send + Sync + 'static,
    ) {
        *self.create_handler.lock() = Some(Box::new(f));
    }

    pub fn on_mkdirs(
        &self,
        f: impl Fn(MkdirsReq) -> Result<MkdirsRsp> + Send + Sync + 'static,
    ) {
        *self.mkdirs_handler.lock() = Some(Box::new(f));
    }

    pub fn on_symlink(
        &self,
        f: impl Fn(SymlinkReq) -> Result<SymlinkRsp> + Send + Sync + 'static,
    ) {
        *self.symlink_handler.lock() = Some(Box::new(f));
    }

    pub fn on_hard_link(
        &self,
        f: impl Fn(HardLinkReq) -> Result<HardLinkRsp> + Send + Sync + 'static,
    ) {
        *self.hard_link_handler.lock() = Some(Box::new(f));
    }

    pub fn on_remove(
        &self,
        f: impl Fn(RemoveReq) -> Result<RemoveRsp> + Send + Sync + 'static,
    ) {
        *self.remove_handler.lock() = Some(Box::new(f));
    }

    pub fn on_open(&self, f: impl Fn(OpenReq) -> Result<OpenRsp> + Send + Sync + 'static) {
        *self.open_handler.lock() = Some(Box::new(f));
    }

    pub fn on_sync(&self, f: impl Fn(SyncReq) -> Result<SyncRsp> + Send + Sync + 'static) {
        *self.sync_handler.lock() = Some(Box::new(f));
    }

    pub fn on_close(&self, f: impl Fn(CloseReq) -> Result<CloseRsp> + Send + Sync + 'static) {
        *self.close_handler.lock() = Some(Box::new(f));
    }

    pub fn on_rename(
        &self,
        f: impl Fn(RenameReq) -> Result<RenameRsp> + Send + Sync + 'static,
    ) {
        *self.rename_handler.lock() = Some(Box::new(f));
    }

    pub fn on_list(&self, f: impl Fn(ListReq) -> Result<ListRsp> + Send + Sync + 'static) {
        *self.list_handler.lock() = Some(Box::new(f));
    }

    pub fn on_truncate(
        &self,
        f: impl Fn(TruncateReq) -> Result<TruncateRsp> + Send + Sync + 'static,
    ) {
        *self.truncate_handler.lock() = Some(Box::new(f));
    }

    pub fn on_get_real_path(
        &self,
        f: impl Fn(GetRealPathReq) -> Result<GetRealPathRsp> + Send + Sync + 'static,
    ) {
        *self.get_real_path_handler.lock() = Some(Box::new(f));
    }

    pub fn on_set_attr(
        &self,
        f: impl Fn(SetAttrReq) -> Result<SetAttrRsp> + Send + Sync + 'static,
    ) {
        *self.set_attr_handler.lock() = Some(Box::new(f));
    }

    pub fn on_prune_session(
        &self,
        f: impl Fn(PruneSessionReq) -> Result<PruneSessionRsp> + Send + Sync + 'static,
    ) {
        *self.prune_session_handler.lock() = Some(Box::new(f));
    }

    pub fn on_drop_user_cache(
        &self,
        f: impl Fn(DropUserCacheReq) -> Result<DropUserCacheRsp> + Send + Sync + 'static,
    ) {
        *self.drop_user_cache_handler.lock() = Some(Box::new(f));
    }

    pub fn on_lock_directory(
        &self,
        f: impl Fn(LockDirectoryReq) -> Result<LockDirectoryRsp> + Send + Sync + 'static,
    ) {
        *self.lock_directory_handler.lock() = Some(Box::new(f));
    }

    pub fn on_test_rpc(
        &self,
        f: impl Fn(TestRpcReq) -> Result<TestRpcRsp> + Send + Sync + 'static,
    ) {
        *self.test_rpc_handler.lock() = Some(Box::new(f));
    }
}

impl Default for MockMetaServiceStub {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IMetaServiceStub for MockMetaServiceStub {
    async fn auth(&self, req: AuthReq) -> Result<AuthRsp> {
        let guard = self.auth_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(AuthRsp::default()),
        }
    }

    async fn stat_fs(&self, req: StatFsReq) -> Result<StatFsRsp> {
        let guard = self.stat_fs_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(StatFsRsp::default()),
        }
    }

    async fn stat(&self, req: StatReq) -> Result<StatRsp> {
        let guard = self.stat_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(StatRsp {
                stat: Inode::default(),
            }),
        }
    }

    async fn batch_stat(&self, req: BatchStatReq) -> Result<BatchStatRsp> {
        let guard = self.batch_stat_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(BatchStatRsp::default()),
        }
    }

    async fn batch_stat_by_path(&self, req: BatchStatByPathReq) -> Result<BatchStatByPathRsp> {
        let guard = self.batch_stat_by_path_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(BatchStatByPathRsp::default()),
        }
    }

    async fn create(&self, req: CreateReq) -> Result<CreateRsp> {
        let guard = self.create_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(CreateRsp {
                stat: Inode::default(),
                need_truncate: false,
            }),
        }
    }

    async fn mkdirs(&self, req: MkdirsReq) -> Result<MkdirsRsp> {
        let guard = self.mkdirs_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(MkdirsRsp {
                stat: Inode::default(),
            }),
        }
    }

    async fn symlink(&self, req: SymlinkReq) -> Result<SymlinkRsp> {
        let guard = self.symlink_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(SymlinkRsp {
                stat: Inode::default(),
            }),
        }
    }

    async fn hard_link(&self, req: HardLinkReq) -> Result<HardLinkRsp> {
        let guard = self.hard_link_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(HardLinkRsp {
                stat: Inode::default(),
            }),
        }
    }

    async fn remove(&self, req: RemoveReq) -> Result<RemoveRsp> {
        let guard = self.remove_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(RemoveRsp {}),
        }
    }

    async fn open(&self, req: OpenReq) -> Result<OpenRsp> {
        let guard = self.open_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(OpenRsp {
                stat: Inode::default(),
                need_truncate: false,
            }),
        }
    }

    async fn sync(&self, req: SyncReq) -> Result<SyncRsp> {
        let guard = self.sync_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(SyncRsp {
                stat: Inode::default(),
            }),
        }
    }

    async fn close(&self, req: CloseReq) -> Result<CloseRsp> {
        let guard = self.close_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(CloseRsp {
                stat: Inode::default(),
            }),
        }
    }

    async fn rename(&self, req: RenameReq) -> Result<RenameRsp> {
        let guard = self.rename_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(RenameRsp { stat: None }),
        }
    }

    async fn list(&self, req: ListReq) -> Result<ListRsp> {
        let guard = self.list_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(ListRsp {
                entries: Vec::new(),
                inodes: Vec::new(),
                more: false,
            }),
        }
    }

    async fn truncate(&self, req: TruncateReq) -> Result<TruncateRsp> {
        let guard = self.truncate_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(TruncateRsp {
                chunks_removed: 0,
                stat: Inode::default(),
                finished: true,
            }),
        }
    }

    async fn get_real_path(&self, req: GetRealPathReq) -> Result<GetRealPathRsp> {
        let guard = self.get_real_path_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(GetRealPathRsp {
                path: String::new(),
            }),
        }
    }

    async fn set_attr(&self, req: SetAttrReq) -> Result<SetAttrRsp> {
        let guard = self.set_attr_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(SetAttrRsp {
                stat: Inode::default(),
            }),
        }
    }

    async fn prune_session(&self, req: PruneSessionReq) -> Result<PruneSessionRsp> {
        let guard = self.prune_session_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(PruneSessionRsp {}),
        }
    }

    async fn drop_user_cache(&self, req: DropUserCacheReq) -> Result<DropUserCacheRsp> {
        let guard = self.drop_user_cache_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(DropUserCacheRsp {}),
        }
    }

    async fn lock_directory(&self, req: LockDirectoryReq) -> Result<LockDirectoryRsp> {
        let guard = self.lock_directory_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(LockDirectoryRsp {}),
        }
    }

    async fn test_rpc(&self, req: TestRpcReq) -> Result<TestRpcRsp> {
        let guard = self.test_rpc_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(TestRpcRsp {
                stat: Inode::default(),
            }),
        }
    }
}

/// Blanket implementation: `Arc<T>` delegates to `T` for any `T: IMetaServiceStub`.
#[async_trait]
impl<T: IMetaServiceStub + ?Sized> IMetaServiceStub for Arc<T> {
    async fn auth(&self, req: AuthReq) -> Result<AuthRsp> {
        (**self).auth(req).await
    }
    async fn stat_fs(&self, req: StatFsReq) -> Result<StatFsRsp> {
        (**self).stat_fs(req).await
    }
    async fn stat(&self, req: StatReq) -> Result<StatRsp> {
        (**self).stat(req).await
    }
    async fn batch_stat(&self, req: BatchStatReq) -> Result<BatchStatRsp> {
        (**self).batch_stat(req).await
    }
    async fn batch_stat_by_path(&self, req: BatchStatByPathReq) -> Result<BatchStatByPathRsp> {
        (**self).batch_stat_by_path(req).await
    }
    async fn create(&self, req: CreateReq) -> Result<CreateRsp> {
        (**self).create(req).await
    }
    async fn mkdirs(&self, req: MkdirsReq) -> Result<MkdirsRsp> {
        (**self).mkdirs(req).await
    }
    async fn symlink(&self, req: SymlinkReq) -> Result<SymlinkRsp> {
        (**self).symlink(req).await
    }
    async fn hard_link(&self, req: HardLinkReq) -> Result<HardLinkRsp> {
        (**self).hard_link(req).await
    }
    async fn remove(&self, req: RemoveReq) -> Result<RemoveRsp> {
        (**self).remove(req).await
    }
    async fn open(&self, req: OpenReq) -> Result<OpenRsp> {
        (**self).open(req).await
    }
    async fn sync(&self, req: SyncReq) -> Result<SyncRsp> {
        (**self).sync(req).await
    }
    async fn close(&self, req: CloseReq) -> Result<CloseRsp> {
        (**self).close(req).await
    }
    async fn rename(&self, req: RenameReq) -> Result<RenameRsp> {
        (**self).rename(req).await
    }
    async fn list(&self, req: ListReq) -> Result<ListRsp> {
        (**self).list(req).await
    }
    async fn truncate(&self, req: TruncateReq) -> Result<TruncateRsp> {
        (**self).truncate(req).await
    }
    async fn get_real_path(&self, req: GetRealPathReq) -> Result<GetRealPathRsp> {
        (**self).get_real_path(req).await
    }
    async fn set_attr(&self, req: SetAttrReq) -> Result<SetAttrRsp> {
        (**self).set_attr(req).await
    }
    async fn prune_session(&self, req: PruneSessionReq) -> Result<PruneSessionRsp> {
        (**self).prune_session(req).await
    }
    async fn drop_user_cache(&self, req: DropUserCacheReq) -> Result<DropUserCacheRsp> {
        (**self).drop_user_cache(req).await
    }
    async fn lock_directory(&self, req: LockDirectoryReq) -> Result<LockDirectoryRsp> {
        (**self).lock_directory(req).await
    }
    async fn test_rpc(&self, req: TestRpcReq) -> Result<TestRpcRsp> {
        (**self).test_rpc(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_proto::meta::types::{PathAt, ReqBase};

    #[tokio::test]
    async fn test_mock_stat_default() {
        let mock = MockMetaServiceStub::new();
        let rsp = mock
            .stat(StatReq {
                base: ReqBase::default(),
                path: PathAt {
                    parent: 0,
                    path: Some("/foo".into()),
                },
                flags: Default::default(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.stat.id, 0); // default inode
    }

    #[tokio::test]
    async fn test_mock_stat_custom_handler() {
        let mock = MockMetaServiceStub::new();
        mock.on_stat(|_req| {
            Ok(StatRsp {
                stat: Inode {
                    id: 42,
                    ..Default::default()
                },
            })
        });
        let rsp = mock
            .stat(StatReq::default())
            .await
            .unwrap();
        assert_eq!(rsp.stat.id, 42);
    }

    #[tokio::test]
    async fn test_mock_list_default() {
        let mock = MockMetaServiceStub::new();
        let rsp = mock.list(ListReq::default()).await.unwrap();
        assert!(rsp.entries.is_empty());
        assert!(!rsp.more);
    }

    #[tokio::test]
    async fn test_mock_create_default() {
        let mock = MockMetaServiceStub::new();
        let rsp = mock.create(CreateReq::default()).await.unwrap();
        assert!(!rsp.need_truncate);
    }

    #[tokio::test]
    async fn test_mock_via_arc() {
        let mock = MockMetaServiceStub::new().into_arc();
        let rsp = mock.remove(RemoveReq::default()).await;
        assert!(rsp.is_ok());
    }
}
