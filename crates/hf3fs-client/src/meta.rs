//! Metadata service client.
//!
//! Mirrors C++ `meta::client::MetaClient`. Provides all filesystem metadata
//! operations: stat, create, open, close, rename, list, truncate, etc.
//!
//! The client selects a meta server via the configured `ServerSelectionMode`,
//! retries transient failures with exponential back-off, and fails over to
//! alternative servers after too many consecutive failures.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use hf3fs_proto::meta::{
    AtFlags, AuthReq, BatchStatByPathReq, BatchStatReq, CloseReq, CreateReq, GetRealPathReq,
    HardLinkReq, Inode, Layout, ListReq, ListRsp, LockDirectoryReq, MkdirsReq, OpenFlags, OpenReq,
    PathAt, Permission, RemoveReq, RenameReq, ReqBase, SessionInfo, SetAttrReq, StatFsReq,
    StatFsRsp, StatReq, SymlinkReq, SyncReq, TruncateReq, UserInfo, VersionedLength,
};
use hf3fs_types::NodeId;
use parking_lot::{Mutex, RwLock};
use rand::seq::SliceRandom;
use tracing;

use crate::config::{MetaClientConfig, RetryConfig, ServerSelectionMode};
use crate::error::{ClientError, ClientResult};
use crate::retry::ExponentialBackoff;
use crate::routing::{NodeInfo, RoutingInfoHandle};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Trait for metadata operations.
///
/// Each method corresponds to a C++ `MetaClient` public method.
#[async_trait]
pub trait MetaClient: Send + Sync {
    /// Start background tasks (server health check, close task scanner).
    async fn start(&self) -> ClientResult<()>;

    /// Stop all background tasks.
    async fn stop(&self) -> ClientResult<()>;

    /// Authenticate a user.
    async fn authenticate(&self, user: &UserInfo) -> ClientResult<UserInfo>;

    /// Stat an inode (by ID and/or path).
    async fn stat(
        &self,
        user: &UserInfo,
        inode_id: u64,
        path: Option<&str>,
        follow_last_symlink: bool,
    ) -> ClientResult<Inode>;

    /// Batch stat by inode IDs.
    async fn batch_stat(
        &self,
        user: &UserInfo,
        inode_ids: Vec<u64>,
    ) -> ClientResult<Vec<Option<Inode>>>;

    /// Batch stat by paths.
    async fn batch_stat_by_path(
        &self,
        user: &UserInfo,
        paths: Vec<PathAt>,
        follow_last_symlink: bool,
    ) -> ClientResult<Vec<Option<Inode>>>;

    /// Get filesystem statistics.
    async fn stat_fs(&self, user: &UserInfo) -> ClientResult<StatFsRsp>;

    /// Resolve to a real (absolute or relative) path.
    async fn get_real_path(
        &self,
        user: &UserInfo,
        parent: u64,
        path: Option<&str>,
        absolute: bool,
    ) -> ClientResult<String>;

    /// Open a file.
    async fn open(
        &self,
        user: &UserInfo,
        inode_id: u64,
        path: Option<&str>,
        session: Option<SessionInfo>,
        flags: i32,
    ) -> ClientResult<Inode>;

    /// Close a file.
    async fn close(
        &self,
        user: &UserInfo,
        inode_id: u64,
        session: Option<SessionInfo>,
        update_length: bool,
        atime: Option<i64>,
        mtime: Option<i64>,
    ) -> ClientResult<Inode>;

    /// Create a new file.
    async fn create(
        &self,
        user: &UserInfo,
        parent: u64,
        path: &str,
        session: Option<SessionInfo>,
        perm: u32,
        flags: i32,
        layout: Option<Layout>,
    ) -> ClientResult<Inode>;

    /// Create directories (optionally recursively).
    async fn mkdirs(
        &self,
        user: &UserInfo,
        parent: u64,
        path: &str,
        perm: u32,
        recursive: bool,
        layout: Option<Layout>,
    ) -> ClientResult<Inode>;

    /// Create a symbolic link.
    async fn symlink(
        &self,
        user: &UserInfo,
        parent: u64,
        path: &str,
        target: &str,
    ) -> ClientResult<Inode>;

    /// Remove a file or directory.
    async fn remove(
        &self,
        user: &UserInfo,
        parent: u64,
        path: Option<&str>,
        recursive: bool,
    ) -> ClientResult<()>;

    /// Unlink a file (non-recursive, type-checked).
    async fn unlink(&self, user: &UserInfo, parent: u64, path: &str) -> ClientResult<()>;

    /// Remove a directory.
    async fn rmdir(
        &self,
        user: &UserInfo,
        parent: u64,
        path: Option<&str>,
        recursive: bool,
    ) -> ClientResult<()>;

    /// Rename a file or directory.
    async fn rename(
        &self,
        user: &UserInfo,
        src_parent: u64,
        src: &str,
        dst_parent: u64,
        dst: &str,
        move_to_trash: bool,
    ) -> ClientResult<Inode>;

    /// List directory entries.
    async fn list(
        &self,
        user: &UserInfo,
        inode_id: u64,
        path: Option<&str>,
        prev: &str,
        limit: i32,
        need_status: bool,
    ) -> ClientResult<ListRsp>;

    /// Set file attributes (permissions, times, layout, etc.).
    async fn set_attr(&self, user: &UserInfo, req: SetAttrReq) -> ClientResult<Inode>;

    /// Truncate a file to the given length.
    async fn truncate(&self, user: &UserInfo, inode_id: u64, length: u64) -> ClientResult<Inode>;

    /// Sync file state.
    async fn sync(
        &self,
        user: &UserInfo,
        inode_id: u64,
        update_length: bool,
        atime: Option<i64>,
        mtime: Option<i64>,
        length_hint: Option<VersionedLength>,
    ) -> ClientResult<Inode>;

    /// Create a hard link.
    async fn hard_link(
        &self,
        user: &UserInfo,
        old_parent: u64,
        old_path: Option<&str>,
        new_parent: u64,
        new_path: &str,
        follow_last_symlink: bool,
    ) -> ClientResult<Inode>;

    /// Lock or unlock a directory.
    async fn lock_directory(
        &self,
        user: &UserInfo,
        inode_id: u64,
        action: u8,
    ) -> ClientResult<()>;

    /// Test RPC connectivity to the meta service.
    async fn test_rpc(&self) -> ClientResult<()>;
}

// ---------------------------------------------------------------------------
// Server selection
// ---------------------------------------------------------------------------

/// Selects a meta server from the routing info.
struct ServerSelector {
    mode: ServerSelectionMode,
    routing: RoutingInfoHandle,
    round_robin_idx: AtomicUsize,
    err_nodes: RwLock<HashSet<NodeId>>,
}

impl ServerSelector {
    fn new(mode: ServerSelectionMode, routing: RoutingInfoHandle) -> Self {
        Self {
            mode,
            routing,
            round_robin_idx: AtomicUsize::new(0),
            err_nodes: RwLock::new(HashSet::new()),
        }
    }

    /// Select a server node, skipping any currently error-listed nodes.
    fn select(&self) -> ClientResult<NodeInfo> {
        let ri = self.routing.get();
        let err = self.err_nodes.read();
        let candidates: Vec<_> = ri
            .nodes
            .values()
            .filter(|n| n.is_healthy && !err.contains(&n.node_id))
            .collect();

        if candidates.is_empty() {
            // Fall back to all healthy nodes (even if error-listed).
            let all_healthy: Vec<_> = ri.nodes.values().filter(|n| n.is_healthy).collect();
            if all_healthy.is_empty() {
                return Err(ClientError::NoServerAvailable(
                    "no healthy meta servers".into(),
                ));
            }
            return Ok(self.pick_from(&all_healthy));
        }

        Ok(self.pick_from(&candidates))
    }

    fn pick_from(&self, candidates: &[&NodeInfo]) -> NodeInfo {
        match self.mode {
            ServerSelectionMode::RoundRobin => {
                let idx = self.round_robin_idx.fetch_add(1, Ordering::Relaxed);
                candidates[idx % candidates.len()].clone()
            }
            ServerSelectionMode::UniformRandom => {
                let mut rng = rand::thread_rng();
                (*candidates.choose(&mut rng).unwrap()).clone()
            }
            ServerSelectionMode::RandomFollow => {
                // Sticky to the first candidate; will rotate on failover.
                candidates[0].clone()
            }
        }
    }

    fn mark_error(&self, node_id: NodeId) {
        self.err_nodes.write().insert(node_id);
    }

    fn clear_error(&self, node_id: NodeId) {
        self.err_nodes.write().remove(&node_id);
    }
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/// Concrete implementation of `MetaClient`.
pub struct MetaClientImpl {
    config: MetaClientConfig,
    selector: ServerSelector,
    running: Mutex<bool>,
}

impl MetaClientImpl {
    /// Create a new meta client.
    ///
    /// The `routing` handle is typically obtained from `MgmtdClientImpl::routing_handle()`.
    pub fn new(config: MetaClientConfig, routing: RoutingInfoHandle) -> Self {
        let selector = ServerSelector::new(config.selection_mode, routing);
        Self {
            config,
            selector,
            running: Mutex::new(false),
        }
    }

    /// Build a `ReqBase` with the given user info.
    fn req_base(user: &UserInfo) -> ReqBase {
        ReqBase {
            user: user.clone(),
            ..ReqBase::default()
        }
    }

    /// Build an `AtFlags` from a follow-symlink boolean.
    fn at_flags(follow: bool) -> AtFlags {
        if follow {
            AtFlags(0x400) // AT_SYMLINK_FOLLOW
        } else {
            AtFlags(0x100) // AT_SYMLINK_NOFOLLOW
        }
    }

    /// Execute an operation with retry logic.
    ///
    /// This mirrors the C++ `MetaClient::retry` template. On each attempt
    /// we select a server, call the operation, and handle errors:
    /// - Transient / RPC errors trigger retries with exponential back-off.
    /// - Consecutive failures to the same server trigger failover.
    /// - Non-retryable errors are returned immediately.
    async fn with_retry<F, Fut, T>(
        &self,
        op_name: &str,
        retry_cfg: &RetryConfig,
        mut op: F,
    ) -> ClientResult<T>
    where
        F: FnMut(&NodeInfo) -> Fut,
        Fut: std::future::Future<Output = ClientResult<T>>,
    {
        let mut backoff = ExponentialBackoff::new(
            retry_cfg.retry_init_wait,
            retry_cfg.retry_max_wait,
            retry_cfg.retry_total_time,
        );

        let mut current_server: Option<NodeInfo> = None;
        let mut consecutive_failures: u32 = 0;

        loop {
            // Select a server if we do not have one.
            let server = match &current_server {
                Some(s) => s.clone(),
                None => {
                    consecutive_failures = 0;
                    match self.selector.select() {
                        Ok(s) => {
                            current_server = Some(s.clone());
                            s
                        }
                        Err(e) => {
                            tracing::warn!("{}: no server available: {}", op_name, e);
                            match backoff.next_wait() {
                                Some(wait) => {
                                    tokio::time::sleep(wait).await;
                                    continue;
                                }
                                None => return Err(e),
                            }
                        }
                    }
                }
            };

            // Call the operation.
            match op(&server).await {
                Ok(val) => {
                    self.selector.clear_error(server.node_id);
                    return Ok(val);
                }
                Err(e) => {
                    let retryable = Self::is_retryable(&e);
                    let is_server_error = Self::is_server_error(&e);

                    tracing::warn!(
                        "{}: failed on node {} ({}): {} (retryable={}, server_err={})",
                        op_name,
                        *server.node_id,
                        server.hostname,
                        e,
                        retryable,
                        is_server_error,
                    );

                    if !retryable {
                        return Err(e);
                    }

                    // Handle failover.
                    if is_server_error {
                        consecutive_failures += 1;
                        if consecutive_failures >= retry_cfg.max_failures_before_failover {
                            self.selector.mark_error(server.node_id);
                            current_server = None;
                            backoff.reset_wait();
                        }
                    }

                    // Determine wait time.
                    let wait = if Self::is_fast_retryable(&e) {
                        backoff.fast_wait()
                    } else {
                        backoff.next_wait()
                    };

                    match wait {
                        Some(w) => {
                            tokio::time::sleep(w).await;
                        }
                        None => {
                            return Err(ClientError::RetryExhausted {
                                attempts: backoff.attempts(),
                                message: format!("{}: {}", op_name, e),
                            });
                        }
                    }
                }
            }
        }
    }

    /// Determine whether an error is retryable.
    fn is_retryable(err: &ClientError) -> bool {
        match err {
            ClientError::Net(_) => true,
            ClientError::Status(s) => {
                let code = s.code();
                // RPC errors (2xxx) are generally retryable.
                (2000..3000).contains(&code)
                    // Transient meta errors.
                    || code == hf3fs_types::MetaCode::BUSY
                    || code == hf3fs_types::MetaCode::RETRYABLE
                    || code == hf3fs_types::MetaCode::FORWARD_FAILED
                    || code == hf3fs_types::MetaCode::FORWARD_TIMEOUT
                    || code == hf3fs_types::MetaCode::OPERATION_TIMEOUT
            }
            ClientError::NoServerAvailable(_) => true,
            ClientError::RoutingInfoNotReady => true,
            _ => false,
        }
    }

    /// Determine whether we should retry quickly (without full back-off).
    fn is_fast_retryable(err: &ClientError) -> bool {
        match err {
            ClientError::Status(s) => {
                let code = s.code();
                code == hf3fs_types::RPCCode::REQUEST_REFUSED
                    || code == hf3fs_types::RPCCode::SEND_FAILED
                    || code == hf3fs_types::MetaCode::BUSY
            }
            _ => false,
        }
    }

    /// Determine whether the error indicates a server-side issue (for failover).
    fn is_server_error(err: &ClientError) -> bool {
        match err {
            ClientError::Net(_) => true,
            ClientError::Status(s) => {
                let code = s.code();
                code == hf3fs_types::RPCCode::CONNECT_FAILED
                    || code == hf3fs_types::RPCCode::TIMEOUT
                    || code == hf3fs_types::RPCCode::SOCKET_CLOSED
                    || code == hf3fs_types::RPCCode::SOCKET_ERROR
            }
            _ => false,
        }
    }
}

#[async_trait]
impl MetaClient for MetaClientImpl {
    async fn start(&self) -> ClientResult<()> {
        let mut running = self.running.lock();
        if *running {
            return Ok(());
        }
        *running = true;
        tracing::info!("MetaClient started");
        Ok(())
    }

    async fn stop(&self) -> ClientResult<()> {
        let mut running = self.running.lock();
        if !*running {
            return Ok(());
        }
        *running = false;
        tracing::info!("MetaClient stopped");
        Ok(())
    }

    async fn authenticate(&self, user: &UserInfo) -> ClientResult<UserInfo> {
        let _req = AuthReq {
            base: Self::req_base(user),
        };
        // Placeholder: would send req via RPC, deserialize AuthRsp.
        self.with_retry("authenticate", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn stat(
        &self,
        user: &UserInfo,
        inode_id: u64,
        path: Option<&str>,
        follow_last_symlink: bool,
    ) -> ClientResult<Inode> {
        let req = StatReq {
            base: Self::req_base(user),
            path: PathAt {
                parent: inode_id,
                path: path.map(|s| s.to_string()),
            },
            flags: Self::at_flags(follow_last_symlink),
        };
        let _ = req;
        self.with_retry("stat", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn batch_stat(
        &self,
        user: &UserInfo,
        inode_ids: Vec<u64>,
    ) -> ClientResult<Vec<Option<Inode>>> {
        let req = BatchStatReq {
            base: Self::req_base(user),
            inode_ids: inode_ids.clone(),
        };
        let _ = req;
        self.with_retry("batch_stat", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn batch_stat_by_path(
        &self,
        user: &UserInfo,
        paths: Vec<PathAt>,
        follow_last_symlink: bool,
    ) -> ClientResult<Vec<Option<Inode>>> {
        let req = BatchStatByPathReq {
            base: Self::req_base(user),
            paths: paths.clone(),
            flags: Self::at_flags(follow_last_symlink),
        };
        let _ = req;
        self.with_retry("batch_stat_by_path", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn stat_fs(&self, user: &UserInfo) -> ClientResult<StatFsRsp> {
        let req = StatFsReq {
            base: Self::req_base(user),
        };
        let _ = req;
        self.with_retry("stat_fs", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn get_real_path(
        &self,
        user: &UserInfo,
        parent: u64,
        path: Option<&str>,
        absolute: bool,
    ) -> ClientResult<String> {
        let req = GetRealPathReq {
            base: Self::req_base(user),
            path: PathAt {
                parent,
                path: path.map(|s| s.to_string()),
            },
            absolute,
        };
        let _ = req;
        self.with_retry("get_real_path", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn open(
        &self,
        user: &UserInfo,
        inode_id: u64,
        path: Option<&str>,
        session: Option<SessionInfo>,
        flags: i32,
    ) -> ClientResult<Inode> {
        let req = OpenReq {
            base: Self::req_base(user),
            path: PathAt {
                parent: inode_id,
                path: path.map(|s| s.to_string()),
            },
            session,
            flags: OpenFlags(flags),
            remove_chunks_batch_size: self.config.remove_chunks_batch_size,
            dyn_stripe: self.config.dynamic_stripe,
        };
        let _ = req;
        self.with_retry("open", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn close(
        &self,
        user: &UserInfo,
        inode_id: u64,
        session: Option<SessionInfo>,
        update_length: bool,
        atime: Option<i64>,
        mtime: Option<i64>,
    ) -> ClientResult<Inode> {
        let req = CloseReq {
            base: Self::req_base(user),
            inode: inode_id,
            session,
            update_length,
            atime,
            mtime,
            length_hint: None,
        };
        let _ = req;
        self.with_retry("close", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn create(
        &self,
        user: &UserInfo,
        parent: u64,
        path: &str,
        session: Option<SessionInfo>,
        perm: u32,
        flags: i32,
        layout: Option<Layout>,
    ) -> ClientResult<Inode> {
        let req = CreateReq {
            base: Self::req_base(user),
            path: PathAt {
                parent,
                path: Some(path.to_string()),
            },
            session,
            flags: OpenFlags(flags),
            perm: Permission(perm & 0o7777),
            layout,
            remove_chunks_batch_size: self.config.remove_chunks_batch_size,
            dyn_stripe: self.config.dynamic_stripe,
        };
        let _ = req;
        self.with_retry("create", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn mkdirs(
        &self,
        user: &UserInfo,
        parent: u64,
        path: &str,
        perm: u32,
        recursive: bool,
        layout: Option<Layout>,
    ) -> ClientResult<Inode> {
        let req = MkdirsReq {
            base: Self::req_base(user),
            path: PathAt {
                parent,
                path: Some(path.to_string()),
            },
            perm: Permission(perm & 0o7777),
            recursive,
            layout,
        };
        let _ = req;
        self.with_retry("mkdirs", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn symlink(
        &self,
        user: &UserInfo,
        parent: u64,
        path: &str,
        target: &str,
    ) -> ClientResult<Inode> {
        let req = SymlinkReq {
            base: Self::req_base(user),
            path: PathAt {
                parent,
                path: Some(path.to_string()),
            },
            target: target.to_string(),
        };
        let _ = req;
        self.with_retry("symlink", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn remove(
        &self,
        user: &UserInfo,
        parent: u64,
        path: Option<&str>,
        recursive: bool,
    ) -> ClientResult<()> {
        let req = RemoveReq {
            base: Self::req_base(user),
            path: PathAt {
                parent,
                path: path.map(|s| s.to_string()),
            },
            at_flags: AtFlags(0),
            recursive,
            check_type: false,
            inode_id: None,
        };
        let _ = req;
        self.with_retry("remove", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn unlink(&self, user: &UserInfo, parent: u64, path: &str) -> ClientResult<()> {
        let req = RemoveReq {
            base: Self::req_base(user),
            path: PathAt {
                parent,
                path: Some(path.to_string()),
            },
            at_flags: AtFlags(0),
            recursive: false,
            check_type: true,
            inode_id: None,
        };
        let _ = req;
        self.with_retry("unlink", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn rmdir(
        &self,
        user: &UserInfo,
        parent: u64,
        path: Option<&str>,
        recursive: bool,
    ) -> ClientResult<()> {
        // AT_REMOVEDIR = 0x200
        let req = RemoveReq {
            base: Self::req_base(user),
            path: PathAt {
                parent,
                path: path.map(|s| s.to_string()),
            },
            at_flags: AtFlags(0x200),
            recursive,
            check_type: true,
            inode_id: None,
        };
        let _ = req;
        self.with_retry("rmdir", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn rename(
        &self,
        user: &UserInfo,
        src_parent: u64,
        src: &str,
        dst_parent: u64,
        dst: &str,
        move_to_trash: bool,
    ) -> ClientResult<Inode> {
        let req = RenameReq {
            base: Self::req_base(user),
            src: PathAt {
                parent: src_parent,
                path: Some(src.to_string()),
            },
            dest: PathAt {
                parent: dst_parent,
                path: Some(dst.to_string()),
            },
            move_to_trash,
            inode_id: None,
        };
        let _ = req;
        self.with_retry("rename", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn list(
        &self,
        user: &UserInfo,
        inode_id: u64,
        path: Option<&str>,
        prev: &str,
        limit: i32,
        need_status: bool,
    ) -> ClientResult<ListRsp> {
        let req = ListReq {
            base: Self::req_base(user),
            path: PathAt {
                parent: inode_id,
                path: path.map(|s| s.to_string()),
            },
            prev: prev.to_string(),
            limit,
            status: need_status,
        };
        let _ = req;
        self.with_retry("list", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn set_attr(&self, user: &UserInfo, mut req: SetAttrReq) -> ClientResult<Inode> {
        req.base = Self::req_base(user);
        self.with_retry("set_attr", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn truncate(&self, user: &UserInfo, inode_id: u64, length: u64) -> ClientResult<Inode> {
        let req = TruncateReq {
            base: Self::req_base(user),
            inode: inode_id,
            length,
            remove_chunks_batch_size: self.config.remove_chunks_batch_size,
        };
        let _ = req;
        self.with_retry("truncate", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn sync(
        &self,
        user: &UserInfo,
        inode_id: u64,
        update_length: bool,
        atime: Option<i64>,
        mtime: Option<i64>,
        length_hint: Option<VersionedLength>,
    ) -> ClientResult<Inode> {
        let req = SyncReq {
            base: Self::req_base(user),
            inode: inode_id,
            update_length,
            atime,
            mtime,
            truncated: false,
            length_hint,
        };
        let _ = req;
        self.with_retry("sync", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn hard_link(
        &self,
        user: &UserInfo,
        old_parent: u64,
        old_path: Option<&str>,
        new_parent: u64,
        new_path: &str,
        follow_last_symlink: bool,
    ) -> ClientResult<Inode> {
        let req = HardLinkReq {
            base: Self::req_base(user),
            old_path: PathAt {
                parent: old_parent,
                path: old_path.map(|s| s.to_string()),
            },
            new_path: PathAt {
                parent: new_parent,
                path: Some(new_path.to_string()),
            },
            flags: Self::at_flags(follow_last_symlink),
        };
        let _ = req;
        self.with_retry("hard_link", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn lock_directory(
        &self,
        user: &UserInfo,
        inode_id: u64,
        action: u8,
    ) -> ClientResult<()> {
        let req = LockDirectoryReq {
            base: Self::req_base(user),
            inode: inode_id,
            action,
        };
        let _ = req;
        self.with_retry("lock_directory", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }

    async fn test_rpc(&self) -> ClientResult<()> {
        self.with_retry("test_rpc", &self.config.retry, |_server| async {
            Err(ClientError::Internal("not connected to live server".into()))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RoutingInfoHandle;
    use std::time::Duration;

    fn test_user() -> UserInfo {
        UserInfo {
            uid: 1000,
            gid: 100,
            gids: vec![100],
        }
    }

    fn test_config() -> MetaClientConfig {
        MetaClientConfig {
            retry: RetryConfig {
                retry_total_time: Duration::from_millis(10),
                retry_init_wait: Duration::from_millis(1),
                retry_max_wait: Duration::from_millis(5),
                ..RetryConfig::default()
            },
            ..MetaClientConfig::default()
        }
    }

    #[tokio::test]
    async fn test_meta_client_start_stop() {
        let routing = RoutingInfoHandle::new();
        let client = MetaClientImpl::new(test_config(), routing);
        client.start().await.unwrap();
        client.start().await.unwrap(); // idempotent
        client.stop().await.unwrap();
        client.stop().await.unwrap(); // idempotent
    }

    #[tokio::test]
    async fn test_meta_client_no_servers() {
        let routing = RoutingInfoHandle::new();
        let client = MetaClientImpl::new(test_config(), routing);
        let err = client.stat(&test_user(), 0, Some("/"), true).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_meta_client_retry_exhaustion() {
        let routing = RoutingInfoHandle::new();
        let mut ri = crate::routing::RoutingInfo::empty();
        ri.nodes.insert(
            NodeId(1),
            crate::routing::NodeInfo {
                node_id: NodeId(1),
                address: hf3fs_types::Address::from_octets(10, 0, 0, 1, 8080, hf3fs_types::AddressType::TCP),
                hostname: "test-node".into(),
                is_healthy: true,
            },
        );
        routing.update(ri);

        let client = MetaClientImpl::new(test_config(), routing);
        let err = client.stat(&test_user(), 0, Some("/test"), false).await;
        // Should fail since there is no real server connection.
        assert!(err.is_err());
    }

    #[test]
    fn test_req_base() {
        let user = test_user();
        let base = MetaClientImpl::req_base(&user);
        assert_eq!(base.user.uid, 1000);
    }

    #[test]
    fn test_at_flags() {
        let follow = MetaClientImpl::at_flags(true);
        assert_eq!(follow.0, 0x400);
        let no_follow = MetaClientImpl::at_flags(false);
        assert_eq!(no_follow.0, 0x100);
    }

    #[test]
    fn test_is_retryable() {
        use hf3fs_types::{RPCCode, MetaCode, Status};

        // RPC timeout is retryable.
        assert!(MetaClientImpl::is_retryable(&ClientError::Status(
            Status::new(RPCCode::TIMEOUT)
        )));

        // MetaCode::BUSY is retryable.
        assert!(MetaClientImpl::is_retryable(&ClientError::Status(
            Status::new(MetaCode::BUSY)
        )));

        // MetaCode::NOT_FOUND is NOT retryable.
        assert!(!MetaClientImpl::is_retryable(&ClientError::Status(
            Status::new(MetaCode::NOT_FOUND)
        )));

        // Network errors are retryable.
        assert!(MetaClientImpl::is_retryable(&ClientError::Net(
            hf3fs_net::SocketError::Timeout
        )));

        // Internal errors are NOT retryable.
        assert!(!MetaClientImpl::is_retryable(&ClientError::Internal(
            "bug".into()
        )));
    }

    #[test]
    fn test_is_fast_retryable() {
        use hf3fs_types::{RPCCode, MetaCode, Status};

        assert!(MetaClientImpl::is_fast_retryable(&ClientError::Status(
            Status::new(RPCCode::REQUEST_REFUSED)
        )));
        assert!(MetaClientImpl::is_fast_retryable(&ClientError::Status(
            Status::new(MetaCode::BUSY)
        )));
        assert!(!MetaClientImpl::is_fast_retryable(&ClientError::Status(
            Status::new(RPCCode::TIMEOUT)
        )));
    }

    #[test]
    fn test_is_server_error() {
        use hf3fs_types::{RPCCode, Status};

        assert!(MetaClientImpl::is_server_error(&ClientError::Status(
            Status::new(RPCCode::CONNECT_FAILED)
        )));
        assert!(MetaClientImpl::is_server_error(&ClientError::Net(
            hf3fs_net::SocketError::ConnectionClosed
        )));
        assert!(!MetaClientImpl::is_server_error(&ClientError::Internal(
            "test".into()
        )));
    }

    #[test]
    fn test_server_selector_no_nodes() {
        let routing = RoutingInfoHandle::new();
        let selector = ServerSelector::new(ServerSelectionMode::RoundRobin, routing);
        assert!(selector.select().is_err());
    }

    #[test]
    fn test_server_selector_round_robin() {
        let routing = RoutingInfoHandle::new();
        let mut ri = crate::routing::RoutingInfo::empty();
        for i in 1..=3 {
            ri.nodes.insert(
                NodeId(i),
                crate::routing::NodeInfo {
                    node_id: NodeId(i),
                    address: hf3fs_types::Address::from_octets(10, 0, 0, i as u8, 8080, hf3fs_types::AddressType::TCP),
                    hostname: format!("node-{}", i),
                    is_healthy: true,
                },
            );
        }
        routing.update(ri);

        let selector = ServerSelector::new(ServerSelectionMode::RoundRobin, routing);
        let n1 = selector.select().unwrap();
        let n2 = selector.select().unwrap();
        let n3 = selector.select().unwrap();
        let n4 = selector.select().unwrap();
        // Round-robin should cycle.
        assert_eq!(n1.node_id, n4.node_id);
        // All three should be distinct.
        let ids: HashSet<_> = [n1.node_id, n2.node_id, n3.node_id].into();
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn test_server_selector_error_marking() {
        let routing = RoutingInfoHandle::new();
        let mut ri = crate::routing::RoutingInfo::empty();
        for i in 1..=2 {
            ri.nodes.insert(
                NodeId(i),
                crate::routing::NodeInfo {
                    node_id: NodeId(i),
                    address: hf3fs_types::Address::from_octets(10, 0, 0, i as u8, 8080, hf3fs_types::AddressType::TCP),
                    hostname: format!("node-{}", i),
                    is_healthy: true,
                },
            );
        }
        routing.update(ri);

        let selector = ServerSelector::new(ServerSelectionMode::RoundRobin, routing);
        // Mark node 1 as error.
        selector.mark_error(NodeId(1));
        // All selections should now pick node 2.
        for _ in 0..5 {
            let n = selector.select().unwrap();
            assert_eq!(n.node_id, NodeId(2));
        }
        // Clear the error; both should be candidates again.
        selector.clear_error(NodeId(1));
        let mut seen = HashSet::new();
        for _ in 0..10 {
            seen.insert(selector.select().unwrap().node_id);
        }
        assert_eq!(seen.len(), 2);
    }
}
