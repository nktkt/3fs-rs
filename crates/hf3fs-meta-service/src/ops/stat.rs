//! Stat operation: retrieve inode metadata by path or inode ID.
//!
//! Based on 3FS/src/meta/store/ops/Stat.cc

use hf3fs_kv::ReadOnlyTransaction;
use hf3fs_proto::meta;
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::inode::{Inode, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Execute a stat operation within a read-only transaction.
pub async fn stat(
    txn: &dyn ReadOnlyTransaction,
    config: &MetaServiceConfig,
    req: &meta::StatReq,
) -> Result<meta::StatRsp> {
    let parent = if req.path.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.path.parent
    };

    let follow_symlinks = req.flags.0 & libc::AT_SYMLINK_NOFOLLOW == 0;
    let check_ref_cnt = !config.allow_stat_deleted_inodes;

    let inode = match &req.path.path {
        Some(path) if !path.is_empty() => {
            let mut resolver = PathResolver::new(
                txn,
                &req.base.user,
                config.max_symlink_count,
                config.max_symlink_depth,
            );
            resolver
                .resolve_inode(parent, path, follow_symlinks, check_ref_cnt)
                .await?
        }
        _ => {
            // Stat by inode ID (no path, just parent)
            let inode = Inode::snapshot_load(txn, parent)
                .await?
                .ok_or_else(|| {
                    hf3fs_types::Status::new(
                        hf3fs_types::status_code::MetaCode::NOT_FOUND,
                    )
                })?;
            if check_ref_cnt && inode.inner.nlink == 0 {
                return Err(hf3fs_types::Status::new(
                    hf3fs_types::status_code::MetaCode::NOT_FOUND,
                ));
            }
            inode
        }
    };

    Ok(meta::StatRsp {
        stat: inode.into_proto(),
    })
}

/// Execute a batch stat operation (by inode IDs).
pub async fn batch_stat(
    txn: &dyn ReadOnlyTransaction,
    _config: &MetaServiceConfig,
    req: &meta::BatchStatReq,
) -> Result<meta::BatchStatRsp> {
    let mut inodes = Vec::with_capacity(req.inode_ids.len());
    for &id in &req.inode_ids {
        let inode = Inode::snapshot_load(txn, id).await?;
        inodes.push(inode.map(|i| i.into_proto()));
    }
    Ok(meta::BatchStatRsp { inodes })
}
