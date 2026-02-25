//! SetAttr operation: modify inode attributes (permissions, timestamps, etc.).
//!
//! Based on 3FS/src/meta/store/ops/SetAttr.cc

use hf3fs_kv::ReadWriteTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::MetaCode;
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::inode::{AccessType, Inode, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Sentinel value meaning "set to current time" (matches C++ `SETATTR_TIME_NOW`).
pub const SETATTR_TIME_NOW: i64 = -1;

/// Execute a set-attr operation.
pub async fn set_attr(
    txn: &mut dyn ReadWriteTransaction,
    config: &MetaServiceConfig,
    req: &meta::SetAttrReq,
) -> Result<meta::SetAttrRsp> {
    let parent = if req.path.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.path.parent
    };

    let follow_symlinks = req.flags.0 & libc::AT_SYMLINK_NOFOLLOW == 0;

    // Resolve the path
    let mut resolver = PathResolver::new(
        txn,
        &req.base.user,
        config.max_symlink_count,
        config.max_symlink_depth,
    );

    let inode = match &req.path.path {
        Some(path) if !path.is_empty() => {
            resolver
                .resolve_inode(parent, path, follow_symlinks, true)
                .await?
        }
        _ => {
            Inode::load(txn, parent)
                .await?
                .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?
        }
    };

    let mut updated = inode.clone();
    let is_owner = req.base.user.uid == 0 || req.base.user.uid == inode.inner.uid;

    // Set UID
    if let Some(uid) = req.uid {
        if req.base.user.uid != 0 {
            return make_error_msg(
                MetaCode::NO_PERMISSION,
                "only root can change uid",
            );
        }
        updated.inner.uid = uid;
    }

    // Set GID
    if let Some(gid) = req.gid {
        if !is_owner {
            return make_error_msg(
                MetaCode::NO_PERMISSION,
                "only owner or root can change gid",
            );
        }
        updated.inner.gid = gid;
    }

    // Set permission
    if let Some(perm) = req.perm {
        if !is_owner {
            return make_error_msg(
                MetaCode::NO_PERMISSION,
                "only owner or root can change permission",
            );
        }
        updated.inner.permission = perm & 0o7777;
    }

    // Set timestamps
    let now_ns = chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or(0);
    if let Some(atime) = req.atime {
        if atime == SETATTR_TIME_NOW {
            updated.inner.atime_ns = now_ns;
        } else {
            updated.inner.atime_ns = atime;
        }
    }
    if let Some(mtime) = req.mtime {
        if mtime == SETATTR_TIME_NOW {
            updated.inner.mtime_ns = now_ns;
        } else {
            updated.inner.mtime_ns = mtime;
        }
    }

    // Set iflags
    if let Some(iflags) = req.iflags {
        if !is_owner {
            return make_error_msg(
                MetaCode::NO_PERMISSION,
                "only owner or root can change iflags",
            );
        }
        updated.inner.iflags = iflags;
    }

    // Set layout
    if let Some(layout) = &req.layout {
        if !is_owner {
            return make_error_msg(
                MetaCode::NO_PERMISSION,
                "only owner or root can change layout",
            );
        }
        updated.inner.layout = Some(layout.clone());
    }

    // Update ctime (always changes on setattr)
    updated.inner.ctime_ns = now_ns;

    // Store
    inode.add_read_conflict(txn).await?;
    updated.store(txn).await?;

    tracing::debug!(
        inode_id = updated.id(),
        "set_attr: updated"
    );

    Ok(meta::SetAttrRsp {
        stat: updated.into_proto(),
    })
}
