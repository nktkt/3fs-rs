//! List operation: read directory entries.
//!
//! Based on 3FS/src/meta/store/ops/List.cc

use hf3fs_kv::ReadOnlyTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::make_error;
use hf3fs_types::status_code::MetaCode;
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::dir_entry::DirEntryList;
use crate::inode::{AccessType, Inode, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Execute a list (readdir) operation.
pub async fn list(
    txn: &dyn ReadOnlyTransaction,
    config: &MetaServiceConfig,
    req: &meta::ListReq,
) -> Result<meta::ListRsp> {
    let parent = if req.path.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.path.parent
    };

    // Resolve the path to find the directory to list
    let dir_inode = match &req.path.path {
        Some(path) if !path.is_empty() => {
            let mut resolver = PathResolver::new(
                txn,
                &req.base.user,
                config.max_symlink_count,
                config.max_symlink_depth,
            );
            resolver.resolve_inode(parent, path, true, true).await?
        }
        _ => {
            Inode::snapshot_load(txn, parent)
                .await?
                .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?
        }
    };

    if !dir_inode.is_directory() {
        return make_error(MetaCode::NOT_DIRECTORY);
    }

    // Check read permission
    dir_inode
        .acl()
        .check_permission(&req.base.user, AccessType::Read)?;

    let limit = if req.limit > 0 {
        req.limit
    } else {
        config.list_default_limit
    };

    let result = DirEntryList::snapshot_load(
        txn,
        dir_inode.id(),
        &req.prev,
        limit,
        req.status,
    )
    .await?;

    Ok(result.into_proto())
}
