//! Remove operation: unlink a file or remove a directory.
//!
//! Based on 3FS/src/meta/store/ops/Remove.cc

use hf3fs_kv::ReadWriteTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::MetaCode;
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::dir_entry::DirEntryList;
use crate::inode::{is_tree_root, AccessType, Inode, FS_IMMUTABLE_FL, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Execute a remove operation within a read-write transaction.
pub async fn remove(
    txn: &mut dyn ReadWriteTransaction,
    config: &MetaServiceConfig,
    req: &meta::RemoveReq,
) -> Result<meta::RemoveRsp> {
    let parent = if req.path.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.path.parent
    };

    // Resolve the path (do not follow last symlink for remove)
    let resolve_result = match &req.path.path {
        Some(path) if !path.is_empty() => {
            let mut resolver = PathResolver::new(
                txn,
                &req.base.user,
                config.max_symlink_count,
                config.max_symlink_depth,
            );
            resolver.resolve_path(parent, path, false).await?
        }
        _ => {
            return make_error_msg(MetaCode::NOT_FOUND, "no path specified for remove");
        }
    };

    let entry = resolve_result.dir_entry.ok_or_else(|| {
        hf3fs_types::Status::new(MetaCode::NOT_FOUND)
    })?;

    // Don't allow removing tree roots
    if is_tree_root(entry.inode_id) {
        return make_error_msg(
            MetaCode::NO_PERMISSION,
            "cannot remove tree root",
        );
    }

    // Verify inode_id if specified
    if let Some(expected_id) = req.inode_id {
        if entry.inode_id != expected_id {
            return make_error(MetaCode::NOT_FOUND);
        }
    }

    // Check write permission on parent directory
    let parent_inode = Inode::load(txn, resolve_result.parent_id)
        .await?
        .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
    parent_inode.acl().check_permission(
        &req.base.user,
        AccessType::Write,
    )?;

    // Load the target inode
    let inode = Inode::load(txn, entry.inode_id)
        .await?
        .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;

    // Check immutable flag
    if inode.inner.iflags & FS_IMMUTABLE_FL != 0 {
        return make_error_msg(
            MetaCode::NO_PERMISSION,
            "cannot remove inode with FS_IMMUTABLE_FL",
        );
    }

    // Check sticky bit on parent
    let s_isvtx: u32 = 0o1000;
    if parent_inode.inner.permission & s_isvtx != 0
        && req.base.user.uid != 0
        && req.base.user.uid != parent_inode.inner.uid
        && req.base.user.uid != inode.inner.uid
    {
        return make_error_msg(
            MetaCode::NO_PERMISSION,
            "sticky bit set on parent directory",
        );
    }

    // Type checking
    if req.check_type {
        let at_removedir: i32 = 0x200; // AT_REMOVEDIR on Linux
        if req.at_flags.0 & at_removedir != 0 && inode.is_file() {
            return make_error(MetaCode::NOT_DIRECTORY);
        }
        if req.at_flags.0 & at_removedir == 0 && inode.is_directory() {
            return make_error(MetaCode::IS_DIRECTORY);
        }
    }

    if inode.is_directory() {
        // Check if directory is empty
        let is_empty = DirEntryList::check_empty(txn, entry.inode_id).await?;
        if is_empty {
            // Remove the empty directory directly
            entry.add_read_conflict(txn).await?;
            inode.add_read_conflict(txn).await?;
            entry.remove(txn).await?;
            inode.remove(txn).await?;
        } else {
            if !req.recursive {
                return make_error(MetaCode::NOT_EMPTY);
            }
            // For recursive remove, in the full implementation we would move the
            // directory to the GC root for background cleanup. Here we perform
            // a simplified direct removal.
            entry.add_read_conflict(txn).await?;
            inode.add_read_conflict(txn).await?;
            entry.remove(txn).await?;
            inode.remove(txn).await?;
        }
    } else {
        // Remove file or symlink
        // For files with nlink > 1, decrement nlink. Otherwise remove the inode.
        entry.add_read_conflict(txn).await?;
        entry.remove(txn).await?;

        if inode.inner.nlink <= 1 {
            inode.add_read_conflict(txn).await?;
            inode.remove(txn).await?;
        } else {
            // Decrement nlink
            let mut updated = inode.clone();
            updated.inner.nlink -= 1;
            updated.add_read_conflict(txn).await?;
            updated.store(txn).await?;
        }
    }

    tracing::debug!(
        parent = resolve_result.parent_id,
        name = entry.name.as_str(),
        inode_id = entry.inode_id,
        "remove: removed entry"
    );

    Ok(meta::RemoveRsp {})
}
