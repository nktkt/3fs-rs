//! Symlink operation: create a symbolic link.
//!
//! Based on 3FS/src/meta/store/ops/Symlink.cc

use hf3fs_kv::ReadWriteTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::{MetaCode, StatusCode};
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::dir_entry::DirEntry;
use crate::inode::{AccessType, Inode, InodeId, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Execute a symlink creation operation.
pub async fn symlink<F>(
    txn: &mut dyn ReadWriteTransaction,
    config: &MetaServiceConfig,
    req: &meta::SymlinkReq,
    now_ns: i64,
    mut alloc_inode_id: F,
) -> Result<meta::SymlinkRsp>
where
    F: FnMut() -> InodeId,
{
    // Validate path
    let path = req
        .path
        .path
        .as_deref()
        .ok_or_else(|| {
            hf3fs_types::Status::with_message(StatusCode::INVALID_ARG, "path not set")
        })?;

    if path.is_empty() {
        return make_error_msg(StatusCode::INVALID_ARG, "path is empty");
    }

    if req.target.is_empty() {
        return make_error_msg(StatusCode::INVALID_ARG, "symlink target is empty");
    }

    let parent = if req.path.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.path.parent
    };

    // Resolve the path (do not follow last symlink)
    let mut resolver = PathResolver::new(
        txn,
        &req.base.user,
        config.max_symlink_count,
        config.max_symlink_depth,
    );
    let resolve_result = resolver.resolve_path(parent, path, false).await?;

    if resolve_result.dir_entry.is_some() {
        // An entry already exists at this path
        return make_error(MetaCode::EXISTS);
    }

    // Check write permission on parent
    let parent_inode = Inode::load(txn, resolve_result.parent_id)
        .await?
        .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
    parent_inode.acl().check_permission(
        &req.base.user,
        AccessType::Write,
    )?;

    let inode_id = alloc_inode_id();

    // Extract filename
    let filename = std::path::Path::new(path)
        .file_name()
        .and_then(|f| f.to_str())
        .ok_or_else(|| {
            hf3fs_types::Status::with_message(
                StatusCode::INVALID_ARG,
                "path has no filename",
            )
        })?
        .to_string();

    let inode = Inode::new_symlink(
        inode_id,
        req.target.clone(),
        req.base.user.uid,
        req.base.user.gid,
        now_ns,
    );

    let entry = DirEntry::new_symlink(resolve_result.parent_id, filename, inode_id);

    // Add conflict keys
    txn.add_read_conflict(&Inode::pack_key_for(resolve_result.parent_id))
        .await?;
    entry.add_read_conflict(txn).await?;

    // Store
    entry.store(txn).await?;
    inode.store(txn).await?;

    tracing::debug!(
        parent = resolve_result.parent_id,
        inode_id = inode_id,
        target = req.target.as_str(),
        "symlink: created"
    );

    Ok(meta::SymlinkRsp {
        stat: inode.into_proto(),
    })
}
