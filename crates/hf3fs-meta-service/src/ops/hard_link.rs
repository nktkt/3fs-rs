//! Hard link operation: create an additional directory entry pointing to an existing inode.
//!
//! Based on 3FS/src/meta/store/ops/HardLink.cc

use hf3fs_kv::ReadWriteTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::{MetaCode, StatusCode};
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::dir_entry::DirEntry;
use crate::inode::{AccessType, Inode, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Execute a hard link operation.
pub async fn hard_link(
    txn: &mut dyn ReadWriteTransaction,
    config: &MetaServiceConfig,
    req: &meta::HardLinkReq,
) -> Result<meta::HardLinkRsp> {
    // Validate new path
    let new_path = req
        .new_path
        .path
        .as_deref()
        .ok_or_else(|| {
            hf3fs_types::Status::with_message(StatusCode::INVALID_ARG, "new_path not set")
        })?;

    if new_path.is_empty() {
        return make_error_msg(StatusCode::INVALID_ARG, "new_path is empty");
    }

    let old_parent = if req.old_path.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.old_path.parent
    };
    let new_parent = if req.new_path.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.new_path.parent
    };

    // Resolve the old (source) inode
    let follow_symlinks = req.flags.0 & libc::AT_SYMLINK_NOFOLLOW == 0;
    let mut resolver = PathResolver::new(
        txn,
        &req.base.user,
        config.max_symlink_count,
        config.max_symlink_depth,
    );

    let source_inode = match &req.old_path.path {
        Some(path) if !path.is_empty() => {
            resolver
                .resolve_inode(old_parent, path, follow_symlinks, true)
                .await?
        }
        _ => {
            Inode::load(txn, old_parent)
                .await?
                .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?
        }
    };

    // Cannot hard link directories
    if source_inode.is_directory() {
        return make_error(MetaCode::IS_DIRECTORY);
    }

    // Resolve the new (destination) path
    let mut resolver2 = PathResolver::new(
        txn,
        &req.base.user,
        config.max_symlink_count,
        config.max_symlink_depth,
    );
    let dest_result = resolver2
        .resolve_path(new_parent, new_path, false)
        .await?;

    if dest_result.dir_entry.is_some() {
        return make_error(MetaCode::EXISTS);
    }

    // Check write permission on destination parent
    let dest_parent_inode = Inode::load(txn, dest_result.parent_id)
        .await?
        .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
    dest_parent_inode.acl().check_permission(
        &req.base.user,
        AccessType::Write,
    )?;

    // Extract filename
    let filename = std::path::Path::new(new_path)
        .file_name()
        .and_then(|f| f.to_str())
        .ok_or_else(|| {
            hf3fs_types::Status::with_message(
                StatusCode::INVALID_ARG,
                "new_path has no filename",
            )
        })?
        .to_string();

    // Create the new directory entry
    let entry = DirEntry::new_file(dest_result.parent_id, filename, source_inode.id());

    // Increment nlink
    let mut updated_inode = source_inode.clone();
    updated_inode.inner.nlink += 1;

    // Add conflict keys
    txn.add_read_conflict(&Inode::pack_key_for(dest_result.parent_id))
        .await?;
    entry.add_read_conflict(txn).await?;
    updated_inode.add_read_conflict(txn).await?;

    // Store
    entry.store(txn).await?;
    updated_inode.store(txn).await?;

    tracing::debug!(
        inode_id = source_inode.id(),
        nlink = updated_inode.inner.nlink,
        "hard_link: created"
    );

    Ok(meta::HardLinkRsp {
        stat: updated_inode.into_proto(),
    })
}
