//! Rename operation: move/rename a file or directory.
//!
//! Based on 3FS/src/meta/store/ops/Rename.cc

use hf3fs_kv::ReadWriteTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::{MetaCode, StatusCode};
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::dir_entry::DirEntry;
use crate::inode::{AccessType, Inode, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Execute a rename operation within a read-write transaction.
pub async fn rename(
    txn: &mut dyn ReadWriteTransaction,
    config: &MetaServiceConfig,
    req: &meta::RenameReq,
) -> Result<meta::RenameRsp> {
    // Validate source path
    let src_path = req
        .src
        .path
        .as_deref()
        .ok_or_else(|| {
            hf3fs_types::Status::with_message(StatusCode::INVALID_ARG, "src path not set")
        })?;
    // Validate destination path
    let dest_path = req
        .dest
        .path
        .as_deref()
        .ok_or_else(|| {
            hf3fs_types::Status::with_message(StatusCode::INVALID_ARG, "dest path not set")
        })?;

    let src_parent = if req.src.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.src.parent
    };
    let dest_parent = if req.dest.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.dest.parent
    };

    // Resolve source path
    let mut resolver = PathResolver::new(
        txn,
        &req.base.user,
        config.max_symlink_count,
        config.max_symlink_depth,
    );
    let src_result = resolver.resolve_path(src_parent, src_path, false).await?;
    let src_entry = src_result.dir_entry.ok_or_else(|| {
        hf3fs_types::Status::new(MetaCode::NOT_FOUND)
    })?;

    // Verify inode_id if specified
    if let Some(expected_id) = req.inode_id {
        if src_entry.inode_id != expected_id {
            return make_error(MetaCode::NOT_FOUND);
        }
    }

    // Check write permission on source parent
    let src_parent_inode = Inode::load(txn, src_result.parent_id)
        .await?
        .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
    src_parent_inode.acl().check_permission(
        &req.base.user,
        AccessType::Write,
    )?;

    // Resolve destination path
    let mut resolver2 = PathResolver::new(
        txn,
        &req.base.user,
        config.max_symlink_count,
        config.max_symlink_depth,
    );
    let dest_result = resolver2
        .resolve_path(dest_parent, dest_path, false)
        .await?;

    // Check write permission on destination parent
    let dest_parent_inode = Inode::load(txn, dest_result.parent_id)
        .await?
        .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
    dest_parent_inode.acl().check_permission(
        &req.base.user,
        AccessType::Write,
    )?;

    // Extract destination filename
    let dest_filename = std::path::Path::new(dest_path)
        .file_name()
        .and_then(|f| f.to_str())
        .ok_or_else(|| {
            hf3fs_types::Status::with_message(
                StatusCode::INVALID_ARG,
                "dest path has no filename",
            )
        })?
        .to_string();

    // Handle destination that already exists
    if let Some(dest_entry) = &dest_result.dir_entry {
        // If destination exists, we need to replace it
        if dest_entry.is_directory() && !src_entry.is_directory() {
            return make_error(MetaCode::IS_DIRECTORY);
        }
        if !dest_entry.is_directory() && src_entry.is_directory() {
            return make_error(MetaCode::NOT_DIRECTORY);
        }

        // Remove the existing destination entry
        dest_entry.add_read_conflict(txn).await?;
        let dest_inode = Inode::load(txn, dest_entry.inode_id)
            .await?
            .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
        dest_entry.remove(txn).await?;

        if dest_inode.inner.nlink <= 1 {
            dest_inode.remove(txn).await?;
        } else {
            let mut updated = dest_inode;
            updated.inner.nlink -= 1;
            updated.store(txn).await?;
        }
    }

    // Remove source entry
    src_entry.add_read_conflict(txn).await?;
    src_entry.remove(txn).await?;

    // Create new destination entry with the same inode
    let new_entry = DirEntry {
        parent: dest_result.parent_id,
        name: dest_filename,
        inode_id: src_entry.inode_id,
        inode_type: src_entry.inode_type,
        dir_acl: src_entry.dir_acl.clone(),
    };
    new_entry.store(txn).await?;

    // Add conflict keys
    txn.add_read_conflict(&Inode::pack_key_for(src_result.parent_id))
        .await?;
    txn.add_read_conflict(&Inode::pack_key_for(dest_result.parent_id))
        .await?;

    // Load and return the renamed inode stat
    let inode = Inode::snapshot_load(txn, src_entry.inode_id)
        .await?
        .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;

    tracing::debug!(
        src_parent = src_result.parent_id,
        src_name = src_entry.name.as_str(),
        dest_parent = dest_result.parent_id,
        inode_id = src_entry.inode_id,
        "rename: completed"
    );

    Ok(meta::RenameRsp {
        stat: Some(inode.into_proto()),
    })
}
