//! Mkdirs operation: create one or more directories along a path.
//!
//! Based on 3FS/src/meta/store/ops/Mkdirs.cc

use hf3fs_kv::ReadWriteTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::{MetaCode, StatusCode};
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::dir_entry::DirEntry;
use crate::inode::{Acl, Inode, InodeId, FS_FL_INHERITABLE, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Execute a mkdirs operation within a read-write transaction.
///
/// `alloc_inode_id` is a callback that allocates a new unique inode ID.
pub async fn mkdirs<F>(
    txn: &mut dyn ReadWriteTransaction,
    config: &MetaServiceConfig,
    req: &meta::MkdirsReq,
    now_ns: i64,
    mut alloc_inode_id: F,
) -> Result<meta::MkdirsRsp>
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

    let parent = if req.path.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.path.parent
    };

    // Resolve the path to find which components need creating
    let mut resolver = PathResolver::new(
        txn,
        &req.base.user,
        config.max_symlink_count,
        config.max_symlink_depth,
    );
    let resolve_result = resolver.resolve_path_range(parent, path).await?;

    // If nothing is missing, the path already exists
    if resolve_result.missing.is_empty() {
        if resolve_result.base.dir_entry.is_some() {
            return make_error(MetaCode::EXISTS);
        }
        // The path itself was resolved to a directory; load it
        let inode = Inode::snapshot_load(txn, resolve_result.base.parent_id)
            .await?
            .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
        return Ok(meta::MkdirsRsp {
            stat: inode.into_proto(),
        });
    }

    // If multiple components are missing but not recursive, error
    if resolve_result.missing.len() > 1 && !req.recursive {
        return make_error(MetaCode::NOT_FOUND);
    }

    // Check write permission on the parent
    let parent_inode = Inode::load(txn, resolve_result.base.parent_id)
        .await?
        .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;

    parent_inode.acl().check_permission(
        &req.base.user,
        crate::inode::AccessType::Write,
    )?;

    let layout = req.layout.clone().or_else(|| parent_inode.inner.layout.clone());

    // Inherit the parent's inheritable iflags
    let inherited_iflags = parent_inode.inner.iflags & FS_FL_INHERITABLE;
    let base_perm = req.perm.0 & 0o7777; // ALLPERMS

    // S_ISGID: inherit group ID from parent
    let s_isgid: u32 = 0o2000;
    let (gid, perm) = if parent_inode.inner.permission & s_isgid != 0 {
        (parent_inode.inner.gid, base_perm | s_isgid)
    } else {
        (req.base.user.gid, base_perm)
    };

    let acl = Acl {
        uid: req.base.user.uid,
        gid,
        perm,
        iflags: inherited_iflags,
    };

    let mut current_parent_id = resolve_result.base.parent_id;
    let mut last_inode = None;

    // Add parent inode to read conflict set to prevent concurrent removal
    txn.add_read_conflict(&Inode::pack_key_for(current_parent_id))
        .await?;

    for (i, component) in resolve_result.missing.iter().enumerate() {
        // Validate component name
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.contains('/')
        {
            return make_error_msg(
                StatusCode::INVALID_ARG,
                format!("invalid directory name: {}", component),
            );
        }

        let inode_id = alloc_inode_id();

        let inode = Inode::new_directory(
            inode_id,
            current_parent_id,
            component,
            &acl,
            layout.clone(),
            now_ns,
        );

        let entry = DirEntry::new_directory(
            current_parent_id,
            component.clone(),
            inode_id,
            acl.clone(),
        );

        // Add the dir entry key to read conflict set (prevent concurrent create)
        if i == 0 {
            entry.add_read_conflict(txn).await?;
        }

        // Store the entry and inode
        entry.store(txn).await?;
        inode.store(txn).await?;

        tracing::debug!(
            parent = current_parent_id,
            name = component.as_str(),
            inode_id = inode_id,
            "mkdirs: created directory"
        );

        current_parent_id = inode_id;
        last_inode = Some(inode);
    }

    let inode = last_inode.ok_or_else(|| {
        hf3fs_types::Status::with_message(MetaCode::FOUND_BUG, "no inode created in mkdirs")
    })?;

    Ok(meta::MkdirsRsp {
        stat: inode.into_proto(),
    })
}
