//! Create operation: create a new file (O_CREAT).
//!
//! Based on 3FS/src/meta/store/ops/Open.cc (the CreateReq codepath)

use hf3fs_kv::ReadWriteTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::{MetaCode, StatusCode};
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::dir_entry::DirEntry;
use crate::inode::{Acl, Inode, InodeId, FS_FL_INHERITABLE, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Execute a create (file) operation within a read-write transaction.
///
/// `alloc_inode_id` is a callback that allocates a new unique inode ID.
pub async fn create<F>(
    txn: &mut dyn ReadWriteTransaction,
    config: &MetaServiceConfig,
    req: &meta::CreateReq,
    now_ns: i64,
    mut alloc_inode_id: F,
) -> Result<meta::CreateRsp>
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

    // Resolve the parent path
    let mut resolver = PathResolver::new(
        txn,
        &req.base.user,
        config.max_symlink_count,
        config.max_symlink_depth,
    );

    // Follow symlinks when resolving the path for create
    let resolve_result = resolver.resolve_path(parent, path, true).await?;

    if let Some(entry) = &resolve_result.dir_entry {
        // File already exists
        let inode = entry.snapshot_load_inode(txn).await?;

        // O_EXCL check
        if req.flags.0 & libc::O_EXCL != 0 {
            return make_error(MetaCode::EXISTS);
        }

        if inode.is_directory() {
            return make_error(MetaCode::IS_DIRECTORY);
        }

        // Check permission
        inode.acl().check_permission(
            &req.base.user,
            crate::inode::AccessType::Write,
        )?;

        let need_truncate = req.flags.0 & libc::O_TRUNC != 0;
        Ok(meta::CreateRsp {
            stat: inode.into_proto(),
            need_truncate,
        })
    } else {
        // File does not exist, create it
        let parent_inode = Inode::load(txn, resolve_result.parent_id)
            .await?
            .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;

        // Check write permission on parent
        parent_inode.acl().check_permission(
            &req.base.user,
            crate::inode::AccessType::Write,
        )?;

        // Determine file layout (use specified or inherit from parent)
        let layout = req
            .layout
            .clone()
            .or_else(|| parent_inode.inner.layout.clone());

        let inherited_iflags = parent_inode.inner.iflags & FS_FL_INHERITABLE;
        let acl = Acl {
            uid: req.base.user.uid,
            gid: req.base.user.gid,
            perm: req.perm.0 & 0o7777,
            iflags: inherited_iflags,
        };

        let inode_id = alloc_inode_id();
        let inode = Inode::new_file(inode_id, &acl, layout, now_ns);

        // Extract the filename from the path
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

        let entry = DirEntry::new_file(resolve_result.parent_id, filename, inode_id);

        // Add parent and entry to read conflict set
        txn.add_read_conflict(&Inode::pack_key_for(resolve_result.parent_id))
            .await?;
        entry.add_read_conflict(txn).await?;

        // Store entry and inode
        entry.store(txn).await?;
        inode.store(txn).await?;

        tracing::debug!(
            parent = resolve_result.parent_id,
            inode_id = inode_id,
            "create: created file"
        );

        Ok(meta::CreateRsp {
            stat: inode.into_proto(),
            need_truncate: false,
        })
    }
}
