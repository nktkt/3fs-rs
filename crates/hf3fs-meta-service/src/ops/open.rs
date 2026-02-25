//! Open operation: open an existing file.
//!
//! Based on 3FS/src/meta/store/ops/Open.cc (the OpenReq codepath)

use hf3fs_kv::ReadOnlyTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::make_error;
use hf3fs_types::status_code::MetaCode;
use hf3fs_types::Result;

use crate::config::MetaServiceConfig;
use crate::inode::{AccessType, Inode, FS_IMMUTABLE_FL, ROOT_INODE_ID};
use crate::path_resolve::PathResolver;

/// Execute an open operation.
///
/// For a read-only open, this only needs a read-only transaction.
/// Write opens with O_TRUNC or sessions would need a read-write transaction,
/// but this simplified version handles the common case.
pub async fn open(
    txn: &dyn ReadOnlyTransaction,
    config: &MetaServiceConfig,
    req: &meta::OpenReq,
) -> Result<meta::OpenRsp> {
    let parent = if req.path.parent == 0 {
        ROOT_INODE_ID
    } else {
        req.path.parent
    };

    // Resolve the path (open always follows symlinks)
    let inode = match &req.path.path {
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

    // Determine access type from flags
    let access_mode = req.flags.0 & libc::O_ACCMODE;
    let access_type = match access_mode {
        libc::O_RDONLY => AccessType::Read,
        libc::O_WRONLY => AccessType::Write,
        _ => AccessType::ReadWrite, // O_RDWR or default
    };

    if inode.is_directory() {
        // Can only open directories for reading
        if access_type != AccessType::Read || req.flags.0 & libc::O_TRUNC != 0 {
            return make_error(MetaCode::IS_DIRECTORY);
        }
        inode.acl().check_permission(&req.base.user, access_type)?;
        return Ok(meta::OpenRsp {
            stat: inode.into_proto(),
            need_truncate: false,
        });
    }

    if inode.is_file() {
        // Check O_DIRECTORY flag
        if req.flags.0 & libc::O_DIRECTORY != 0 {
            return make_error(MetaCode::NOT_DIRECTORY);
        }

        // Check immutable flag for writes
        if access_type != AccessType::Read && (inode.inner.iflags & FS_IMMUTABLE_FL != 0) {
            return make_error(MetaCode::NO_PERMISSION);
        }

        // Check permission
        inode.acl().check_permission(&req.base.user, access_type)?;

        let need_truncate = req.flags.0 & libc::O_TRUNC != 0;

        Ok(meta::OpenRsp {
            stat: inode.into_proto(),
            need_truncate,
        })
    } else {
        // Symlink should have been followed during resolution
        make_error(MetaCode::NOT_FILE)
    }
}
