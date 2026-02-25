//! Path resolution for the metadata service.
//!
//! Resolves file system paths (e.g. `/foo/bar/baz`) to their corresponding
//! directory entries and inodes, following symlinks as needed, and checking
//! permissions at each directory along the path.
//!
//! Based on 3FS/src/meta/store/PathResolve.h and PathResolve.cc

use hf3fs_kv::ReadOnlyTransaction;
use hf3fs_proto::meta;
use hf3fs_types::result::{make_error, make_error_msg};
use hf3fs_types::status_code::MetaCode;
use hf3fs_types::Result;

use crate::dir_entry::DirEntry;
use crate::inode::{AccessType, Acl, Inode, InodeId, ROOT_INODE_ID};

/// Result of resolving a path to its parent directory and optional entry.
#[derive(Debug, Clone)]
pub struct ResolveResult {
    /// The parent directory inode ID.
    pub parent_id: InodeId,
    /// The parent directory ACL.
    pub parent_acl: Acl,
    /// The directory entry at the resolved path, if it exists.
    pub dir_entry: Option<DirEntry>,
}

/// Result of resolving a path range (for mkdirs).
#[derive(Debug, Clone)]
pub struct ResolveRangeResult {
    /// The base resolve result (parent of the first missing component).
    pub base: ResolveResult,
    /// The path components that are missing and need to be created.
    pub missing: Vec<String>,
}

/// Path resolver that walks directory entries and follows symlinks.
///
/// Generic over `T` which must implement `ReadOnlyTransaction`. This allows
/// the resolver to work with both read-only and read-write transactions.
pub struct PathResolver<'a, T: ReadOnlyTransaction + ?Sized> {
    txn: &'a T,
    user: &'a meta::UserInfo,
    max_symlink_count: usize,
    max_symlink_depth: usize,
    symlink_count: usize,
    depth: usize,
}

impl<'a, T: ReadOnlyTransaction + ?Sized> PathResolver<'a, T> {
    /// Create a new path resolver.
    pub fn new(
        txn: &'a T,
        user: &'a meta::UserInfo,
        max_symlink_count: usize,
        max_symlink_depth: usize,
    ) -> Self {
        Self {
            txn,
            user,
            max_symlink_count,
            max_symlink_depth,
            symlink_count: 0,
            depth: 0,
        }
    }

    /// Resolve a path to its inode.
    ///
    /// `parent` is the starting inode ID (usually ROOT for absolute paths).
    /// `path` is the path string to resolve.
    /// `follow_last_symlink` controls whether to follow a symlink at the leaf.
    /// `check_ref_cnt` when true, verifies nlink > 0 on the result.
    pub async fn resolve_inode(
        &mut self,
        parent: InodeId,
        path: &str,
        follow_last_symlink: bool,
        check_ref_cnt: bool,
    ) -> Result<Inode> {
        let result = self
            .resolve_path(parent, path, follow_last_symlink)
            .await?;

        match result.dir_entry {
            Some(entry) => {
                let inode = entry.snapshot_load_inode(self.txn).await?;
                if check_ref_cnt && inode.inner.nlink == 0 {
                    return make_error(MetaCode::NOT_FOUND);
                }
                Ok(inode)
            }
            None => {
                // If we resolved a non-empty path but got no dir_entry,
                // the target does not exist.
                let trimmed = path.trim_matches('/');
                if !trimmed.is_empty() {
                    return make_error(MetaCode::NOT_FOUND);
                }
                // Path was just the parent inode itself (e.g. empty path or "/")
                let inode = Inode::snapshot_load(self.txn, parent)
                    .await?
                    .ok_or_else(|| {
                        hf3fs_types::Status::with_message(
                            MetaCode::NOT_FOUND,
                            format!("inode {} not found", parent),
                        )
                    })?;
                if check_ref_cnt && inode.inner.nlink == 0 {
                    return make_error(MetaCode::NOT_FOUND);
                }
                Ok(inode)
            }
        }
    }

    /// Resolve a path to a `ResolveResult` containing parent info and optional entry.
    pub async fn resolve_path(
        &mut self,
        parent: InodeId,
        path: &str,
        follow_last_symlink: bool,
    ) -> Result<ResolveResult> {
        // Handle empty path: just resolve the parent itself
        if path.is_empty() {
            let parent_inode = Inode::snapshot_load(self.txn, parent)
                .await?
                .ok_or_else(|| {
                    hf3fs_types::Status::with_message(
                        MetaCode::NOT_FOUND,
                        format!("parent inode {} not found", parent),
                    )
                })?;
            return Ok(ResolveResult {
                parent_id: parent,
                parent_acl: parent_inode.acl(),
                dir_entry: None,
            });
        }

        // Determine the starting point
        let (start_id, components) = if path.starts_with('/') {
            // Absolute path: start from root
            let trimmed = path.trim_start_matches('/');
            (ROOT_INODE_ID, trimmed)
        } else {
            // Relative path: start from parent
            (parent, path)
        };

        let parts: Vec<&str> = components
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if parts.is_empty() {
            let parent_inode = Inode::snapshot_load(self.txn, start_id)
                .await?
                .ok_or_else(|| {
                    hf3fs_types::Status::with_message(
                        MetaCode::NOT_FOUND,
                        format!("inode {} not found", start_id),
                    )
                })?;
            return Ok(ResolveResult {
                parent_id: start_id,
                parent_acl: parent_inode.acl(),
                dir_entry: None,
            });
        }

        let mut current_id = start_id;
        let mut current_acl: Option<Acl> = None;

        for (i, component) in parts.iter().enumerate() {
            let is_last = i == parts.len() - 1;

            // Handle special directory names
            if *component == "." {
                if is_last {
                    let parent_inode = Inode::snapshot_load(self.txn, current_id)
                        .await?
                        .ok_or_else(|| {
                            hf3fs_types::Status::with_message(
                                MetaCode::NOT_FOUND,
                                format!("inode {} not found", current_id),
                            )
                        })?;
                    return Ok(ResolveResult {
                        parent_id: current_id,
                        parent_acl: parent_inode.acl(),
                        dir_entry: None,
                    });
                }
                continue;
            }

            if *component == ".." {
                // Root's parent is itself
                if current_id == ROOT_INODE_ID {
                    if is_last {
                        let root_inode = Inode::snapshot_load(self.txn, ROOT_INODE_ID)
                            .await?
                            .ok_or_else(|| {
                                hf3fs_types::Status::new(MetaCode::NOT_FOUND)
                            })?;
                        return Ok(ResolveResult {
                            parent_id: ROOT_INODE_ID,
                            parent_acl: root_inode.acl(),
                            dir_entry: None,
                        });
                    }
                    continue;
                }
                return make_error_msg(
                    MetaCode::NOT_FOUND,
                    ".. traversal not fully supported in simplified resolver",
                );
            }

            // Load the current directory's inode to check permissions
            let dir_inode = Inode::snapshot_load(self.txn, current_id)
                .await?
                .ok_or_else(|| {
                    hf3fs_types::Status::with_message(
                        MetaCode::NOT_FOUND,
                        format!("directory inode {} not found", current_id),
                    )
                })?;

            if !dir_inode.is_directory() {
                return make_error(MetaCode::NOT_DIRECTORY);
            }

            // Check execute permission on directory
            dir_inode
                .acl()
                .check_permission(self.user, AccessType::Exec)?;
            let dir_acl = dir_inode.acl();

            // Look up the component
            let entry = DirEntry::snapshot_load(self.txn, current_id, component).await?;

            match entry {
                Some(entry) => {
                    if is_last {
                        // Symlink following at the leaf
                        if entry.is_symlink() && follow_last_symlink {
                            // TODO: Implement recursive symlink resolution.
                            // For now, return the symlink entry directly.
                            self.symlink_count += 1;
                            if self.symlink_count > self.max_symlink_count {
                                return make_error(MetaCode::TOO_MANY_SYMLINKS);
                            }
                        }
                        return Ok(ResolveResult {
                            parent_id: current_id,
                            parent_acl: dir_acl,
                            dir_entry: Some(entry),
                        });
                    }

                    // Not the last component: must be a directory or symlink
                    if entry.is_symlink() {
                        // TODO: Implement recursive symlink resolution for mid-path symlinks.
                        self.symlink_count += 1;
                        if self.symlink_count > self.max_symlink_count {
                            return make_error(MetaCode::TOO_MANY_SYMLINKS);
                        }
                        // Load the symlink target and try to resolve it as a directory.
                        let inode = entry.snapshot_load_inode(self.txn).await?;
                        if let Some(_target) = &inode.inner.symlink_target {
                            // For now, return an error for mid-path symlinks.
                            return make_error_msg(
                                MetaCode::NOT_FOUND,
                                "mid-path symlink resolution not yet implemented",
                            );
                        } else {
                            return make_error(MetaCode::NOT_FOUND);
                        }
                    } else if entry.is_directory() {
                        current_id = entry.inode_id;
                        current_acl = entry.dir_acl.clone();
                    } else {
                        // A file in the middle of the path
                        return make_error(MetaCode::NOT_DIRECTORY);
                    }
                }
                None => {
                    if is_last {
                        return Ok(ResolveResult {
                            parent_id: current_id,
                            parent_acl: dir_acl,
                            dir_entry: None,
                        });
                    } else {
                        return make_error(MetaCode::NOT_FOUND);
                    }
                }
            }
        }

        // If we get here, resolve the final current_id
        let acl = match current_acl {
            Some(acl) => acl,
            None => {
                let inode = Inode::snapshot_load(self.txn, current_id)
                    .await?
                    .ok_or_else(|| {
                        hf3fs_types::Status::new(MetaCode::NOT_FOUND)
                    })?;
                inode.acl()
            }
        };
        Ok(ResolveResult {
            parent_id: current_id,
            parent_acl: acl,
            dir_entry: None,
        })
    }

    /// Resolve a path for `mkdirs`, returning missing components.
    pub async fn resolve_path_range(
        &mut self,
        parent: InodeId,
        path: &str,
    ) -> Result<ResolveRangeResult> {
        let (start_id, components) = if path.starts_with('/') {
            (ROOT_INODE_ID, path.trim_start_matches('/'))
        } else {
            (parent, path)
        };

        let parts: Vec<&str> = components
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        if parts.is_empty() {
            let inode = Inode::snapshot_load(self.txn, start_id)
                .await?
                .ok_or_else(|| {
                    hf3fs_types::Status::new(MetaCode::NOT_FOUND)
                })?;
            return Ok(ResolveRangeResult {
                base: ResolveResult {
                    parent_id: start_id,
                    parent_acl: inode.acl(),
                    dir_entry: None,
                },
                missing: Vec::new(),
            });
        }

        let mut current_id = start_id;

        for (i, component) in parts.iter().enumerate() {
            // Load current directory inode for permission checks
            let dir_inode = Inode::snapshot_load(self.txn, current_id)
                .await?
                .ok_or_else(|| {
                    hf3fs_types::Status::with_message(
                        MetaCode::NOT_FOUND,
                        format!("directory inode {} not found", current_id),
                    )
                })?;

            if !dir_inode.is_directory() {
                return make_error(MetaCode::NOT_DIRECTORY);
            }

            dir_inode
                .acl()
                .check_permission(self.user, AccessType::Exec)?;

            let entry = DirEntry::snapshot_load(self.txn, current_id, component).await?;

            match entry {
                Some(entry) => {
                    if entry.is_directory() {
                        if i == parts.len() - 1 {
                            return Ok(ResolveRangeResult {
                                base: ResolveResult {
                                    parent_id: current_id,
                                    parent_acl: dir_inode.acl(),
                                    dir_entry: Some(entry),
                                },
                                missing: Vec::new(),
                            });
                        }
                        current_id = entry.inode_id;
                    } else if i == parts.len() - 1 {
                        return Ok(ResolveRangeResult {
                            base: ResolveResult {
                                parent_id: current_id,
                                parent_acl: dir_inode.acl(),
                                dir_entry: Some(entry),
                            },
                            missing: Vec::new(),
                        });
                    } else {
                        return make_error(MetaCode::NOT_DIRECTORY);
                    }
                }
                None => {
                    let missing = parts[i..]
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                    return Ok(ResolveRangeResult {
                        base: ResolveResult {
                            parent_id: current_id,
                            parent_acl: dir_inode.acl(),
                            dir_entry: None,
                        },
                        missing,
                    });
                }
            }
        }

        let inode = Inode::snapshot_load(self.txn, current_id)
            .await?
            .ok_or_else(|| hf3fs_types::Status::new(MetaCode::NOT_FOUND))?;
        Ok(ResolveRangeResult {
            base: ResolveResult {
                parent_id: current_id,
                parent_acl: inode.acl(),
                dir_entry: None,
            },
            missing: Vec::new(),
        })
    }

    // NOTE: Recursive symlink following (follow_symlink / follow_symlink_to_dir)
    // is deferred. The current implementation handles leaf symlinks inline and
    // returns an error for mid-path symlinks. A full implementation will need
    // manual future boxing to handle the async recursion.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_result_defaults() {
        let r = ResolveResult {
            parent_id: ROOT_INODE_ID,
            parent_acl: Acl::root(),
            dir_entry: None,
        };
        assert_eq!(r.parent_id, 0);
        assert!(r.dir_entry.is_none());
    }

    #[test]
    fn test_resolve_range_result() {
        let r = ResolveRangeResult {
            base: ResolveResult {
                parent_id: 0,
                parent_acl: Acl::root(),
                dir_entry: None,
            },
            missing: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        };
        assert_eq!(r.missing.len(), 3);
    }
}
