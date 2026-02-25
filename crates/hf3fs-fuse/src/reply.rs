//! Reply types for FUSE operations.
//!
//! Each FUSE operation produces one of these reply types. The reply is
//! consumed by the FUSE transport layer to send the appropriate kernel
//! response. This abstraction allows the `FuseOps` trait to be tested
//! without a real kernel FUSE connection.

use crate::types::{FileAttr, FuseDirEntryPlus, FuseEntryParam, StatFs};
use std::time::Duration;

/// Reply for operations that return an entry (lookup, mkdir, mknod, symlink, link, create).
#[derive(Debug, Clone)]
pub struct ReplyEntry {
    pub entry: FuseEntryParam,
}

/// Reply for getattr.
#[derive(Debug, Clone)]
pub struct ReplyAttr {
    pub attr: FileAttr,
    pub attr_timeout: Duration,
}

/// Reply for open/opendir.
#[derive(Debug, Clone)]
pub struct ReplyOpen {
    /// File handle assigned by the filesystem.
    pub fh: u64,
    /// Flags back to the kernel (direct_io, keep_cache, etc.).
    pub flags: u32,
}

/// Reply for read operations.
#[derive(Debug)]
pub struct ReplyData {
    pub data: Vec<u8>,
}

/// Reply for write operations.
#[derive(Debug, Clone, Copy)]
pub struct ReplyWrite {
    /// Number of bytes written.
    pub written: u32,
}

/// Reply for readdir operations (list of entries).
#[derive(Debug)]
pub struct ReplyDirectory {
    pub entries: Vec<crate::types::FuseDirEntry>,
}

/// Reply for readdirplus operations (list of entries with attributes).
#[derive(Debug)]
pub struct ReplyDirectoryPlus {
    pub entries: Vec<FuseDirEntryPlus>,
}

/// Reply for readlink.
#[derive(Debug, Clone)]
pub struct ReplyReadlink {
    pub target: String,
}

/// Reply for statfs.
#[derive(Debug, Clone)]
pub struct ReplyStatFs {
    pub stat: StatFs,
}

/// Reply for create (returns both entry and open info).
#[derive(Debug, Clone)]
pub struct ReplyCreate {
    pub entry: FuseEntryParam,
    pub fh: u64,
    pub flags: u32,
}

/// Reply for xattr operations that return data.
#[derive(Debug)]
pub struct ReplyXattr {
    /// If `data` is `Some`, the xattr value; if `None`, just the size.
    pub data: Option<Vec<u8>>,
    /// Size of the xattr value (always set).
    pub size: u32,
}

/// Unified FUSE reply enum covering all operation result types.
///
/// Each variant wraps the specific reply type for that operation.
/// Operations that only return an error code (unlink, rmdir, rename, etc.)
/// return `Ok(ReplyEmpty)` on success or `Err(errno)` on failure.
#[derive(Debug)]
pub enum FuseReply {
    /// Operations that return no data on success (unlink, rmdir, rename, fsync, etc.).
    Empty,
    /// Entry-based replies (lookup, mkdir, mknod, symlink, link).
    Entry(ReplyEntry),
    /// Attribute reply (getattr, setattr).
    Attr(ReplyAttr),
    /// Open reply (open, opendir).
    Open(ReplyOpen),
    /// Data reply (read).
    Data(ReplyData),
    /// Write reply (write).
    Write(ReplyWrite),
    /// Directory listing reply (readdir).
    Directory(ReplyDirectory),
    /// Directory listing with attributes (readdirplus).
    DirectoryPlus(ReplyDirectoryPlus),
    /// Readlink reply.
    Readlink(ReplyReadlink),
    /// Statfs reply.
    StatFs(ReplyStatFs),
    /// Create reply (entry + open).
    Create(ReplyCreate),
    /// Xattr reply.
    Xattr(ReplyXattr),
}

/// Result type for FUSE operations.
///
/// The error is an errno value (positive integer).
pub type FuseResult<T> = std::result::Result<T, i32>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FileAttr;

    #[test]
    fn test_reply_write() {
        let r = ReplyWrite { written: 4096 };
        assert_eq!(r.written, 4096);
    }

    #[test]
    fn test_reply_data() {
        let r = ReplyData {
            data: vec![1, 2, 3, 4],
        };
        assert_eq!(r.data.len(), 4);
    }

    #[test]
    fn test_reply_readlink() {
        let r = ReplyReadlink {
            target: "/some/target".to_string(),
        };
        assert_eq!(r.target, "/some/target");
    }

    #[test]
    fn test_reply_attr() {
        let r = ReplyAttr {
            attr: FileAttr::new(42),
            attr_timeout: Duration::from_secs(30),
        };
        assert_eq!(r.attr.ino, 42);
        assert_eq!(r.attr_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_fuse_result_ok() {
        let result: FuseResult<ReplyWrite> = Ok(ReplyWrite { written: 100 });
        assert!(result.is_ok());
    }

    #[test]
    fn test_fuse_result_err() {
        let result: FuseResult<ReplyWrite> = Err(libc::EIO);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), libc::EIO);
    }

    #[test]
    fn test_fuse_reply_enum_variants() {
        // Ensure all variants can be constructed
        let _ = FuseReply::Empty;
        let _ = FuseReply::Write(ReplyWrite { written: 0 });
        let _ = FuseReply::Data(ReplyData { data: vec![] });
        let _ = FuseReply::Readlink(ReplyReadlink {
            target: String::new(),
        });
    }
}
