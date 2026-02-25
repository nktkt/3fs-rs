//! Type re-exports and additional library-level types.
//!
//! Mirrors the type definitions from the C++ `hf3fs::lib` namespace.

/// A result type using a pair of (errno, message) as the error type,
/// matching the C++ `hf3fs::lib::Result<T>` = `nonstd::expected<T, pair<int, string>>`.
pub type Result<T> = std::result::Result<T, LibError>;

/// Error type for library operations, carrying an errno and message.
#[derive(Debug, Clone)]
pub struct LibError {
    /// The errno value.
    pub errno: i32,
    /// A human-readable description of the error.
    pub message: String,
}

impl LibError {
    pub fn new(errno: i32, message: impl Into<String>) -> Self {
        Self {
            errno,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for LibError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "errno {}: {}", self.errno, self.message)
    }
}

impl std::error::Error for LibError {}

/// A directory entry, mirroring the C++ `hf3fs::lib::dirent`.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// The file type (matches `d_type` from readdir).
    pub d_type: u8,
    /// The entry name.
    pub d_name: String,
}

/// Extended stat information, mirroring the C++ `hf3fs::lib::stat`.
#[derive(Debug, Clone)]
pub struct Stat {
    /// Standard stat fields (inode, mode, size, etc.).
    pub ino: u64,
    pub mode: u32,
    pub nlink: u64,
    pub uid: u32,
    pub gid: u32,
    pub size: i64,
    pub atime: i64,
    pub mtime: i64,
    pub ctime: i64,
    /// Symlink target, if this is a symlink.
    pub symlink_target: Option<String>,
}

/// An I/O segment descriptor for vectored I/O.
///
/// Corresponds to the C++ `hf3fs::lib::ioseg`.
#[derive(Debug, Clone, Copy)]
pub struct IoSeg {
    /// File descriptor.
    pub fd: i32,
    /// Offset within the file.
    pub off: i64,
}

impl Default for IoSeg {
    fn default() -> Self {
        Self { fd: -1, off: -1 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lib_error_display() {
        let err = LibError::new(libc::ENOENT, "file not found");
        let s = err.to_string();
        assert!(s.contains("file not found"));
        assert!(s.contains(&libc::ENOENT.to_string()));
    }

    #[test]
    fn test_dir_entry() {
        let entry = DirEntry {
            d_type: libc::DT_REG,
            d_name: "test.txt".to_string(),
        };
        assert_eq!(entry.d_name, "test.txt");
    }

    #[test]
    fn test_io_seg_default() {
        let seg = IoSeg::default();
        assert_eq!(seg.fd, -1);
        assert_eq!(seg.off, -1);
    }
}
