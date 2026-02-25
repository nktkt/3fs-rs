use hf3fs_types::{Result, Status, StatusCode};
use std::path::Path;

/// Read an entire file to a string.
pub fn read_file(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|e| {
        Status::with_message(StatusCode::IO_ERROR, format!("read {}: {}", path.display(), e))
    })
}

/// Read an entire file to bytes.
pub fn read_file_bytes(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| {
        Status::with_message(StatusCode::IO_ERROR, format!("read {}: {}", path.display(), e))
    })
}

/// Write data to a file.
pub fn write_file(path: &Path, data: &[u8]) -> Result<()> {
    std::fs::write(path, data).map_err(|e| {
        Status::with_message(StatusCode::IO_ERROR, format!("write {}: {}", path.display(), e))
    })
}

/// Atomic write: write to temp file then rename.
pub fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    let tmp_path = dir.join(format!(".tmp.{}", std::process::id()));
    write_file(&tmp_path, data)?;
    std::fs::rename(&tmp_path, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp_path);
        Status::with_message(StatusCode::IO_ERROR, format!("rename: {}", e))
    })
}
