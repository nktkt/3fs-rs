//! Chunk engine: manages chunk data layout, metadata, and I/O on local disks.
//!
//! This crate provides the low-level building blocks for storing and retrieving
//! fixed-size chunks of data.  For now it exposes stub types that the storage
//! service depends on; real disk I/O will be added later.

use hf3fs_types::ChunkId;
use serde::{Deserialize, Serialize};

/// Maximum chunk size (256 KiB by default, matching the C++ `kDefaultChunkSize`).
pub const DEFAULT_CHUNK_SIZE: u32 = 256 * 1024;

/// Metadata associated with a single chunk on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkMeta {
    /// Unique identifier for this chunk.
    pub chunk_id: ChunkId,
    /// Current length of valid data in the chunk (bytes).
    pub length: u32,
    /// CRC-32C checksum over the chunk data.
    pub checksum: u32,
}

impl ChunkMeta {
    /// Create metadata for a new, empty chunk.
    pub fn new(chunk_id: ChunkId) -> Self {
        Self {
            chunk_id,
            length: 0,
            checksum: 0,
        }
    }
}

/// The state a chunk can be in during its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkState {
    /// Chunk has been allocated but not yet written.
    Clean,
    /// Chunk has been written to but not yet committed.
    Dirty,
    /// Chunk has been committed and is considered durable.
    Committed,
}

impl Default for ChunkState {
    fn default() -> Self {
        Self::Clean
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_meta_new() {
        let meta = ChunkMeta::new(ChunkId(42));
        assert_eq!(meta.chunk_id, ChunkId(42));
        assert_eq!(meta.length, 0);
        assert_eq!(meta.checksum, 0);
    }

    #[test]
    fn test_chunk_state_default() {
        assert_eq!(ChunkState::default(), ChunkState::Clean);
    }

    #[test]
    fn test_default_chunk_size() {
        assert_eq!(DEFAULT_CHUNK_SIZE, 256 * 1024);
    }

    #[test]
    fn test_chunk_meta_serde() {
        let meta = ChunkMeta {
            chunk_id: ChunkId(100),
            length: 4096,
            checksum: 0xDEADBEEF,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let parsed: ChunkMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, meta);
    }
}
