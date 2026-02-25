//! In-memory chunk store for testing and development.
//!
//! [`ChunkStore`] uses a `DashMap` to hold chunk data in memory.  This is a
//! stand-in for the real on-disk engine backed by `hf3fs-chunk-engine`.  The
//! API is intentionally kept close to what the disk-backed version will expose
//! so that the service layer does not need to change.

use dashmap::DashMap;
use hf3fs_chunk_engine::DEFAULT_CHUNK_SIZE;
use hf3fs_types::{
    make_error_msg,
    status_code::StorageCode,
    Result,
};

/// In-memory chunk store backed by a concurrent hash map.
///
/// Each chunk is stored as a `Vec<u8>`.  Reads and writes are performed at
/// arbitrary byte offsets within a chunk; the underlying buffer is grown as
/// needed up to [`DEFAULT_CHUNK_SIZE`].
#[derive(Debug)]
pub struct ChunkStore {
    chunks: DashMap<u64, Vec<u8>>,
    max_chunk_size: u32,
}

impl Default for ChunkStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkStore {
    /// Create a new, empty chunk store with the default maximum chunk size.
    pub fn new() -> Self {
        Self {
            chunks: DashMap::new(),
            max_chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Create a chunk store with a custom maximum chunk size.
    pub fn with_max_chunk_size(max_chunk_size: u32) -> Self {
        Self {
            chunks: DashMap::new(),
            max_chunk_size,
        }
    }

    /// Read `length` bytes starting at `offset` from the given chunk.
    ///
    /// Returns the data as a `Vec<u8>`.  If the chunk does not exist, an error
    /// with [`StorageCode::CHUNK_METADATA_NOT_FOUND`] is returned.  If the
    /// requested range extends beyond the current data, only the available
    /// bytes are returned (which may be an empty vector if `offset` is past
    /// the end).
    pub fn read(&self, chunk_id: u64, offset: u64, length: u32) -> Result<Vec<u8>> {
        let entry = self.chunks.get(&chunk_id).ok_or_else(|| {
            hf3fs_types::Status::with_message(
                StorageCode::CHUNK_METADATA_NOT_FOUND,
                format!("chunk {} not found", chunk_id),
            )
        })?;

        let data = entry.value();
        let start = offset as usize;

        if start >= data.len() {
            // Offset is beyond current data -- return empty.
            return Ok(Vec::new());
        }

        let end = std::cmp::min(start + length as usize, data.len());
        Ok(data[start..end].to_vec())
    }

    /// Write `data` into the given chunk starting at `offset`.
    ///
    /// If the chunk does not exist, it is created.  The buffer is extended with
    /// zeroes if `offset` is beyond the current length.  Returns the number of
    /// bytes written.
    ///
    /// Returns an error with [`StorageCode::CHUNK_SIZE_MISMATCH`] if the write
    /// would exceed the maximum chunk size.
    pub fn write(&self, chunk_id: u64, offset: u64, data: &[u8]) -> Result<u32> {
        let end = offset as usize + data.len();

        if end > self.max_chunk_size as usize {
            return make_error_msg(
                StorageCode::CHUNK_SIZE_MISMATCH,
                format!(
                    "write to chunk {} would exceed max size ({} > {})",
                    chunk_id, end, self.max_chunk_size
                ),
            );
        }

        let mut entry = self.chunks.entry(chunk_id).or_insert_with(Vec::new);
        let buf = entry.value_mut();

        // Extend the buffer if needed.
        if end > buf.len() {
            buf.resize(end, 0);
        }

        let start = offset as usize;
        buf[start..end].copy_from_slice(data);

        Ok(data.len() as u32)
    }

    /// Remove a chunk from the store.
    ///
    /// Returns an error with [`StorageCode::CHUNK_METADATA_NOT_FOUND`] if the
    /// chunk does not exist.
    pub fn remove(&self, chunk_id: u64) -> Result<()> {
        self.chunks
            .remove(&chunk_id)
            .map(|_| ())
            .ok_or_else(|| {
                hf3fs_types::Status::with_message(
                    StorageCode::CHUNK_METADATA_NOT_FOUND,
                    format!("chunk {} not found", chunk_id),
                )
            })
    }

    /// Return the number of chunks currently stored.
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    /// Return `true` if the store contains no chunks.
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// Check whether a chunk with the given id exists.
    pub fn contains(&self, chunk_id: u64) -> bool {
        self.chunks.contains_key(&chunk_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read() {
        let store = ChunkStore::new();
        let data = b"hello, chunks!";
        let written = store.write(1, 0, data).unwrap();
        assert_eq!(written, data.len() as u32);

        let read_back = store.read(1, 0, data.len() as u32).unwrap();
        assert_eq!(read_back, data);
    }

    #[test]
    fn test_read_nonexistent_chunk() {
        let store = ChunkStore::new();
        let result = store.read(999, 0, 100);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), StorageCode::CHUNK_METADATA_NOT_FOUND);
    }

    #[test]
    fn test_read_offset_beyond_data() {
        let store = ChunkStore::new();
        store.write(1, 0, b"short").unwrap();

        let result = store.read(1, 100, 10).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_read_partial() {
        let store = ChunkStore::new();
        store.write(1, 0, b"abcdefgh").unwrap();

        // Read more than available from offset 4.
        let result = store.read(1, 4, 100).unwrap();
        assert_eq!(result, b"efgh");
    }

    #[test]
    fn test_write_at_offset() {
        let store = ChunkStore::new();
        // First write creates the chunk.
        store.write(1, 0, b"aaaa").unwrap();
        // Second write at an offset.
        store.write(1, 4, b"bbbb").unwrap();

        let result = store.read(1, 0, 8).unwrap();
        assert_eq!(result, b"aaaabbbb");
    }

    #[test]
    fn test_write_with_gap() {
        let store = ChunkStore::new();
        // Write at offset 4 with no prior data -- gap filled with zeroes.
        store.write(1, 4, b"data").unwrap();

        let result = store.read(1, 0, 8).unwrap();
        assert_eq!(&result[..4], &[0, 0, 0, 0]);
        assert_eq!(&result[4..], b"data");
    }

    #[test]
    fn test_write_overwrite() {
        let store = ChunkStore::new();
        store.write(1, 0, b"aaaa").unwrap();
        store.write(1, 0, b"bb").unwrap();

        let result = store.read(1, 0, 4).unwrap();
        assert_eq!(result, b"bbaa");
    }

    #[test]
    fn test_write_exceeds_max_size() {
        let store = ChunkStore::with_max_chunk_size(16);
        let result = store.write(1, 0, &[0u8; 32]);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), StorageCode::CHUNK_SIZE_MISMATCH);
    }

    #[test]
    fn test_remove_existing() {
        let store = ChunkStore::new();
        store.write(1, 0, b"data").unwrap();
        assert!(store.contains(1));

        store.remove(1).unwrap();
        assert!(!store.contains(1));
    }

    #[test]
    fn test_remove_nonexistent() {
        let store = ChunkStore::new();
        let result = store.remove(999);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), StorageCode::CHUNK_METADATA_NOT_FOUND);
    }

    #[test]
    fn test_len_and_is_empty() {
        let store = ChunkStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);

        store.write(1, 0, b"a").unwrap();
        store.write(2, 0, b"b").unwrap();
        assert_eq!(store.len(), 2);
        assert!(!store.is_empty());
    }
}
