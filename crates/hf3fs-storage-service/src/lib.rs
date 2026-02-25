//! Storage service crate for hf3fs.
//!
//! This crate implements the chunk storage service that manages chunk data on
//! local disks (or in memory for testing).  It receives read, write, and remove
//! requests via the `IStorageService` trait and delegates to a [`ChunkStore`].

pub mod chunk_store;
pub mod service;

use async_trait::async_trait;
use hf3fs_proto::storage::{
    ReadChunkReq, ReadChunkRsp, RemoveChunkReq, RemoveChunkRsp, WriteChunkReq, WriteChunkRsp,
};
use hf3fs_service_derive::{hf3fs_service, method};
use hf3fs_types::Result;

// ---------------------------------------------------------------------------
// Service trait
// ---------------------------------------------------------------------------

/// The storage service RPC interface.
///
/// Manages chunk data on storage targets.  Each method corresponds to a single
/// RPC endpoint identified by a numeric method id.
#[hf3fs_service(id = 3, name = "Storage")]
#[async_trait]
pub trait IStorageService: Send + Sync {
    /// Read chunk data at the specified offset and length.
    #[method(id = 1)]
    async fn read_chunk(&self, req: ReadChunkReq) -> Result<ReadChunkRsp>;

    /// Write data into a chunk at the specified offset.
    #[method(id = 2)]
    async fn write_chunk(&self, req: WriteChunkReq) -> Result<WriteChunkRsp>;

    /// Remove a chunk from storage.
    #[method(id = 3)]
    async fn remove_chunk(&self, req: RemoveChunkReq) -> Result<RemoveChunkRsp>;
}

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

pub use chunk_store::ChunkStore;
pub use service::StorageServiceImpl;

// Re-export the generated service metadata module.
pub use i_storage_service_service_meta as storage_service_meta;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_meta_constants() {
        assert_eq!(storage_service_meta::SERVICE_ID, 3);
        assert_eq!(storage_service_meta::SERVICE_NAME, "Storage");
    }

    #[test]
    fn test_method_ids() {
        use storage_service_meta::MethodId;

        assert_eq!(MethodId::ReadChunk.as_u16(), 1);
        assert_eq!(MethodId::WriteChunk.as_u16(), 2);
        assert_eq!(MethodId::RemoveChunk.as_u16(), 3);
    }

    #[test]
    fn test_method_id_from_u16() {
        use storage_service_meta::MethodId;

        assert_eq!(MethodId::from_u16(1), Some(MethodId::ReadChunk));
        assert_eq!(MethodId::from_u16(2), Some(MethodId::WriteChunk));
        assert_eq!(MethodId::from_u16(3), Some(MethodId::RemoveChunk));
        assert_eq!(MethodId::from_u16(99), None);
    }

    #[test]
    fn test_method_name_lookup() {
        assert_eq!(storage_service_meta::method_name(1), Some("read_chunk"));
        assert_eq!(storage_service_meta::method_name(2), Some("write_chunk"));
        assert_eq!(storage_service_meta::method_name(3), Some("remove_chunk"));
        assert_eq!(storage_service_meta::method_name(0), None);
    }
}
