//! Storage service implementation.
//!
//! [`StorageServiceImpl`] implements [`IStorageService`] by delegating chunk
//! read/write/remove operations to a [`ChunkStore`].

use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, warn};

use hf3fs_proto::storage::{
    ReadChunkReq, ReadChunkRsp, RemoveChunkReq, RemoveChunkRsp, WriteChunkReq, WriteChunkRsp,
};
use hf3fs_types::Result;

use crate::chunk_store::ChunkStore;
use crate::IStorageService;

/// Concrete implementation of the storage service trait.
///
/// Holds a shared reference to a [`ChunkStore`] so that it can be cheaply
/// cloned and shared across async tasks.
#[derive(Debug, Clone)]
pub struct StorageServiceImpl {
    store: Arc<ChunkStore>,
}

impl StorageServiceImpl {
    /// Create a new service backed by the given chunk store.
    pub fn new(store: Arc<ChunkStore>) -> Self {
        Self { store }
    }

    /// Create a new service with a default in-memory chunk store.
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(ChunkStore::new()),
        }
    }

    /// Return a reference to the underlying chunk store.
    pub fn store(&self) -> &ChunkStore {
        &self.store
    }
}

#[async_trait]
impl IStorageService for StorageServiceImpl {
    async fn read_chunk(&self, req: ReadChunkReq) -> Result<ReadChunkRsp> {
        debug!(
            chunk_id = req.chunk_id,
            offset = req.chunk_offset,
            length = req.length,
            "read_chunk"
        );

        let data = self.store.read(req.chunk_id, req.chunk_offset, req.length)?;
        Ok(ReadChunkRsp { data })
    }

    async fn write_chunk(&self, req: WriteChunkReq) -> Result<WriteChunkRsp> {
        debug!(
            chunk_id = req.chunk_id,
            offset = req.chunk_offset,
            data_len = req.data.len(),
            "write_chunk"
        );

        let bytes_written = self.store.write(req.chunk_id, req.chunk_offset, &req.data)?;
        Ok(WriteChunkRsp { bytes_written })
    }

    async fn remove_chunk(&self, req: RemoveChunkReq) -> Result<RemoveChunkRsp> {
        debug!(chunk_id = req.chunk_id, "remove_chunk");

        match self.store.remove(req.chunk_id) {
            Ok(()) => Ok(RemoveChunkRsp {}),
            Err(e) => {
                warn!(chunk_id = req.chunk_id, error = %e, "remove_chunk failed");
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_service() -> StorageServiceImpl {
        StorageServiceImpl::in_memory()
    }

    #[tokio::test]
    async fn test_write_and_read_chunk() {
        let svc = make_service();
        let payload = vec![1u8, 2, 3, 4, 5];

        let write_rsp = svc
            .write_chunk(WriteChunkReq {
                chunk_id: 10,
                chunk_offset: 0,
                data: payload.clone(),
            })
            .await
            .unwrap();
        assert_eq!(write_rsp.bytes_written, 5);

        let read_rsp = svc
            .read_chunk(ReadChunkReq {
                chunk_id: 10,
                chunk_offset: 0,
                length: 5,
            })
            .await
            .unwrap();
        assert_eq!(read_rsp.data, payload);
    }

    #[tokio::test]
    async fn test_read_nonexistent_chunk() {
        let svc = make_service();
        let result = svc
            .read_chunk(ReadChunkReq {
                chunk_id: 999,
                chunk_offset: 0,
                length: 10,
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_at_offset_and_read_full() {
        let svc = make_service();

        // Write "aaaa" at offset 0.
        svc.write_chunk(WriteChunkReq {
            chunk_id: 1,
            chunk_offset: 0,
            data: vec![0xAA; 4],
        })
        .await
        .unwrap();

        // Write "bbbb" at offset 4.
        svc.write_chunk(WriteChunkReq {
            chunk_id: 1,
            chunk_offset: 4,
            data: vec![0xBB; 4],
        })
        .await
        .unwrap();

        let rsp = svc
            .read_chunk(ReadChunkReq {
                chunk_id: 1,
                chunk_offset: 0,
                length: 8,
            })
            .await
            .unwrap();
        assert_eq!(rsp.data, vec![0xAA, 0xAA, 0xAA, 0xAA, 0xBB, 0xBB, 0xBB, 0xBB]);
    }

    #[tokio::test]
    async fn test_remove_chunk() {
        let svc = make_service();

        svc.write_chunk(WriteChunkReq {
            chunk_id: 42,
            chunk_offset: 0,
            data: vec![1, 2, 3],
        })
        .await
        .unwrap();

        let remove_rsp = svc
            .remove_chunk(RemoveChunkReq { chunk_id: 42 })
            .await;
        assert!(remove_rsp.is_ok());

        // Reading after removal should fail.
        let read_result = svc
            .read_chunk(ReadChunkReq {
                chunk_id: 42,
                chunk_offset: 0,
                length: 3,
            })
            .await;
        assert!(read_result.is_err());
    }

    #[tokio::test]
    async fn test_remove_nonexistent_chunk() {
        let svc = make_service();
        let result = svc.remove_chunk(RemoveChunkReq { chunk_id: 777 }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_partial_read() {
        let svc = make_service();

        svc.write_chunk(WriteChunkReq {
            chunk_id: 1,
            chunk_offset: 0,
            data: vec![10, 20, 30, 40, 50],
        })
        .await
        .unwrap();

        // Read 2 bytes starting at offset 2.
        let rsp = svc
            .read_chunk(ReadChunkReq {
                chunk_id: 1,
                chunk_offset: 2,
                length: 2,
            })
            .await
            .unwrap();
        assert_eq!(rsp.data, vec![30, 40]);
    }

    #[tokio::test]
    async fn test_store_accessor() {
        let svc = make_service();
        assert!(svc.store().is_empty());

        svc.write_chunk(WriteChunkReq {
            chunk_id: 1,
            chunk_offset: 0,
            data: vec![0],
        })
        .await
        .unwrap();
        assert_eq!(svc.store().len(), 1);
    }
}
