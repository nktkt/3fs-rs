//! Storage service stub trait and mock implementation.

use async_trait::async_trait;
use hf3fs_proto::storage::{
    ReadChunkReq, ReadChunkRsp, RemoveChunkReq, RemoveChunkRsp, WriteChunkReq, WriteChunkRsp,
};
use hf3fs_types::Result;
use parking_lot::Mutex;
use std::sync::Arc;

/// Client-side stub for calling the storage service.
///
/// The storage service is responsible for reading, writing, and removing
/// individual chunks. Each chunk is identified by a `chunk_id` and is stored
/// on one or more storage nodes.
#[async_trait]
pub trait IStorageServiceStub: Send + Sync {
    async fn read_chunk(&self, req: ReadChunkReq) -> Result<ReadChunkRsp>;
    async fn write_chunk(&self, req: WriteChunkReq) -> Result<WriteChunkRsp>;
    async fn remove_chunk(&self, req: RemoveChunkReq) -> Result<RemoveChunkRsp>;
}

// ---------------------------------------------------------------------------
// Mock implementation
// ---------------------------------------------------------------------------

type Handler<Req, Rsp> = Box<dyn Fn(Req) -> Result<Rsp> + Send + Sync>;

/// A configurable mock for [`IStorageServiceStub`].
///
/// Each RPC method can be overridden with a closure. If no handler is
/// installed the mock returns a default (success) response.
pub struct MockStorageServiceStub {
    pub read_chunk_handler: Mutex<Option<Handler<ReadChunkReq, ReadChunkRsp>>>,
    pub write_chunk_handler: Mutex<Option<Handler<WriteChunkReq, WriteChunkRsp>>>,
    pub remove_chunk_handler: Mutex<Option<Handler<RemoveChunkReq, RemoveChunkRsp>>>,
}

impl MockStorageServiceStub {
    pub fn new() -> Self {
        Self {
            read_chunk_handler: Mutex::new(None),
            write_chunk_handler: Mutex::new(None),
            remove_chunk_handler: Mutex::new(None),
        }
    }

    /// Wrap in an `Arc` for convenient sharing.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub fn on_read_chunk(
        &self,
        f: impl Fn(ReadChunkReq) -> Result<ReadChunkRsp> + Send + Sync + 'static,
    ) {
        *self.read_chunk_handler.lock() = Some(Box::new(f));
    }

    pub fn on_write_chunk(
        &self,
        f: impl Fn(WriteChunkReq) -> Result<WriteChunkRsp> + Send + Sync + 'static,
    ) {
        *self.write_chunk_handler.lock() = Some(Box::new(f));
    }

    pub fn on_remove_chunk(
        &self,
        f: impl Fn(RemoveChunkReq) -> Result<RemoveChunkRsp> + Send + Sync + 'static,
    ) {
        *self.remove_chunk_handler.lock() = Some(Box::new(f));
    }
}

impl Default for MockStorageServiceStub {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IStorageServiceStub for MockStorageServiceStub {
    async fn read_chunk(&self, req: ReadChunkReq) -> Result<ReadChunkRsp> {
        let guard = self.read_chunk_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(ReadChunkRsp {
                data: Vec::new(),
            }),
        }
    }

    async fn write_chunk(&self, req: WriteChunkReq) -> Result<WriteChunkRsp> {
        let guard = self.write_chunk_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(WriteChunkRsp {
                bytes_written: req.data.len() as u32,
            }),
        }
    }

    async fn remove_chunk(&self, req: RemoveChunkReq) -> Result<RemoveChunkRsp> {
        let guard = self.remove_chunk_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(RemoveChunkRsp {}),
        }
    }
}

/// Blanket implementation: `Arc<T>` delegates to `T` for any `T: IStorageServiceStub`.
#[async_trait]
impl<T: IStorageServiceStub + ?Sized> IStorageServiceStub for Arc<T> {
    async fn read_chunk(&self, req: ReadChunkReq) -> Result<ReadChunkRsp> {
        (**self).read_chunk(req).await
    }
    async fn write_chunk(&self, req: WriteChunkReq) -> Result<WriteChunkRsp> {
        (**self).write_chunk(req).await
    }
    async fn remove_chunk(&self, req: RemoveChunkReq) -> Result<RemoveChunkRsp> {
        (**self).remove_chunk(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_read_chunk_default() {
        let mock = MockStorageServiceStub::new();
        let rsp = mock
            .read_chunk(ReadChunkReq {
                chunk_id: 1,
                chunk_offset: 0,
                length: 4096,
            })
            .await
            .unwrap();
        assert!(rsp.data.is_empty());
    }

    #[tokio::test]
    async fn test_mock_write_chunk_default() {
        let mock = MockStorageServiceStub::new();
        let data = vec![0xAB; 128];
        let rsp = mock
            .write_chunk(WriteChunkReq {
                chunk_id: 1,
                chunk_offset: 0,
                data: data.clone(),
            })
            .await
            .unwrap();
        assert_eq!(rsp.bytes_written, 128);
    }

    #[tokio::test]
    async fn test_mock_read_chunk_custom() {
        let mock = MockStorageServiceStub::new();
        mock.on_read_chunk(|req| {
            Ok(ReadChunkRsp {
                data: vec![0xFF; req.length as usize],
            })
        });
        let rsp = mock
            .read_chunk(ReadChunkReq {
                chunk_id: 42,
                chunk_offset: 0,
                length: 10,
            })
            .await
            .unwrap();
        assert_eq!(rsp.data.len(), 10);
        assert!(rsp.data.iter().all(|&b| b == 0xFF));
    }

    #[tokio::test]
    async fn test_mock_remove_chunk_default() {
        let mock = MockStorageServiceStub::new();
        let rsp = mock
            .remove_chunk(RemoveChunkReq { chunk_id: 99 })
            .await;
        assert!(rsp.is_ok());
    }

    #[tokio::test]
    async fn test_mock_via_arc() {
        let mock = MockStorageServiceStub::new().into_arc();
        let rsp = mock
            .read_chunk(ReadChunkReq {
                chunk_id: 1,
                chunk_offset: 0,
                length: 100,
            })
            .await;
        assert!(rsp.is_ok());
    }

    #[tokio::test]
    async fn test_mock_write_chunk_error() {
        use hf3fs_types::{Status, StatusCode};

        let mock = MockStorageServiceStub::new();
        mock.on_write_chunk(|_req| Err(Status::new(StatusCode::INVALID_ARG)));
        let rsp = mock
            .write_chunk(WriteChunkReq {
                chunk_id: 1,
                chunk_offset: 0,
                data: vec![1, 2, 3],
            })
            .await;
        assert!(rsp.is_err());
    }
}
