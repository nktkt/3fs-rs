//! Storage service stub trait and mock implementation.

use async_trait::async_trait;
use hf3fs_proto::storage::{
    ReadChunkReq, ReadChunkRsp, RemoveChunkReq, RemoveChunkRsp, WriteChunkReq, WriteChunkRsp,
};
use hf3fs_types::Result;
use parking_lot::Mutex;
use std::sync::Arc;

/// Client-side stub for calling the storage service.
#[async_trait]
pub trait IStorageServiceStub: Send + Sync {
    async fn read_chunk(&self, req: ReadChunkReq) -> Result<ReadChunkRsp>;
    async fn write_chunk(&self, req: WriteChunkReq) -> Result<WriteChunkRsp>;
    async fn remove_chunk(&self, req: RemoveChunkReq) -> Result<RemoveChunkRsp>;
}

type Handler<Req, Rsp> = Box<dyn Fn(Req) -> Result<Rsp> + Send + Sync>;

/// A configurable mock for [`IStorageServiceStub`].
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
            None => Ok(ReadChunkRsp::default()),
        }
    }

    async fn write_chunk(&self, req: WriteChunkReq) -> Result<WriteChunkRsp> {
        let guard = self.write_chunk_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(WriteChunkRsp::default()),
        }
    }

    async fn remove_chunk(&self, req: RemoveChunkReq) -> Result<RemoveChunkRsp> {
        let guard = self.remove_chunk_handler.lock();
        match guard.as_ref() {
            Some(f) => f(req),
            None => Ok(RemoveChunkRsp::default()),
        }
    }
}

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
        let rsp = mock.read_chunk(ReadChunkReq::default()).await.unwrap();
        assert!(rsp.data.is_empty());
    }

    #[tokio::test]
    async fn test_mock_write_chunk_default() {
        let mock = MockStorageServiceStub::new();
        let rsp = mock.write_chunk(WriteChunkReq::default()).await.unwrap();
        assert_eq!(rsp.bytes_written, 0);
    }

    #[tokio::test]
    async fn test_mock_remove_chunk_default() {
        let mock = MockStorageServiceStub::new();
        let rsp = mock.remove_chunk(RemoveChunkReq::default()).await;
        assert!(rsp.is_ok());
    }

    #[tokio::test]
    async fn test_mock_via_arc() {
        let mock = MockStorageServiceStub::new().into_arc();
        let rsp = mock.read_chunk(ReadChunkReq::default()).await;
        assert!(rsp.is_ok());
    }
}
