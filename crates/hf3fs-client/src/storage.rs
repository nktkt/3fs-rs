//! Storage service client.
//!
//! Mirrors C++ `storage::client::StorageClient`. Provides chunk-level I/O
//! operations: read, write, remove, truncate, and query.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use hf3fs_proto::storage::{ReadChunkReq, WriteChunkReq};
use hf3fs_types::{ChainId, ChunkId, NodeId};
use parking_lot::Mutex;
use tracing;

use crate::config::StorageClientConfig;
use crate::error::{ClientError, ClientResult};
use crate::routing::RoutingInfoHandle;

// ---------------------------------------------------------------------------
// I/O types
// ---------------------------------------------------------------------------

/// Result of a single read I/O.
#[derive(Debug, Clone)]
pub struct ReadResult {
    /// The data that was read.
    pub data: Vec<u8>,
    /// Actual number of bytes read (may be less than requested).
    pub bytes_read: u32,
}

/// Result of a single write I/O.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Number of bytes written.
    pub bytes_written: u32,
}

/// Result of a query for the last chunk in a range.
#[derive(Debug, Clone)]
pub struct QueryLastChunkResult {
    /// Total length of all chunks in the queried range.
    pub total_chunk_len: u64,
    /// Total number of chunks in the queried range.
    pub total_num_chunks: u32,
    /// Whether there are more chunks beyond the scan limit.
    pub more_chunks_in_range: bool,
}

/// Result of a chunk removal operation.
#[derive(Debug, Clone)]
pub struct RemoveChunksResult {
    /// Number of chunks actually removed.
    pub num_chunks_removed: u32,
    /// Whether there are more chunks to remove.
    pub more_chunks_in_range: bool,
}

/// Result of a chunk truncation operation.
#[derive(Debug, Clone)]
pub struct TruncateChunkResult {
    /// Resulting length of the chunk after truncation.
    pub chunk_len: u32,
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Trait for storage operations.
///
/// Each method corresponds to a C++ `StorageClient` virtual method.
#[async_trait]
pub trait StorageClient: Send + Sync {
    /// Start the storage client (allocate resources, start background tasks).
    async fn start(&self) -> ClientResult<()>;

    /// Stop the storage client.
    async fn stop(&self) -> ClientResult<()>;

    /// Read data from a chunk.
    async fn read_chunk(
        &self,
        chain_id: ChainId,
        chunk_id: ChunkId,
        offset: u32,
        length: u32,
    ) -> ClientResult<ReadResult>;

    /// Write data to a chunk.
    async fn write_chunk(
        &self,
        chain_id: ChainId,
        chunk_id: ChunkId,
        offset: u32,
        chunk_size: u32,
        data: &[u8],
    ) -> ClientResult<WriteResult>;

    /// Read a batch of chunks.
    async fn batch_read(
        &self,
        ops: Vec<(ChainId, ChunkId, u32, u32)>,
    ) -> ClientResult<Vec<ClientResult<ReadResult>>>;

    /// Write a batch of chunks.
    async fn batch_write(
        &self,
        ops: Vec<(ChainId, ChunkId, u32, u32, Vec<u8>)>,
    ) -> ClientResult<Vec<ClientResult<WriteResult>>>;

    /// Query the last chunk in a range.
    async fn query_last_chunk(
        &self,
        chain_id: ChainId,
        begin: ChunkId,
        end: ChunkId,
        max_scan: u32,
    ) -> ClientResult<QueryLastChunkResult>;

    /// Remove chunks in a range.
    async fn remove_chunks(
        &self,
        chain_id: ChainId,
        begin: ChunkId,
        end: ChunkId,
        max_scan: u32,
    ) -> ClientResult<RemoveChunksResult>;

    /// Truncate a chunk to a given length.
    async fn truncate_chunk(
        &self,
        chain_id: ChainId,
        chunk_id: ChunkId,
        chunk_len: u32,
        chunk_size: u32,
        only_extend: bool,
    ) -> ClientResult<TruncateChunkResult>;

    /// Generate a new unique request ID.
    fn next_request_id(&self) -> u64;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

/// Concrete implementation of `StorageClient`.
pub struct StorageClientImpl {
    config: StorageClientConfig,
    routing: RoutingInfoHandle,
    running: Mutex<bool>,
    next_req_id: AtomicU64,
}

impl StorageClientImpl {
    /// Create a new storage client.
    pub fn new(config: StorageClientConfig, routing: RoutingInfoHandle) -> Self {
        Self {
            config,
            routing,
            running: Mutex::new(false),
            next_req_id: AtomicU64::new(1),
        }
    }

    /// Resolve the chain to a target node address for I/O.
    fn resolve_chain(&self, chain_id: ChainId) -> ClientResult<NodeId> {
        let ri = self.routing.get();
        let chain = ri
            .get_chain(chain_id)
            .ok_or_else(|| ClientError::Internal(format!("chain {} not found", *chain_id)))?;
        if chain.targets.is_empty() {
            return Err(ClientError::Internal(format!(
                "chain {} has no targets",
                *chain_id
            )));
        }
        let target_id = chain.targets[0]; // Primary target.
        let target = ri.get_target(target_id).ok_or_else(|| {
            ClientError::Internal(format!("target {} not found", *target_id))
        })?;
        Ok(target.node_id)
    }
}

#[async_trait]
impl StorageClient for StorageClientImpl {
    async fn start(&self) -> ClientResult<()> {
        let mut running = self.running.lock();
        if *running {
            return Ok(());
        }
        *running = true;
        tracing::info!("StorageClient started");
        Ok(())
    }

    async fn stop(&self) -> ClientResult<()> {
        let mut running = self.running.lock();
        if !*running {
            return Ok(());
        }
        *running = false;
        tracing::info!("StorageClient stopped");
        Ok(())
    }

    async fn read_chunk(
        &self,
        chain_id: ChainId,
        chunk_id: ChunkId,
        offset: u32,
        length: u32,
    ) -> ClientResult<ReadResult> {
        let _node = self.resolve_chain(chain_id)?;
        let _req = ReadChunkReq {
            chunk_id: *chunk_id,
            chunk_offset: offset as u64,
            length,
        };
        // Placeholder: would send RPC to the resolved node.
        Err(ClientError::Internal("not connected to live server".into()))
    }

    async fn write_chunk(
        &self,
        chain_id: ChainId,
        chunk_id: ChunkId,
        offset: u32,
        _chunk_size: u32,
        data: &[u8],
    ) -> ClientResult<WriteResult> {
        let _node = self.resolve_chain(chain_id)?;
        let _req = WriteChunkReq {
            chunk_id: *chunk_id,
            chunk_offset: offset as u64,
            data: data.to_vec(),
        };
        Err(ClientError::Internal("not connected to live server".into()))
    }

    async fn batch_read(
        &self,
        ops: Vec<(ChainId, ChunkId, u32, u32)>,
    ) -> ClientResult<Vec<ClientResult<ReadResult>>> {
        let mut results = Vec::with_capacity(ops.len());
        for (chain_id, chunk_id, offset, length) in ops {
            results.push(self.read_chunk(chain_id, chunk_id, offset, length).await);
        }
        Ok(results)
    }

    async fn batch_write(
        &self,
        ops: Vec<(ChainId, ChunkId, u32, u32, Vec<u8>)>,
    ) -> ClientResult<Vec<ClientResult<WriteResult>>> {
        let mut results = Vec::with_capacity(ops.len());
        for (chain_id, chunk_id, offset, chunk_size, data) in ops {
            results.push(
                self.write_chunk(chain_id, chunk_id, offset, chunk_size, &data)
                    .await,
            );
        }
        Ok(results)
    }

    async fn query_last_chunk(
        &self,
        chain_id: ChainId,
        _begin: ChunkId,
        _end: ChunkId,
        _max_scan: u32,
    ) -> ClientResult<QueryLastChunkResult> {
        let _node = self.resolve_chain(chain_id)?;
        Err(ClientError::Internal("not connected to live server".into()))
    }

    async fn remove_chunks(
        &self,
        chain_id: ChainId,
        _begin: ChunkId,
        _end: ChunkId,
        _max_scan: u32,
    ) -> ClientResult<RemoveChunksResult> {
        let _node = self.resolve_chain(chain_id)?;
        Err(ClientError::Internal("not connected to live server".into()))
    }

    async fn truncate_chunk(
        &self,
        chain_id: ChainId,
        _chunk_id: ChunkId,
        _chunk_len: u32,
        _chunk_size: u32,
        _only_extend: bool,
    ) -> ClientResult<TruncateChunkResult> {
        let _node = self.resolve_chain(chain_id)?;
        Err(ClientError::Internal("not connected to live server".into()))
    }

    fn next_request_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::{ChainInfo, NodeInfo, RoutingInfo, RoutingInfoHandle, TargetInfo};
    use hf3fs_types::{Address, AddressType, TargetId};

    fn setup_routing() -> RoutingInfoHandle {
        let handle = RoutingInfoHandle::new();
        let mut ri = RoutingInfo::empty();
        ri.nodes.insert(
            NodeId(1),
            NodeInfo {
                node_id: NodeId(1),
                address: Address::from_octets(10, 0, 0, 1, 9000, AddressType::TCP),
                hostname: "storage-1".into(),
                is_healthy: true,
            },
        );
        ri.targets.insert(
            TargetId(10),
            TargetInfo {
                target_id: TargetId(10),
                node_id: NodeId(1),
            },
        );
        ri.chains.insert(
            ChainId(100),
            ChainInfo {
                chain_id: ChainId(100),
                targets: vec![TargetId(10)],
            },
        );
        handle.update(ri);
        handle
    }

    #[tokio::test]
    async fn test_storage_client_start_stop() {
        let routing = setup_routing();
        let client = StorageClientImpl::new(StorageClientConfig::default(), routing);
        client.start().await.unwrap();
        client.start().await.unwrap(); // idempotent
        client.stop().await.unwrap();
        client.stop().await.unwrap(); // idempotent
    }

    #[test]
    fn test_next_request_id() {
        let routing = RoutingInfoHandle::new();
        let client = StorageClientImpl::new(StorageClientConfig::default(), routing);
        let id1 = client.next_request_id();
        let id2 = client.next_request_id();
        assert_eq!(id2, id1 + 1);
    }

    #[test]
    fn test_resolve_chain_success() {
        let routing = setup_routing();
        let client = StorageClientImpl::new(StorageClientConfig::default(), routing);
        let node = client.resolve_chain(ChainId(100)).unwrap();
        assert_eq!(node, NodeId(1));
    }

    #[test]
    fn test_resolve_chain_not_found() {
        let routing = RoutingInfoHandle::new();
        let client = StorageClientImpl::new(StorageClientConfig::default(), routing);
        assert!(client.resolve_chain(ChainId(999)).is_err());
    }

    #[tokio::test]
    async fn test_read_chunk_no_server() {
        let routing = setup_routing();
        let client = StorageClientImpl::new(StorageClientConfig::default(), routing);
        let result = client
            .read_chunk(ChainId(100), ChunkId(1), 0, 4096)
            .await;
        // Expected to fail since we are not connected to a real server.
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_chunk_no_server() {
        let routing = setup_routing();
        let client = StorageClientImpl::new(StorageClientConfig::default(), routing);
        let data = vec![0u8; 128];
        let result = client
            .write_chunk(ChainId(100), ChunkId(1), 0, 4096, &data)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_batch_read() {
        let routing = setup_routing();
        let client = StorageClientImpl::new(StorageClientConfig::default(), routing);
        let ops = vec![
            (ChainId(100), ChunkId(1), 0u32, 4096u32),
            (ChainId(100), ChunkId(2), 0, 4096),
        ];
        let results = client.batch_read(ops).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_err());
        assert!(results[1].is_err());
    }

    #[tokio::test]
    async fn test_batch_write() {
        let routing = setup_routing();
        let client = StorageClientImpl::new(StorageClientConfig::default(), routing);
        let ops = vec![
            (ChainId(100), ChunkId(1), 0u32, 4096u32, vec![1u8; 64]),
            (ChainId(100), ChunkId(2), 0, 4096, vec![2u8; 64]),
        ];
        let results = client.batch_write(ops).await.unwrap();
        assert_eq!(results.len(), 2);
    }
}
