//! Storage service RPC types.
//!
//! Based on 3FS/src/fbs/storage/Common.h and Service.h
//!
//! Service methods (from C++ StorageSerde):
//!   1  - batchRead(BatchReadReq, BatchReadRsp)
//!   2  - write(WriteReq, WriteRsp)
//!   3  - update(UpdateReq, UpdateRsp)
//!   5  - queryLastChunk(QueryLastChunkReq, QueryLastChunkRsp)
//!   6  - truncateChunks(TruncateChunksReq, TruncateChunksRsp)
//!   7  - removeChunks(RemoveChunksReq, RemoveChunksRsp)
//!   8  - syncStart(SyncStartReq, TargetSyncInfo)
//!   9  - syncDone(SyncDoneReq, SyncDoneRsp)
//!   10 - spaceInfo(SpaceInfoReq, SpaceInfoRsp)
//!   11 - createTarget(CreateTargetReq, CreateTargetRsp)
//!   12 - queryChunk(QueryChunkReq, QueryChunkRsp)
//!   13 - getAllChunkMetadata(GetAllChunkMetadataReq, GetAllChunkMetadataRsp)
//!   16 - offlineTarget(OfflineTargetReq, OfflineTargetRsp)
//!   17 - removeTarget(RemoveTargetReq, RemoveTargetRsp)

use hf3fs_serde::{WireDeserialize, WireSerialize};
use serde::{Deserialize, Serialize};

use crate::common::UserInfo;

// ---------------------------------------------------------------------------
// Storage-specific enums
// ---------------------------------------------------------------------------

/// Type of update operation on a chunk.
///
/// Mirrors `storage::UpdateType` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum UpdateType {
    Invalid = 0,
    Write = 1,
    Remove = 2,
    Truncate = 4,
    Extend = 8,
    Commit = 16,
}

impl Default for UpdateType {
    fn default() -> Self {
        Self::Invalid
    }
}

impl TryFrom<u8> for UpdateType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Invalid),
            1 => Ok(Self::Write),
            2 => Ok(Self::Remove),
            4 => Ok(Self::Truncate),
            8 => Ok(Self::Extend),
            16 => Ok(Self::Commit),
            _ => Err(()),
        }
    }
}

impl WireSerialize for UpdateType {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for UpdateType {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "UpdateType",
            value: v as u64,
        })
    }
}

/// State of a chunk on disk.
///
/// Mirrors `storage::ChunkState` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ChunkState {
    Commit = 0,
    Dirty = 1,
    Clean = 2,
}

impl Default for ChunkState {
    fn default() -> Self {
        Self::Commit
    }
}

impl TryFrom<u8> for ChunkState {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::Commit),
            1 => Ok(Self::Dirty),
            2 => Ok(Self::Clean),
            _ => Err(()),
        }
    }
}

impl WireSerialize for ChunkState {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for ChunkState {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "ChunkState",
            value: v as u64,
        })
    }
}

/// Checksum type for chunk data.
///
/// Mirrors `storage::ChecksumType` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ChecksumType {
    None = 0,
    Crc32c = 1,
    Crc32 = 2,
}

impl Default for ChecksumType {
    fn default() -> Self {
        Self::None
    }
}

impl TryFrom<u8> for ChecksumType {
    type Error = ();
    fn try_from(v: u8) -> Result<Self, ()> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Crc32c),
            2 => Ok(Self::Crc32),
            _ => Err(()),
        }
    }
}

impl WireSerialize for ChecksumType {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        (*self as u8).wire_serialize(buf)
    }
}

impl WireDeserialize for ChecksumType {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Self::try_from(v).map_err(|_| hf3fs_serde::WireError::InvalidEnumVariant {
            enum_name: "ChecksumType",
            value: v as u64,
        })
    }
}

/// Feature flags for storage requests.
///
/// Mirrors `storage::FeatureFlags` in C++.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FeatureFlags(pub u32);

impl FeatureFlags {
    pub const DEFAULT: Self = Self(0);
    pub const BYPASS_DISKIO: Self = Self(1);
    pub const BYPASS_RDMAXMIT: Self = Self(2);
    pub const SEND_DATA_INLINE: Self = Self(4);
    pub const ALLOW_READ_UNCOMMITTED: Self = Self(8);

    pub fn contains(&self, bits: u32) -> bool {
        (self.0 & bits) == bits
    }
}

impl WireSerialize for FeatureFlags {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), hf3fs_serde::WireError> {
        self.0.wire_serialize(buf)
    }
}

impl WireDeserialize for FeatureFlags {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, hf3fs_serde::WireError> {
        Ok(Self(u32::wire_deserialize(buf, offset)?))
    }
}

// ---------------------------------------------------------------------------
// Common sub-types
// ---------------------------------------------------------------------------

/// Checksum information for a chunk.
///
/// Mirrors `storage::ChecksumInfo` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct ChecksumInfo {
    pub checksum_type: u8,
    pub value: u32,
}

/// Versioned chain ID.
///
/// Mirrors `storage::VersionedChainId` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct VersionedChainId {
    pub chain_id: u32,
    pub chain_ver: u32,
}

/// Global key identifying a chunk across the cluster.
///
/// Mirrors `storage::GlobalKey` in C++.
/// ChunkId is serialized as a string (variable-length bytes).
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GlobalKey {
    pub v_chain_id: VersionedChainId,
    pub chunk_id: String,
}

/// Update channel for write ordering.
///
/// Mirrors `storage::UpdateChannel` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct UpdateChannel {
    pub id: u16,
    pub seqnum: u64,
}

/// Message tag identifying a client request.
///
/// Mirrors `storage::MessageTag` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct MessageTag {
    pub client_id: u64,
    pub request_id: u64,
    pub channel: UpdateChannel,
}

/// Debug flags for fault injection.
///
/// Mirrors `storage::DebugFlags` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct DebugFlags {
    pub inject_random_server_error: bool,
    pub inject_random_client_error: bool,
    pub num_of_inject_pts_before_fail: u16,
}

/// IO result from a storage operation.
///
/// Mirrors `storage::IOResult` in C++.
/// Note: In C++ this contains a `Result<uint32_t>` for length_info;
/// in Rust we represent it as a status code + optional length.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct IoResult {
    pub status_code: u32,
    pub length: u32,
    pub commit_ver: u32,
    pub update_ver: u32,
    pub checksum: ChecksumInfo,
    pub commit_chain_ver: u32,
}

/// RDMA remote buffer descriptor.
///
/// Simplified representation of `net::RDMARemoteBuf` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct RdmaRemoteBuf {
    pub addr: u64,
    pub length: u32,
    pub rkey: u32,
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

/// A single read IO operation within a batch read.
///
/// Mirrors `storage::ReadIO` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ReadIo {
    pub offset: u32,
    pub length: u32,
    pub key: GlobalKey,
    pub rdmabuf: RdmaRemoteBuf,
}

/// Batch read request.
///
/// Mirrors `storage::BatchReadReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct BatchReadReq {
    pub payloads: Vec<ReadIo>,
    pub tag: MessageTag,
    pub retry_count: u32,
    pub user_info: UserInfo,
    pub feature_flags: u32,
    pub checksum_type: u8,
    pub debug_flags: DebugFlags,
}

/// Batch read response.
///
/// Mirrors `storage::BatchReadRsp` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct BatchReadRsp {
    pub tag: MessageTag,
    pub results: Vec<IoResult>,
    pub inline_buf: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

/// A single update IO operation.
///
/// Mirrors `storage::UpdateIO` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct UpdateIo {
    pub offset: u32,
    pub length: u32,
    pub chunk_size: u32,
    pub key: GlobalKey,
    pub rdmabuf: RdmaRemoteBuf,
    pub update_ver: u32,
    pub update_type: u8,
    pub checksum: ChecksumInfo,
    pub inline_buf: Vec<u8>,
}

/// Write request (client -> head of chain).
///
/// Mirrors `storage::WriteReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct WriteReq {
    pub payload: UpdateIo,
    pub tag: MessageTag,
    pub retry_count: u32,
    pub user_info: UserInfo,
    pub feature_flags: u32,
    pub debug_flags: DebugFlags,
}

/// Write response.
///
/// Mirrors `storage::WriteRsp` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct WriteRsp {
    pub tag: MessageTag,
    pub result: IoResult,
}

// ---------------------------------------------------------------------------
// Update operations (chain replication forwarding)
// ---------------------------------------------------------------------------

/// Options for an update operation.
///
/// Mirrors `storage::UpdateOptions` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct UpdateOptions {
    pub is_syncing: bool,
    pub from_client: bool,
    pub commit_chain_ver: u32,
}

/// Update request (chain forwarding).
///
/// Mirrors `storage::UpdateReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct UpdateReq {
    pub payload: UpdateIo,
    pub options: UpdateOptions,
    pub tag: MessageTag,
    pub retry_count: u32,
    pub user_info: UserInfo,
    pub feature_flags: u32,
    pub debug_flags: DebugFlags,
}

/// Update response.
///
/// Mirrors `storage::UpdateRsp` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct UpdateRsp {
    pub tag: MessageTag,
    pub result: IoResult,
}

// ---------------------------------------------------------------------------
// Query last chunk
// ---------------------------------------------------------------------------

/// Range of chunk IDs for query/removal.
///
/// Mirrors `storage::ChunkIdRange` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ChunkIdRange {
    pub begin: String,
    pub end: String,
    pub max_num_chunk_ids_to_process: u32,
}

/// A single query-last-chunk operation.
///
/// Mirrors `storage::QueryLastChunkOp` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct QueryLastChunkOp {
    pub v_chain_id: VersionedChainId,
    pub chunk_id_range: ChunkIdRange,
}

/// Request to query the last chunk in a range.
///
/// Mirrors `storage::QueryLastChunkReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct QueryLastChunkReq {
    pub payloads: Vec<QueryLastChunkOp>,
    pub tag: MessageTag,
    pub retry_count: u32,
    pub user_info: UserInfo,
    pub feature_flags: u32,
    pub debug_flags: DebugFlags,
}

/// Result of a single query-last-chunk operation.
///
/// Mirrors `storage::QueryLastChunkResult` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct QueryLastChunkResult {
    pub status_code: u32,
    pub last_chunk_id: String,
    pub last_chunk_len: u32,
    pub total_chunk_len: u64,
    pub total_num_chunks: u64,
    pub more_chunks_in_range: bool,
}

/// Response to a query-last-chunk request.
///
/// Mirrors `storage::QueryLastChunkRsp` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct QueryLastChunkRsp {
    pub results: Vec<QueryLastChunkResult>,
}

// ---------------------------------------------------------------------------
// Remove chunks
// ---------------------------------------------------------------------------

/// A single remove-chunks operation.
///
/// Mirrors `storage::RemoveChunksOp` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RemoveChunksOp {
    pub v_chain_id: VersionedChainId,
    pub chunk_id_range: ChunkIdRange,
    pub tag: MessageTag,
    pub retry_count: u32,
}

/// Request to remove chunks.
///
/// Mirrors `storage::RemoveChunksReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RemoveChunksReq {
    pub payloads: Vec<RemoveChunksOp>,
    pub user_info: UserInfo,
    pub feature_flags: u32,
    pub debug_flags: DebugFlags,
}

/// Result of a single remove-chunks operation.
///
/// Mirrors `storage::RemoveChunksResult` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct RemoveChunksResult {
    pub status_code: u32,
    pub num_chunks_removed: u32,
    pub more_chunks_in_range: bool,
}

/// Response to a remove-chunks request.
///
/// Mirrors `storage::RemoveChunksRsp` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RemoveChunksRsp {
    pub results: Vec<RemoveChunksResult>,
}

// ---------------------------------------------------------------------------
// Truncate chunks
// ---------------------------------------------------------------------------

/// A single truncate-chunk operation.
///
/// Mirrors `storage::TruncateChunkOp` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct TruncateChunkOp {
    pub v_chain_id: VersionedChainId,
    pub chunk_id: String,
    pub chunk_len: u32,
    pub chunk_size: u32,
    pub only_extend_chunk: bool,
    pub tag: MessageTag,
    pub retry_count: u32,
}

/// Request to truncate chunks.
///
/// Mirrors `storage::TruncateChunksReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct TruncateChunksReq {
    pub payloads: Vec<TruncateChunkOp>,
    pub user_info: UserInfo,
    pub feature_flags: u32,
    pub debug_flags: DebugFlags,
}

/// Response to a truncate-chunks request.
///
/// Mirrors `storage::TruncateChunksRsp` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct TruncateChunksRsp {
    pub results: Vec<IoResult>,
}

// ---------------------------------------------------------------------------
// Sync
// ---------------------------------------------------------------------------

/// Request to start syncing a target.
///
/// Mirrors `storage::SyncStartReq` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct SyncStartReq {
    pub v_chain_id: VersionedChainId,
}

/// Chunk metadata for sync.
///
/// Mirrors `storage::ChunkMeta` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ChunkMeta {
    pub chunk_id: String,
    pub update_ver: u32,
    pub commit_ver: u32,
    pub chain_ver: u32,
    pub chunk_state: u8,
    pub checksum: ChecksumInfo,
    pub length: u32,
}

/// Sync info for a target (response to SyncStartReq).
///
/// Mirrors `storage::TargetSyncInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct TargetSyncInfo {
    pub metas: Vec<ChunkMeta>,
}

/// Request to finish syncing.
///
/// Mirrors `storage::SyncDoneReq` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct SyncDoneReq {
    pub v_chain_id: VersionedChainId,
}

/// Response to sync done.
///
/// Mirrors `storage::SyncDoneRsp` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct SyncDoneRsp {
    pub result: IoResult,
}

// ---------------------------------------------------------------------------
// Space info
// ---------------------------------------------------------------------------

/// Request for space information.
///
/// Mirrors `storage::SpaceInfoReq` in C++.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    Serialize,
    Deserialize,
    WireSerialize,
    WireDeserialize,
)]
pub struct SpaceInfoReq {
    pub tag: MessageTag,
    pub force: bool,
}

/// Information about disk space.
///
/// Mirrors `storage::SpaceInfo` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SpaceInfo {
    pub path: String,
    pub capacity: u64,
    pub free: u64,
    pub available: u64,
    pub target_ids: Vec<u64>,
    pub manufacturer: String,
}

/// Response with disk space information.
///
/// Mirrors `storage::SpaceInfoRsp` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct SpaceInfoRsp {
    pub space_infos: Vec<SpaceInfo>,
}

// ---------------------------------------------------------------------------
// Target management
// ---------------------------------------------------------------------------

/// Request to create a storage target.
///
/// Mirrors `storage::CreateTargetReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct CreateTargetReq {
    pub target_id: u64,
    pub disk_index: u32,
    pub physical_file_count: u32,
    pub chunk_size_list: Vec<u32>,
    pub allow_existing_target: bool,
    pub chain_id: u32,
    pub add_chunk_size: bool,
    pub only_chunk_engine: bool,
}

/// Response to create-target request.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct CreateTargetRsp {}

/// Request to offline a target.
///
/// Mirrors `storage::OfflineTargetReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct OfflineTargetReq {
    pub target_id: u64,
    pub force: bool,
}

/// Response to offline-target request.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct OfflineTargetRsp {}

/// Request to remove a target.
///
/// Mirrors `storage::RemoveTargetReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RemoveTargetReq {
    pub target_id: u64,
    pub force: bool,
}

/// Response to remove-target request.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RemoveTargetRsp {}

// ---------------------------------------------------------------------------
// Query chunk metadata
// ---------------------------------------------------------------------------

/// Request to query a specific chunk.
///
/// Mirrors `storage::QueryChunkReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct QueryChunkReq {
    pub chain_id: u32,
    pub chunk_id: String,
}

/// Response to a chunk query. Contains chunk metadata.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct QueryChunkRsp {
    pub status_code: u32,
    pub commit_ver: u32,
    pub update_ver: u32,
    pub chain_ver: u32,
    pub chunk_state: u8,
    pub size: u32,
    pub checksum_type: u8,
    pub checksum_value: u32,
}

/// Request to get all chunk metadata for a target.
///
/// Mirrors `storage::GetAllChunkMetadataReq` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetAllChunkMetadataReq {
    pub target_id: u64,
}

/// Response with all chunk metadata.
///
/// Mirrors `storage::GetAllChunkMetadataRsp` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct GetAllChunkMetadataRsp {
    pub chunk_meta_vec: Vec<ChunkMeta>,
}

// ---------------------------------------------------------------------------
// Commit IO
// ---------------------------------------------------------------------------

/// A commit IO operation.
///
/// Mirrors `storage::CommitIO` in C++.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct CommitIo {
    pub key: GlobalKey,
    pub commit_ver: u32,
    pub is_syncing: bool,
    pub commit_chain_ver: u32,
    pub is_remove: bool,
    pub is_force: bool,
}

// ---------------------------------------------------------------------------
// Compatibility aliases used by hf3fs-storage-service
// ---------------------------------------------------------------------------

/// Simplified read-chunk request used by `hf3fs-storage-service`.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ReadChunkReq {
    pub chunk_id: u64,
    pub chunk_offset: u64,
    pub length: u32,
}

/// Simplified read-chunk response.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct ReadChunkRsp {
    pub data: Vec<u8>,
}

/// Simplified write-chunk request used by `hf3fs-storage-service`.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct WriteChunkReq {
    pub chunk_id: u64,
    pub chunk_offset: u64,
    pub data: Vec<u8>,
}

/// Simplified write-chunk response.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct WriteChunkRsp {
    pub bytes_written: u32,
}

/// Simplified remove-chunk request.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RemoveChunkReq {
    pub chunk_id: u64,
}

/// Simplified remove-chunk response.
#[derive(
    Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize,
)]
pub struct RemoveChunkRsp {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_serde::{WireDeserialize, WireSerialize};

    fn roundtrip<T: WireSerialize + WireDeserialize + std::fmt::Debug + PartialEq>(val: &T) -> T {
        let mut buf = Vec::new();
        val.wire_serialize(&mut buf).unwrap();
        let mut offset = 0;
        let result = T::wire_deserialize(&buf, &mut offset).unwrap();
        assert_eq!(offset, buf.len());
        result
    }

    #[test]
    fn test_update_type_roundtrip() {
        assert_eq!(roundtrip(&UpdateType::Invalid), UpdateType::Invalid);
        assert_eq!(roundtrip(&UpdateType::Write), UpdateType::Write);
        assert_eq!(roundtrip(&UpdateType::Remove), UpdateType::Remove);
        assert_eq!(roundtrip(&UpdateType::Truncate), UpdateType::Truncate);
        assert_eq!(roundtrip(&UpdateType::Extend), UpdateType::Extend);
        assert_eq!(roundtrip(&UpdateType::Commit), UpdateType::Commit);
    }

    #[test]
    fn test_chunk_state_roundtrip() {
        assert_eq!(roundtrip(&ChunkState::Commit), ChunkState::Commit);
        assert_eq!(roundtrip(&ChunkState::Dirty), ChunkState::Dirty);
        assert_eq!(roundtrip(&ChunkState::Clean), ChunkState::Clean);
    }

    #[test]
    fn test_checksum_type_roundtrip() {
        assert_eq!(roundtrip(&ChecksumType::None), ChecksumType::None);
        assert_eq!(roundtrip(&ChecksumType::Crc32c), ChecksumType::Crc32c);
        assert_eq!(roundtrip(&ChecksumType::Crc32), ChecksumType::Crc32);
    }

    #[test]
    fn test_checksum_info_roundtrip() {
        let ci = ChecksumInfo {
            checksum_type: ChecksumType::Crc32c as u8,
            value: 0xDEADBEEF,
        };
        assert_eq!(roundtrip(&ci), ci);
    }

    #[test]
    fn test_versioned_chain_id_roundtrip() {
        let vci = VersionedChainId {
            chain_id: 10,
            chain_ver: 3,
        };
        assert_eq!(roundtrip(&vci), vci);
    }

    #[test]
    fn test_global_key_roundtrip() {
        let gk = GlobalKey {
            v_chain_id: VersionedChainId {
                chain_id: 1,
                chain_ver: 2,
            },
            chunk_id: "chunk-001".to_string(),
        };
        assert_eq!(roundtrip(&gk), gk);
    }

    #[test]
    fn test_message_tag_roundtrip() {
        let mt = MessageTag {
            client_id: 42,
            request_id: 100,
            channel: UpdateChannel { id: 1, seqnum: 5 },
        };
        assert_eq!(roundtrip(&mt), mt);
    }

    #[test]
    fn test_debug_flags_roundtrip() {
        let df = DebugFlags {
            inject_random_server_error: true,
            inject_random_client_error: false,
            num_of_inject_pts_before_fail: 3,
        };
        assert_eq!(roundtrip(&df), df);
    }

    #[test]
    fn test_io_result_roundtrip() {
        let ir = IoResult {
            status_code: 0,
            length: 4096,
            commit_ver: 1,
            update_ver: 2,
            checksum: ChecksumInfo {
                checksum_type: 1,
                value: 123,
            },
            commit_chain_ver: 5,
        };
        assert_eq!(roundtrip(&ir), ir);
    }

    #[test]
    fn test_read_io_roundtrip() {
        let rio = ReadIo {
            offset: 0,
            length: 4096,
            key: GlobalKey {
                v_chain_id: VersionedChainId {
                    chain_id: 1,
                    chain_ver: 1,
                },
                chunk_id: "c1".to_string(),
            },
            rdmabuf: RdmaRemoteBuf::default(),
        };
        assert_eq!(roundtrip(&rio), rio);
    }

    #[test]
    fn test_batch_read_req_roundtrip() {
        let req = BatchReadReq {
            payloads: vec![],
            tag: MessageTag::default(),
            retry_count: 0,
            user_info: UserInfo::default(),
            feature_flags: 0,
            checksum_type: ChecksumType::None as u8,
            debug_flags: DebugFlags::default(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_batch_read_rsp_roundtrip() {
        let rsp = BatchReadRsp {
            tag: MessageTag::default(),
            results: vec![IoResult::default()],
            inline_buf: vec![1, 2, 3],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_write_req_roundtrip() {
        let req = WriteReq {
            payload: UpdateIo {
                offset: 0,
                length: 1024,
                chunk_size: 1024 * 1024,
                key: GlobalKey::default(),
                rdmabuf: RdmaRemoteBuf::default(),
                update_ver: 1,
                update_type: UpdateType::Write as u8,
                checksum: ChecksumInfo::default(),
                inline_buf: vec![0xFF; 32],
            },
            tag: MessageTag::default(),
            retry_count: 0,
            user_info: UserInfo::default(),
            feature_flags: 0,
            debug_flags: DebugFlags::default(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_write_rsp_roundtrip() {
        let rsp = WriteRsp {
            tag: MessageTag::default(),
            result: IoResult::default(),
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_update_req_roundtrip() {
        let req = UpdateReq {
            payload: UpdateIo::default(),
            options: UpdateOptions {
                is_syncing: true,
                from_client: false,
                commit_chain_ver: 5,
            },
            tag: MessageTag::default(),
            retry_count: 1,
            user_info: UserInfo::default(),
            feature_flags: 0,
            debug_flags: DebugFlags::default(),
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_query_last_chunk_roundtrip() {
        let req = QueryLastChunkReq {
            payloads: vec![QueryLastChunkOp {
                v_chain_id: VersionedChainId {
                    chain_id: 1,
                    chain_ver: 1,
                },
                chunk_id_range: ChunkIdRange {
                    begin: "a".to_string(),
                    end: "z".to_string(),
                    max_num_chunk_ids_to_process: 100,
                },
            }],
            tag: MessageTag::default(),
            retry_count: 0,
            user_info: UserInfo::default(),
            feature_flags: 0,
            debug_flags: DebugFlags::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = QueryLastChunkRsp {
            results: vec![QueryLastChunkResult {
                status_code: 0,
                last_chunk_id: "chunk-99".to_string(),
                last_chunk_len: 512,
                total_chunk_len: 4096,
                total_num_chunks: 10,
                more_chunks_in_range: false,
            }],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_remove_chunks_roundtrip() {
        let req = RemoveChunksReq {
            payloads: vec![RemoveChunksOp {
                v_chain_id: VersionedChainId::default(),
                chunk_id_range: ChunkIdRange::default(),
                tag: MessageTag::default(),
                retry_count: 0,
            }],
            user_info: UserInfo::default(),
            feature_flags: 0,
            debug_flags: DebugFlags::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = RemoveChunksRsp {
            results: vec![RemoveChunksResult {
                status_code: 0,
                num_chunks_removed: 5,
                more_chunks_in_range: false,
            }],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_truncate_chunks_roundtrip() {
        let req = TruncateChunksReq {
            payloads: vec![TruncateChunkOp {
                v_chain_id: VersionedChainId::default(),
                chunk_id: "chunk".to_string(),
                chunk_len: 512,
                chunk_size: 1024 * 1024,
                only_extend_chunk: false,
                tag: MessageTag::default(),
                retry_count: 0,
            }],
            user_info: UserInfo::default(),
            feature_flags: 0,
            debug_flags: DebugFlags::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = TruncateChunksRsp {
            results: vec![IoResult::default()],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_sync_start_roundtrip() {
        let req = SyncStartReq {
            v_chain_id: VersionedChainId {
                chain_id: 1,
                chain_ver: 5,
            },
        };
        assert_eq!(roundtrip(&req), req);
    }

    #[test]
    fn test_chunk_meta_roundtrip() {
        let cm = ChunkMeta {
            chunk_id: "c1".to_string(),
            update_ver: 3,
            commit_ver: 2,
            chain_ver: 1,
            chunk_state: ChunkState::Commit as u8,
            checksum: ChecksumInfo {
                checksum_type: 1,
                value: 42,
            },
            length: 1024,
        };
        assert_eq!(roundtrip(&cm), cm);
    }

    #[test]
    fn test_target_sync_info_roundtrip() {
        let tsi = TargetSyncInfo {
            metas: vec![ChunkMeta::default()],
        };
        assert_eq!(roundtrip(&tsi), tsi);
    }

    #[test]
    fn test_sync_done_roundtrip() {
        let req = SyncDoneReq {
            v_chain_id: VersionedChainId::default(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = SyncDoneRsp {
            result: IoResult::default(),
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_space_info_roundtrip() {
        let req = SpaceInfoReq {
            tag: MessageTag::default(),
            force: true,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = SpaceInfoRsp {
            space_infos: vec![SpaceInfo {
                path: "/data/0".to_string(),
                capacity: 1_000_000_000_000,
                free: 500_000_000_000,
                available: 400_000_000_000,
                target_ids: vec![100, 200],
                manufacturer: "Samsung".to_string(),
            }],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_create_target_roundtrip() {
        let req = CreateTargetReq {
            target_id: 100,
            disk_index: 0,
            physical_file_count: 256,
            chunk_size_list: vec![524288, 1048576, 2097152],
            allow_existing_target: true,
            chain_id: 1,
            add_chunk_size: false,
            only_chunk_engine: false,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = CreateTargetRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_offline_target_roundtrip() {
        let req = OfflineTargetReq {
            target_id: 100,
            force: false,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = OfflineTargetRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_remove_target_roundtrip() {
        let req = RemoveTargetReq {
            target_id: 100,
            force: true,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = RemoveTargetRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_query_chunk_roundtrip() {
        let req = QueryChunkReq {
            chain_id: 5,
            chunk_id: "chunk-001".to_string(),
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = QueryChunkRsp {
            status_code: 0,
            commit_ver: 3,
            update_ver: 4,
            chain_ver: 2,
            chunk_state: ChunkState::Commit as u8,
            size: 1024,
            checksum_type: ChecksumType::Crc32c as u8,
            checksum_value: 0xABCD,
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_get_all_chunk_metadata_roundtrip() {
        let req = GetAllChunkMetadataReq { target_id: 42 };
        assert_eq!(roundtrip(&req), req);

        let rsp = GetAllChunkMetadataRsp {
            chunk_meta_vec: vec![ChunkMeta::default()],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_commit_io_roundtrip() {
        let cio = CommitIo {
            key: GlobalKey::default(),
            commit_ver: 5,
            is_syncing: false,
            commit_chain_ver: 3,
            is_remove: true,
            is_force: false,
        };
        assert_eq!(roundtrip(&cio), cio);
    }

    #[test]
    fn test_feature_flags() {
        let flags = FeatureFlags(FeatureFlags::SEND_DATA_INLINE.0 | FeatureFlags::BYPASS_DISKIO.0);
        assert!(flags.contains(FeatureFlags::SEND_DATA_INLINE.0));
        assert!(flags.contains(FeatureFlags::BYPASS_DISKIO.0));
        assert!(!flags.contains(FeatureFlags::ALLOW_READ_UNCOMMITTED.0));
    }

    #[test]
    fn test_feature_flags_roundtrip() {
        let flags = FeatureFlags(7);
        assert_eq!(roundtrip(&flags), flags);
    }

    #[test]
    fn test_rdma_remote_buf_roundtrip() {
        let buf = RdmaRemoteBuf {
            addr: 0x7FFF_0000_1000,
            length: 4096,
            rkey: 0xABCD_EF01,
        };
        assert_eq!(roundtrip(&buf), buf);
    }
}
