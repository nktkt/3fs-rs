//! Storage service RPC types.
//!
//! Stubs for now - will be expanded when storage service is implemented.

use hf3fs_serde::{WireDeserialize, WireSerialize};
use serde::{Deserialize, Serialize};

/// Request to read a chunk from storage.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct ReadChunkReq {
    pub chunk_id: u64,
    pub chunk_offset: u64,
    pub length: u32,
}

/// Response with chunk data.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct ReadChunkRsp {
    pub data: Vec<u8>,
}

/// Request to write a chunk to storage.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct WriteChunkReq {
    pub chunk_id: u64,
    pub chunk_offset: u64,
    pub data: Vec<u8>,
}

/// Response acknowledging a chunk write.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct WriteChunkRsp {
    pub bytes_written: u32,
}

/// Request to remove a chunk.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct RemoveChunkReq {
    pub chunk_id: u64,
}

/// Response acknowledging chunk removal.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, WireSerialize, WireDeserialize)]
pub struct RemoveChunkRsp {}

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
    fn test_read_chunk_roundtrip() {
        let req = ReadChunkReq {
            chunk_id: 12345,
            chunk_offset: 0,
            length: 4096,
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = ReadChunkRsp {
            data: vec![1, 2, 3, 4, 5],
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_write_chunk_roundtrip() {
        let req = WriteChunkReq {
            chunk_id: 99,
            chunk_offset: 512,
            data: vec![0xFF; 128],
        };
        assert_eq!(roundtrip(&req), req);

        let rsp = WriteChunkRsp {
            bytes_written: 128,
        };
        assert_eq!(roundtrip(&rsp), rsp);
    }

    #[test]
    fn test_remove_chunk_roundtrip() {
        let req = RemoveChunkReq { chunk_id: 42 };
        assert_eq!(roundtrip(&req), req);

        let rsp = RemoveChunkRsp {};
        assert_eq!(roundtrip(&rsp), rsp);
    }
}
