use hf3fs_types::{Result, Status, StatusCode};

/// Compress data using ZSTD at the given level (1-22).
pub fn zstd_compress(data: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::encode_all(data, level).map_err(|e| {
        Status::with_message(StatusCode::IO_ERROR, format!("ZSTD compress failed: {}", e))
    })
}

/// Decompress ZSTD-compressed data.
pub fn zstd_decompress(data: &[u8]) -> Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| {
        Status::with_message(StatusCode::IO_ERROR, format!("ZSTD decompress failed: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let data = b"hello world, this is a test of ZSTD compression!";
        let compressed = zstd_compress(data, 3).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_empty() {
        let data = b"";
        let compressed = zstd_compress(data, 1).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }
}
