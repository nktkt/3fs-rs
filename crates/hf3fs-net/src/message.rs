/// Size of the message header in bytes.
pub const MESSAGE_HEADER_SIZE: usize = 8;

/// Magic number identifying a serde-serialized message (occupies the low byte of `checksum`).
pub const SERDE_MESSAGE_MAGIC_NUM: u8 = 0x86;

/// Maximum allowed message size (512 MiB).
pub const MESSAGE_MAX_SIZE: usize = 512 * 1024 * 1024;

/// Wire header prepended to every network message.
///
/// Mirrors the C++ `MessageHeader` struct: 4-byte checksum followed by 4-byte payload size,
/// both in little-endian byte order.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    pub checksum: u32,
    pub size: u32,
}

impl MessageHeader {
    /// Create a new header for the given payload.
    ///
    /// Computes the CRC32C checksum with the serde magic number embedded in the
    /// low byte.
    pub fn for_payload(payload: &[u8], compressed: bool) -> Self {
        Self {
            checksum: calc_serde_checksum(payload, compressed),
            size: payload.len() as u32,
        }
    }

    /// Returns `true` if this header carries a serde-encoded message.
    ///
    /// The magic number `0x86` is stored in the low byte of `checksum` with bit 0
    /// reserved for the compression flag, so we mask with `0xFE` before comparing.
    pub fn is_serde_message(&self) -> bool {
        (self.checksum & 0xFE) == SERDE_MESSAGE_MAGIC_NUM as u32
    }

    /// Returns `true` if the payload is compressed.
    pub fn is_compressed(&self) -> bool {
        self.checksum & 1 != 0
    }

    /// Deserialize a header from an 8-byte little-endian buffer.
    pub fn from_bytes(data: &[u8; MESSAGE_HEADER_SIZE]) -> Self {
        let checksum = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
        Self { checksum, size }
    }

    /// Serialize this header into an 8-byte little-endian buffer.
    pub fn to_bytes(&self) -> [u8; MESSAGE_HEADER_SIZE] {
        let mut buf = [0u8; MESSAGE_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.checksum.to_le_bytes());
        buf[4..8].copy_from_slice(&self.size.to_le_bytes());
        buf
    }

    /// Validate that this header describes a legitimate serde message and that
    /// the payload checksum matches.
    ///
    /// Returns `Ok(())` on success or the appropriate `NetError`.
    pub fn validate(&self, payload: &[u8]) -> Result<(), crate::error::NetError> {
        if !self.is_serde_message() {
            return Err(crate::error::NetError::InvalidMagic(
                (self.checksum & 0xFF) as u8,
            ));
        }

        let size = self.size as usize;
        if size > MESSAGE_MAX_SIZE {
            return Err(crate::error::NetError::MessageTooLarge {
                size,
                max: MESSAGE_MAX_SIZE,
            });
        }

        let expected = calc_serde_checksum(payload, self.is_compressed());
        if self.checksum != expected {
            return Err(crate::error::NetError::ChecksumMismatch {
                expected,
                actual: self.checksum,
            });
        }

        Ok(())
    }
}

/// Calculate CRC32C checksum with the serde magic number in the low byte.
///
/// This matches the C++ `calc_serde_checksum` implementation:
/// - Compute CRC32C over `data`.
/// - Replace the low byte with `0x86` (the magic number).
/// - Set bit 0 if `compressed` is true.
pub fn calc_serde_checksum(data: &[u8], compressed: bool) -> u32 {
    let crc = crc32c::crc32c(data);
    (crc & !0xff) | (SERDE_MESSAGE_MAGIC_NUM as u32) | (compressed as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let hdr = MessageHeader {
            checksum: 0xAABB_CC86,
            size: 1024,
        };
        let bytes = hdr.to_bytes();
        let hdr2 = MessageHeader::from_bytes(&bytes);
        assert_eq!(hdr, hdr2);
    }

    #[test]
    fn test_is_serde_message() {
        let hdr = MessageHeader {
            checksum: 0x0000_0086,
            size: 0,
        };
        assert!(hdr.is_serde_message());
        assert!(!hdr.is_compressed());
    }

    #[test]
    fn test_is_serde_message_compressed() {
        let hdr = MessageHeader {
            checksum: 0x0000_0087,
            size: 0,
        };
        assert!(hdr.is_serde_message());
        assert!(hdr.is_compressed());
    }

    #[test]
    fn test_not_serde_message() {
        let hdr = MessageHeader {
            checksum: 0x1234_5678,
            size: 0,
        };
        assert!(!hdr.is_serde_message());
    }

    #[test]
    fn test_header_size() {
        assert_eq!(std::mem::size_of::<MessageHeader>(), MESSAGE_HEADER_SIZE);
    }

    #[test]
    fn test_from_bytes_le() {
        let data = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08];
        let hdr = MessageHeader::from_bytes(&data);
        assert_eq!(hdr.checksum, 0x04030201);
        assert_eq!(hdr.size, 0x08070605);
    }

    #[test]
    fn test_calc_serde_checksum_magic() {
        let data = b"hello world";
        let checksum = calc_serde_checksum(data, false);
        assert_eq!(checksum & 0xff, SERDE_MESSAGE_MAGIC_NUM as u32);
    }

    #[test]
    fn test_calc_serde_checksum_compressed() {
        let data = b"hello world";
        let checksum = calc_serde_checksum(data, true);
        assert_eq!(checksum & 0xff, (SERDE_MESSAGE_MAGIC_NUM as u32) | 1);
    }

    #[test]
    fn test_for_payload() {
        let payload = b"test data";
        let hdr = MessageHeader::for_payload(payload, false);
        assert!(hdr.is_serde_message());
        assert!(!hdr.is_compressed());
        assert_eq!(hdr.size, payload.len() as u32);
    }

    #[test]
    fn test_validate_success() {
        let payload = b"some payload data";
        let hdr = MessageHeader::for_payload(payload, false);
        assert!(hdr.validate(payload).is_ok());
    }

    #[test]
    fn test_validate_bad_magic() {
        let hdr = MessageHeader {
            checksum: 0x1234_5678, // low byte is 0x78, not 0x86
            size: 5,
        };
        let result = hdr.validate(b"12345");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, crate::error::NetError::InvalidMagic(0x78)));
    }

    #[test]
    fn test_validate_bad_checksum() {
        let payload = b"some data";
        let mut hdr = MessageHeader::for_payload(payload, false);
        // Corrupt the upper bits of the checksum but keep the magic byte valid
        hdr.checksum ^= 0xFF00_0000;
        let result = hdr.validate(payload);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::NetError::ChecksumMismatch { .. }
        ));
    }

    #[test]
    fn test_validate_too_large() {
        let hdr = MessageHeader {
            checksum: SERDE_MESSAGE_MAGIC_NUM as u32,
            size: (MESSAGE_MAX_SIZE + 1) as u32,
        };
        let result = hdr.validate(&[]);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::NetError::MessageTooLarge { .. }
        ));
    }
}
