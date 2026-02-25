const MESSAGE_HEADER_SIZE: usize = 8;
const SERDE_MESSAGE_MAGIC_NUM: u8 = 0x86;
#[allow(dead_code)]
const MESSAGE_MAX_SIZE: usize = 512 * 1024 * 1024; // 512 MB

#[allow(dead_code)]
pub struct MessageHeader {
    pub checksum: u32,
    pub size: u32,
}

impl MessageHeader {
    #[allow(dead_code)]
    pub fn is_serde_message(&self) -> bool {
        (self.checksum & 0xfe) == SERDE_MESSAGE_MAGIC_NUM as u32
    }

    #[allow(dead_code)]
    pub fn is_compressed(&self) -> bool {
        self.checksum & 1 != 0
    }
}

/// Calculate CRC32C checksum with magic number in low byte.
pub fn calc_serde_checksum(data: &[u8], compressed: bool) -> u32 {
    let crc = crc32c::crc32c(data);
    (crc & !0xff) | (SERDE_MESSAGE_MAGIC_NUM as u32) | (compressed as u32)
}

pub struct MessagePacket;

impl MessagePacket {
    /// Pack a serializable message into framed bytes.
    pub fn pack<T: super::WireSerialize>(msg: &T) -> Result<Vec<u8>, super::WireError> {
        let mut payload = Vec::new();
        msg.wire_serialize(&mut payload)?;

        let size = payload.len() as u32;
        let checksum = calc_serde_checksum(&payload, false);

        let mut result = Vec::with_capacity(MESSAGE_HEADER_SIZE + payload.len());
        result.extend_from_slice(&checksum.to_le_bytes());
        result.extend_from_slice(&size.to_le_bytes());
        result.extend(payload);

        Ok(result)
    }

    /// Unpack a framed message.
    pub fn unpack<T: super::WireDeserialize>(data: &[u8]) -> Result<T, super::WireError> {
        if data.len() < MESSAGE_HEADER_SIZE {
            return Err(super::WireError::InsufficientData {
                need: MESSAGE_HEADER_SIZE,
                have: data.len(),
            });
        }
        let payload = &data[MESSAGE_HEADER_SIZE..];
        let mut offset = 0;
        T::wire_deserialize(payload, &mut offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calc_serde_checksum_magic() {
        let data = b"hello world";
        let checksum = calc_serde_checksum(data, false);
        // Low byte should be the magic number (0x86)
        assert_eq!(checksum & 0xff, SERDE_MESSAGE_MAGIC_NUM as u32);
    }

    #[test]
    fn test_calc_serde_checksum_compressed() {
        let data = b"hello world";
        let checksum = calc_serde_checksum(data, true);
        // Low byte should be magic | 1 = 0x87
        assert_eq!(checksum & 0xff, (SERDE_MESSAGE_MAGIC_NUM as u32) | 1);
    }

    #[test]
    fn test_message_header_is_serde_message() {
        let data = b"test";
        let checksum = calc_serde_checksum(data, false);
        let header = MessageHeader { checksum, size: 4 };
        assert!(header.is_serde_message());
        assert!(!header.is_compressed());
    }

    #[test]
    fn test_message_header_compressed() {
        let data = b"test";
        let checksum = calc_serde_checksum(data, true);
        let header = MessageHeader { checksum, size: 4 };
        assert!(header.is_serde_message());
        assert!(header.is_compressed());
    }

    #[test]
    fn test_pack_unpack_u32() {
        let val: u32 = 0xDEADBEEF;
        let packed = MessagePacket::pack(&val).unwrap();
        assert_eq!(packed.len(), 8 + 4); // header + u32
        let unpacked: u32 = MessagePacket::unpack(&packed).unwrap();
        assert_eq!(unpacked, val);
    }

    #[test]
    fn test_pack_unpack_string() {
        let val = "hello, world!".to_string();
        let packed = MessagePacket::pack(&val).unwrap();
        let unpacked: String = MessagePacket::unpack(&packed).unwrap();
        assert_eq!(unpacked, val);
    }

    #[test]
    fn test_pack_unpack_tuple() {
        let val = (42u32, "test".to_string());
        let packed = MessagePacket::pack(&val).unwrap();
        let unpacked: (u32, String) = MessagePacket::unpack(&packed).unwrap();
        assert_eq!(unpacked, val);
    }

    #[test]
    fn test_unpack_insufficient_data() {
        let data = vec![0u8; 4]; // less than header size
        let result = MessagePacket::unpack::<u32>(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_pack_header_format() {
        let val = 0u32;
        let packed = MessagePacket::pack(&val).unwrap();
        // First 4 bytes: checksum (LE), next 4 bytes: size (LE)
        let checksum = u32::from_le_bytes(packed[0..4].try_into().unwrap());
        let size = u32::from_le_bytes(packed[4..8].try_into().unwrap());
        assert_eq!(size, 4); // u32 is 4 bytes
        assert_eq!(checksum & 0xff, SERDE_MESSAGE_MAGIC_NUM as u32);
    }
}
