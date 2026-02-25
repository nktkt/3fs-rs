/// LEB128-style variable-length integer encoding/decoding.

/// Encode a u64 value as a varint into the buffer. Returns the number of bytes written.
pub fn encode_varint(mut value: u64, buf: &mut [u8]) -> usize {
    let mut i = 0;
    loop {
        if i >= buf.len() {
            panic!("buffer too small for varint encoding");
        }
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf[i] = byte;
            return i + 1;
        } else {
            buf[i] = byte | 0x80;
        }
        i += 1;
    }
}

/// Encode a u64 value as a varint, returning a Vec.
pub fn encode_varint_vec(value: u64) -> Vec<u8> {
    let mut buf = [0u8; 10];
    let n = encode_varint(value, &mut buf);
    buf[..n].to_vec()
}

/// Decode a varint from a byte slice. Returns `(value, bytes_consumed)`.
/// Returns `None` if the input is truncated or the varint exceeds 10 bytes.
pub fn decode_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;

    for (i, &byte) in buf.iter().enumerate() {
        if i >= 10 {
            return None; // overflow
        }
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
        shift += 7;
    }

    None // truncated input
}

/// Encode a signed i64 as a varint using ZigZag encoding.
pub fn encode_signed_varint(value: i64, buf: &mut [u8]) -> usize {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint(zigzag, buf)
}

/// Decode a ZigZag-encoded signed varint.
pub fn decode_signed_varint(buf: &[u8]) -> Option<(i64, usize)> {
    let (zigzag, n) = decode_varint(buf)?;
    let value = ((zigzag >> 1) as i64) ^ (-((zigzag & 1) as i64));
    Some((value, n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_small() {
        let mut buf = [0u8; 10];
        let n = encode_varint(0, &mut buf);
        assert_eq!(n, 1);
        assert_eq!(buf[0], 0);
        assert_eq!(decode_varint(&buf[..n]), Some((0, 1)));

        let n = encode_varint(1, &mut buf);
        assert_eq!(n, 1);
        assert_eq!(decode_varint(&buf[..n]), Some((1, 1)));

        let n = encode_varint(127, &mut buf);
        assert_eq!(n, 1);
        assert_eq!(decode_varint(&buf[..n]), Some((127, 1)));
    }

    #[test]
    fn test_encode_decode_multi_byte() {
        let mut buf = [0u8; 10];
        let n = encode_varint(128, &mut buf);
        assert_eq!(n, 2);
        assert_eq!(decode_varint(&buf[..n]), Some((128, 2)));

        let n = encode_varint(300, &mut buf);
        assert_eq!(decode_varint(&buf[..n]), Some((300, n)));

        let n = encode_varint(u64::MAX, &mut buf);
        assert_eq!(decode_varint(&buf[..n]), Some((u64::MAX, n)));
    }

    #[test]
    fn test_signed_varint() {
        let mut buf = [0u8; 10];

        let n = encode_signed_varint(0, &mut buf);
        assert_eq!(decode_signed_varint(&buf[..n]), Some((0, n)));

        let n = encode_signed_varint(-1, &mut buf);
        assert_eq!(decode_signed_varint(&buf[..n]), Some((-1, n)));

        let n = encode_signed_varint(1, &mut buf);
        assert_eq!(decode_signed_varint(&buf[..n]), Some((1, n)));

        let n = encode_signed_varint(i64::MIN, &mut buf);
        assert_eq!(decode_signed_varint(&buf[..n]), Some((i64::MIN, n)));

        let n = encode_signed_varint(i64::MAX, &mut buf);
        assert_eq!(decode_signed_varint(&buf[..n]), Some((i64::MAX, n)));
    }

    #[test]
    fn test_encode_varint_vec() {
        let v = encode_varint_vec(300);
        assert_eq!(decode_varint(&v), Some((300, v.len())));
    }

    #[test]
    fn test_decode_truncated() {
        assert_eq!(decode_varint(&[]), None);
        assert_eq!(decode_varint(&[0x80]), None); // continuation bit set, no more bytes
    }
}
