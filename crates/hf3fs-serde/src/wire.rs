use byteorder::{ByteOrder, LittleEndian};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WireError {
    #[error("insufficient data: need {need} bytes but only {have} remain")]
    InsufficientData { need: usize, have: usize },
    #[error("invalid enum variant for {enum_name}: {value}")]
    InvalidEnumVariant {
        enum_name: &'static str,
        value: u64,
    },
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    #[error("data too large: {size} bytes")]
    DataTooLarge { size: usize },
}

pub trait WireSerialize {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError>;
}

pub trait WireDeserialize: Sized {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError>;
}

fn read_bytes<'a>(buf: &'a [u8], offset: &mut usize, n: usize) -> Result<&'a [u8], WireError> {
    if buf.len() - *offset < n {
        return Err(WireError::InsufficientData {
            need: n,
            have: buf.len() - *offset,
        });
    }
    let slice = &buf[*offset..*offset + n];
    *offset += n;
    Ok(slice)
}

// ---------------------------------------------------------------------------
// Integer types
// ---------------------------------------------------------------------------

macro_rules! impl_wire_for_int {
    ($ty:ty, $size:expr, $read:ident, $write:ident) => {
        impl WireSerialize for $ty {
            fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
                let mut tmp = [0u8; $size];
                LittleEndian::$write(&mut tmp, *self);
                buf.extend_from_slice(&tmp);
                Ok(())
            }
        }

        impl WireDeserialize for $ty {
            fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
                let bytes = read_bytes(buf, offset, $size)?;
                Ok(LittleEndian::$read(bytes))
            }
        }
    };
}

impl_wire_for_int!(u16, 2, read_u16, write_u16);
impl_wire_for_int!(u32, 4, read_u32, write_u32);
impl_wire_for_int!(u64, 8, read_u64, write_u64);
impl_wire_for_int!(i16, 2, read_i16, write_i16);
impl_wire_for_int!(i32, 4, read_i32, write_i32);
impl_wire_for_int!(i64, 8, read_i64, write_i64);

// u8 and i8 are single-byte, no endianness needed.

impl WireSerialize for u8 {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        buf.push(*self);
        Ok(())
    }
}

impl WireDeserialize for u8 {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let bytes = read_bytes(buf, offset, 1)?;
        Ok(bytes[0])
    }
}

impl WireSerialize for i8 {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        buf.push(*self as u8);
        Ok(())
    }
}

impl WireDeserialize for i8 {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let bytes = read_bytes(buf, offset, 1)?;
        Ok(bytes[0] as i8)
    }
}

// ---------------------------------------------------------------------------
// Floating point types
// ---------------------------------------------------------------------------

impl WireSerialize for f32 {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        buf.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl WireDeserialize for f32 {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let bytes = read_bytes(buf, offset, 4)?;
        Ok(f32::from_le_bytes(bytes.try_into().unwrap()))
    }
}

impl WireSerialize for f64 {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        buf.extend_from_slice(&self.to_le_bytes());
        Ok(())
    }
}

impl WireDeserialize for f64 {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let bytes = read_bytes(buf, offset, 8)?;
        Ok(f64::from_le_bytes(bytes.try_into().unwrap()))
    }
}

// ---------------------------------------------------------------------------
// bool
// ---------------------------------------------------------------------------

impl WireSerialize for bool {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        buf.push(if *self { 1u8 } else { 0u8 });
        Ok(())
    }
}

impl WireDeserialize for bool {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let v = u8::wire_deserialize(buf, offset)?;
        Ok(v != 0)
    }
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

impl WireSerialize for String {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        let len = self.len() as u32;
        len.wire_serialize(buf)?;
        buf.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

impl WireDeserialize for String {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let len = u32::wire_deserialize(buf, offset)? as usize;
        let bytes = read_bytes(buf, offset, len)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| WireError::InvalidUtf8)
    }
}

// Note: Vec<u8> is handled by the generic Vec<T> impl below.
// Since u8 serializes as a single byte, Vec<u8> produces identical bytes
// to a raw length-prefixed byte buffer (u32 count + raw bytes).

// ---------------------------------------------------------------------------
// bytes::Bytes
// ---------------------------------------------------------------------------

impl WireSerialize for bytes::Bytes {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        let len = self.len() as u32;
        len.wire_serialize(buf)?;
        buf.extend_from_slice(self);
        Ok(())
    }
}

impl WireDeserialize for bytes::Bytes {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let len = u32::wire_deserialize(buf, offset)? as usize;
        let bytes = read_bytes(buf, offset, len)?;
        Ok(bytes::Bytes::copy_from_slice(bytes))
    }
}

// ---------------------------------------------------------------------------
// Vec<T> (generic)
// ---------------------------------------------------------------------------

// Note: Vec<u8> above has a specialized impl that treats bytes as raw data.
// This blanket impl handles all other Vec<T> as u32 count + element-wise serialization.
// Since Vec<u8> is implemented directly above, Rust's coherence rules prefer
// the concrete impl over this blanket impl for Vec<u8>.

impl<T: WireSerialize> WireSerialize for Vec<T> {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        let len = self.len() as u32;
        len.wire_serialize(buf)?;
        for item in self {
            item.wire_serialize(buf)?;
        }
        Ok(())
    }
}

impl<T: WireDeserialize> WireDeserialize for Vec<T> {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let len = u32::wire_deserialize(buf, offset)? as usize;
        let mut result = Vec::with_capacity(len);
        for _ in 0..len {
            result.push(T::wire_deserialize(buf, offset)?);
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Option<T>
// ---------------------------------------------------------------------------

impl<T: WireSerialize> WireSerialize for Option<T> {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        match self {
            None => 0u8.wire_serialize(buf),
            Some(val) => {
                1u8.wire_serialize(buf)?;
                val.wire_serialize(buf)
            }
        }
    }
}

impl<T: WireDeserialize> WireDeserialize for Option<T> {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let tag = u8::wire_deserialize(buf, offset)?;
        match tag {
            0 => Ok(None),
            _ => Ok(Some(T::wire_deserialize(buf, offset)?)),
        }
    }
}

// ---------------------------------------------------------------------------
// Box<T>
// ---------------------------------------------------------------------------

impl<T: WireSerialize> WireSerialize for Box<T> {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        (**self).wire_serialize(buf)
    }
}

impl<T: WireDeserialize> WireDeserialize for Box<T> {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        Ok(Box::new(T::wire_deserialize(buf, offset)?))
    }
}

// ---------------------------------------------------------------------------
// Tuples
// ---------------------------------------------------------------------------

impl<A: WireSerialize, B: WireSerialize> WireSerialize for (A, B) {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        self.0.wire_serialize(buf)?;
        self.1.wire_serialize(buf)
    }
}

impl<A: WireDeserialize, B: WireDeserialize> WireDeserialize for (A, B) {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let a = A::wire_deserialize(buf, offset)?;
        let b = B::wire_deserialize(buf, offset)?;
        Ok((a, b))
    }
}

impl<A: WireSerialize, B: WireSerialize, C: WireSerialize> WireSerialize for (A, B, C) {
    fn wire_serialize(&self, buf: &mut Vec<u8>) -> Result<(), WireError> {
        self.0.wire_serialize(buf)?;
        self.1.wire_serialize(buf)?;
        self.2.wire_serialize(buf)
    }
}

impl<A: WireDeserialize, B: WireDeserialize, C: WireDeserialize> WireDeserialize for (A, B, C) {
    fn wire_deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, WireError> {
        let a = A::wire_deserialize(buf, offset)?;
        let b = B::wire_deserialize(buf, offset)?;
        let c = C::wire_deserialize(buf, offset)?;
        Ok((a, b, c))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_serialize<T: WireSerialize + WireDeserialize + std::fmt::Debug + PartialEq>(
        val: &T,
    ) -> T {
        let mut buf = Vec::new();
        val.wire_serialize(&mut buf).unwrap();
        let mut offset = 0;
        let result = T::wire_deserialize(&buf, &mut offset).unwrap();
        assert_eq!(offset, buf.len(), "all bytes should be consumed");
        result
    }

    #[test]
    fn test_u8() {
        assert_eq!(roundtrip_serialize(&0u8), 0u8);
        assert_eq!(roundtrip_serialize(&255u8), 255u8);
        assert_eq!(roundtrip_serialize(&42u8), 42u8);
    }

    #[test]
    fn test_i8() {
        assert_eq!(roundtrip_serialize(&0i8), 0i8);
        assert_eq!(roundtrip_serialize(&-128i8), -128i8);
        assert_eq!(roundtrip_serialize(&127i8), 127i8);
    }

    #[test]
    fn test_u16() {
        assert_eq!(roundtrip_serialize(&0u16), 0u16);
        assert_eq!(roundtrip_serialize(&0x1234u16), 0x1234u16);
        assert_eq!(roundtrip_serialize(&u16::MAX), u16::MAX);
    }

    #[test]
    fn test_u32() {
        assert_eq!(roundtrip_serialize(&0u32), 0u32);
        assert_eq!(roundtrip_serialize(&0xDEADBEEFu32), 0xDEADBEEFu32);
    }

    #[test]
    fn test_u64() {
        assert_eq!(roundtrip_serialize(&0u64), 0u64);
        assert_eq!(roundtrip_serialize(&u64::MAX), u64::MAX);
    }

    #[test]
    fn test_i16() {
        assert_eq!(roundtrip_serialize(&-1i16), -1i16);
        assert_eq!(roundtrip_serialize(&i16::MIN), i16::MIN);
    }

    #[test]
    fn test_i32() {
        assert_eq!(roundtrip_serialize(&-42i32), -42i32);
        assert_eq!(roundtrip_serialize(&i32::MAX), i32::MAX);
    }

    #[test]
    fn test_i64() {
        assert_eq!(roundtrip_serialize(&i64::MIN), i64::MIN);
        assert_eq!(roundtrip_serialize(&i64::MAX), i64::MAX);
    }

    #[test]
    fn test_f32() {
        assert_eq!(roundtrip_serialize(&0.0f32), 0.0f32);
        assert_eq!(roundtrip_serialize(&3.14f32), 3.14f32);
        assert_eq!(roundtrip_serialize(&f32::MIN), f32::MIN);
    }

    #[test]
    fn test_f64() {
        assert_eq!(roundtrip_serialize(&0.0f64), 0.0f64);
        assert_eq!(
            roundtrip_serialize(&std::f64::consts::PI),
            std::f64::consts::PI
        );
    }

    #[test]
    fn test_bool() {
        assert_eq!(roundtrip_serialize(&true), true);
        assert_eq!(roundtrip_serialize(&false), false);
    }

    #[test]
    fn test_string() {
        assert_eq!(roundtrip_serialize(&String::new()), String::new());
        assert_eq!(
            roundtrip_serialize(&"hello world".to_string()),
            "hello world".to_string()
        );
        assert_eq!(
            roundtrip_serialize(&"utf-8: \u{1F600}".to_string()),
            "utf-8: \u{1F600}".to_string()
        );
    }

    #[test]
    fn test_vec_u8() {
        assert_eq!(roundtrip_serialize(&Vec::<u8>::new()), Vec::<u8>::new());
        assert_eq!(
            roundtrip_serialize(&vec![1u8, 2, 3, 4, 5]),
            vec![1u8, 2, 3, 4, 5]
        );
    }

    #[test]
    fn test_bytes() {
        let empty = bytes::Bytes::new();
        assert_eq!(roundtrip_serialize(&empty), empty);
        let data = bytes::Bytes::from_static(b"hello");
        assert_eq!(roundtrip_serialize(&data), data);
    }

    #[test]
    fn test_vec_u32() {
        assert_eq!(roundtrip_serialize(&Vec::<u32>::new()), Vec::<u32>::new());
        assert_eq!(
            roundtrip_serialize(&vec![100u32, 200, 300]),
            vec![100u32, 200, 300]
        );
    }

    #[test]
    fn test_vec_string() {
        let v = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];
        assert_eq!(roundtrip_serialize(&v), v);
    }

    #[test]
    fn test_option_none() {
        let val: Option<u32> = None;
        assert_eq!(roundtrip_serialize(&val), None);
    }

    #[test]
    fn test_option_some() {
        let val: Option<u32> = Some(42);
        assert_eq!(roundtrip_serialize(&val), Some(42));
    }

    #[test]
    fn test_option_string() {
        let val: Option<String> = Some("test".to_string());
        assert_eq!(roundtrip_serialize(&val), Some("test".to_string()));
    }

    #[test]
    fn test_box() {
        let val = Box::new(42u32);
        assert_eq!(roundtrip_serialize(&val), Box::new(42u32));
    }

    #[test]
    fn test_tuple_2() {
        let val = (42u32, "hello".to_string());
        assert_eq!(roundtrip_serialize(&val), (42u32, "hello".to_string()));
    }

    #[test]
    fn test_tuple_3() {
        let val = (1u8, 2u16, 3u32);
        assert_eq!(roundtrip_serialize(&val), (1u8, 2u16, 3u32));
    }

    #[test]
    fn test_little_endian_encoding() {
        let mut buf = Vec::new();
        0x04030201u32.wire_serialize(&mut buf).unwrap();
        assert_eq!(buf, vec![0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_insufficient_data() {
        let buf = vec![0u8; 2];
        let mut offset = 0;
        let result = u32::wire_deserialize(&buf, &mut offset);
        assert!(result.is_err());
        match result.unwrap_err() {
            WireError::InsufficientData { need, have } => {
                assert_eq!(need, 4);
                assert_eq!(have, 2);
            }
            _ => panic!("expected InsufficientData error"),
        }
    }

    #[test]
    fn test_invalid_utf8() {
        let mut buf = Vec::new();
        2u32.wire_serialize(&mut buf).unwrap();
        buf.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8
        let mut offset = 0;
        let result = String::wire_deserialize(&buf, &mut offset);
        assert!(matches!(result, Err(WireError::InvalidUtf8)));
    }

    #[test]
    fn test_nested_option_vec() {
        let val: Option<Vec<u32>> = Some(vec![1, 2, 3]);
        assert_eq!(roundtrip_serialize(&val), Some(vec![1, 2, 3]));

        let val: Option<Vec<u32>> = None;
        assert_eq!(roundtrip_serialize(&val), None);
    }

    #[test]
    fn test_multiple_values_in_buffer() {
        let mut buf = Vec::new();
        42u32.wire_serialize(&mut buf).unwrap();
        "hello".to_string().wire_serialize(&mut buf).unwrap();
        true.wire_serialize(&mut buf).unwrap();

        let mut offset = 0;
        assert_eq!(u32::wire_deserialize(&buf, &mut offset).unwrap(), 42);
        assert_eq!(
            String::wire_deserialize(&buf, &mut offset).unwrap(),
            "hello"
        );
        assert_eq!(bool::wire_deserialize(&buf, &mut offset).unwrap(), true);
        assert_eq!(offset, buf.len());
    }
}
