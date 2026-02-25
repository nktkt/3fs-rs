mod engine;
mod transaction;

pub use engine::KvEngine;
pub use transaction::*;

/// Return the key immediately after the given key (for range queries).
///
/// This appends a zero byte, making the result strictly greater than the input
/// in lexicographic order.
pub fn key_after(key: &[u8]) -> Vec<u8> {
    let mut result = key.to_vec();
    result.push(0);
    result
}

/// Return the end key for a prefix range scan.
///
/// Increments the last non-0xFF byte of the prefix. If the prefix is all 0xFF
/// bytes (or empty), returns an empty vec to indicate "no upper bound".
pub fn prefix_list_end_key(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return end;
        }
        end.pop();
    }
    // Empty means the prefix was all 0xFF bytes - return empty to mean "no upper bound"
    end
}

/// The metadata version key, matching the C++ `kMetadataVersionKey`.
pub const METADATA_VERSION_KEY: &[u8] = b"\xff/metadataVersion";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_after() {
        assert_eq!(key_after(b"hello"), b"hello\0");
        assert_eq!(key_after(b""), b"\0");
        assert_eq!(key_after(b"\xff"), b"\xff\0");
    }

    #[test]
    fn test_prefix_list_end_key_simple() {
        // Increment last byte
        assert_eq!(prefix_list_end_key(b"abc"), b"abd");
        assert_eq!(prefix_list_end_key(b"\x00"), b"\x01");
        assert_eq!(prefix_list_end_key(b"a\x00"), b"a\x01");
    }

    #[test]
    fn test_prefix_list_end_key_carries() {
        // Last byte is 0xFF, so pop and increment the one before it
        assert_eq!(prefix_list_end_key(b"a\xff"), b"b");
        assert_eq!(prefix_list_end_key(b"ab\xff\xff"), b"ac");
    }

    #[test]
    fn test_prefix_list_end_key_all_ff() {
        // All 0xFF bytes - returns empty
        assert_eq!(prefix_list_end_key(b"\xff\xff\xff"), Vec::<u8>::new());
    }

    #[test]
    fn test_prefix_list_end_key_empty() {
        assert_eq!(prefix_list_end_key(b""), Vec::<u8>::new());
    }

    #[test]
    fn test_key_selector_new() {
        let ks = KeySelector::new(b"test".to_vec(), true);
        assert_eq!(ks.key, b"test");
        assert!(ks.inclusive);

        let ks2 = KeySelector::new("prefix", false);
        assert_eq!(ks2.key, b"prefix");
        assert!(!ks2.inclusive);
    }
}
