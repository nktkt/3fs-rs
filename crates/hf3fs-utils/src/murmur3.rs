/// MurmurHash3 128-bit (x64 variant).

#[inline(always)]
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51afd7ed558ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ceb9fe1a85ec53);
    k ^= k >> 33;
    k
}

/// Compute MurmurHash3 128-bit hash (x64 variant) of the given data with a seed.
/// Returns `(h1, h2)` as two u64 values.
pub fn murmurhash3_x64_128(data: &[u8], seed: u64) -> (u64, u64) {
    let c1: u64 = 0x87c37b91114253d5;
    let c2: u64 = 0x4cf5ad432745937f;

    let mut h1: u64 = seed;
    let mut h2: u64 = seed;

    let nblocks = data.len() / 16;

    // Body: process 16-byte blocks.
    for i in 0..nblocks {
        let offset = i * 16;
        let mut k1 = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        let mut k2 = u64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap());

        k1 = k1.wrapping_mul(c1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(c2);
        h1 ^= k1;

        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dce729);

        k2 = k2.wrapping_mul(c2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(c1);
        h2 ^= k2;

        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x38495ab5);
    }

    // Tail: remaining bytes.
    let tail = &data[nblocks * 16..];
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;

    // Fall through intentionally (like the C switch-case).
    match tail.len() {
        15 => {
            k2 ^= (tail[14] as u64) << 48;
            k2 ^= (tail[13] as u64) << 40;
            k2 ^= (tail[12] as u64) << 32;
            k2 ^= (tail[11] as u64) << 24;
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        14 => {
            k2 ^= (tail[13] as u64) << 40;
            k2 ^= (tail[12] as u64) << 32;
            k2 ^= (tail[11] as u64) << 24;
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        13 => {
            k2 ^= (tail[12] as u64) << 32;
            k2 ^= (tail[11] as u64) << 24;
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        12 => {
            k2 ^= (tail[11] as u64) << 24;
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        11 => {
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        10 => {
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        9 => {
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        8 => {
            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        7 => {
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        6 => {
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        5 => {
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        4 => {
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        3 => {
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        2 => {
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        1 => {
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        _ => {}
    }

    // Finalization.
    h1 ^= data.len() as u64;
    h2 ^= data.len() as u64;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1, h2)
}

/// Convenience: compute MurmurHash3 128-bit and return as `[u8; 16]`.
pub fn murmurhash3_x64_128_bytes(data: &[u8], seed: u64) -> [u8; 16] {
    let (h1, h2) = murmurhash3_x64_128(data, seed);
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&h1.to_le_bytes());
    out[8..].copy_from_slice(&h2.to_le_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty() {
        // Empty input with seed 0 produces a deterministic result.
        let (h1, h2) = murmurhash3_x64_128(b"", 0);
        let (h1b, h2b) = murmurhash3_x64_128(b"", 0);
        assert_eq!(h1, h1b);
        assert_eq!(h2, h2b);
    }

    #[test]
    fn test_deterministic() {
        let (a1, a2) = murmurhash3_x64_128(b"hello", 42);
        let (b1, b2) = murmurhash3_x64_128(b"hello", 42);
        assert_eq!(a1, b1);
        assert_eq!(a2, b2);
    }

    #[test]
    fn test_different_seeds() {
        let (a1, a2) = murmurhash3_x64_128(b"hello", 0);
        let (b1, b2) = murmurhash3_x64_128(b"hello", 1);
        assert!(a1 != b1 || a2 != b2);
    }

    #[test]
    fn test_different_data() {
        let (a1, a2) = murmurhash3_x64_128(b"hello", 0);
        let (b1, b2) = murmurhash3_x64_128(b"world", 0);
        assert!(a1 != b1 || a2 != b2);
    }

    #[test]
    fn test_bytes_format() {
        let bytes = murmurhash3_x64_128_bytes(b"test", 0);
        let (h1, h2) = murmurhash3_x64_128(b"test", 0);
        assert_eq!(&bytes[..8], &h1.to_le_bytes());
        assert_eq!(&bytes[8..], &h2.to_le_bytes());
    }

    #[test]
    fn test_various_lengths() {
        // Ensure no panics for lengths 0..32.
        for len in 0..32 {
            let data: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let _ = murmurhash3_x64_128(&data, 0);
        }
    }
}
