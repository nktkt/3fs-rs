//! RDMA buffer types.
//!
//! Corresponds to the C++ `RDMABuf` and `RDMARemoteBuf` classes from
//! `src/common/net/ib/RDMABuf.h`.

use serde::{Deserialize, Serialize};

/// Maximum number of RDMA devices supported simultaneously.
pub const MAX_DEVICE_COUNT: usize = 4;

/// A remote key for a specific device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rkey {
    /// The remote key value.
    pub rkey: u32,
    /// The device ID this key belongs to. -1 means unused.
    pub dev_id: i32,
}

impl Default for Rkey {
    fn default() -> Self {
        Self {
            rkey: 0,
            dev_id: -1,
        }
    }
}

/// A reference to a remote RDMA buffer.
///
/// Contains the remote memory address, length, and per-device remote keys
/// needed to perform RDMA read/write operations.
///
/// Corresponds to the C++ `RDMARemoteBuf`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RdmaRemoteBuf {
    /// Remote memory address.
    addr: u64,
    /// Length of the buffer in bytes.
    length: u64,
    /// Per-device remote keys.
    rkeys: [Rkey; MAX_DEVICE_COUNT],
}

impl Default for RdmaRemoteBuf {
    fn default() -> Self {
        Self {
            addr: 0,
            length: 0,
            rkeys: [Rkey::default(); MAX_DEVICE_COUNT],
        }
    }
}

impl RdmaRemoteBuf {
    /// Create a new remote buffer reference.
    pub fn new(addr: u64, length: u64, rkeys: [Rkey; MAX_DEVICE_COUNT]) -> Self {
        Self {
            addr,
            length,
            rkeys,
        }
    }

    /// Return the remote memory address.
    pub fn addr(&self) -> u64 {
        self.addr
    }

    /// Return the buffer size in bytes.
    pub fn size(&self) -> u64 {
        self.length
    }

    /// Return whether this buffer reference is valid (non-zero address).
    pub fn valid(&self) -> bool {
        self.addr != 0
    }

    /// Get the remote key for a specific device.
    pub fn get_rkey(&self, dev_id: i32) -> Option<u32> {
        self.rkeys
            .iter()
            .find(|rk| rk.dev_id == dev_id)
            .map(|rk| rk.rkey)
    }

    /// Return a reference to the rkeys array.
    pub fn rkeys(&self) -> &[Rkey; MAX_DEVICE_COUNT] {
        &self.rkeys
    }

    /// Advance the buffer start by `len` bytes.
    ///
    /// Returns `false` if `len` exceeds the current size.
    pub fn advance(&mut self, len: u64) -> bool {
        if self.length < len {
            return false;
        }
        self.addr += len;
        self.length -= len;
        true
    }

    /// Shrink the buffer by `n` bytes from the end.
    ///
    /// Returns `false` if `n` exceeds the current size.
    pub fn subtract(&mut self, n: u64) -> bool {
        if n > self.length {
            return false;
        }
        self.length -= n;
        true
    }

    /// Return a sub-range of this buffer.
    pub fn subrange(&self, offset: u64, len: u64) -> Option<Self> {
        if offset + len > self.length {
            return None;
        }
        let mut sub = self.clone();
        sub.addr = self.addr + offset;
        sub.length = len;
        Some(sub)
    }

    /// Return the first `len` bytes as a new remote buffer.
    pub fn first(&self, len: u64) -> Option<Self> {
        self.subrange(0, len)
    }

    /// Return the last `len` bytes as a new remote buffer.
    pub fn last(&self, len: u64) -> Option<Self> {
        if len > self.length {
            return None;
        }
        self.subrange(self.length - len, len)
    }
}

/// A local RDMA buffer.
///
/// Represents a region of memory that has been registered with RDMA devices
/// for use in RDMA operations. Corresponds to the C++ `RDMABuf`.
///
/// Without the `rdma` feature, this is a simple owned byte buffer that can
/// be used for testing and non-RDMA code paths.
#[derive(Debug)]
pub struct RdmaBuf {
    /// The buffer data.
    data: Vec<u8>,
    /// Start offset within the data (for sub-ranges).
    offset: usize,
    /// Length of the active region.
    length: usize,
}

impl RdmaBuf {
    /// Allocate a new RDMA buffer of the given size.
    ///
    /// Without the `rdma` feature, this is a simple heap allocation.
    /// With `rdma`, it would allocate page-aligned memory and register
    /// it with all available IB devices.
    pub fn allocate(size: usize) -> Self {
        Self {
            data: vec![0u8; size],
            offset: 0,
            length: size,
        }
    }

    /// Create an RDMA buffer wrapping an existing user buffer.
    pub fn from_vec(data: Vec<u8>) -> Self {
        let length = data.len();
        Self {
            data,
            offset: 0,
            length,
        }
    }

    /// Return whether this buffer is valid (non-empty).
    pub fn valid(&self) -> bool {
        !self.data.is_empty()
    }

    /// Return a pointer to the start of the active region.
    pub fn ptr(&self) -> *const u8 {
        self.data.as_ptr().wrapping_add(self.offset)
    }

    /// Return a mutable pointer to the start of the active region.
    pub fn ptr_mut(&mut self) -> *mut u8 {
        self.data.as_mut_ptr().wrapping_add(self.offset)
    }

    /// Return the total capacity of the underlying allocation.
    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Return the size of the active region.
    pub fn size(&self) -> usize {
        self.length
    }

    /// Return whether the active region is empty.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Return the active region as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.offset..self.offset + self.length]
    }

    /// Return the active region as a mutable byte slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data[self.offset..self.offset + self.length]
    }

    /// Reset the active range to cover the full allocation.
    pub fn reset_range(&mut self) {
        self.offset = 0;
        self.length = self.data.len();
    }

    /// Advance the start of the active region by `n` bytes.
    pub fn advance(&mut self, n: usize) -> bool {
        if n > self.length {
            return false;
        }
        self.offset += n;
        self.length -= n;
        true
    }

    /// Shrink the active region by `n` bytes from the end.
    pub fn subtract(&mut self, n: usize) -> bool {
        if n > self.length {
            return false;
        }
        self.length -= n;
        true
    }
}

impl Clone for RdmaBuf {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            offset: self.offset,
            length: self.length,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdma_remote_buf_default() {
        let buf = RdmaRemoteBuf::default();
        assert_eq!(buf.addr(), 0);
        assert_eq!(buf.size(), 0);
        assert!(!buf.valid());
    }

    #[test]
    fn test_rdma_remote_buf_new() {
        let mut rkeys = [Rkey::default(); MAX_DEVICE_COUNT];
        rkeys[0] = Rkey {
            rkey: 42,
            dev_id: 0,
        };
        rkeys[1] = Rkey {
            rkey: 99,
            dev_id: 1,
        };

        let buf = RdmaRemoteBuf::new(0x1000, 4096, rkeys);
        assert!(buf.valid());
        assert_eq!(buf.addr(), 0x1000);
        assert_eq!(buf.size(), 4096);
        assert_eq!(buf.get_rkey(0), Some(42));
        assert_eq!(buf.get_rkey(1), Some(99));
        assert_eq!(buf.get_rkey(2), None);
    }

    #[test]
    fn test_rdma_remote_buf_advance() {
        let mut buf = RdmaRemoteBuf::new(0x1000, 4096, [Rkey::default(); MAX_DEVICE_COUNT]);

        assert!(buf.advance(1024));
        assert_eq!(buf.addr(), 0x1400);
        assert_eq!(buf.size(), 3072);

        assert!(!buf.advance(10000)); // Too much.
    }

    #[test]
    fn test_rdma_remote_buf_subrange() {
        let buf = RdmaRemoteBuf::new(0x1000, 4096, [Rkey::default(); MAX_DEVICE_COUNT]);

        let sub = buf.subrange(1024, 2048).unwrap();
        assert_eq!(sub.addr(), 0x1400);
        assert_eq!(sub.size(), 2048);

        assert!(buf.subrange(0, 5000).is_none()); // Out of bounds.
    }

    #[test]
    fn test_rdma_buf_allocate() {
        let buf = RdmaBuf::allocate(4096);
        assert!(buf.valid());
        assert_eq!(buf.size(), 4096);
        assert_eq!(buf.capacity(), 4096);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_rdma_buf_from_vec() {
        let data = vec![1u8, 2, 3, 4, 5];
        let buf = RdmaBuf::from_vec(data);
        assert_eq!(buf.size(), 5);
        assert_eq!(buf.as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_rdma_buf_advance() {
        let mut buf = RdmaBuf::from_vec(vec![10, 20, 30, 40, 50]);
        assert!(buf.advance(2));
        assert_eq!(buf.size(), 3);
        assert_eq!(buf.as_slice(), &[30, 40, 50]);

        assert!(buf.subtract(1));
        assert_eq!(buf.size(), 2);
        assert_eq!(buf.as_slice(), &[30, 40]);
    }

    #[test]
    fn test_rdma_buf_reset_range() {
        let mut buf = RdmaBuf::from_vec(vec![1, 2, 3, 4, 5]);
        buf.advance(2);
        assert_eq!(buf.size(), 3);

        buf.reset_range();
        assert_eq!(buf.size(), 5);
        assert_eq!(buf.as_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_rkey_default() {
        let rk = Rkey::default();
        assert_eq!(rk.rkey, 0);
        assert_eq!(rk.dev_id, -1);
    }
}
