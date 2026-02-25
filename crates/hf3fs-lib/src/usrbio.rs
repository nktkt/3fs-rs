//! User-space block I/O (USRBIO) types.
//!
//! Corresponds to the C header `hf3fs_usrbio.h` which defines the user-space
//! block I/O interface for direct I/O to hf3fs storage nodes. These types are
//! used for the high-performance data path that bypasses the FUSE layer.

/// Handle to an I/O vector (shared memory region registered with hf3fs).
///
/// Corresponds to `hf3fs_iov_handle` in the C header.
pub type IovHandle = *mut std::ffi::c_void;

/// An I/O vector descriptor for USRBIO operations.
///
/// Corresponds to `struct hf3fs_iov` in the C header. Represents a region
/// of memory (typically shared memory) registered with hf3fs for zero-copy I/O.
#[repr(C)]
pub struct Iov {
    /// Pointer to the base of the memory region.
    pub base: *mut u8,
    /// Opaque handle for the I/O vector.
    pub iovh: IovHandle,
    /// Unique identifier for this I/O vector (16 bytes).
    pub id: [u8; 16],
    /// Mount point path.
    pub mount_point: [u8; 256],
    /// Total size of the memory region in bytes.
    pub size: usize,
    /// Block size for alignment.
    pub block_size: usize,
    /// NUMA node affinity (-1 for any).
    pub numa: i32,
}

/// Handle to an I/O ring for submitting and reaping I/O operations.
///
/// Corresponds to `hf3fs_ior_handle` in the C header.
pub type IorHandle = *mut std::ffi::c_void;

/// An I/O ring descriptor for USRBIO operations.
///
/// Corresponds to `struct hf3fs_ior` in the C header. Used for submitting
/// batched I/O operations to hf3fs and collecting results.
#[repr(C)]
pub struct Ior {
    /// The underlying I/O vector for this ring.
    pub iov: Iov,
    /// Opaque handle for the I/O ring.
    pub iorh: IorHandle,
    /// Mount point path.
    pub mount_point: [u8; 256],
    /// Whether this ring is for read operations.
    pub for_read: bool,
    /// Number of I/Os to process per batch.
    ///
    /// - `> 0`: Process exactly this many I/Os per submission.
    /// - `== 0`: Process all prepared I/Os ASAP.
    /// - `< 0`: Process up to `-io_depth` I/Os per batch.
    pub io_depth: i32,
    /// I/O priority level.
    pub priority: i32,
    /// Timeout in milliseconds.
    pub timeout: i32,
    /// Flags (e.g., `HF3FS_IOR_ALLOW_READ_UNCOMMITTED`).
    pub flags: u64,
}

/// A completion queue entry returned after I/O completes.
///
/// Corresponds to `struct hf3fs_cqe` in the C header.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Cqe {
    /// Index of the completed I/O operation.
    pub index: i32,
    /// Reserved for future use.
    pub reserved: i32,
    /// Result of the I/O operation (bytes transferred, or negative errno).
    pub result: i64,
    /// User data pointer associated with this I/O operation.
    pub userdata: *const std::ffi::c_void,
}

/// Flag: allow reading uncommitted data.
pub const HF3FS_IOR_ALLOW_READ_UNCOMMITTED: u64 = 1;

/// Flag: forbid reading holes (sparse file regions).
pub const HF3FS_IOR_FORBID_READ_HOLES: u64 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cqe_layout() {
        // Verify the CQE is the expected size for C interop.
        assert_eq!(
            std::mem::size_of::<Cqe>(),
            std::mem::size_of::<i32>()       // index
                + std::mem::size_of::<i32>()  // reserved
                + std::mem::size_of::<i64>()  // result
                + std::mem::size_of::<*const std::ffi::c_void>() // userdata
        );
    }

    #[test]
    fn test_flags() {
        assert_eq!(HF3FS_IOR_ALLOW_READ_UNCOMMITTED, 1);
        assert_eq!(HF3FS_IOR_FORBID_READ_HOLES, 2);
    }
}
