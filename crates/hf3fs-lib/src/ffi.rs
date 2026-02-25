//! C FFI bindings for hf3fs.
//!
//! This module provides C-compatible functions that external (non-Rust) code can
//! call. It corresponds to the C header `hf3fs_usrbio.h` and is only compiled
//! when the `ffi` feature is enabled.
//!
//! These are stub implementations that establish the ABI. Full implementations
//! will wire into the hf3fs-client crate.

use crate::usrbio::{Cqe, Ior, Iov};
use crate::HF3FS_SUPER_MAGIC;

/// Check whether a file descriptor belongs to an hf3fs filesystem.
///
/// Returns 1 if yes, 0 if no.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_is_hf3fs(fd: libc::c_int) -> bool {
    // Check via fstatfs and compare f_type with HF3FS_SUPER_MAGIC.
    let mut statfs: libc::statfs = std::mem::zeroed();
    let ret = libc::fstatfs(fd, &mut statfs);
    if ret != 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    {
        statfs.f_type as u32 == HF3FS_SUPER_MAGIC
    }
    #[cfg(not(target_os = "linux"))]
    {
        // On non-Linux, fstatfs.f_type may not be available.
        let _ = statfs;
        false
    }
}

/// Extract the mount point from a path if it resides on an hf3fs instance.
///
/// Returns the length of the mount point + 1 on success, or -1 if the path
/// is not on an hf3fs instance. If the returned length is greater than `size`,
/// the buffer was too small.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_extract_mount_point(
    hf3fs_mount_point: *mut libc::c_char,
    size: libc::c_int,
    _path: *const libc::c_char,
) -> libc::c_int {
    // Stub: always returns -1 (not on hf3fs).
    // Full implementation would parse /proc/mounts to find the mount point.
    let _ = (hf3fs_mount_point, size);
    -1
}

/// Calculate the required memory size for an I/O ring with the given number
/// of entries.
#[no_mangle]
pub extern "C" fn hf3fs_ior_size(entries: libc::c_int) -> libc::size_t {
    // Each entry needs space for an SQE and a CQE.
    // This is a simplified calculation; the real one depends on the ring layout.
    let entry_size = std::mem::size_of::<Cqe>() * 2;
    (entries as usize) * entry_size + 4096 // Extra page for metadata.
}

/// Create an I/O vector (allocate shared memory and register with hf3fs).
///
/// Returns 0 on success, -errno on failure.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_iovcreate(
    iov: *mut Iov,
    _hf3fs_mount_point: *const libc::c_char,
    _size: libc::size_t,
    _block_size: libc::size_t,
    _numa: libc::c_int,
) -> libc::c_int {
    if iov.is_null() {
        return -libc::EINVAL;
    }
    // Stub: not implemented yet.
    -libc::ENOSYS
}

/// Destroy an I/O vector (unregister and free shared memory).
#[no_mangle]
pub unsafe extern "C" fn hf3fs_iovdestroy(iov: *mut Iov) {
    if iov.is_null() {
        return;
    }
    // Stub: would unmap shared memory and deregister.
    (*iov).base = std::ptr::null_mut();
    (*iov).size = 0;
}

/// Create an I/O ring for submitting batched I/O operations.
///
/// Returns 0 on success, -errno on failure.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_iorcreate(
    ior: *mut Ior,
    _hf3fs_mount_point: *const libc::c_char,
    _entries: libc::c_int,
    _for_read: bool,
    _io_depth: libc::c_int,
    _numa: libc::c_int,
) -> libc::c_int {
    if ior.is_null() {
        return -libc::EINVAL;
    }
    // Stub: not implemented yet.
    -libc::ENOSYS
}

/// Destroy an I/O ring.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_iordestroy(ior: *mut Ior) {
    if ior.is_null() {
        return;
    }
    // Stub: would clean up the ring resources.
    (*ior).iorh = std::ptr::null_mut();
}

/// Report the maximum number of entries in the I/O ring.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_io_entries(ior: *const Ior) -> libc::c_int {
    if ior.is_null() {
        return 0;
    }
    // Stub: returns 0.
    0
}

/// Prepare an I/O operation in the ring.
///
/// Returns the I/O index (>= 0) on success, or -errno on failure.
/// This function is NOT thread-safe for the same I/O ring.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_prep_io(
    _ior: *const Ior,
    _iov: *const Iov,
    _read: bool,
    _ptr: *mut libc::c_void,
    _fd: libc::c_int,
    _off: libc::size_t,
    _len: u64,
    _userdata: *const libc::c_void,
) -> libc::c_int {
    // Stub: not implemented yet.
    -libc::ENOSYS
}

/// Submit all prepared I/O operations.
///
/// Returns 0 on success, -errno on failure.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_submit_ios(_ior: *const Ior) -> libc::c_int {
    // Stub: not implemented yet.
    -libc::ENOSYS
}

/// Wait for I/O completions.
///
/// Returns the number of completions (>= 0) or -errno on failure.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_wait_for_ios(
    _ior: *const Ior,
    _cqes: *mut Cqe,
    _cqec: libc::c_int,
    _min_results: libc::c_int,
    _abs_timeout: *const libc::timespec,
) -> libc::c_int {
    // Stub: not implemented yet.
    -libc::ENOSYS
}

/// Register a file descriptor for use with USRBIO.
///
/// Returns 0 on success, errno on error.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_reg_fd(
    _fd: libc::c_int,
    _flags: u64,
) -> libc::c_int {
    // Stub: not implemented yet.
    -libc::ENOSYS
}

/// Deregister a file descriptor from USRBIO.
#[no_mangle]
pub unsafe extern "C" fn hf3fs_dereg_fd(_fd: libc::c_int) {
    // Stub: nothing to do yet.
}
