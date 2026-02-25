//! hf3fs library for external consumers.
//!
//! This crate provides the public API for interacting with hf3fs from external
//! code. It includes:
//!
//! - Re-exports of core types from `hf3fs-types`.
//! - C FFI bindings (feature-gated behind "ffi") for non-Rust consumers.
//! - USRBIO (user-space block I/O) types matching the C header `hf3fs_usrbio.h`.
//!
//! The C++ equivalent is `src/lib/api/hf3fs.h` and `hf3fs_usrbio.h`.

pub mod types;

#[cfg(feature = "ffi")]
pub mod ffi;

pub mod usrbio;

// Re-export core types for convenience.
pub use hf3fs_types::{Address, AddressType, Status, StatusCode};

/// The hf3fs super magic number, used to identify hf3fs filesystems.
///
/// Corresponds to `HF3FS_SUPER_MAGIC` in the C header.
pub const HF3FS_SUPER_MAGIC: u32 = 0x8f3f5fff;
