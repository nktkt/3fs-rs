//! Key prefix constants for the KV store.
//!
//! Each data type stored in the KV engine uses a distinct prefix byte to avoid
//! key collisions. These match the C++ `kv::KeyPrefix` enum values.

/// Prefix byte for inode keys.
pub const INODE_PREFIX: u8 = 0x01;

/// Prefix byte for directory entry keys.
pub const DIR_ENTRY_PREFIX: u8 = 0x02;

/// Prefix byte for file session keys.
pub const FILE_SESSION_PREFIX: u8 = 0x03;

/// Prefix byte for GC (garbage collection) entry keys.
pub const GC_ENTRY_PREFIX: u8 = 0x04;

/// Prefix byte for idempotent record keys.
pub const IDEMPOTENT_PREFIX: u8 = 0x05;

/// Prefix byte for inode ID allocation keys.
pub const INODE_ID_ALLOC_PREFIX: u8 = 0x06;

/// Prefix byte for chain allocation counter keys.
pub const CHAIN_ALLOC_PREFIX: u8 = 0x07;
