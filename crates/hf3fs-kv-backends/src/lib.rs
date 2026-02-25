//! KV store backend implementations for hf3fs.
//!
//! Provides concrete implementations of the [`hf3fs_kv::KvEngine`] trait:
//!
//! - **memdb** -- In-memory BTreeMap-backed store (always available).
//! - **rocksdb** -- RocksDB backend (feature-gated behind `"rocksdb"`).
//! - **leveldb** -- LevelDB backend (feature-gated behind `"leveldb"`).

pub mod memdb;

#[cfg(feature = "rocksdb")]
pub mod rocksdb;

#[cfg(feature = "leveldb")]
pub mod leveldb;

// Re-export the primary in-memory engine for convenience.
pub use memdb::MemDbEngine;
