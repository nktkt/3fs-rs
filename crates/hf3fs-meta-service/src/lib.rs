//! hf3fs-meta-service: Metadata service for the 3FS distributed file system.
//!
//! This crate implements the metadata service that manages the file system
//! namespace, including inodes, directory entries, and path resolution.
//! It is backed by a transactional KV store (e.g. FoundationDB).
//!
//! Architecture:
//! - [`MetaStore`] - low-level metadata storage layer that operates within KV transactions
//! - [`MetaService`] - high-level async service trait defining all metadata operations
//! - [`MetaServiceImpl`] - concrete implementation that wraps MetaStore with retry/transaction logic
//! - [`inode::Inode`] - server-side inode with KV store serialization
//! - [`dir_entry::DirEntry`] - server-side directory entry with KV store serialization

pub mod config;
pub mod dir_entry;
pub mod inode;
pub mod key_prefix;
pub mod meta_store;
pub mod ops;
pub mod path_resolve;
pub mod service;

pub use config::MetaServiceConfig;
pub use meta_store::MetaStore;
pub use service::{MetaService, MetaServiceImpl};
