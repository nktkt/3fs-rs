//! hf3fs-fuse: FUSE filesystem interface for 3FS.
//!
//! This crate provides the FUSE (Filesystem in Userspace) layer that
//! translates kernel filesystem requests into 3FS metadata and storage
//! operations. It is a Rust port of the C++ implementation in
//! `3FS/src/fuse/`.
//!
//! # Architecture
//!
//! The crate is organized as follows:
//!
//! - **[`config`]** - `FuseConfig` with hot-reload support, mirroring the
//!   C++ `FuseConfig` struct. Includes cache timeouts, I/O parameters,
//!   thread pool sizing, and periodic sync settings.
//!
//! - **[`types`]** - FUSE-specific types (`FileAttr`, `FuseEntryParam`,
//!   `StatFs`, `OpenFlags`, etc.) that abstract the FUSE kernel protocol.
//!
//! - **[`reply`]** - Reply types for each FUSE operation (`ReplyEntry`,
//!   `ReplyData`, `ReplyWrite`, etc.).
//!
//! - **[`ops`]** - The `FuseOps` trait with all FUSE operations (init,
//!   lookup, getattr, read, write, etc.). Default implementations
//!   return `ENOSYS`.
//!
//! - **[`inode`]** - In-memory inode state management (`Inode`, `RcInode`,
//!   `InodeTable`, `FileHandle`, `DirHandle`, `HandleTable`).
//!
//! - **[`filesystem`]** - `FuseFileSystem`, the main struct implementing
//!   `FuseOps`. It holds an `Arc<dyn FuseClient>` for communicating with
//!   meta/storage services, plus the inode table and handle table.
//!
//! # Usage
//!
//! The `FuseOps` trait is designed to be backed by the `fuser` crate for
//! actual FUSE kernel communication. Since we define the interface as a
//! Rust trait, the implementation can be tested without a real FUSE mount.
//!
//! ```rust,no_run
//! use hf3fs_fuse::config::FuseConfig;
//! use hf3fs_fuse::filesystem::{FuseFileSystem, FuseClient};
//! use hf3fs_fuse::ops::FuseOps;
//!
//! // Create a FuseFileSystem with your client implementation:
//! // let client: Arc<dyn FuseClient> = ...;
//! // let config = FuseConfig::default();
//! // let fs = FuseFileSystem::new(client, config, token);
//! // Then pass `fs` to a fuser::Session or similar transport.
//! ```

pub mod config;
pub mod filesystem;
pub mod inode;
pub mod ops;
pub mod reply;
pub mod types;

// Re-export key types at the crate level for convenience.
pub use config::FuseConfig;
pub use filesystem::{FuseClient, FuseError, FuseFileSystem};
pub use ops::FuseOps;
pub use reply::FuseResult;
pub use types::{FileAttr, FuseEntryParam, FuseRequestContext, StatFs};
