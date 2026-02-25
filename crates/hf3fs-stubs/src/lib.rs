//! Client-side RPC stub trait definitions for hf3fs services.
//!
//! Each stub trait defines the async interface for calling a remote service.
//! Concrete implementations (backed by real network connections) live in
//! `hf3fs-client`. This crate also provides mock implementations that are
//! useful for unit testing without a running server.

pub mod core_stub;
pub mod meta_stub;
pub mod mgmtd_stub;
pub mod storage_stub;

pub use core_stub::{ICoreServiceStub, MockCoreServiceStub};
pub use meta_stub::{IMetaServiceStub, MockMetaServiceStub};
pub use mgmtd_stub::{IMgmtdServiceStub, MockMgmtdServiceStub};
pub use storage_stub::{IStorageServiceStub, MockStorageServiceStub};
