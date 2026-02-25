//! Client-side API for connecting to and interacting with 3FS services.
//!
//! Provides `MetaClient`, `StorageClient`, `MgmtdClient`, and the top-level
//! `Client` that composes them all. Each client is defined as a trait (for
//! testability / mocking) together with a concrete implementation that
//! communicates over the network layer from `hf3fs-net`.

pub mod config;
pub mod error;
pub mod meta;
pub mod mgmtd;
pub mod routing;
pub mod retry;
pub mod storage;
pub mod client;

pub use client::Client;
pub use config::ClientConfig;
pub use error::ClientError;
pub use meta::{MetaClient, MetaClientImpl};
pub use mgmtd::{MgmtdClient, MgmtdClientImpl};
pub use routing::RoutingInfo;
pub use storage::{StorageClient, StorageClientImpl};
