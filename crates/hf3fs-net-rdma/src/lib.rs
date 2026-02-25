//! RDMA transport for hf3fs networking.
//!
//! This crate provides RDMA (Remote Direct Memory Access) transport types that
//! implement the `Socket`, `Listener`, and `AsyncConnector` traits from
//! `hf3fs-net`. It is modeled after the C++ `IBSocket`, `IBDevice`, `IBManager`,
//! and `RDMABuf` classes from `src/common/net/ib/`.
//!
//! Actual RDMA functionality requires the `rdma` feature flag and the `libibverbs`
//! library to be installed. Without this feature, only type definitions and stub
//! implementations are available, allowing the rest of the codebase to compile
//! and be tested without RDMA hardware.
//!
//! # Architecture
//!
//! - `RdmaConfig`: Configuration for RDMA connections (QP parameters, buffer sizes, etc.)
//! - `RdmaDevice`: Represents an InfiniBand device (wraps `ibv_context` / `ibv_pd`).
//! - `RdmaConnection`: An RDMA-based socket implementing `hf3fs_net::Socket`.
//! - `RdmaListener`: Accepts incoming RDMA connections.
//! - `RdmaBuf` / `RdmaRemoteBuf`: RDMA memory buffer types for zero-copy I/O.

pub mod buf;
pub mod config;
pub mod connection;
pub mod device;
pub mod listener;

pub use buf::{RdmaBuf, RdmaRemoteBuf};
pub use config::RdmaConfig;
pub use connection::RdmaConnection;
pub use device::RdmaDevice;
pub use listener::RdmaListener;
