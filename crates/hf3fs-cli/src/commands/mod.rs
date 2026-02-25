//! CLI command definitions and handlers.
//!
//! Each subcommand group is in its own module mirroring the C++ command
//! registration structure from `3FS/src/client/cli/admin/registerAdminCommands.cc`.

pub mod admin;
pub mod cluster;
pub mod config;
pub mod meta;
pub mod storage;

pub use admin::AdminCommands;
