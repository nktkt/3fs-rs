//! 3FS CLI (Command Line Interface) utilities.
//!
//! This crate provides the command-line interface for administering a 3FS cluster.
//! It mirrors the C++ CLI tools from `3FS/src/client/cli/` and `3FS/src/tools/`,
//! organized into logical command groups using clap's derive macros.
//!
//! # Architecture
//!
//! The CLI is structured as:
//!
//! - **[`commands::AdminCommands`]** -- Top-level command enum dispatching to subgroups.
//! - **[`commands::config`]** -- Configuration management (get, set, validate, hot-update).
//! - **[`commands::cluster`]** -- Cluster operations (list-nodes, register/unregister, init).
//! - **[`commands::storage`]** -- Storage operations (targets, chains, chunks, checksums).
//! - **[`commands::meta`]** -- Metadata/filesystem operations (stat, ls, mkdir, rm, etc.).
//!
//! Supporting modules:
//!
//! - **[`output`]** -- Output formatting (aligned tables, JSON).
//! - **[`connection`]** -- Connection helpers and admin environment (`AdminEnv`).
//! - **[`progress`]** -- Progress bars and timing utilities.
//!
//! # Usage
//!
//! This crate is used by the `hf3fs-admin` binary. Typical usage:
//!
//! ```ignore
//! use clap::Parser;
//! use hf3fs_cli::commands::AdminCommands;
//! use hf3fs_cli::connection::{AdminEnv, ConnectionOptions};
//! use hf3fs_cli::output::OutputFormat;
//!
//! #[derive(Parser)]
//! struct Cli {
//!     #[command(flatten)]
//!     connection: ConnectionOptions,
//!
//!     #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
//!     format: OutputFormat,
//!
//!     #[command(subcommand)]
//!     command: AdminCommands,
//! }
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let cli = Cli::parse();
//!     let mut env = AdminEnv::new(cli.connection);
//!     cli.command.run(&mut env, cli.format).await
//! }
//! ```
//!
//! # C++ Source Mapping
//!
//! | Rust Module | C++ Source |
//! |---|---|
//! | `commands::config` | `admin/GetConfig.cc`, `admin/SetConfig.cc`, `admin/VerifyConfig.cc`, `admin/HotUpdateConfig.cc`, `admin/RenderConfig.cc` |
//! | `commands::cluster` | `admin/ListNodes.cc`, `admin/RegisterNode.cc`, `admin/UnregisterNode.cc`, `admin/InitCluster.cc`, `admin/ListClients.cc` |
//! | `commands::storage` | `admin/ListTargets.cc`, `admin/ListChains.cc`, `admin/QueryChunk.cc`, `admin/Checksum.cc`, etc. |
//! | `commands::meta` | `admin/Stat.cc`, `admin/List.cc`, `admin/Mkdir.cc`, `admin/Remove.cc`, `admin/Rename.cc`, etc. |
//! | `output` | `common/Printer.cc` |
//! | `connection` | `admin/AdminEnv.h`, `common/IEnv.h` |

pub mod commands;
pub mod connection;
pub mod output;
pub mod progress;

// Re-export the main types for convenience.
pub use commands::AdminCommands;
pub use connection::{AdminEnv, ConnectionOptions};
pub use output::{OutputFormat, OutputTable, Printer};
