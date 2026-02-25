//! Individual metadata operations.
//!
//! Each operation corresponds to a method on the `MetaStore` and implements
//! the core logic that runs within a KV transaction.

pub mod create;
pub mod hard_link;
pub mod list;
pub mod mkdirs;
pub mod open;
pub mod remove;
pub mod rename;
pub mod set_attr;
pub mod stat;
pub mod symlink;
