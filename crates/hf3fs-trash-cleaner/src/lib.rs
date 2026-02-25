//! Trash cleaner for hf3fs.
//!
//! Provides the logic for scanning trash directories and removing expired items.
//! This is a port of the C++ `src/client/trash_cleaner/` which was already written
//! in Rust in the original 3FS codebase. This crate extracts the core logic into
//! a library (as opposed to the binary in the original).
//!
//! Trash directories follow a naming convention:
//!   `<prefix>-<begin_time>-<expire_time>`
//! where times are formatted as `YYYYMMDD_HHMM`. An item is considered expired
//! when `now > expire_time`.

pub mod config;
pub mod item;
pub mod scanner;

pub use config::TrashCleanerConfig;
pub use item::{TrashItem, TrashItemStatus};
pub use scanner::TrashScanner;
