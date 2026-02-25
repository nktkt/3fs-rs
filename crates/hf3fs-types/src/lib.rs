#[allow(non_snake_case)]
pub mod status_code;

pub mod status;
pub mod result;

#[macro_use]
pub mod strong_type;

pub mod ids;
pub mod address;
pub mod time;

// Re-export commonly used items at the crate root.
pub use address::{Address, AddressType};
pub use ids::*;
pub use result::{Result, Void, make_error, make_error_msg};
pub use status::Status;
pub use status_code::*;
pub use time::{Duration, UtcTime};
