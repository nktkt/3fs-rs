//! Trash item parsing and status checking.
//!
//! Trash items are directories following the naming convention:
//!   `<prefix>-<begin_time>-<expire_time>`
//! where times are formatted as `YYYYMMDD_HHMM`.
//!
//! This corresponds to the C++ `Trash::check_item()` logic.

use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use serde::Serialize;

/// The status of a trash item after checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrashItemStatus {
    /// The item has expired and should be cleaned.
    Expired,
    /// The item has not yet expired.
    NotExpired,
    /// The item name could not be parsed (not a valid trash item).
    Invalid(String),
}

/// A parsed trash item.
#[derive(Debug, Clone, Serialize)]
pub struct TrashItem {
    /// The original directory name.
    pub name: String,
    /// The prefix portion of the name.
    pub prefix: String,
    /// When the item was created (begin time).
    pub begin_time: DateTime<FixedOffset>,
    /// When the item expires.
    pub expire_time: DateTime<FixedOffset>,
}

impl TrashItem {
    /// Parse a trash item from a directory name.
    ///
    /// Expected format: `<prefix>-<YYYYMMDD_HHMM>-<YYYYMMDD_HHMM>`
    ///
    /// Returns `Ok(TrashItem)` on success, or `Err(reason)` if the name
    /// cannot be parsed.
    pub fn parse(name: &str) -> Result<Self, String> {
        let parts: Vec<&str> = name.split('-').collect();
        if parts.len() != 3 {
            return Err("invalid name: expected 3 parts separated by '-'".to_string());
        }

        let prefix = parts[0].to_string();
        let begin_time =
            parse_datetime(parts[1]).ok_or_else(|| "invalid begin timestamp".to_string())?;
        let expire_time =
            parse_datetime(parts[2]).ok_or_else(|| "invalid expire timestamp".to_string())?;

        if begin_time > expire_time {
            return Err(format!(
                "invalid timestamp: begin {} > expire {}",
                begin_time, expire_time
            ));
        }

        Ok(Self {
            name: name.to_string(),
            prefix,
            begin_time,
            expire_time,
        })
    }

    /// Check whether this item has expired.
    pub fn check(&self) -> TrashItemStatus {
        let now = Utc::now();
        if now > self.expire_time {
            TrashItemStatus::Expired
        } else {
            TrashItemStatus::NotExpired
        }
    }

    /// Check a directory entry name and return its status.
    ///
    /// This combines parsing and expiry checking, similar to the C++
    /// `Trash::check_item()`.
    pub fn check_name(name: &str) -> TrashItemStatus {
        match Self::parse(name) {
            Ok(item) => item.check(),
            Err(reason) => TrashItemStatus::Invalid(reason),
        }
    }
}

/// Parse a datetime string in `YYYYMMDD_HHMM` format.
///
/// Returns `None` if the string cannot be parsed. The timezone is assumed
/// to be UTC+8, matching the original C++ implementation.
pub fn parse_datetime(s: &str) -> Option<DateTime<FixedOffset>> {
    let naive = NaiveDateTime::parse_from_str(s, "%Y%m%d_%H%M").ok()?;
    let utc8 = FixedOffset::east_opt(8 * 3600)?;
    Some(naive.and_local_timezone(utc8).single()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_datetime_valid() {
        let dt = parse_datetime("20240801_0000").unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 8);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 0);
        assert_eq!(dt.minute(), 0);
    }

    #[test]
    fn test_parse_datetime_invalid() {
        assert!(parse_datetime("invalid").is_none());
        assert!(parse_datetime("2024-08-01").is_none());
        assert!(parse_datetime("").is_none());
    }

    #[test]
    fn test_trash_item_parse_valid() {
        let item = TrashItem::parse("1d-20240801_0000-20240802_0100").unwrap();
        assert_eq!(item.prefix, "1d");
        assert_eq!(item.begin_time.year(), 2024);
        assert_eq!(item.expire_time.month(), 8);
        assert_eq!(item.expire_time.day(), 2);
    }

    #[test]
    fn test_trash_item_parse_invalid_name() {
        assert!(TrashItem::parse("invalid-name").is_err());
        assert!(TrashItem::parse("too-many-parts-here").is_err());
        assert!(TrashItem::parse("single").is_err());
    }

    #[test]
    fn test_trash_item_parse_begin_after_expire() {
        let result = TrashItem::parse("1d-20240802_0000-20240801_0000");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("begin"));
    }

    #[test]
    fn test_trash_item_expired() {
        // An item that expired in 2020 should be expired.
        let item = TrashItem::parse("1d-20200101_0000-20200102_0000").unwrap();
        assert_eq!(item.check(), TrashItemStatus::Expired);
    }

    #[test]
    fn test_trash_item_not_expired() {
        // An item that expires far in the future.
        let item = TrashItem::parse("1d-20200101_0000-20990101_0000").unwrap();
        assert_eq!(item.check(), TrashItemStatus::NotExpired);
    }

    #[test]
    fn test_check_name_invalid() {
        let status = TrashItem::check_name("not-a-valid-item");
        assert!(matches!(status, TrashItemStatus::Invalid(_)));
    }

    #[test]
    fn test_check_name_expired() {
        let status = TrashItem::check_name("1d-20200101_0000-20200102_0000");
        assert_eq!(status, TrashItemStatus::Expired);
    }

    #[test]
    fn test_check_name_not_expired() {
        let status = TrashItem::check_name("nd-20200101_0000-20991231_2359");
        assert_eq!(status, TrashItemStatus::NotExpired);
    }

    // Use chrono Datelike/Timelike traits for the test assertions.
    use chrono::{Datelike, Timelike};
}
