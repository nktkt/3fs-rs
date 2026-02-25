use std::fmt;
use std::ops::{Add, Sub};
use std::time;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A duration wrapper providing convenient conversions.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Duration {
    nanos: u64,
}

impl Duration {
    pub const ZERO: Duration = Duration { nanos: 0 };

    pub fn from_nanos(nanos: u64) -> Self {
        Self { nanos }
    }

    pub fn from_micros(micros: u64) -> Self {
        Self {
            nanos: micros * 1_000,
        }
    }

    pub fn from_millis(millis: u64) -> Self {
        Self {
            nanos: millis * 1_000_000,
        }
    }

    pub fn from_secs(secs: u64) -> Self {
        Self {
            nanos: secs * 1_000_000_000,
        }
    }

    pub fn as_nanos(&self) -> u64 {
        self.nanos
    }

    pub fn as_micros(&self) -> u64 {
        self.nanos / 1_000
    }

    pub fn as_millis(&self) -> u64 {
        self.nanos / 1_000_000
    }

    pub fn as_secs(&self) -> u64 {
        self.nanos / 1_000_000_000
    }

    pub fn as_secs_f64(&self) -> f64 {
        self.nanos as f64 / 1_000_000_000.0
    }
}

impl fmt::Debug for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Duration({}ns)", self.nanos)
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.nanos >= 1_000_000_000 {
            write!(f, "{:.3}s", self.as_secs_f64())
        } else if self.nanos >= 1_000_000 {
            write!(f, "{}ms", self.as_millis())
        } else if self.nanos >= 1_000 {
            write!(f, "{}us", self.as_micros())
        } else {
            write!(f, "{}ns", self.nanos)
        }
    }
}

impl Default for Duration {
    fn default() -> Self {
        Self::ZERO
    }
}

impl From<time::Duration> for Duration {
    fn from(d: time::Duration) -> Self {
        Self {
            nanos: d.as_nanos() as u64,
        }
    }
}

impl From<Duration> for time::Duration {
    fn from(d: Duration) -> Self {
        time::Duration::from_nanos(d.nanos)
    }
}

impl Add for Duration {
    type Output = Duration;
    fn add(self, rhs: Duration) -> Duration {
        Duration {
            nanos: self.nanos + rhs.nanos,
        }
    }
}

impl Sub for Duration {
    type Output = Duration;
    fn sub(self, rhs: Duration) -> Duration {
        Duration {
            nanos: self.nanos.saturating_sub(rhs.nanos),
        }
    }
}

/// A UTC timestamp wrapper around `chrono::DateTime<Utc>`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UtcTime {
    inner: DateTime<Utc>,
}

impl UtcTime {
    /// Get the current UTC time.
    pub fn now() -> Self {
        Self { inner: Utc::now() }
    }

    /// Create from a chrono `DateTime<Utc>`.
    pub fn from_chrono(dt: DateTime<Utc>) -> Self {
        Self { inner: dt }
    }

    /// Access the inner `DateTime<Utc>`.
    pub fn as_chrono(&self) -> &DateTime<Utc> {
        &self.inner
    }

    /// Milliseconds since Unix epoch.
    pub fn timestamp_millis(&self) -> i64 {
        self.inner.timestamp_millis()
    }

    /// Seconds since Unix epoch.
    pub fn timestamp(&self) -> i64 {
        self.inner.timestamp()
    }
}

impl fmt::Debug for UtcTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UtcTime({})", self.inner.to_rfc3339())
    }
}

impl fmt::Display for UtcTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner.to_rfc3339())
    }
}

impl Default for UtcTime {
    fn default() -> Self {
        Self {
            inner: DateTime::<Utc>::default(),
        }
    }
}

impl From<DateTime<Utc>> for UtcTime {
    fn from(dt: DateTime<Utc>) -> Self {
        Self { inner: dt }
    }
}

impl From<UtcTime> for DateTime<Utc> {
    fn from(t: UtcTime) -> Self {
        t.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration_conversions() {
        let d = Duration::from_secs(2);
        assert_eq!(d.as_secs(), 2);
        assert_eq!(d.as_millis(), 2000);
        assert_eq!(d.as_micros(), 2_000_000);
        assert_eq!(d.as_nanos(), 2_000_000_000);
    }

    #[test]
    fn test_duration_display() {
        assert_eq!(format!("{}", Duration::from_secs(1)), "1.000s");
        assert_eq!(format!("{}", Duration::from_millis(500)), "500ms");
        assert_eq!(format!("{}", Duration::from_micros(42)), "42us");
        assert_eq!(format!("{}", Duration::from_nanos(100)), "100ns");
    }

    #[test]
    fn test_duration_add_sub() {
        let a = Duration::from_millis(100);
        let b = Duration::from_millis(50);
        assert_eq!((a + b).as_millis(), 150);
        assert_eq!((a - b).as_millis(), 50);
        // Saturating subtraction
        assert_eq!((b - a).as_millis(), 0);
    }

    #[test]
    fn test_duration_std_roundtrip() {
        let d = Duration::from_millis(1234);
        let std_d: std::time::Duration = d.into();
        let back: Duration = std_d.into();
        assert_eq!(d, back);
    }

    #[test]
    fn test_duration_serde() {
        let d = Duration::from_millis(42);
        let json = serde_json::to_string(&d).unwrap();
        let parsed: Duration = serde_json::from_str(&json).unwrap();
        assert_eq!(d, parsed);
    }

    #[test]
    fn test_utc_time_now() {
        let t = UtcTime::now();
        assert!(t.timestamp() > 0);
    }

    #[test]
    fn test_utc_time_default() {
        let t = UtcTime::default();
        assert_eq!(t.timestamp(), 0); // chrono default: Unix epoch
    }

    #[test]
    fn test_utc_time_display() {
        let t = UtcTime::now();
        let s = format!("{}", t);
        assert!(s.contains("T")); // RFC3339 format
    }

    #[test]
    fn test_utc_time_serde() {
        let t = UtcTime::now();
        let json = serde_json::to_string(&t).unwrap();
        let parsed: UtcTime = serde_json::from_str(&json).unwrap();
        assert_eq!(t, parsed);
    }
}
