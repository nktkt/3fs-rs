//! Exponential back-off retry logic.
//!
//! Mirrors the C++ `ExponentialBackoffRetry` used in `MetaClient::retry`.

use std::time::{Duration, Instant};

/// Tracks exponential back-off state for a sequence of retries.
///
/// Each call to `next_wait` doubles the wait time (capped at `max_wait`)
/// and returns `None` once the total elapsed time exceeds `total_budget`.
pub struct ExponentialBackoff {
    init_wait: Duration,
    max_wait: Duration,
    total_budget: Duration,
    current_wait: Duration,
    start: Instant,
    attempts: u32,
}

impl ExponentialBackoff {
    /// Create a new back-off tracker.
    pub fn new(init_wait: Duration, max_wait: Duration, total_budget: Duration) -> Self {
        Self {
            init_wait,
            max_wait,
            total_budget,
            current_wait: init_wait,
            start: Instant::now(),
            attempts: 0,
        }
    }

    /// Return the next wait duration, or `None` if the budget is exhausted.
    ///
    /// Increments the attempt counter.
    pub fn next_wait(&mut self) -> Option<Duration> {
        let elapsed = self.start.elapsed();
        if elapsed >= self.total_budget {
            return None;
        }

        let wait = self.current_wait;
        self.current_wait = (self.current_wait * 2).min(self.max_wait);
        self.attempts += 1;

        // Clamp so we do not exceed the budget.
        let remaining = self.total_budget.saturating_sub(elapsed);
        Some(wait.min(remaining))
    }

    /// Return a fast-retry wait (capped at `init_wait`).
    ///
    /// Used for transient errors like `kBusy` where we want to retry
    /// sooner than the normal exponential schedule.
    pub fn fast_wait(&mut self) -> Option<Duration> {
        let elapsed = self.start.elapsed();
        if elapsed >= self.total_budget {
            return None;
        }

        self.attempts += 1;
        let wait = self.init_wait.min(Duration::from_secs(1));
        let remaining = self.total_budget.saturating_sub(elapsed);
        Some(wait.min(remaining))
    }

    /// Reset the back-off wait to the initial value (e.g. after a failover).
    pub fn reset_wait(&mut self) {
        self.current_wait = self.init_wait;
    }

    /// Return how many attempts have been made so far.
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Return total elapsed time since creation.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Return whether the total time budget has been exceeded.
    pub fn is_exhausted(&self) -> bool {
        self.start.elapsed() >= self.total_budget
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff_basic() {
        let mut bo = ExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_secs(2),
            Duration::from_secs(60),
        );
        assert_eq!(bo.attempts(), 0);

        let w1 = bo.next_wait().unwrap();
        assert_eq!(w1, Duration::from_millis(100));
        assert_eq!(bo.attempts(), 1);

        let w2 = bo.next_wait().unwrap();
        assert_eq!(w2, Duration::from_millis(200));

        let w3 = bo.next_wait().unwrap();
        assert_eq!(w3, Duration::from_millis(400));

        let w4 = bo.next_wait().unwrap();
        assert_eq!(w4, Duration::from_millis(800));

        let w5 = bo.next_wait().unwrap();
        assert_eq!(w5, Duration::from_millis(1600));

        // Should cap at max_wait = 2s.
        let w6 = bo.next_wait().unwrap();
        assert_eq!(w6, Duration::from_secs(2));
    }

    #[test]
    fn test_exponential_backoff_exhaustion() {
        let mut bo = ExponentialBackoff::new(
            Duration::from_millis(10),
            Duration::from_millis(50),
            Duration::from_millis(100),
        );
        // Consume all retry budget.
        let mut count = 0;
        while bo.next_wait().is_some() {
            count += 1;
            if count > 1000 {
                break;
            }
            // Simulate some elapsed time by just spinning.
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(bo.is_exhausted());
    }

    #[test]
    fn test_fast_wait() {
        let mut bo = ExponentialBackoff::new(
            Duration::from_millis(500),
            Duration::from_secs(5),
            Duration::from_secs(60),
        );
        let fw = bo.fast_wait().unwrap();
        // fast_wait caps at min(init_wait, 1s), so 500ms.
        assert_eq!(fw, Duration::from_millis(500));
        assert_eq!(bo.attempts(), 1);
    }

    #[test]
    fn test_reset_wait() {
        let mut bo = ExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_secs(2),
            Duration::from_secs(60),
        );
        // Advance a few steps.
        bo.next_wait();
        bo.next_wait();
        bo.next_wait();

        bo.reset_wait();

        // After reset, next wait should be back to init_wait.
        let w = bo.next_wait().unwrap();
        assert_eq!(w, Duration::from_millis(100));
    }
}
