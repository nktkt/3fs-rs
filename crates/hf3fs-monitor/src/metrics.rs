use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use crate::distribution::Distribution;

pub struct Counter {
    name: String,
    value: AtomicU64,
}

impl Counter {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: AtomicU64::new(0),
        }
    }

    pub fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub struct Gauge {
    name: String,
    value: AtomicI64,
}

impl Gauge {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: AtomicI64::new(0),
        }
    }

    pub fn set(&self, val: i64) {
        self.value.store(val, Ordering::Relaxed);
    }

    pub fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn decrement(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub struct Histogram {
    name: String,
    values: parking_lot::Mutex<Vec<f64>>,
}

impl Histogram {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            values: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub fn observe(&self, value: f64) {
        self.values.lock().push(value);
    }

    pub fn snapshot(&self) -> Distribution {
        let mut values = self.values.lock().clone();
        if values.is_empty() {
            return Distribution::default();
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let count = values.len() as u64;
        let sum: f64 = values.iter().sum();
        let min = values[0];
        let max = values[values.len() - 1];
        let p50 = percentile(&values, 50.0);
        let p90 = percentile(&values, 90.0);
        let p99 = percentile(&values, 99.0);
        Distribution {
            count,
            sum,
            min,
            max,
            p50,
            p90,
            p99,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (pct / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
