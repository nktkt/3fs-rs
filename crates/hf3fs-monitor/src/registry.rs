use dashmap::DashMap;
use std::sync::Arc;

use crate::metrics::{Counter, Gauge, Histogram};
use crate::sample::Sample;

pub struct MetricsRegistry {
    counters: DashMap<String, Arc<Counter>>,
    gauges: DashMap<String, Arc<Gauge>>,
    histograms: DashMap<String, Arc<Histogram>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
            gauges: DashMap::new(),
            histograms: DashMap::new(),
        }
    }

    pub fn counter(&self, name: &str) -> Arc<Counter> {
        self.counters
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Counter::new(name)))
            .value()
            .clone()
    }

    pub fn gauge(&self, name: &str) -> Arc<Gauge> {
        self.gauges
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Gauge::new(name)))
            .value()
            .clone()
    }

    pub fn histogram(&self, name: &str) -> Arc<Histogram> {
        self.histograms
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Histogram::new(name)))
            .value()
            .clone()
    }

    pub fn collect(&self) -> Vec<Sample> {
        let mut samples = Vec::new();

        for entry in self.counters.iter() {
            let counter = entry.value();
            samples.push(Sample::new(counter.name(), counter.get() as f64));
        }

        for entry in self.gauges.iter() {
            let gauge = entry.value();
            samples.push(Sample::new(gauge.name(), gauge.get() as f64));
        }

        for entry in self.histograms.iter() {
            let histogram = entry.value();
            let dist = histogram.snapshot();
            let name = histogram.name();
            samples.push(Sample::new(format!("{}.count", name), dist.count as f64));
            samples.push(Sample::new(format!("{}.sum", name), dist.sum));
            samples.push(Sample::new(format!("{}.min", name), dist.min));
            samples.push(Sample::new(format!("{}.max", name), dist.max));
            samples.push(Sample::new(format!("{}.p50", name), dist.p50));
            samples.push(Sample::new(format!("{}.p90", name), dist.p90));
            samples.push(Sample::new(format!("{}.p99", name), dist.p99));
        }

        samples
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}
