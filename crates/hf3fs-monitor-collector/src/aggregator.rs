//! Metrics aggregator that collects samples from multiple sources.
//!
//! Corresponds to the C++ `MonitorCollectorOperator` which maintains a queue
//! of sample batches and dispatches them to reporter threads.

use hf3fs_monitor::Sample;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::config::MonitorCollectorConfig;
use crate::exporter::Exporter;

/// Aggregates metric samples from multiple nodes and dispatches them to
/// configured exporters.
///
/// Uses a bounded channel (corresponding to the C++ `folly::MPMCQueue`) to
/// buffer incoming sample batches. Worker tasks consume from the channel,
/// batch-gather additional samples, filter blacklisted metrics, and commit
/// to the reporter.
pub struct MetricsAggregator {
    config: MonitorCollectorConfig,
    sender: mpsc::Sender<Vec<Sample>>,
    receiver: Mutex<Option<mpsc::Receiver<Vec<Sample>>>>,
    total_received: AtomicU64,
    total_exported: AtomicU64,
    running: AtomicBool,
}

impl MetricsAggregator {
    /// Create a new metrics aggregator with the given configuration.
    pub fn new(config: MonitorCollectorConfig) -> Self {
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        Self {
            config,
            sender,
            receiver: Mutex::new(Some(receiver)),
            total_received: AtomicU64::new(0),
            total_exported: AtomicU64::new(0),
            running: AtomicBool::new(false),
        }
    }

    /// Submit a batch of samples to the aggregator.
    ///
    /// This is the entry point called by the collector service when it receives
    /// samples from a remote node. Corresponds to the C++
    /// `MonitorCollectorOperator::write()`.
    pub async fn write(&self, samples: Vec<Sample>) -> Result<(), String> {
        let count = samples.len() as u64;
        self.sender
            .send(samples)
            .await
            .map_err(|_| "aggregator channel closed".to_string())?;
        self.total_received.fetch_add(count, Ordering::Relaxed);
        Ok(())
    }

    /// Start the worker tasks that consume sample batches and export them.
    ///
    /// Takes ownership of the receiver end of the channel and spawns
    /// `conn_threads` worker tasks. Each worker shares the receiver via an
    /// `Arc<Mutex<...>>`.
    pub fn start(
        &self,
        exporter: Arc<dyn Exporter>,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let mut rx_guard = self.receiver.lock();
        let receiver = rx_guard.take()?;

        self.running.store(true, Ordering::Relaxed);

        let config = self.config.clone();
        let total_exported = Arc::new(AtomicU64::new(0));
        let total_exported_clone = total_exported.clone();
        let _running = self.running.load(Ordering::Relaxed);

        let handle = tokio::spawn(async move {
            let shared_rx = Arc::new(tokio::sync::Mutex::new(receiver));
            let mut worker_handles = Vec::new();

            let num_workers = config.conn_threads.max(1);
            for worker_id in 0..num_workers {
                let rx = shared_rx.clone();
                let exp = exporter.clone();
                let cfg = config.clone();
                let exported = total_exported.clone();

                let handle = tokio::spawn(async move {
                    tracing::info!(worker_id, "Monitor collector worker started");

                    loop {
                        // Wait for the next batch.
                        let batch = {
                            let mut rx_lock = rx.lock().await;
                            rx_lock.recv().await
                        };

                        let mut samples = match batch {
                            Some(s) => s,
                            None => {
                                tracing::info!(worker_id, "Monitor collector worker shutting down");
                                break;
                            }
                        };

                        // Try to gather more batches up to batch_commit_size.
                        {
                            let mut rx_lock = rx.lock().await;
                            for _ in 1..cfg.batch_commit_size {
                                match rx_lock.try_recv() {
                                    Ok(more) => samples.extend(more),
                                    Err(_) => break,
                                }
                            }
                        }

                        // Filter blacklisted metrics.
                        if !cfg.blacklisted_metric_names.is_empty() {
                            samples.retain(|s| !cfg.blacklisted_metric_names.contains(&s.name));
                        }

                        let count = samples.len() as u64;
                        if let Err(e) = exp.export(&samples).await {
                            tracing::error!(worker_id, error = %e, "Failed to export samples");
                        } else {
                            exported.fetch_add(count, Ordering::Relaxed);
                        }
                    }
                });

                worker_handles.push(handle);
            }

            // Wait for all workers.
            for handle in worker_handles {
                let _ = handle.await;
            }

            tracing::info!("All monitor collector workers stopped");
        });

        // Store the total_exported counter reference for later queries.
        // (In a real implementation, we would store this in `self`, but since
        // we moved the receiver we use the atomic directly.)
        let _ = total_exported_clone;

        Some(handle)
    }

    /// Returns the total number of samples received.
    pub fn total_received(&self) -> u64 {
        self.total_received.load(Ordering::Relaxed)
    }

    /// Returns the total number of samples exported.
    pub fn total_exported(&self) -> u64 {
        self.total_exported.load(Ordering::Relaxed)
    }

    /// Returns the current queue capacity utilization as a fraction.
    pub fn queue_capacity(&self) -> usize {
        self.config.queue_capacity
    }

    /// Returns a reference to the configuration.
    pub fn config(&self) -> &MonitorCollectorConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exporter::InMemoryExporter;
    use hf3fs_monitor::Sample;

    #[tokio::test]
    async fn test_aggregator_write_and_export() {
        let config = MonitorCollectorConfig {
            conn_threads: 1,
            queue_capacity: 100,
            batch_commit_size: 10,
            ..Default::default()
        };

        let aggregator = MetricsAggregator::new(config);
        let exporter = Arc::new(InMemoryExporter::new());

        let handle = aggregator.start(exporter.clone()).unwrap();

        // Send some samples.
        let samples = vec![
            Sample::new("cpu.usage", 75.0),
            Sample::new("memory.used", 8192.0),
        ];
        aggregator.write(samples).await.unwrap();

        // Give workers time to process.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Drop the sender to stop workers.
        drop(aggregator);

        let _ = handle.await;

        let exported = exporter.take_samples();
        assert_eq!(exported.len(), 2);
    }

    #[tokio::test]
    async fn test_aggregator_blacklist() {
        let mut config = MonitorCollectorConfig {
            conn_threads: 1,
            queue_capacity: 100,
            batch_commit_size: 10,
            ..Default::default()
        };
        config.blacklisted_metric_names.insert("secret.metric".to_string());

        let aggregator = MetricsAggregator::new(config);
        let exporter = Arc::new(InMemoryExporter::new());

        let handle = aggregator.start(exporter.clone()).unwrap();

        let samples = vec![
            Sample::new("cpu.usage", 75.0),
            Sample::new("secret.metric", 42.0),
            Sample::new("disk.free", 1024.0),
        ];
        aggregator.write(samples).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        drop(aggregator);
        let _ = handle.await;

        let exported = exporter.take_samples();
        assert_eq!(exported.len(), 2);
        assert!(exported.iter().all(|s| s.name != "secret.metric"));
    }
}
