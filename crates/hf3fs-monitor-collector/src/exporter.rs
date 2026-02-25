//! Exporter backends for the monitor collector.
//!
//! Corresponds to the C++ reporter pattern where collected samples are committed
//! to a backend (ClickHouse, log, etc.). This Rust version provides:
//!
//! - `LogExporter`: Logs samples via `tracing` (like the C++ `LogReporter`).
//! - `FileExporter`: Writes samples to JSON-lines files.
//! - `InMemoryExporter`: Stores samples in memory for testing.

use async_trait::async_trait;
use hf3fs_monitor::Sample;
use parking_lot::Mutex;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Trait for exporting collected metrics samples to a backend.
///
/// The C++ equivalent is the `Reporter` interface with `init()` and `commit()`
/// methods.
#[async_trait]
pub trait Exporter: Send + Sync {
    /// Export a batch of samples.
    async fn export(&self, samples: &[Sample]) -> Result<(), String>;
}

/// Exports samples by logging them via tracing.
///
/// Corresponds to the C++ `LogReporter`.
pub struct LogExporter;

#[async_trait]
impl Exporter for LogExporter {
    async fn export(&self, samples: &[Sample]) -> Result<(), String> {
        for sample in samples {
            tracing::info!(
                name = %sample.name,
                value = sample.value,
                tags = ?sample.tags,
                timestamp = %sample.timestamp,
                "metric sample"
            );
        }
        Ok(())
    }
}

/// Exports samples to a JSON-lines file.
///
/// Each call to `export` appends a batch of JSON-serialized samples to the
/// configured file path.
pub struct FileExporter {
    path: PathBuf,
}

impl FileExporter {
    /// Create a new file exporter that writes to the given path.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Return the export file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl Exporter for FileExporter {
    async fn export(&self, samples: &[Sample]) -> Result<(), String> {
        // Ensure parent directory exists.
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create directory: {}", e))?;
        }

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("failed to open file {}: {}", self.path.display(), e))?;

        for sample in samples {
            let json = serde_json::to_string(sample)
                .map_err(|e| format!("failed to serialize sample: {}", e))?;
            writeln!(file, "{}", json)
                .map_err(|e| format!("failed to write to file: {}", e))?;
        }

        Ok(())
    }
}

/// Stores exported samples in memory for testing and inspection.
pub struct InMemoryExporter {
    samples: Mutex<Vec<Sample>>,
}

impl InMemoryExporter {
    /// Create a new in-memory exporter.
    pub fn new() -> Self {
        Self {
            samples: Mutex::new(Vec::new()),
        }
    }

    /// Take all stored samples, leaving the internal buffer empty.
    pub fn take_samples(&self) -> Vec<Sample> {
        std::mem::take(&mut *self.samples.lock())
    }

    /// Return the number of currently stored samples.
    pub fn count(&self) -> usize {
        self.samples.lock().len()
    }
}

impl Default for InMemoryExporter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Exporter for InMemoryExporter {
    async fn export(&self, samples: &[Sample]) -> Result<(), String> {
        self.samples.lock().extend(samples.iter().cloned());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hf3fs_monitor::Sample;

    #[tokio::test]
    async fn test_log_exporter() {
        let exporter = LogExporter;
        let samples = vec![
            Sample::new("test.metric", 42.0),
            Sample::new("test.other", 99.0),
        ];

        // Should not panic; just logs.
        let result = exporter.export(&samples).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_file_exporter() {
        let dir = std::env::temp_dir().join("hf3fs-test-file-exporter");
        let _ = fs::remove_dir_all(&dir);

        let path = dir.join("metrics.jsonl");
        let exporter = FileExporter::new(&path);

        let samples = vec![
            Sample::new("cpu.usage", 75.5),
            Sample::new("memory.free", 2048.0),
        ];

        exporter.export(&samples).await.unwrap();

        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        // Each line should be valid JSON.
        for line in &lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(v.get("name").is_some());
            assert!(v.get("value").is_some());
        }

        // Append more.
        let more = vec![Sample::new("disk.io", 100.0)];
        exporter.export(&more).await.unwrap();

        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 3);

        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_in_memory_exporter() {
        let exporter = InMemoryExporter::new();

        let samples = vec![
            Sample::new("a", 1.0),
            Sample::new("b", 2.0),
        ];
        exporter.export(&samples).await.unwrap();
        assert_eq!(exporter.count(), 2);

        let taken = exporter.take_samples();
        assert_eq!(taken.len(), 2);
        assert_eq!(exporter.count(), 0);
    }
}
