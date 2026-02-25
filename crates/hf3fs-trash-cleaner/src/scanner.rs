//! Trash directory scanner.
//!
//! Scans a trash root directory and identifies expired items for removal.
//! This corresponds to the `scan_trash()` and `Trash::clean()` functions
//! in the original C++ (Rust) implementation.

use std::fs;
use std::path::{Path, PathBuf};

use crate::config::TrashCleanerConfig;
use crate::item::{TrashItem, TrashItemStatus};

/// Result of scanning a single trash item.
#[derive(Debug)]
pub struct ScanResult {
    /// The directory name.
    pub name: String,
    /// Full path of the item.
    pub path: PathBuf,
    /// Status after checking.
    pub status: TrashItemStatus,
    /// Whether the item was removed.
    pub removed: bool,
    /// Error message if removal failed.
    pub error: Option<String>,
}

/// Trash directory scanner.
///
/// Scans trash directories listed in the configuration, identifies expired
/// trash items, and optionally removes them.
pub struct TrashScanner {
    config: TrashCleanerConfig,
}

impl TrashScanner {
    /// Create a new scanner with the given configuration.
    pub fn new(config: TrashCleanerConfig) -> Self {
        Self { config }
    }

    /// Scan all configured trash paths.
    ///
    /// Returns a summary of what was found and what was cleaned.
    pub fn scan_all(&self) -> Vec<ScanResult> {
        let mut results = Vec::new();

        for path in &self.config.paths {
            tracing::info!(path = %path.display(), "Scanning trash directory");
            match self.scan_path(path) {
                Ok(mut path_results) => {
                    results.append(&mut path_results);
                }
                Err(e) => {
                    tracing::error!(
                        path = %path.display(),
                        error = %e,
                        "Failed to scan trash directory"
                    );
                    if self.config.abort_on_error {
                        tracing::error!("Aborting due to abort_on_error configuration");
                        break;
                    }
                }
            }
        }

        results
    }

    /// Scan a single trash root path.
    ///
    /// Iterates over subdirectories in the trash root. Each subdirectory is
    /// expected to be named according to the trash item convention.
    pub fn scan_path(&self, trash_root: &Path) -> Result<Vec<ScanResult>, String> {
        let entries = fs::read_dir(trash_root)
            .map_err(|e| format!("failed to read directory {}: {}", trash_root.display(), e))?;

        let mut results = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|e| {
                format!(
                    "failed to read entry in {}: {}",
                    trash_root.display(),
                    e
                )
            })?;

            let name = match entry.file_name().to_str() {
                Some(name) => name.to_string(),
                None => {
                    tracing::warn!(
                        path = %entry.path().display(),
                        "Skipping non-UTF8 directory name"
                    );
                    continue;
                }
            };

            // Skip . and ..
            if name == "." || name == ".." {
                continue;
            }

            let full_path = trash_root.join(&name);

            // Only process directories.
            let metadata = match entry.metadata() {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        path = %full_path.display(),
                        error = %e,
                        "Failed to stat entry"
                    );
                    continue;
                }
            };

            if !metadata.is_dir() {
                results.push(ScanResult {
                    name: name.clone(),
                    path: full_path,
                    status: TrashItemStatus::Invalid("not a directory".to_string()),
                    removed: false,
                    error: None,
                });
                continue;
            }

            let status = TrashItem::check_name(&name);

            let (removed, error) = match &status {
                TrashItemStatus::Expired => self.try_remove(&full_path, &name),
                TrashItemStatus::NotExpired => {
                    tracing::trace!(
                        name = &name,
                        "Trash item not yet expired"
                    );
                    (false, None)
                }
                TrashItemStatus::Invalid(reason) => {
                    tracing::warn!(
                        name = &name,
                        reason = reason.as_str(),
                        "Unknown trash item"
                    );
                    if self.config.clean_unknown {
                        self.try_remove(&full_path, &name)
                    } else {
                        (false, None)
                    }
                }
            };

            results.push(ScanResult {
                name,
                path: full_path,
                status,
                removed,
                error,
            });
        }

        Ok(results)
    }

    /// Try to remove a trash item directory recursively.
    fn try_remove(&self, path: &Path, name: &str) -> (bool, Option<String>) {
        tracing::info!(
            name = name,
            path = %path.display(),
            "Removing expired trash item"
        );

        match fs::remove_dir_all(path) {
            Ok(()) => {
                tracing::info!(
                    name = name,
                    path = %path.display(),
                    "Successfully removed trash item"
                );
                (true, None)
            }
            Err(e) => {
                let error = format!("failed to remove {}: {}", path.display(), e);
                tracing::error!(
                    name = name,
                    path = %path.display(),
                    error = %e,
                    "Failed to remove trash item"
                );
                (false, Some(error))
            }
        }
    }

    /// Scan all paths and return only the count of removed items.
    ///
    /// Convenience method for the simple run-once case.
    pub fn clean(&self) -> (usize, usize) {
        let results = self.scan_all();
        let expired = results.iter().filter(|r| r.status == TrashItemStatus::Expired).count();
        let removed = results.iter().filter(|r| r.removed).count();
        (expired, removed)
    }

    /// Run the scanner in a loop according to the configured interval.
    ///
    /// If `interval_secs` is 0, scans once and returns. Otherwise, scans
    /// repeatedly with the configured delay.
    pub async fn run(&self) {
        loop {
            let (expired, removed) = self.clean();
            tracing::info!(expired, removed, "Trash scan completed");

            match self.config.interval() {
                Some(interval) => {
                    tokio::time::sleep(interval).await;
                }
                None => {
                    // Run once and exit.
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_test_trash(dir: &Path) {
        // Expired item (expired in 2020).
        fs::create_dir_all(dir.join("1d-20200101_0000-20200102_0000")).unwrap();

        // Not expired (far future).
        fs::create_dir_all(dir.join("1d-20200101_0000-20990101_0000")).unwrap();

        // Invalid name.
        fs::create_dir_all(dir.join("invalid-name")).unwrap();

        // Not a directory.
        fs::write(dir.join("some-file"), b"data").unwrap();
    }

    #[test]
    fn test_scanner_scan_path() {
        let dir = std::env::temp_dir().join("hf3fs-test-trash-scanner");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        create_test_trash(&dir);

        let config = TrashCleanerConfig {
            paths: vec![dir.clone()],
            ..Default::default()
        };

        let scanner = TrashScanner::new(config);
        let results = scanner.scan_path(&dir).unwrap();

        // Should have at least 4 entries (expired, not-expired, invalid dir, file).
        assert!(results.len() >= 3);

        // The expired one should have been removed.
        let expired: Vec<_> = results
            .iter()
            .filter(|r| r.status == TrashItemStatus::Expired)
            .collect();
        assert_eq!(expired.len(), 1);
        assert!(expired[0].removed);
        assert!(!dir.join("1d-20200101_0000-20200102_0000").exists());

        // The not-expired one should still exist.
        assert!(dir.join("1d-20200101_0000-20990101_0000").exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_scanner_clean_summary() {
        let dir = std::env::temp_dir().join("hf3fs-test-trash-clean");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        // Create 3 expired items.
        fs::create_dir_all(dir.join("a-20200101_0000-20200201_0000")).unwrap();
        fs::create_dir_all(dir.join("b-20200301_0000-20200401_0000")).unwrap();
        fs::create_dir_all(dir.join("c-20200501_0000-20200601_0000")).unwrap();

        // And 1 not-expired.
        fs::create_dir_all(dir.join("d-20200101_0000-20990101_0000")).unwrap();

        let config = TrashCleanerConfig {
            paths: vec![dir.clone()],
            ..Default::default()
        };

        let scanner = TrashScanner::new(config);
        let (expired, removed) = scanner.clean();

        assert_eq!(expired, 3);
        assert_eq!(removed, 3);

        // Only the non-expired one remains.
        let remaining: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(remaining.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_scanner_run_once() {
        let dir = std::env::temp_dir().join("hf3fs-test-trash-run-once");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        fs::create_dir_all(dir.join("x-20200101_0000-20200201_0000")).unwrap();

        let config = TrashCleanerConfig {
            paths: vec![dir.clone()],
            interval_secs: 0, // Run once.
            ..Default::default()
        };

        let scanner = TrashScanner::new(config);
        scanner.run().await;

        // The expired item should be removed.
        assert!(!dir.join("x-20200101_0000-20200201_0000").exists());

        let _ = fs::remove_dir_all(&dir);
    }
}
