//! Output formatting utilities for CLI commands.
//!
//! Mirrors the C++ `Printer` class and `OutputTable` type from
//! `3FS/src/client/cli/common/Printer.h`.
//!
//! Provides both human-readable table output and machine-readable JSON output.

use serde::Serialize;
use std::fmt;
use std::io::Write;

/// A single row in a table output.
pub type OutputRow = Vec<String>;

/// A table of output rows, the standard return type for CLI command handlers.
///
/// Mirrors `Dispatcher::OutputTable` (`std::vector<std::vector<String>>`) from C++.
pub type OutputTable = Vec<OutputRow>;

/// Output format selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Human-readable column-aligned table output.
    Table,
    /// Machine-readable JSON output.
    Json,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Table
    }
}

/// Printer that formats and writes output tables.
///
/// Mirrors the C++ `Printer` class from `3FS/src/client/cli/common/Printer.h`.
pub struct Printer<W: Write = Box<dyn Write>> {
    stdout: W,
    format: OutputFormat,
}

impl Printer<Box<dyn Write>> {
    /// Create a new printer writing to stdout with the given format.
    pub fn stdout(format: OutputFormat) -> Self {
        Self {
            stdout: Box::new(std::io::stdout()),
            format,
        }
    }
}

impl<W: Write> Printer<W> {
    /// Create a new printer with a custom writer.
    pub fn new(writer: W, format: OutputFormat) -> Self {
        Self {
            stdout: writer,
            format,
        }
    }

    /// Print a single message string.
    pub fn print_message(&mut self, msg: &str) -> std::io::Result<()> {
        match self.format {
            OutputFormat::Table => {
                writeln!(self.stdout, "{}", msg)
            }
            OutputFormat::Json => {
                let obj = serde_json::json!({ "message": msg });
                writeln!(self.stdout, "{}", serde_json::to_string_pretty(&obj).unwrap_or_default())
            }
        }
    }

    /// Print an error message to the writer.
    pub fn print_error(&mut self, err: &str) -> std::io::Result<()> {
        match self.format {
            OutputFormat::Table => {
                writeln!(self.stdout, "Error: {}", err)
            }
            OutputFormat::Json => {
                let obj = serde_json::json!({ "error": err });
                writeln!(self.stdout, "{}", serde_json::to_string_pretty(&obj).unwrap_or_default())
            }
        }
    }

    /// Print an output table.
    ///
    /// In table mode, columns are aligned with a two-space separator between them,
    /// mirroring the C++ `Printer::print(OutputTable)` method.
    ///
    /// In JSON mode, if the first row looks like a header (i.e. the table has more
    /// than one row), the first row is used as field names and subsequent rows are
    /// output as JSON objects.
    pub fn print_table(&mut self, table: &OutputTable) -> std::io::Result<()> {
        if table.is_empty() {
            return Ok(());
        }

        match self.format {
            OutputFormat::Table => self.print_table_aligned(table),
            OutputFormat::Json => self.print_table_json(table),
        }
    }

    /// Print a serializable value as JSON (only in JSON mode; in table mode this
    /// is a no-op that returns Ok).
    pub fn print_value<T: Serialize>(&mut self, value: &T) -> std::io::Result<()> {
        match self.format {
            OutputFormat::Table => Ok(()),
            OutputFormat::Json => {
                let json = serde_json::to_string_pretty(value)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                writeln!(self.stdout, "{}", json)
            }
        }
    }

    /// Column-aligned table output mirroring C++ Printer::print(OutputTable).
    fn print_table_aligned(&mut self, table: &OutputTable) -> std::io::Result<()> {
        const SEPARATOR: &str = "  ";

        // Compute maximum width for each column.
        let mut widths: Vec<usize> = Vec::new();
        for row in table {
            if widths.len() < row.len() {
                widths.resize(row.len(), 0);
            }
            for (col, cell) in row.iter().enumerate() {
                let needed = cell.len() + SEPARATOR.len();
                if needed > widths[col] {
                    widths[col] = needed;
                }
            }
        }

        // Print each row with padding.
        for row in table {
            let mut line = String::new();
            for (col, cell) in row.iter().enumerate() {
                line.push_str(cell);
                if col + 1 < row.len() {
                    let padding = widths[col].saturating_sub(cell.len());
                    for _ in 0..padding {
                        line.push(' ');
                    }
                }
            }
            writeln!(self.stdout, "{}", line)?;
        }

        Ok(())
    }

    /// JSON table output. If the table has a header row, use it as keys.
    fn print_table_json(&mut self, table: &OutputTable) -> std::io::Result<()> {
        if table.len() <= 1 {
            // Single row or empty: output as array of arrays.
            let json = serde_json::to_string_pretty(table)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            return writeln!(self.stdout, "{}", json);
        }

        // Use first row as header keys.
        let headers = &table[0];
        let rows: Vec<serde_json::Map<String, serde_json::Value>> = table[1..]
            .iter()
            .map(|row| {
                let mut map = serde_json::Map::new();
                for (i, cell) in row.iter().enumerate() {
                    let key = headers.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                    map.insert(key, serde_json::Value::String(cell.clone()));
                }
                map
            })
            .collect();

        let json = serde_json::to_string_pretty(&rows)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        writeln!(self.stdout, "{}", json)
    }
}

impl<W: Write> fmt::Debug for Printer<W> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Printer")
            .field("format", &self.format)
            .finish()
    }
}

/// Helper to create an output table with a header row.
pub fn table_with_header(headers: &[&str]) -> OutputTable {
    vec![headers.iter().map(|h| h.to_string()).collect()]
}

/// Helper to format a key-value pair as a two-column row.
pub fn kv_row(key: &str, value: impl fmt::Display) -> OutputRow {
    vec![key.to_string(), value.to_string()]
}

/// Format a timestamp (nanoseconds since epoch) as a human-readable string.
pub fn format_timestamp_ns(ns: i64) -> String {
    use chrono::{DateTime, Utc};
    if ns == 0 {
        return "N/A".to_string();
    }
    let secs = ns / 1_000_000_000;
    let nsecs = (ns % 1_000_000_000) as u32;
    match DateTime::from_timestamp(secs, nsecs) {
        Some(dt) => {
            let utc: DateTime<Utc> = dt;
            utc.format("%Y-%m-%d %H:%M:%S").to_string()
        }
        None => "InvalidTime".to_string(),
    }
}

/// Format bytes in a human-friendly way (e.g., "1.5 GiB").
pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    if bytes >= TIB {
        format!("{:.2} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a permission mode as an octal string (e.g., "0755").
pub fn format_permission(perm: u32) -> String {
    format!("{:04o}", perm)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_aligned_output() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Table);

        let table = vec![
            vec!["Name".to_string(), "Type".to_string(), "Size".to_string()],
            vec!["file.txt".to_string(), "File".to_string(), "1024".to_string()],
            vec!["dir".to_string(), "Directory".to_string(), "-".to_string()],
        ];

        printer.print_table(&table).unwrap();
        let output = String::from_utf8(buf).unwrap();

        // Verify alignment: columns should be padded.
        assert!(output.contains("Name"));
        assert!(output.contains("file.txt"));
        assert!(output.contains("dir"));

        // Verify each row is on its own line.
        let lines: Vec<&str> = output.trim().lines().collect();
        assert_eq!(lines.len(), 3);
    }

    #[test]
    fn test_table_json_output() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Json);

        let table = vec![
            vec!["Id".to_string(), "Status".to_string()],
            vec!["1".to_string(), "Active".to_string()],
            vec!["2".to_string(), "Offline".to_string()],
        ];

        printer.print_table(&table).unwrap();
        let output = String::from_utf8(buf).unwrap();

        let parsed: Vec<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["Id"], "1");
        assert_eq!(parsed[0]["Status"], "Active");
        assert_eq!(parsed[1]["Id"], "2");
        assert_eq!(parsed[1]["Status"], "Offline");
    }

    #[test]
    fn test_table_with_header() {
        let table = table_with_header(&["A", "B", "C"]);
        assert_eq!(table.len(), 1);
        assert_eq!(table[0], vec!["A", "B", "C"]);
    }

    #[test]
    fn test_kv_row() {
        let row = kv_row("Key", 42);
        assert_eq!(row, vec!["Key", "42"]);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KiB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GiB");
        assert_eq!(format_bytes(1024u64 * 1024 * 1024 * 1024), "1.00 TiB");
    }

    #[test]
    fn test_format_permission() {
        assert_eq!(format_permission(0o755), "0755");
        assert_eq!(format_permission(0o644), "0644");
        assert_eq!(format_permission(0), "0000");
    }

    #[test]
    fn test_format_timestamp_ns_zero() {
        assert_eq!(format_timestamp_ns(0), "N/A");
    }

    #[test]
    fn test_format_timestamp_ns_valid() {
        // 2023-01-01T00:00:00Z in nanoseconds
        let ns = 1672531200i64 * 1_000_000_000;
        let formatted = format_timestamp_ns(ns);
        assert!(formatted.contains("2023"), "got: {}", formatted);
        assert!(formatted.contains("01-01"), "got: {}", formatted);
    }

    #[test]
    fn test_print_message_table() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Table);
        printer.print_message("hello").unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert_eq!(output.trim(), "hello");
    }

    #[test]
    fn test_print_message_json() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Json);
        printer.print_message("hello").unwrap();
        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["message"], "hello");
    }

    #[test]
    fn test_print_error_table() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Table);
        printer.print_error("something broke").unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.contains("Error:"));
        assert!(output.contains("something broke"));
    }

    #[test]
    fn test_print_error_json() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Json);
        printer.print_error("something broke").unwrap();
        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["error"], "something broke");
    }

    #[test]
    fn test_empty_table() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Table);
        let table: OutputTable = vec![];
        printer.print_table(&table).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.is_empty());
    }

    #[test]
    fn test_single_row_json() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Json);
        let table = vec![vec!["only".to_string(), "row".to_string()]];
        printer.print_table(&table).unwrap();
        let output = String::from_utf8(buf).unwrap();
        // Should be an array of arrays since there's no data rows to map.
        let parsed: Vec<Vec<String>> = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn test_print_value_json() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Json);
        let data = serde_json::json!({"foo": "bar", "count": 42});
        printer.print_value(&data).unwrap();
        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["foo"], "bar");
        assert_eq!(parsed["count"], 42);
    }

    #[test]
    fn test_print_value_table_noop() {
        let mut buf = Vec::new();
        let mut printer = Printer::new(&mut buf, OutputFormat::Table);
        let data = serde_json::json!({"foo": "bar"});
        printer.print_value(&data).unwrap();
        let output = String::from_utf8(buf).unwrap();
        assert!(output.is_empty());
    }
}
