use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing_appender::rolling;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Re-export tracing macros for convenience.
pub use tracing::{debug, error, info, instrument, trace, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    /// Log level filter (trace, debug, info, warn, error).
    #[serde(default = "default_level")]
    pub level: String,

    /// Directory for log files. If None, no file logging.
    pub log_dir: Option<PathBuf>,

    /// Prefix for log file names.
    #[serde(default = "default_prefix")]
    pub file_prefix: String,

    /// Log rotation: "hourly", "daily", "never".
    #[serde(default = "default_rotation")]
    pub rotation: String,

    /// Whether to output JSON format.
    #[serde(default)]
    pub json_format: bool,

    /// Whether to also output to console (stdout).
    #[serde(default = "default_true")]
    pub console_output: bool,
}

fn default_level() -> String {
    "info".into()
}

fn default_prefix() -> String {
    "hf3fs".into()
}

fn default_rotation() -> String {
    "hourly".into()
}

fn default_true() -> bool {
    true
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            level: default_level(),
            log_dir: None,
            file_prefix: default_prefix(),
            rotation: default_rotation(),
            json_format: false,
            console_output: true,
        }
    }
}

/// Initialize the logging system. Should be called once at program startup.
/// Returns a guard that must be held alive for the duration of the program
/// (for the non-blocking file writer).
pub fn init_logging(config: &LogConfig) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&config.level));

    let registry = tracing_subscriber::registry().with(env_filter);

    // Build console layer (boxed to unify types).
    let console_layer: Option<Box<dyn tracing_subscriber::Layer<_> + Send + Sync>> =
        if config.console_output {
            if config.json_format {
                Some(Box::new(fmt::layer().json()))
            } else {
                Some(Box::new(fmt::layer()))
            }
        } else {
            None
        };

    // Build file layer and obtain the guard.
    let (file_layer, guard): (
        Option<Box<dyn tracing_subscriber::Layer<_> + Send + Sync>>,
        Option<tracing_appender::non_blocking::WorkerGuard>,
    ) = if let Some(ref log_dir) = config.log_dir {
        let rotation = match config.rotation.as_str() {
            "daily" => rolling::Rotation::DAILY,
            "never" => rolling::Rotation::NEVER,
            // default to hourly
            _ => rolling::Rotation::HOURLY,
        };

        let file_appender = rolling::RollingFileAppender::builder()
            .rotation(rotation)
            .filename_prefix(&config.file_prefix)
            .filename_suffix("log")
            .build(log_dir)
            .expect("failed to create rolling file appender");

        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let layer: Box<dyn tracing_subscriber::Layer<_> + Send + Sync> = if config.json_format {
            Box::new(fmt::layer().json().with_writer(non_blocking))
        } else {
            Box::new(fmt::layer().with_writer(non_blocking))
        };

        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    registry.with(console_layer).with(file_layer).init();

    guard
}
