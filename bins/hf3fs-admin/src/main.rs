use clap::Parser;
use tracing_subscriber::EnvFilter;

use hf3fs_cli::commands::AdminCommands;
use hf3fs_cli::connection::{AdminEnv, ConnectionOptions};
use hf3fs_cli::output::OutputFormat;

/// 3FS Administration Tool
///
/// Command-line interface for managing a 3FS (Fire-Flyer File System) cluster.
/// Provides commands for configuration management, cluster operations, storage
/// management, and filesystem metadata operations.
#[derive(Parser, Debug)]
#[command(name = "hf3fs-admin", version, about)]
struct Cli {
    /// Connection and authentication options.
    #[command(flatten)]
    connection: ConnectionOptions,

    /// Output format (table or json).
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,

    /// Enable verbose logging.
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    /// Show timing information for the executed command.
    #[arg(long, default_value_t = false)]
    profile: bool,

    /// Subcommand to execute.
    #[command(subcommand)]
    command: AdminCommands,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialize tracing.
    let default_level = if cli.verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::WARN
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive(default_level.into()),
        )
        .init();

    // Create the admin environment.
    let mut env = AdminEnv::new(cli.connection);

    // Optionally profile the command execution.
    let timer = if cli.profile {
        Some(hf3fs_cli::progress::Timer::start("command"))
    } else {
        None
    };

    // Execute the command.
    let result = cli.command.run(&mut env, cli.format).await;

    if let Some(timer) = timer {
        timer.stop();
    }

    result
}
