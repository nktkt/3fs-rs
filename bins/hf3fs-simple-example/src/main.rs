use clap::Parser;
use tracing_subscriber::EnvFilter;

/// 3FS Simple Example Application
///
/// Demonstrates the hf3fs_app lifecycle:
///   1. Parse CLI arguments and load config
///   2. Initialize the application
///   3. Wait for shutdown signal
///   4. Clean shutdown
#[derive(Parser, Debug)]
#[command(name = "hf3fs-simple-example", version, about)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "simple-example.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    tracing::info!(config = %args.config, "Starting 3FS Simple Example");
    tracing::info!("Application initialized — press Ctrl+C to stop");

    hf3fs_app::wait_for_shutdown_signal().await;
    tracing::info!("Simple example shutting down. Goodbye!");

    Ok(())
}
