use clap::Parser;
use tracing_subscriber::EnvFilter;

/// 3FS Storage Server
#[derive(Parser, Debug)]
#[command(name = "hf3fs-storage-server", version, about)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "storage-server.toml")]
    config: String,

    /// Dump default configuration and exit
    #[arg(long)]
    dump_default_config: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    if args.dump_default_config {
        tracing::info!("Dumping default configuration");
        return Ok(());
    }

    tracing::info!(config = %args.config, "Starting 3FS Storage Server");

    // TODO: Load config, create StorageServer Application, run via hf3fs_app::run_application
    tracing::info!("Storage server initialization complete");
    hf3fs_app::wait_for_shutdown_signal().await;
    tracing::info!("Storage server shutting down");

    Ok(())
}
