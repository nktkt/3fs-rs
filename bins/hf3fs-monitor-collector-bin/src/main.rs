use clap::Parser;
use tracing_subscriber::EnvFilter;

/// 3FS Monitor Collector Service
#[derive(Parser, Debug)]
#[command(name = "hf3fs-monitor-collector", version, about)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "monitor-collector.toml")]
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

    tracing::info!(config = %args.config, "Starting 3FS Monitor Collector");

    // TODO: Load config, start collecting metrics from cluster nodes
    tracing::info!("Monitor collector initialization complete");
    hf3fs_app::wait_for_shutdown_signal().await;
    tracing::info!("Monitor collector shutting down");

    Ok(())
}
