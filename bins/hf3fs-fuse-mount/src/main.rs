use clap::Parser;
use tracing_subscriber::EnvFilter;

/// 3FS FUSE Mount Daemon
#[derive(Parser, Debug)]
#[command(name = "hf3fs-fuse-mount", version, about)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "fuse-mount.toml")]
    config: String,

    /// Mount point path
    #[arg(short, long)]
    mountpoint: Option<String>,

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

    tracing::info!(
        config = %args.config,
        mountpoint = ?args.mountpoint,
        "Starting 3FS FUSE Mount"
    );

    // TODO: Load config, connect to meta/storage, mount FUSE filesystem
    tracing::info!("FUSE mount initialization complete");
    hf3fs_app::wait_for_shutdown_signal().await;
    tracing::info!("FUSE mount shutting down");

    Ok(())
}
