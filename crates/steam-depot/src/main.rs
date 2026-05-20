//! `steam-depot` — mount one or more Steam depots as a single FUSE
//! filesystem. Linux only.

mod auth;
mod config;
mod stats;

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use steam_depot_mount::{Mount, MountConfig};
use steam_depot_vfs::DepotStore;
use tracing_subscriber::EnvFilter;

use crate::auth::Auth;
use crate::config::Config;

#[derive(Parser)]
#[command(about = "Mount Steam depots as a single FUSE filesystem, or inspect the local cache")]
struct Cli {
    /// Path to the TOML config file.
    #[arg(short, long)]
    config: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Mount the depots in the config and run until SIGINT/SIGTERM.
    Mount,
    /// Print local-cache stats: which manifests are how completely
    /// downloaded, total bytes on disk, etc. No network access.
    Stats,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // One-shot reporting subcommands stay quiet by default; the daemon
    // ones log at info.
    let default_filter = match cli.cmd {
        Cmd::Mount => "info,steam_depot_vfs=debug,fuser=error",
        Cmd::Stats => "warn",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| default_filter.into()),
        )
        .init();

    let cfg = Config::from_file(&cli.config)?;
    match cli.cmd {
        Cmd::Mount => mount(cfg).await,
        Cmd::Stats => stats::run(&cfg),
    }
}

async fn mount(cfg: Config) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.mountpoint)?;
    std::fs::create_dir_all(&cfg.store_root)?;

    let auth = Auth::prepare(cfg.steam.account.clone(), cfg.steam.password.clone()).await?;
    let store = DepotStore::new(cfg.store_root.clone());

    let mount = Mount::start(MountConfig::new(cfg.mountpoint.clone()))?;
    tracing::info!(mountpoint = %cfg.mountpoint.display(), "mounted");

    for m in &cfg.manifests {
        tracing::info!(
            app_id = m.app_id,
            depot_id = m.depot_id,
            gid = m.gid,
            branch = m.branch,
            "opening manifest",
        );
        let snapshot = store
            .open_depot_manifest(auth.clone(), m.app_id, m.depot_id, m.gid, &m.branch)
            .await?;
        mount.add(m.app_id, m.depot_id, m.gid, snapshot)?;
    }

    mount.wait_for_signal().await?;
    Ok(())
}
