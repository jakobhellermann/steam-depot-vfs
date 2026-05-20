// TODO(ai-review): review for correctness/style
//! `steam-depot` — mount one or more Steam depots as a single FUSE
//! filesystem. Linux only.

mod auth;
mod config;
mod prefetch;
mod stats;

use std::fs::File;
use std::path::PathBuf;
use std::sync::Mutex;

use clap::{Parser, Subcommand};
use steam_depot_mount::{Mount, MountConfig};
use steam_depot_vfs::DepotStore;
use tracing_indicatif::IndicatifLayer;
use tracing_perfetto::PerfettoLayer;
use tracing_subscriber::{EnvFilter, prelude::*};

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
    Mount {
        /// Write a Perfetto trace of every FUSE op and chunk fetch.
        /// Bare `--timings` writes to `./trace.pftrace`; pass a path to
        /// override. Open the result at https://ui.perfetto.dev .
        #[arg(
            long,
            value_name = "FILE",
            num_args = 0..=1,
            default_missing_value = "trace.pftrace",
        )]
        timings: Option<PathBuf>,
    },
    /// Download every chunk of every configured manifest into the local
    /// cache. Skips anything already on disk. Exits when done.
    Prefetch {
        /// Same semantics as `mount --timings`.
        #[arg(
            long,
            value_name = "FILE",
            num_args = 0..=1,
            default_missing_value = "trace.pftrace",
        )]
        timings: Option<PathBuf>,
    },
    /// Print local-cache stats: which manifests are how completely
    /// downloaded, total bytes on disk, etc. No network access.
    Stats,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // One-shot reporting subcommands stay quiet by default; the daemon
    // ones log at info.
    let default_filter = match cli.cmd {
        Cmd::Mount { .. } => "info,steam_depot_vfs=debug,fuser=error",
        // Per-chunk INFO is still noisy here, even with indicatif
        // keeping the bar intact. Warn-only for chunk_store keeps the
        // output focused on progress and real failures.
        Cmd::Prefetch { .. } => "info,steam_depot_vfs::chunk_store=warn,fuser=error",
        Cmd::Stats => "warn",
    };
    // Display filter is per-fmt-layer so we can keep the user-facing
    // output sparse without starving the perfetto trace of spans.
    let display_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| default_filter.into());

    let timings_path = match &cli.cmd {
        Cmd::Mount { timings } | Cmd::Prefetch { timings } => timings.clone(),
        Cmd::Stats => None,
    };
    let perfetto = timings_path
        .as_deref()
        .map(|p| -> anyhow::Result<_> {
            let file = File::create(p)?;
            Ok(PerfettoLayer::new(Mutex::new(file)).with_debug_annotations(true))
        })
        .transpose()?;

    // Route the fmt layer's writer through indicatif so any active
    // ProgressBars (currently only the prefetch one) aren't clobbered
    // by tracing output during the prefetch loop. We turn off the
    // default span-as-bar behavior so transient framework spans (like
    // websocket connects in steam-vent) don't flash a bar.
    let indicatif_layer = IndicatifLayer::new().with_max_progress_bars(0, None);
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(indicatif_layer.get_stderr_writer())
                .with_filter(display_filter),
        )
        .with(indicatif_layer)
        .with(perfetto)
        .init();

    if let Some(p) = &timings_path {
        tracing::info!(path = %p.display(), "writing perfetto trace");
    }

    let cfg = Config::from_file(&cli.config)?;
    match cli.cmd {
        Cmd::Mount { .. } => mount(cfg),
        Cmd::Prefetch { .. } => prefetch::run(cfg),
        Cmd::Stats => stats::run(&cfg),
    }
}

fn mount(cfg: Config) -> anyhow::Result<()> {
    std::fs::create_dir_all(&cfg.mountpoint)?;
    std::fs::create_dir_all(&cfg.store_root)?;

    // `Mount::start` constructs fuser's internal Tokio runtime via
    // `TokioAdapter`. That runtime must not be created (and especially
    // not dropped on error) from inside another active runtime — so we
    // build the mount synchronously here, *before* entering `block_on`.
    let mount = Mount::start(MountConfig::new(cfg.mountpoint.clone()))?;
    tracing::info!(mountpoint = %cfg.mountpoint.display(), "mounted");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let auth = Auth::prepare(cfg.steam.account.clone(), cfg.steam.password.clone()).await?;
        let store = DepotStore::new(cfg.store_root.clone());

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
        Ok::<_, anyhow::Error>(())
    })?;
    Ok(())
}
