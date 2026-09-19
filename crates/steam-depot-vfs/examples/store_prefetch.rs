// TODO(ai-review): review for style and correctness
//! Setup for the store-pattern tools: download every chunk of the pinned
//! manifest that isn't on disk yet, through the store's own fetch path
//! (auth → CDN chunk store → write-through cache). The measuring
//! examples stay offline-only by design — this fills the store for
//! them, once. Re-runs stat what's present and exit without touching
//! Steam, so `just profile` needs credentials only on a cold store.
//!
//! Needs `STEAM_USERNAME` / `STEAM_PASSWORD` in the environment (e.g.
//! via direnv) and a cached refresh token; the manifest must already be
//! in the store cache (open it in the viewer once first).
//!
//! ```text
//! set -a; source ../steam-multiversion-viewer/.env; set +a
//! cargo run --release -p steam-depot-vfs --example store_prefetch
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context as _, Result};
use clap::Parser;
use steam_depot_vfs::chunk_store::ChunkStore as _;
use steam_depot_vfs::session::LazyCachedAuth;
use steam_vent_depot::Chunk;

#[path = "common/mod.rs"]
mod store_common;

use store_common::StoreCtx;

/// Chunks fetched concurrently. One chunk is one CDN round trip; a
/// sequential loop spends its time waiting on latency instead of
/// bandwidth (the first version of this tool ran ~1 chunk/s).
const IN_FLIGHT: usize = 32;

#[derive(Parser)]
/// Download the pinned manifest's missing chunks into the store.
struct Cli {
    /// Manifest gid to fill.
    #[arg(long, default_value_t = store_common::PINNED_GID)]
    manifest_gid: u64,
    /// Store root; defaults to the steam-multiversion-viewer store.
    #[arg(long)]
    store: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let ctx = StoreCtx::open(cli.store.as_deref(), cli.manifest_gid)?;
    let manifest = ctx.manifest();

    // Exactly what the patterns measure: the bulk set (largest files
    // until the budget) plus the single-file patterns' pinned bundle —
    // not the whole manifest.
    let targets: Vec<_> = ctx
        .bulk_files()
        .into_iter()
        .chain([ctx.file(store_common::PINNED_FILE)?])
        .collect();
    let chunks_root = ctx.chunks_root().to_path_buf();

    // Unique chunks of the pinned set, and which are still missing.
    let mut seen = HashSet::new();
    let mut missing = Vec::new();
    for f in &targets {
        for c in f.chunks() {
            if seen.insert(c.sha) && !chunks_root.join(c.sha.to_string()).try_exists()? {
                missing.push(c.clone());
            }
        }
    }
    println!(
        "pinned set of manifest {}: {} files, {} unique chunks, {} missing ({:.0} MB compressed)",
        manifest.manifest_id,
        targets.len(),
        seen.len(),
        missing.len(),
        missing
            .iter()
            .map(|c| c.size_compressed as u64)
            .sum::<u64>() as f64
            / 1e6,
    );
    if missing.is_empty() {
        println!("store complete; nothing to download");
        return Ok(());
    }

    // Show what the silent-looking network phases are doing: the login
    // and the depot-key fetch can each take seconds without this.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .try_init();

    let account = std::env::var("STEAM_USERNAME").context("STEAM_USERNAME (e.g. via direnv)")?;
    let password = std::env::var("STEAM_PASSWORD").context("STEAM_PASSWORD (e.g. via direnv)")?;
    println!("authenticating with Steam (refresh-token login)…");
    let auth = LazyCachedAuth::prepare(
        LazyCachedAuth::default_refresh_token_cache(),
        account,
        password,
    )
    .await?;

    // The store's real fetch path, as the viewer takes it: the snapshot's
    // chunk store fetches from the CDN and persists through the cache.
    // `ensure` is write-only — fetch and persist without holding the
    // bytes — and chunks go in bounded waves, one CDN round trip each.
    println!("opening the manifest with a fetching store (depot key, CDN servers)…");
    let snap = Arc::new(
        ctx.store()
            .open_depot_manifest(
                Arc::new(auth),
                ctx.app_id,
                ctx.depot_id,
                manifest.manifest_id,
                "public",
            )
            .await
            .context("opening the manifest with a fetching store")?,
    );
    println!(
        "downloading {} chunks in waves of {IN_FLIGHT}…",
        missing.len()
    );
    let permits = Arc::new(tokio::sync::Semaphore::new(IN_FLIGHT));
    let mut joins = tokio::task::JoinSet::new();
    let started = Instant::now();
    let mut last_report = Instant::now();
    for (i, c) in missing.iter().enumerate() {
        let permit = permits.clone().acquire_owned().await?;
        let snap = Arc::clone(&snap);
        let sha = c.sha; // Copy out: the task must not borrow the list.
        joins.spawn(async move {
            let _permit = permit;
            snap.chunks().ensure(sha).await
        });
        // Keep ~IN_FLIGHT waves in the air, draining finished tasks for
        // progress and errors as we go.
        while joins.len() >= IN_FLIGHT {
            joins.join_next().await.expect("join task")??;
            report(i + 1 - joins.len(), &missing, started, &mut last_report);
        }
    }
    while let Some(res) = joins.join_next().await {
        res.expect("join task")?;
        report(
            missing.len() - joins.len(),
            &missing,
            started,
            &mut last_report,
        );
    }
    println!(
        "store filled: {} chunks on disk for manifest {}",
        seen.len(),
        manifest.manifest_id
    );
    Ok(())
}

/// Progress line per 32 chunks, per 1.5 s, and at the end — long enough
/// to not scroll, often enough to see that the download is alive.
/// `done` counts completions, which finish out of order; the MB column
/// sums the first `done` entries of the list, so it drifts slightly —
/// fine for a progress line.
fn report(done: usize, missing: &[Chunk], started: Instant, last: &mut Instant) {
    if done != missing.len() && !done.is_multiple_of(32) && last.elapsed().as_secs_f64() < 1.5 {
        return;
    }
    *last = Instant::now();
    let mb: u64 = missing[..done]
        .iter()
        .map(|c| c.size_compressed as u64)
        .sum();
    println!(
        "  {done:>5}/{} chunks, {mb:>7.0} MB, {:.0}s",
        missing.len(),
        started.elapsed().as_secs_f64(),
    );
}
