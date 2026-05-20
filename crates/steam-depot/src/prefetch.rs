// TODO(ai-review): review for correctness/style
//! `prefetch` subcommand: walk every chunk in every configured manifest
//! and populate the local chunk cache. The mount's read path stays cold
//! until something asks for a file; this is the "warm me up" knob.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::{ChunkHash, DepotStore, SteamAuth};

use crate::auth::Auth;
use crate::config::Config;

/// Default max concurrent in-flight CDN fetches. DepotDownloader uses
/// 8 (or 25 with `-validate-all`); 16 is a middle ground that
/// noticeably improves throughput on most connections without hammering
/// Steam.
const DEFAULT_PARALLELISM: usize = 16;

pub fn run(cfg: Config, parallelism: Option<usize>, seconds: Option<u64>) -> Result<()> {
    let parallelism = parallelism.unwrap_or(DEFAULT_PARALLELISM).max(1);
    let max_duration = seconds.map(std::time::Duration::from_secs);
    std::fs::create_dir_all(&cfg.store_root)?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let auth = Auth::prepare(cfg.steam.account.clone(), cfg.steam.password.clone()).await?;
        // Force the connection + cdn-server discovery to happen up
        // front so the auth chatter doesn't break up the per-manifest
        // progress bar later.
        auth.resolve().await?;
        let store = DepotStore::new(cfg.store_root.clone());

        // Walk the chunk cache once to find what's already on disk.
        // We use this to make the progress bar honest: cached chunks
        // are skipped entirely rather than counted as 1.3 GB/s reads.
        let cached = scan_chunks_on_disk(&store.chunks_root())?;
        tracing::info!(cached_chunks = cached.len(), "scanned chunk cache");

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
            // `Arc` so each spawned per-chunk task can hold a cheap
            // clone of the snapshot without borrowing across the spawn.
            let snapshot = Arc::new(snapshot);
            prefetch_snapshot(snapshot, &cached, parallelism, max_duration).await?;
        }
        Ok::<_, anyhow::Error>(())
    })
}

fn scan_chunks_on_disk(root: &std::path::Path) -> Result<HashSet<ChunkHash>> {
    let mut out = HashSet::new();
    let read = match std::fs::read_dir(root) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e.into()),
    };
    for entry in read {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(sha) = parse_sha(name) {
            out.insert(sha);
        }
    }
    Ok(out)
}

fn parse_sha(s: &str) -> Option<ChunkHash> {
    if s.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(ChunkHash(out))
}

async fn prefetch_snapshot<C>(
    snapshot: Arc<steam_depot_vfs::fs::DepotSnapshot<C>>,
    already_cached: &HashSet<ChunkHash>,
    parallelism: usize,
    max_duration: Option<std::time::Duration>,
) -> Result<()>
where
    C: ChunkStore + Send + Sync + 'static,
{
    let manifest = snapshot.manifest();

    // Deduplicate, and drop anything already on disk. Skipped chunks
    // are tracked separately so we can report what didn't need fetching
    // without polluting the progress bar's "bytes/s" with stat-rate.
    let mut unique: HashMap<_, u64> = HashMap::new();
    let mut skipped: HashSet<_> = HashSet::new();
    let mut skipped_bytes: u64 = 0;
    for f in &manifest.files {
        for c in &f.chunks {
            if already_cached.contains(&c.sha) {
                if skipped.insert(c.sha) {
                    skipped_bytes += c.size_compressed as u64;
                }
                continue;
            }
            unique.entry(c.sha).or_insert(c.size_compressed as u64);
        }
    }
    let skipped_chunks = skipped.len() as u64;
    let total_chunks = unique.len() as u64;
    let new_bytes: u64 = unique.values().sum();
    // The bar's total is the full manifest size, but its position
    // starts at the already-cached bytes so the displayed "x / y"
    // matches the real download progress without making the rate look
    // like 1.3 GiB/s for the first second.
    let total_bytes = new_bytes + skipped_bytes;
    tracing::info!(
        manifest_id = manifest.manifest_id,
        chunks = total_chunks,
        bytes_compressed = total_bytes,
        skipped_chunks,
        skipped_bytes,
        "prefetching depot",
    );
    if total_chunks == 0 {
        tracing::info!(manifest_id = manifest.manifest_id, "nothing to fetch");
        return Ok(());
    }

    let started = Instant::now();
    let pb = ProgressBar::new(total_bytes).with_style(
        ProgressStyle::with_template(
            "{prefix:>20} [{bar:40.cyan/blue}] {bytes:>10}/{total_bytes:>10}  {bytes_per_sec:>11}  ETA {eta:>4}",
        )
        .expect("valid template")
        .progress_chars("=> "),
    );
    pb.set_prefix(format!("manifest {}", manifest.manifest_id));
    pb.set_position(skipped_bytes);
    // `reset_eta` clears the rate/ETA estimator so the cached bytes
    // we just pre-credited don't poison the first few seconds of
    // rate display.
    pb.reset_eta();
    pb.enable_steady_tick(std::time::Duration::from_millis(1000));

    // Each chunk fetch is its own `tokio::spawn`'d task so the
    // tokio scheduler can distribute work across worker threads
    // (otherwise `FuturesUnordered` polls everything on one task =
    // one worker thread, which made the single-thread CPU
    // bottleneck visible in the perfetto trace).
    use futures::stream::{FuturesUnordered, StreamExt};

    // Stable order makes the trace easier to read across runs.
    let mut work: Vec<_> = unique.into_iter().collect();
    work.sort_unstable_by_key(|(sha, _)| sha.0);

    let mut in_flight = FuturesUnordered::new();
    let mut iter = work.into_iter();

    let spawn_one = |sha, size| {
        let snapshot = Arc::clone(&snapshot);
        tokio::spawn(async move {
            let r = snapshot
                .chunks()
                .ensure(sha)
                .await
                .map_err(anyhow::Error::from);
            (sha, size, r)
        })
    };

    // Prime the pipeline.
    for _ in 0..parallelism {
        if let Some((sha, size)) = iter.next() {
            in_flight.push(spawn_one(sha, size));
        }
    }
    let mut chunks_done: u64 = 0;
    let mut bytes_done: u64 = 0;
    // On Ctrl-C / SIGTERM (or an explicit `--seconds` deadline) we
    // stop submitting new fetches but keep draining the in-flight ones
    // so their writes commit cleanly.
    let mut shutdown = std::pin::pin!(async move {
        match max_duration {
            Some(d) => {
                tokio::select! {
                    _ = wait_for_term_signal() => {}
                    _ = tokio::time::sleep(d) => {}
                }
            }
            None => wait_for_term_signal().await,
        }
    });
    let mut interrupted = false;
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown, if !interrupted => {
                let reason = if max_duration.is_some() {
                    "time limit reached, draining in-flight fetches"
                } else {
                    "interrupted, draining in-flight fetches"
                };
                tracing::info!(reason);
                interrupted = true;
            }
            res = in_flight.next() => {
                let Some(joined) = res else { break };
                let (sha, size, outcome) = match joined {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(%e, "fetch task panicked");
                        continue;
                    }
                };
                match outcome {
                    Ok(()) => {
                        chunks_done += 1;
                        bytes_done += size;
                        pb.inc(size);
                    }
                    Err(e) => {
                        tracing::warn!(%sha, %e, "chunk fetch failed");
                    }
                }
                if !interrupted && let Some((sha, size)) = iter.next() {
                    in_flight.push(spawn_one(sha, size));
                }
            }
        }
    }

    pb.finish_and_clear();
    let elapsed = started.elapsed();
    let secs = elapsed.as_secs_f64().max(1e-3);
    let rate = bytes_done as f64 / secs;
    println!(
        "manifest {} — {} / {} in {:.1}s  ({}/s, {}/{} chunks){}",
        manifest.manifest_id,
        human_bytes(bytes_done),
        human_bytes(new_bytes),
        secs,
        human_bytes(rate as u64),
        chunks_done,
        total_chunks,
        if interrupted { "  [interrupted]" } else { "" },
    );
    // Hitting a `--seconds` deadline is a normal exit; only an
    // out-of-band signal counts as failure.
    if interrupted && max_duration.is_none() {
        anyhow::bail!("prefetch interrupted by signal");
    }
    Ok(())
}

fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Wait for SIGINT or SIGTERM, whichever comes first.
async fn wait_for_term_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
        // If we can't install the SIGTERM handler we fall back to
        // SIGINT only — losing a bit of robustness rather than failing
        // the whole prefetch over signal-handler setup.
        let _ = tokio::signal::ctrl_c().await;
        return;
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}
