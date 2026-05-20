// TODO(ai-review): review for correctness/style
//! `stats` subcommand: walk the local chunk cache and report per-manifest
//! download progress.
//!
//! Mostly offline. The one network step is fetching any manifests that
//! aren't yet on disk — without them we can't know how many chunks the
//! manifest references. Subsequent runs are fully offline.

use std::collections::HashSet;

use anyhow::{Context, Result};
use steam_depot_vfs::{ChunkHash, DepotStore, SteamAuth};

use crate::auth::Auth;
use crate::config::Config;

pub fn run(cfg: &Config, verify: bool) -> Result<()> {
    let store = DepotStore::new(cfg.store_root.clone());

    // First pass: see which configured manifests are missing on disk
    // and only spin up a runtime + auth if at least one is missing.
    let missing: Vec<&crate::config::Manifest> = cfg
        .manifests
        .iter()
        .filter(|m| {
            store
                .load_cached_manifest(m.depot_id, m.gid)
                .ok()
                .flatten()
                .is_none()
        })
        .collect();
    if !missing.is_empty() {
        tracing::info!(count = missing.len(), "fetching missing manifests");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        rt.block_on(async {
            use futures::stream::{FuturesUnordered, StreamExt};
            let auth = Auth::prepare(cfg.steam.account.clone(), cfg.steam.password.clone())
                .await
                .context("preparing auth for manifest fetch")?;
            // Force the connection up front so it isn't repeated by
            // each parallel fetch.
            auth.resolve().await?;

            let mut in_flight: FuturesUnordered<_> = missing
                .iter()
                .map(|m| {
                    let store = &store;
                    let auth = auth.clone();
                    async move {
                        store
                            .open_depot_manifest(auth, m.app_id, m.depot_id, m.gid, &m.branch)
                            .await
                            .with_context(|| {
                                format!(
                                    "fetching manifest {} for depot {} (branch {:?})",
                                    m.gid, m.depot_id, m.branch
                                )
                            })
                    }
                })
                .collect();
            while let Some(res) = in_flight.next().await {
                res?;
            }
            Ok::<_, anyhow::Error>(())
        })?;
    }

    let cache = scan_chunks(&store)?;
    println!(
        "chunk cache: {} files, {} on disk",
        cache.count,
        human_bytes(cache.bytes),
    );
    println!("            at {}", store.chunks_root().display());
    println!();

    if cfg.manifests.is_empty() {
        println!("no manifests in config");
        return Ok(());
    }

    println!(
        "{:<10}  {:<23}  {:<15}  {:<6}  manifest",
        "uncompr", "bytes (fetched/total)", "chunks", "%",
    );
    // Aggregated counters: dedup across all configured manifests so
    // chunks shared between manifest versions only count once.
    let mut all_chunks: std::collections::HashMap<ChunkHash, u32> =
        std::collections::HashMap::new();
    let mut all_uncompr_per_manifest: u64 = 0;
    let mut missing_count: u64 = 0;
    for m in &cfg.manifests {
        let Some(manifest) = store
            .load_cached_manifest(m.depot_id, m.gid)
            .with_context(|| {
                format!(
                    "loading cached manifest for depot {} gid {}",
                    m.depot_id, m.gid
                )
            })?
        else {
            println!(
                "{:<10}  {:<23}  {:<15}  {:<6}  {}/{}/{:>20} (manifest not cached)",
                "-", "-", "-", "-", m.app_id, m.depot_id, m.gid,
            );
            missing_count += 1;
            continue;
        };
        let mut total_uncompr: u64 = 0;
        let mut total_compr: u64 = 0;
        let mut fetched_compr: u64 = 0;
        let mut total_chunks: u64 = 0;
        let mut fetched_chunks: u64 = 0;
        let mut unique = HashSet::new();
        for f in &manifest.files {
            for c in &f.chunks {
                if !unique.insert(c.sha) {
                    continue;
                }
                total_uncompr += c.size_uncompressed as u64;
                total_compr += c.size_compressed as u64;
                total_chunks += 1;
                if cache.shas.contains(&c.sha) {
                    fetched_compr += c.size_compressed as u64;
                    fetched_chunks += 1;
                }
                // Aggregate across all manifests, deduplicated by sha.
                all_chunks.entry(c.sha).or_insert(c.size_compressed);
            }
        }
        all_uncompr_per_manifest += total_uncompr;
        let pct = if total_compr == 0 {
            100.0
        } else {
            100.0 * fetched_compr as f64 / total_compr as f64
        };
        println!(
            "{:<10}  {:<23}  {:<15}  {:<6}  {}/{}/{:<20}",
            human_bytes(total_uncompr),
            format!(
                "{}/{}",
                human_bytes(fetched_compr),
                human_bytes(total_compr)
            ),
            format!("{fetched_chunks}/{total_chunks}"),
            format!("{pct:.1}%"),
            m.app_id,
            m.depot_id,
            m.gid,
        );
    }

    // Aggregated summary across all configured manifests. The "total"
    // here is dedup'd across manifests (chunks shared between
    // manifest versions count once), so it represents the real disk
    // footprint to fully cache the whole config.
    let mut all_compr: u64 = 0;
    let mut all_fetched: u64 = 0;
    let mut all_fetched_chunks: u64 = 0;
    for (sha, &size) in &all_chunks {
        all_compr += size as u64;
        if cache.shas.contains(sha) {
            all_fetched += size as u64;
            all_fetched_chunks += 1;
        }
    }
    let byte_pct = if all_compr == 0 {
        100.0
    } else {
        100.0 * all_fetched as f64 / all_compr as f64
    };
    let chunk_pct = if all_chunks.is_empty() {
        100.0
    } else {
        100.0 * all_fetched_chunks as f64 / all_chunks.len() as f64
    };
    println!();
    let missing_note = if missing_count == 0 {
        String::new()
    } else {
        format!(" ({missing_count} missing)")
    };
    println!(
        "total across {} manifests{}: {} uncompr (sum), {}/{} ({:.1}%) compressed, {}/{} ({:.1}%) chunks",
        cfg.manifests.len(),
        missing_note,
        human_bytes(all_uncompr_per_manifest),
        human_bytes(all_fetched),
        human_bytes(all_compr),
        byte_pct,
        all_fetched_chunks,
        all_chunks.len(),
        chunk_pct,
    );

    if verify {
        verify_cache(&store, &cache)?;
    }
    Ok(())
}

/// Read every chunk file in the cache and confirm its SHA-1 matches
/// its filename. Catches bit-rot and partial writes. Parallelised via
/// `std::thread::scope` so we saturate the disk + CPU without depending
/// on the rest of the program's Tokio runtime.
fn verify_cache(store: &DepotStore, cache: &CacheScan) -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;

    println!();
    println!(
        "verifying {} cached chunks ({})...",
        cache.shas.len(),
        human_bytes(cache.bytes)
    );
    let started = std::time::Instant::now();
    let root = store.chunks_root();
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    // mpsc job queue: each job is a `(expected_sha, file_path)` pair.
    let (tx, rx) = mpsc::channel::<(ChunkHash, std::path::PathBuf)>();
    let rx = std::sync::Mutex::new(rx);

    let ok = AtomicU64::new(0);
    let mismatched = std::sync::Mutex::new(Vec::<ChunkHash>::new());
    let errored = std::sync::Mutex::new(Vec::<(ChunkHash, String)>::new());

    std::thread::scope(|s| {
        // Worker pool.
        for _ in 0..workers {
            let rx = &rx;
            let ok = &ok;
            let mismatched = &mismatched;
            let errored = &errored;
            s.spawn(move || {
                loop {
                    let job = rx.lock().unwrap().recv();
                    let Ok((expected, path)) = job else {
                        return;
                    };
                    match verify_one(&expected, &path) {
                        Ok(true) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(false) => {
                            mismatched.lock().unwrap().push(expected);
                        }
                        Err(e) => {
                            errored.lock().unwrap().push((expected, e.to_string()));
                        }
                    }
                }
            });
        }
        // Feed jobs.
        for sha in &cache.shas {
            tx.send((*sha, root.join(sha.to_string()))).unwrap();
        }
        drop(tx);
    });

    let elapsed = started.elapsed();
    let ok = ok.load(Ordering::Relaxed);
    let mismatched = mismatched.into_inner().unwrap();
    let errored = errored.into_inner().unwrap();
    let throughput = cache.bytes as f64 / elapsed.as_secs_f64().max(1e-3);
    println!(
        "verified {ok} ok, {} mismatched, {} errored in {:.1}s ({}/s)",
        mismatched.len(),
        errored.len(),
        elapsed.as_secs_f64(),
        human_bytes(throughput as u64),
    );
    for sha in &mismatched {
        println!("  mismatch: {sha}");
    }
    for (sha, err) in &errored {
        println!("  error {sha}: {err}");
    }
    Ok(())
}

fn verify_one(expected: &ChunkHash, path: &std::path::Path) -> Result<bool> {
    use sha1::{Digest, Sha1};
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let mut hasher = Sha1::new();
    hasher.update(&bytes);
    let got = hasher.finalize();
    Ok(got.as_slice() == expected.0.as_slice())
}

struct CacheScan {
    /// Shas present in the cache directory.
    shas: HashSet<ChunkHash>,
    /// Total bytes on disk (sum of chunk file sizes).
    bytes: u64,
    /// Number of chunk files on disk.
    count: u64,
}

fn scan_chunks(store: &DepotStore) -> Result<CacheScan> {
    let root = store.chunks_root();
    let mut shas = HashSet::new();
    let mut bytes = 0u64;
    let mut count = 0u64;
    let read = match std::fs::read_dir(&root) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(CacheScan { shas, bytes, count });
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", root.display())),
    };
    for entry in read {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // `parse_sha` rejects anything that isn't 40 hex chars, which
        // also excludes the half-written `<sha>.tmp.<pid>` files that
        // the cache writes alongside committed chunks.
        let Some(sha) = parse_sha(name) else { continue };
        let meta = entry.metadata()?;
        bytes += meta.len();
        count += 1;
        shas.insert(sha);
    }
    Ok(CacheScan { shas, bytes, count })
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
