//! `stats` subcommand: walk the local chunk cache and report per-manifest
//! download progress. Purely offline — no Steam roundtrips.

use std::collections::HashSet;

use anyhow::{Context, Result};
use steam_depot_vfs::{ChunkHash, DepotStore};

use crate::config::Config;

pub fn run(cfg: &Config) -> Result<()> {
    let store = DepotStore::new(cfg.store_root.clone());
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
                "{:<10}  {:<23}  {:<15}  {:<6}  {}/{}/{} (manifest not cached)",
                "-", "-", "-", "-", m.app_id, m.depot_id, m.gid,
            );
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
            }
        }
        let pct = if total_compr == 0 {
            100.0
        } else {
            100.0 * fetched_compr as f64 / total_compr as f64
        };
        println!(
            "{:<10}  {:<23}  {:<15}  {:<6}  {}/{}/{}",
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
    Ok(())
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
