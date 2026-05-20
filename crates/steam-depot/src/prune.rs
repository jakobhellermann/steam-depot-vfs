// TODO(ai-review): review for correctness/style
//! `prune` subcommand: delete every cached chunk in the local store.
//! Manifests are kept (they're tiny and offline-replayable). Future
//! work: filter to manifests not currently referenced by config.

use std::io::Write as _;

use anyhow::{Context, Result};
use steam_depot_vfs::DepotStore;

use crate::config::Config;

pub fn run(cfg: &Config, yes: bool) -> Result<()> {
    let store = DepotStore::new(cfg.store_root.clone());
    let root = store.chunks_root();

    // Pre-walk so the prompt can show what's at stake.
    let (count, bytes) = scan(&root)?;
    if count == 0 {
        println!("chunk cache at {} is already empty", root.display());
        return Ok(());
    }
    println!(
        "would delete {} files ({} on disk) at {}",
        count,
        human_bytes(bytes),
        root.display(),
    );

    if !yes && !confirm("proceed?")? {
        println!("aborted");
        return Ok(());
    }

    let read = std::fs::read_dir(&root).with_context(|| format!("reading {}", root.display()))?;
    let mut deleted = 0u64;
    for entry in read {
        let entry = entry?;
        let path = entry.path();
        // Skip non-files (e.g. the parent dirs of a future sharded
        // layout); the current layout is flat but defensiveness is
        // cheap here.
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        deleted += 1;
    }
    println!("deleted {deleted} files");
    Ok(())
}

fn scan(root: &std::path::Path) -> Result<(u64, u64)> {
    let mut count = 0u64;
    let mut bytes = 0u64;
    let read = match std::fs::read_dir(root) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(e) => return Err(e).with_context(|| format!("reading {}", root.display())),
    };
    for entry in read {
        let entry = entry?;
        if !entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            continue;
        }
        count += 1;
        bytes += entry.metadata()?.len();
    }
    Ok((count, bytes))
}

fn confirm(prompt: &str) -> Result<bool> {
    print!("{prompt} [y/N] ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(matches!(input.trim(), "y" | "Y" | "yes"))
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
