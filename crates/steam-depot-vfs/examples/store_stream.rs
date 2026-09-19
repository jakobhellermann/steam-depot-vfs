// TODO(ai-review): review for style and correctness
//! Access pattern: sliding-window bulk read with a reused buffer over
//! the pinned bulk set — the export shape (`read_into` at an 8 MiB
//! window, mirroring
//! steam-multiversion-viewer's export writer).
//!
//! ```text
//! cargo run --release -p steam-depot-vfs --example store_stream -- --help
//! ```

use std::time::Instant;

use anyhow::Result;
use clap::Parser;

#[path = "common/mod.rs"]
mod store_common;

use store_common::{StoreCtx, cpu_seconds};

/// steam-multiversion-viewer's export window size.
const WINDOW: u64 = 8 * 1024 * 1024;

#[derive(Parser)]
/// Read the pinned bulk set through a reused sliding window,
/// repeatedly.
struct Cli {
    /// Number of passes over the files.
    #[arg(long, default_value_t = 3)]
    passes: usize,
    /// Manifest gid to read.
    #[arg(long, default_value_t = store_common::PINNED_GID)]
    manifest_gid: u64,
    /// Store root; defaults to the steam-multiversion-viewer store.
    #[arg(long)]
    store: Option<std::path::PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let ctx = StoreCtx::open(cli.store.as_deref(), cli.manifest_gid)?;
        let files = ctx.bulk_files();
        ctx.require_complete(&files)?;
        let total: u64 = files.iter().map(|f| f.size).sum();
        println!(
            "stream: {} pinned files (largest until {} MiB), {:.0} MB plaintext total",
            files.len(),
            store_common::BULK_BUDGET >> 20,
            total as f64 / 1e6
        );
        let snapshot = ctx.fresh_snapshot();
        let mut buf = Vec::with_capacity(WINDOW as usize);
        for pass in 1..=cli.passes {
            let cpu0 = cpu_seconds();
            let t = Instant::now();
            let mut bytes = 0u64;
            for f in &files {
                let mut off = 0;
                while off < f.size {
                    let len = WINDOW.min(f.size - off);
                    buf.clear();
                    snapshot.read_into(&f.path, off, len, &mut buf).await?;
                    std::hint::black_box(&buf);
                    off += len;
                }
                bytes += f.size;
            }
            let wall = t.elapsed();
            let cpu = cpu_seconds() - cpu0;
            println!(
                "pass {pass}: wall {wall:>9.3?}  cpu {cpu:>5.2}s  {:.0} MB/s",
                bytes as f64 / 1e6 / wall.as_secs_f64()
            );
        }
        Ok(())
    })
}
