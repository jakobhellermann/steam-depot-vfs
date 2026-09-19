// TODO(ai-review): review for style and correctness
//! Access pattern: single whole-file read — the interactive shape of
//! `/file/raw`, structured tree loads, texture previews, text diffs
//! (all `read_full` in the routes). One cold read on a fresh snapshot,
//! then warm repeats on the same snapshot.
//!
//! ```text
//! cargo run --release -p steam-depot-vfs --example store_file -- --help
//! ```

use std::time::Instant;

use anyhow::Result;
use clap::Parser;

#[path = "common/mod.rs"]
mod store_common;

use store_common::{StoreCtx, cpu_seconds};

#[derive(Parser)]
/// Read one file whole, cold, then repeatedly warm.
struct Cli {
    /// Manifest path to read.
    #[arg(
        long,
        default_value = store_common::PINNED_FILE
    )]
    file: String,
    /// Number of warm repeats after the cold read.
    #[arg(long, default_value_t = 5)]
    repeats: usize,
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
        let file = ctx.find_file(&cli.file)?;
        println!(
            "file: {} ({} B, {} chunks)",
            file.path,
            file.size,
            file.chunks().len()
        );

        let snapshot = ctx.fresh_snapshot();
        for i in 0..=cli.repeats {
            let label = if i == 0 { "cold" } else { "warm" };
            let (cpu0, t) = (cpu_seconds(), Instant::now());
            std::hint::black_box(snapshot.read_full(&file.path).await?);
            println!(
                "{label} {i}: wall {:>9.3?}  cpu {:.2}s",
                t.elapsed(),
                cpu_seconds() - cpu0
            );
        }
        Ok(())
    })
}
