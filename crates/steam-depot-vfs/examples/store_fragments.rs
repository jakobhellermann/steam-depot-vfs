// TODO(ai-review): review for style and correctness
//! Access pattern: fragmented reads — the FUSE mount shape as external
//! tools (rabex CLI, dd) see it: small range reads over the same
//! chunks. `--mode sequential` walks the file in fragment-sized reads;
//! `--mode random` seeks like a header-parsing tool.
//!
//! ```text
//! cargo run --release -p steam-depot-vfs --example store_fragments -- --help
//! ```

use std::time::Instant;

use anyhow::{Result, bail};
use clap::Parser;

#[path = "common/mod.rs"]
mod store_common;

use store_common::{StoreCtx, cpu_seconds};

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    Sequential,
    Random,
}

#[derive(Parser)]
/// Read one file in small range reads, cold once, then repeatedly
/// warm.
struct Cli {
    /// Read pattern over the file.
    #[arg(long, value_enum, default_value_t = Mode::Sequential)]
    mode: Mode,
    /// Range-read size in bytes.
    #[arg(long, default_value_t = 128 * 1024)]
    fragment: u64,
    /// Range reads per pass.
    #[arg(long, default_value_t = 2000)]
    reads: u64,
    /// Warm passes after the cold one.
    #[arg(long, default_value_t = 3)]
    passes: usize,
    /// Manifest path to read.
    #[arg(
        long,
        default_value = store_common::PINNED_FILE
    )]
    file: String,
    /// Manifest gid to read.
    #[arg(long, default_value_t = store_common::PINNED_GID)]
    manifest_gid: u64,
    /// Store root; defaults to the steam-multiversion-viewer store.
    #[arg(long)]
    store: Option<std::path::PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if cli.fragment == 0 {
        bail!("--fragment must not be 0");
    }
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let ctx = StoreCtx::open(cli.store.as_deref(), cli.manifest_gid)?;
        let file = ctx.find_file(&cli.file)?;
        let count = (file.size / cli.fragment).max(1);
        println!(
            "file: {} ({} B)\nmode: {:?}, fragment {} B, reads {}",
            file.path, file.size, cli.mode, cli.fragment, cli.reads
        );

        let mut rng: u64 = 0x9e3779b97f4a7c15;
        let snapshot = ctx.fresh_snapshot();
        for pass in 0..=cli.passes {
            let label = if pass == 0 { "cold" } else { "warm" };
            let (cpu0, t) = (cpu_seconds(), Instant::now());
            let mut bytes = 0u64;
            for i in 0..cli.reads {
                let (off, len) = match cli.mode {
                    Mode::Sequential => {
                        let at = (i % count) * cli.fragment;
                        (at, cli.fragment.min(file.size - at))
                    }
                    Mode::Random => {
                        // xorshift: cheap, deterministic, good enough for seeks.
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        let at = (rng % count) * cli.fragment;
                        (at, cli.fragment.min(file.size - at))
                    }
                };
                std::hint::black_box(snapshot.read(&file.path, off, len).await?);
                bytes += len;
            }
            let wall = t.elapsed();
            let cpu = cpu_seconds() - cpu0;
            println!(
                "{label} pass {pass}: wall {wall:>9.3?}  cpu {cpu:>5.2}s  {:.0} MB/s",
                bytes as f64 / 1e6 / wall.as_secs_f64()
            );
        }
        Ok(())
    })
}
