// TODO(ai-review): review for correctness/style
//! Browse a Steam depot manifest like a filesystem.

use std::path::PathBuf;
use std::{io::Write as _, sync::Arc};

use anyhow::Result;
use clap::{Parser, Subcommand};
use steam_depot_vfs::{
    DepotStore, FileKind, chunk_store::ChunkStore, fs::DepotManifestStore, session::LazyCachedAuth,
};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(about = "Browse a Steam depot manifest like a filesystem")]
struct Cli {
    /// Steam account name.
    account: String,
    /// Steam password (ignored if a refresh token for this account is cached).
    password: String,
    /// Steam app id (e.g. 1030300 for Silksong).
    app_id: u32,
    /// Depot id within the app (e.g. 1030303 for the Linux depot).
    depot_id: u32,
    /// Manifest GID — look these up on SteamDB.
    manifest_gid: u64,
    /// Branch the manifest belongs to; mostly only matters for restricted branches.
    #[arg(long, default_value = "public")]
    branch: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List a directory in the depot.
    Ls {
        /// Directory path; defaults to the root.
        #[arg(default_value = "/")]
        path: String,
    },
    /// Stream a single file from the depot to stdout.
    Cat {
        /// File path inside the depot.
        path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cli = Cli::parse();

    let store_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/vfs-store");

    let auth = LazyCachedAuth::prepare(
        LazyCachedAuth::default_refresh_token_cache(),
        cli.account,
        cli.password,
    )
    .await?;
    let vfs = DepotStore::new(store_root);
    let fs = vfs
        .open_depot_manifest(
            Arc::new(auth),
            cli.app_id,
            cli.depot_id,
            cli.manifest_gid,
            &cli.branch,
        )
        .await?;

    match cli.cmd {
        Cmd::Ls { path } => ls(&fs, &path)?,
        Cmd::Cat { path } => cat(&fs, &path).await?,
    }
    Ok(())
}

fn ls(fs: &DepotManifestStore<impl ChunkStore>, path: &str) -> Result<()> {
    let mut entries = fs.list_dir(path)?;
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    for e in entries {
        let marker = match e.meta.kind {
            FileKind::Directory => "d",
            FileKind::Symlink => "l",
            FileKind::File => "f",
        };
        let size = if matches!(e.meta.kind, FileKind::File) {
            human_bytes(e.meta.size)
        } else {
            "-".into()
        };
        println!("{marker} {:>10} {}", size, e.name);
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

async fn cat(fs: &DepotManifestStore<impl ChunkStore>, path: &str) -> Result<()> {
    let bytes = fs.read_full(path).await?;
    std::io::stdout().write_all(&bytes)?;
    Ok(())
}
