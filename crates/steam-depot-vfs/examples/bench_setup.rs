// TODO(ai-review): review for style and correctness
//! One-time fixture bootstrap for the store benchmarks: samples real chunks
//! from the on-disk store, fetches their CDN form, and writes
//! raw/container/plaintext triples plus an `index.json` to the fixture dir.
//!
//! Needs Steam credentials in the environment (`STEAM_USERNAME`,
//! `STEAM_PASSWORD`, e.g. via direnv) and a cached refresh token; the
//! manifest must already be in the store cache (open it in the viewer
//! once first).
//!
//! ```text
//! set -a; source ../steam-multiversion-viewer/.env; set +a
//! cargo run -p steam-depot-vfs --example bench_setup -- --help
//! ```

use std::collections::HashSet;
use std::time::Instant;

use anyhow::{Context as _, Result, bail};
use clap::Parser;
use sha1::{Digest as _, Sha1};
use steam_depot_vfs::SteamAuth as _;
use steam_depot_vfs::session::LazyCachedAuth;
use steam_vent_depot::{CdnServer, Chunk};

#[path = "common/mod.rs"]
mod store_common;
#[path = "../benches/support.rs"]
mod support;

#[derive(Parser)]
/// Sample chunks from the store and fetch their CDN form as benchmark
/// fixtures.
struct Cli {
    /// How many chunks to sample.
    #[arg(long, default_value_t = 64)]
    sample: usize,
    /// Fixture dir; defaults to the steam-depot-vfs cache dir.
    #[arg(long)]
    fixtures: Option<std::path::PathBuf>,
    /// Manifest gid to read.
    #[arg(long, default_value_t = store_common::PINNED_GID)]
    manifest_gid: u64,
    /// Store root; defaults to the steam-multiversion-viewer store.
    #[arg(long)]
    store: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let ctx = store_common::StoreCtx::open(cli.store.as_deref(), cli.manifest_gid)?;
    let manifest = ctx.manifest();
    let manifest_gid = manifest.manifest_id;
    let sample = cli.sample;

    // Unique chunks of the manifest that are present in the local store;
    // their plaintext is the ground truth the containers must decode to.
    let chunks_root = ctx.chunks_root();
    let mut seen = HashSet::new();
    let mut present = Vec::new();
    for f in &manifest.files {
        for c in f.chunks() {
            if !seen.insert(c.sha) {
                continue;
            }
            if chunks_root.join(c.sha.to_string()).try_exists()? {
                present.push(c.clone());
            }
        }
    }
    if present.is_empty() {
        bail!("no chunks of this manifest are present in the store");
    }
    let stride = (present.len() / sample).max(1);
    let sampled: Vec<&Chunk> = present.iter().step_by(stride).take(sample).collect();
    println!(
        "sampling {} of {} present chunks of manifest {manifest_gid}",
        sampled.len(),
        present.len(),
    );

    let account = std::env::var("STEAM_USERNAME").context("STEAM_USERNAME")?;
    let password = std::env::var("STEAM_PASSWORD").context("STEAM_PASSWORD")?;
    let auth = LazyCachedAuth::prepare(
        LazyCachedAuth::default_refresh_token_cache(),
        account,
        password,
    )
    .await?;
    let session = auth.resolve().await?;
    let depot_key = session.client.depot_key(ctx.app_id, ctx.depot_id).await?;
    let http = reqwest::Client::builder().build()?;

    let dir = cli.fixtures.unwrap_or_else(support::fixture_dir);
    std::fs::create_dir_all(&dir)?;
    println!("fixtures: {}", dir.display());

    let mut index = support::FixtureIndex {
        app_id: ctx.app_id,
        depot_id: ctx.depot_id,
        manifest_gid,
        chunks: Vec::new(),
    };
    let started = Instant::now();
    for (i, chunk) in sampled.iter().enumerate() {
        let raw = fetch_raw(&http, &session.cdn_servers, ctx.depot_id, chunk).await?;
        let container = support::decrypt_chunk(&raw, depot_key.as_bytes())?;
        let codec = support::classify(&container)?;

        let frame = std::fs::read(chunks_root.join(chunk.sha.to_string()))?;
        let (claimed, plain) = steam_depot_vfs::chunk_store::decode_frame(&frame)
            .ok_or_else(|| anyhow::anyhow!("stored frame of {} does not decode", chunk.sha))?;
        if claimed != chunk.sha {
            bail!("stored frame of {} names itself as {claimed}", chunk.sha);
        }
        // The frame chain says the decoded bytes hash to the name; this
        // ground-truth hash checks the chain itself against the manifest.
        let digest: [u8; 20] = Sha1::digest(&plain).into();
        if digest != chunk.sha.0 || plain.len() != chunk.size_uncompressed as usize {
            bail!("store chunk {} does not match the manifest", chunk.sha);
        }

        // Every decoder candidate must agree with the store plaintext.
        let decoded = match codec {
            support::Codec::Lzma => support::decode_vza_lzma_rs(&container)?,
            support::Codec::Zstd => support::decode_vsza(&container)?,
            support::Codec::Zip => support::decode_zip(&container)?,
        };
        if decoded != plain {
            bail!(
                "decoding the container of {} disagrees with the store plaintext",
                chunk.sha
            );
        }

        std::fs::write(dir.join(format!("{}.raw", chunk.sha)), &raw)?;
        std::fs::write(dir.join(format!("{}.con", chunk.sha)), &container)?;
        std::fs::write(dir.join(format!("{}.plain", chunk.sha)), &plain)?;

        // Size study: how zstd levels trade store size against encode cost.
        let mut zstd_sizes = [0u64; 4];
        for (slot, level) in [1, 3, 9, 19].into_iter().enumerate() {
            zstd_sizes[slot] = support::encode_zstd(&plain, level)?.len() as u64;
        }

        index.chunks.push(support::FixtureChunk {
            sha: chunk.sha.to_string(),
            codec: codec.as_str().to_owned(),
            size_uncompressed: plain.len() as u64,
            raw_len: raw.len() as u64,
            container_len: container.len() as u64,
            zstd_sizes,
        });
        if (i + 1) % 16 == 0 {
            println!(
                "  {} / {} chunks, {:.0}s elapsed",
                i + 1,
                sampled.len(),
                started.elapsed().as_secs_f64()
            );
        }
    }

    std::fs::write(
        dir.join("index.json"),
        serde_json::to_string_pretty(&index)?,
    )?;

    print_summary(&index);
    println!(
        "wrote {} fixtures in {:.0}s",
        index.chunks.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Try the CDN hosts in order until one serves the chunk.
async fn fetch_raw(
    http: &reqwest::Client,
    servers: &[CdnServer],
    depot_id: u32,
    chunk: &Chunk,
) -> Result<bytes::Bytes> {
    let sha_hex = chunk.sha.to_string();
    let mut last_err = String::new();
    for server in servers {
        let url = format!("{}/depot/{depot_id}/chunk/{sha_hex}", server.base_url());
        match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(resp.bytes().await?),
            Ok(resp) => last_err = format!("{}: HTTP {}", server.host, resp.status()),
            Err(e) => last_err = format!("{}: {e}", server.host),
        }
    }
    bail!("no CDN host would serve {sha_hex}: {last_err}")
}

fn print_summary(index: &support::FixtureIndex) {
    use std::collections::BTreeMap;
    // codec -> (count, container bytes, plaintext bytes, zstd[4] bytes)
    let mut per_codec: BTreeMap<&str, (usize, u64, u64, [u64; 4])> = BTreeMap::new();
    for c in &index.chunks {
        let e = per_codec.entry(c.codec.as_str()).or_default();
        e.0 += 1;
        e.1 += c.container_len;
        e.2 += c.size_uncompressed;
        for (slot, size) in e.3.iter_mut().zip(c.zstd_sizes) {
            *slot += size;
        }
    }
    println!("codec mix (sampled):");
    for (codec, (n, con, plain, zstd)) in &per_codec {
        println!(
            "  {codec:>5}: {n:>3} chunks  container/plain {con:>10}/{plain:<10} ({:.3})\n\
             \x20        zstd L1/L3/L9/L19 of plain: {}/{}/{}/{}",
            *con as f64 / *plain as f64,
            zstd[0],
            zstd[1],
            zstd[2],
            zstd[3],
        );
    }
}
