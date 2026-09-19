// TODO(ai-review): review for style and correctness
//! Shared setup for the store-pattern examples, `bench_setup`, and the
//! e2e bench: open the real store offline for one pinned manifest.
//! Targets are pinned, not picked, so runs measure the same thing every
//! time; a pinned target that is not in the store is an error, not a
//! silent fallback to something else.
//!
//! The chunks of every exercised file must be on disk; a miss is a setup
//! error, not something to fetch. A store that refuses to fall back keeps
//! the tools honest.

// Every consumer compiles this module but uses a different subset of it.
#![allow(dead_code)]

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use bytes::Bytes;
use steam_depot_vfs::chunk_store::{ChunkStore, FsCacheStore};
use steam_depot_vfs::fs::DepotManifestStore;
use steam_depot_vfs::{ChunkHash, DepotStore, VfsError};
use steam_vent_depot::{DepotFile, Manifest};

/// Store-backed snapshot with no fallback fetch: reads only ever hit the
/// local cache.
pub struct MissingChunks;

impl ChunkStore for MissingChunks {
    async fn get(&self, sha: ChunkHash) -> Result<Bytes, VfsError> {
        Err(VfsError::Other(
            format!("chunk {sha} missing; store tools need fully-present files").into(),
        ))
    }
}

pub type Snapshot = DepotManifestStore<FsCacheStore<MissingChunks>>;

/// Manifest everything here measures. Pinned, not picked, so numbers
/// are comparable over time.
pub const PINNED_GID: u64 = 4421626056705534276;
/// The single-file patterns' target: the manifest's largest bundle.
pub const PINNED_FILE: &str = "Hollow Knight Silksong_Data/StreamingAssets/aa/StandaloneWindows64/tk2dcollections_assets_areacoral.bundle";
/// Byte budget of the bulk patterns' pinned set (sweep/stream): the
/// manifest's largest files until this much plaintext. Comfortably
/// above the decoded LRU so the steady state still measures decode
/// under eviction, and far below the whole manifest so the one-time
/// prefetch stays a fraction of the full depot.
pub const BULK_BUDGET: u64 = 512 << 20;

pub struct StoreCtx {
    store: DepotStore,
    pub app_id: u32,
    pub depot_id: u32,
    manifest: Arc<Manifest>,
    chunks_root: PathBuf,
    /// Indices into [`Manifest::files`], largest file first.
    present_files: Vec<usize>,
}

impl StoreCtx {
    /// Open the viewer's store (or the given root) for `gid`. The
    /// manifest must be cached; its fully-present files are what the
    /// pattern tools read. Targets are pinned, not picked, so runs
    /// measure the same thing every time.
    pub fn open(store_root: Option<&Path>, gid: u64) -> Result<Self> {
        let root = store_root
            .map(Path::to_path_buf)
            .unwrap_or_else(default_store_root);
        let store = DepotStore::new(root);

        let mut present = HashSet::new();
        for sha in store.list_chunks()? {
            present.insert(sha?);
        }

        let Some((app_id, depot_id, _)) = store
            .list_manifests()?
            .into_iter()
            .find(|(_, _, listed)| *listed == gid)
        else {
            bail!("manifest {gid} is not in the store cache; open it in the viewer first");
        };
        let manifest = store
            .load_cached_manifest(app_id, depot_id, gid)?
            .with_context(|| format!("manifest {gid} is listed but does not load"))?;
        let present_files = present_file_indices(&manifest, &present);
        println!(
            "manifest {}: {} fully-present files",
            manifest.manifest_id,
            present_files.len()
        );
        let chunks_root = store.chunks_root();
        Ok(Self {
            store,
            app_id,
            depot_id,
            manifest: Arc::new(manifest),
            chunks_root,
            present_files,
        })
    }

    pub fn manifest(&self) -> &Arc<Manifest> {
        &self.manifest
    }

    /// The store the ctx was opened from. The measuring tools never use
    /// it — `store_prefetch` does, to fill the store through the real
    /// fetch path.
    pub fn store(&self) -> &DepotStore {
        &self.store
    }

    pub fn chunks_root(&self) -> &Path {
        &self.chunks_root
    }

    /// A fresh snapshot: empty decoded cache, like a process start.
    pub fn fresh_snapshot(&self) -> Snapshot {
        DepotManifestStore::new(
            Arc::clone(&self.manifest),
            FsCacheStore::new(MissingChunks, self.chunks_root.clone()),
        )
    }

    /// The fully-present manifest file at `path`.
    pub fn find_file(&self, path: &str) -> Result<&'_ DepotFile> {
        self.present_files()
            .find(|f| f.path == path)
            .with_context(|| format!("{path:?} is not fully present in the store"))
    }

    /// Manifest files whose chunks are all on disk, largest first.
    pub fn present_files(&self) -> impl Iterator<Item = &'_ DepotFile> + '_ {
        self.present_files.iter().map(|&i| &self.manifest.files[i])
    }

    /// The manifest file at `path`, regardless of whether its chunks
    /// are on disk — the prefetch tool resolves its targets this way.
    pub fn file(&self, path: &str) -> Result<&'_ DepotFile> {
        self.manifest
            .files
            .iter()
            .find(|f| f.path == path)
            .with_context(|| format!("{path:?} is not in the manifest"))
    }

    /// The bulk patterns' pinned set: the manifest's largest files
    /// until [`BULK_BUDGET`] bytes of plaintext. Deterministic from the
    /// manifest alone, so sweep/stream numbers are comparable over
    /// time; `store_prefetch` downloads exactly this set's chunks.
    pub fn bulk_files(&self) -> Vec<&'_ DepotFile> {
        let mut files: Vec<&DepotFile> = self
            .manifest
            .files
            .iter()
            .filter(|f| f.is_file() && f.size > 0)
            .collect();
        files.sort_by_key(|f| std::cmp::Reverse(f.size));
        let mut total = 0;
        files
            .into_iter()
            .take_while(|f| {
                let keep = total < BULK_BUDGET;
                total += f.size;
                keep
            })
            .collect()
    }

    /// Fail loudly — with the fix hint — unless every chunk of `files`
    /// is on disk. The measuring tools call this up front so a missing
    /// chunk surfaces as a setup error, not a mid-measurement panic.
    pub fn require_complete(&self, files: &[&'_ DepotFile]) -> Result<()> {
        let root = self.chunks_root();
        let mut seen = HashSet::new();
        let mut missing = 0usize;
        for f in files {
            for c in f.chunks() {
                if seen.insert(c.sha) && !root.join(c.sha.to_string()).try_exists()? {
                    missing += 1;
                }
            }
        }
        if missing > 0 {
            bail!(
                "{missing} chunks of the pinned measurement set are missing; \
                 fill them with `cargo run --release -p steam-depot-vfs --example store_prefetch`"
            );
        }
        Ok(())
    }
}

fn present_file_indices(manifest: &Manifest, present: &HashSet<ChunkHash>) -> Vec<usize> {
    let mut idxs: Vec<usize> = manifest
        .files
        .iter()
        .enumerate()
        .filter(|(_, f)| f.is_file())
        .filter(|(_, f)| f.chunks().iter().all(|c| present.contains(&c.sha)))
        .map(|(i, _)| i)
        .collect();
    idxs.sort_by_key(|&i| std::cmp::Reverse(manifest.files[i].size));
    idxs
}

pub fn default_store_root() -> PathBuf {
    directories::ProjectDirs::from("", "", "steam-multiversion-viewer")
        .expect("no home directory")
        .data_dir()
        .join("store")
}

/// CPU seconds (user+system) of this process, from `/proc/self/stat`.
/// Linux-only — fine for profiling tools, not for portability.
pub fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("procfs");
    // `pid (comm) state …` — everything after the final `)` starts with
    // a space, so without the trim every index shifts by one and
    // `fields[11]` would read `cmajflt`, not `utime`.
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .expect("comm field")
        .1
        .split_whitespace()
        .collect();
    let utime: u64 = fields[11].parse().expect("utime");
    let stime: u64 = fields[12].parse().expect("stime");
    (utime + stime) as f64 / 100.0
}
