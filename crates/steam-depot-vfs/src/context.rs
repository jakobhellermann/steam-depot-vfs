// TODO(ai-review): review for correctness/style
//! High-level entry point that wires up auth, manifest cache, and chunk cache.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use steam_vent_depot::{ChunkHash, DepotKey};
use tokio::sync::OnceCell;

use crate::auth::SteamAuth;
use crate::chunk_store::{CdnChunkStore, FsCacheStore};
use crate::error::Result;
use crate::fs::DepotSnapshot;
use crate::manifest_cache::ManifestCache;

/// Cache root that ties manifests and chunks to a single directory.
///
/// One `DepotStore` is meant to outlive many [`DepotSnapshot`] instances: every
/// [`open_depot_manifest`](Self::open_depot_manifest) call writes into the same chunk cache, so identical
/// chunks across manifests/branches are downloaded at most once. Depot
/// keys are likewise fetched at most once per process, in memory only.
///
/// On-disk layout:
/// - `<root>/manifests/<depot_id>/<gid>.postcard` — parsed manifest cache.
/// - `<root>/chunks/<sha-hex>`                    — chunk content cache.
pub struct DepotStore {
    root: PathBuf,
    manifests: ManifestCache,
    /// In-process depot key cache, keyed by `(app_id, depot_id)`. The
    /// stored `OnceCell` is empty until something actually needs the
    /// key — handed out to [`CdnChunkStore`]s so the cache stays
    /// coherent if multiple stores point at the same depot.
    /// Persisting these across restarts is future work.
    depot_keys: Mutex<HashMap<(u32, u32), Arc<OnceCell<DepotKey>>>>,
}

impl DepotStore {
    pub fn new(root: PathBuf) -> Self {
        let manifests = ManifestCache::new(root.join("manifests"));
        Self {
            root,
            manifests,
            depot_keys: Mutex::new(HashMap::new()),
        }
    }

    /// Open a depot manifest by gid, with both manifest and chunks cached on
    /// local disk. Auth is consulted only when something has to be fetched
    /// from Steam — load-from-cache paths stay offline.
    pub async fn open_depot_manifest<A: SteamAuth + 'static>(
        &self,
        auth: Arc<A>,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        branch: &str,
    ) -> Result<DepotSnapshot<FsCacheStore<CdnChunkStore<A>>>> {
        let depot_key_cell = self.depot_key_cell(app_id, depot_id);
        let manifest = self
            .manifests
            .get_or_fetch(depot_id, manifest_gid, || async {
                let ctx = auth.resolve().await?;
                let depot_key = depot_key_cell
                    .get_or_try_init(|| async {
                        tracing::info!(app_id, depot_id, "fetching depot key");
                        Ok::<_, crate::VfsError>(ctx.client.depot_key(app_id, depot_id).await?)
                    })
                    .await?;
                tracing::info!(depot_id, manifest_gid, branch, "fetching manifest");
                let code = ctx
                    .client
                    .manifest_request_code(app_id, depot_id, manifest_gid, branch)
                    .await?;
                let m = ctx
                    .client
                    .fetch_manifest(&ctx.cdn_servers, depot_id, manifest_gid, code, depot_key)
                    .await?;
                Ok::<_, crate::VfsError>(m)
            })
            .await?;
        let manifest = Arc::new(manifest);
        let cdn_store = CdnChunkStore::new(
            Arc::clone(&auth),
            app_id,
            depot_id,
            Arc::clone(&depot_key_cell),
            &manifest,
        );
        let chunks = FsCacheStore::new(cdn_store, self.root.join("chunks"));
        Ok(DepotSnapshot::new(manifest, chunks))
    }

    /// Load a manifest from the on-disk cache without touching Steam.
    /// Returns `None` if it's not cached.
    pub fn load_cached_manifest(
        &self,
        depot_id: u32,
        manifest_gid: u64,
    ) -> Result<Option<steam_vent_depot::Manifest>> {
        Ok(self.manifests.load(depot_id, manifest_gid)?)
    }

    /// Root directory of the on-disk chunk cache. Useful for stats /
    /// cache management tools.
    pub fn chunks_root(&self) -> PathBuf {
        self.root.join("chunks")
    }

    /// Root directory of the on-disk manifest postcard cache. Layout
    /// underneath is `<manifests_root>/<depot_id>/<gid>.postcard`.
    pub fn manifests_root(&self) -> PathBuf {
        self.root.join("manifests")
    }

    /// Enumerate every chunk currently stored on disk. Stray files whose
    /// names don't parse as a hex chunk hash are silently skipped. Returns
    /// an empty iterator if the chunks dir doesn't exist yet.
    pub fn list_chunks(&self) -> std::io::Result<impl Iterator<Item = std::io::Result<ChunkHash>>> {
        let dir = self.chunks_root();
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => Some(e),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        };
        Ok(entries.into_iter().flatten().filter_map(|res| match res {
            Err(e) => Some(Err(e)),
            Ok(entry) => entry
                .file_name()
                .to_str()
                .and_then(ChunkHash::from_hex)
                .map(Ok),
        }))
    }

    /// Enumerate every cached manifest as `(depot_id, manifest_gid)` pairs.
    /// Stray files / directories with non-numeric names are silently
    /// skipped. Returns an empty vec if the manifests dir doesn't exist yet.
    pub fn list_manifests(&self) -> std::io::Result<Vec<(u32, u64)>> {
        let root = self.manifests_root();
        let depot_entries = match std::fs::read_dir(&root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for depot_entry in depot_entries {
            let depot_entry = depot_entry?;
            let Some(depot_id) = depot_entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            for manifest_entry in std::fs::read_dir(depot_entry.path())? {
                let manifest_entry = manifest_entry?;
                let Some(gid) = manifest_entry
                    .file_name()
                    .to_str()
                    .and_then(|s| s.strip_suffix(".postcard"))
                    .and_then(|s| s.parse::<u64>().ok())
                else {
                    continue;
                };
                out.push((depot_id, gid));
            }
        }
        Ok(out)
    }

    /// Get-or-create the shared [`OnceCell`] for a depot's key. The
    /// cell itself is empty until something actually awaits on it.
    fn depot_key_cell(&self, app_id: u32, depot_id: u32) -> Arc<OnceCell<DepotKey>> {
        self.depot_keys
            .lock()
            .expect("depot_keys mutex poisoned")
            .entry((app_id, depot_id))
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone()
    }
}
