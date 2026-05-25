// TODO(ai-review): review for correctness/style
//! High-level entry point that wires up auth, manifest cache, and chunk cache.

use std::path::PathBuf;
use std::sync::Arc;

use steam_vent_depot::{ChunkHash, Manifest};

use crate::auth::SteamAuth;
use crate::chunk_store::{CdnChunkStore, FsCacheStore};
use crate::depot_key_cache::{DepotKeyMemoryCache, LazyDepotKey};
use crate::error::Result;
use crate::fs::DepotManifestStore;
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
    depot_keys: DepotKeyMemoryCache,
}

impl DepotStore {
    pub fn new(root: PathBuf) -> Self {
        let manifests = ManifestCache::new(root.join("manifests"));
        Self {
            root,
            manifests,
            depot_keys: DepotKeyMemoryCache::new(),
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
    ) -> Result<DepotManifestStore<FsCacheStore<CdnChunkStore<A>>>> {
        let depot_key = self.depot_keys.get_lazy(app_id, depot_id);
        let manifest = self
            .get_manifest(&auth, app_id, depot_id, manifest_gid, branch, &depot_key)
            .await?;
        let manifest = Arc::new(manifest);
        let cdn_store = CdnChunkStore::new(Arc::clone(&auth), depot_id, depot_key, &manifest);
        let chunks = FsCacheStore::new(cdn_store, self.root.join("chunks"));
        Ok(DepotManifestStore::new(manifest, chunks))
    }

    async fn get_manifest<A: SteamAuth + 'static>(
        &self,
        auth: &Arc<A>,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        branch: &str,
        depot_key: &LazyDepotKey,
    ) -> Result<Manifest, crate::VfsError> {
        let manifest = self
            .manifests
            .get_or_fetch(
                app_id,
                depot_id,
                manifest_gid,
                async move || -> Result<_> {
                    let depot_key = depot_key.get(&**auth).await?;
                    let ctx = auth.resolve().await?;
                    tracing::info!(depot_id, manifest_gid, branch, "fetching manifest");
                    let m = ctx
                        .client
                        .fetch_manifest(
                            &ctx.cdn_servers,
                            app_id,
                            depot_id,
                            manifest_gid,
                            branch,
                            depot_key,
                        )
                        .await?;
                    Ok(m)
                },
            )
            .await?;
        Ok(manifest)
    }

    /// Load a manifest from the on-disk cache without touching Steam.
    /// Returns `None` if it's not cached.
    pub fn load_cached_manifest(
        &self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
    ) -> Result<Option<steam_vent_depot::Manifest>> {
        Ok(self.manifests.load(app_id, depot_id, manifest_gid)?)
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
    pub fn list_chunks(
        &self,
    ) -> Result<impl Iterator<Item = Result<ChunkHash, std::io::Error>>, std::io::Error> {
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

    /// Enumerate every cached manifest as `(app_id, depot_id, manifest_gid)`
    /// triples. Stray files / directories with non-numeric names are
    /// silently skipped. Returns an empty vec if the manifests dir
    /// doesn't exist yet.
    pub fn list_manifests(&self) -> Result<Vec<(u32, u32, u64)>, std::io::Error> {
        let root = self.manifests_root();
        let app_entries = match std::fs::read_dir(&root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for app_entry in app_entries {
            let app_entry = app_entry?;
            let Some(app_id) = app_entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<u32>().ok())
            else {
                continue;
            };
            for depot_entry in std::fs::read_dir(app_entry.path())? {
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
                    out.push((app_id, depot_id, gid));
                }
            }
        }
        Ok(out)
    }
}
