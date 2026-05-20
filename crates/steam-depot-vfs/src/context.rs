//! High-level entry point that wires up auth, manifest cache, and chunk cache.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use steam_vent_depot::DepotKey;
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
