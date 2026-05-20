//! High-level entry point that wires up auth, manifest cache, and chunk cache.

use std::path::PathBuf;
use std::sync::Arc;

use crate::auth::DepotAuth;
use crate::chunk_store::{CdnChunkStore, FsCacheStore};
use crate::error::Result;
use crate::fs::DepotSnapshot;
use crate::manifest_cache::ManifestCache;

/// Cache root that ties manifests and chunks to a single directory.
///
/// One `DepotStore` is meant to outlive many [`DepotSnapshot`] instances: every
/// [`open_depot_manifest`](Self::open_depot_manifest) call writes into the same chunk cache, so identical
/// chunks across manifests/branches are downloaded at most once.
///
/// On-disk layout:
/// - `<root>/manifests/<depot_id>/<gid>.postcard` — parsed manifest cache.
/// - `<root>/chunks/<sha-hex>`                    — chunk content cache.
pub struct DepotStore {
    root: PathBuf,
    manifests: ManifestCache,
}

impl DepotStore {
    pub fn new(root: PathBuf) -> Self {
        let manifests = ManifestCache::new(root.join("manifests"));
        Self { root, manifests }
    }

    /// Open a depot manifest by gid, with both manifest and chunks cached on
    /// local disk. Auth is consulted only when something has to be fetched
    /// from Steam — load-from-cache paths stay offline.
    pub async fn open_depot_manifest<A: DepotAuth + 'static>(
        &self,
        auth: Arc<A>,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        branch: &str,
    ) -> Result<DepotSnapshot<FsCacheStore<CdnChunkStore<A>>>> {
        let manifest = self
            .manifests
            .get_or_fetch(depot_id, manifest_gid, || async {
                let ctx = auth.resolve().await?;
                tracing::info!(depot_id, manifest_gid, branch, "fetching manifest");
                let code = ctx
                    .client
                    .manifest_request_code(app_id, depot_id, manifest_gid, branch)
                    .await?;
                let m = ctx
                    .client
                    .fetch_manifest(
                        &ctx.cdn_servers,
                        depot_id,
                        manifest_gid,
                        code,
                        &ctx.depot_key,
                    )
                    .await?;
                Ok::<_, crate::VfsError>(m)
            })
            .await?;
        let manifest = Arc::new(manifest);
        let cdn_store = CdnChunkStore::new(Arc::clone(&auth), depot_id, &manifest);
        let chunks = FsCacheStore::new(cdn_store, self.root.join("chunks"));
        Ok(DepotSnapshot::new(manifest, chunks))
    }
}
