//! Steam CDN-backed chunk store.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use steam_vent_depot::{Chunk, ChunkHash, DepotKey, Manifest};
use tokio::sync::OnceCell;

use super::ChunkStore;
use crate::auth::SteamAuth;
use crate::error::{Result, VfsError};

/// [`ChunkStore`] backed by Steam's CDN.
///
/// Both the connection and the depot key are resolved lazily on first
/// `get()`. Constructing a `CdnChunkStore` doesn't touch the network —
/// the manifest-cache hit path can build one and never resolve auth if
/// no chunk is ever read.
pub struct CdnChunkStore<A: SteamAuth> {
    auth: Arc<A>,
    app_id: u32,
    depot_id: u32,
    /// Resolved on first chunk fetch via [`SteamAuth::resolve`] +
    /// [`DepotClient::depot_key`]. Shared (via [`Arc`]) with the
    /// owning [`crate::DepotStore`] so its in-process cache stays
    /// consistent across stores for the same `(app_id, depot_id)`.
    depot_key: Arc<OnceCell<DepotKey>>,
    /// Lookup: sha -> chunk descriptor (size + crc) needed by `fetch_chunk`.
    chunk_index: HashMap<ChunkHash, Chunk>,
}

impl<A: SteamAuth> CdnChunkStore<A> {
    pub fn new(
        auth: Arc<A>,
        app_id: u32,
        depot_id: u32,
        depot_key: Arc<OnceCell<DepotKey>>,
        manifest: &Manifest,
    ) -> Self {
        let mut chunk_index = HashMap::new();
        for f in &manifest.files {
            for c in &f.chunks {
                chunk_index.entry(c.sha).or_insert_with(|| c.clone());
            }
        }
        Self {
            auth,
            app_id,
            depot_id,
            depot_key,
            chunk_index,
        }
    }

    async fn resolve_depot_key(&self) -> Result<&DepotKey> {
        self.depot_key
            .get_or_try_init(|| async {
                let ctx = self.auth.resolve().await?;
                tracing::info!(
                    app_id = self.app_id,
                    depot_id = self.depot_id,
                    "fetching depot key"
                );
                Ok::<_, VfsError>(ctx.client.depot_key(self.app_id, self.depot_id).await?)
            })
            .await
    }
}

impl<A: SteamAuth> ChunkStore for CdnChunkStore<A> {
    async fn get(&self, sha: ChunkHash) -> Result<Bytes> {
        let chunk = self
            .chunk_index
            .get(&sha)
            .ok_or_else(|| VfsError::ChunkNotInManifest(sha))?;
        let depot_key = self.resolve_depot_key().await?;
        let ctx = self.auth.resolve().await?;
        tracing::info!(
            %sha,
            size_compressed = chunk.size_compressed,
            "fetching chunk from steam cdn"
        );
        let bytes = ctx
            .client
            .fetch_chunk(&ctx.cdn_servers, self.depot_id, chunk, depot_key)
            .await?;
        Ok(Bytes::from(bytes))
    }
}
