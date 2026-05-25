// TODO(ai-review): review for correctness/style
//! Steam CDN-backed chunk store.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use steam_vent_depot::{Chunk, ChunkHash, Manifest};

use super::ChunkStore;
use crate::auth::SteamAuth;
use crate::depot_key_cache::LazyDepotKey;
use crate::error::{Result, VfsError};

/// [`ChunkStore`] backed by Steam's CDN.
///
/// Both the connection and the depot key are resolved lazily on first
/// `get()`. Constructing a `CdnChunkStore` doesn't touch the network —
/// the manifest-cache hit path can build one and never resolve auth if
/// no chunk is ever read.
pub struct CdnChunkStore<A: SteamAuth> {
    auth: Arc<A>,
    depot_id: u32,
    depot_key: LazyDepotKey,
    /// Lookup: sha -> chunk descriptor (size + crc) needed by `fetch_chunk`.
    chunk_index: HashMap<ChunkHash, Chunk>,
}

impl<A: SteamAuth> CdnChunkStore<A> {
    pub fn new(auth: Arc<A>, depot_id: u32, depot_key: LazyDepotKey, manifest: &Manifest) -> Self {
        let mut chunk_index = HashMap::new();
        for f in &manifest.files {
            for c in &f.chunks {
                chunk_index.entry(c.sha).or_insert_with(|| c.clone());
            }
        }
        Self {
            auth,
            depot_id,
            depot_key,
            chunk_index,
        }
    }
}

impl<A: SteamAuth> ChunkStore for CdnChunkStore<A> {
    #[tracing::instrument(name = "cdn.get", skip(self), fields(%sha))]
    async fn get(&self, sha: ChunkHash) -> Result<Bytes> {
        let chunk = self
            .chunk_index
            .get(&sha)
            .ok_or_else(|| VfsError::ChunkNotInManifest(sha))?;
        let depot_key = self.depot_key.get(&*self.auth).await?;
        let ctx = self.auth.resolve().await?;
        tracing::debug!(
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
