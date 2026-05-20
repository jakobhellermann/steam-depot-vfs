//! Steam CDN-backed chunk store, parameterised over a [`DepotAuth`].

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use steam_vent_depot::{Chunk, ChunkHash, Manifest};

use super::ChunkStore;
use crate::auth::DepotAuth;
use crate::error::{Result, VfsError};

/// [`ChunkStore`] backed by Steam's CDN, parameterised over a [`DepotAuth`].
///
/// Auth (connection, depot key, CDN list) is resolved lazily via the supplied
/// [`DepotAuth`] — so this struct itself can be constructed without any Steam
/// roundtrips. Only the actual `get()` triggers auth resolution.
pub struct CdnChunkStore<A: DepotAuth> {
    auth: Arc<A>,
    depot_id: u32,
    /// Lookup: sha -> chunk descriptor (size + crc) needed by `fetch_chunk`.
    chunk_index: HashMap<ChunkHash, Chunk>,
}

impl<A: DepotAuth> CdnChunkStore<A> {
    pub fn new(auth: Arc<A>, depot_id: u32, manifest: &Manifest) -> Self {
        let mut chunk_index = HashMap::new();
        for f in &manifest.files {
            for c in &f.chunks {
                chunk_index.entry(c.sha).or_insert_with(|| c.clone());
            }
        }
        Self {
            auth,
            depot_id,
            chunk_index,
        }
    }
}

impl<A: DepotAuth> ChunkStore for CdnChunkStore<A> {
    async fn get(&self, sha: ChunkHash) -> Result<Bytes> {
        let chunk = self
            .chunk_index
            .get(&sha)
            .ok_or_else(|| VfsError::ChunkNotInManifest(sha))?;
        let ctx = self.auth.resolve().await?;
        tracing::info!(
            %sha,
            size_compressed = chunk.size_compressed,
            "fetching chunk from steam cdn"
        );
        let bytes = ctx
            .client
            .fetch_chunk(&ctx.cdn_servers, self.depot_id, chunk, &ctx.depot_key)
            .await?;
        Ok(Bytes::from(bytes))
    }
}
