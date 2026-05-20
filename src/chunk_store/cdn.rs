//! Steam CDN-backed chunk store.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use steam_vent_depot::{CdnServer, DepotClient, DepotKey, Manifest};

use super::{BoxedError, ChunkStore};
use crate::sha::ChunkSha;

/// Chunk store backed by Steam's CDN via [`steam_vent_depot::DepotClient`].
pub struct SteamCdnChunkStore {
    pub client: Arc<DepotClient>,
    pub cdn_servers: Vec<CdnServer>,
    pub depot_id: u32,
    pub depot_key: DepotKey,
    /// Lookup: sha -> the [`steam_vent_depot::Chunk`] descriptor from the
    /// manifest. Required because `fetch_chunk` wants compressed size + crc,
    /// not just the sha.
    chunk_index: HashMap<ChunkSha, steam_vent_depot::Chunk>,
}

impl SteamCdnChunkStore {
    pub fn new(
        client: Arc<DepotClient>,
        cdn_servers: Vec<CdnServer>,
        depot_id: u32,
        depot_key: DepotKey,
        manifest: &Manifest,
    ) -> Self {
        let mut chunk_index = HashMap::new();
        for f in &manifest.files {
            for c in &f.chunks {
                chunk_index
                    .entry(ChunkSha(c.sha))
                    .or_insert_with(|| c.clone());
            }
        }
        Self {
            client,
            cdn_servers,
            depot_id,
            depot_key,
            chunk_index,
        }
    }
}

impl ChunkStore for SteamCdnChunkStore {
    fn get(&self, sha: ChunkSha) -> impl Future<Output = Result<Bytes, BoxedError>> + Send {
        let chunk = self.chunk_index.get(&sha).cloned();
        let client = Arc::clone(&self.client);
        let cdn = self.cdn_servers.clone();
        let depot_id = self.depot_id;
        let depot_key = self.depot_key.clone();
        async move {
            let chunk = chunk.ok_or_else(|| -> BoxedError {
                format!("chunk {sha} not in manifest index").into()
            })?;
            tracing::info!(
                %sha,
                size_compressed = chunk.size_compressed,
                "fetching chunk from steam cdn"
            );
            let bytes = client
                .fetch_chunk(&cdn, depot_id, &chunk, &depot_key)
                .await
                .map_err(|e| -> BoxedError { Box::new(e) })?;
            Ok(Bytes::from(bytes))
        }
    }
}
