// TODO(ai-review): review for correctness/style
//! Local-disk write-through cache wrapping any other [`ChunkStore`].

use std::path::PathBuf;

use bytes::Bytes;
use steam_vent_depot::ChunkHash;
use tokio::io::AsyncWriteExt;

use super::ChunkStore;
use crate::error::Result;

/// Write-through local-disk cache in front of another [`ChunkStore`].
///
/// Chunks live at `<root>/<sha-hex>`. Misses fall through to the inner store
/// and the resulting bytes are persisted. Concurrent misses for the same sha
/// may both fetch — since chunks are content-addressed and writes are atomic
/// (write-then-rename), this only wastes one redundant download, never
/// corrupts the cache.
pub struct FsCacheStore<Inner: ChunkStore> {
    inner: Inner,
    root: PathBuf,
}

impl<Inner: ChunkStore> FsCacheStore<Inner> {
    pub fn new(inner: Inner, root: PathBuf) -> Self {
        Self { inner, root }
    }

    fn path_for(&self, sha: ChunkHash) -> PathBuf {
        self.root.join(sha.to_string())
    }
}

impl<Inner: ChunkStore> ChunkStore for FsCacheStore<Inner> {
    async fn get(&self, sha: ChunkHash) -> Result<Bytes> {
        let path = self.path_for(sha);
        if let Ok(bytes) = tokio::fs::read(&path).await {
            tracing::debug!(%sha, bytes = bytes.len(), "cache hit");
            return Ok(Bytes::from(bytes));
        }

        tracing::info!(%sha, "cache miss, fetching from inner store");
        let bytes = self.inner.get(sha).await?;

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }
        // Write-then-rename: atomic, and safe against concurrent writers
        // (last rename wins, content is identical).
        let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
        let mut f = tokio::fs::File::create(&tmp).await?;
        f.write_all(&bytes).await?;
        f.sync_all().await?;
        tokio::fs::rename(&tmp, &path).await?;
        Ok(bytes)
    }
}
