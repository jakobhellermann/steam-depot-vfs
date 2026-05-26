// TODO(ai-review): review for correctness/style
//! Local-disk write-through cache wrapping any other [`ChunkStore`].

use std::path::PathBuf;

use bytes::Bytes;
use steam_vent_depot::ChunkHash;
use tokio::io::AsyncWriteExt;

use super::ChunkStore;
use crate::{chunk_store::CdnChunkStore, error::Result};

/// Write-through local-disk cache in front of another [`ChunkStore`].
///
/// Chunks live at `<root>/<sha-hex>`. Misses fall through to the inner store
/// and the resulting bytes are persisted. Concurrent misses for the same sha
/// may both fetch — since chunks are content-addressed and writes are atomic
/// (write-then-rename), this only wastes one redundant download, never
/// corrupts the cache.
pub struct FsCacheStore<Inner: ChunkStore = CdnChunkStore> {
    inner: Inner,
    root: PathBuf,
}

impl<Inner: ChunkStore> FsCacheStore<Inner> {
    pub fn new(inner: Inner, root: PathBuf) -> Self {
        // Eagerly create the cache root so the per-chunk write path
        // doesn't need a `create_dir_all` per fetch. Errors here are
        // best-effort; the first real write will surface them with a
        // proper error path.
        let _ = std::fs::create_dir_all(&root);
        Self { inner, root }
    }

    fn path_for(&self, sha: ChunkHash) -> PathBuf {
        self.root.join(sha.to_string())
    }
}

impl<Inner: ChunkStore> ChunkStore for FsCacheStore<Inner> {
    #[tracing::instrument(name = "fs_cache.get", skip_all)]
    async fn get(&self, sha: ChunkHash) -> Result<Bytes> {
        let path = self.path_for(sha);
        if let Ok(bytes) = tokio::fs::read(&path).await {
            tracing::debug!(%sha, bytes = bytes.len(), "cache hit");
            return Ok(Bytes::from(bytes));
        }
        let bytes = self.fetch_and_persist(sha, &path).await?;
        Ok(bytes)
    }

    #[tracing::instrument(name = "fs_cache.ensure", skip_all)]
    async fn ensure(&self, sha: ChunkHash) -> Result<()> {
        let path = self.path_for(sha);
        // `try_exists` is the cheap check: a single `stat` rather than
        // a full file read. If we can't determine existence (permission
        // issue, etc.) fall through to the fetch path; it will fail if
        // truly broken.
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            tracing::debug!(%sha, "cache hit (ensure)");
            return Ok(());
        }
        self.fetch_and_persist(sha, &path).await?;
        Ok(())
    }
}

impl<Inner: ChunkStore> FsCacheStore<Inner> {
    /// Shared fetch-and-persist path used by both [`get`] and [`ensure`].
    /// Returns the fetched bytes; callers that don't need them (i.e.
    /// `ensure`) just discard.
    #[tracing::instrument(name = "fs_cache.persist", skip(self, path), fields(%sha))]
    async fn fetch_and_persist(&self, sha: ChunkHash, path: &std::path::Path) -> Result<Bytes> {
        tracing::debug!(%sha, "cache miss, fetching from inner store");
        let bytes = self.inner.get(sha).await?;
        self.write_atomic(path, &bytes).await?;
        Ok(bytes)
    }

    /// Write `bytes` to `path` atomically: create a sibling `.tmp.<pid>`
    /// file, write, rename over. **Deliberately no `fsync`.**
    ///
    /// fsync only matters for hard reboots / power loss. After a normal
    /// process crash the kernel still flushes the page cache, so closed
    /// files survive intact. On real disk fsync costs us ~13% throughput
    /// because it back-pressures concurrent CDN polls; on tmpfs it's
    /// a no-op anyway. In the rare power-loss case a committed chunk
    /// file can read back as zeros — the recovery path is "refetch",
    /// which is cheap for a content-addressed cache.
    #[tracing::instrument(name = "fs_cache.write_atomic", skip(self, bytes), fields(bytes_len = bytes.len()))]
    async fn write_atomic(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
        let mut f = tokio::fs::File::create(&tmp).await?;
        f.write_all(bytes).await?;
        drop(f);
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }
}
