//! Local-disk write-through cache wrapping any other [`ChunkStore`].

use std::path::PathBuf;
use std::{future::Future, result::Result};

use bytes::Bytes;
use tokio::io::AsyncWriteExt;

use super::{BoxedError, ChunkStore};
use crate::sha::ChunkSha;

/// Chunks are stored at `<root>/<sha-hex>`. Misses go to the inner store and
/// write-through to disk. Concurrent misses for the same sha may both fetch —
/// since chunks are content-addressed and writes are atomic (write-then-rename),
/// this only wastes one redundant download, never corrupts the cache.
pub struct FsCacheStore<Inner: ChunkStore> {
    inner: Inner,
    root: PathBuf,
}

impl<Inner: ChunkStore> FsCacheStore<Inner> {
    pub fn new(inner: Inner, root: impl Into<PathBuf>) -> Self {
        Self {
            inner,
            root: root.into(),
        }
    }

    fn path_for(&self, sha: ChunkSha) -> PathBuf {
        self.root.join(sha.to_hex())
    }
}

impl<Inner: ChunkStore> ChunkStore for FsCacheStore<Inner> {
    fn get(&self, sha: ChunkSha) -> impl Future<Output = Result<Bytes, BoxedError>> + Send {
        async move {
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
}
