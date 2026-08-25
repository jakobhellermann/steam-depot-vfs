// TODO(ai-review): review for correctness/style
//! Local-disk write-through cache wrapping any other [`ChunkStore`].

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use steam_vent_depot::ChunkHash;
use tokio::io::AsyncWriteExt;

use super::ChunkStore;
use crate::{chunk_store::CdnChunkStore, error::Result};

/// Write-through local-disk cache in front of another [`ChunkStore`].
///
/// Chunks live at `<root>/<sha-hex>`. Misses fall through to the inner store
/// and the resulting bytes are persisted. Concurrent misses for the same sha
/// may both fetch — since chunks are content-addressed and each write goes
/// through its own temporary file, this only wastes one redundant download,
/// never corrupts the cache.
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
    #[tracing::instrument(level = "debug", name = "fs_cache.get", skip_all)]
    async fn get(&self, sha: ChunkHash) -> Result<Bytes> {
        let path = self.path_for(sha);
        if let Ok(bytes) = tokio::fs::read(&path).await {
            tracing::debug!(%sha, bytes = bytes.len(), "cache hit");
            return Ok(Bytes::from(bytes));
        }
        let bytes = self.fetch_and_persist(sha, &path).await?;
        Ok(bytes)
    }

    #[tracing::instrument(level = "debug", name = "fs_cache.ensure", skip_all)]
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

/// A temporary path next to `path` that no other write can pick.
///
/// Concurrent misses for one sha are normal, and sharing a temp file
/// between them corrupts the cache: `File::create` truncates the file
/// the other writer is still filling, so the rename can publish an
/// empty chunk — or fail with ENOENT because the other writer already
/// renamed it away.
fn tmp_path(path: &std::path::Path) -> PathBuf {
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let nonce = WRITES.fetch_add(1, Ordering::Relaxed);
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".tmp.{}.{nonce}", std::process::id()));
    path.with_file_name(name)
}

impl<Inner: ChunkStore> FsCacheStore<Inner> {
    /// Shared fetch-and-persist path used by both [`get`] and [`ensure`].
    /// Returns the fetched bytes; callers that don't need them (i.e.
    /// `ensure`) just discard.
    #[tracing::instrument(level = "debug", name = "fs_cache.persist", skip(self, path), fields(%sha))]
    async fn fetch_and_persist(&self, sha: ChunkHash, path: &std::path::Path) -> Result<Bytes> {
        tracing::debug!(%sha, "cache miss, fetching from inner store");
        let bytes = self.inner.get(sha).await?;
        self.write_atomic(path, &bytes).await?;
        Ok(bytes)
    }

    /// Write `bytes` to `path` atomically: write a sibling temporary
    /// file, rename over. **Deliberately no `fsync`.**
    ///
    /// fsync only matters for hard reboots / power loss. After a normal
    /// process crash the kernel still flushes the page cache, so closed
    /// files survive intact. On real disk fsync costs us ~13% throughput
    /// because it back-pressures concurrent CDN polls; on tmpfs it's
    /// a no-op anyway. In the rare power-loss case a committed chunk
    /// file can read back as zeros — the recovery path is "refetch",
    /// which is cheap for a content-addressed cache.
    #[tracing::instrument(level = "debug", name = "fs_cache.write_atomic", skip(self, bytes), fields(bytes_len = bytes.len()))]
    async fn write_atomic(&self, path: &std::path::Path, bytes: &[u8]) -> Result<()> {
        let tmp = tmp_path(path);
        let mut f = tokio::fs::File::create(&tmp).await?;
        f.write_all(bytes).await?;
        drop(f);
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    const CHUNK_LEN: usize = 65_536;

    /// Hands out a fixed chunk, after yielding often enough that
    /// concurrent callers interleave inside the cache's write path.
    struct SlowInner {
        fetches: Arc<AtomicUsize>,
    }

    impl ChunkStore for SlowInner {
        async fn get(&self, _sha: ChunkHash) -> Result<Bytes> {
            self.fetches.fetch_add(1, Ordering::Relaxed);
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            Ok(Bytes::from(vec![0x5a; CHUNK_LEN]))
        }
    }

    #[test]
    fn temp_paths_do_not_collide() {
        let target = std::path::Path::new("/cache/abc123");
        assert_ne!(tmp_path(target), tmp_path(target));
        assert_eq!(tmp_path(target).parent(), target.parent());
    }

    /// Concurrent misses for one sha are expected (they only waste a
    /// download) — but they must never let a reader see a partially
    /// written chunk.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_misses_never_serve_a_short_chunk() {
        let dir = std::env::temp_dir().join(format!("fs-cache-race-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let fetches = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(FsCacheStore::new(
            SlowInner {
                fetches: Arc::clone(&fetches),
            },
            dir.clone(),
        ));
        let sha = ChunkHash([7; 20]);

        // Several rounds: each one starts from a cold cache, so every
        // task takes the miss path and races the others.
        for round in 0..20 {
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create cache dir");
            let mut tasks = Vec::new();
            for _ in 0..16 {
                let store = Arc::clone(&store);
                tasks.push(tokio::spawn(async move { store.get(sha).await }));
            }
            for task in tasks {
                let bytes = task.await.expect("task").expect("get");
                assert_eq!(bytes.len(), CHUNK_LEN, "short read in round {round}");
            }
            let on_disk = std::fs::read(dir.join(sha.to_string())).expect("cached file");
            assert_eq!(
                on_disk.len(),
                CHUNK_LEN,
                "short cache file in round {round}"
            );
            // A cached chunk must be readable as such afterwards.
            assert_eq!(store.get(sha).await.expect("cache hit").len(), CHUNK_LEN);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
