// TODO(ai-review): review for correctness/style
//! Local-disk write-through cache wrapping any other [`ChunkStore`].

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use sha1::{Digest, Sha1};
use steam_vent_depot::ChunkHash;
use tokio::io::AsyncWriteExt;

use super::ChunkStore;
use crate::{chunk_store::CdnChunkStore, error::Result};

/// Write-through local-disk cache in front of another [`ChunkStore`].
///
/// Chunks live at `<root>/<sha-hex>`. Misses fall through to the inner store
/// and the resulting bytes are persisted.
///
/// One chunk is fetched once even when many callers miss on it at the same
/// time: a mounted filesystem turns a single client read into a dozen
/// concurrent reads of the same chunk, and fetching per read would multiply
/// the download by that factor.
///
/// What comes off the disk is checked against the name it is filed under
/// before it is handed out, and refetched if it doesn't match. Writes here
/// skip `fsync`, so a chunk file can survive a power cut with the right
/// length and the wrong bytes — and serving that would hand corrupt data
/// to every later reader.
pub struct FsCacheStore<Inner: ChunkStore = CdnChunkStore> {
    inner: Inner,
    root: PathBuf,
    /// One lock per chunk being fetched. Entries are dropped once the
    /// last waiter is gone, so this doesn't grow with the cache.
    fetching: Mutex<HashMap<ChunkHash, Weak<tokio::sync::Mutex<()>>>>,
    /// Chunks already checked in this process. The check costs one hash
    /// per chunk rather than one per read, which matters because a
    /// mounted filesystem reads each chunk many times over.
    verified: Mutex<HashSet<ChunkHash>>,
}

impl<Inner: ChunkStore> FsCacheStore<Inner> {
    pub fn new(inner: Inner, root: PathBuf) -> Self {
        // Eagerly create the cache root so the per-chunk write path
        // doesn't need a `create_dir_all` per fetch. Errors here are
        // best-effort; the first real write will surface them with a
        // proper error path.
        let _ = std::fs::create_dir_all(&root);
        Self {
            inner,
            root,
            fetching: Mutex::new(HashMap::new()),
            verified: Mutex::new(HashSet::new()),
        }
    }

    /// The lock guarding fetches of `sha`, shared with everyone else
    /// currently interested in it.
    fn fetch_lock(&self, sha: ChunkHash) -> Arc<tokio::sync::Mutex<()>> {
        let mut fetching = self.fetching.lock().expect("fetching poisoned");
        if let Some(existing) = fetching.get(&sha).and_then(Weak::upgrade) {
            return existing;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        fetching.insert(sha, Arc::downgrade(&lock));
        lock
    }

    fn path_for(&self, sha: ChunkHash) -> PathBuf {
        self.root.join(sha.to_string())
    }
}

impl<Inner: ChunkStore> ChunkStore for FsCacheStore<Inner> {
    #[tracing::instrument(level = "debug", name = "fs_cache.get", skip_all)]
    async fn get(&self, sha: ChunkHash) -> Result<Bytes> {
        let path = self.path_for(sha);
        if let Some(bytes) = self.read_verified(sha, &path).await {
            tracing::debug!(%sha, bytes = bytes.len(), "cache hit");
            return Ok(bytes);
        }
        let lock = self.fetch_lock(sha);
        let _fetching = lock.lock().await;
        // Whoever held the lock has written the chunk by now. Checking
        // the file rather than passing the bytes along keeps this to one
        // lock and no result plumbing; the loser pays a disk read.
        if let Some(bytes) = self.read_verified(sha, &path).await {
            tracing::debug!(%sha, bytes = bytes.len(), "cache hit after waiting for a fetch");
            return Ok(bytes);
        }
        self.fetch_and_persist(sha, &path).await
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
        let lock = self.fetch_lock(sha);
        let _fetching = lock.lock().await;
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
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
        // The inner store vouches for what it hands out — the CDN one
        // checks each chunk's Adler-32 — so what we just wrote needs no
        // hashing to be trusted for the rest of this process.
        self.verified.lock().expect("verified poisoned").insert(sha);
        Ok(bytes)
    }

    /// The cached chunk at `path`, or `None` if there is nothing usable
    /// there. A file whose bytes don't hash to `sha` is deleted rather
    /// than returned, so the next reader refetches instead of finding
    /// the same corruption.
    async fn read_verified(&self, sha: ChunkHash, path: &std::path::Path) -> Option<Bytes> {
        let bytes = Bytes::from(tokio::fs::read(path).await.ok()?);
        if self
            .verified
            .lock()
            .expect("verified poisoned")
            .contains(&sha)
        {
            return Some(bytes);
        }
        let mut hasher = Sha1::new();
        hasher.update(&bytes);
        let digest: [u8; 20] = hasher.finalize().into();
        if digest != sha.0 {
            tracing::error!(
                %sha,
                bytes = bytes.len(),
                "cached chunk does not hash to its name; deleting it and refetching",
            );
            if let Err(e) = tokio::fs::remove_file(path).await {
                tracing::error!(%sha, %e, "could not delete the corrupt chunk");
            }
            return None;
        }
        self.verified.lock().expect("verified poisoned").insert(sha);
        Some(bytes)
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    const CHUNK_LEN: usize = 65_536;

    /// The cache is content-addressed: a chunk's name is the SHA-1 of
    /// its bytes, which is what makes it verifiable.
    fn chunk_bytes(fill: u8) -> (ChunkHash, Bytes) {
        let bytes = Bytes::from(vec![fill; CHUNK_LEN]);
        let mut hasher = <sha1::Sha1 as sha1::Digest>::new();
        sha1::Digest::update(&mut hasher, &bytes);
        let digest: [u8; 20] = sha1::Digest::finalize(hasher).into();
        (ChunkHash(digest), bytes)
    }

    /// Hands out a fixed chunk, after yielding often enough that
    /// concurrent callers interleave inside the cache's write path.
    struct SlowInner {
        fetches: Arc<AtomicUsize>,
        fill: u8,
    }

    impl ChunkStore for SlowInner {
        async fn get(&self, _sha: ChunkHash) -> Result<Bytes> {
            self.fetches.fetch_add(1, Ordering::Relaxed);
            for _ in 0..8 {
                tokio::task::yield_now().await;
            }
            Ok(chunk_bytes(self.fill).1)
        }
    }

    #[test]
    fn temp_paths_do_not_collide() {
        let target = std::path::Path::new("/cache/abc123");
        assert_ne!(tmp_path(target), tmp_path(target));
        assert_eq!(tmp_path(target).parent(), target.parent());
    }

    /// A mounted filesystem turns one client read into a dozen
    /// concurrent reads of the same chunk. Fetching it once per read
    /// would multiply the download by that factor.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_misses_fetch_once() {
        let dir = std::env::temp_dir().join(format!("fs-cache-single-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let fetches = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(FsCacheStore::new(
            SlowInner {
                fetches: Arc::clone(&fetches),
                fill: 0x5a,
            },
            dir.clone(),
        ));
        let (sha, _) = chunk_bytes(0x5a);

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let store = Arc::clone(&store);
            tasks.push(tokio::spawn(async move { store.get(sha).await }));
        }
        for task in tasks {
            assert_eq!(
                task.await.expect("task").expect("get").len(),
                CHUNK_LEN,
                "every caller gets the whole chunk",
            );
        }
        assert_eq!(
            fetches.load(Ordering::Relaxed),
            1,
            "16 concurrent readers of one chunk must fetch it once",
        );

        let _ = std::fs::remove_dir_all(&dir);
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
                fill: 0x5a,
            },
            dir.clone(),
        ));
        let (sha, _) = chunk_bytes(0x5a);

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

    /// A chunk file that reads back with the right length but the wrong
    /// bytes — what a torn write leaves behind, since the cache skips
    /// fsync. Serving that forever would hand out corrupt data.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_corrupt_cache_file_is_replaced_not_served() {
        let dir = std::env::temp_dir().join(format!("fs-cache-corrupt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create cache dir");
        let (sha, good) = chunk_bytes(0x5a);
        std::fs::write(dir.join(sha.to_string()), vec![0; CHUNK_LEN]).expect("plant corruption");

        let fetches = Arc::new(AtomicUsize::new(0));
        let store = FsCacheStore::new(
            SlowInner {
                fetches: Arc::clone(&fetches),
                fill: 0x5a,
            },
            dir.clone(),
        );

        assert_eq!(
            store.get(sha).await.expect("get"),
            good,
            "the read is served the real bytes"
        );
        assert_eq!(
            fetches.load(Ordering::Relaxed),
            1,
            "the corrupt file was refetched"
        );
        assert_eq!(
            std::fs::read(dir.join(sha.to_string())).expect("cached file"),
            good,
            "and replaced on disk",
        );

        assert_eq!(store.get(sha).await.expect("get"), good);
        assert_eq!(
            fetches.load(Ordering::Relaxed),
            1,
            "the repaired file is then served from disk",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
