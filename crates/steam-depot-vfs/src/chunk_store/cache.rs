// TODO(ai-review): review for correctness/style
//! Local-disk write-through cache wrapping any other [`ChunkStore`].
//!
//! Chunks are persisted compressed and decompressed on every read, so
//! the cache costs a fraction of the plaintext size on disk. Frames
//! carry their own integrity (a zstd content checksum) and identity (an
//! embedded SHA-1, checked against the file's name), so reading *is*
//! verifying — no per-chunk hash bookkeeping on the read path.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use lru::LruCache;
use sha1::{Digest, Sha1};
use steam_vent_depot::ChunkHash;
use tokio::io::AsyncWriteExt;

use super::ChunkStore;
use crate::{VfsError, chunk_store::CdnChunkStore, error::Result};

/// The shared state behind a chunk cache: the decoded chunks currently
/// held, and the per-chunk load locks. A [`DepotStore`](crate::DepotStore)
/// creates one and hands it to every snapshot it opens, so chunks are
/// decoded — and on a miss, fetched — once per store, not once per
/// snapshot; in practice one per process. Shared as `Arc<ChunkDir>`.
pub struct ChunkDir {
    root: PathBuf,
    /// One lock per chunk being loaded. Entries are dropped once the
    /// last waiter is gone, so this doesn't grow with the cache.
    loading: Mutex<HashMap<ChunkHash, Weak<tokio::sync::Mutex<()>>>>,
    /// Decoded chunks, most-recently-used first. Kernel-backed readers
    /// (FUSE, NFS) deliver one client read as several small range reads
    /// of the same chunk, and every range read fetches its whole chunk —
    /// without this cache the decode runs once per fragment (~8× per
    /// 1 MiB chunk under 128 KiB reads).
    decoded: Mutex<LruCache<ChunkHash, Bytes>>,
}

/// Entries kept in a [`ChunkDir`]'s decoded-chunk cache — one pool per
/// [`DepotStore`](crate::DepotStore), shared by every snapshot it
/// opens; in practice one pool per process. Chunks are ~1 MiB, so this
/// bounds the pool to roughly that many MiB.
///
/// Sized by measurement (run the `store_*` examples with this patched):
/// a seeky tool or a re-read file needs its whole working set resident —
/// the largest bundle here is ~134 chunks, and it reads at ~0.7 GB/s
/// with 64 entries but ~25 GB/s with ≥134 — while a stream through a
/// working set larger than the cache pays for every entry (~2.6 GB/s
/// at 8, ~2.0 GB/s at 256). 160 covers the biggest browsed file plus
/// headroom; beyond that, entries only cost memory and stream speed.
const DECODED_CACHE_ENTRIES: NonZeroUsize = NonZeroUsize::new(160).unwrap();

/// zstd level for persisted chunk frames. Higher levels shrink the store
/// further at a cost on the fetch path — at 9 the whole frame build
/// (hash + compress + checksum) is ~6 ms of CPU per MiB on the store
/// benches; reads decode at the same speed regardless of level. Public
/// so benchmarks track the shipped write path.
pub const ZSTD_LEVEL: i32 = 9;

/// Header of one frame: plaintext length + SHA-1 of the plaintext.
const FRAME_HEADER_LEN: usize = 8 + 20;

/// The on-disk form of one chunk, as written to and read from the cache
/// directory; public for tools that inspect or rebuild stores.
///
/// Layout: `u64` LE plaintext length + SHA-1 of the plaintext + a zstd
/// frame with its content checksum enabled. The checksum makes every
/// decode an integrity check — torn writes and bit rot fail to decode —
/// and the embedded digest, checked against the file's name, is the
/// identity check: a valid frame filed under a wrong name is rejected
/// without ever hashing the content on the read path.
pub fn encode_frame(plain: &[u8]) -> Result<Vec<u8>> {
    let digest: [u8; 20] = Sha1::digest(plain).into();
    encode_frame_as(plain, digest)
}

/// Build a frame for `plain`, attested by `digest` — the fetch path
/// already hashed the bytes and wants the digest in the same pass.
fn encode_frame_as(plain: &[u8], digest: [u8; 20]) -> Result<Vec<u8>> {
    let mut compressor =
        zstd::bulk::Compressor::new(ZSTD_LEVEL).map_err(|e| VfsError::Other(e.into()))?;
    compressor
        .set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(true))
        .map_err(|e| VfsError::Other(e.into()))?;
    let compressed = compressor
        .compress(plain)
        .map_err(|e| VfsError::Other(e.into()))?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + compressed.len());
    frame.extend_from_slice(&(plain.len() as u64).to_le_bytes());
    frame.extend_from_slice(&digest);
    frame.extend_from_slice(&compressed);
    Ok(frame)
}

/// Inverse of [`encode_frame`]: the decoded plaintext and the identity
/// the frame claims for itself. The caller checks that identity against
/// the name the chunk is filed under; `None` marks anything that is not
/// a decodable frame of the promised length — torn writes, bit rot, and
/// foreign bodies land here.
///
/// The claimed length is capped before any allocation: zstd's bulk
/// decompress allocates its output buffer upfront, so a foreign or
/// bit-rotted header with a huge length would abort the process on
/// the allocation instead of returning an error.
pub fn decode_frame(frame: &[u8]) -> Option<(ChunkHash, Bytes)> {
    /// Steam chunks are ~1 MiB; nothing legitimate comes close to this,
    /// and it bounds the speculative allocation for corrupt headers.
    const MAX_PLAIN_LEN: usize = 64 << 20;

    let plain_len = usize::try_from(u64::from_le_bytes(frame.get(..8)?.try_into().ok()?)).ok()?;
    if plain_len > MAX_PLAIN_LEN {
        return None;
    }
    let digest: [u8; 20] = frame.get(8..FRAME_HEADER_LEN)?.try_into().ok()?;
    // A decoding zstd frame validates its content checksum: success
    // means the output is exactly what was compressed into it.
    let plain = zstd::bulk::decompress(frame.get(FRAME_HEADER_LEN..)?, plain_len).ok()?;
    if plain.len() != plain_len {
        return None;
    }
    Some((ChunkHash(digest), plain.into()))
}

impl ChunkDir {
    /// Open (creating if needed) the cache directory at `root`.
    pub fn new(root: PathBuf) -> Self {
        // Eagerly create the cache root so the per-chunk write path
        // doesn't need a `create_dir_all` per fetch. Errors here are
        // best-effort; the first real write will surface them with a
        // proper error path.
        let _ = std::fs::create_dir_all(&root);
        Self {
            root,
            loading: Mutex::new(HashMap::new()),
            decoded: Mutex::new(LruCache::new(DECODED_CACHE_ENTRIES)),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path_for(&self, sha: ChunkHash) -> PathBuf {
        self.root.join(sha.to_string())
    }

    /// The lock guarding loads of `sha`, shared with everyone else
    /// currently interested in it.
    fn chunk_lock(&self, sha: ChunkHash) -> Arc<tokio::sync::Mutex<()>> {
        let mut loading = self.loading.lock().expect("loading poisoned");
        if let Some(existing) = loading.get(&sha).and_then(Weak::upgrade) {
            return existing;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        loading.insert(sha, Arc::downgrade(&lock));
        lock
    }

    /// The decoded cache's entry for `sha`, if it has one.
    fn cache_hit(&self, sha: ChunkHash) -> Option<Bytes> {
        self.decoded
            .lock()
            .expect("decoded poisoned")
            .get(&sha)
            .cloned()
    }

    fn cache_decoded(&self, sha: ChunkHash, bytes: &Bytes) {
        self.decoded
            .lock()
            .expect("decoded poisoned")
            .put(sha, bytes.clone());
    }
}

/// Write-through local-disk cache in front of another [`ChunkStore`].
///
/// One chunk is loaded once even when many callers want it at the same
/// time — from disk (read + decode) or from the inner store (fetch).
/// Both happen under a per-chunk lock shared by every store over the
/// same [`ChunkDir`], and the loser rechecks the decoded cache first:
/// without this, a mounted filesystem's fragment reads and parallel
/// deep compares over shared chunks would each re-read and re-decode
/// the same chunk.
///
/// Chunks live at `<root>/<sha-hex>` in the frame format of
/// [`encode_frame`]. Reads decompress on the way out — which is the
/// verification, see [`decode_frame`] — and the decoded plaintext is
/// kept in the directory's shared most-recently-used cache.
pub struct FsCacheStore<Inner: ChunkStore = CdnChunkStore> {
    dir: Arc<ChunkDir>,
    inner: Inner,
}

impl<Inner: ChunkStore> FsCacheStore<Inner> {
    /// A store with private state over `root`: its decoded cache and
    /// load locks start empty and are shared with no one. What tests
    /// and benchmarks need to measure cold loads.
    pub fn new(inner: Inner, root: PathBuf) -> Self {
        Self::over(Arc::new(ChunkDir::new(root)), inner)
    }

    /// A store over a shared [`ChunkDir`]: shares its decoded chunks
    /// and load locks with every store built over the same one, so a
    /// chunk is decoded and fetched once per
    /// [`DepotStore`](crate::DepotStore), not once per snapshot — the
    /// shape [`DepotStore`](crate::DepotStore) gives every snapshot it
    /// opens.
    pub fn over(dir: Arc<ChunkDir>, inner: Inner) -> Self {
        Self { dir, inner }
    }
}

impl<Inner: ChunkStore> ChunkStore for FsCacheStore<Inner> {
    #[tracing::instrument(level = "debug", name = "fs_cache.get", skip_all)]
    async fn get(&self, sha: ChunkHash) -> Result<Bytes> {
        if let Some(bytes) = self.dir.cache_hit(sha) {
            return Ok(bytes);
        }
        let path = self.dir.path_for(sha);
        // The lock covers the whole load — disk read, decode, fetch —
        // so concurrent callers of one chunk pay it once; the loser's
        // cache recheck then serves from memory.
        let lock = self.dir.chunk_lock(sha);
        let _loading = lock.lock().await;
        if let Some(bytes) = self.dir.cache_hit(sha) {
            return Ok(bytes);
        }
        if let Some(bytes) = self.read_verified(sha, &path).await {
            tracing::debug!(%sha, bytes = bytes.len(), "cache hit");
            self.dir.cache_decoded(sha, &bytes);
            return Ok(bytes);
        }
        self.fetch_and_persist(sha, &path).await
    }

    #[tracing::instrument(level = "debug", name = "fs_cache.ensure", skip_all)]
    async fn ensure(&self, sha: ChunkHash) -> Result<()> {
        let path = self.dir.path_for(sha);
        // `try_exists` is the cheap check: a single `stat` rather than
        // a full file read. If we can't determine existence (permission
        // issue, etc.) fall through to the fetch path; it will fail if
        // truly broken.
        if tokio::fs::try_exists(&path).await.unwrap_or(false) {
            tracing::debug!(%sha, "cache hit (ensure)");
            return Ok(());
        }
        let lock = self.dir.chunk_lock(sha);
        let _loading = lock.lock().await;
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
fn tmp_path(path: &Path) -> PathBuf {
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
    async fn fetch_and_persist(&self, sha: ChunkHash, path: &Path) -> Result<Bytes> {
        tracing::debug!(%sha, "cache miss, fetching from inner store");
        let bytes = self.inner.get(sha).await?;
        // Hash once, here on the fetch path: the digest is embedded in
        // the frame, and a fetch whose bytes don't match the chunk's
        // name is refused rather than persisted — a frame that lies
        // about its identity would be rejected by every later read,
        // an endless refetch loop.
        let frame = tokio::task::spawn_blocking({
            let bytes = bytes.clone();
            move || -> Result<Vec<u8>> {
                let digest: [u8; 20] = Sha1::digest(&bytes).into();
                if digest != sha.0 {
                    return Err(VfsError::Other(
                        "fetched chunk does not hash to its name".into(),
                    ));
                }
                encode_frame_as(&bytes, digest)
            }
        })
        .await
        .map_err(|e| VfsError::Other(e.into()))??;
        // zstd-9 is ~6 ms of CPU per MiB; the async workers also serve
        // warm reads, so the encode is offloaded.
        self.write_atomic(path, &frame).await?;
        self.dir.cache_decoded(sha, &bytes);
        Ok(bytes)
    }

    /// The cached chunk at `path`, or `None` if there is nothing usable
    /// there. Decoding is the verification: the frame's content
    /// checksum rejects torn writes and bit rot, its embedded digest
    /// must match the name the chunk is filed under, and anything that
    /// fails either is deleted so the next reader refetches instead of
    /// hitting the same corruption again.
    async fn read_verified(&self, sha: ChunkHash, path: &Path) -> Option<Bytes> {
        let frame = tokio::fs::read(path).await.ok()?;
        let Some((claimed, plain)) = decode_frame(&frame) else {
            self.delete_corrupt(sha, path, "does not decode").await;
            return None;
        };
        if claimed != sha {
            self.delete_corrupt(sha, path, "does not name itself as this chunk")
                .await;
            return None;
        }
        Some(plain)
    }

    async fn delete_corrupt(&self, sha: ChunkHash, path: &Path, why: &str) {
        tracing::error!(%sha, why, "deleting the corrupt cached chunk");
        if let Err(e) = tokio::fs::remove_file(path).await {
            tracing::error!(%sha, %e, "could not delete the corrupt chunk");
        }
    }

    /// Write `bytes` to `path` atomically: write a sibling temporary
    /// file, rename over. **Deliberately no `fsync`.**
    ///
    /// fsync only matters for hard reboots / power loss. After a normal
    /// process crash the kernel still flushes the page cache, so closed
    /// files survive intact. On real disk fsync costs us ~13% throughput
    /// because it back-pressures concurrent CDN polls; on tmpfs it's
    /// a no-op anyway. In the rare power-loss case a committed chunk
    /// file can read back as zeros — the frame's checksum rejects that
    /// on the next read, and the recovery path is "refetch", which is
    /// cheap for a content-addressed cache.
    #[tracing::instrument(level = "debug", name = "fs_cache.write_atomic", skip(self, bytes), fields(bytes_len = bytes.len()))]
    async fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<()> {
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
        let digest: [u8; 20] = Sha1::digest(&bytes).into();
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
        let target = Path::new("/cache/abc123");
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
    /// written chunk. The stores race through one shared [`ChunkDir`],
    /// the cross-instance shape production has once per-request stores
    /// share a directory: the shared load lock must collapse the race
    /// to one fetch, and the file it publishes must be whole.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_misses_never_serve_a_short_chunk() {
        let dir = std::env::temp_dir().join(format!("fs-cache-race-{}", std::process::id()));
        let (sha, _) = chunk_bytes(0x5a);

        // Several rounds: each one starts from a cold directory and a
        // fresh shared state, so every task takes the miss path and
        // races the others.
        for round in 0..20 {
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create cache dir");
            let chunk_dir = Arc::new(ChunkDir::new(dir.clone()));
            let fetches = Arc::new(AtomicUsize::new(0));
            let mut tasks = Vec::new();
            for _ in 0..16 {
                let store = FsCacheStore::over(
                    Arc::clone(&chunk_dir),
                    SlowInner {
                        fetches: Arc::clone(&fetches),
                        fill: 0x5a,
                    },
                );
                tasks.push(tokio::spawn(async move { store.get(sha).await }));
            }
            for task in tasks {
                let bytes = task.await.expect("task").expect("get");
                assert_eq!(bytes.len(), CHUNK_LEN, "short read in round {round}");
            }
            assert_eq!(
                fetches.load(Ordering::Relaxed),
                1,
                "16 stores over one directory must fetch the chunk once",
            );
            // The on-disk file is a compressed frame, so "complete" is
            // proven by decoding it back — through an isolated store,
            // which exercises the full read path (checksum + name
            // check) that a later process would take.
            let cold_reader = FsCacheStore::new(
                SlowInner {
                    fetches: Arc::new(AtomicUsize::new(0)),
                    fill: 0x5a,
                },
                dir.clone(),
            );
            assert_eq!(
                cold_reader
                    .get(sha)
                    .await
                    .expect("decode cached file")
                    .len(),
                CHUNK_LEN,
                "torn frame in round {round}"
            );
            // The shared decoded cache must serve it afterwards too.
            let warm = FsCacheStore::over(
                Arc::clone(&chunk_dir),
                SlowInner {
                    fetches: Arc::new(AtomicUsize::new(0)),
                    fill: 0x5a,
                },
            );
            assert_eq!(warm.get(sha).await.expect("cache hit").len(), CHUNK_LEN);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A chunk file of garbage where a frame should be — the all-zeros
    /// shape a lost power flush can leave behind, since the cache
    /// skips fsync. Serving that forever would hand out corrupt data.
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
        // A fresh store proves the repaired file on disk; the warm one
        // would serve from its decoded cache.
        assert_eq!(
            FsCacheStore::new(
                SlowInner {
                    fetches: Arc::new(AtomicUsize::new(0)),
                    fill: 0x5a,
                },
                dir.clone(),
            )
            .get(sha)
            .await
            .expect("cold read of the repaired file"),
            good,
            "the repaired file on disk decodes to the chunk's bytes",
        );
        assert_eq!(
            store.get(sha).await.expect("get"),
            good,
            "and the warm store serves it without a refetch",
        );
        assert_eq!(
            fetches.load(Ordering::Relaxed),
            1,
            "served from the cache, not refetched",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A well-formed frame of *other* bytes: decodes and self-checks
    /// fine, but names a different chunk than the one it is filed
    /// under — the identity class the embedded digest catches without
    /// hashing on the read path. It must not be served.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_frame_with_the_wrong_plaintext_is_replaced_not_served() {
        let dir = std::env::temp_dir().join(format!("fs-cache-wrongplain-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create cache dir");
        let (sha, good) = chunk_bytes(0x5a);
        let (_, other_plain) = chunk_bytes(0xa5);
        std::fs::write(
            dir.join(sha.to_string()),
            encode_frame(&other_plain).expect("encode"),
        )
        .expect("plant wrong-plaintext frame");

        let fetches = Arc::new(AtomicUsize::new(0));
        let store = FsCacheStore::new(
            SlowInner {
                fetches: Arc::clone(&fetches),
                fill: 0x5a,
            },
            dir.clone(),
        );

        assert_eq!(store.get(sha).await.expect("get"), good);
        assert_eq!(
            fetches.load(Ordering::Relaxed),
            1,
            "the wrong-plaintext frame was refetched"
        );
        // A fresh store proves the repaired frame on disk; the warm one
        // would serve from its decoded cache.
        assert_eq!(
            FsCacheStore::new(
                SlowInner {
                    fetches: Arc::new(AtomicUsize::new(0)),
                    fill: 0x5a,
                },
                dir.clone(),
            )
            .get(sha)
            .await
            .expect("cold read of the repaired frame"),
            good,
            "the repaired frame on disk decodes to the chunk's bytes",
        );
        assert_eq!(
            store.get(sha).await.expect("get"),
            good,
            "and the warm store serves it without a refetch",
        );
        assert_eq!(fetches.load(Ordering::Relaxed), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A frame whose zstd body has a flipped byte: the content checksum
    /// must reject it on the read. This is the per-read integrity the
    /// checksum gives us — corruption of an already-trusted file is
    /// caught too, not just torn writes.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_bitrotted_frame_is_refetched_not_served() {
        let dir = std::env::temp_dir().join(format!("fs-cache-bitrot-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create cache dir");
        let (sha, good) = chunk_bytes(0x5a);
        let mut frame = encode_frame(&good).expect("encode");
        let last = frame.len() - 1;
        frame[last] ^= 0xff;
        std::fs::write(dir.join(sha.to_string()), &frame).expect("plant bitrotted frame");

        let fetches = Arc::new(AtomicUsize::new(0));
        let store = FsCacheStore::new(
            SlowInner {
                fetches: Arc::clone(&fetches),
                fill: 0x5a,
            },
            dir.clone(),
        );

        assert_eq!(store.get(sha).await.expect("get"), good);
        assert_eq!(
            fetches.load(Ordering::Relaxed),
            1,
            "the bitrotted frame was refetched"
        );
        assert_eq!(store.get(sha).await.expect("get"), good);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A frame header claiming a huge plaintext over a garbage body: the
    /// decode must refuse it without ever allocating for the claimed
    /// length — zstd's bulk decompress allocates its output buffer
    /// upfront, so an unchecked length aborts the process instead of
    /// returning an error.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_frame_with_a_huge_claimed_length_is_refetched_not_crashed() {
        let dir = std::env::temp_dir().join(format!("fs-cache-hugelen-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create cache dir");
        let (sha, good) = chunk_bytes(0x5a);
        let mut frame = Vec::new();
        frame.extend_from_slice(&u64::MAX.to_le_bytes());
        frame.extend_from_slice(b"not a frame body");
        std::fs::write(dir.join(sha.to_string()), &frame).expect("plant huge-length frame");

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
            "the huge-length frame is refused and the chunk refetched"
        );
        assert_eq!(fetches.load(Ordering::Relaxed), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A fetch whose bytes don't hash to the requested chunk's name is
    /// a broken source, not a cache entry: it must be refused, not
    /// persisted — a frame that lies about its identity would be
    /// rejected by every later read, an endless refetch loop.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fetch_that_does_not_hash_to_its_name_is_refused() {
        let dir = std::env::temp_dir().join(format!("fs-cache-badfetch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create cache dir");
        let (sha, _) = chunk_bytes(0x5a);

        let fetches = Arc::new(AtomicUsize::new(0));
        let store = FsCacheStore::new(
            SlowInner {
                fetches: Arc::clone(&fetches),
                fill: 0xa5, // the wrong bytes for `sha`
            },
            dir.clone(),
        );

        assert!(
            store.get(sha).await.is_err(),
            "a fetch of mismatched bytes is an error",
        );
        assert!(
            !dir.join(sha.to_string()).try_exists().expect("stat"),
            "nothing was persisted",
        );
        assert_eq!(fetches.load(Ordering::Relaxed), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The decoded-chunk cache serves a chunk whose file has since been
    /// removed: a mount fragments one client read into many range reads,
    /// and they must not each re-decode — or worse, refetch — the same
    /// chunk.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_decoded_chunk_survives_its_file_disappearing() {
        let dir = std::env::temp_dir().join(format!("fs-cache-lru-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create cache dir");
        let (sha, good) = chunk_bytes(0x5a);
        std::fs::write(
            dir.join(sha.to_string()),
            encode_frame(&good).expect("encode"),
        )
        .expect("plant frame");

        let fetches = Arc::new(AtomicUsize::new(0));
        let store = FsCacheStore::new(
            SlowInner {
                fetches: Arc::clone(&fetches),
                fill: 0xa5,
            },
            dir.clone(),
        );

        assert_eq!(store.get(sha).await.expect("first get"), good);
        std::fs::remove_file(dir.join(sha.to_string())).expect("remove chunk file");

        assert_eq!(
            store.get(sha).await.expect("second get"),
            good,
            "served from the decoded cache, not from disk or the inner store",
        );
        assert_eq!(fetches.load(Ordering::Relaxed), 0, "nothing was fetched");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Stores built [`over`](FsCacheStore::over) the same [`ChunkDir`]
    /// share warmth: what one decodes, the other serves from memory.
    /// This is the cross-request shape [`DepotStore`](crate::DepotStore)
    /// gives its snapshots.
    #[tokio::test(flavor = "multi_thread")]
    async fn stores_over_one_dir_share_their_warmth() {
        let dir = std::env::temp_dir().join(format!("fs-cache-shared-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create cache dir");
        let (sha, good) = chunk_bytes(0x5a);
        std::fs::write(
            dir.join(sha.to_string()),
            encode_frame(&good).expect("encode"),
        )
        .expect("plant frame");

        let fetches = Arc::new(AtomicUsize::new(0));
        let chunk_dir = Arc::new(ChunkDir::new(dir.clone()));
        let first = FsCacheStore::over(
            Arc::clone(&chunk_dir),
            SlowInner {
                fetches: Arc::clone(&fetches),
                fill: 0xa5,
            },
        );
        let second = FsCacheStore::over(
            Arc::clone(&chunk_dir),
            SlowInner {
                fetches: Arc::clone(&fetches),
                fill: 0xa5,
            },
        );

        assert_eq!(first.get(sha).await.expect("first store"), good);
        // No file, no fetch — the second store sees the first's decoded
        // chunk through the shared dir.
        std::fs::remove_file(dir.join(sha.to_string())).expect("remove chunk file");
        assert_eq!(
            second.get(sha).await.expect("second store"),
            good,
            "served from the shared decoded cache",
        );
        assert_eq!(fetches.load(Ordering::Relaxed), 0, "nothing was fetched");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
