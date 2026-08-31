// TODO(ai-review): review for correctness/style
//! Persistent cache for parsed [`Manifest`]s.
//!
//! Manifests are immutable for a given `(app_id, depot_id, manifest_id)` —
//! Steam publishes a new GID for every build — so caching them locally
//! avoids the login + manifest-request-code roundtrip on subsequent runs.
//! `app_id` is part of the path so the FUSE mount can reconstruct the
//! `/<app>/<depot>/<gid>` tree from a fresh cache scan; a depot id alone
//! is ambiguous because the same depot can belong to multiple apps
//! (e.g. Steamworks Common Redistributables).
//!
//! Layout: `<root>/<app_id>/<depot_id>/<manifest_id>.postcard`.
//!
//! Because [`steam_vent_depot::Manifest`] doesn't implement [`serde`], this
//! module mirrors its fields with a private serde-friendly representation and
//! converts on the way in/out.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use steam_vent_depot::{Chunk, DepotFile, DepotFileKind, FileHash, Manifest};

use crate::Result;

type ManifestKey = (u32, u32, u64);

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("{op} `{}`", path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("postcard encode: {0}")]
    Encode(#[from] postcard::Error),
}

impl CacheError {
    fn io(op: &'static str, path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            op,
            path: path.into(),
            source,
        }
    }
}

/// One mutex per in-flight key so concurrent callers share a fetch instead of racing the CDN.
#[derive(Clone)]
pub struct ManifestCache {
    root: PathBuf,
    in_flight: Arc<Mutex<HashMap<ManifestKey, Arc<tokio::sync::Mutex<()>>>>>,
}

impl ManifestCache {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn path_for(&self, app_id: u32, depot_id: u32, manifest_id: u64) -> PathBuf {
        self.root
            .join(app_id.to_string())
            .join(depot_id.to_string())
            .join(format!("{manifest_id}.postcard"))
    }

    pub fn load(
        &self,
        app_id: u32,
        depot_id: u32,
        manifest_id: u64,
    ) -> Result<Option<Manifest>, CacheError> {
        let path = self.path_for(app_id, depot_id, manifest_id);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(CacheError::io("reading", path, e)),
        };
        let cached: CachedManifest = match postcard::from_bytes(&bytes) {
            Ok(c) => c,
            // Format changed, remove and re-fetch next time
            Err(err) => {
                tracing::warn!(
                    app_id,
                    depot_id,
                    manifest_id,
                    %err,
                    "discarding stale manifest cache entry, will refetch"
                );
                std::fs::remove_file(&path).map_err(|e| CacheError::io("removing", path, e))?;
                return Ok(None);
            }
        };
        tracing::debug!(
            app_id,
            depot_id,
            manifest_id,
            bytes = bytes.len(),
            "manifest cache hit"
        );
        Ok(Some(cached.into()))
    }

    /// decode and end up serialized.
    pub async fn load_async(
        &self,
        app_id: u32,
        depot_id: u32,
        manifest_id: u64,
    ) -> Result<Option<Manifest>, CacheError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.load(app_id, depot_id, manifest_id))
            .await
            .expect("manifest cache load task panicked")
    }

    pub fn save(&self, app_id: u32, manifest: &Manifest) -> Result<(), CacheError> {
        let path = self.path_for(app_id, manifest.depot_id, manifest.manifest_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| CacheError::io("creating", parent, e))?;
        }
        let bytes = postcard::to_allocvec(&CachedManifest::from(manifest))?;
        // Write-then-rename for atomicity.
        let tmp = path.with_extension(format!("tmp.{}", std::process::id()));
        std::fs::write(&tmp, &bytes).map_err(|e| CacheError::io("writing", &tmp, e))?;
        std::fs::rename(&tmp, &path).map_err(|e| CacheError::io("renaming to", &path, e))?;
        tracing::info!(
            app_id,
            depot_id = manifest.depot_id,
            manifest_id = manifest.manifest_id,
            bytes = bytes.len(),
            "saved manifest to cache"
        );
        Ok(())
    }

    /// Convenience: load from cache, or fall back to `fetch()` and persist.
    pub async fn get_or_fetch<F, Fut, E>(
        &self,
        app_id: u32,
        depot_id: u32,
        manifest_id: u64,
        fetch: F,
    ) -> Result<Manifest, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Manifest, E>>,
        E: From<CacheError>,
    {
        if let Some(m) = self.load_async(app_id, depot_id, manifest_id).await? {
            return Ok(m);
        }

        let key = (app_id, depot_id, manifest_id);
        let key_lock = {
            let mut in_flight = self.in_flight.lock().expect("in_flight poisoned");
            Arc::clone(
                in_flight
                    .entry(key)
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };
        let _permit = key_lock.lock().await;

        // Someone else may have fetched and saved it while we waited.
        if let Some(m) = self.load_async(app_id, depot_id, manifest_id).await? {
            return Ok(m);
        }
        let manifest = fetch().await?;
        self.save(app_id, &manifest)?;
        self.in_flight
            .lock()
            .expect("in_flight poisoned")
            .remove(&key);
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn empty_manifest(depot_id: u32, manifest_id: u64) -> Manifest {
        Manifest {
            depot_id,
            manifest_id,
            creation_time: 0,
            size_uncompressed: 0,
            size_compressed: 0,
            files: Vec::new(),
        }
    }

    fn tmp_cache() -> (ManifestCache, PathBuf) {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("manifest-cache-test-{}-{n}", std::process::id()));
        (ManifestCache::new(dir.clone()), dir)
    }

    #[tokio::test]
    async fn concurrent_get_or_fetch_only_fetches_once() {
        let (cache, dir) = tmp_cache();
        let fetch_count = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let fetch_count = Arc::clone(&fetch_count);
                tokio::spawn(async move {
                    cache
                        .get_or_fetch(1, 2, 3, || {
                            let fetch_count = Arc::clone(&fetch_count);
                            async move {
                                fetch_count.fetch_add(1, Ordering::SeqCst);
                                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                                Ok::<_, CacheError>(empty_manifest(2, 3))
                            }
                        })
                        .await
                })
            })
            .collect();
        for h in handles {
            h.await.unwrap().unwrap();
        }

        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// --- Serde mirror types ----------------------------------------------------

#[derive(Serialize, Deserialize)]
struct CachedManifest {
    depot_id: u32,
    manifest_id: u64,
    creation_time: u32,
    size_uncompressed: u64,
    size_compressed: u64,
    files: Vec<CachedFile>,
}

#[derive(Serialize, Deserialize)]
struct CachedFile {
    path: String,
    size: u64,
    kind: CachedFileKind,
}

#[derive(Serialize, Deserialize)]
enum CachedFileKind {
    File {
        sha: [u8; 20],
        executable: bool,
        chunks: Vec<CachedChunk>,
    },
    Directory,
    Symlink {
        target: String,
    },
}

#[derive(Serialize, Deserialize)]
struct CachedChunk {
    sha: [u8; 20],
    crc: u32,
    offset: u64,
    size_uncompressed: u32,
    size_compressed: u32,
}

impl From<&Manifest> for CachedManifest {
    fn from(m: &Manifest) -> Self {
        Self {
            depot_id: m.depot_id,
            manifest_id: m.manifest_id,
            creation_time: m.creation_time,
            size_uncompressed: m.size_uncompressed,
            size_compressed: m.size_compressed,
            files: m.files.iter().map(CachedFile::from).collect(),
        }
    }
}

impl From<CachedManifest> for Manifest {
    fn from(c: CachedManifest) -> Self {
        Self {
            depot_id: c.depot_id,
            manifest_id: c.manifest_id,
            creation_time: c.creation_time,
            size_uncompressed: c.size_uncompressed,
            size_compressed: c.size_compressed,
            files: c.files.into_iter().map(DepotFile::from).collect(),
        }
    }
}

impl From<&DepotFile> for CachedFile {
    fn from(f: &DepotFile) -> Self {
        Self {
            path: f.path.clone(),
            size: f.size,
            kind: (&f.kind).into(),
        }
    }
}

impl From<CachedFile> for DepotFile {
    fn from(c: CachedFile) -> Self {
        Self {
            path: c.path,
            size: c.size,
            kind: c.kind.into(),
        }
    }
}

impl From<&Chunk> for CachedChunk {
    fn from(c: &Chunk) -> Self {
        Self {
            sha: c.sha.0,
            crc: c.crc,
            offset: c.offset,
            size_uncompressed: c.size_uncompressed,
            size_compressed: c.size_compressed,
        }
    }
}

impl From<CachedChunk> for Chunk {
    fn from(c: CachedChunk) -> Self {
        Self {
            sha: steam_vent_depot::ChunkHash(c.sha),
            crc: c.crc,
            offset: c.offset,
            size_uncompressed: c.size_uncompressed,
            size_compressed: c.size_compressed,
        }
    }
}

impl From<&DepotFileKind> for CachedFileKind {
    fn from(k: &DepotFileKind) -> Self {
        match k {
            DepotFileKind::File {
                sha,
                executable,
                chunks,
            } => Self::File {
                sha: sha.0,
                executable: *executable,
                chunks: chunks.iter().map(CachedChunk::from).collect(),
            },
            DepotFileKind::Directory => Self::Directory,
            DepotFileKind::Symlink { target } => Self::Symlink {
                target: target.clone(),
            },
        }
    }
}

impl From<CachedFileKind> for DepotFileKind {
    fn from(k: CachedFileKind) -> Self {
        match k {
            CachedFileKind::File {
                sha,
                executable,
                chunks,
            } => Self::File {
                sha: FileHash(sha),
                executable,
                chunks: chunks.into_iter().map(Chunk::from).collect(),
            },
            CachedFileKind::Directory => Self::Directory,
            CachedFileKind::Symlink { target } => Self::Symlink { target },
        }
    }
}
