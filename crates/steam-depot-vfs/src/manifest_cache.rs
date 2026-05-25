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

use std::future::Future;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use steam_vent_depot::{Chunk, DepotFile, FileKind, Manifest};

use crate::Result;

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

#[derive(Clone)]
pub struct ManifestCache {
    root: PathBuf,
}

impl ManifestCache {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
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
        let cached: CachedManifest = postcard::from_bytes(&bytes)?;
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
        let manifest = fetch().await?;
        self.save(app_id, &manifest)?;
        Ok(manifest)
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
    sha: Option<[u8; 20]>,
    linktarget: Option<String>,
    chunks: Vec<CachedChunk>,
}

#[derive(Serialize, Deserialize)]
enum CachedFileKind {
    File,
    Directory,
    Symlink,
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
            kind: f.kind.into(),
            sha: f.sha,
            linktarget: f.linktarget.clone(),
            chunks: f.chunks.iter().map(CachedChunk::from).collect(),
        }
    }
}

impl From<CachedFile> for DepotFile {
    fn from(c: CachedFile) -> Self {
        Self {
            path: c.path,
            size: c.size,
            kind: c.kind.into(),
            sha: c.sha,
            linktarget: c.linktarget,
            chunks: c.chunks.into_iter().map(Chunk::from).collect(),
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

impl From<FileKind> for CachedFileKind {
    fn from(k: FileKind) -> Self {
        match k {
            FileKind::File => Self::File,
            FileKind::Directory => Self::Directory,
            FileKind::Symlink => Self::Symlink,
        }
    }
}

impl From<CachedFileKind> for FileKind {
    fn from(k: CachedFileKind) -> Self {
        match k {
            CachedFileKind::File => Self::File,
            CachedFileKind::Directory => Self::Directory,
            CachedFileKind::Symlink => Self::Symlink,
        }
    }
}
