// TODO(ai-review): review for correctness/style
//! Filesystem-shaped view over a single depot manifest.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use steam_vent_depot::{DepotFile, FileKind, Manifest};

use crate::chunk_store::ChunkStore;
use crate::error::{Result, VfsError};

/// Cheap metadata for a file/directory/symlink entry.
#[derive(Debug, Clone)]
pub struct FileMeta {
    pub size: u64,
    pub kind: FileKind,
    pub linktarget: Option<String>,
}

/// A directory entry returned by [`DepotSnapshot::list_dir`].
#[derive(Debug, Clone)]
pub struct Entry {
    /// Last path component.
    pub name: String,
    pub meta: FileMeta,
}

/// File-system-style view over a single depot manifest.
///
/// Created via [`crate::DepotStore::open_depot_manifest`] (recommended) or directly with
/// [`DepotSnapshot::new`] if you want to bring your own chunk store.
pub struct DepotSnapshot<C: ChunkStore> {
    manifest: Arc<Manifest>,
    /// path -> index into `manifest.files`.
    by_path: HashMap<String, usize>,
    /// dir path -> indices of direct children.
    children: HashMap<String, Vec<usize>>,
    chunks: C,
}

impl<C: ChunkStore> DepotSnapshot<C> {
    pub fn new(manifest: Arc<Manifest>, chunks: C) -> Self {
        let mut by_path = HashMap::with_capacity(manifest.files.len());
        let mut children: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, f) in manifest.files.iter().enumerate() {
            by_path.insert(f.path.clone(), i);
            let parent = parent_of(&f.path).unwrap_or("").to_string();
            children.entry(parent).or_default().push(i);
        }
        tracing::info!(
            manifest_id = manifest.manifest_id,
            depot_id = manifest.depot_id,
            files = manifest.files.len(),
            "depot fs indexed"
        );
        Self {
            manifest,
            by_path,
            children,
            chunks,
        }
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Underlying chunk store. Mostly useful for tools that want to
    /// drive a custom warmup loop over `manifest().files[].chunks[]`.
    pub fn chunks(&self) -> &C {
        &self.chunks
    }

    /// Index into [`Manifest::files`] for `path`, if present. The empty path
    /// and `"/"` are *not* valid here — they refer to the synthetic root, which
    /// has no entry in `manifest.files`.
    pub fn index_of(&self, path: &str) -> Option<usize> {
        self.by_path.get(strip_leading_slash(path)).copied()
    }

    pub fn metadata(&self, path: &str) -> Result<FileMeta> {
        if path.is_empty() || path == "/" {
            return Ok(FileMeta {
                size: 0,
                kind: FileKind::Directory,
                linktarget: None,
            });
        }
        let path = strip_leading_slash(path);
        let idx = self
            .by_path
            .get(path)
            .ok_or_else(|| VfsError::NotFound(path.to_string()))?;
        let f = &self.manifest.files[*idx];
        Ok(FileMeta {
            size: f.size,
            kind: f.kind,
            linktarget: f.linktarget.clone(),
        })
    }

    pub fn list_dir(&self, path: &str) -> Result<Vec<Entry>> {
        let key = if path.is_empty() || path == "/" {
            ""
        } else {
            strip_leading_slash(path)
        };
        let idxs = self
            .children
            .get(key)
            .ok_or_else(|| VfsError::NotFound(path.to_string()))?;
        let mut out = Vec::with_capacity(idxs.len());
        for &i in idxs {
            let f = &self.manifest.files[i];
            let name = Path::new(&f.path)
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| f.path.clone());
            out.push(Entry {
                name,
                meta: FileMeta {
                    size: f.size,
                    kind: f.kind,
                    linktarget: f.linktarget.clone(),
                },
            });
        }
        Ok(out)
    }

    /// Read the entire file at `path`. Convenience wrapper around [`read`](Self::read).
    pub async fn read_full(&self, path: &str) -> Result<Bytes> {
        let meta = self.metadata(path)?;
        if !matches!(meta.kind, FileKind::File) {
            return Err(VfsError::NotAFile(path.into()));
        }
        self.read(path, 0, meta.size).await
    }

    /// Read up to `len` bytes from `path` starting at `offset`. Returns fewer
    /// bytes than requested only at EOF.
    pub async fn read(&self, path: &str, offset: u64, len: u64) -> Result<Bytes> {
        let p = strip_leading_slash(path);
        let idx = *self
            .by_path
            .get(p)
            .ok_or_else(|| VfsError::NotFound(path.into()))?;
        let f: &DepotFile = &self.manifest.files[idx];
        if !matches!(f.kind, FileKind::File) {
            return Err(VfsError::NotAFile(path.into()));
        }
        if offset > f.size {
            return Err(VfsError::OutOfRange {
                size: f.size,
                offset,
            });
        }
        let end = (offset + len).min(f.size);
        let want_len = (end - offset) as usize;
        tracing::info!(
            path = %f.path,
            offset,
            len = want_len,
            file_size = f.size,
            chunks = f.chunks.len(),
            "reading file"
        );
        let mut out = Vec::with_capacity(want_len);

        // `DepotFile::chunks` is sorted by offset (enforced upstream), so we
        // can `break` once we're past `end`.
        debug_assert!(f.chunks.is_sorted_by_key(|c| c.offset));

        for c in &f.chunks {
            let c_start = c.offset;
            let c_end = c.offset + c.size_uncompressed as u64;
            if c_end <= offset {
                continue;
            }
            if c_start >= end {
                break;
            }
            let bytes = self.chunks.get(c.sha).await?;
            let slice_start = offset.saturating_sub(c_start) as usize;
            let slice_end = (end.min(c_end) - c_start) as usize;
            out.extend_from_slice(&bytes[slice_start..slice_end]);
        }
        Ok(Bytes::from(out))
    }
}

fn parent_of(path: &str) -> Option<&str> {
    path.rfind('/').map(|i| &path[..i])
}

fn strip_leading_slash(p: &str) -> &str {
    p.strip_prefix('/').unwrap_or(p)
}
