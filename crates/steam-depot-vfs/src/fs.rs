// TODO(ai-review): review for correctness/style
//! Filesystem-shaped view over a single depot manifest.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use steam_vent_depot::{DepotFile, FileKind, Manifest};

use crate::chunk_store::ChunkStore;

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

#[deprecated = "Renamed to ManifestContents"]
pub type DepotSnapshot<C> = DepotManifestStore<C>;

/// File-system-style view over a single depot manifest.
///
/// Created via [`crate::DepotStore::open_depot_manifest`] (recommended) or directly with
/// [`DepotSnapshot::new`] if you want to bring your own chunk store.
pub struct DepotManifestStore<C: ChunkStore> {
    manifest: Arc<Manifest>,
    /// path -> index into `manifest.files`.
    by_path: HashMap<String, usize>,
    /// dir path -> indices of direct children.
    children: HashMap<String, Vec<usize>>,
    chunks: C,
}

impl<C: ChunkStore> DepotManifestStore<C> {
    pub fn new(manifest: Arc<Manifest>, chunks: C) -> Self {
        let mut by_path = HashMap::with_capacity(manifest.files.len());
        let mut children: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, f) in manifest.files.iter().enumerate() {
            by_path.insert(f.path.clone(), i);
            let parent = parent_of(&f.path).unwrap_or("").to_string();
            children.entry(parent).or_default().push(i);
        }
        tracing::debug!(
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

    /// Build a `NotFound` error that names the first path component that
    /// does not exist in the manifest. `requested` is the full path the
    /// caller asked about (kept verbatim in the message); `stripped` is the
    /// same path with any leading `/` removed, which is what we use to walk
    /// the index. Empty `stripped` means the synthetic root, which always
    /// exists — in that case we just report the requested path.
    fn not_found_error(&self, requested: &str, stripped: &str) -> std::io::Error {
        let missing = self.first_missing_component(stripped);
        let msg = match missing {
            Some(component) => format!(
                "'{}' not found in steam depot (missing component '{}')",
                requested, component
            ),
            None => format!("'{}' not found in steam depot", requested),
        };
        std::io::Error::new(std::io::ErrorKind::NotFound, msg)
    }

    /// Walk `path` component by component and return the prefix at which the
    /// walk first fails. A component is considered present if it is either a
    /// file (key in `by_path`) or a directory (key in `children`).
    fn first_missing_component<'a>(&self, path: &'a str) -> Option<&'a str> {
        if path.is_empty() {
            return None;
        }
        let mut end = 0;
        loop {
            let next = path[end..].find('/').map(|i| end + i).unwrap_or(path.len());
            let prefix = &path[..next];
            if !self.by_path.contains_key(prefix) && !self.children.contains_key(prefix) {
                return Some(prefix);
            }
            if next == path.len() {
                return None;
            }
            end = next + 1;
        }
    }

    pub fn metadata(&self, path: &str) -> Result<FileMeta, std::io::Error> {
        if path.is_empty() || path == "/" {
            return Ok(FileMeta {
                size: 0,
                kind: FileKind::Directory,
                linktarget: None,
            });
        }
        let stripped = strip_leading_slash(path);
        let idx = self
            .by_path
            .get(stripped)
            .ok_or_else(|| self.not_found_error(path, stripped))?;
        let f = &self.manifest.files[*idx];
        Ok(FileMeta {
            size: f.size,
            kind: f.kind,
            linktarget: f.linktarget.clone(),
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn list_dir(&self, path: &str) -> Result<Vec<Entry>, std::io::Error> {
        let key = if path.is_empty() || path == "/" {
            ""
        } else {
            strip_leading_slash(path)
        };
        let idxs = self
            .children
            .get(key)
            .ok_or_else(|| self.not_found_error(path, key))?;
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
    pub async fn read_full(&self, path: &str) -> Result<Bytes, std::io::Error> {
        let meta = self.metadata(path)?;
        if !matches!(meta.kind, FileKind::File) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("'{}' is not a regular file", path),
            ));
        }
        self.read(path, 0, meta.size).await
    }

    /// Read up to `len` bytes from `path` starting at `offset`. Returns fewer
    /// bytes than requested only at EOF.
    pub async fn read(&self, path: &str, offset: u64, len: u64) -> Result<Bytes, std::io::Error> {
        let mut out = Vec::new();
        self.read_into(path, offset, len, &mut out).await?;
        Ok(Bytes::from(out))
    }

    /// Read into a caller-provided buffer. Avoids the allocate-then-copy
    /// that the `Bytes`-returning [`read`](Self::read) needs when the
    /// caller (e.g. FUSE) already has a buffer to fill.
    ///
    /// Bytes are *appended* (existing contents of `out` are preserved);
    /// call `out.clear()` first if you want replace semantics.
    pub async fn read_into(
        &self,
        path: &str,
        offset: u64,
        len: u64,
        out: &mut Vec<u8>,
    ) -> Result<(), std::io::Error> {
        let p = strip_leading_slash(path);
        let idx = *self
            .by_path
            .get(p)
            .ok_or_else(|| self.not_found_error(path, p))?;
        let f: &DepotFile = &self.manifest.files[idx];
        if !matches!(f.kind, FileKind::File) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("'{}' is not a regular file", path),
            ));
        }
        if offset > f.size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("read past end of file (size={}, offset={})", f.size, offset),
            ));
        }
        let end = (offset + len).min(f.size);
        let want_len = (end - offset) as usize;
        tracing::trace!(
            path = %f.path,
            offset,
            len = want_len,
            file_size = f.size,
            chunks = f.chunks.len(),
            "reading file"
        );
        out.reserve(want_len);

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
            let bytes = self
                .chunks
                .get(c.sha)
                .await
                .map_err(std::io::Error::other)?;
            let slice_start = offset.saturating_sub(c_start) as usize;
            let slice_end = (end.min(c_end) - c_start) as usize;
            out.extend_from_slice(&bytes[slice_start..slice_end]);
        }
        Ok(())
    }
}

fn parent_of(path: &str) -> Option<&str> {
    path.rfind('/').map(|i| &path[..i])
}

fn strip_leading_slash(p: &str) -> &str {
    p.strip_prefix('/').unwrap_or(p)
}
