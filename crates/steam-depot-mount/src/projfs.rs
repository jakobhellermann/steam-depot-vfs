//! [`projfs::Filesystem`] over a [`MountTree`]. A path walks the synthetic
//! dirs (root, app, depot) by name until it reaches a snapshot root; the
//! components after that are the path inside the snapshot. ProjFS hands
//! out `\`-paths; `DepotManifestStore` wants `/`-paths.

use std::path::{Component, Path};
use std::sync::Arc;

use parking_lot::RwLock;
use projfs::{DirEntry, Filesystem, Metadata};
use steam_depot_vfs::FileType;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::FileMeta;
use tokio::runtime::Handle;

use crate::inode::{self, Ino, SYNTHETIC};
use crate::tree::{MountTree, SnapshotEntry, SnapshotId};
use crate::view::{self, SlotLookup};

pub(crate) struct ProjFsSource<C: ChunkStore + 'static> {
    tree: Arc<RwLock<MountTree<C>>>,
    handle: Handle,
}

/// Where a path lands in the tree.
enum Located {
    Synthetic(Ino),
    /// A snapshot, with the path inside it (`""` for its root).
    Snapshot(inode::SnapshotId, String),
}

impl<C: ChunkStore + 'static> ProjFsSource<C> {
    pub(crate) fn new(tree: Arc<RwLock<MountTree<C>>>, handle: Handle) -> Self {
        Self { tree, handle }
    }

    /// Walk `comps` from the root, naming a synthetic dir's snapshots when
    /// a component isn't found among its children yet. `block_on` is safe:
    /// ProjFS callbacks run on their own worker threads, never a tokio one.
    fn locate(&self, comps: &[String]) -> Option<Located> {
        let mut ino = inode::ROOT;
        for (i, comp) in comps.iter().enumerate() {
            let (sid, _) = inode::unpack(ino);
            if sid != SYNTHETIC {
                return Some(Located::Snapshot(sid, comps[i..].join("/")));
            }
            let found = view::synthetic_child(&self.tree.read(), ino, comp);
            let child = match found {
                Some(node) => node.ino,
                None if view::needs_naming(&self.tree, ino) => {
                    self.ensure_named(ino)?;
                    view::synthetic_child(&self.tree.read(), ino, comp)?.ino
                }
                None => return None,
            };
            ino = child;
        }
        let (sid, _) = inode::unpack(ino);
        Some(if sid == SYNTHETIC {
            Located::Synthetic(ino)
        } else {
            Located::Snapshot(sid, String::new())
        })
    }

    fn ensure_named(&self, dir: Ino) -> Option<()> {
        match self.handle.block_on(view::ensure_named(&self.tree, dir)) {
            Ok(()) => Some(()),
            Err(e) => {
                tracing::error!(%e, "projfs naming snapshots failed");
                None
            }
        }
    }

    /// Resolve snapshot `sid`, driving a lazy opener on first access.
    fn resolve(&self, sid: inode::SnapshotId) -> Option<Arc<SnapshotEntry<C>>> {
        match view::slot_lookup(&self.tree, sid) {
            SlotLookup::Ready(entry) => Some(entry),
            SlotLookup::Pending(lazy) => {
                match self.handle.block_on(view::resolve(
                    Arc::clone(&self.tree),
                    lazy,
                    SnapshotId(sid),
                )) {
                    Ok(entry) => Some(entry),
                    Err(e) => {
                        tracing::warn!(%e, sid, "projfs lazy resolve failed");
                        None
                    }
                }
            }
            SlotLookup::Missing => None,
        }
    }
}

fn components(path: &Path) -> Vec<String> {
    path.components()
        .filter_map(|c| match c {
            Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect()
}

fn dir_meta() -> Metadata {
    Metadata {
        is_dir: true,
        size: 0,
        creation_time: 0,
        last_write_time: 0,
    }
}

/// Symlinks degrade to regular files, as in the other back ends.
fn file_meta(m: &FileMeta, creation_unix: u32) -> Metadata {
    let ft = projfs::unix_to_filetime(creation_unix as i64);
    Metadata {
        is_dir: matches!(m.kind, FileType::Directory),
        size: m.size,
        creation_time: ft,
        last_write_time: ft,
    }
}

impl<C: ChunkStore + Send + Sync + 'static> Filesystem for ProjFsSource<C> {
    fn list_directory(&self, path: &Path) -> Vec<DirEntry> {
        match self.locate(&components(path)) {
            Some(Located::Synthetic(ino)) => {
                if view::needs_naming(&self.tree, ino) && self.ensure_named(ino).is_none() {
                    return vec![];
                }
                let tree = self.tree.read();
                let Some(dir) = tree.synthetic(ino) else {
                    return vec![];
                };
                dir.children
                    .keys()
                    .map(|name| DirEntry {
                        name: name.clone(),
                        metadata: dir_meta(),
                    })
                    .collect()
            }
            Some(Located::Snapshot(sid, rest)) => {
                let Some(entry) = self.resolve(sid) else {
                    return vec![];
                };
                let creation = entry.snapshot.manifest().creation_time;
                match entry.snapshot.list_dir(&rest) {
                    Ok(entries) => entries
                        .into_iter()
                        .map(|e| DirEntry {
                            name: e.name,
                            metadata: file_meta(&e.meta, creation),
                        })
                        .collect(),
                    Err(_) => vec![],
                }
            }
            None => vec![],
        }
    }

    fn get_metadata(&self, path: &Path) -> Option<Metadata> {
        match self.locate(&components(path))? {
            Located::Synthetic(_) => Some(dir_meta()),
            // A registered snapshot root is a dir without opening the
            // manifest — mirrors the NFS back end's lazy `getattr`.
            Located::Snapshot(_, rest) if rest.is_empty() => Some(dir_meta()),
            Located::Snapshot(sid, rest) => {
                let entry = self.resolve(sid)?;
                let meta = entry.snapshot.metadata(&rest).ok()?;
                Some(file_meta(&meta, entry.snapshot.manifest().creation_time))
            }
        }
    }

    fn read_file(&self, path: &Path, offset: u64, length: u32) -> std::io::Result<Vec<u8>> {
        let (sid, rest) = match self.locate(&components(path)) {
            Some(Located::Snapshot(sid, rest)) if !rest.is_empty() => (sid, rest),
            Some(_) => return Ok(Vec::new()),
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "not a depot path",
                ));
            }
        };
        let entry = self.resolve(sid).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "snapshot not found")
        })?;

        let meta = entry.snapshot.metadata(&rest)?;
        if !matches!(meta.kind, FileType::File) {
            return Ok(Vec::new());
        }
        // Defensive: our reader errors if offset is past EOF. ProjFS keeps
        // requests within the file, so this normally never triggers.
        if offset >= meta.size {
            return Ok(Vec::new());
        }
        let len = u64::from(length).min(meta.size - offset);
        let bytes = self
            .handle
            .block_on(entry.snapshot.read(&rest, offset, len))?;
        Ok(bytes.to_vec())
    }
}
