//! [`projfs::Filesystem`] over a [`MountTree`]. Path layout, by number of
//! components (ProjFS hands out `\`-paths; `DepotManifestStore` wants
//! `/`-paths):
//!
//! ```text
//! []                        app ids  (synthetic children of the root)
//! [app]                     depot ids
//! [app, depot]              manifest gids
//! [app, depot, gid, rest…]  file/dir `rest` inside that snapshot
//! ```

use std::path::{Component, Path};
use std::sync::Arc;

use parking_lot::RwLock;
use projfs::{DirEntry, Filesystem, Metadata};
use steam_depot_vfs::FileType;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::FileMeta;
use tokio::runtime::Handle;

use crate::tree::{MountTree, SnapshotEntry, SnapshotId};
use crate::view::{self, SlotLookup};

pub(crate) struct ProjFsSource<C: ChunkStore + 'static> {
    tree: Arc<RwLock<MountTree<C>>>,
    handle: Handle,
}

impl<C: ChunkStore + 'static> ProjFsSource<C> {
    pub(crate) fn new(tree: Arc<RwLock<MountTree<C>>>, handle: Handle) -> Self {
        Self { tree, handle }
    }

    /// Resolve the snapshot at `<app>/<depot>/<gid>`, driving a lazy opener
    /// on first access. `block_on` is safe: ProjFS callbacks run on their
    /// own worker threads, never a tokio one.
    fn resolve(&self, app: u32, depot: u32, gid: u64) -> Option<Arc<SnapshotEntry<C>>> {
        let sid = self.tree.read().find(app, depot, gid)?;
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
                        tracing::warn!(%e, app, depot, gid, "projfs lazy resolve failed");
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

fn parse_triple(comps: &[String]) -> Option<(u32, u32, u64)> {
    let app = comps.first()?.parse().ok()?;
    let depot = comps.get(1)?.parse().ok()?;
    let gid = comps.get(2)?.parse().ok()?;
    Some((app, depot, gid))
}

/// `""`, `"/app"` or `"/app/depot"` — the key `MountTree` indexes by.
fn synthetic_prefix(comps: &[String]) -> String {
    if comps.is_empty() {
        String::new()
    } else {
        format!("/{}", comps.join("/"))
    }
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
        let comps = components(path);

        // Prefix levels (root / app / depot) live in the synthetic tree.
        if comps.len() < 3 {
            let prefix = synthetic_prefix(&comps);
            let Some(names) = self.tree.read().synthetic_children_at(&prefix) else {
                return vec![];
            };
            return names
                .into_iter()
                .map(|name| DirEntry {
                    name,
                    metadata: dir_meta(),
                })
                .collect();
        }

        let Some((app, depot, gid)) = parse_triple(&comps) else {
            return vec![];
        };
        let Some(entry) = self.resolve(app, depot, gid) else {
            return vec![];
        };
        let rest = comps[3..].join("/");
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

    fn get_metadata(&self, path: &Path) -> Option<Metadata> {
        let comps = components(path);
        match comps.len() {
            0 => Some(dir_meta()),
            1 | 2 => {
                let prefix = synthetic_prefix(&comps);
                self.tree
                    .read()
                    .synthetic_children_at(&prefix)
                    .map(|_| dir_meta())
            }
            // A registered `<gid>` is a dir without opening the manifest —
            // mirrors the NFS back end's lazy `getattr`.
            3 => {
                let (app, depot, gid) = parse_triple(&comps)?;
                self.tree.read().find(app, depot, gid).map(|_| dir_meta())
            }
            _ => {
                let (app, depot, gid) = parse_triple(&comps)?;
                let entry = self.resolve(app, depot, gid)?;
                let rest = comps[3..].join("/");
                let meta = entry.snapshot.metadata(&rest).ok()?;
                Some(file_meta(&meta, entry.snapshot.manifest().creation_time))
            }
        }
    }

    fn read_file(&self, path: &Path, offset: u64, length: u32) -> std::io::Result<Vec<u8>> {
        let comps = components(path);
        if comps.len() < 4 {
            return Ok(Vec::new());
        }
        let (app, depot, gid) = parse_triple(&comps)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "not a depot path"))?;
        let entry = self.resolve(app, depot, gid).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::NotFound, "snapshot not found")
        })?;
        let rest = comps[3..].join("/");

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
