// TODO(ai-review): review for correctness/style
//! Backend-neutral reads of [`MountTree`]. Answers "what is node N" and
//! "what is in directory N" in terms of kind, size and mtime, leaving
//! the protocol-specific attribute structs to the back ends.

#![cfg_attr(not(any(feature = "fuse", feature = "nfs")), allow(dead_code))]

use std::sync::Arc;

use parking_lot::RwLock;
use steam_depot_vfs::FileType;
use steam_depot_vfs::chunk_store::ChunkStore;

use crate::inode::{self, Ino, SYNTHETIC};
use crate::tree::{LazyEntry, MountTree, SlotState, SnapshotEntry, SnapshotId};

#[derive(Debug, Clone)]
pub(crate) enum NodeKind {
    Dir,
    File {
        executable: bool,
    },
    Symlink {
        // Not every back end can represent a link target.
        #[allow(dead_code)]
        target: String,
    },
}

/// A resolved node in the mount tree.
#[derive(Debug, Clone)]
pub(crate) struct Node {
    pub ino: Ino,
    pub kind: NodeKind,
    pub size: u64,
    /// Manifest `creation_time` in Unix seconds; 0 where unknown (the
    /// synthetic prefix dirs and lazy snapshots that carry no hint).
    pub mtime_secs: u32,
}

impl Node {
    pub(crate) fn dir(ino: Ino, mtime_secs: u32) -> Self {
        Self {
            ino,
            kind: NodeKind::Dir,
            size: 0,
            mtime_secs,
        }
    }
}

/// Outcome of looking up a snapshot slot — three-way so callsites can
/// branch on it without nested `Option`/`Result`.
pub(crate) enum SlotLookup<C: ChunkStore> {
    /// Slot is loaded; serve from `entry` directly.
    Ready(Arc<SnapshotEntry<C>>),
    /// Slot is registered but not yet opened. Drive the opener.
    Pending(Arc<LazyEntry<C>>),
    /// The id is out of bounds or the slot was removed.
    Missing,
}

pub(crate) fn slot_lookup<C: ChunkStore>(
    tree: &RwLock<MountTree<C>>,
    sid: inode::SnapshotId,
) -> SlotLookup<C> {
    let tree = tree.read();
    let Some(slot) = tree.slot(sid) else {
        return SlotLookup::Missing;
    };
    match &slot.state {
        SlotState::Ready(entry) => SlotLookup::Ready(Arc::clone(entry)),
        SlotState::Pending(lazy) => SlotLookup::Pending(Arc::clone(lazy)),
    }
}

/// Drive a pending lazy opener (if not already done) and promote the
/// slot to `Ready`. Concurrent callers coalesce on the slot's `OnceCell`.
pub(crate) async fn resolve<C: ChunkStore>(
    tree: Arc<RwLock<MountTree<C>>>,
    lazy: Arc<LazyEntry<C>>,
    sid: SnapshotId,
) -> Result<Arc<SnapshotEntry<C>>, String> {
    let cell = Arc::clone(&lazy.cell);
    let opener = Arc::clone(&lazy.opener);
    let result = cell
        .get_or_init(|| async move {
            match (opener)().await {
                Ok(store) => Ok(Arc::new(SnapshotEntry { snapshot: store })),
                Err(e) => Err(e.to_string()),
            }
        })
        .await
        .clone()?;
    tree.write().promote(sid, Arc::clone(&result));
    Ok(result)
}

/// mtime for a snapshot root, taken from the slot's cached
/// `creation_time` so callers don't have to resolve the manifest.
pub(crate) fn slot_mtime<C: ChunkStore>(tree: &MountTree<C>, sid: inode::SnapshotId) -> u32 {
    tree.slot(sid).and_then(|s| s.creation_time).unwrap_or(0)
}

pub(crate) fn synthetic_node<C: ChunkStore>(tree: &MountTree<C>, ino: Ino) -> Option<Node> {
    tree.synthetic(ino)?;
    Some(Node::dir(ino, 0))
}

pub(crate) fn synthetic_child<C: ChunkStore>(
    tree: &MountTree<C>,
    parent: Ino,
    name: &str,
) -> Option<Node> {
    let dir = tree.synthetic(parent)?;
    let &child = dir.children.get(name)?;
    Some(node_below_synthetic(tree, child))
}

pub(crate) fn synthetic_dir<C: ChunkStore>(
    tree: &MountTree<C>,
    ino: Ino,
) -> Option<Vec<(String, Node)>> {
    let dir = tree.synthetic(ino)?;
    let mut out: Vec<(String, Node)> = dir
        .children
        .iter()
        .map(|(name, &child)| (name.clone(), node_below_synthetic(tree, child)))
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Some(out)
}

/// A synthetic dir's child is either another synthetic dir or the root
/// of a snapshot; both are directories, they differ only in mtime.
fn node_below_synthetic<C: ChunkStore>(tree: &MountTree<C>, child: Ino) -> Node {
    let (sid, _) = inode::unpack(child);
    let mtime = if sid == SYNTHETIC {
        0
    } else {
        slot_mtime(tree, sid)
    };
    Node::dir(child, mtime)
}

pub(crate) fn snapshot_node<C: ChunkStore>(entry: &SnapshotEntry<C>, ino: Ino) -> Option<Node> {
    let (_, idx) = inode::unpack(ino);
    let manifest = entry.snapshot.manifest();
    if idx == 0 {
        return Some(Node::dir(ino, manifest.creation_time));
    }
    let f = manifest.files.get(idx as usize - 1)?;
    Some(Node {
        ino,
        kind: kind_of(f.file_type(), f.executable(), f.linktarget()),
        size: f.size,
        mtime_secs: manifest.creation_time,
    })
}

pub(crate) fn snapshot_child<C: ChunkStore>(
    entry: &SnapshotEntry<C>,
    parent: Ino,
    name: &str,
) -> Option<Node> {
    let (sid, idx) = inode::unpack(parent);
    let parent_path = path_within_snapshot(entry, idx)?;
    let child_path = join(&parent_path, name);
    let child_idx = entry.snapshot.index_of(&child_path)?;
    snapshot_node(entry, inode::pack(sid, (child_idx + 1) as u64))
}

pub(crate) fn snapshot_dir<C: ChunkStore>(
    entry: &SnapshotEntry<C>,
    ino: Ino,
) -> Option<Vec<(String, Node)>> {
    let (sid, idx) = inode::unpack(ino);
    let dir_path = path_within_snapshot(entry, idx)?;
    let listed = entry.snapshot.list_dir(&dir_path).ok()?;
    let manifest = entry.snapshot.manifest();
    let mut out = Vec::with_capacity(listed.len());
    for e in listed {
        let Some(child_idx) = entry.snapshot.index_of(&join(&dir_path, &e.name)) else {
            continue;
        };
        out.push((
            e.name,
            Node {
                ino: inode::pack(sid, (child_idx + 1) as u64),
                kind: kind_of(
                    e.meta.kind,
                    manifest
                        .files
                        .get(child_idx)
                        .is_some_and(|f| f.executable()),
                    e.meta.linktarget.as_deref(),
                ),
                size: e.meta.size,
                mtime_secs: manifest.creation_time,
            },
        ));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Some(out)
}

/// Path of index `idx` *within its snapshot*. Empty string = snapshot
/// root. `None` if `idx` is out of range.
pub(crate) fn path_within_snapshot<C: ChunkStore>(
    entry: &SnapshotEntry<C>,
    idx: u64,
) -> Option<String> {
    if idx == 0 {
        return Some(String::new());
    }
    let file_idx = (idx as usize).checked_sub(1)?;
    Some(entry.snapshot.manifest().files.get(file_idx)?.path.clone())
}

/// Symlinks without a target can't be represented as links, so they
/// degrade to regular files — same as the FUSE back end does.
fn kind_of(kind: FileType, executable: bool, linktarget: Option<&str>) -> NodeKind {
    match (kind, linktarget) {
        (FileType::Directory, _) => NodeKind::Dir,
        (FileType::Symlink, Some(target)) => NodeKind::Symlink {
            target: target.to_string(),
        },
        _ => NodeKind::File { executable },
    }
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}
