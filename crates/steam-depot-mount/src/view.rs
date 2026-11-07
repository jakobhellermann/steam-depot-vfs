// TODO(ai-review): review for correctness/style
//! Backend-neutral reads of [`MountTree`]. Answers "what is node N" and
//! "what is in directory N" in terms of kind, size and mtime, leaving
//! the protocol-specific attribute structs to the back ends.

#![cfg_attr(not(any(feature = "fuse", feature = "nfs")), allow(dead_code))]

use std::sync::Arc;

use futures_util::StreamExt;
use parking_lot::RwLock;
use steam_depot_vfs::FileType;
use steam_depot_vfs::chunk_store::ChunkStore;

use crate::inode::{self, Ino, SYNTHETIC};
use crate::tree::{LazyEntry, MountTree, SlotState, SnapshotEntry, SnapshotId, sanitize_name};

/// How many namers of one dir run at once.
const NAMING_CONCURRENCY: usize = 4;

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

/// Whether synthetic dir `ino` holds snapshots that get their name on first listing.
pub(crate) fn needs_naming<C: ChunkStore>(tree: &RwLock<MountTree<C>>, ino: Ino) -> bool {
    tree.read()
        .synthetic(ino)
        .is_some_and(|d| !d.unnamed.is_empty())
}

/// Run the namers of synthetic dir `ino`'s unnamed snapshots and enter
/// them under their names. A failed namer leaves its snapshot unnamed for
/// the next call to retry, and fails this one.
pub(crate) async fn ensure_named<C: ChunkStore>(
    tree: &RwLock<MountTree<C>>,
    ino: Ino,
) -> Result<(), String> {
    let Some(lock) = tree.read().synthetic(ino).map(|d| Arc::clone(&d.naming)) else {
        return Ok(());
    };
    let _naming = lock.lock().await;
    let pending = tree.read().pending_names(ino);
    if pending.is_empty() {
        return Ok(());
    }
    let results: Vec<_> = futures_util::stream::iter(pending)
        .map(|p| async move {
            let name = (p.namer)().await;
            (p, name)
        })
        .buffer_unordered(NAMING_CONCURRENCY)
        .collect()
        .await;

    let mut named = Vec::with_capacity(results.len());
    let mut errors = Vec::new();
    for (p, result) in results {
        match result {
            Ok(name) => {
                let name = name
                    .as_deref()
                    .and_then(sanitize_name)
                    .unwrap_or_else(|| p.manifest_gid.to_string());
                named.push((p, name));
            }
            Err(e) => errors.push(format!("manifest {}: {e}", p.manifest_gid)),
        }
    }
    tree.write().assign_names(ino, named);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use bytes::Bytes;
    use steam_depot_vfs::ChunkHash;

    use super::*;
    use crate::tree::{self, Placement};

    struct NoChunks;

    impl ChunkStore for NoChunks {
        async fn get(&self, _sha: ChunkHash) -> steam_depot_vfs::Result<Bytes> {
            unreachable!("naming never reads chunks")
        }
    }

    type Tree = RwLock<MountTree<NoChunks>>;

    fn unopenable() -> tree::Opener<NoChunks> {
        tree::opener(|| async { unreachable!("naming never opens a manifest") })
    }

    fn add(tree: &Tree, app_name: &str, gid: u64, creation_time: u32, name: Option<&'static str>) {
        tree.write()
            .add_lazy(
                1,
                2,
                gid,
                unopenable(),
                Some(creation_time),
                Placement {
                    app_name: Some(app_name.to_owned()),
                    namer: Some(tree::namer(
                        move || async move { Ok(name.map(str::to_owned)) },
                    )),
                },
            )
            .expect("add");
    }

    fn child(tree: &Tree, dir: Ino, name: &str) -> Ino {
        synthetic_child(&tree.read(), dir, name)
            .unwrap_or_else(|| panic!("no {name:?}"))
            .ino
    }

    /// Dir entry name → manifest gid of the snapshot it points at.
    fn listing(tree: &Tree, dir: Ino) -> BTreeMap<String, u64> {
        let tree = tree.read();
        synthetic_dir(&tree, dir)
            .expect("dir")
            .into_iter()
            .map(|(name, node)| {
                let (sid, _) = inode::unpack(node.ino);
                (name, tree.slot(sid).expect("slot").manifest_gid)
            })
            .collect()
    }

    #[test]
    fn ids_placement_names_by_ids() {
        let tree = Tree::new(MountTree::new());
        tree.write()
            .add_lazy(10, 20, 30, unopenable(), None, Placement::IDS)
            .expect("add");
        let depot = child(&tree, child(&tree, inode::ROOT, "10"), "20");
        assert!(!needs_naming(&tree, depot));
        assert_eq!(listing(&tree, depot), BTreeMap::from([("30".into(), 30)]));
    }

    #[tokio::test]
    async fn names_appear_on_first_listing_and_duplicates_count_up_by_age() {
        let tree = Tree::new(MountTree::new());
        add(&tree, "Some: Game", 300, 3_000, Some("1.5.78"));
        add(
            &tree,
            "ignored, the app dir exists",
            100,
            1_000,
            Some("1.5.78"),
        );
        add(&tree, "", 200, 2_000, Some("1.5.77"));
        add(&tree, "", 400, 4_000, None);
        add(&tree, "", 500, 5_000, Some(" ../"));

        assert_eq!(
            synthetic_dir(&tree.read(), inode::ROOT)
                .expect("root")
                .into_iter()
                .map(|(name, _)| name)
                .collect::<Vec<_>>(),
            ["Some_ Game"],
        );
        let depot = child(&tree, child(&tree, inode::ROOT, "Some_ Game"), "2");
        assert!(needs_naming(&tree, depot));
        assert_eq!(listing(&tree, depot), BTreeMap::new());

        ensure_named(&tree, depot).await.expect("naming");
        assert!(!needs_naming(&tree, depot));
        assert_eq!(
            listing(&tree, depot),
            BTreeMap::from([
                ("1.5.78".into(), 100),
                ("1.5.78 (2)".into(), 300),
                ("1.5.77".into(), 200),
                ("400".into(), 400),
                (".._".into(), 500),
            ]),
        );

        add(&tree, "", 50, 500, Some("1.5.78"));
        ensure_named(&tree, depot).await.expect("naming");
        assert_eq!(listing(&tree, depot).get("1.5.78 (3)"), Some(&50));
    }

    #[tokio::test]
    async fn a_failed_namer_fails_the_listing_and_retries_on_the_next() {
        let tree = Tree::new(MountTree::new());
        add(&tree, "Game", 100, 1_000, Some("1.0.1"));
        let fail = Arc::new(AtomicBool::new(true));
        let fail_in_namer = Arc::clone(&fail);
        tree.write()
            .add_lazy(
                1,
                2,
                200,
                unopenable(),
                Some(2_000),
                Placement {
                    app_name: None,
                    namer: Some(tree::namer(move || {
                        let fail = fail_in_namer.load(Ordering::Relaxed);
                        async move {
                            if fail {
                                Err(std::io::Error::other("no depot key"))
                            } else {
                                Ok(Some("1.0.2".to_owned()))
                            }
                        }
                    })),
                },
            )
            .expect("add");
        let depot = child(&tree, child(&tree, inode::ROOT, "Game"), "2");

        assert_eq!(
            ensure_named(&tree, depot).await,
            Err("manifest 200: no depot key".to_owned()),
        );
        assert!(needs_naming(&tree, depot));
        assert_eq!(
            listing(&tree, depot),
            BTreeMap::from([("1.0.1".into(), 100)])
        );

        fail.store(false, Ordering::Relaxed);
        ensure_named(&tree, depot).await.expect("naming");
        assert_eq!(
            listing(&tree, depot),
            BTreeMap::from([("1.0.1".into(), 100), ("1.0.2".into(), 200)]),
        );
    }

    #[tokio::test]
    async fn removing_an_unnamed_snapshot_drops_it_from_naming() {
        let tree = Tree::new(MountTree::new());
        add(&tree, "Game", 100, 1_000, Some("1.0"));
        let depot = child(&tree, child(&tree, inode::ROOT, "Game"), "2");
        let sid = SnapshotId(tree.read().synthetic(depot).expect("dir").unnamed[0].0);
        tree.write().remove(sid).expect("removed");
        assert!(!needs_naming(&tree, depot));
        ensure_named(&tree, depot).await.expect("naming");
        assert_eq!(listing(&tree, depot), BTreeMap::new());
    }
}
