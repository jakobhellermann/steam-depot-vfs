// TODO(ai-review): review for correctness/style
//! `Filesystem` impl on top of [`MountTree`]. Bridges fuser's blocking
//! callbacks to async operations on a Tokio runtime supplied by the
//! caller. We hold a `Handle`, not a `Runtime`, so the FUSE adapter
//! shares the binary's main runtime instead of building its own.

use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    Errno, FileAttr, FileType, Filesystem, Generation, INodeNo, OpenFlags, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, Request,
};
use parking_lot::RwLock;
use steam_depot_vfs::FileKind;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::Entry;
use tokio::runtime::Handle;

use crate::inode::{self, SYNTHETIC};
use crate::tree::{LazyEntry, MountTree, SlotState, SnapshotEntry, SnapshotId};

/// FUSE attribute TTL. Snapshots are immutable for their lifetime; on
/// snapshot remove the kernel will see ENOENT only after this expires.
/// One hour matches user expectations for "static-ish" content.
const TTL: Duration = Duration::from_secs(60 * 60);

pub(crate) struct FuseFs<C: ChunkStore + 'static> {
    tree: Arc<RwLock<MountTree<C>>>,
    rt: Handle,
}

impl<C: ChunkStore + 'static> FuseFs<C> {
    pub fn new(tree: Arc<RwLock<MountTree<C>>>, rt: Handle) -> Self {
        Self { tree, rt }
    }

    /// Look up `sid` in the tree.
    fn slot_lookup(&self, sid: inode::SnapshotId) -> SlotLookup<C> {
        let tree = self.tree.read();
        let Some(slot) = tree.slot(sid) else {
            return SlotLookup::Missing;
        };
        match &slot.state {
            SlotState::Ready(entry) => SlotLookup::Ready(Arc::clone(entry)),
            SlotState::Pending(lazy) => SlotLookup::Pending(Arc::clone(lazy)),
        }
    }

    /// Drive a pending lazy opener (if not already done) and promote
    /// the slot to `Ready`. Concurrent callers coalesce on the slot's
    /// `OnceCell`.
    async fn resolve(
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
}

/// Outcome of looking up a snapshot slot — three-way so callsites can
/// branch on it without nested `Option`/`Result`.
enum SlotLookup<C: ChunkStore> {
    /// Slot is loaded; serve from `entry` directly.
    Ready(Arc<SnapshotEntry<C>>),
    /// Slot is registered but not yet opened. Drive the opener.
    Pending(Arc<LazyEntry<C>>),
    /// `sid` is out of bounds or the slot was removed. Reply ENOENT.
    Missing,
}

impl<C: ChunkStore + 'static> Filesystem for FuseFs<C> {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        let (sid, _) = inode::unpack(parent);
        if sid == SYNTHETIC {
            // Synthetic dirs are fully described by the tree itself.
            let tree = self.tree.read();
            match resolve_synthetic_child(&tree, parent, name) {
                Some((_ino, attr)) => reply.entry(&TTL, &attr, Generation(0)),
                None => reply.error(Errno::ENOENT),
            }
            return;
        }
        // Inside a snapshot subtree — make sure it's resolved first.
        let entry = match self.slot_lookup(sid) {
            SlotLookup::Ready(e) => e,
            SlotLookup::Pending(lazy) => {
                let tree = Arc::clone(&self.tree);
                let name = name.to_string();
                self.rt.spawn(async move {
                    match Self::resolve(tree, lazy, SnapshotId(sid)).await {
                        Ok(entry) => match resolve_snapshot_child(&entry, parent, &name) {
                            Some((_ino, attr)) => reply.entry(&TTL, &attr, Generation(0)),
                            None => reply.error(Errno::ENOENT),
                        },
                        Err(e) => {
                            tracing::warn!(%e, "lazy resolve failed during lookup");
                            reply.error(Errno::EIO);
                        }
                    }
                });
                return;
            }
            SlotLookup::Missing => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        match resolve_snapshot_child(&entry, parent, name) {
            Some((_ino, attr)) => reply.entry(&TTL, &attr, Generation(0)),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn getattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: Option<fuser::FileHandle>,
        reply: ReplyAttr,
    ) {
        let (sid, _) = inode::unpack(ino);
        if sid == SYNTHETIC {
            let tree = self.tree.read();
            match attr_for_synthetic(&tree, ino) {
                Some(attr) => reply.attr(&TTL, &attr),
                None => reply.error(Errno::ENOENT),
            }
            return;
        }
        // Inside a snapshot. For idx==0 (the gid root) we can answer
        // without resolving, since it's just "a directory" until we
        // need actual file metadata.
        let (_, idx) = inode::unpack(ino);
        if idx == 0 {
            // Snapshot root: answer without resolving so we don't force a
            // manifest fetch just for getattr. mtime falls back to EPOCH
            // until the slot has a cached `creation_time` (set on eager
            // add and on lazy promote).
            let mtime = self
                .tree
                .read()
                .slot(sid)
                .and_then(|s| s.creation_time)
                .map(manifest_mtime)
                .unwrap_or(UNIX_EPOCH);
            reply.attr(&TTL, &dir_attr(ino, 0, mtime));
            return;
        }
        let entry = match self.slot_lookup(sid) {
            SlotLookup::Ready(e) => e,
            SlotLookup::Pending(lazy) => {
                let tree = Arc::clone(&self.tree);
                self.rt.spawn(async move {
                    match Self::resolve(tree, lazy, SnapshotId(sid)).await {
                        Ok(entry) => match attr_within_snapshot(&entry, ino) {
                            Some(attr) => reply.attr(&TTL, &attr),
                            None => reply.error(Errno::ENOENT),
                        },
                        Err(e) => {
                            tracing::warn!(%e, "lazy resolve failed during getattr");
                            reply.error(Errno::EIO);
                        }
                    }
                });
                return;
            }
            SlotLookup::Missing => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        match attr_within_snapshot(&entry, ino) {
            Some(attr) => reply.attr(&TTL, &attr),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let (sid, _) = inode::unpack(ino);
        if sid == SYNTHETIC {
            let entries = {
                let tree = self.tree.read();
                match collect_synthetic_dir(&tree, ino) {
                    Some(e) => e,
                    None => {
                        reply.error(Errno::ENOENT);
                        return;
                    }
                }
            };
            emit_readdir(ino, &entries, offset, reply);
            return;
        }
        let entry = match self.slot_lookup(sid) {
            SlotLookup::Ready(e) => e,
            SlotLookup::Pending(lazy) => {
                let tree = Arc::clone(&self.tree);
                self.rt.spawn(async move {
                    match Self::resolve(tree, lazy, SnapshotId(sid)).await {
                        Ok(entry) => {
                            let kids = collect_snapshot_dir(&entry, ino).unwrap_or_default();
                            emit_readdir(ino, &kids, offset, reply);
                        }
                        Err(e) => {
                            tracing::warn!(%e, "lazy resolve failed during readdir");
                            reply.error(Errno::EIO);
                        }
                    }
                });
                return;
            }
            SlotLookup::Missing => {
                reply.error(Errno::ENOENT);
                return;
            }
        };
        let kids = collect_snapshot_dir(&entry, ino).unwrap_or_default();
        emit_readdir(ino, &kids, offset, reply);
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: ReplyData,
    ) {
        let (sid, idx) = inode::unpack(ino);
        if sid == SYNTHETIC {
            reply.error(Errno::EISDIR);
            return;
        }
        let Some(file_idx) = (idx as usize).checked_sub(1) else {
            reply.error(Errno::EISDIR);
            return;
        };
        let tree_arc = Arc::clone(&self.tree);
        let initial = self.slot_lookup(sid);
        self.rt.spawn(async move {
            let entry = match initial {
                SlotLookup::Ready(e) => e,
                SlotLookup::Pending(lazy) => {
                    match Self::resolve(tree_arc, lazy, SnapshotId(sid)).await {
                        Ok(e) => e,
                        Err(e) => {
                            tracing::warn!(%e, "lazy resolve failed during read");
                            reply.error(Errno::EIO);
                            return;
                        }
                    }
                }
                SlotLookup::Missing => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            };
            let path = match entry.snapshot.manifest().files.get(file_idx) {
                Some(file) => {
                    if matches!(file.kind, FileKind::Symlink) {
                        tracing::warn!(
                            path = %file.path,
                            target = ?file.linktarget,
                            "reading symlink as regular file; link target not resolved",
                        );
                    }
                    file.path.clone()
                }
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            };
            let mut buf = Vec::with_capacity(size as usize);
            match entry
                .snapshot
                .read_into(&path, offset, size as u64, &mut buf)
                .await
            {
                Ok(()) => reply.data(&buf),
                Err(e) => {
                    tracing::warn!(path = %path, offset, size, %e, "read failed");
                    reply.error(Errno::EIO);
                }
            }
        });
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: fuser::AccessFlags, reply: ReplyEmpty) {
        // Read-only mount, all paths are world-readable; rather than
        // letting fuser's default fall through to ENOSYS (which clutters
        // logs), say "yes" to every check. The kernel does its own
        // perm check against the `perm` we report in getattr anyway.
        reply.ok();
    }
}

/// Push `entries` into a `ReplyDirectory`, including `.` and `..`.
fn emit_readdir(
    ino: INodeNo,
    entries: &[(INodeNo, FileType, String)],
    offset: u64,
    mut reply: ReplyDirectory,
) {
    // Cookie semantics: each entry is given a `next_offset` that the
    // kernel echoes back so we can resume. Skip entries whose cookie is
    // <= the offset the kernel already saw. `reply.add` returns true
    // when the buffer is full — stop adding but still call `reply.ok()`
    // so the kernel knows to come back with a higher offset instead of
    // looping on the same one.
    let mut all: Vec<(INodeNo, FileType, &str)> = Vec::with_capacity(entries.len() + 2);
    all.push((ino, FileType::Directory, "."));
    all.push((ino, FileType::Directory, ".."));
    for (child_ino, kind, name) in entries {
        all.push((*child_ino, *kind, name.as_str()));
    }
    for (i, (child_ino, kind, name)) in all.iter().enumerate() {
        let next_offset = (i + 1) as u64;
        if next_offset <= offset {
            continue;
        }
        if reply.add(*child_ino, next_offset, *kind, name) {
            break;
        }
    }
    reply.ok();
}

/// Resolve `(parent, name)` on a synthetic dir parent.
fn resolve_synthetic_child<C: ChunkStore>(
    tree: &MountTree<C>,
    parent: INodeNo,
    name: &str,
) -> Option<(INodeNo, FileAttr)> {
    let dir = tree.synthetic(parent)?;
    let &child = dir.children.get(name)?;
    let attr = attr_for_synthetic(tree, child).or_else(|| {
        // Child lives inside a snapshot — only its idx==0 root makes
        // sense to answer here (a synthetic parent's child is always
        // either another synthetic dir or a snapshot root). Use the
        // slot's cached creation_time if available, EPOCH otherwise.
        let (sid, idx) = inode::unpack(child);
        if idx != 0 {
            return None;
        }
        let mtime = tree
            .slot(sid)
            .and_then(|s| s.creation_time)
            .map(manifest_mtime)
            .unwrap_or(UNIX_EPOCH);
        Some(dir_attr(child, 0, mtime))
    })?;
    Some((child, attr))
}

/// Resolve `(parent, name)` within a snapshot subtree.
fn resolve_snapshot_child<C: ChunkStore>(
    entry: &SnapshotEntry<C>,
    parent: INodeNo,
    name: &str,
) -> Option<(INodeNo, FileAttr)> {
    let (sid, idx) = inode::unpack(parent);
    let parent_path = snapshot_path_of(entry, idx)?;
    let child_path = if parent_path.is_empty() {
        name.to_string()
    } else {
        format!("{parent_path}/{name}")
    };
    let child_idx = entry.snapshot.index_of(&child_path)?;
    let child_ino = inode::pack(sid, (child_idx + 1) as u64);
    let manifest = entry.snapshot.manifest();
    let f = manifest.files.get(child_idx)?;
    let mtime = manifest_mtime(manifest.creation_time);
    Some((child_ino, file_kind_attr(f.kind, child_ino, f.size, mtime)))
}

fn attr_for_synthetic<C: ChunkStore>(tree: &MountTree<C>, ino: INodeNo) -> Option<FileAttr> {
    let (sid, _) = inode::unpack(ino);
    if sid != SYNTHETIC {
        return None;
    }
    tree.synthetic(ino)?;
    Some(dir_attr(ino, 0, UNIX_EPOCH))
}

fn attr_within_snapshot<C: ChunkStore>(entry: &SnapshotEntry<C>, ino: INodeNo) -> Option<FileAttr> {
    let (_, idx) = inode::unpack(ino);
    let manifest = entry.snapshot.manifest();
    let mtime = manifest_mtime(manifest.creation_time);
    if idx == 0 {
        return Some(dir_attr(ino, 0, mtime));
    }
    let f = manifest.files.get(idx as usize - 1)?;
    Some(file_kind_attr(f.kind, ino, f.size, mtime))
}

fn file_kind_attr(kind: FileKind, ino: INodeNo, size: u64, mtime: SystemTime) -> FileAttr {
    match kind {
        FileKind::Directory => dir_attr(ino, size, mtime),
        FileKind::File | FileKind::Symlink => file_attr(ino, size, mtime),
    }
}

/// Path of inode `(sid, idx)` *within its snapshot*. Empty string =
/// snapshot root. Returns `None` if `idx` is out of range.
fn snapshot_path_of<C: ChunkStore>(entry: &SnapshotEntry<C>, idx: u64) -> Option<String> {
    if idx == 0 {
        return Some(String::new());
    }
    let file_idx = (idx as usize).checked_sub(1)?;
    Some(entry.snapshot.manifest().files.get(file_idx)?.path.clone())
}

fn collect_synthetic_dir<C: ChunkStore>(
    tree: &MountTree<C>,
    ino: INodeNo,
) -> Option<Vec<(INodeNo, FileType, String)>> {
    let dir = tree.synthetic(ino)?;
    let mut out: Vec<_> = dir
        .children
        .iter()
        .map(|(name, &child_ino)| (child_ino, FileType::Directory, name.clone()))
        .collect();
    out.sort_by(|a, b| a.2.cmp(&b.2));
    Some(out)
}

fn collect_snapshot_dir<C: ChunkStore>(
    entry: &SnapshotEntry<C>,
    ino: INodeNo,
) -> Option<Vec<(INodeNo, FileType, String)>> {
    let (sid, idx) = inode::unpack(ino);
    let dir_path = snapshot_path_of(entry, idx)?;
    let entries: Vec<Entry> = entry.snapshot.list_dir(&dir_path).ok()?;
    let mut out = Vec::with_capacity(entries.len());
    for e in entries {
        let child = if dir_path.is_empty() {
            e.name.clone()
        } else {
            format!("{dir_path}/{}", e.name)
        };
        let Some(child_idx) = entry.snapshot.index_of(&child) else {
            continue;
        };
        let kind = match e.meta.kind {
            FileKind::Directory => FileType::Directory,
            FileKind::Symlink | FileKind::File => FileType::RegularFile,
        };
        let child_ino = inode::pack(sid, (child_idx + 1) as u64);
        out.push((child_ino, kind, e.name));
    }
    Some(out)
}

fn dir_attr(ino: INodeNo, size: u64, mtime: SystemTime) -> FileAttr {
    base_attr(ino, size, FileType::Directory, 0o555, 2, mtime)
}

fn file_attr(ino: INodeNo, size: u64, mtime: SystemTime) -> FileAttr {
    base_attr(ino, size, FileType::RegularFile, 0o444, 1, mtime)
}

fn base_attr(
    ino: INodeNo,
    size: u64,
    kind: FileType,
    perm: u16,
    nlink: u32,
    mtime: SystemTime,
) -> FileAttr {
    FileAttr {
        ino,
        size,
        blocks: if matches!(kind, FileType::RegularFile) {
            size.div_ceil(512)
        } else {
            0
        },
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        kind,
        perm,
        nlink,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

/// Convert a manifest's `creation_time` (Unix seconds) into a `SystemTime`.
fn manifest_mtime(creation_time: u32) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(u64::from(creation_time))
}
