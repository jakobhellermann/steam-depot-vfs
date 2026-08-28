// TODO(ai-review): review for correctness/style
//! `Filesystem` impl on top of [`MountTree`]. Bridges fuser's blocking
//! callbacks to async operations on a Tokio runtime supplied by the
//! caller. We hold a `Handle`, not a `Runtime`, so the FUSE adapter
//! shares the binary's main runtime instead of building its own.
//!
//! The tree walking itself lives in [`crate::view`]; what's left here is
//! fuser's callback protocol and the [`Node`] → [`FileAttr`] mapping.

use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use fuser::{
    Errno, FileAttr, FileType, Filesystem, Generation, INodeNo, OpenFlags, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, Request,
};
use parking_lot::RwLock;
use steam_depot_vfs::chunk_store::ChunkStore;
use tokio::runtime::Handle;

use crate::inode::{self, Ino, SYNTHETIC};
use crate::tree::{MountTree, SnapshotEntry, SnapshotId};
use crate::view::{self, Node, NodeKind, SlotLookup};

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
}

impl<C: ChunkStore + 'static> Filesystem for FuseFs<C> {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        let parent = parent.0;
        let (sid, _) = inode::unpack(parent);
        if sid == SYNTHETIC {
            // Synthetic dirs are fully described by the tree itself.
            match view::synthetic_child(&self.tree.read(), parent, name) {
                Some(node) => reply.entry(&TTL, &attr_of(&node), Generation(0)),
                None => reply.error(Errno::ENOENT),
            }
            return;
        }
        let name = name.to_string();
        self.with_snapshot(
            sid,
            "lookup",
            reply,
            move |entry, reply| match view::snapshot_child(entry, parent, &name) {
                Some(node) => reply.entry(&TTL, &attr_of(&node), Generation(0)),
                None => reply.error(Errno::ENOENT),
            },
        );
    }

    fn getattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: Option<fuser::FileHandle>,
        reply: ReplyAttr,
    ) {
        let ino = ino.0;
        let (sid, idx) = inode::unpack(ino);
        if sid == SYNTHETIC {
            match view::synthetic_node(&self.tree.read(), ino) {
                Some(node) => reply.attr(&TTL, &attr_of(&node)),
                None => reply.error(Errno::ENOENT),
            }
            return;
        }
        // Snapshot root: answer without resolving so we don't force a
        // manifest fetch just for getattr. mtime falls back to the
        // epoch until the slot has a cached `creation_time` (set on
        // eager add and on lazy promote).
        if idx == 0 {
            let tree = self.tree.read();
            if tree.slot(sid).is_none() {
                reply.error(Errno::ENOENT);
                return;
            }
            let node = Node::dir(ino, view::slot_mtime(&tree, sid));
            reply.attr(&TTL, &attr_of(&node));
            return;
        }
        self.with_snapshot(
            sid,
            "getattr",
            reply,
            move |entry, reply| match view::snapshot_node(entry, ino) {
                Some(node) => reply.attr(&TTL, &attr_of(&node)),
                None => reply.error(Errno::ENOENT),
            },
        );
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        let ino = ino.0;
        let (sid, _) = inode::unpack(ino);
        if sid == SYNTHETIC {
            match view::synthetic_dir(&self.tree.read(), ino) {
                Some(entries) => emit_readdir(ino, &entries, offset, reply),
                None => reply.error(Errno::ENOENT),
            }
            return;
        }
        self.with_snapshot(sid, "readdir", reply, move |entry, reply| {
            let entries = view::snapshot_dir(entry, ino).unwrap_or_default();
            emit_readdir(ino, &entries, offset, reply);
        });
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
        let (sid, idx) = inode::unpack(ino.0);
        if sid == SYNTHETIC {
            reply.error(Errno::EISDIR);
            return;
        }
        let Some(file_idx) = (idx as usize).checked_sub(1) else {
            reply.error(Errno::EISDIR);
            return;
        };
        let tree = Arc::clone(&self.tree);
        let initial = view::slot_lookup(&self.tree, sid);
        self.rt.spawn(async move {
            let entry = match resolve_lookup(tree, initial, sid, "read").await {
                Ok(entry) => entry,
                Err(errno) => {
                    reply.error(errno);
                    return;
                }
            };
            let Some(file) = entry.snapshot.manifest().files.get(file_idx) else {
                reply.error(Errno::ENOENT);
                return;
            };
            if file.linktarget().is_some() {
                tracing::warn!(
                    path = %file.path,
                    target = ?file.linktarget(),
                    "reading symlink as regular file; link target not resolved",
                );
            }
            let path = file.path.clone();
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

/// Everything a fuser reply type needs so [`FuseFs::with_snapshot`] can
/// hand the error path back to the kernel without knowing which reply
/// it holds.
trait ReplyError: Send + 'static {
    fn error(self, errno: Errno);
}

macro_rules! impl_reply_error {
    ($($ty:ty),+) => {
        $(impl ReplyError for $ty {
            fn error(self, errno: Errno) {
                <$ty>::error(self, errno)
            }
        })+
    };
}

impl_reply_error!(ReplyEntry, ReplyAttr, ReplyDirectory);

impl<C: ChunkStore + 'static> FuseFs<C> {
    /// Run `f` against the snapshot that owns `sid`, on this thread if
    /// the slot is already loaded and on the runtime if a lazy opener
    /// has to run first. `op` only names the operation for the log line
    /// when resolving fails.
    fn with_snapshot<R, F>(&self, sid: inode::SnapshotId, op: &'static str, reply: R, f: F)
    where
        R: ReplyError,
        F: FnOnce(&SnapshotEntry<C>, R) + Send + 'static,
    {
        match view::slot_lookup(&self.tree, sid) {
            SlotLookup::Ready(entry) => f(&entry, reply),
            SlotLookup::Pending(lazy) => {
                let tree = Arc::clone(&self.tree);
                self.rt.spawn(async move {
                    match view::resolve(tree, lazy, SnapshotId(sid)).await {
                        Ok(entry) => f(&entry, reply),
                        Err(e) => {
                            tracing::warn!(%e, op, "lazy resolve failed");
                            reply.error(Errno::EIO);
                        }
                    }
                });
            }
            SlotLookup::Missing => reply.error(Errno::ENOENT),
        }
    }
}

/// `read` can't use [`FuseFs::with_snapshot`] — it is already inside a
/// spawned future, so it resolves the lookup it took beforehand itself.
async fn resolve_lookup<C: ChunkStore + 'static>(
    tree: Arc<RwLock<MountTree<C>>>,
    lookup: SlotLookup<C>,
    sid: inode::SnapshotId,
    op: &'static str,
) -> Result<Arc<SnapshotEntry<C>>, Errno> {
    match lookup {
        SlotLookup::Ready(entry) => Ok(entry),
        SlotLookup::Pending(lazy) => {
            view::resolve(tree, lazy, SnapshotId(sid))
                .await
                .map_err(|e| {
                    tracing::warn!(%e, op, "lazy resolve failed");
                    Errno::EIO
                })
        }
        SlotLookup::Missing => Err(Errno::ENOENT),
    }
}

/// Push `entries` into a `ReplyDirectory`, including `.` and `..`.
fn emit_readdir(ino: Ino, entries: &[(String, Node)], offset: u64, mut reply: ReplyDirectory) {
    // Cookie semantics: each entry is given a `next_offset` that the
    // kernel echoes back so we can resume. Skip entries whose cookie is
    // <= the offset the kernel already saw. `reply.add` returns true
    // when the buffer is full — stop adding but still call `reply.ok()`
    // so the kernel knows to come back with a higher offset instead of
    // looping on the same one.
    let mut all: Vec<(Ino, FileType, &str)> = Vec::with_capacity(entries.len() + 2);
    all.push((ino, FileType::Directory, "."));
    all.push((ino, FileType::Directory, ".."));
    for (name, node) in entries {
        all.push((node.ino, file_type_of(node), name.as_str()));
    }
    for (i, (child_ino, kind, name)) in all.iter().enumerate() {
        let next_offset = (i + 1) as u64;
        if next_offset <= offset {
            continue;
        }
        if reply.add(INodeNo(*child_ino), next_offset, *kind, name) {
            break;
        }
    }
    reply.ok();
}

/// Symlinks are reported as regular files: `readlink` isn't implemented
/// here, and the kernel would follow a link we can't resolve.
fn file_type_of(node: &Node) -> FileType {
    match node.kind {
        NodeKind::Dir => FileType::Directory,
        NodeKind::File { .. } | NodeKind::Symlink { .. } => FileType::RegularFile,
    }
}

fn attr_of(node: &Node) -> FileAttr {
    let kind = file_type_of(node);
    let (perm, nlink) = match &node.kind {
        NodeKind::Dir => (0o555, 2),
        NodeKind::File { executable } => (if *executable { 0o555 } else { 0o444 }, 1),
        NodeKind::Symlink { .. } => (0o444, 1),
    };
    let mtime = UNIX_EPOCH + Duration::from_secs(u64::from(node.mtime_secs));
    FileAttr {
        ino: INodeNo(node.ino),
        size: node.size,
        blocks: if matches!(kind, FileType::RegularFile) {
            node.size.div_ceil(512)
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
