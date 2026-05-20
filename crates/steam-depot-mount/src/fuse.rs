// TODO(ai-review): review for correctness/style
//! `Filesystem` impl on top of [`MountTree`]. Bridges fuser's blocking
//! callbacks to async operations on a Tokio runtime supplied by the
//! caller. We hold a `Handle`, not a `Runtime`, so the FUSE adapter
//! shares the binary's main runtime instead of building its own.

use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use fuser::{
    Errno, FileAttr, FileType, Filesystem, Generation, INodeNo, OpenFlags, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, Request,
};
use parking_lot::RwLock;
use steam_depot_vfs::FileKind;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::Entry;
use tokio::runtime::Handle;
use tracing::Instrument as _;

use crate::inode::{self, SYNTHETIC};
use crate::tree::{MountTree, SnapshotEntry};

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
        let _span = tracing::info_span!("fuse.lookup").entered();
        let tree = self.tree.read();
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        match resolve_child(&tree, parent, name) {
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
        let _span = tracing::info_span!("fuse.getattr").entered();
        let tree = self.tree.read();
        match attr_for(&tree, ino) {
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
        mut reply: ReplyDirectory,
    ) {
        let _span = tracing::info_span!("fuse.readdir").entered();
        let entries = {
            let tree = self.tree.read();
            match collect_dir(&tree, ino) {
                Some(e) => e,
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        // Cookie semantics match the experimental API: each `reply.add`
        // is given `offset+1`; the kernel echoes it back on the next
        // call so we can resume.
        let mut cookie = 0u64;
        if cookie >= offset || !reply.add(ino, cookie + 1, FileType::Directory, ".") {
            cookie += 1;
        } else {
            reply.ok();
            return;
        }
        if cookie >= offset || !reply.add(ino, cookie + 1, FileType::Directory, "..") {
            cookie += 1;
        } else {
            reply.ok();
            return;
        }
        for (child_ino, kind, name) in entries {
            cookie += 1;
            if cookie <= offset {
                continue;
            }
            if reply.add(child_ino, cookie, kind, &name) {
                break;
            }
        }
        reply.ok();
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
        // Clone the snapshot Arc out of the tree before going async so
        // the CDN fetch doesn't block concurrent `add`/`remove`.
        let entry = {
            let tree = self.tree.read();
            match tree.snapshot(sid) {
                Some(e) => e.clone(),
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
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
        // Drop into our shared runtime to run the async fetch + cache
        // read. fuser's main loop is blocking, but each callback is
        // allowed to return without sending a reply as long as
        // *something* eventually does — so we hand the reply to the
        // task and let it complete out-of-band.
        let span = tracing::info_span!("fuse.read");
        self.rt.spawn(
            async move {
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
            }
            .instrument(span),
        );
    }

    fn access(&self, _req: &Request, _ino: INodeNo, _mask: fuser::AccessFlags, reply: ReplyEmpty) {
        // Read-only mount, all paths are world-readable; rather than
        // letting fuser's default fall through to ENOSYS (which clutters
        // logs), say "yes" to every check. The kernel does its own
        // perm check against the `perm` we report in getattr anyway.
        reply.ok();
    }
}

/// Resolve `(parent, name)` to `(child_ino, attr)`, walking synthetic
/// dirs and snapshot interiors transparently.
fn resolve_child<C: ChunkStore>(
    tree: &MountTree<C>,
    parent: INodeNo,
    name: &str,
) -> Option<(INodeNo, FileAttr)> {
    let (sid, idx) = inode::unpack(parent);
    if sid == SYNTHETIC {
        let dir = tree.synthetic(parent)?;
        let &child = dir.children.get(name)?;
        let attr = attr_for(tree, child)?;
        return Some((child, attr));
    }
    let entry = tree.snapshot(sid)?;
    let parent_path = snapshot_path_of(entry, idx)?;
    let child_path = if parent_path.is_empty() {
        name.to_string()
    } else {
        format!("{parent_path}/{name}")
    };
    let child_idx = entry.snapshot.index_of(&child_path)?;
    let child_ino = inode::pack(sid, (child_idx + 1) as u64);
    let f = entry.snapshot.manifest().files.get(child_idx)?;
    Some((child_ino, file_kind_attr(f.kind, child_ino, f.size)))
}

fn attr_for<C: ChunkStore>(tree: &MountTree<C>, ino: INodeNo) -> Option<FileAttr> {
    let (sid, idx) = inode::unpack(ino);
    if sid == SYNTHETIC {
        return Some(dir_attr(ino, 0));
    }
    let entry = tree.snapshot(sid)?;
    if idx == 0 {
        return Some(dir_attr(ino, 0));
    }
    let f = entry.snapshot.manifest().files.get(idx as usize - 1)?;
    Some(file_kind_attr(f.kind, ino, f.size))
}

fn file_kind_attr(kind: FileKind, ino: INodeNo, size: u64) -> FileAttr {
    match kind {
        FileKind::Directory => dir_attr(ino, size),
        FileKind::File | FileKind::Symlink => file_attr(ino, size),
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

/// Gather direct children of `ino` as `(inode, type, name)` for readdir.
fn collect_dir<C: ChunkStore>(
    tree: &MountTree<C>,
    ino: INodeNo,
) -> Option<Vec<(INodeNo, FileType, String)>> {
    let (sid, idx) = inode::unpack(ino);
    if sid == SYNTHETIC {
        let dir = tree.synthetic(ino)?;
        // Synthetic children are always directories: app-id dirs,
        // depot-id dirs, or snapshot roots (which themselves are dirs).
        let mut out: Vec<_> = dir
            .children
            .iter()
            .map(|(name, &child_ino)| (child_ino, FileType::Directory, name.clone()))
            .collect();
        out.sort_by(|a, b| a.2.cmp(&b.2));
        return Some(out);
    }
    let entry = tree.snapshot(sid)?;
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

fn dir_attr(ino: INodeNo, size: u64) -> FileAttr {
    base_attr(ino, size, FileType::Directory, 0o555, 2)
}

fn file_attr(ino: INodeNo, size: u64) -> FileAttr {
    base_attr(ino, size, FileType::RegularFile, 0o444, 1)
}

fn base_attr(ino: INodeNo, size: u64, kind: FileType, perm: u16, nlink: u32) -> FileAttr {
    FileAttr {
        ino,
        size,
        blocks: if matches!(kind, FileType::RegularFile) {
            size.div_ceil(512)
        } else {
            0
        },
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
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
