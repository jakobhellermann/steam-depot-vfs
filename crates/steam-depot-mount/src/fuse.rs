// TODO(ai-review): review for correctness/style
//! `AsyncFilesystem` impl on top of [`MountTree`]. Translates between
//! fuser's inode/handle world and the tree's snapshot+index world.

use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use fuser::experimental::{
    AsyncFilesystem, DirEntListBuilder, GetAttrResponse, LookupResponse, RequestContext,
};
use fuser::{
    Errno, FileAttr, FileHandle, FileType, Generation, INodeNo, LockOwner, OpenFlags, experimental,
};
use parking_lot::RwLock;
use steam_depot_vfs::FileKind;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::Entry;

use crate::inode::{self, SYNTHETIC};
use crate::tree::{MountTree, SnapshotEntry};

/// FUSE attribute TTL. The tree is mutable (snapshots come and go), but
/// individual snapshots are immutable for their lifetime. One hour
/// matches user expectations for "static-ish" content; on snapshot
/// remove we'd need [`fuser::Notifier::inval_entry`] to invalidate
/// eagerly. v1 doesn't bother — the next `lookup` will see `ENOENT`.
const TTL: Duration = Duration::from_secs(60 * 60);

pub(crate) struct FuseFs<C: ChunkStore + 'static> {
    tree: Arc<RwLock<MountTree<C>>>,
}

impl<C: ChunkStore + 'static> FuseFs<C> {
    pub fn new(tree: Arc<RwLock<MountTree<C>>>) -> Self {
        Self { tree }
    }
}

#[async_trait::async_trait]
impl<C: ChunkStore + 'static> AsyncFilesystem for FuseFs<C> {
    async fn lookup(
        &self,
        _ctx: &RequestContext,
        parent: INodeNo,
        name: &OsStr,
    ) -> experimental::Result<LookupResponse> {
        let _span = tracing::info_span!("fuse.lookup").entered();
        let name = name.to_str().ok_or(Errno::ENOENT)?;
        let tree = self.tree.read();
        let (_ino, attr) = resolve_child(&tree, parent, name).ok_or(Errno::ENOENT)?;
        Ok(LookupResponse::new(TTL, attr, Generation(0)))
    }

    async fn getattr(
        &self,
        _ctx: &RequestContext,
        ino: INodeNo,
        _fh: Option<FileHandle>,
    ) -> experimental::Result<GetAttrResponse> {
        let _span = tracing::info_span!("fuse.getattr").entered();
        let tree = self.tree.read();
        let attr = attr_for(&tree, ino).ok_or(Errno::ENOENT)?;
        Ok(GetAttrResponse::new(TTL, attr))
    }

    async fn readdir(
        &self,
        _ctx: &RequestContext,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut builder: DirEntListBuilder<'_>,
    ) -> experimental::Result<()> {
        let _span = tracing::info_span!("fuse.readdir").entered();
        // Snapshot the entries to release the lock before calling into
        // `builder.add` (which is sync but we don't want to hold the
        // RwLock across the whole iteration regardless).
        let entries = {
            let tree = self.tree.read();
            collect_dir(&tree, ino).ok_or(Errno::ENOENT)?
        };

        let mut cookie = 0u64;
        let mut add = |child_ino, kind, name: &str| -> bool {
            cookie += 1;
            cookie <= offset || builder.add(child_ino, cookie, kind, name)
        };
        if add(ino, FileType::Directory, ".") {
            return Ok(());
        }
        if add(ino, FileType::Directory, "..") {
            return Ok(());
        }
        for (child_ino, kind, name) in entries {
            if add(child_ino, kind, &name) {
                break;
            }
        }
        Ok(())
    }

    #[tracing::instrument(name = "fuse.read", skip_all)]
    async fn read(
        &self,
        _ctx: &RequestContext,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        out_data: &mut Vec<u8>,
    ) -> experimental::Result<()> {
        let (sid, idx) = inode::unpack(ino);
        if sid == SYNTHETIC {
            return Err(Errno::EISDIR);
        }
        let file_idx = (idx as usize).checked_sub(1).ok_or(Errno::EISDIR)?;
        // Clone the Arc out from under the read lock so the CDN fetch
        // can run without blocking concurrent `add`/`remove`.
        let entry = {
            let tree = self.tree.read();
            tree.snapshot(sid).ok_or(Errno::ENOENT)?.clone()
        };
        let path = {
            let file = entry
                .snapshot
                .manifest()
                .files
                .get(file_idx)
                .ok_or(Errno::ENOENT)?;
            if matches!(file.kind, FileKind::Symlink) {
                tracing::warn!(
                    path = %file.path,
                    target = ?file.linktarget,
                    "reading symlink as regular file; link target not resolved",
                );
            }
            file.path.clone()
        };
        let bytes = entry
            .snapshot
            .read(&path, offset, size as u64)
            .await
            .map_err(|e| {
                tracing::warn!(path = %path, offset, size, %e, "read failed");
                Errno::EIO
            })?;
        out_data.extend_from_slice(&bytes);
        Ok(())
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
        // Synthetic dirs report size 0; the children count would be
        // more accurate but `du`/`ls` work fine with 0.
        return Some(dir_attr(ino, 0));
    }
    let entry = tree.snapshot(sid)?;
    if idx == 0 {
        // Snapshot root: a directory.
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
    // nlink for directories conventionally counts `.`, `..`, and each
    // subdirectory. We always report 2 — accurate for leaf dirs, lazy
    // for non-leaves but standard practice (FUSE filesystems frequently
    // do this; `find` is the main thing that cares).
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
