// TODO(ai-review): review for correctness/style
//! [`NFSFileSystem`] impl on top of [`MountTree`], for platforms where a
//! localhost NFSv3 server is a better deal than FUSE — on macOS it needs
//! neither macFUSE nor an FSKit extension, just the built-in NFS client.
//!
//! Unlike the FUSE adapter this one is natively async: nfsserve's trait
//! methods are `async fn`, so lazy manifest opens are awaited in place
//! instead of being spawned onto a runtime handle.

use std::sync::Arc;

use async_trait::async_trait;
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use parking_lot::RwLock;
use steam_depot_vfs::chunk_store::ChunkStore;

use crate::inode::{self, SYNTHETIC};
use crate::tree::{MountTree, SnapshotEntry, SnapshotId};
use crate::view::{self, Node, NodeKind, SlotLookup};

pub(crate) struct NfsFs<C: ChunkStore + 'static> {
    tree: Arc<RwLock<MountTree<C>>>,
}

impl<C: ChunkStore + 'static> NfsFs<C> {
    pub(crate) fn new(tree: Arc<RwLock<MountTree<C>>>) -> Self {
        Self { tree }
    }

    /// Resolve the snapshot that owns `sid`, driving a lazy opener if
    /// this is the first access.
    async fn snapshot_of(&self, sid: inode::SnapshotId) -> Result<Arc<SnapshotEntry<C>>, nfsstat3> {
        match view::slot_lookup(&self.tree, sid) {
            SlotLookup::Ready(entry) => Ok(entry),
            SlotLookup::Pending(lazy) => {
                view::resolve(Arc::clone(&self.tree), lazy, SnapshotId(sid))
                    .await
                    .map_err(|e| {
                        tracing::warn!(%e, "lazy resolve failed");
                        nfsstat3::NFS3ERR_IO
                    })
            }
            SlotLookup::Missing => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    async fn node(&self, id: fileid3) -> Result<Node, nfsstat3> {
        let (sid, idx) = inode::unpack(id);
        if sid == SYNTHETIC {
            return view::synthetic_node(&*self.tree.read(), id).ok_or(nfsstat3::NFS3ERR_NOENT);
        }
        // The snapshot root is "a directory" until someone asks for a
        // child, so answer it without forcing a manifest fetch.
        if idx == 0 {
            let tree = self.tree.read();
            if tree.slot(sid).is_none() {
                return Err(nfsstat3::NFS3ERR_NOENT);
            }
            let mtime = view::slot_mtime(&tree, sid);
            drop(tree);
            return Ok(Node {
                ino: id,
                kind: NodeKind::Dir,
                size: 0,
                mtime_secs: mtime,
            });
        }
        let entry = self.snapshot_of(sid).await?;
        view::snapshot_node(&entry, id).ok_or(nfsstat3::NFS3ERR_NOENT)
    }
}

#[async_trait]
impl<C: ChunkStore + 'static> NFSFileSystem for NfsFs<C> {
    fn capabilities(&self) -> VFSCapabilities {
        // Not a lie we enjoy: nfsserve masks ACCESS3_EXECUTE out of every
        // ACCESS reply for a `ReadOnly` filesystem, so `access(X_OK)`
        // fails and nothing on the mount can be executed. Every write
        // method below answers ROFS and the client mounts `rdonly`, so
        // claiming ReadWrite costs nothing and buys back exec.
        VFSCapabilities::ReadWrite
    }

    fn root_dir(&self) -> fileid3 {
        inode::ROOT
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let name = std::str::from_utf8(&filename.0).map_err(|_| nfsstat3::NFS3ERR_NOENT)?;
        let (sid, _) = inode::unpack(dirid);
        if sid == SYNTHETIC {
            return view::synthetic_child(&*self.tree.read(), dirid, name)
                .map(|n| n.ino)
                .ok_or(nfsstat3::NFS3ERR_NOENT);
        }
        let entry = self.snapshot_of(sid).await?;
        view::snapshot_child(&entry, dirid, name)
            .map(|n| n.ino)
            .ok_or(nfsstat3::NFS3ERR_NOENT)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        Ok(attr_of(&self.node(id).await?))
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let (sid, idx) = inode::unpack(id);
        if sid == SYNTHETIC {
            return Err(nfsstat3::NFS3ERR_ISDIR);
        }
        let Some(file_idx) = (idx as usize).checked_sub(1) else {
            return Err(nfsstat3::NFS3ERR_ISDIR);
        };
        let entry = self.snapshot_of(sid).await?;
        let file = entry
            .snapshot
            .manifest()
            .files
            .get(file_idx)
            .ok_or(nfsstat3::NFS3ERR_NOENT)?;
        let (path, size) = (file.path.clone(), file.size);
        // NFS clients read ahead past EOF; clamping here keeps
        // `read_into`'s "read past end of file" guard from turning that
        // into an I/O error.
        if offset >= size {
            return Ok((Vec::new(), true));
        }
        let len = u64::from(count).min(size - offset);
        let mut buf = Vec::with_capacity(len as usize);
        entry
            .snapshot
            .read_into(&path, offset, len, &mut buf)
            .await
            .map_err(|e| {
                tracing::warn!(path = %path, offset, len, %e, "read failed");
                nfsstat3::NFS3ERR_IO
            })?;
        let eof = offset + buf.len() as u64 >= size;
        Ok((buf, eof))
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let (sid, _) = inode::unpack(dirid);
        let entries = if sid == SYNTHETIC {
            view::synthetic_dir(&*self.tree.read(), dirid).ok_or(nfsstat3::NFS3ERR_NOENT)?
        } else {
            let entry = self.snapshot_of(sid).await?;
            view::snapshot_dir(&entry, dirid).ok_or(nfsstat3::NFS3ERR_NOENT)?
        };
        // The cookie nfsserve hands back is the fileid of the last entry
        // it emitted, so resume right after it. An unknown cookie
        // restarts the listing rather than failing it — see the
        // BAD_COOKIE discussion in nfsserve's readdir handler.
        let skip = if start_after == 0 {
            0
        } else {
            entries
                .iter()
                .position(|(_, node)| node.ino == start_after)
                .map_or(0, |i| i + 1)
        };
        let page: Vec<DirEntry> = entries[skip..]
            .iter()
            .take(max_entries)
            .map(|(name, node)| DirEntry {
                fileid: node.ino,
                name: name.as_bytes().into(),
                attr: attr_of(node),
            })
            .collect();
        let end = skip + page.len() >= entries.len();
        Ok(ReadDirResult { entries: page, end })
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        match self.node(id).await?.kind {
            NodeKind::Symlink { target } => Ok(target.as_bytes().into()),
            _ => Err(nfsstat3::NFS3ERR_INVAL),
        }
    }

    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create_exclusive(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn mkdir(
        &self,
        _dirid: fileid3,
        _dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }
}

fn attr_of(node: &Node) -> fattr3 {
    let (ftype, mode, nlink) = match &node.kind {
        NodeKind::Dir => (ftype3::NF3DIR, 0o555, 2),
        NodeKind::File { executable } => {
            (ftype3::NF3REG, if *executable { 0o555 } else { 0o444 }, 1)
        }
        NodeKind::Symlink { .. } => (ftype3::NF3LNK, 0o777, 1),
    };
    let time = nfstime3 {
        seconds: node.mtime_secs,
        nseconds: 0,
    };
    fattr3 {
        ftype,
        mode,
        nlink,
        uid: 0,
        gid: 0,
        size: node.size,
        used: node.size.div_ceil(512) * 512,
        rdev: specdata3 {
            specdata1: 0,
            specdata2: 0,
        },
        fsid: 0,
        fileid: node.ino,
        atime: time,
        mtime: time,
        ctime: time,
    }
}
