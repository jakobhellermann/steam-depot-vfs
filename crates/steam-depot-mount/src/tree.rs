// TODO(ai-review): review for correctness/style
//! Multi-snapshot tree that backs the FUSE filesystem.
//!
//! See [`crate::inode`] for the inode encoding. The tree owns:
//!
//! - A vector of [`SyntheticDir`]s for the root, app-id dirs, and
//!   depot-id dirs. Their inodes are `(SYNTHETIC, slot_index)`.
//! - A slot map of [`SnapshotEntry`]s for mounted manifests. Removing a
//!   snapshot leaves its slot tombstoned so its `SnapshotId` is never
//!   reused — open file handles pointing at it return `ENOENT` cleanly
//!   instead of accidentally hitting a fresh snapshot.

use std::collections::HashMap;
use std::sync::Arc;

use fuser::INodeNo;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::DepotSnapshot;

use crate::inode::{self, SYNTHETIC};

/// Stable handle to a mounted snapshot. Becomes invalid once
/// [`MountTree::remove`] is called for it; the id is never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub(crate) inode::SnapshotId);

pub(crate) struct MountTree<C: ChunkStore> {
    /// Index 1 is the root. Slot 0 is unused (kept so indices line up
    /// with [`fuser::INodeNo::ROOT`]).
    synthetic: Vec<SyntheticDir>,
    /// Map from "/app_id" or "/app_id/depot_id" path to the synthetic
    /// dir representing it. Used by [`MountTree::ensure_synthetic`].
    synth_by_path: HashMap<String, INodeNo>,
    /// Indexed by `SnapshotId.0`. Slot 0 is unused (snapshot ids start
    /// at 1 so `(synthetic, *)` and `(snapshot 1, *)` are distinct).
    ///
    /// Entries are `Arc`'d so a [`FuseFs::read`] can clone the handle
    /// out from under the tree lock and run the async fetch without
    /// blocking concurrent `add`/`remove`.
    snapshots: Vec<Option<Arc<SnapshotEntry<C>>>>,
}

/// A synthetic directory in the mount prefix.
pub(crate) struct SyntheticDir {
    /// Parent inode. None only for the root.
    pub parent: Option<INodeNo>,
    /// Display name for this entry, e.g. "1030300".
    pub name: String,
    /// Children indexed by name. Values are inodes.
    pub children: HashMap<String, INodeNo>,
}

pub(crate) struct SnapshotEntry<C: ChunkStore> {
    pub app_id: u32,
    pub depot_id: u32,
    pub manifest_gid: u64,
    pub snapshot: DepotSnapshot<C>,
}

impl<C: ChunkStore> MountTree<C> {
    pub fn new() -> Self {
        let mut synthetic = Vec::with_capacity(4);
        // Slot 0: placeholder so slot indices map to INodeNo values.
        synthetic.push(SyntheticDir {
            parent: None,
            name: String::new(),
            children: HashMap::new(),
        });
        // Slot 1: the root.
        synthetic.push(SyntheticDir {
            parent: None,
            name: String::new(),
            children: HashMap::new(),
        });
        Self {
            synthetic,
            synth_by_path: HashMap::new(),
            // Slot 0: placeholder so `SnapshotId(0)` is invalid and
            // `(SYNTHETIC, *)` doesn't alias snapshot 0.
            snapshots: vec![None],
        }
    }

    /// Insert `snapshot` under `/<app_id>/<depot_id>/<manifest_gid>`,
    /// creating the synthetic dirs as needed.
    pub fn add(
        &mut self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        snapshot: DepotSnapshot<C>,
    ) -> Result<SnapshotId, AddError> {
        let app_dir = self.ensure_synthetic(inode::ROOT, app_id.to_string());
        let depot_dir = self.ensure_synthetic(app_dir, depot_id.to_string());
        let gid_name = manifest_gid.to_string();
        if self.children_of(depot_dir).contains_key(&gid_name) {
            return Err(AddError::AlreadyMounted {
                app_id,
                depot_id,
                manifest_gid,
            });
        }

        let next_id: u32 = self
            .snapshots
            .len()
            .try_into()
            .expect("snapshot count fits u32");
        if next_id > inode::SnapshotId::MAX as u32 {
            return Err(AddError::TooManySnapshots);
        }
        let id = inode::SnapshotId::try_from(next_id).unwrap();
        self.snapshots.push(Some(Arc::new(SnapshotEntry {
            app_id,
            depot_id,
            manifest_gid,
            snapshot,
        })));
        let snapshot_root = inode::pack(id, 0);
        self.children_of_mut(depot_dir)
            .insert(gid_name, snapshot_root);
        Ok(SnapshotId(id))
    }

    /// Drop the snapshot identified by `id`. The slot is left tombstoned
    /// (never reused). Synthetic dirs are *not* pruned — they stick
    /// around for the lifetime of the mount.
    pub fn remove(&mut self, id: SnapshotId) -> Option<Arc<SnapshotEntry<C>>> {
        let slot = self.snapshots.get_mut(id.0 as usize)?;
        let entry = slot.take()?;
        // Detach from the parent depot dir so it stops appearing in
        // readdir. We don't shrink the depot dir or its ancestors.
        let depot_path = format!("/{}/{}", entry.app_id, entry.depot_id);
        if let Some(&depot_ino) = self.synth_by_path.get(&depot_path) {
            self.children_of_mut(depot_ino)
                .remove(&entry.manifest_gid.to_string());
        }
        Some(entry)
    }

    pub fn synthetic(&self, ino: INodeNo) -> Option<&SyntheticDir> {
        let (sid, idx) = inode::unpack(ino);
        if sid != SYNTHETIC {
            return None;
        }
        self.synthetic.get(idx as usize)
    }

    pub fn snapshot(&self, id: inode::SnapshotId) -> Option<&Arc<SnapshotEntry<C>>> {
        self.snapshots.get(id as usize)?.as_ref()
    }

    fn ensure_synthetic(&mut self, parent: INodeNo, name: String) -> INodeNo {
        if let Some(&existing) = self.children_of(parent).get(&name) {
            return existing;
        }
        let slot = self.synthetic.len();
        let ino = inode::pack(SYNTHETIC, slot as u64);
        let parent_path = self.path_of_synthetic(parent);
        let path = if parent_path.is_empty() {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        };
        self.synthetic.push(SyntheticDir {
            parent: Some(parent),
            name: name.clone(),
            children: HashMap::new(),
        });
        self.synth_by_path.insert(path, ino);
        self.children_of_mut(parent).insert(name, ino);
        ino
    }

    fn children_of(&self, ino: INodeNo) -> &HashMap<String, INodeNo> {
        let (sid, idx) = inode::unpack(ino);
        debug_assert_eq!(sid, SYNTHETIC, "children_of called with non-synthetic ino");
        &self.synthetic[idx as usize].children
    }

    fn children_of_mut(&mut self, ino: INodeNo) -> &mut HashMap<String, INodeNo> {
        let (sid, idx) = inode::unpack(ino);
        debug_assert_eq!(
            sid, SYNTHETIC,
            "children_of_mut called with non-synthetic ino"
        );
        &mut self.synthetic[idx as usize].children
    }

    fn path_of_synthetic(&self, ino: INodeNo) -> String {
        if ino == inode::ROOT {
            return String::new();
        }
        let dir = &self.synthetic[inode::unpack(ino).1 as usize];
        let parent = dir.parent.expect("non-root synthetic has parent");
        let parent_path = self.path_of_synthetic(parent);
        if parent_path.is_empty() {
            format!("/{}", dir.name)
        } else {
            format!("{parent_path}/{}", dir.name)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AddError {
    #[error("manifest {manifest_gid} for app {app_id} depot {depot_id} is already mounted")]
    AlreadyMounted {
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
    },
    #[error("too many concurrent snapshots; the 16-bit id space is exhausted")]
    TooManySnapshots,
}
