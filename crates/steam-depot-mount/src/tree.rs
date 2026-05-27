// TODO(ai-review): review for correctness/style
//! Multi-snapshot tree that backs the FUSE filesystem.
//!
//! See [`crate::inode`] for the inode encoding. The tree owns:
//!
//! - A vector of [`SyntheticDir`]s for the root, app-id dirs, and
//!   depot-id dirs. Their inodes are `(SYNTHETIC, slot_index)`.
//! - A slot map indexed by snapshot id. Each slot is either
//!   [`SlotState::Pending`] (registered lazily via [`MountTree::add_lazy`]
//!   but not yet resolved) or [`SlotState::Ready`] (snapshot is loaded
//!   and usable). Removing a slot leaves it as `None` — the id is never
//!   reused, so stale FUSE handles cleanly return `ENOENT` instead of
//!   accidentally hitting a fresh snapshot.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use fuser::INodeNo;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::DepotManifestStore;
use tokio::sync::OnceCell;

use crate::inode::{self, SYNTHETIC};

/// Stable handle to a mounted snapshot. Becomes invalid once
/// [`MountTree::remove`] is called for it; the id is never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub(crate) inode::SnapshotId);

/// Future returned by a lazy opener. Boxed so the closure type stays
/// erased — the mount stores `Arc<dyn Fn() -> BoxFuture<…>>` entries.
pub type OpenerFuture<C> =
    Pin<Box<dyn Future<Output = Result<DepotManifestStore<C>, std::io::Error>> + Send>>;

pub(crate) type Opener<C> = Arc<dyn Fn() -> OpenerFuture<C> + Send + Sync>;

/// Lazy slot contents: the opener that loads the manifest on demand and
/// a `OnceCell` so concurrent first-lookups coalesce on a single call.
pub(crate) struct LazyEntry<C: ChunkStore> {
    pub opener: Opener<C>,
    pub cell: Arc<OnceCell<Result<Arc<SnapshotEntry<C>>, String>>>,
}

pub(crate) enum SlotState<C: ChunkStore> {
    Pending(Arc<LazyEntry<C>>),
    Ready(Arc<SnapshotEntry<C>>),
}

pub(crate) struct Slot<C: ChunkStore> {
    pub app_id: u32,
    pub depot_id: u32,
    pub manifest_gid: u64,
    pub state: SlotState<C>,
    /// Manifest `creation_time` (Unix seconds). `None` until the snapshot
    /// is resolved — lets `getattr` on the `<gid>` root answer without
    /// forcing a resolve, while still reporting the real release date
    /// once available.
    pub creation_time: Option<u32>,
}

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
    /// `Arc<Slot>` so a [`FuseFs`] callback can clone the slot handle
    /// out from under the tree lock and run the async fetch / resolve
    /// without blocking concurrent `add` / `remove`.
    slots: Vec<Option<Arc<Slot<C>>>>,
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

/// Loaded contents of a mounted manifest. The owning [`Slot`] carries
/// the `(app_id, depot_id, manifest_gid)` triple — duplicating those
/// here would be redundant.
pub(crate) struct SnapshotEntry<C: ChunkStore> {
    pub snapshot: DepotManifestStore<C>,
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
            slots: vec![None],
        }
    }

    /// Insert an already-loaded `snapshot` under
    /// `/<app_id>/<depot_id>/<manifest_gid>`, creating the synthetic
    /// dirs as needed. Equivalent to `add_lazy` with an opener that
    /// immediately returns this snapshot, but skips the indirection.
    pub fn add(
        &mut self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        snapshot: DepotManifestStore<C>,
    ) -> Result<SnapshotId, AddError> {
        let id = self.reserve_slot(
            app_id,
            depot_id,
            manifest_gid,
            |entry| SlotState::Ready(Arc::new(entry(snapshot))),
            None,
        )?;
        Ok(id)
    }

    /// Register a manifest gid under `/<app_id>/<depot_id>/<manifest_gid>`
    /// without loading it. The first FUSE operation that needs the
    /// manifest contents calls `opener` (via a `OnceCell` so concurrent
    /// callers coalesce) and the result is cached for the lifetime of
    /// the slot.
    pub fn add_lazy(
        &mut self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        opener: Opener<C>,
        creation_time: Option<u32>,
    ) -> Result<SnapshotId, AddError> {
        let lazy = Arc::new(LazyEntry {
            opener,
            cell: Arc::new(OnceCell::new()),
        });
        self.reserve_slot(
            app_id,
            depot_id,
            manifest_gid,
            |_| SlotState::Pending(lazy),
            creation_time,
        )
    }

    /// Common path for [`add`] and [`add_lazy`]: ensures the synthetic
    /// dirs, picks a fresh snapshot id, and inserts the slot. `build`
    /// receives a constructor for `SnapshotEntry` (used only by the
    /// eager `add`) — keeping it in a closure means lazy registrations
    /// don't have to materialise a snapshot they don't have yet.
    /// `creation_time_hint` lets lazy registrations preseed the slot's
    /// mtime from e.g. a cached manifest, so `<gid>` getattr returns
    /// the real release date even before the manifest is opened.
    fn reserve_slot(
        &mut self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        build: impl FnOnce(&dyn Fn(DepotManifestStore<C>) -> SnapshotEntry<C>) -> SlotState<C>,
        creation_time_hint: Option<u32>,
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
            .slots
            .len()
            .try_into()
            .expect("snapshot count fits u32");
        if next_id > inode::SnapshotId::MAX as u32 {
            return Err(AddError::TooManySnapshots);
        }
        let id = inode::SnapshotId::try_from(next_id).unwrap();
        let make_entry = |snapshot: DepotManifestStore<C>| SnapshotEntry { snapshot };
        let state = build(&make_entry);
        let creation_time = match &state {
            SlotState::Ready(entry) => Some(entry.snapshot.manifest().creation_time),
            SlotState::Pending(_) => creation_time_hint,
        };
        self.slots.push(Some(Arc::new(Slot {
            app_id,
            depot_id,
            manifest_gid,
            state,
            creation_time,
        })));
        let snapshot_root = inode::pack(id, 0);
        self.children_of_mut(depot_dir)
            .insert(gid_name, snapshot_root);
        Ok(SnapshotId(id))
    }

    /// Drop the slot identified by `id`. The slot is left as `None`
    /// (never reused). Synthetic dirs are *not* pruned — they stick
    /// around for the lifetime of the mount.
    pub fn remove(&mut self, id: SnapshotId) -> Option<Arc<Slot<C>>> {
        let slot = self.slots.get_mut(id.0 as usize)?;
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

    /// Lookup the slot for `id`. The returned `Arc<Slot>` can be held
    /// across awaits — the tree lock is dropped on return.
    pub fn slot(&self, id: inode::SnapshotId) -> Option<&Arc<Slot<C>>> {
        self.slots.get(id as usize)?.as_ref()
    }

    /// After a lazy opener finishes, replace the slot's `Pending` state
    /// with `Ready`. Idempotent: if some other caller already wrote a
    /// `Ready` (or removed the slot), this is a no-op.
    pub fn promote(&mut self, id: SnapshotId, entry: Arc<SnapshotEntry<C>>) {
        let Some(slot) = self.slots.get_mut(id.0 as usize).and_then(|s| s.as_mut()) else {
            return;
        };
        if matches!(slot.state, SlotState::Ready(_)) {
            return;
        }
        let creation_time = entry.snapshot.manifest().creation_time;
        // Slot is Arc'd — clone the metadata to a fresh Slot rather
        // than mutating through the Arc (which would require unique
        // ownership we don't have).
        *slot = Arc::new(Slot {
            app_id: slot.app_id,
            depot_id: slot.depot_id,
            manifest_gid: slot.manifest_gid,
            state: SlotState::Ready(entry),
            creation_time: Some(creation_time),
        });
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
