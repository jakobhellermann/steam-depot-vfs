// TODO(ai-review): review for correctness/style
//! Multi-snapshot tree that backs the FUSE filesystem.
//!
//! See [`crate::inode`] for the inode encoding. The tree owns:
//!
//! - A vector of [`SyntheticDir`]s for the root, app dirs, and depot
//!   dirs. Their inodes are `(SYNTHETIC, slot_index)`. App and depot dirs
//!   are keyed by id; their names are display only.
//! - A slot map indexed by snapshot id. Each slot is either
//!   [`SlotState::Pending`] (registered lazily via [`MountTree::add_lazy`]
//!   but not yet resolved) or [`SlotState::Ready`] (snapshot is loaded
//!   and usable). Removing a slot leaves it as `None` — the id is never
//!   reused, so stale FUSE handles cleanly return `ENOENT` instead of
//!   accidentally hitting a fresh snapshot.
//!
//! A snapshot registered with a [`Namer`] stays out of its depot dir until
//! the dir is first listed or looked up in; then all of the dir's unnamed
//! snapshots are named at once, and the names hold for the mount's lifetime.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::DepotManifestStore;
use tokio::sync::OnceCell;

use crate::inode::{self, Ino, SYNTHETIC};

/// Stable handle to a mounted snapshot. Becomes invalid once
/// [`MountTree::remove`] is called for it; the id is never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub(crate) inode::SnapshotId);

/// Future returned by a lazy opener. Boxed so the closure type stays
/// erased — the mount stores `Arc<dyn Fn() -> BoxFuture<…>>` entries.
pub type OpenerFuture<C> =
    Pin<Box<dyn Future<Output = Result<DepotManifestStore<C>, std::io::Error>> + Send>>;

pub(crate) type Opener<C> = Arc<dyn Fn() -> OpenerFuture<C> + Send + Sync>;

/// Future returned by a [`Namer`]: the snapshot dir's display name, or
/// `None` to name it by its manifest gid.
pub type NamerFuture = Pin<Box<dyn Future<Output = Result<Option<String>, std::io::Error>> + Send>>;

pub(crate) type Namer = Arc<dyn Fn() -> NamerFuture + Send + Sync>;

pub(crate) fn opener<C, F, Fut>(f: F) -> Opener<C>
where
    C: ChunkStore,
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<DepotManifestStore<C>, std::io::Error>> + Send + 'static,
{
    Arc::new(move || -> OpenerFuture<C> { Box::pin(f()) })
}

pub(crate) fn namer<F, Fut>(f: F) -> Namer
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Option<String>, std::io::Error>> + Send + 'static,
{
    Arc::new(move || -> NamerFuture { Box::pin(f()) })
}

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
    /// The depot dir this snapshot lives in.
    pub parent: Ino,
    pub state: SlotState<C>,
    /// Manifest `creation_time` (Unix seconds). `None` until the snapshot
    /// is resolved — lets `getattr` on the `<gid>` root answer without
    /// forcing a resolve, while still reporting the real release date
    /// once available.
    pub creation_time: Option<u32>,
}

pub(crate) struct MountTree<C: ChunkStore> {
    /// Index 1 is the root. Slot 0 is unused (kept so indices line up
    /// with [`inode::ROOT`]).
    synthetic: Vec<SyntheticDir>,
    app_dirs: HashMap<u32, Ino>,
    depot_dirs: HashMap<(u32, u32), Ino>,
    by_key: HashMap<(u32, u32, u64), inode::SnapshotId>,
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
    /// Children indexed by name. Values are inodes.
    pub children: HashMap<String, Ino>,
    /// Snapshots registered here that have no name yet.
    pub unnamed: Vec<(inode::SnapshotId, Namer)>,
    /// Held for a naming pass so concurrent listings run the namers once.
    pub naming: Arc<tokio::sync::Mutex<()>>,
}

impl SyntheticDir {
    fn new() -> Self {
        Self {
            children: HashMap::new(),
            unnamed: Vec::new(),
            naming: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

/// Loaded contents of a mounted manifest. The owning [`Slot`] carries
/// the `(app_id, depot_id, manifest_gid)` triple — duplicating those
/// here would be redundant.
pub(crate) struct SnapshotEntry<C: ChunkStore> {
    pub snapshot: DepotManifestStore<C>,
}

/// An unnamed snapshot of a depot dir, as handed to a naming pass.
pub(crate) struct PendingName {
    pub sid: inode::SnapshotId,
    pub manifest_gid: u64,
    pub creation_time: Option<u32>,
    pub namer: Namer,
}

/// Where a registration places its snapshot.
pub(crate) struct Placement {
    /// Name of the app dir if it doesn't exist yet; the app id otherwise.
    pub app_name: Option<String>,
    /// Names the snapshot dir on first listing; the manifest gid otherwise.
    pub namer: Option<Namer>,
}

impl Placement {
    pub(crate) const IDS: Placement = Placement {
        app_name: None,
        namer: None,
    };
}

impl<C: ChunkStore> MountTree<C> {
    pub fn new() -> Self {
        Self {
            // Slot 0: placeholder so slot indices map to node ids. Slot 1: the root.
            synthetic: vec![SyntheticDir::new(), SyntheticDir::new()],
            app_dirs: HashMap::new(),
            depot_dirs: HashMap::new(),
            by_key: HashMap::new(),
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
        let creation_time = Some(snapshot.manifest().creation_time);
        let state = SlotState::Ready(Arc::new(SnapshotEntry { snapshot }));
        self.reserve_slot(
            app_id,
            depot_id,
            manifest_gid,
            state,
            creation_time,
            Placement::IDS,
        )
    }

    /// Register a manifest without loading it. The first FUSE operation
    /// that needs the manifest contents calls `opener` (via a `OnceCell`
    /// so concurrent callers coalesce) and the result is cached for the
    /// lifetime of the slot. `creation_time` preseeds the snapshot root's
    /// mtime, e.g. from a cached manifest.
    pub fn add_lazy(
        &mut self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        opener: Opener<C>,
        creation_time: Option<u32>,
        placement: Placement,
    ) -> Result<SnapshotId, AddError> {
        let state = SlotState::Pending(Arc::new(LazyEntry {
            opener,
            cell: Arc::new(OnceCell::new()),
        }));
        self.reserve_slot(
            app_id,
            depot_id,
            manifest_gid,
            state,
            creation_time,
            placement,
        )
    }

    fn reserve_slot(
        &mut self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        state: SlotState<C>,
        creation_time: Option<u32>,
        placement: Placement,
    ) -> Result<SnapshotId, AddError> {
        if self.by_key.contains_key(&(app_id, depot_id, manifest_gid)) {
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

        let app_dir = match self.app_dirs.get(&app_id) {
            Some(&ino) => ino,
            None => {
                let name = placement
                    .app_name
                    .as_deref()
                    .and_then(sanitize_name)
                    .unwrap_or_else(|| app_id.to_string());
                let ino = self.new_synthetic(inode::ROOT, &name);
                self.app_dirs.insert(app_id, ino);
                ino
            }
        };
        let depot_dir = match self.depot_dirs.get(&(app_id, depot_id)) {
            Some(&ino) => ino,
            None => {
                let ino = self.new_synthetic(app_dir, &depot_id.to_string());
                self.depot_dirs.insert((app_id, depot_id), ino);
                ino
            }
        };

        self.slots.push(Some(Arc::new(Slot {
            app_id,
            depot_id,
            manifest_gid,
            parent: depot_dir,
            state,
            creation_time,
        })));
        self.by_key.insert((app_id, depot_id, manifest_gid), id);
        match placement.namer {
            Some(namer) => self.dir_mut(depot_dir).unnamed.push((id, namer)),
            None => {
                let name = unique_name(self.children_of(depot_dir), &manifest_gid.to_string());
                self.children_of_mut(depot_dir)
                    .insert(name, inode::pack(id, 0));
            }
        }
        Ok(SnapshotId(id))
    }

    /// Drop the slot identified by `id`. The slot is left as `None`
    /// (never reused). Synthetic dirs are *not* pruned — they stick
    /// around for the lifetime of the mount.
    pub fn remove(&mut self, id: SnapshotId) -> Option<Arc<Slot<C>>> {
        let slot = self.slots.get_mut(id.0 as usize)?;
        let entry = slot.take()?;
        self.by_key
            .remove(&(entry.app_id, entry.depot_id, entry.manifest_gid));
        let root = inode::pack(id.0, 0);
        let dir = self.dir_mut(entry.parent);
        dir.children.retain(|_, &mut ino| ino != root);
        dir.unnamed.retain(|&(sid, _)| sid != id.0);
        Some(entry)
    }

    pub fn has_app(&self, app_id: u32) -> bool {
        self.app_dirs.contains_key(&app_id)
    }

    pub fn synthetic(&self, ino: Ino) -> Option<&SyntheticDir> {
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
            parent: slot.parent,
            state: SlotState::Ready(entry),
            creation_time: Some(creation_time),
        });
    }

    /// The unnamed snapshots of synthetic dir `dir`.
    pub fn pending_names(&self, dir: Ino) -> Vec<PendingName> {
        let Some(d) = self.synthetic(dir) else {
            return Vec::new();
        };
        d.unnamed
            .iter()
            .filter_map(|(sid, namer)| {
                let slot = self.slot(*sid)?;
                Some(PendingName {
                    sid: *sid,
                    manifest_gid: slot.manifest_gid,
                    creation_time: slot.creation_time,
                    namer: Arc::clone(namer),
                })
            })
            .collect()
    }

    /// Give each of `named` its dir entry in `dir`. Snapshots sharing a
    /// name get `<name> (2)`, `(3)`, … with the oldest keeping the plain one;
    /// a later batch never takes a name an earlier one already holds.
    pub fn assign_names(&mut self, dir: Ino, mut named: Vec<(PendingName, String)>) {
        named.sort_by_key(|(p, _)| (p.creation_time.is_none(), p.creation_time, p.manifest_gid));
        let d = self.dir_mut(dir);
        for (pending, name) in named {
            // removed while its namer ran
            let Some(i) = d.unnamed.iter().position(|&(sid, _)| sid == pending.sid) else {
                continue;
            };
            d.unnamed.swap_remove(i);
            let name = unique_name(&d.children, &name);
            d.children.insert(name, inode::pack(pending.sid, 0));
        }
    }

    fn new_synthetic(&mut self, parent: Ino, name: &str) -> Ino {
        let slot = self.synthetic.len();
        let ino = inode::pack(SYNTHETIC, slot as u64);
        self.synthetic.push(SyntheticDir::new());
        let name = unique_name(self.children_of(parent), name);
        self.children_of_mut(parent).insert(name, ino);
        ino
    }

    fn dir_mut(&mut self, ino: Ino) -> &mut SyntheticDir {
        let (sid, idx) = inode::unpack(ino);
        debug_assert_eq!(sid, SYNTHETIC, "dir_mut called with non-synthetic ino");
        &mut self.synthetic[idx as usize]
    }

    fn children_of(&self, ino: Ino) -> &HashMap<String, Ino> {
        let (sid, idx) = inode::unpack(ino);
        debug_assert_eq!(sid, SYNTHETIC, "children_of called with non-synthetic ino");
        &self.synthetic[idx as usize].children
    }

    fn children_of_mut(&mut self, ino: Ino) -> &mut HashMap<String, Ino> {
        &mut self.dir_mut(ino).children
    }
}

/// `base`, or the first free `base (n)` for n ≥ 2.
fn unique_name(taken: &HashMap<String, Ino>, base: &str) -> String {
    if !taken.contains_key(base) {
        return base.to_owned();
    }
    (2..)
        .map(|n| format!("{base} ({n})"))
        .find(|name| !taken.contains_key(name))
        .expect("unbounded range")
}

/// `name` as a path component valid on every back end's platform, or
/// `None` if nothing usable is left.
pub(crate) fn sanitize_name(name: &str) -> Option<String> {
    let replaced: String = name
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    // Windows drops trailing dots and spaces from path components.
    let trimmed = replaced.trim_start().trim_end_matches([' ', '.']);
    match trimmed {
        "" | "." | ".." => None,
        name => Some(name.to_owned()),
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
