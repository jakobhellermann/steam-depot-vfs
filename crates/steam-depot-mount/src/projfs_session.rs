//! Mount lifecycle for the Windows ProjFS back end. No server or external
//! mount command: ProjFS virtualizes the root in-process, and dropping the
//! handle tears it down.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::DepotManifestStore;
use tokio::runtime::Handle;

use crate::projfs::ProjFsSource;
use crate::tree::{self, AddError, MountTree, Placement, SnapshotId};

pub struct ProjFsMountConfig {
    /// Directory to virtualize. Must be empty/fresh.
    pub root: PathBuf,
}

impl ProjFsMountConfig {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }
}

/// Live ProjFS mount. Mirrors [`crate::NfsMount`]'s surface.
pub struct ProjFsMount<C: ChunkStore + 'static> {
    // Drop order matters: `_pfs` must stop virtualization (and any in-flight
    // callback touching the tree) before `tree` is dropped.
    _pfs: projfs::ProjFS,
    tree: Arc<RwLock<MountTree<C>>>,
}

impl<C: ChunkStore + Send + Sync + 'static> ProjFsMount<C> {
    /// `rt` must run on *other* threads: ProjFS's callbacks drive async
    /// chunk/manifest work on it via `block_on`.
    pub fn start(cfg: ProjFsMountConfig, rt: Handle) -> Result<Self, ProjFsMountError> {
        let tree = Arc::new(RwLock::new(MountTree::<C>::new()));
        let source = ProjFsSource::new(Arc::clone(&tree), rt);
        let pfs = projfs::ProjFS::new(&cfg.root, source).map_err(ProjFsMountError::Start)?;
        tracing::info!(root = %cfg.root.display(), "ProjFS mount ready");
        Ok(Self { _pfs: pfs, tree })
    }

    pub fn add(
        &self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        snapshot: DepotManifestStore<C>,
    ) -> Result<SnapshotId, AddError> {
        self.tree
            .write()
            .add(app_id, depot_id, manifest_gid, snapshot)
    }

    /// Register a manifest lazily; `opener` runs on first access.
    pub fn add_lazy<F, Fut>(
        &self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        opener: F,
        creation_time: Option<u32>,
    ) -> Result<SnapshotId, AddError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<DepotManifestStore<C>, std::io::Error>> + Send + 'static,
    {
        self.tree.write().add_lazy(
            app_id,
            depot_id,
            manifest_gid,
            tree::opener(opener),
            creation_time,
            Placement::IDS,
        )
    }

    /// Register a manifest like `add_lazy`, but under
    /// `/<app_name>/<depot_id>/<name>`. `app_name` is called when this is
    /// the app's first snapshot, to name its dir (`None`: the app id).
    /// `namer` names the snapshot dir when its depot dir is first listed or
    /// looked up in (`Ok(None)`: the manifest gid); an error fails that
    /// operation and the next one retries.
    #[allow(clippy::too_many_arguments)]
    pub fn add_lazy_named<F, Fut, N, NFut>(
        &self,
        app_id: u32,
        app_name: impl FnOnce() -> Option<String>,
        depot_id: u32,
        manifest_gid: u64,
        opener: F,
        namer: N,
        creation_time: Option<u32>,
    ) -> Result<SnapshotId, AddError>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<DepotManifestStore<C>, std::io::Error>> + Send + 'static,
        N: Fn() -> NFut + Send + Sync + 'static,
        NFut: Future<Output = Result<Option<String>, std::io::Error>> + Send + 'static,
    {
        // outside the write lock, as it may do I/O
        let app_name = (!self.tree.read().has_app(app_id)).then(app_name).flatten();
        self.tree.write().add_lazy(
            app_id,
            depot_id,
            manifest_gid,
            tree::opener(opener),
            creation_time,
            Placement {
                app_name,
                namer: Some(tree::namer(namer)),
            },
        )
    }

    pub fn remove(&self, id: SnapshotId) -> bool {
        self.tree.write().remove(id).is_some()
    }

    /// Stop the virtualization. The root dir is left on disk (with ProjFS
    /// placeholder state); the caller cleans it up.
    pub fn unmount(self) {
        drop(self._pfs);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProjFsMountError {
    #[error(transparent)]
    Start(#[from] std::io::Error),
}
