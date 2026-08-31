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
use crate::tree::{AddError, MountTree, Opener, OpenerFuture, SnapshotId};

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
        let pfs =
            projfs::ProjFS::new(&cfg.root, source).map_err(ProjFsMountError::Start)?;
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
        let opener: Opener<C> = Arc::new(move || -> OpenerFuture<C> { Box::pin(opener()) });
        self.tree
            .write()
            .add_lazy(app_id, depot_id, manifest_gid, opener, creation_time)
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
