// TODO(ai-review): review for correctness/style
//! Mount lifecycle: the FUSE session, signal handling, and the public
//! [`Mount`] handle.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use fuser::{BackgroundSession, MountOption};
use parking_lot::RwLock;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::DepotSnapshot;
use tokio::runtime::Handle;

use crate::fuse::FuseFs;
use crate::tree::{AddError, MountTree, SnapshotId};

/// Configuration for [`Mount::start`].
pub struct MountConfig {
    pub mountpoint: PathBuf,
    /// Filesystem name shown in `/proc/mounts`. Defaults to
    /// `"steam-depot-mount"` if `None`.
    pub fs_name: Option<String>,
}

impl MountConfig {
    pub fn new(mountpoint: PathBuf) -> Self {
        Self {
            mountpoint,
            fs_name: None,
        }
    }
}

/// Live FUSE mount. Snapshots can be added and removed for the lifetime
/// of the mount; dropping or calling [`Mount::unmount`] tears it down.
pub struct Mount<C: ChunkStore + 'static> {
    bg: BackgroundSession,
    tree: Arc<RwLock<MountTree<C>>>,
}

impl<C: ChunkStore + 'static> Mount<C> {
    /// Mount an empty FUSE filesystem at `cfg.mountpoint`. The mount
    /// becomes visible immediately but contains no manifests until you
    /// call [`Mount::add`].
    ///
    /// `rt` is the Tokio runtime handle the FUSE adapter spawns async
    /// chunk fetches on. Pass the caller's main runtime so we don't
    /// build a second one — keep the runtime alive at least as long as
    /// the returned [`Mount`].
    ///
    /// The mountpoint directory must already exist.
    pub fn start(cfg: MountConfig, rt: Handle) -> Result<Self, MountError> {
        let tree = Arc::new(RwLock::new(MountTree::<C>::new()));
        let fs = FuseFs::new(Arc::clone(&tree), rt);
        let mut fuser_cfg = fuser::Config::default();
        fuser_cfg.mount_options = vec![
            MountOption::RO,
            MountOption::FSName(
                cfg.fs_name
                    .unwrap_or_else(|| "steam-depot-mount".to_string()),
            ),
        ];
        let session =
            fuser::Session::new(fs, &cfg.mountpoint, &fuser_cfg).map_err(MountError::Fuse)?;
        let bg = session.spawn().map_err(MountError::Fuse)?;
        Ok(Self { bg, tree })
    }

    /// Add `snapshot` under `/<app_id>/<depot_id>/<manifest_gid>`.
    /// Visible immediately; the kernel will pick it up on the next
    /// `lookup` against that path.
    pub fn add(
        &self,
        app_id: u32,
        depot_id: u32,
        manifest_gid: u64,
        snapshot: DepotSnapshot<C>,
    ) -> Result<SnapshotId, AddError> {
        self.tree
            .write()
            .add(app_id, depot_id, manifest_gid, snapshot)
    }

    /// Detach a snapshot from the mount. New lookups against its
    /// subtree return `ENOENT`. In-flight reads against the snapshot
    /// (via cloned `Arc`s) finish normally.
    pub fn remove(&self, id: SnapshotId) -> bool {
        self.tree.write().remove(id).is_some()
    }

    /// Block until SIGINT (Ctrl-C) or SIGTERM is received, then
    /// unmount cleanly and join the FUSE thread.
    pub async fn wait_for_signal(self) -> io::Result<()> {
        wait_for_term_signal().await?;
        tracing::info!("signal received, unmounting");
        self.bg.umount_and_join()
    }

    /// Unmount immediately and join the FUSE thread.
    pub fn unmount(self) -> io::Result<()> {
        self.bg.umount_and_join()
    }
}

/// Wait for SIGINT or SIGTERM, whichever comes first.
async fn wait_for_term_signal() -> io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        r = tokio::signal::ctrl_c() => r,
        _ = sigterm.recv() => Ok(()),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MountError {
    #[error("FUSE error: {0}")]
    Fuse(#[source] io::Error),
}
