// TODO(ai-review): review for correctness/style
//! Mount lifecycle for the NFS back end: a localhost NFSv3 server plus
//! the platform `mount` command pointed at it.
//!
//! The mount is unprivileged on macOS — the built-in NFS client accepts
//! a user-initiated mount of 127.0.0.1 — which is the whole reason this
//! back end exists next to FUSE. On linux `mount.nfs` insists on root,
//! so there the FUSE back end is the better deal unless `/dev/fuse` is
//! unavailable.

use std::future::Future;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use parking_lot::RwLock;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::DepotManifestStore;
use tokio::task::JoinHandle;

use crate::nfs::NfsFs;
use crate::tree::{AddError, MountTree, Opener, OpenerFuture, SnapshotId};

/// How long the client may cache attributes and directory entries.
/// Snapshots are immutable, so this only delays visibility of `add` and
/// `remove` — one hour matches the FUSE back end's TTL.
const ATTR_CACHE_SECS: u64 = 60 * 60;

pub struct NfsMountConfig {
    pub mountpoint: PathBuf,
}

impl NfsMountConfig {
    pub fn new(mountpoint: PathBuf) -> Self {
        Self { mountpoint }
    }
}

/// Live NFS mount. Mirrors [`crate::Mount`]'s surface so callers can
/// pick a back end per platform without changing how they register
/// snapshots.
pub struct NfsMount<C: ChunkStore + 'static> {
    tree: Arc<RwLock<MountTree<C>>>,
    mountpoint: PathBuf,
    server: JoinHandle<()>,
}

impl<C: ChunkStore + Send + Sync + 'static> NfsMount<C> {
    /// Serve an empty filesystem on a loopback port and mount it at
    /// `cfg.mountpoint`, which is created if missing.
    pub async fn start(cfg: NfsMountConfig) -> Result<Self, NfsMountError> {
        let tree = Arc::new(RwLock::new(MountTree::<C>::new()));
        let fs = NfsFs::new(Arc::clone(&tree));
        let listener = NFSTcpListener::bind("127.0.0.1:0", fs)
            .await
            .map_err(NfsMountError::Bind)?;
        let port = listener.get_listen_port();
        // Serve before mounting: the mount command talks to the server
        // during the mount itself and fails if nobody answers.
        let server = tokio::spawn(async move {
            if let Err(e) = listener.handle_forever().await {
                tracing::error!(%e, "NFS server stopped");
            }
        });

        std::fs::create_dir_all(&cfg.mountpoint).map_err(NfsMountError::Mountpoint)?;
        if let Err(e) = run_mount(&cfg.mountpoint, port) {
            server.abort();
            return Err(e);
        }
        tracing::info!(
            mountpoint = %cfg.mountpoint.display(),
            port,
            "NFS mount ready",
        );
        Ok(Self {
            tree,
            mountpoint: cfg.mountpoint,
            server,
        })
    }

    /// Add an already-loaded `snapshot` under
    /// `/<app_id>/<depot_id>/<manifest_gid>`.
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

    /// Register a manifest without loading it; the first NFS operation
    /// that needs its contents runs `opener`.
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

    /// Unmount and stop serving. The server task is aborted only after
    /// the unmount ran, so the client can still complete the in-flight
    /// teardown RPCs.
    pub fn unmount(self) -> Result<(), NfsMountError> {
        let status = privileged("umount")
            .arg(&self.mountpoint)
            .status()
            .map_err(NfsMountError::Unmount)?;
        self.server.abort();
        if !status.success() {
            return Err(NfsMountError::UnmountFailed { status });
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn run_mount(mountpoint: &Path, port: u16) -> Result<(), NfsMountError> {
    // `locallocks` rather than `nolocks`: the latter makes the client
    // answer ENOTSUP to flock/fcntl, and we serve no NLM, so locking has
    // to stay inside the client kernel.
    let opts = format!(
        "rdonly,locallocks,vers=3,tcp,rsize=1048576,actimeo={ATTR_CACHE_SECS},port={port},mountport={port}"
    );
    run("mount_nfs", &opts, mountpoint)
}

#[cfg(target_os = "linux")]
fn run_mount(mountpoint: &Path, port: u16) -> Result<(), NfsMountError> {
    // No NLM server here, so `nolock` — unlike macOS, the linux client
    // reads that as "handle locks locally" rather than "unsupported".
    let opts = format!(
        "ro,nolock,vers=3,tcp,rsize=1048576,actimeo={ATTR_CACHE_SECS},port={port},mountport={port}"
    );
    run("mount.nfs", &opts, mountpoint)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn run_mount(_mountpoint: &Path, _port: u16) -> Result<(), NfsMountError> {
    Err(NfsMountError::UnsupportedPlatform)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn run(program: &str, opts: &str, mountpoint: &Path) -> Result<(), NfsMountError> {
    let status = privileged(program)
        .args(["-o", opts])
        .arg("127.0.0.1:/")
        .arg(mountpoint)
        .status()
        .map_err(NfsMountError::MountCommand)?;
    if !status.success() {
        return Err(NfsMountError::MountFailed { status });
    }
    Ok(())
}

/// macOS lets a plain user mount a loopback NFS export; linux doesn't,
/// so there the command goes through a non-interactive `sudo` unless we
/// are already root.
fn privileged(program: &str) -> Command {
    #[cfg(target_os = "linux")]
    if unsafe { libc::geteuid() } != 0 {
        let mut cmd = Command::new("sudo");
        cmd.arg("-n").arg(program);
        return cmd;
    }
    Command::new(program)
}

#[derive(Debug, thiserror::Error)]
pub enum NfsMountError {
    #[error("could not bind the loopback NFS server: {0}")]
    Bind(#[source] IoError),
    #[error("could not create the mountpoint: {0}")]
    Mountpoint(#[source] IoError),
    #[error("could not run the mount command: {0}")]
    MountCommand(#[source] IoError),
    #[error("mount command exited with {status}")]
    MountFailed { status: std::process::ExitStatus },
    #[error("could not run umount: {0}")]
    Unmount(#[source] IoError),
    #[error("umount exited with {status}")]
    UnmountFailed { status: std::process::ExitStatus },
    #[error("no NFS mount command is known for this platform")]
    UnsupportedPlatform,
}
