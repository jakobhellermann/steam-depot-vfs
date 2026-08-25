// TODO(ai-review): review for correctness/style
//! Mount lifecycle for the NFS back end: a localhost NFSv3 server plus
//! the platform `mount` command pointed at it.
//!
//! The mount is unprivileged on macOS — the built-in NFS client accepts
//! a user-initiated mount of 127.0.0.1 — which is the whole reason this
//! back end exists next to FUSE. On linux `mount.nfs` insists on root,
//! so there the FUSE back end is the better deal unless `/dev/fuse` is
//! unavailable.

#[cfg(target_os = "macos")]
use std::ffi::OsStr;
use std::future::Future;
use std::io::Error as IoError;
#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use parking_lot::RwLock;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::DepotManifestStore;
use tokio::task::JoinHandle;

use crate::nfs::NfsFs;
use crate::tree::{AddError, MountTree, Opener, OpenerFuture, SnapshotId};

/// How long to wait for the kernel to let go of a forced-out mount
/// before giving up on reusing the mountpoint.
const UMOUNT_SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// Cleared by [`NfsMount::unmount`] so [`Drop`] doesn't try again.
    mounted: bool,
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

        if let Err(e) = prepare_mountpoint(&cfg.mountpoint).await {
            server.abort();
            return Err(e);
        }
        if let Err(e) = run_mount(&cfg.mountpoint, port).await {
            server.abort();
            // A rejected option doesn't stop the mount from happening,
            // so a failed start can still leave one behind — and one
            // nobody serves hangs every access to it.
            if let Err(cleanup) = force_unmount_if_mounted(&cfg.mountpoint).await {
                tracing::error!(
                    path = %cfg.mountpoint.display(),
                    %cleanup,
                    "the failed mount could not be cleaned up",
                );
            }
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
            mounted: true,
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
    ///
    /// Async because `umount` waits for RPCs that only this process can
    /// answer: blocking on it from the thread driving the server task
    /// deadlocks.
    pub async fn unmount(mut self) -> Result<(), NfsMountError> {
        let status = privileged_async("umount")
            .arg(&self.mountpoint)
            .status()
            .await
            .map_err(NfsMountError::Unmount)?;
        self.server.abort();
        if !status.success() {
            // Left `mounted`, so the drop that follows forces it out.
            return Err(NfsMountError::UnmountFailed { status });
        }
        self.mounted = false;
        Ok(())
    }
}

impl<C: ChunkStore + 'static> Drop for NfsMount<C> {
    /// Last resort for handles that are dropped rather than unmounted.
    /// `Drop` can't await, and the server is about to stop answering
    /// either way, so this forces the mount out rather than asking
    /// politely — same as recovering from a crashed process.
    fn drop(&mut self) {
        if !self.mounted {
            return;
        }
        self.server.abort();
        let status = privileged("umount")
            .arg("-f")
            .arg(&self.mountpoint)
            .status();
        match status {
            Ok(status) if status.success() => {
                tracing::debug!(path = %self.mountpoint.display(), "forced out on drop");
            }
            Ok(status) => tracing::error!(
                path = %self.mountpoint.display(),
                %status,
                "dropped without unmounting and the forced unmount failed; \
                 the mountpoint will hang until it is unmounted by hand",
            ),
            Err(e) => tracing::error!(
                path = %self.mountpoint.display(),
                %e,
                "dropped without unmounting and umount could not be run",
            ),
        }
    }
}

/// Make `mountpoint` an empty directory ready to be mounted on.
///
/// Crash recovery: a process that dies without unmounting leaves the
/// mount in place, and every access to it — including the `stat` that
/// `create_dir_all` does — then hangs until someone forces it out. So
/// the check asks the kernel's mount table instead of the path itself.
async fn prepare_mountpoint(mountpoint: &Path) -> Result<(), NfsMountError> {
    force_unmount_if_mounted(mountpoint).await?;
    std::fs::create_dir_all(mountpoint).map_err(NfsMountError::Mountpoint)
}

/// Force out whatever is mounted at `mountpoint` and wait for the kernel
/// to agree that it is gone.
async fn force_unmount_if_mounted(mountpoint: &Path) -> Result<(), NfsMountError> {
    if !is_mounted(mountpoint)? {
        return Ok(());
    }
    tracing::warn!(
        path = %mountpoint.display(),
        "mountpoint still carries a mount; forcing it out",
    );
    let status = privileged_async("umount")
        .arg("-f")
        .arg(mountpoint)
        .status()
        .await
        .map_err(NfsMountError::Unmount)?;
    if !status.success() {
        return Err(NfsMountError::UnmountFailed { status });
    }
    // `umount -f` returns before the kernel has dropped the mount, and
    // mounting onto a mountpoint it still knows about fails with EPERM.
    // Wait for the mount table to agree.
    let deadline = Instant::now() + UMOUNT_SETTLE_TIMEOUT;
    while is_mounted(mountpoint)? {
        if Instant::now() >= deadline {
            return Err(NfsMountError::UnmountDidNotSettle);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
async fn run_mount(mountpoint: &Path, port: u16) -> Result<(), NfsMountError> {
    // `locallocks` rather than `nolocks`: the latter makes the client
    // answer ENOTSUP to flock/fcntl, and we serve no NLM, so locking has
    // to stay inside the client kernel.
    let opts = format!(
        "rdonly,locallocks,vers=3,tcp,rsize=1048576,actimeo={ATTR_CACHE_SECS},port={port},mountport={port}"
    );
    run("mount_nfs", &opts, mountpoint).await
}

#[cfg(target_os = "linux")]
async fn run_mount(mountpoint: &Path, port: u16) -> Result<(), NfsMountError> {
    // No NLM server here, so `nolock` — unlike macOS, the linux client
    // reads that as "handle locks locally" rather than "unsupported".
    let opts = format!(
        "ro,nolock,vers=3,tcp,rsize=1048576,actimeo={ATTR_CACHE_SECS},port={port},mountport={port}"
    );
    run("mount.nfs", &opts, mountpoint).await
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
async fn run_mount(_mountpoint: &Path, _port: u16) -> Result<(), NfsMountError> {
    Err(NfsMountError::UnsupportedPlatform)
}

/// The mount command talks to our own server while it runs, so it has to
/// be awaited rather than blocked on — a blocking wait here deadlocks a
/// current-thread runtime.
#[cfg(any(target_os = "macos", target_os = "linux"))]
async fn run(program: &str, opts: &str, mountpoint: &Path) -> Result<(), NfsMountError> {
    let out = privileged_async(program)
        .args(["-o", opts])
        .arg("127.0.0.1:/")
        .arg(mountpoint)
        .output()
        .await
        .map_err(NfsMountError::MountCommand)?;
    check_mount_output(out.status, &out.stdout, &out.stderr)
}

/// A mount that worked says nothing, so anything the command printed is
/// a rejected option — and `mount_nfs` reports those while still exiting
/// 0 and mounting with a default in place of what we asked for. Asking
/// for `rsize=1048576` gets "illegal rsize value", status 0, and 32 KiB
/// reads instead of 112 KiB: silently three times the requests.
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn check_mount_output(
    status: std::process::ExitStatus,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<(), NfsMountError> {
    let complaint = [stdout, stderr]
        .iter()
        .map(|out| String::from_utf8_lossy(out).trim().to_string())
        .filter(|out| !out.is_empty())
        .collect::<Vec<_>>()
        .join("; ");
    if !status.success() {
        return Err(NfsMountError::MountFailed { status, complaint });
    }
    if !complaint.is_empty() {
        return Err(NfsMountError::MountOptionRejected { complaint });
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

/// The same command as an awaitable one.
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn privileged_async(program: &str) -> tokio::process::Command {
    tokio::process::Command::from(privileged(program))
}

#[derive(Debug, thiserror::Error)]
pub enum NfsMountError {
    #[error("could not bind the loopback NFS server: {0}")]
    Bind(#[source] IoError),
    #[error("could not create the mountpoint: {0}")]
    Mountpoint(#[source] IoError),
    #[error("could not run the mount command: {0}")]
    MountCommand(#[source] IoError),
    #[error("mount command exited with {status}: {complaint}")]
    MountFailed {
        status: std::process::ExitStatus,
        complaint: String,
    },
    #[error("the mount command rejected an option and mounted with a default instead: {complaint}")]
    MountOptionRejected { complaint: String },
    #[error("could not run umount: {0}")]
    Unmount(#[source] IoError),
    #[error("umount exited with {status}")]
    UnmountFailed { status: std::process::ExitStatus },
    #[error("could not read the mount table: {0}")]
    MountTable(#[source] IoError),
    #[error("the mountpoint is still mounted after a forced unmount")]
    UnmountDidNotSettle,
    #[error("no NFS mount command is known for this platform")]
    UnsupportedPlatform,
}

/// Whether the kernel currently has anything mounted at `path`.
///
/// Reads the kernel's mount table, never the path — a mount whose server
/// is gone makes any `stat` on it block forever.
#[cfg(target_os = "macos")]
fn is_mounted(path: &Path) -> Result<bool, NfsMountError> {
    let Some(wanted) = resolved(path) else {
        return Ok(false);
    };
    // MNT_NOWAIT: report cached values instead of asking each filesystem,
    // which is the part that could hang.
    let count = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
    if count <= 0 {
        return Err(NfsMountError::MountTable(std::io::Error::last_os_error()));
    }
    let mut entries: Vec<libc::statfs> = Vec::with_capacity(count as usize);
    let bytes = (entries.capacity() * std::mem::size_of::<libc::statfs>()) as libc::c_int;
    let got = unsafe { libc::getfsstat(entries.as_mut_ptr(), bytes, libc::MNT_NOWAIT) };
    if got < 0 {
        return Err(NfsMountError::MountTable(std::io::Error::last_os_error()));
    }
    unsafe { entries.set_len(got as usize) };
    Ok(entries.iter().any(|fs| {
        let end = fs
            .f_mntonname
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(fs.f_mntonname.len());
        let bytes: Vec<u8> = fs.f_mntonname[..end].iter().map(|&c| c as u8).collect();
        Path::new(OsStr::from_bytes(&bytes)) == wanted
    }))
}

#[cfg(target_os = "linux")]
fn is_mounted(path: &Path) -> Result<bool, NfsMountError> {
    let Some(wanted) = resolved(path) else {
        return Ok(false);
    };
    let mounts = std::fs::read_to_string("/proc/self/mounts").map_err(NfsMountError::MountTable)?;
    Ok(mounts.lines().any(|line| {
        line.split_whitespace()
            .nth(1)
            .is_some_and(|on| Path::new(on) == wanted)
    }))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn is_mounted(_path: &Path) -> Result<bool, NfsMountError> {
    Err(NfsMountError::UnsupportedPlatform)
}

/// `path` with its parent resolved, because mount tables carry resolved
/// paths (`/private/var/…` rather than `/var/…`). The parent is safe to
/// resolve even when `path` itself is a hung mount. `None` if the parent
/// doesn't exist, in which case nothing can be mounted there either.
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn resolved(path: &Path) -> Option<PathBuf> {
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        // No parent means the root, which is safe to resolve directly —
        // it is never the hung mount we are avoiding a `stat` on.
        return path.canonicalize().ok();
    };
    Some(parent.canonicalize().ok()?.join(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exit status alone can't be trusted: `mount_nfs` prints
    /// "illegal rsize value -- 4194304" and still exits 0.
    #[test]
    fn a_rejected_option_fails_the_mount_despite_a_zero_exit() {
        let ok = std::process::Command::new("true")
            .status()
            .expect("run true");
        assert!(ok.success(), "the fixture status has to be a success");
        let err = check_mount_output(ok, b"", b"mount_nfs: illegal rsize value -- 4194304\n")
            .expect_err("a complaint must fail the mount");
        assert_eq!(
            err.to_string(),
            "the mount command rejected an option and mounted with a default instead: \
             mount_nfs: illegal rsize value -- 4194304",
        );
    }

    #[test]
    fn a_silent_success_is_a_success() {
        let ok = std::process::Command::new("true")
            .status()
            .expect("run true");
        check_mount_output(ok, b"", b"").expect("a silent mount is fine");
    }

    #[test]
    fn the_root_filesystem_counts_as_mounted() {
        assert!(is_mounted(Path::new("/")).expect("read mount table"));
    }

    #[test]
    fn an_unmounted_directory_does_not() {
        assert!(!is_mounted(&std::env::temp_dir()).expect("read mount table"));
    }
}
