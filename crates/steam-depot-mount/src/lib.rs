// TODO(ai-review): review for correctness/style
//! Mount one or more Steam depot manifests as a single read-only
//! filesystem. The mount exposes a fixed three-level prefix:
//!
//! ```text
//! /<mountpoint>/<app_id>/<depot_id>/<manifest_gid>/<file path inside depot>
//! ```
//!
//! Snapshots can be added and removed at runtime — adds are visible
//! immediately; removes drop the subtree from new lookups and reject
//! reads against any inodes that referenced it.
//!
//! Two back ends serve the same tree, each behind its own feature:
//!
//! - `fuse` gives you [`Mount`], which needs a FUSE-speaking kernel and
//!   is the fast path on linux.
//! - `nfs` gives you [`NfsMount`], which serves a loopback NFSv3 server
//!   and mounts it with the platform NFS client. On macOS that needs
//!   neither a kernel extension nor elevated privileges, which is why it
//!   exists; on linux the mount itself wants root.
//!
//! Which one fits is usually decided by the platform, but not always —
//! containers without `/dev/fuse` and Windows have no FUSE at all — so
//! the choice is a feature rather than a `cfg(target_os)`.

//! - `projfs` gives you [`ProjFsMount`], which uses the Windows Projected
//!   File System to virtualize the tree in-process — the native fit on
//!   Windows, where there is no FUSE.

// Without a back end there is nothing to serve the tree to, so a
// featureless build collapses to an empty crate.
#![cfg(any(feature = "fuse", feature = "nfs", feature = "projfs"))]

mod inode;
mod tree;
mod view;

#[cfg(feature = "fuse")]
mod fuse;
#[cfg(feature = "fuse")]
mod session;

#[cfg(feature = "nfs")]
mod nfs;
#[cfg(feature = "nfs")]
mod nfs_session;

// Windows-only: the ProjFS binding doesn't exist elsewhere.
#[cfg(all(windows, feature = "projfs"))]
mod projfs;
#[cfg(all(windows, feature = "projfs"))]
mod projfs_session;

#[cfg(feature = "fuse")]
pub use session::{Mount, MountConfig, MountError};

#[cfg(feature = "nfs")]
pub use nfs_session::{NfsMount, NfsMountConfig, NfsMountError};

#[cfg(all(windows, feature = "projfs"))]
pub use ::projfs::{EnableOutcome, enable_feature_elevated};
#[cfg(all(windows, feature = "projfs"))]
pub use projfs_session::{ProjFsMount, ProjFsMountConfig, ProjFsMountError};

pub use tree::{AddError, SnapshotId};
