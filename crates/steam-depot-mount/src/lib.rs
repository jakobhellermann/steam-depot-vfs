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

// Without a back end there is nothing to serve the tree to, so a
// featureless build collapses to an empty crate.
#![cfg(any(feature = "fuse", feature = "nfs"))]

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

#[cfg(feature = "fuse")]
pub use session::{Mount, MountConfig, MountError};

#[cfg(feature = "nfs")]
pub use nfs_session::{NfsMount, NfsMountConfig, NfsMountError};

pub use tree::{AddError, SnapshotId};
