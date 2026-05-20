//! Mount one or more Steam depot manifests as a single read-only FUSE
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
//! Linux only. macOS doesn't have a usable FUSE story for fuser 0.17.

#![cfg(target_os = "linux")]

mod fuse;
mod inode;
mod session;
mod tree;

pub use session::{Mount, MountConfig, MountError};
pub use tree::{AddError, SnapshotId};
