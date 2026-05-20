//! Programmatic, file-system-shaped access to a Steam depot manifest.
//!
//! Given a parsed [`steam_vent_depot::Manifest`] and a [`ChunkStore`] (something
//! that turns chunk SHAs into bytes), you can read individual files, slice
//! arbitrary byte ranges, and walk the directory tree — without materialising
//! the whole depot on disk.
//!
//! See `examples/cat.rs` for end-to-end usage. Mount-style frontends (FUSE,
//! WebDAV, …) live in their own examples and use the same API.

mod chunk_store;
mod depot_fs;
mod error;
mod manifest_cache;
mod sha;

pub use chunk_store::{ChunkStore, FsCacheStore, SteamCdnChunkStore};
pub use depot_fs::{DepotFs, Entry, FileMeta};
pub use error::{Result, VfsError};
pub use manifest_cache::{CacheError, ManifestCache};
pub use sha::ChunkSha;
pub use steam_vent_depot::FileKind;
