// TODO(ai-review): review for correctness/style
//! Programmatic, file-system-shaped access to a Steam depot manifest, with
//! transparent local caching of manifests and chunks.
//!
//! # Entry points
//!
//! - [`DepotStore`] — owns the on-disk cache (manifests + chunks) and is the
//!   high-level entry point. Call [`DepotStore::open_depot_manifest`] with auth + ids to get
//!   back a ready-to-use [`fs::DepotSnapshot`].
//! - [`fs::DepotSnapshot`] — file-system view over a single manifest. Provides
//!   `list_dir`, `metadata`, `read`, `read_full`.
//!
//! # Auth
//!
//! Steam access goes through the [`SteamAuth`] trait, which yields a
//! [`SteamSession`] (client + CDN servers) on demand. Depot keys are
//! fetched by [`DepotStore`] itself and cached in-process — auth impls
//! don't need to know about them. Implement [`SteamAuth`] for whatever
//! lazy/eager login flow you want; the lib stays out of login policy.
//!
//! # Chunk plumbing
//!
//! Most users don't touch this — [`DepotStore::open_depot_manifest`] wires it up. The pieces
//! live in [`chunk_store`]:
//! [`ChunkStore`](chunk_store::ChunkStore) trait,
//! [`CdnChunkStore`](chunk_store::CdnChunkStore) (Steam-CDN-backed), and
//! [`FsCacheStore`](chunk_store::FsCacheStore) (write-through local cache).
//!
//! # Examples
//!
//! See `examples/cat.rs` for an end-to-end CLI. Mount-style frontends (FUSE,
//! WebDAV, …) live in further examples and reuse `DepotSnapshot`.

mod auth;
pub mod chunk_store;
mod context;
mod error;
pub mod fs;
mod manifest_cache;

pub use auth::{SteamAuth, SteamSession};
pub use context::DepotStore;
pub use error::{Result, VfsError};
pub use manifest_cache::CacheError;
pub use steam_vent_depot::ChunkHash;
pub use steam_vent_depot::FileKind;
