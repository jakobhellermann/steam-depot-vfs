// TODO(ai-review): review for correctness/style
use thiserror::Error;

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("path not found: {0}")]
    NotFound(String),
    #[error("not a regular file: {0}")]
    NotAFile(String),
    #[error("read past end of file (size={size}, offset={offset})")]
    OutOfRange { size: u64, offset: u64 },
    #[error("chunk {0} not present in manifest index")]
    ChunkNotInManifest(steam_vent_depot::ChunkHash),
    /// Escape hatch for [`ChunkStore`](crate::chunk_store::ChunkStore) impls
    /// that need to wrap an error from a non-Steam source (custom transports,
    /// alternative caches, …). Built-in impls don't use this.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Depot(#[from] steam_vent_depot::DepotError),
    #[error(transparent)]
    ServerDiscoveryError(steam_vent::ServerDiscoveryError),
    #[error(transparent)]
    ConnectionError(steam_vent::ConnectionError),
    #[error(transparent)]
    ManifestCache(#[from] crate::manifest_cache::CacheError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = VfsError> = std::result::Result<T, E>;
