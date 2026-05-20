use thiserror::Error;

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("path not found: {0}")]
    NotFound(String),
    #[error("not a regular file: {0}")]
    NotAFile(String),
    #[error("read past end of file (size={size}, offset={offset})")]
    OutOfRange { size: u64, offset: u64 },
    #[error("chunk store error: {0}")]
    ChunkStore(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error(transparent)]
    Depot(#[from] steam_vent_depot::DepotError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T, E = VfsError> = std::result::Result<T, E>;
