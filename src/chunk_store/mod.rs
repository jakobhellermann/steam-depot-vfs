//! Pluggable byte source for chunks, keyed by SHA-1.
//!
//! The library ships with two implementations:
//! - [`cdn::SteamCdnChunkStore`] — fetches directly from Steam's CDN.
//! - [`cache::FsCacheStore`] — write-through local-disk cache wrapping any
//!   other store.
//!
//! They compose: typical setup is `FsCacheStore<SteamCdnChunkStore>`.

use std::future::Future;

use bytes::Bytes;

use crate::sha::ChunkSha;

pub mod cache;
pub mod cdn;

pub use cache::FsCacheStore;
pub use cdn::SteamCdnChunkStore;

pub(crate) type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// Source of decrypted + decompressed chunk bytes, keyed by SHA-1.
///
/// Implementations are responsible for whatever encryption/decompression
/// the underlying transport needs — the bytes returned here are the raw
/// plaintext content of the chunk.
pub trait ChunkStore: Send + Sync {
    fn get(&self, sha: ChunkSha) -> impl Future<Output = Result<Bytes, BoxedError>> + Send;
}
