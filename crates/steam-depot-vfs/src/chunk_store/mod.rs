// TODO(ai-review): review for correctness/style
//! Pluggable byte source for chunks, keyed by SHA-1.
//!
//! The library ships with two implementations:
//! - [`cdn::CdnChunkStore`] — fetches directly from Steam's CDN.
//! - [`cache::FsCacheStore`] — write-through local-disk cache wrapping any
//!   other store.
//!
//! They compose: typical setup is `FsCacheStore<CdnChunkStore<A>>`.

use std::future::Future;

use bytes::Bytes;
use steam_vent_depot::ChunkHash;

use crate::error::Result;

pub mod cache;
pub mod cdn;

pub use cache::FsCacheStore;
pub use cdn::CdnChunkStore;

/// Source of decrypted + decompressed chunk bytes, keyed by SHA-1.
///
/// Implementations are responsible for whatever encryption/decompression
/// the underlying transport needs — the bytes returned here are the raw
/// plaintext content of the chunk.
pub trait ChunkStore: Send + Sync {
    fn get(&self, sha: ChunkHash) -> impl Future<Output = Result<Bytes>> + Send;

    /// Make sure `sha` is present in the store, but don't return the
    /// bytes. Useful for prefetch loops that want to populate a cache
    /// without paying to read every chunk back through memory.
    ///
    /// Default impl falls back to [`get`](Self::get) and discards the
    /// bytes. [`cache::FsCacheStore`] overrides this to short-circuit
    /// when the chunk already exists on disk.
    fn ensure(&self, sha: ChunkHash) -> impl Future<Output = Result<()>> + Send {
        async move {
            self.get(sha).await?;
            Ok(())
        }
    }
}
