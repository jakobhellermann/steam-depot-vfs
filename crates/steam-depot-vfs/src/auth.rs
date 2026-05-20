//! Pluggable auth source for talking to Steam.
//!
//! The lib needs a connected [`DepotClient`] and a list of [`CdnServer`]s
//! to fetch from Steam's CDN. Implementations decide *when* to acquire
//! them (eagerly at startup, or lazily on first need) and *how* to cache
//! the result.
//!
//! Per-depot bits (the AES depot key) are not part of auth — they're
//! fetched by [`crate::DepotStore`] using a resolved [`SteamSession`].

use std::future::Future;
use std::sync::Arc;

use steam_vent_depot::{CdnServer, DepotClient};

use crate::error::Result;

/// Resolved Steam connection plus CDN host list. Cheap to clone.
#[derive(Clone)]
pub struct SteamSession {
    pub client: Arc<DepotClient>,
    pub cdn_servers: Arc<[CdnServer]>,
}

/// Source of a [`SteamSession`]. Implementations typically cache the
/// resolved state internally so repeated calls are O(1) after the first.
pub trait SteamAuth: Send + Sync {
    fn resolve(&self) -> impl Future<Output = Result<SteamSession>> + Send;
}
