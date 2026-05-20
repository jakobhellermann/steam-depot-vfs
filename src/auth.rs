//! Pluggable auth source for talking to Steam.
//!
//! The lib needs three things to fetch from a depot's CDN: an authenticated
//! [`DepotClient`], the depot's [`DepotKey`], and a list of [`CdnServer`]s.
//! Implementations decide *when* to acquire them — eagerly at startup, or
//! lazily on first need — and *how* to cache the result.

use std::future::Future;
use std::sync::Arc;

use steam_vent_depot::{CdnServer, DepotClient, DepotKey};

use crate::error::Result;

/// Resolved authenticated session. All fields are cheap to clone (Arc or
/// 32-byte key), so the lib doesn't sweat re-cloning across many chunk
/// fetches.
#[derive(Clone)]
pub struct AuthSession {
    pub client: Arc<DepotClient>,
    pub depot_key: DepotKey,
    pub cdn_servers: Arc<[CdnServer]>,
}

/// Source of an [`AuthSession`]. Implementations typically cache the resolved
/// state internally so repeated calls are O(1) after the first.
pub trait DepotAuth: Send + Sync {
    fn resolve(&self) -> impl Future<Output = Result<AuthSession>> + Send;
}
