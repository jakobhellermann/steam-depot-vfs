// TODO(ai-review): review for correctness/style
//! In-memory cache for Steam depot keys.
//!
//! Each depot key (one per `(app_id, depot_id)`) decrypts both the
//! manifest blobs and every chunk in that depot. Hand the same
//! `Arc<OnceCell<DepotKey>>` to the manifest fetch path and to every
//! [`CdnChunkStore`](crate::chunk_store::CdnChunkStore) for the depot
//! so the key is requested at most once per process, regardless of how
//! many manifests or chunks are touched.
//!
//! Keys aren't persisted across restarts — re-fetching one is cheap.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use steam_vent_depot::DepotKey;
use tokio::sync::OnceCell;

use crate::{SteamAuth, VfsError};

#[derive(Default)]
#[allow(clippy::type_complexity)]
pub struct DepotKeyMemoryCache {
    cells: Mutex<HashMap<(u32, u32), Arc<OnceCell<DepotKey>>>>,
}

impl DepotKeyMemoryCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_lazy(&self, app_id: u32, depot_id: u32) -> LazyDepotKey {
        let cell = self
            .cells
            .lock()
            .expect("depot_keys mutex poisoned")
            .entry((app_id, depot_id))
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();
        LazyDepotKey {
            depot_key_cell: cell,
            app_id,
            depot_id,
        }
    }

    #[allow(dead_code)]
    pub async fn get<A: SteamAuth>(
        &self,
        auth: A,
        app_id: u32,
        depot_id: u32,
    ) -> Result<DepotKey, VfsError> {
        let cell = self.get_lazy(app_id, depot_id);
        cell.get(auth).await.cloned()
    }
}

pub struct LazyDepotKey {
    depot_key_cell: Arc<OnceCell<DepotKey>>,
    app_id: u32,
    depot_id: u32,
}
impl LazyDepotKey {
    pub async fn get<A: SteamAuth>(&self, auth: A) -> Result<&DepotKey, VfsError> {
        let ctx = auth.resolve().await?;
        let depot_key = self
            .depot_key_cell
            .get_or_try_init(|| async {
                tracing::info!(self.app_id, self.depot_id, "fetching depot key");
                Ok::<_, crate::VfsError>(ctx.client.depot_key(self.app_id, self.depot_id).await?)
            })
            .await?;

        Ok(depot_key)
    }
}
