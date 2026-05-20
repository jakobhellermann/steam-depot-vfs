// TODO(ai-review): review for correctness/style
//! Lazy auth wrapper that caches a Steam refresh token on disk so the
//! daemon can resume without re-prompting after a restart.

use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use directories::ProjectDirs;
use steam_depot_vfs::{SteamAuth, SteamSession, VfsError};
use steam_vent::auth::{
    AuthConfirmationHandler, ConsoleAuthConfirmationHandler, DeviceConfirmationHandler,
    FileGuardDataStore,
};
use steam_vent::{Connection, DiscoverOptions, ServerList};
use steam_vent_depot::{CdnServer, DepotClient};
use tokio::sync::OnceCell;

/// Caches the [`SteamSession`] after the first resolve. Lazy: if a
/// refresh token is on disk we don't even attempt a connection until
/// the first time something asks for it.
pub struct Auth {
    account: String,
    password: String,
    inner: OnceCell<SteamSession>,
}

impl Auth {
    /// Build an [`Auth`]. Logs in eagerly if no refresh token is cached
    /// (so any Steam-Guard prompt happens before we daemonise);
    /// otherwise defers everything until first use.
    pub async fn prepare(account: String, password: String) -> Result<Arc<Self>> {
        let inner = if has_refresh_token(&account) {
            tracing::info!(account, "refresh token cached, auth will run lazily");
            OnceCell::new()
        } else {
            tracing::info!(
                account,
                "no refresh token cached, logging in eagerly (may prompt for steam guard)"
            );
            let ctx = authenticate(&account, &password).await?;
            OnceCell::new_with(Some(ctx))
        };
        Ok(Arc::new(Self {
            account,
            password,
            inner,
        }))
    }
}

impl SteamAuth for Auth {
    fn resolve(&self) -> impl Future<Output = Result<SteamSession, VfsError>> + Send {
        async move {
            self.inner
                .get_or_try_init(|| authenticate(&self.account, &self.password))
                .await
                .cloned()
                .map_err(|e: anyhow::Error| VfsError::Other(e.to_string().into()))
        }
    }
}

async fn authenticate(account: &str, password: &str) -> Result<SteamSession> {
    tracing::info!("establishing connection");
    let connection: Connection = establish_connection(account, password).await?;
    let client = Arc::new(DepotClient::new(connection));
    tracing::info!("discovering cdn servers");
    let cdn_servers: Arc<[CdnServer]> = client.cdn_servers().await?.into();
    tracing::info!(count = cdn_servers.len(), "got cdn servers");
    Ok(SteamSession {
        client,
        cdn_servers,
    })
}

/// Deliberately namespaced under `steam-vent`, not `steam-depot`, so we
/// share the refresh-token cache with the `cat` example (and any other
/// steam-vent-based tool run against the same Steam account).
fn refresh_token_path() -> PathBuf {
    ProjectDirs::from("", "steam-vent", "steam-vent")
        .expect("no cache dir")
        .cache_dir()
        .join("refresh_tokens.json")
}

fn load(account: &str) -> Option<String> {
    let raw = fs::read_to_string(refresh_token_path()).ok()?;
    let map: HashMap<String, String> = serde_json::from_str(&raw).ok()?;
    map.get(account).cloned().filter(|t| !t.is_empty())
}

fn has_refresh_token(account: &str) -> bool {
    load(account).is_some()
}

fn save(account: &str, token: &str) -> Result<()> {
    let path = refresh_token_path();
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?;
    }
    let mut map: HashMap<String, String> = fs::read_to_string(&path)
        .ok()
        .and_then(|r| serde_json::from_str(&r).ok())
        .unwrap_or_default();
    map.insert(account.into(), token.into());
    fs::write(&path, serde_json::to_string(&map)?)?;
    Ok(())
}

async fn establish_connection(account: &str, password: &str) -> Result<Connection> {
    tracing::info!(account, "discovering steam servers");
    // Cell IDs are Steam's geographic regions. 4 = Frankfurt (DE).
    // Full list: https://raw.githubusercontent.com/SteamDatabase/SteamTracking/6d23ebb0070998ae851278cfae5f38832f4ac28d/ClientExtracted/steam/cached/CellMap.vdf
    let server_list = ServerList::discover_with(DiscoverOptions::default().with_cell(4)).await?;
    if let Some(t) = load(account) {
        tracing::info!(account, "trying cached refresh token");
        match Connection::access(&server_list, account, &t).await {
            Ok(c) => {
                tracing::info!(account, "logged in via cached refresh token");
                return Ok(c);
            }
            Err(e) => tracing::warn!(
                ?e,
                "cached refresh token rejected, falling back to password login"
            ),
        }
    }
    tracing::info!(account, "logging in with password");
    let c = Connection::login(
        &server_list,
        account,
        password,
        FileGuardDataStore::user_cache(),
        ConsoleAuthConfirmationHandler::default().or(DeviceConfirmationHandler),
    )
    .await?;
    if let Some(t) = c.access_token() {
        save(account, t)?;
        tracing::info!(account, "saved fresh refresh token");
    }
    Ok(c)
}
