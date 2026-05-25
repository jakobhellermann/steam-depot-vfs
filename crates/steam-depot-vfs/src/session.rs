use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use directories::ProjectDirs;
use steam_vent::Connection;
use steam_vent_depot::{CdnServer, DepotClient};
use tokio::sync::OnceCell;

use crate::{SteamAuth, SteamSession, VfsError};

/// Authenticated session that defers `login + cdn_servers` until
/// something actually needs them.
pub struct LazyCachedAuth {
    cache_path: PathBuf,

    account: String,
    password: String,
    inner: OnceCell<SteamSession>,
}

impl LazyCachedAuth {
    pub fn default_refresh_token_cache() -> PathBuf {
        ProjectDirs::from("", "steam-vent", "steam-vent")
            .expect("no cache dir")
            .cache_dir()
            .join("refresh_tokens.json")
    }

    /// Defers login if a refresh token is cached (silent), otherwise logs in
    /// eagerly so any Steam-Guard prompt happens up front instead of mid-run.
    pub async fn prepare(
        cache_path: PathBuf,
        account: String,
        password: String,
    ) -> Result<Self, VfsError> {
        let inner = if login::has_refresh_token(&cache_path, &account) {
            tracing::info!(account, "refresh token cached, auth will run lazily");
            OnceCell::new()
        } else {
            tracing::info!(
                account,
                "no refresh token cached, logging in eagerly (may prompt for steam guard)"
            );
            let ctx = authenticate(&cache_path, &account, &password).await?;
            OnceCell::new_with(Some(ctx))
        };
        Ok(Self {
            cache_path,
            account,
            password,
            inner,
        })
    }
}

impl SteamAuth for LazyCachedAuth {
    async fn resolve(&self) -> Result<SteamSession, VfsError> {
        self.inner
            .get_or_try_init(|| authenticate(&self.cache_path, &self.account, &self.password))
            .await
            .map_err(|e| VfsError::Other(e.to_string().into()))
            .cloned()
    }
}

async fn authenticate(
    cache_path: &Path,
    account: &str,
    password: &str,
) -> Result<SteamSession, VfsError> {
    tracing::info!("establishing connection");
    let connection: Connection = login::establish_connection(cache_path, account, password).await?;
    let client = DepotClient::new(connection);
    tracing::info!("discovering cdn servers");
    let cdn_servers: Arc<[CdnServer]> = client.cdn_servers().await?.into();
    tracing::info!(count = cdn_servers.len(), "got cdn servers");
    Ok(SteamSession {
        client,
        cdn_servers,
    })
}

pub mod login {
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};

    use directories::ProjectDirs;
    use steam_vent::auth::{
        AuthConfirmationHandler, ConsoleAuthConfirmationHandler, DeviceConfirmationHandler,
        FileGuardDataStore,
    };
    use steam_vent::{Connection, DiscoverOptions, ServerList};

    use crate::VfsError;

    /// Cache path in steam-vent cache dir. Useful for for examples sharing authentication.
    pub fn default_refresh_token_path() -> PathBuf {
        ProjectDirs::from("", "steam-vent", "steam-vent")
            .expect("no cache dir")
            .cache_dir()
            .join("refresh_tokens.json")
    }

    fn load(cache_path: &Path, account: &str) -> Option<String> {
        let raw = fs::read_to_string(cache_path).ok()?;
        let map: HashMap<String, String> = serde_json::from_str(&raw).ok()?;
        map.get(account).cloned().filter(|t| !t.is_empty())
    }
    pub fn has_refresh_token(cache_path: &Path, account: &str) -> bool {
        load(cache_path, account).is_some()
    }
    fn save(cache_path: &Path, account: &str, token: &str) -> Result<(), std::io::Error> {
        if let Some(p) = cache_path.parent() {
            fs::create_dir_all(p)?;
        }
        let mut map: HashMap<String, String> = fs::read_to_string(cache_path)
            .ok()
            .and_then(|r| serde_json::from_str(&r).ok())
            .unwrap_or_default();
        map.insert(account.into(), token.into());
        fs::write(cache_path, serde_json::to_string(&map)?)?;
        Ok(())
    }
    pub async fn establish_connection(
        cache_path: &Path,
        account: &str,
        password: &str,
    ) -> Result<Connection, VfsError> {
        tracing::info!(account, "discovering steam servers");
        let server_list =
            // Cell IDs are Steam's geographic regions. 4 = Frankfurt (DE).
            // Full list: https://raw.githubusercontent.com/SteamDatabase/SteamTracking/6d23ebb0070998ae851278cfae5f38832f4ac28d/ClientExtracted/steam/cached/CellMap.vdf
            ServerList::discover_with(DiscoverOptions::default().with_cell(4)).await.map_err(VfsError::ServerDiscoveryError)?;
        if let Some(t) = load(cache_path, account) {
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
        .await
        .map_err(VfsError::ConnectionError)?;
        if let Some(t) = c.access_token() {
            save(cache_path, account, t)?;
            tracing::info!(account, "saved fresh refresh token");
        }
        Ok(c)
    }
}
