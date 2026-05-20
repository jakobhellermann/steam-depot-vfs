//! Browse a Steam depot manifest like a filesystem.
//!
//! Usage:
//!   cargo run --example cat -- <user> <pw> <app_id> <depot_id> <manifest_gid> ls [<path>]
//!   cargo run --example cat -- <user> <pw> <app_id> <depot_id> <manifest_gid> cat <path>
//!
//! `ls` defaults to the root directory if no path is given.
//!
//! Example:
//!   cargo run --example cat -- USER PW 1030300 1030303 7921642076658611197 ls
//!   cargo run --example cat -- USER PW 1030300 1030303 7921642076658611197 cat \
//!       "Hollow Knight Silksong_Data/StreamingAssets/aa/AddressablesLink/link.xml"

use std::env::args;
use std::future::Future;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use steam_depot_vfs::{
    ChunkSha, ChunkStore, DepotFs, FileKind, FsCacheStore, ManifestCache, SteamCdnChunkStore,
};
use steam_vent::Connection;
use steam_vent_depot::{CdnServer, DepotClient, DepotKey, Manifest};
use tokio::sync::OnceCell;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut args = args().skip(1);
    let account = args.next().context("missing username")?;
    let password = args.next().context("missing password")?;
    let app_id: u32 = args
        .next()
        .context("missing app_id")?
        .parse()
        .context("app_id must be a number")?;
    let depot_id: u32 = args
        .next()
        .context("missing depot_id")?
        .parse()
        .context("depot_id must be a number")?;
    let manifest_gid: u64 = args
        .next()
        .context("missing manifest_gid")?
        .parse()
        .context("manifest_gid must be a number")?;
    let cmd = args.next().context("missing subcommand (ls|cat)")?;
    let path = args.next();
    let branch = std::env::var("BRANCH").unwrap_or_else(|_| "public".into());

    let store_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("vfs-store");
    let manifest_cache = ManifestCache::new(store_root.join("manifests"));

    let auth = Auth::prepare(account, password, app_id, depot_id).await?;

    let manifest = match manifest_cache.load(depot_id, manifest_gid)? {
        Some(m) => m,
        None => {
            let ctx = auth.get().await?;
            tracing::info!(depot_id, manifest_gid, branch, "fetching manifest");
            let code = ctx
                .client
                .manifest_request_code(app_id, depot_id, manifest_gid, &branch)
                .await?;
            let m = ctx
                .client
                .fetch_manifest(
                    &ctx.cdn_servers,
                    depot_id,
                    manifest_gid,
                    code,
                    &ctx.depot_key,
                )
                .await?;
            manifest_cache.save(&m)?;
            m
        }
    };

    let manifest = Arc::new(manifest);
    let lazy_cdn = CdnStore::new(Arc::clone(&auth), depot_id, Arc::clone(&manifest));
    let store = FsCacheStore::new(lazy_cdn, store_root.join("chunks"));

    let fs = DepotFs::new(
        Arc::try_unwrap(manifest).unwrap_or_else(|a| (*a).clone()),
        store,
    );

    match cmd.as_str() {
        "ls" => {
            let p = path.as_deref().unwrap_or("/");
            let mut entries = fs.list_dir(p)?;
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            for e in entries {
                let marker = match e.meta.kind {
                    FileKind::Directory => "d",
                    FileKind::Symlink => "l",
                    FileKind::File => "f",
                };
                let size = if matches!(e.meta.kind, FileKind::File) {
                    e.meta.size.to_string()
                } else {
                    "-".into()
                };
                println!("{marker} {:>12} {}", size, e.name);
            }
        }
        "cat" => {
            let p = path.context("cat requires a file path")?;
            let bytes = fs.read_full(&p).await?;
            std::io::stdout().write_all(&bytes)?;
        }
        other => anyhow::bail!("unknown subcommand: {other} (expected ls|cat)"),
    }
    Ok(())
}

/// Authenticated session that defers `login + depot_key + cdn_servers` until
/// something actually needs them.
struct Auth {
    account: String,
    password: String,
    app_id: u32,
    depot_id: u32,
    inner: OnceCell<AuthCtx>,
}

struct AuthCtx {
    client: Arc<DepotClient>,
    depot_key: DepotKey,
    cdn_servers: Vec<CdnServer>,
}

impl Auth {
    /// Defers login if a refresh token is cached (silent), otherwise logs in
    /// eagerly so any Steam-Guard prompt happens up front instead of mid-run.
    async fn prepare(
        account: String,
        password: String,
        app_id: u32,
        depot_id: u32,
    ) -> Result<Arc<Self>> {
        let inner = if login::has_refresh_token(&account) {
            tracing::info!(account, "refresh token cached, auth will run lazily");
            OnceCell::new()
        } else {
            tracing::info!(
                account,
                "no refresh token cached, logging in eagerly (may prompt for steam guard)"
            );
            let ctx = authenticate(&account, &password, app_id, depot_id).await?;
            OnceCell::new_with(Some(ctx))
        };
        Ok(Arc::new(Self {
            account,
            password,
            app_id,
            depot_id,
            inner,
        }))
    }

    async fn get(&self) -> Result<&AuthCtx> {
        self.inner
            .get_or_try_init(|| {
                authenticate(&self.account, &self.password, self.app_id, self.depot_id)
            })
            .await
    }
}

async fn authenticate(
    account: &str,
    password: &str,
    app_id: u32,
    depot_id: u32,
) -> Result<AuthCtx> {
    tracing::info!("establishing connection");
    let connection: Connection = login::establish_connection(account, password).await?;
    let client = Arc::new(DepotClient::new(connection));
    tracing::info!(app_id, depot_id, "fetching depot key");
    let depot_key = client.depot_key(app_id, depot_id).await?;
    tracing::info!("discovering cdn servers");
    let cdn_servers = client.cdn_servers().await?;
    tracing::info!(count = cdn_servers.len(), "got cdn servers");
    Ok(AuthCtx {
        client,
        depot_key,
        cdn_servers,
    })
}

/// `ChunkStore` that lazily builds its inner `SteamCdnChunkStore` on the first
/// miss. Until something actually needs a chunk, no Steam calls happen.
struct CdnStore {
    auth: Arc<Auth>,
    depot_id: u32,
    manifest: Arc<Manifest>,
    inner: OnceCell<SteamCdnChunkStore>,
}

impl CdnStore {
    fn new(auth: Arc<Auth>, depot_id: u32, manifest: Arc<Manifest>) -> Self {
        Self {
            auth,
            depot_id,
            manifest,
            inner: OnceCell::new(),
        }
    }
}

impl ChunkStore for CdnStore {
    fn get(
        &self,
        sha: ChunkSha,
    ) -> impl Future<Output = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> + Send {
        async move {
            let inner = self
                .inner
                .get_or_try_init(|| async {
                    let ctx = self.auth.get().await.map_err(
                        |e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() },
                    )?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(SteamCdnChunkStore::new(
                        Arc::clone(&ctx.client),
                        ctx.cdn_servers.clone(),
                        self.depot_id,
                        ctx.depot_key.clone(),
                        &self.manifest,
                    ))
                })
                .await?;
            inner.get(sha).await
        }
    }
}

mod login {
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;

    use anyhow::Result;
    use directories::ProjectDirs;
    use steam_vent::auth::{
        AuthConfirmationHandler, ConsoleAuthConfirmationHandler, DeviceConfirmationHandler,
        FileGuardDataStore,
    };
    use steam_vent::{Connection, DiscoverOptions, ServerList};

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
    pub fn has_refresh_token(account: &str) -> bool {
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
    pub async fn establish_connection(account: &str, password: &str) -> Result<Connection> {
        tracing::info!(account, "discovering steam servers");
        let server_list =
            ServerList::discover_with(DiscoverOptions::default().with_cell(4)).await?;
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
}
