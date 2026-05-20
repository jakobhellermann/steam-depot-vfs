// TODO(ai-review): review for correctness/style
//! Browse a Steam depot manifest like a filesystem.

use std::future::Future;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, Subcommand};
use steam_depot_vfs::{
    DepotStore, FileKind, SteamAuth, SteamSession, VfsError, chunk_store::ChunkStore,
    fs::DepotSnapshot,
};
use steam_vent::Connection;
use steam_vent_depot::{CdnServer, DepotClient};
use tokio::sync::OnceCell;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(about = "Browse a Steam depot manifest like a filesystem")]
struct Cli {
    /// Steam account name.
    account: String,
    /// Steam password (ignored if a refresh token for this account is cached).
    password: String,
    /// Steam app id (e.g. 1030300 for Silksong).
    app_id: u32,
    /// Depot id within the app (e.g. 1030303 for the Linux depot).
    depot_id: u32,
    /// Manifest GID — look these up on SteamDB.
    manifest_gid: u64,
    /// Branch the manifest belongs to; mostly only matters for restricted branches.
    #[arg(long, default_value = "public")]
    branch: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// List a directory in the depot.
    Ls {
        /// Directory path; defaults to the root.
        #[arg(default_value = "/")]
        path: String,
    },
    /// Stream a single file from the depot to stdout.
    Cat {
        /// File path inside the depot.
        path: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let cli = Cli::parse();

    let store_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/vfs-store");

    let auth = Auth::prepare(cli.account, cli.password).await?;
    let vfs = DepotStore::new(store_root);
    let fs = vfs
        .open_depot_manifest(
            auth,
            cli.app_id,
            cli.depot_id,
            cli.manifest_gid,
            &cli.branch,
        )
        .await?;

    match cli.cmd {
        Cmd::Ls { path } => ls(&fs, &path)?,
        Cmd::Cat { path } => cat(&fs, &path).await?,
    }
    Ok(())
}

fn ls(fs: &DepotSnapshot<impl ChunkStore>, path: &str) -> Result<()> {
    let mut entries = fs.list_dir(path)?;
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    for e in entries {
        let marker = match e.meta.kind {
            FileKind::Directory => "d",
            FileKind::Symlink => "l",
            FileKind::File => "f",
        };
        let size = if matches!(e.meta.kind, FileKind::File) {
            human_bytes(e.meta.size)
        } else {
            "-".into()
        };
        println!("{marker} {:>10} {}", size, e.name);
    }
    Ok(())
}

fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

async fn cat(fs: &DepotSnapshot<impl ChunkStore>, path: &str) -> Result<()> {
    let bytes = fs.read_full(path).await?;
    std::io::stdout().write_all(&bytes)?;
    Ok(())
}

/// Authenticated session that defers `login + cdn_servers` until
/// something actually needs them.
struct Auth {
    account: String,
    password: String,
    inner: OnceCell<SteamSession>,
}

impl Auth {
    /// Defers login if a refresh token is cached (silent), otherwise logs in
    /// eagerly so any Steam-Guard prompt happens up front instead of mid-run.
    async fn prepare(account: String, password: String) -> Result<Arc<Self>> {
        let inner = if login::has_refresh_token(&account) {
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
    let connection: Connection = login::establish_connection(account, password).await?;
    let client = Arc::new(DepotClient::new(connection));
    tracing::info!("discovering cdn servers");
    let cdn_servers: Arc<[CdnServer]> = client.cdn_servers().await?.into();
    tracing::info!(count = cdn_servers.len(), "got cdn servers");
    Ok(SteamSession {
        client,
        cdn_servers,
    })
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
