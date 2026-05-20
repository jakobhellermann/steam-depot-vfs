//! Mount a Steam depot manifest as a read-only filesystem via FUSE.
//!
//! **Linux only.** Requires `fuse3` (`apt install fuse3` on Debian/Ubuntu).
//!
//! macOS is not supported: fuser doesn't speak fuse-t's wire protocol
//! (see https://github.com/cberner/fuser/issues/273) and macFUSE needs a
//! kext that's hostile on modern macOS.
//!
//! Unmount on Ctrl-C, or `fusermount -u <path>`.

use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use anyhow::Result;
use clap::Parser;
use fuser::experimental::{
    AsyncFilesystem, DirEntListBuilder, GetAttrResponse, LookupResponse, RequestContext,
    TokioAdapter,
};
use fuser::{
    Errno, FileAttr, FileHandle, FileType, Generation, INodeNo, LockOwner, MountOption, OpenFlags,
    experimental,
};
use steam_depot_vfs::{
    AuthSession, DepotAuth, DepotStore, FileKind, VfsError, chunk_store::ChunkStore,
    fs::DepotSnapshot,
};
use steam_vent::Connection;
use steam_vent_depot::{CdnServer, DepotClient};
use tokio::sync::OnceCell;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(about = "Mount a Steam depot manifest as a read-only filesystem")]
struct Cli {
    account: String,
    password: String,
    app_id: u32,
    depot_id: u32,
    manifest_gid: u64,
    /// Where to mount.
    mountpoint: PathBuf,
    #[arg(long, default_value = "public")]
    branch: String,
}

const TTL: Duration = Duration::from_secs(60 * 60);

/// Inodes map 1:1 to `DepotSnapshot::manifest().files`:
/// - inode 1 = synthetic root (no manifest entry)
/// - inode N (N ≥ 2) = `manifest.files[N - 2]`
struct Mount<C: ChunkStore + 'static> {
    fs: DepotSnapshot<C>,
}

const ROOT_INO: INodeNo = INodeNo::ROOT;

impl<C: ChunkStore + 'static> Mount<C> {
    fn new(fs: DepotSnapshot<C>) -> Self {
        Self { fs }
    }

    fn path_for(&self, ino: INodeNo) -> Option<&str> {
        if ino == ROOT_INO {
            return Some("");
        }
        let idx = (ino.0 as usize).checked_sub(2)?;
        self.fs.manifest().files.get(idx).map(|f| f.path.as_str())
    }

    fn ino_for(&self, path: &str) -> Option<INodeNo> {
        if path.is_empty() {
            return Some(ROOT_INO);
        }
        Some(INodeNo((self.fs.index_of(path)? + 2) as u64))
    }

    fn attr_for(&self, ino: INodeNo) -> Option<FileAttr> {
        let path = self.path_for(ino)?;
        if path.is_empty() {
            return Some(dir_attr(ino, 0));
        }
        let meta = self.fs.metadata(path).ok()?;
        Some(match meta.kind {
            FileKind::Directory => dir_attr(ino, meta.size),
            FileKind::File | FileKind::Symlink => file_attr(ino, meta.size),
        })
    }
}

fn dir_attr(ino: INodeNo, size: u64) -> FileAttr {
    FileAttr {
        ino,
        size,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::Directory,
        perm: 0o555,
        nlink: 2,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn file_attr(ino: INodeNo, size: u64) -> FileAttr {
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::RegularFile,
        perm: 0o444,
        nlink: 1,
        uid: 1000,
        gid: 1000,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

#[async_trait::async_trait]
impl<C: ChunkStore + 'static> AsyncFilesystem for Mount<C> {
    async fn lookup(
        &self,
        _ctx: &RequestContext,
        parent: INodeNo,
        name: &OsStr,
    ) -> experimental::Result<LookupResponse> {
        tracing::debug!(?parent, ?name, "lookup");
        let parent_path = self.path_for(parent).ok_or(Errno::ENOENT)?;
        let name = name.to_str().ok_or(Errno::ENOENT)?;
        let child_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{parent_path}/{name}")
        };
        let ino = self.ino_for(&child_path).ok_or(Errno::ENOENT)?;
        let attr = self.attr_for(ino).ok_or(Errno::ENOENT)?;
        Ok(LookupResponse::new(TTL, attr, Generation(0)))
    }

    async fn getattr(
        &self,
        _ctx: &RequestContext,
        ino: INodeNo,
        _fh: Option<FileHandle>,
    ) -> experimental::Result<GetAttrResponse> {
        tracing::debug!(?ino, "getattr");
        let attr = self.attr_for(ino).ok_or(Errno::ENOENT)?;
        Ok(GetAttrResponse::new(TTL, attr))
    }

    async fn readdir(
        &self,
        _ctx: &RequestContext,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut builder: DirEntListBuilder<'_>,
    ) -> experimental::Result<()> {
        tracing::debug!(?ino, offset, "readdir");
        let dir_path = self.path_for(ino).ok_or(Errno::ENOENT)?.to_string();
        let entries = self.fs.list_dir(&dir_path).map_err(|_| Errno::ENOENT)?;

        // Synthetic `.` and `..` come first, then the real entries. `cookie`
        // is the FUSE offset we tell the kernel to resume from on the next
        // call — `offset + 1` of whatever we last added.
        let mut cookie = 0u64;
        let mut add = |child_ino, kind, name: &str| -> bool {
            cookie += 1;
            cookie <= offset || builder.add(child_ino, cookie, kind, name)
        };
        if add(ino, FileType::Directory, ".") {
            return Ok(());
        }
        if add(ino, FileType::Directory, "..") {
            return Ok(());
        }
        for e in entries {
            let child = if dir_path.is_empty() {
                e.name.clone()
            } else {
                format!("{dir_path}/{}", e.name)
            };
            let Some(child_ino) = self.ino_for(&child) else {
                continue;
            };
            let kind = match e.meta.kind {
                FileKind::Directory => FileType::Directory,
                FileKind::Symlink | FileKind::File => FileType::RegularFile,
            };
            if add(child_ino, kind, &e.name) {
                break;
            }
        }
        Ok(())
    }

    async fn read(
        &self,
        _ctx: &RequestContext,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
        out_data: &mut Vec<u8>,
    ) -> experimental::Result<()> {
        tracing::debug!(?ino, offset, size, "read");
        let path = self.path_for(ino).ok_or(Errno::ENOENT)?.to_string();
        // `AsyncFilesystem` has no `readlink`; symlinks are exposed as
        // regular files, so reading one returns the raw bytes from the
        // depot rather than following the link target.
        if let Ok(meta) = self.fs.metadata(&path)
            && matches!(meta.kind, FileKind::Symlink)
        {
            tracing::warn!(
                path = %path,
                target = ?meta.linktarget,
                "reading symlink as regular file; link target not resolved",
            );
        }
        let bytes = self
            .fs
            .read(&path, offset, size as u64)
            .await
            .map_err(|e| {
                tracing::warn!(path = %path, offset, size, %e, "read failed");
                Errno::EIO
            })?;
        out_data.extend_from_slice(&bytes);
        Ok(())
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,steam_depot_vfs=info,fuser=error".into()),
        )
        .init();

    let cli = Cli::parse();

    let store_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("vfs-store");

    let auth = Auth::prepare(cli.account, cli.password, cli.app_id, cli.depot_id).await?;
    let store = DepotStore::new(store_root);
    let snapshot = store
        .open_depot_manifest(
            auth,
            cli.app_id,
            cli.depot_id,
            cli.manifest_gid,
            &cli.branch,
        )
        .await?;
    let mount = Mount::new(snapshot);

    std::fs::create_dir_all(&cli.mountpoint)?;

    tracing::info!(mountpoint = %cli.mountpoint.display(), "mounting");
    let mut cfg = fuser::Config::default();
    cfg.mount_options = vec![
        MountOption::RO,
        MountOption::FSName("steam-depot-vfs".into()),
    ];

    // `Session::spawn` runs the FUSE loop on a dedicated thread and gives us
    // a handle we can use to unmount cleanly on Ctrl-C. `mount2` would also
    // work but gives us no way to trigger unmount.
    let session = fuser::Session::new(TokioAdapter::new(mount), &cli.mountpoint, &cfg)?;
    let bg = session.spawn()?;

    tokio::signal::ctrl_c().await?;
    tracing::info!("ctrl-c received, unmounting");
    bg.umount_and_join()?;
    Ok(())
}

// --- Auth wrapper, identical to examples/cat.rs ---

struct Auth {
    account: String,
    password: String,
    app_id: u32,
    depot_id: u32,
    inner: OnceCell<AuthSession>,
}

impl Auth {
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
            tracing::info!(account, "no refresh token cached, logging in eagerly");
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
}

impl DepotAuth for Auth {
    async fn resolve(&self) -> Result<AuthSession, VfsError> {
        self.inner
            .get_or_try_init(|| {
                authenticate(&self.account, &self.password, self.app_id, self.depot_id)
            })
            .await
            .cloned()
            .map_err(|e: anyhow::Error| VfsError::Other(e.to_string().into()))
    }
}

async fn authenticate(
    account: &str,
    password: &str,
    app_id: u32,
    depot_id: u32,
) -> Result<AuthSession> {
    let connection: Connection = login::establish_connection(account, password).await?;
    let client = Arc::new(DepotClient::new(connection));
    let depot_key = client.depot_key(app_id, depot_id).await?;
    let cdn_servers: Arc<[CdnServer]> = client.cdn_servers().await?.into();
    Ok(AuthSession {
        client,
        depot_key,
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
        let server_list =
            ServerList::discover_with(DiscoverOptions::default().with_cell(4)).await?;
        if let Some(t) = load(account)
            && let Ok(c) = Connection::access(&server_list, account, &t).await
        {
            return Ok(c);
        }
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
        }
        Ok(c)
    }
}
