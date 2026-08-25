// TODO(ai-review): review for correctness/style
//! Mount one real depot manifest over NFS and report what a read of it
//! costs — how many chunks left the CDN, how many bytes, how long.
//!
//! Logs in with the refresh token `steam-vent` already cached, so it
//! runs without a password and without a Steam Guard prompt.
//!
//! ```text
//! cargo run -p steam-depot-mount --bin nfs-live --features probe -- \
//!     <account> <app_id> <depot_id> <manifest_gid> <path-in-depot> [mountpoint]
//! ```
//!
//! `STEAM_VFS_STORE` picks the store root; it defaults to a directory of
//! its own so a run starts cold instead of reading someone's cache.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use bytes::Bytes;
use steam_depot_mount::{NfsMount, NfsMountConfig};
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::session::LazyCachedAuth;
use steam_depot_vfs::{ChunkHash, DepotStore, VfsError};
use tracing_subscriber::EnvFilter;

#[derive(Default, Clone)]
struct Counters {
    fetches: usize,
    bytes: u64,
    inflight: usize,
    max_inflight: usize,
}

/// Wraps the CDN store, which is the only place real network traffic
/// happens — disk-cache hits never reach it.
struct CountingCdn<Inner> {
    inner: Inner,
    counters: Arc<Mutex<Counters>>,
}

impl<Inner: ChunkStore> ChunkStore for CountingCdn<Inner> {
    async fn get(&self, sha: ChunkHash) -> Result<Bytes, VfsError> {
        {
            let mut c = self.counters.lock().expect("counters");
            c.fetches += 1;
            c.inflight += 1;
            c.max_inflight = c.max_inflight.max(c.inflight);
        }
        let result = self.inner.get(sha).await;
        let mut c = self.counters.lock().expect("counters");
        c.inflight -= 1;
        if let Ok(bytes) = &result {
            c.bytes += bytes.len() as u64;
        }
        result
    }
}

struct Args {
    account: String,
    app_id: u32,
    depot_id: u32,
    manifest_gid: u64,
    path: String,
    mountpoint: PathBuf,
    /// Read only this many bytes from the start, to see what a partial
    /// read of a big file costs.
    bytes: Option<u64>,
}

fn args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let mut next = |what: &str| it.next().ok_or_else(|| format!("missing {what}"));
    let account = next("account")?;
    let app_id = next("app_id")?
        .parse()
        .map_err(|e| format!("app_id: {e}"))?;
    let depot_id = next("depot_id")?
        .parse()
        .map_err(|e| format!("depot_id: {e}"))?;
    let manifest_gid = next("manifest_gid")?
        .parse()
        .map_err(|e| format!("manifest_gid: {e}"))?;
    let path = next("path-in-depot")?;
    let mountpoint = it
        .next()
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("nfs-live"));
    let bytes = it.next().and_then(|a| a.parse().ok());
    Ok(Args {
        account,
        app_id,
        depot_id,
        manifest_gid,
        path,
        mountpoint,
        bytes,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = match args() {
        Ok(args) => args,
        Err(e) => {
            eprintln!(
                "{e}\nusage: nfs-live <account> <app_id> <depot_id> <manifest_gid> <path> [mountpoint]"
            );
            std::process::exit(2);
        }
    };

    let store_root = std::env::var_os("STEAM_VFS_STORE")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("nfs-live-store"));
    println!("store:      {}", store_root.display());
    println!("mountpoint: {}", args.mountpoint.display());

    let auth = Arc::new(
        LazyCachedAuth::prepare(
            LazyCachedAuth::default_refresh_token_cache(),
            args.account,
            String::new(),
        )
        .await?,
    );
    let store = Arc::new(DepotStore::new(store_root));
    let counters = Arc::new(Mutex::new(Counters::default()));

    let mount = NfsMount::start(NfsMountConfig::new(args.mountpoint.clone())).await?;
    let opened = Arc::new(Mutex::new(0usize));
    mount.add_lazy(
        args.app_id,
        args.depot_id,
        args.manifest_gid,
        {
            let (store, auth, counters, opened) = (
                Arc::clone(&store),
                Arc::clone(&auth),
                Arc::clone(&counters),
                Arc::clone(&opened),
            );
            move || {
                let (store, auth, counters, opened) = (
                    Arc::clone(&store),
                    Arc::clone(&auth),
                    Arc::clone(&counters),
                    Arc::clone(&opened),
                );
                async move {
                    *opened.lock().expect("opened") += 1;
                    store
                        .open_depot_manifest_with_chunks(
                            auth,
                            args.app_id,
                            args.depot_id,
                            args.manifest_gid,
                            "public",
                            |cdn| CountingCdn {
                                inner: cdn,
                                counters,
                            },
                        )
                        .await
                        .map_err(|e| std::io::Error::other(e.to_string()))
                }
            }
        },
        None,
    )?;

    let file = args
        .mountpoint
        .join(format!(
            "{}/{}/{}",
            args.app_id, args.depot_id, args.manifest_gid
        ))
        .join(&args.path);

    // `-` as the path means "just mount and stay", for poking at the
    // mount with other tools.
    if args.path == "-" {
        println!("mounted, ctrl-c to unmount:");
        println!(
            "{}",
            args.mountpoint
                .join(format!(
                    "{}/{}/{}",
                    args.app_id, args.depot_id, args.manifest_gid
                ))
                .display()
        );
        tokio::signal::ctrl_c().await?;
        mount.unmount().await?;
        return Ok(());
    }

    for pass in ["cold", "second"] {
        let before = counters.lock().expect("counters").clone();
        let started = Instant::now();
        let read = tokio::task::spawn_blocking({
            let file = file.clone();
            let want = args.bytes;
            move || -> std::io::Result<usize> {
                use std::io::Read;
                let mut f = std::fs::File::open(&file)?;
                match want {
                    Some(n) => {
                        let mut buf = vec![0; n as usize];
                        f.read_exact(&mut buf)?;
                        Ok(buf.len())
                    }
                    None => {
                        let mut buf = Vec::new();
                        f.read_to_end(&mut buf)?;
                        Ok(buf.len())
                    }
                }
            }
        })
        .await??;
        let took = started.elapsed();
        // Readahead fetches outlive the read that triggered them, and a
        // fetch counts its bytes when it finishes — so snapshotting here
        // would report the requests without their traffic.
        let drain = Instant::now();
        while counters.lock().expect("counters").inflight > 0 {
            if drain.elapsed() > std::time::Duration::from_secs(120) {
                eprintln!("gave up waiting for readahead fetches to finish");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let after = counters.lock().expect("counters").clone();
        println!(
            "{pass:<7} {read} bytes in {took:.2?} ({:.1} MB/s) — {} cdn fetches, {:.1} MiB fetched (incl. readahead), up to {} in flight",
            read as f64 / 1e6 / took.as_secs_f64(),
            after.fetches - before.fetches,
            (after.bytes - before.bytes) as f64 / (1 << 20) as f64,
            after.max_inflight,
        );
    }
    println!("manifest opens: {}", opened.lock().expect("opened"));

    mount.unmount().await?;
    Ok(())
}
