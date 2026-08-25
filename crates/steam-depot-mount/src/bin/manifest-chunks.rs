// TODO(ai-review): review for correctness/style
//! Print `sha path offset size` for every chunk of a manifest, so a set
//! of chunk files can be traced back to the files they belong to.
//!
//! Reads the cached manifest; no login happens unless it has to be
//! fetched.
//!
//! ```text
//! cargo run -p steam-depot-mount --bin manifest-chunks --features probe -- \
//!     <account> <app_id> <depot_id> <manifest_gid>
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use steam_depot_vfs::DepotStore;
use steam_depot_vfs::session::LazyCachedAuth;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let account = args.next().ok_or("missing account")?;
    let app_id: u32 = args.next().ok_or("missing app_id")?.parse()?;
    let depot_id: u32 = args.next().ok_or("missing depot_id")?.parse()?;
    let manifest_gid: u64 = args.next().ok_or("missing manifest_gid")?.parse()?;

    let store_root = std::env::var_os("STEAM_VFS_STORE")
        .map(PathBuf::from)
        .ok_or("set STEAM_VFS_STORE")?;
    let auth = Arc::new(
        LazyCachedAuth::prepare(
            LazyCachedAuth::default_refresh_token_cache(),
            account,
            String::new(),
        )
        .await?,
    );
    let fs = DepotStore::new(store_root)
        .open_depot_manifest(auth, app_id, depot_id, manifest_gid, "public")
        .await?;

    for file in &fs.manifest().files {
        for chunk in &file.chunks {
            println!(
                "{} {} {} {}",
                chunk.sha, chunk.offset, chunk.size_uncompressed, file.path
            );
        }
    }
    Ok(())
}
