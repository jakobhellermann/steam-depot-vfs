// TODO(ai-review): review for correctness/style
//! TOML config for the `steam-depot` binary.
//!
//! Example:
//! ```toml
//! mountpoint = "/mnt/steam"
//! store_root = "/var/lib/steam-depot"
//!
//! [steam]
//! account = "myaccount"
//! # password optional; refresh token cache preferred
//! password = ""
//!
//! [[depot]]
//! app_id = 1030300
//! depot_id = 1030303
//! manifests = [
//!     "7921642076658611197",            # public branch
//!     "3678462974375346661:public-beta", # explicit branch
//! ]
//! ```

use std::path::PathBuf;

use anyhow::Context;
use serde::Deserialize;

/// Public, ergonomic shape used by the rest of the binary. Built from
/// [`RawConfig`] which mirrors the on-disk TOML.
#[derive(Debug)]
pub struct Config {
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub mountpoint: PathBuf,
    pub store_root: PathBuf,
    pub steam: Steam,
    pub manifests: Vec<Manifest>,
}

#[derive(Debug, Deserialize)]
pub struct Steam {
    pub account: String,
    #[serde(default)]
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct Manifest {
    pub app_id: u32,
    pub depot_id: u32,
    pub gid: u64,
    pub branch: String,
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    mountpoint: PathBuf,
    store_root: PathBuf,
    steam: Steam,
    #[serde(default, rename = "depot")]
    depots: Vec<RawDepot>,
}

#[derive(Debug, Deserialize)]
struct RawDepot {
    app_id: u32,
    depot_id: u32,
    #[serde(default)]
    manifests: Vec<String>,
}

const DEFAULT_BRANCH: &str = "public";

impl Config {
    pub fn from_file(path: &std::path::Path) -> anyhow::Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let raw: RawConfig =
            toml::from_str(&raw).with_context(|| format!("parsing {}", path.display()))?;

        let mut manifests = Vec::new();
        for d in &raw.depots {
            for entry in &d.manifests {
                manifests.push(parse_manifest_entry(d.app_id, d.depot_id, entry)?);
            }
        }
        // Relative paths in the config resolve against the config file's
        // parent directory, so the same config works from any CWD.
        let base = path.parent().unwrap_or(std::path::Path::new(""));
        Ok(Config {
            mountpoint: resolve(base, raw.mountpoint),
            store_root: resolve(base, raw.store_root),
            steam: raw.steam,
            manifests,
        })
    }
}

fn resolve(base: &std::path::Path, p: PathBuf) -> PathBuf {
    if p.is_absolute() { p } else { base.join(p) }
}

/// Parse a `"<gid>"` or `"<gid>:<branch>"` string from a `manifests`
/// list into a fully resolved [`Manifest`].
fn parse_manifest_entry(app_id: u32, depot_id: u32, raw: &str) -> anyhow::Result<Manifest> {
    let (gid_str, branch) = match raw.split_once(':') {
        Some((g, b)) => (g, b.to_string()),
        None => (raw, DEFAULT_BRANCH.to_string()),
    };
    let gid: u64 = gid_str
        .parse()
        .with_context(|| format!("invalid manifest gid {gid_str:?}"))?;
    Ok(Manifest {
        app_id,
        depot_id,
        gid,
        branch,
    })
}
