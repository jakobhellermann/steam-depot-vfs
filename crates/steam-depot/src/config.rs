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
//! [[manifest]]
//! app_id = 1030300
//! depot_id = 1030303
//! gid = 7921642076658611197
//!
//! [[manifest]]
//! app_id = 1030300
//! depot_id = 1030303
//! gid = 4789012345678901234
//! branch = "public" # optional, defaults to "public"
//! ```

use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub mountpoint: PathBuf,
    pub store_root: PathBuf,
    pub steam: Steam,
    #[serde(default, rename = "manifest")]
    pub manifests: Vec<Manifest>,
}

#[derive(Debug, Deserialize)]
pub struct Steam {
    pub account: String,
    #[serde(default)]
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub app_id: u32,
    pub depot_id: u32,
    pub gid: u64,
    #[serde(default = "default_branch")]
    pub branch: String,
}

fn default_branch() -> String {
    "public".into()
}

impl Config {
    pub fn from_file(path: &std::path::Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        let cfg: Config =
            toml::from_str(&raw).map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;
        Ok(cfg)
    }
}
