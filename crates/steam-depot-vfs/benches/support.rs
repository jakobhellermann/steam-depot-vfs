// TODO(ai-review): review for style and correctness
//! Codec helpers shared by the store benchmarks (`benches/store.rs`,
//! `benches/store_e2e.rs`) and their fixture bootstrap
//! (`examples/bench_setup.rs`).
//!
//! The chunk container handling mirrors the private decode logic of the
//! steam-vent-depot fork. It lives in dev-land first because these
//! benchmarks decide which decoder the compressed-store rebuild should use.

// Both consumers compile this module but use different subsets of it.
#![allow(dead_code)]

use std::io::{Cursor, Read};
use std::path::PathBuf;

use aes::Aes256;
use aes::cipher::block_padding::Pkcs7;
use aes::cipher::{Array, BlockCipherDecrypt, BlockModeDecrypt, KeyInit, KeyIvInit};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

/// Fixture dir for the benchmarks; overridable via `BENCH_FIXTURES`.
pub fn fixture_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("BENCH_FIXTURES") {
        return PathBuf::from(dir);
    }
    directories::ProjectDirs::from("", "", "steam-depot-vfs")
        .expect("no home directory")
        .cache_dir()
        .join("bench")
}

/// The steam-multiversion-viewer chunk store; overridable via `STORE_ROOT`.
pub fn store_root() -> PathBuf {
    if let Ok(dir) = std::env::var("STORE_ROOT") {
        return PathBuf::from(dir);
    }
    directories::ProjectDirs::from("", "", "steam-multiversion-viewer")
        .expect("no home directory")
        .data_dir()
        .join("store")
}

/// Env variable parsed as `T`, falling back to `default` when unset or
/// non-UTF-8. A malformed value is an error, not silently the default.
pub fn env_parsed<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(v) => v.parse().map_err(|e| anyhow::anyhow!("env {name}: {e}")),
        Err(_) => Ok(default),
    }
}

/// `index.json` written by `bench_setup`, read by the benchmarks.
#[derive(Serialize, Deserialize)]
pub struct FixtureIndex {
    pub app_id: u32,
    pub depot_id: u32,
    pub manifest_gid: u64,
    pub chunks: Vec<FixtureChunk>,
}

#[derive(Serialize, Deserialize)]
pub struct FixtureChunk {
    pub sha: String,
    pub codec: String,
    pub size_uncompressed: u64,
    /// CDN wire form: AES-CBC ciphertext.
    pub raw_len: u64,
    /// Decrypted container: the bytes a compressed store would persist.
    pub container_len: u64,
    /// zstd frame sizes of the plaintext at levels 1/3/9/19 — the
    /// "recompress everything as zstd" storage option.
    pub zstd_sizes: [u64; 4],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    /// `VZa`: LZMA in a Valve header/footer.
    Lzma,
    /// `VSZa`: zstd in a Valve header/footer.
    Zstd,
    /// `PK\x03\x04`: single-entry zip, used for chunks too small for
    /// LZMA/zstd to pay off.
    Zip,
}

impl Codec {
    pub fn as_str(self) -> &'static str {
        match self {
            Codec::Lzma => "lzma",
            Codec::Zstd => "zstd",
            Codec::Zip => "zip",
        }
    }
}

/// AES-256-CBC decryption with Steam's IV trick: the first 16 ciphertext
/// bytes, ECB-decrypted, form the IV for the rest. Throughput is independent
/// of key material, so benchmarks pass a dummy key.
pub fn decrypt_chunk(raw: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    if raw.len() < 32 || !(raw.len() - 16).is_multiple_of(16) {
        bail!("ciphertext of {} bytes is not chunk-shaped", raw.len());
    }
    let cipher = Aes256::new(key.into());
    let mut iv: Array<u8, _> = Array::from(<[u8; 16]>::try_from(&raw[..16]).unwrap());
    cipher.decrypt_block(&mut iv);
    type Aes256CbcDec = cbc::Decryptor<Aes256>;
    let mut body = raw[16..].to_vec();
    let plain = Aes256CbcDec::new(key.into(), &iv)
        .decrypt_padded::<Pkcs7>(&mut body)
        .map_err(|_| anyhow::anyhow!("cbc decrypt failed"))?;
    Ok(plain.to_vec())
}

pub fn classify(container: &[u8]) -> Result<Codec> {
    if container.len() < 4 {
        bail!("container of {} bytes is too small", container.len());
    }
    match &container[..4] {
        b"VSZa" => Ok(Codec::Zstd),
        b"PK\x03\x04" => Ok(Codec::Zip),
        _ if &container[..3] == b"VZa" => Ok(Codec::Lzma),
        magic => bail!("unknown container magic {magic:?}"),
    }
}

/// VZa layout: `VZ` + version `a` + timestamp(4) + LZMA props(5) + body +
/// crc32(4) + plaintext size(4) + `zv`.
///
/// Returns the body framed as `lzma_alone` (props + size + body), the form
/// the lzma-rs, liblzma, and xz decoders take.
fn vza_to_lzma_alone(container: &[u8]) -> Result<Vec<u8>> {
    const HEADER_LEN: usize = 2 + 1 + 4;
    const PROPS_LEN: usize = 5;
    const FOOTER_LEN: usize = 4 + 4 + 2;
    if container.len() < HEADER_LEN + PROPS_LEN + FOOTER_LEN {
        bail!("VZa container of {} bytes is too small", container.len());
    }
    if &container[..3] != b"VZa" {
        bail!("not a VZa container");
    }
    let footer = &container[container.len() - FOOTER_LEN..];
    if &footer[8..] != b"zv" {
        bail!("bad VZa footer");
    }
    let plain_len = u32::from_le_bytes(footer[4..8].try_into().unwrap()) as usize;
    let props = &container[HEADER_LEN..HEADER_LEN + PROPS_LEN];
    let body = &container[HEADER_LEN + PROPS_LEN..container.len() - FOOTER_LEN];
    let mut alone = Vec::with_capacity(PROPS_LEN + 8 + body.len());
    alone.extend_from_slice(props);
    alone.extend_from_slice(&(plain_len as u64).to_le_bytes());
    alone.extend_from_slice(body);
    Ok(alone)
}

/// VZa via lzma-rs (pure Rust) — the decoder the CDN fetch path uses today.
pub fn decode_vza_lzma_rs(container: &[u8]) -> Result<Vec<u8>> {
    let alone = vza_to_lzma_alone(container)?;
    let mut out = Vec::new();
    lzma_rs::lzma_decompress(&mut Cursor::new(alone), &mut out)?;
    Ok(out)
}

/// VSZa layout: `VSZa` + crc32(4) + zstd frame + crc32(4) + plaintext
/// size(4) + unknown(4) + `zsv`.
pub fn decode_vsza(container: &[u8]) -> Result<Vec<u8>> {
    const HEADER_LEN: usize = 4 + 4;
    const FOOTER_LEN: usize = 4 + 4 + 4 + 3;
    if container.len() < HEADER_LEN + FOOTER_LEN || &container[..4] != b"VSZa" {
        bail!("not a VSZa container");
    }
    let footer = &container[container.len() - FOOTER_LEN..];
    if &footer[FOOTER_LEN - 3..] != b"zsv" {
        bail!("bad VSZa footer");
    }
    let plain_len = u32::from_le_bytes(footer[4..8].try_into().unwrap()) as usize;
    // zstd allocates its output buffer upfront; a corrupt footer must
    // not turn into a huge allocation (same reasoning as
    // `decode_frame`'s cap).
    const MAX_PLAIN_LEN: usize = 64 << 20;
    if plain_len > MAX_PLAIN_LEN {
        bail!("VSZa container claims {plain_len} bytes of plaintext");
    }
    let body = &container[HEADER_LEN..container.len() - FOOTER_LEN];
    Ok(zstd::bulk::decompress(body, plain_len)?)
}

pub fn decode_zip(container: &[u8]) -> Result<Vec<u8>> {
    let mut zip = zip::ZipArchive::new(Cursor::new(container))?;
    let mut out = Vec::new();
    zip.by_index(0)?.read_to_end(&mut out)?;
    Ok(out)
}

/// zstd frame of the plaintext, for size studies and write-path benchmarks.
pub fn encode_zstd(plain: &[u8], level: i32) -> Result<Vec<u8>> {
    Ok(zstd::bulk::compress(plain, level)?)
}

/// One divan input per iteration, counted as `bytes` of throughput.
pub fn bench_bytes(bencher: divan::Bencher, bytes: u64, mut run: impl FnMut()) {
    bencher
        .with_inputs(|| ())
        .counter(divan::counter::BytesCount::new(bytes))
        .bench_local_values(|()| run());
}
