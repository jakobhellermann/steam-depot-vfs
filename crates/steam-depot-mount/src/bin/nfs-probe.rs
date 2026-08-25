// TODO(ai-review): review for correctness/style
//! Instrumented NFS mount: serves one synthetic file and reports what
//! the kernel actually asked for, so the read path can be judged on
//! measurements rather than guesses.
//!
//! `cargo run -p steam-depot-mount --bin nfs-probe --features nfs -- <mountpoint> [chunk_delay_ms]`

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use steam_depot_mount::{NfsMount, NfsMountConfig};
use steam_depot_vfs::VfsError;
use steam_depot_vfs::chunk_store::{ChunkStore, FsCacheStore};
use steam_depot_vfs::fs::DepotManifestStore;
use steam_vent_depot::{Chunk, ChunkHash, DepotFile, FileKind, Manifest};

const CHUNK: usize = 1 << 20;
const CHUNKS: usize = 40;

/// The store is content-addressed and verifies what it reads back, so a
/// chunk's name has to be the SHA-1 of its bytes — otherwise the cache
/// throws every chunk away as corrupt and the measurement is nonsense.
fn chunk_of(index: usize) -> (ChunkHash, Bytes) {
    let bytes = Bytes::from(vec![index as u8; CHUNK]);
    let mut hasher = <sha1::Sha1 as sha1::Digest>::new();
    sha1::Digest::update(&mut hasher, &bytes);
    let digest: [u8; 20] = sha1::Digest::finalize(hasher).into();
    (ChunkHash(digest), bytes)
}

#[derive(Default)]
struct Stats {
    /// Fetches per chunk, so repeats are visible.
    per_chunk: HashMap<[u8; 20], usize>,
    /// Fetches that were in flight at the same time as another.
    max_inflight: usize,
    inflight: usize,
    first: Option<Instant>,
    last: Option<Instant>,
}

struct CountingChunks {
    delay: Duration,
    stats: Arc<Mutex<Stats>>,
    chunks: HashMap<[u8; 20], Bytes>,
}

impl ChunkStore for CountingChunks {
    async fn get(&self, sha: ChunkHash) -> Result<Bytes, VfsError> {
        {
            let mut s = self.stats.lock().expect("stats");
            *s.per_chunk.entry(sha.0).or_default() += 1;
            s.inflight += 1;
            s.max_inflight = s.max_inflight.max(s.inflight);
            s.first.get_or_insert_with(Instant::now);
        }
        tokio::time::sleep(self.delay).await;
        let mut s = self.stats.lock().expect("stats");
        s.inflight -= 1;
        s.last = Some(Instant::now());
        self.chunks
            .get(&sha.0)
            .cloned()
            .ok_or(VfsError::ChunkNotInManifest(sha))
    }
}

fn manifest() -> Manifest {
    let chunks: Vec<Chunk> = (0..CHUNKS)
        .map(|i| Chunk {
            sha: chunk_of(i).0,
            crc: 0,
            offset: (i * CHUNK) as u64,
            size_uncompressed: CHUNK as u32,
            size_compressed: CHUNK as u32,
        })
        .collect();
    Manifest {
        depot_id: 4242,
        manifest_id: 99,
        creation_time: 1_700_000_000,
        size_uncompressed: 0,
        size_compressed: 0,
        files: vec![DepotFile {
            path: "big.bin".into(),
            size: (CHUNKS * CHUNK) as u64,
            kind: FileKind::File,
            executable: false,
            sha: None,
            linktarget: None,
            chunks,
        }],
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let mut args = std::env::args().skip(1);
    let mountpoint = args.next().unwrap_or_else(|| "/tmp/nfs-probe".to_string());
    let delay = Duration::from_millis(args.next().and_then(|a| a.parse().ok()).unwrap_or(0));
    // The on-disk cache an embedder puts in front, so re-reading a
    // chunk file is in the measurement instead of a memcpy.
    let disk = args
        .next()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("nfs-probe-cache-{}", std::process::id()))
        });
    // Fourth argument: read only this many bytes from the start instead
    // of the whole file, to see what a partial read costs.
    let want: Option<u64> = args.next().and_then(|a| a.parse().ok());

    let stats = Arc::new(Mutex::new(Stats::default()));
    let counting = CountingChunks {
        delay,
        stats: Arc::clone(&stats),
        chunks: (0..CHUNKS)
            .map(chunk_of)
            .map(|(sha, b)| (sha.0, b))
            .collect(),
    };
    let mount = NfsMount::start(NfsMountConfig::new(mountpoint.clone().into())).await?;
    mount.add(
        1000,
        4242,
        99,
        DepotManifestStore::new(
            Arc::new(manifest()),
            FsCacheStore::new(counting, disk.clone()),
        ),
    )?;

    let path = format!("{mountpoint}/1000/4242/99/big.bin");
    let started = Instant::now();
    let read = tokio::task::spawn_blocking({
        let path = path.clone();
        move || -> std::io::Result<usize> {
            use std::io::Read;
            let mut f = std::fs::File::open(&path)?;
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
    let wall = started.elapsed();

    let s = stats.lock().expect("stats");
    let fetches: usize = s.per_chunk.values().sum();
    let repeated: usize = s.per_chunk.values().filter(|&&n| n > 1).count();
    let worst = s.per_chunk.values().copied().max().unwrap_or(0);
    println!(
        "read {read} bytes in {wall:?} ({:.1} MB/s)",
        read as f64 / 1e6 / wall.as_secs_f64()
    );
    println!("chunks in file:      {CHUNKS}");
    println!("distinct fetched:    {}", s.per_chunk.len());
    println!(
        "total fetches:       {fetches}  (amplification {:.2}x)",
        fetches as f64 / CHUNKS as f64
    );
    println!("chunks fetched >1x:  {repeated} (worst {worst}x)");
    println!("max concurrent:      {}", s.max_inflight);
    println!("per-chunk delay:     {delay:?}");
    println!("disk cache:          {}", disk.display());
    drop(s);

    mount.unmount().await?;
    Ok(())
}
