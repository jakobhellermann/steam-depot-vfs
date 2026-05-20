// TODO(ai-review): review for correctness/style
//! SHA-1 verification of cached chunk files. Used by both `stats
//! --verify` (which only reports) and `prefetch --repair` (which
//! deletes mismatches so they get refetched).

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;

use anyhow::{Context, Result};
use sha1::{Digest, Sha1};
use steam_depot_vfs::ChunkHash;

/// Outcome of one verify pass.
pub struct VerifyReport {
    pub ok: u64,
    pub mismatched: Vec<ChunkHash>,
    pub errored: Vec<(ChunkHash, String)>,
    pub elapsed: std::time::Duration,
    pub total_bytes: u64,
}

/// Read every chunk file under `chunks_root` and confirm its SHA-1
/// matches its filename. `shas` is the set of expected chunk names
/// (filenames). Parallelised via `std::thread::scope`.
pub fn verify_cache(
    chunks_root: &Path,
    shas: &std::collections::HashSet<ChunkHash>,
) -> Result<VerifyReport> {
    let started = std::time::Instant::now();
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let (tx, rx) = mpsc::channel::<(ChunkHash, std::path::PathBuf)>();
    let rx = Mutex::new(rx);

    let ok = AtomicU64::new(0);
    let total_bytes = AtomicU64::new(0);
    let mismatched = Mutex::new(Vec::<ChunkHash>::new());
    let errored = Mutex::new(Vec::<(ChunkHash, String)>::new());

    std::thread::scope(|s| {
        for _ in 0..workers {
            let rx = &rx;
            let ok = &ok;
            let total_bytes = &total_bytes;
            let mismatched = &mismatched;
            let errored = &errored;
            s.spawn(move || {
                loop {
                    let job = rx.lock().unwrap().recv();
                    let Ok((expected, path)) = job else { return };
                    match verify_one(&expected, &path) {
                        Ok((true, len)) => {
                            ok.fetch_add(1, Ordering::Relaxed);
                            total_bytes.fetch_add(len, Ordering::Relaxed);
                        }
                        Ok((false, len)) => {
                            total_bytes.fetch_add(len, Ordering::Relaxed);
                            mismatched.lock().unwrap().push(expected);
                        }
                        Err(e) => {
                            errored.lock().unwrap().push((expected, e.to_string()));
                        }
                    }
                }
            });
        }
        for sha in shas {
            tx.send((*sha, chunks_root.join(sha.to_string()))).unwrap();
        }
        drop(tx);
    });

    Ok(VerifyReport {
        ok: ok.load(Ordering::Relaxed),
        mismatched: mismatched.into_inner().unwrap(),
        errored: errored.into_inner().unwrap(),
        elapsed: started.elapsed(),
        total_bytes: total_bytes.load(Ordering::Relaxed),
    })
}

fn verify_one(expected: &ChunkHash, path: &Path) -> Result<(bool, u64)> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let mut hasher = Sha1::new();
    hasher.update(&bytes);
    let got = hasher.finalize();
    Ok((got.as_slice() == expected.0.as_slice(), bytes.len() as u64))
}
