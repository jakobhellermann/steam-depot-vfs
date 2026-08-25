// TODO(ai-review): review for correctness/style
//! End-to-end test for the NFS back end: serve a synthetic manifest,
//! mount it with the platform NFS client, and read it back through the
//! real filesystem. No Steam connection involved.
// Mounting needs no privileges only on macOS; on linux this would
// have to go through sudo, so the test stays mac-only.
#![cfg(all(feature = "nfs", target_os = "macos"))]

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use steam_depot_mount::{NfsMount, NfsMountConfig};
use steam_depot_vfs::VfsError;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::DepotManifestStore;
use steam_vent_depot::{Chunk, ChunkHash, DepotFile, FileKind, Manifest};

const CREATION_TIME: u32 = 1_700_000_000;

/// Chunk store over an in-memory map, counting fetches so the test can
/// assert that a read only pulls the chunks it needs.
struct MemChunks {
    chunks: HashMap<ChunkHash, Bytes>,
    fetches: Arc<AtomicUsize>,
}

impl ChunkStore for MemChunks {
    async fn get(&self, sha: ChunkHash) -> Result<Bytes, VfsError> {
        self.fetches.fetch_add(1, Ordering::Relaxed);
        self.chunks
            .get(&sha)
            .cloned()
            .ok_or(VfsError::ChunkNotInManifest(sha))
    }
}

fn sha_of(n: u8) -> ChunkHash {
    ChunkHash([n; 20])
}

fn chunk(sha: u8, offset: u64, len: usize) -> Chunk {
    Chunk {
        sha: sha_of(sha),
        crc: 0,
        offset,
        size_uncompressed: len as u32,
        size_compressed: len as u32,
    }
}

fn file(path: &str, size: u64, executable: bool, chunks: Vec<Chunk>) -> DepotFile {
    DepotFile {
        path: path.to_string(),
        size,
        kind: FileKind::File,
        executable,
        sha: None,
        linktarget: None,
        chunks,
    }
}

fn dir(path: &str) -> DepotFile {
    DepotFile {
        path: path.to_string(),
        size: 0,
        kind: FileKind::Directory,
        executable: false,
        sha: None,
        linktarget: None,
        chunks: Vec::new(),
    }
}

fn symlink(path: &str, target: &str) -> DepotFile {
    DepotFile {
        path: path.to_string(),
        size: 0,
        kind: FileKind::Symlink,
        executable: false,
        sha: None,
        linktarget: Some(target.to_string()),
        chunks: Vec::new(),
    }
}

/// A manifest with a two-chunk text file, an executable, a nested
/// directory and a symlink.
fn fixture(fetches: Arc<AtomicUsize>) -> DepotManifestStore<MemChunks> {
    let hello_a = Bytes::from_static(b"first-chunk;");
    let hello_b = Bytes::from_static(b"second-chunk\n");
    let script = Bytes::from_static(b"#!/bin/sh\necho ran-from-nfs\n");
    let nested = Bytes::from_static(b"nested\n");

    let chunks = HashMap::from([
        (sha_of(1), hello_a.clone()),
        (sha_of(2), hello_b.clone()),
        (sha_of(3), script.clone()),
        (sha_of(4), nested.clone()),
    ]);

    let manifest = Manifest {
        depot_id: 4242,
        manifest_id: 99,
        creation_time: CREATION_TIME,
        size_uncompressed: 0,
        size_compressed: 0,
        files: vec![
            file(
                "hello.txt",
                (hello_a.len() + hello_b.len()) as u64,
                false,
                vec![
                    chunk(1, 0, hello_a.len()),
                    chunk(2, hello_a.len() as u64, hello_b.len()),
                ],
            ),
            file(
                "run.sh",
                script.len() as u64,
                true,
                vec![chunk(3, 0, script.len())],
            ),
            dir("sub"),
            file(
                "sub/nested.txt",
                nested.len() as u64,
                false,
                vec![chunk(4, 0, nested.len())],
            ),
            symlink("link.txt", "hello.txt"),
        ],
    };

    DepotManifestStore::new(Arc::new(manifest), MemChunks { chunks, fetches })
}

/// Unmounts on drop so a failing assertion can't leave a live mount
/// behind and wedge the temp dir.
struct MountGuard(Option<NfsMount<MemChunks>>, PathBuf);

impl Drop for MountGuard {
    fn drop(&mut self) {
        if let Some(mount) = self.0.take()
            && let Err(e) = mount.unmount()
        {
            eprintln!("unmount failed: {e}");
        }
        let _ = std::fs::remove_dir_all(&self.1);
    }
}

fn temp_mountpoint(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "steam-depot-nfs-test-{name}-{}",
        std::process::id()
    ));
    p
}

#[tokio::test(flavor = "multi_thread")]
async fn serves_a_manifest_over_the_platform_nfs_client() {
    let fetches = Arc::new(AtomicUsize::new(0));
    let mountpoint = temp_mountpoint("basic");
    let mount = NfsMount::start(NfsMountConfig::new(mountpoint.clone()))
        .await
        .expect("mount");
    mount
        .add(1000, 4242, 99, fixture(Arc::clone(&fetches)))
        .expect("add");
    let guard = MountGuard(Some(mount), mountpoint.clone());

    let root = mountpoint.join("1000/4242/99");

    assert_eq!(
        read_names(&mountpoint),
        vec!["1000".to_string()],
        "app dir listing"
    );
    assert_eq!(
        read_names(&root),
        vec![
            "hello.txt".to_string(),
            "link.txt".to_string(),
            "run.sh".to_string(),
            "sub".to_string(),
        ],
        "snapshot root listing"
    );

    assert_eq!(
        std::fs::read_to_string(root.join("hello.txt")).expect("read hello.txt"),
        "first-chunk;second-chunk\n",
        "a read spanning two chunks",
    );
    assert_eq!(
        std::fs::read_to_string(root.join("sub/nested.txt")).expect("read nested"),
        "nested\n",
    );
    assert_eq!(
        std::fs::read_link(root.join("link.txt")).expect("readlink"),
        Path::new("hello.txt"),
    );
    let run_sh = root.join("run.sh");
    assert_eq!(
        std::fs::metadata(&run_sh)
            .expect("stat run.sh")
            .permissions()
            .mode()
            & 0o777,
        0o555,
        "executable bit survives the NFS attributes",
    );
    // The mode bits alone aren't enough: the client asks the server over
    // NFS ACCESS whether execute is allowed.
    assert!(
        std::process::Command::new("test")
            .args(["-x"])
            .arg(&run_sh)
            .status()
            .expect("test -x")
            .success(),
        "access(X_OK) is granted",
    );

    let out = std::process::Command::new(root.join("run.sh"))
        .output()
        .expect("spawn run.sh");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout),
        "ran-from-nfs\n",
        "executables run from the mount (no noexec)",
    );

    drop(guard);
}

#[tokio::test(flavor = "multi_thread")]
async fn opens_a_manifest_only_on_first_access() {
    let opens = Arc::new(AtomicUsize::new(0));
    let fetches = Arc::new(AtomicUsize::new(0));
    let mountpoint = temp_mountpoint("lazy");
    let mount = NfsMount::start(NfsMountConfig::new(mountpoint.clone()))
        .await
        .expect("mount");

    let opens_for_opener = Arc::clone(&opens);
    let fetches_for_opener = Arc::clone(&fetches);
    mount
        .add_lazy(
            1000,
            4242,
            99,
            move || {
                let opens = Arc::clone(&opens_for_opener);
                let fetches = Arc::clone(&fetches_for_opener);
                async move {
                    opens.fetch_add(1, Ordering::Relaxed);
                    Ok(fixture(fetches))
                }
            },
            Some(CREATION_TIME),
        )
        .expect("add_lazy");
    let guard = MountGuard(Some(mount), mountpoint.clone());

    let root = mountpoint.join("1000/4242/99");
    assert_eq!(
        read_names(&mountpoint.join("1000/4242")),
        vec!["99".to_string()]
    );
    assert_eq!(
        opens.load(Ordering::Relaxed),
        0,
        "listing the gid dir must not open the manifest",
    );
    assert_eq!(
        fetches.load(Ordering::Relaxed),
        0,
        "and must not fetch chunks",
    );

    assert_eq!(
        std::fs::read_to_string(root.join("sub/nested.txt")).expect("read nested"),
        "nested\n",
    );
    assert_eq!(
        opens.load(Ordering::Relaxed),
        1,
        "the first read opens the manifest exactly once",
    );
    assert_eq!(
        fetches.load(Ordering::Relaxed),
        1,
        "and pulls only the one chunk that read needs",
    );

    drop(guard);
}

fn read_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
        .map(|e| {
            e.expect("dir entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}
