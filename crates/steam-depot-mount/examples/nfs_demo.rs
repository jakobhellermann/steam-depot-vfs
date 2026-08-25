// TODO(ai-review): review for correctness/style
//! Serve a hand-built manifest over the NFS back end and keep it mounted
//! until Ctrl-C, so the mount can be poked at from a shell.
//!
//! `cargo run -p steam-depot-mount --example nfs_demo --features nfs -- /tmp/depot-demo`

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use steam_depot_mount::{NfsMount, NfsMountConfig};
use steam_depot_vfs::VfsError;
use steam_depot_vfs::chunk_store::ChunkStore;
use steam_depot_vfs::fs::DepotManifestStore;
use steam_vent_depot::{Chunk, ChunkHash, DepotFile, FileKind, Manifest};

struct MemChunks(HashMap<ChunkHash, Bytes>);

impl ChunkStore for MemChunks {
    async fn get(&self, sha: ChunkHash) -> Result<Bytes, VfsError> {
        self.0
            .get(&sha)
            .cloned()
            .ok_or(VfsError::ChunkNotInManifest(sha))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let mountpoint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/depot-demo".to_string());

    let script = Bytes::from_static(b"#!/bin/sh\necho ran-from-nfs\n");
    let text = Bytes::from_static(b"hello\n");
    let chunks = HashMap::from([
        (ChunkHash([1; 20]), script.clone()),
        (ChunkHash([2; 20]), text.clone()),
    ]);
    let manifest = Manifest {
        depot_id: 4242,
        manifest_id: 99,
        creation_time: 1_700_000_000,
        size_uncompressed: 0,
        size_compressed: 0,
        files: vec![
            DepotFile {
                path: "run.sh".into(),
                size: script.len() as u64,
                kind: FileKind::File,
                executable: true,
                sha: None,
                linktarget: None,
                chunks: vec![Chunk {
                    sha: ChunkHash([1; 20]),
                    crc: 0,
                    offset: 0,
                    size_uncompressed: script.len() as u32,
                    size_compressed: script.len() as u32,
                }],
            },
            DepotFile {
                path: "hello.txt".into(),
                size: text.len() as u64,
                kind: FileKind::File,
                executable: false,
                sha: None,
                linktarget: None,
                chunks: vec![Chunk {
                    sha: ChunkHash([2; 20]),
                    crc: 0,
                    offset: 0,
                    size_uncompressed: text.len() as u32,
                    size_compressed: text.len() as u32,
                }],
            },
        ],
    };

    let mount = NfsMount::start(NfsMountConfig::new(mountpoint.clone().into())).await?;
    mount.add(
        1000,
        4242,
        99,
        DepotManifestStore::new(Arc::new(manifest), MemChunks(chunks)),
    )?;
    println!("mounted at {mountpoint}/1000/4242/99 — Ctrl-C to unmount");
    tokio::signal::ctrl_c().await?;
    mount.unmount()?;
    Ok(())
}
