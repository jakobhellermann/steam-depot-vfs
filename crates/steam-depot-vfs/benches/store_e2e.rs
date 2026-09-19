// TODO(ai-review): review for style and correctness
//! Whole-pipeline read benches over the real on-disk store, exactly as
//! the mounts and backend routes take it. A separate bench target from
//! `store`: this one's precondition — a pinned, fully-present file —
//! fails loudly when the store isn't populated, and as its own process
//! that failure leaves the fixture benches' results intact.
//!
//! The store fills lazily through the viewer (open the manifest there
//! once); `E2E_FILE` overrides the pinned target.

#[path = "support.rs"]
mod support;

#[path = "../examples/common/mod.rs"]
mod store_common;

use std::sync::LazyLock;

use divan::counter::BytesCount;
use store_common::{Snapshot, StoreCtx};

fn main() {
    divan::main();
}

struct E2e {
    rt: tokio::runtime::Runtime,
    ctx: StoreCtx,
    path: String,
    size: u64,
    /// Long-lived store so `read_full` measures the warm steady state.
    store: Snapshot,
}

static E2E: LazyLock<E2e> = LazyLock::new(|| {
    let file = std::env::var("E2E_FILE").unwrap_or_else(|_| store_common::PINNED_FILE.to_owned());
    let ctx = StoreCtx::open(None, store_common::PINNED_GID)
        .unwrap_or_else(|e| panic!("opening the store: {e:#}"));
    let file = ctx
        .find_file(&file)
        .unwrap_or_else(|e| panic!("e2e target: {e}"));
    let (path, size, chunk_count) = (file.path.clone(), file.size, file.chunks().len());
    eprintln!("e2e file: {path} ({size} B, {chunk_count} chunks)");

    E2e {
        rt: tokio::runtime::Runtime::new().expect("building a tokio runtime"),
        store: ctx.fresh_snapshot(),
        ctx,
        path,
        size,
    }
});

mod e2e {
    use super::*;

    /// Whole-file read through the real pipeline:
    /// [`DepotManifestStore::read_full`](steam_depot_vfs::fs::DepotManifestStore::read_full)
    /// over the on-disk chunk store. The store lives across iterations,
    /// so the median is the decoded-cache-warm steady state; the first
    /// iteration pays the cold load — read + decode, which is the whole
    /// verification — and shows in the slowest column.
    #[divan::bench]
    fn read_full(bencher: divan::Bencher) {
        let e2e = &*E2E;
        support::bench_bytes(bencher, e2e.size, || {
            divan::black_box(
                e2e.rt
                    .block_on(e2e.store.read_full(&e2e.path))
                    .expect("read_full"),
            );
        });
    }

    /// The FUSE read shape: one client read arrives as many small range
    /// reads over the same chunks. An isolated store per iteration
    /// (`FsCacheStore::new`, private state) keeps the decoded cache
    /// cold, so this guards the whole cold load path — every chunk is
    /// read and decoded once, and the fragments after the first of a
    /// chunk come from the decoded cache. A broken or missing cache
    /// regresses this bench even though `read_full` stays healthy.
    #[divan::bench]
    fn read_fragments_128k(bencher: divan::Bencher) {
        const FRAGMENT: u64 = 128 * 1024;
        let e2e = &*E2E;
        bencher
            .with_inputs(|| e2e.ctx.fresh_snapshot())
            .counter(BytesCount::new(e2e.size))
            .bench_local_refs(|fs| {
                let mut off = 0u64;
                while off < e2e.size {
                    let len = FRAGMENT.min(e2e.size - off);
                    divan::black_box(
                        e2e.rt
                            .block_on(fs.read(&e2e.path, off, len))
                            .expect("fragment read"),
                    );
                    off += len;
                }
            });
    }
}
