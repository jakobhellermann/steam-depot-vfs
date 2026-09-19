// TODO(ai-review): review for style and correctness
//! Cost baselines of the compressed chunk store, over real Steam chunks:
//! `decode` the shipped frame decoder — which is the whole read-path
//! verification — and `encode` the shipped write path. Both call the
//! public frame functions, so they track the code as it is, not a
//! snapshot of what some bench happened to mirror. The whole-pipeline
//! benches live in the separate `store_e2e` target: they need a
//! populated real store, and their loud failure there must not take
//! these down with them.
//!
//! Fixtures are required — one-time setup downloads a sample from
//! Steam's CDN and writes raw/container/plaintext triples:
//!
//! ```text
//! just fixtures
//! cargo bench -p steam-depot-vfs
//! ```

mod support;

use std::sync::LazyLock;

use support::FixtureIndex;

fn main() {
    divan::main();
}

/// Fixture chunks in the store's on-disk frame form.
struct Set {
    frames: Vec<Vec<u8>>,
    plaintext_total: u64,
}

struct Fixtures {
    /// Plaintext chunk bytes — what the store's frames decode to.
    plain: Vec<Vec<u8>>,
    /// Frames built by the shipped `encode_frame` from every plaintext
    /// chunk — the store's actual on-disk content.
    frames: Set,
}

static FIXTURES: LazyLock<Fixtures> = LazyLock::new(Fixtures::load);

impl Fixtures {
    fn load() -> Self {
        let dir = support::fixture_dir();
        let index: FixtureIndex = std::fs::read_to_string(dir.join("index.json"))
            .map_err(|e| {
                format!(
                    "fixtures missing under {}: {e}\n\
                     run `just fixtures` first",
                    dir.display()
                )
            })
            .and_then(|s| serde_json::from_str(&s).map_err(|e| e.to_string()))
            .unwrap_or_else(|e| panic!("{e}"));

        let mut f = Fixtures {
            plain: Vec::new(),
            frames: Set {
                frames: Vec::new(),
                plaintext_total: 0,
            },
        };
        for chunk in &index.chunks {
            let sha = &chunk.sha;
            let plain = std::fs::read(dir.join(format!("{sha}.plain")))
                .unwrap_or_else(|e| panic!("fixture {sha}.plain: {e}"));
            // Every stored chunk is a frame, regardless of the CDN codec
            // it arrived as; the decode bench measures the shipped
            // decoder on shipped frames.
            let frame =
                steam_depot_vfs::chunk_store::encode_frame(&plain).expect("encode fixture frame");
            f.frames.plaintext_total += plain.len() as u64;
            f.frames.frames.push(frame);
            f.plain.push(plain);
        }
        f
    }
}

mod decode {
    use super::*;

    /// The store's read-path decoder on frames built by the shipped
    /// encoder — every chunk the store holds decodes through exactly
    /// this, whatever CDN codec it arrived as. Decoding is the whole
    /// verification (content checksum + identity), so this bench is
    /// the full cold-read cost per chunk.
    #[divan::bench]
    fn frame_zstd(bencher: divan::Bencher) {
        let set = &FIXTURES.frames;
        support::bench_bytes(bencher, set.plaintext_total, || {
            for frame in &set.frames {
                let (claimed, plain) =
                    steam_depot_vfs::chunk_store::decode_frame(frame).expect("decode frame");
                divan::black_box((claimed, plain));
            }
        });
    }
}

mod encode {
    use super::*;

    /// The store's write path: `encode_frame` — hash, compress, checksum —
    /// at the shipped level, the exact cost a fetch pays before
    /// persisting. Subsampled: the full fixture set would dominate the
    /// suite's run time; throughput is per-byte and unaffected by which
    /// chunks run.
    #[divan::bench]
    fn plain_frame_zstd(bencher: divan::Bencher) {
        let f = &*FIXTURES;
        let plain: Vec<&[u8]> = f.plain.iter().map(|p| p.as_slice()).take(32).collect();
        let total: u64 = plain.iter().map(|p| p.len() as u64).sum();
        support::bench_bytes(bencher, total, || {
            for p in &plain {
                divan::black_box(
                    steam_depot_vfs::chunk_store::encode_frame(p).expect("encode frame"),
                );
            }
        });
    }
}
