If you create a file, start it with a `// TODO(ai-review): review for correctness/style` comment.

# steam-depot-vfs

On-disk chunk store for Steam depot manifests. Chunks are persisted as
self-verifying zstd frames (see `chunk_store::cache`): a content checksum
plus an embedded SHA-1 checked against the file's name, so decoding a
frame is verifying it — no read-path hashing. One shared chunk-cache
state per DepotStore (`ChunkDir`: decoded-chunk LRU + load locks) is
handed to every snapshot `DepotStore` opens.

## Profiling the store

The access-pattern examples run with no arguments and pinned targets
(the Silksong depot manifest and, for single-file patterns, the areacoral
bundle), so numbers are comparable over time. A pinned target that is
not in the store fails loudly instead of silently measuring something
else. Every knob is a clap flag with its default in `--help`.

- `just profile` — fill the store first (`store_prefetch`: downloads
  the pinned manifest's missing chunks through the store's own fetch
  path; needs Steam credentials via direnv only when something is
  missing), then run all four patterns (whole-file pass over the pinned
  bulk set — largest files until 512 MiB — export-style windowed
  pass, single file cold+warm, fragmented reads) and print wall/CPU
  per pass. Tell a fresh session
  "profile mal die examples" and this is the command.
- `just profile-perf` — same, each under `perf record`, top symbols per
  pattern.
- `cargo run --release -p steam-depot-vfs --example <name> -- --help` —
  one pattern with knobs.

## Benchmarks

- `just bench` — the regression suite (`cargo bench -p
  steam-depot-vfs`), two targets: `store` (frame decode/encode, needs
  fixtures) and `store_e2e` (whole read pipeline, needs the real store
  — populated by opening the manifest in the viewer once; a store
  that isn't ready fails that target alone, not the suite).
- `just fixtures` — one-time fixture download for `store` (Steam
  credentials via direnv, refresh token cached).
