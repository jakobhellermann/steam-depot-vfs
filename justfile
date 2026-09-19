_default:
    just --list

profile:
    cargo run --release -p steam-depot-vfs --example store_prefetch
    cargo run --release -p steam-depot-vfs --example store_sweep
    cargo run --release -p steam-depot-vfs --example store_stream
    cargo run --release -p steam-depot-vfs --example store_file
    cargo run --release -p steam-depot-vfs --example store_fragments

profile-perf:
    #!/usr/bin/env bash
    set -euo pipefail
    dir=$(cargo metadata --format-version 1 --no-deps | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
    cargo build --release -p steam-depot-vfs --examples
    cargo run --release -p steam-depot-vfs --example store_prefetch
    bid=$(readelf -n /usr/lib/libc.so.6 2>/dev/null | grep -oP '(?<=Build ID: )\w+' | head -1)
    if [ -n "$bid" ] && [ ! -e "$HOME/.debug/.build-id/${bid:0:2}/${bid:2}/debug" ] && command -v debuginfod-find > /dev/null; then
        if dbg=$(debuginfod-find "debuginfo" "$bid" 2>/dev/null); then
            mkdir -p "$HOME/.debug/.build-id/${bid:0:2}"
            ln -sfn "$dbg" "$HOME/.debug/.build-id/${bid:0:2}/${bid:2}/debug"
            ln -sfn /usr/lib/libc.so.6 "$HOME/.debug/.build-id/${bid:0:2}/${bid:2}/elf"
            echo "installed glibc debuginfo for perf"
        fi
    fi
    for ex in store_sweep store_stream store_file store_fragments; do
        echo
        echo "═══ perf: $ex ═══"
        "$dir/release/examples/$ex" > /dev/null
        perf record -q -F 999 -o "/tmp/perf-$ex.data" -- "$dir/release/examples/$ex"
        perf report -i "/tmp/perf-$ex.data" --stdio --no-children --sort dso,symbol \
            | grep -vE '^#|^$' | sed -n '1,10p'
    done

fixtures *args:
    cargo run --release -p steam-depot-vfs --example bench_setup -- {{args}}

bench *args:
    cargo bench -p steam-depot-vfs {{args}}

fmt:
    cargo fmt

check:
    cargo check -p steam-depot-vfs --all-targets

test:
    cargo test -p steam-depot-vfs

lint:
    cargo clippy -p steam-depot-vfs --all-targets
