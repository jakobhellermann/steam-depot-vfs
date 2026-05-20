# TODO(ai-review): review for correctness/style
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "perfetto",
#     "pandas",
# ]
# ///
"""Quick trace analyzer for steam-depot prefetch/mount traces.

Usage:
    uv run scripts/analyze_trace.py [TRACE]

Default path is `trace.pftrace` in the workspace root.
"""

import sys

import pandas as pd
from perfetto.trace_processor import TraceProcessor

pd.set_option("display.max_rows", None)
pd.set_option("display.width", 200)
pd.set_option("display.float_format", lambda x: f"{x:,.3f}")


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else "trace.pftrace"
    tp = TraceProcessor(trace=path)
    print(f"== {path} ==\n")

    section("Span totals (our instrumented spans only)")
    print(
        tp.query(
            """
            SELECT name,
                   COUNT(*)        AS n,
                   SUM(dur)/1e9    AS total_sec,
                   AVG(dur)/1e6    AS avg_ms,
                   MIN(dur)/1e6    AS min_ms,
                   MAX(dur)/1e6    AS max_ms
            FROM slice
            WHERE name LIKE 'cdn.%'
               OR name LIKE 'fs_cache.%'
               OR name LIKE 'chunk.%'
               OR name LIKE 'fuse.%'
               OR name = 'auth.resolve'
            GROUP BY name
            ORDER BY total_sec DESC
            """
        ).as_pandas_dataframe()
    )

    section("cdn.http_get duration distribution")
    print(percentiles(tp, "cdn.http_get"))

    section("chunk.decode duration distribution")
    print(percentiles(tp, "chunk.decode"))

    section("fs_cache.ensure − cdn.get gap (write/fsync cost)")
    # Each cdn.get sits inside an fs_cache.ensure with a small surrounding
    # cost on miss (read cache → none, then on miss: write_all + sync_all +
    # rename). We approximate by per-name totals.
    df = tp.query(
        """
            WITH t AS (
              SELECT name, SUM(dur)/1e9 AS total_sec FROM slice
              WHERE name IN ('fs_cache.ensure', 'cdn.get')
              GROUP BY name
            )
            SELECT
              (SELECT total_sec FROM t WHERE name='fs_cache.ensure') -
              (SELECT total_sec FROM t WHERE name='cdn.get') AS write_sec
            """
    ).as_pandas_dataframe()
    print(df)

    section("cdn.http_get by host (where 'host' arg is set)")
    print(
        tp.query(
            """
            SELECT
              EXTRACT_ARG(s.arg_set_id, 'debug.host')          AS host,
              COUNT(*)                                   AS chunks,
              SUM(s.dur)/1e9                             AS total_sec,
              AVG(s.dur)/1e6                             AS avg_ms,
              MAX(s.dur)/1e6                             AS max_ms
            FROM slice s
            WHERE s.name = 'cdn.http_get'
            GROUP BY host
            ORDER BY chunks DESC
            """
        ).as_pandas_dataframe()
    )

    section("Tail: 10 slowest cdn.http_get spans")
    print(
        tp.query(
            """
            SELECT
              EXTRACT_ARG(arg_set_id, 'debug.host')           AS host,
              EXTRACT_ARG(arg_set_id, 'debug.size_compressed') AS size_compressed,
              dur/1e6                                   AS ms
            FROM slice
            WHERE name = 'cdn.http_get'
            ORDER BY dur DESC
            LIMIT 10
            """
        ).as_pandas_dataframe()
    )

    section("Wall-clock summary")
    df = tp.query(
        """
            SELECT
              MIN(ts)/1e9               AS start_sec,
              MAX(ts + dur)/1e9         AS end_sec,
              (MAX(ts + dur) - MIN(ts))/1e9 AS wall_sec
            FROM slice
            WHERE name = 'fs_cache.ensure'
            """
    ).as_pandas_dataframe()
    print(df)

    # Throughput, computed from the spans themselves.
    df = tp.query(
        """
            SELECT
              SUM(CAST(EXTRACT_ARG(arg_set_id, 'debug.size_compressed') AS INTEGER)) AS bytes_compressed,
              COUNT(*) AS chunks
            FROM slice
            WHERE name = 'cdn.http_get'
            """
    ).as_pandas_dataframe()
    print("\nfetched chunks/bytes (per cdn.http_get args):")
    print(df)


def percentiles(tp: TraceProcessor, name: str) -> pd.DataFrame:
    return tp.query(
        f"""
        SELECT
          COUNT(*)                                            AS n,
          AVG(dur)/1e6                                        AS avg_ms,
          PERCENTILE(dur, 50)/1e6                             AS p50_ms,
          PERCENTILE(dur, 95)/1e6                             AS p95_ms,
          PERCENTILE(dur, 99)/1e6                             AS p99_ms,
          MAX(dur)/1e6                                        AS max_ms
        FROM slice WHERE name = '{name}'
        """
    ).as_pandas_dataframe()


def section(title: str) -> None:
    print(f"\n--- {title} ---")


if __name__ == "__main__":
    main()
