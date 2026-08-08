"""Large, deterministic benchmark of the sharded archive pipeline.

The input rows come from the real scraper Parquet.  Only the remote Wayback
service is replaced by a local deterministic HTTP endpoint, so this measures
process/thread scheduling, extraction, checkpointing, resume-safe shard output,
and reduction without sending thousands of artificial public-archive requests.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import archive_parallel as archive
from tests.test_archive_parallel import MockWayback


def real_sample(source: Path, size: int) -> pl.DataFrame:
    schema = pl.read_parquet_schema(source)
    if "needs_archive" not in schema:
        raise RuntimeError("Source Parquet has no needs_archive column")
    return (
        pl.scan_parquet(source)
        .filter(pl.col("needs_archive").fill_null(False))
        .with_columns(pl.col("id").cast(pl.String).hash(seed=20260807).alias("__sample"))
        .sort("__sample")
        .head(size)
        .drop("__sample")
        .collect()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=ROOT / "trafilatura_scraped.parquet")
    parser.add_argument("--sample-size", type=int, default=5000)
    parser.add_argument("--shards", type=int, default=archive.DEFAULT_SHARDS)
    parser.add_argument(
        "--profiles", nargs="+", default=["1x8", "2x8", "4x4", "4x8", "4x16", "8x4"],
        help="processes x threads, for example 4x8",
    )
    parser.add_argument("--output", type=Path, default=ROOT / "tests" / "archive_parallel_benchmark")
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f"Refusing to overwrite benchmark output: {args.output}")
    args.output.mkdir(parents=True)

    sample_started = time.perf_counter()
    sample = real_sample(args.source, args.sample_size)
    if sample.height != args.sample_size:
        raise RuntimeError(f"Requested {args.sample_size} pending rows; found {sample.height}")
    sample_seconds = time.perf_counter() - sample_started
    baseline_text = sample.select(
        pl.col("text").fill_null("").str.strip_chars().ne("").sum()
    ).item()
    summary = {
        "source": str(args.source.resolve()),
        "sample_size": sample.height,
        "sample_load_seconds": sample_seconds,
        "sample_text_rows": baseline_text,
        "profiles": {},
    }

    endpoint = MockWayback()
    endpoint.start()
    try:
        for profile in args.profiles:
            processes, threads = (int(value) for value in profile.lower().split("x", 1))
            if processes > args.shards:
                raise ValueError(f"Profile {profile} exceeds {args.shards} shards")
            profile_dir = args.output / profile
            profile_dir.mkdir()
            sample.write_parquet(
                profile_dir / "trafilatura_scraped.parquet", compression="zstd", statistics=True
            )
            run_dir = profile_dir / "archive_runs" / "benchmark"
            shards = args.shards
            prepare_started = time.perf_counter()
            manifest = archive.prepare_run(profile_dir, run_dir, shards)
            prepare_seconds = time.perf_counter() - prepare_started
            config = archive.ArchiveConfig(
                request_rate=1000,
                breaker_pause_s=0.05,
                replay_root=f"http://127.0.0.1:{endpoint.port}/web",
                availability_url=f"http://127.0.0.1:{endpoint.port}/available",
                cdx_url=f"http://127.0.0.1:{endpoint.port}/cdx",
                archive_host="127.0.0.1",
            )
            process_started = time.perf_counter()
            manifest = archive.run_supervisor(
                run_dir, manifest,
                processes=processes,
                threads=threads,
                save_every=250,
                checkpoint_age_s=30,
                max_process_restarts=1,
                config=config,
            )
            process_seconds = time.perf_counter() - process_started
            reduce_started = time.perf_counter()
            final_path = archive.reduce_run(profile_dir, run_dir, manifest)
            reduce_seconds = time.perf_counter() - reduce_started
            final = pl.read_parquet(final_path)
            stats = final.select([
                pl.len().alias("rows"),
                pl.col("id").n_unique().alias("unique_ids"),
                pl.col("text").fill_null("").str.strip_chars().ne("").sum().alias("text_rows"),
                pl.col("needs_archive").fill_null(False).sum().alias("pending_rows"),
                (pl.col("fetch_method") == "wayback").sum().alias("wayback_rows"),
            ]).row(0, named=True)
            if stats["rows"] != sample.height or stats["unique_ids"] != sample.height:
                raise RuntimeError(f"Profile {profile} lost or duplicated IDs")
            if stats["text_rows"] < baseline_text:
                raise RuntimeError(f"Profile {profile} lost existing text")
            summary["profiles"][profile] = {
                "shards": shards,
                "prepare_seconds": prepare_seconds,
                "process_seconds": process_seconds,
                "reduce_seconds": reduce_seconds,
                "processing_rows_per_second": sample.height / process_seconds,
                **stats,
            }
            print(json.dumps({profile: summary["profiles"][profile]}, indent=2))
    finally:
        endpoint.close()

    (args.output / "benchmark_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
