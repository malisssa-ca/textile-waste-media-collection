"""Small read-only probe of the production Wayback transport.

Unlike the large local benchmark, this contacts the public Wayback service.
Keep samples small.  It never writes to or changes the source Parquet.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import archive_parallel as archive


def deterministic_sample(frame: pl.LazyFrame, count: int, seed: int) -> pl.DataFrame:
    return (
        frame.with_columns(pl.col("id").cast(pl.String).hash(seed=seed).alias("__sample"))
        .sort("__sample").head(count).drop("__sample").collect()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=ROOT / "trafilatura_scraped.parquet")
    parser.add_argument("--controls", type=int, default=12,
                        help="Previously recovered Wayback rows used as positive controls")
    parser.add_argument("--pending", type=int, default=12)
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--request-rate", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=20260807)
    parser.add_argument("--output", type=Path,
                        default=ROOT / "tests" / "archive_parallel_network_probe.json")
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f"Refusing to overwrite probe output: {args.output}")

    source = pl.scan_parquet(args.source)
    controls = deterministic_sample(
        source.filter(
            (pl.col("fetch_method") == "wayback")
            & pl.col("text").fill_null("").str.strip_chars().ne("")
        ),
        args.controls,
        args.seed,
    ).with_columns(pl.lit("control").alias("__group"))
    pending = deterministic_sample(
        source.filter(pl.col("needs_archive").fill_null(False)),
        args.pending,
        args.seed + 1,
    ).with_columns(pl.lit("pending").alias("__group"))
    rows = pl.concat([controls, pending], how="diagonal_relaxed").to_dicts()

    config = archive.ArchiveConfig(request_rate=args.request_rate)
    context = mp.get_context("spawn")
    control = archive.SharedArchiveControl(context, config)
    metrics = archive.Metrics()
    client = archive.ArchiveHttpClient(config, control, metrics)
    started = time.perf_counter()
    results = []
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = {
            pool.submit(archive.process_archive_row, row, client, "network-probe"): row
            for row in rows
        }
        for future in as_completed(futures):
            row = futures[future]
            try:
                result = future.result()
                results.append({
                    "id": str(row["id"]),
                    "group": row["__group"],
                    "prior_text_length": len(str(row.get("text") or "")),
                    "result_text_length": len(str(result.get("text") or "")),
                    "fallback_text_length": int(result.get("fallback_text_length") or 0),
                    "verification_level": result.get("verification_level"),
                    "fetch_method": result.get("fetch_method"),
                    "needs_archive": bool(result.get("needs_archive")),
                })
            except (archive.ProviderUnavailable, InterruptedError) as exc:
                results.append({
                    "id": str(row["id"]), "group": row["__group"],
                    "error": type(exc).__name__, "needs_archive": True,
                })

    elapsed = time.perf_counter() - started
    summary = {
        "source": str(args.source.resolve()),
        "controls": args.controls,
        "pending": args.pending,
        "threads": args.threads,
        "request_rate": args.request_rate,
        "elapsed_seconds": elapsed,
        "rows_per_second": len(results) / elapsed if elapsed else 0.0,
        "terminal_results": sum(
            row.get("verification_level") in {"strict_verified", "page_verified"}
            for row in results
        ),
        "control_text_preserved": sum(
            row.get("group") == "control"
            and max(
                row.get("result_text_length", 0), row.get("fallback_text_length", 0)
            ) >= row.get("prior_text_length", 0)
            for row in results
        ),
        "still_pending": sum(bool(row.get("needs_archive")) for row in results),
        "metrics": metrics.snapshot(),
        "provider": control.snapshot(),
        "rows": results,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
