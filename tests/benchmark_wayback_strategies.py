"""Read-only comparison of three public Wayback recovery strategies.

Every strategy receives the same deterministic, stratified rows from the real
scraper Parquet.  Requests are interleaved behind one aggregate rate gate so a
temporary provider slowdown does not systematically favor the first strategy.
The script never modifies the source Parquet or production output directory.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import re
import statistics
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import textile_waste_p2_scrape as scraper


# archive.org/wayback/available returns HTTP 400 for some otherwise valid
# descriptive User-Agent syntaxes (including a parenthesized project URL).
# Keep the project identity plain and non-personal for all three endpoints.
USER_AGENT = "textile-waste-media-collection/2026.08"
STRATEGIES = ("direct", "availability", "cdx")


@dataclass
class ResponseResult:
    status: int | None
    content: bytes | None
    final_url: str | None
    elapsed_s: float
    error: str | None
    physical_requests: int


class RateGate:
    def __init__(self, request_rate: float):
        self.gap_s = 1.0 / request_rate
        self.lock = threading.Lock()
        self.next_at = 0.0

    def wait(self) -> None:
        with self.lock:
            delay = max(0.0, self.next_at - time.monotonic())
            if delay:
                time.sleep(delay)
            self.next_at = time.monotonic() + self.gap_s


class BenchmarkClient:
    def __init__(self, gate: RateGate, connect_timeout: float, read_timeout: float):
        self.gate = gate
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.local = threading.local()
        self.lock = threading.Lock()
        self.request_count = 0

    def _session(self) -> requests.Session:
        session = getattr(self.local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.1",
                "Accept-Encoding": "gzip, deflate",
            })
            adapter = requests.adapters.HTTPAdapter(max_retries=0)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            self.local.session = session
        return session

    def get(self, url: str, *, params=None) -> ResponseResult:
        self.gate.wait()
        with self.lock:
            self.request_count += 1
        started = time.perf_counter()
        try:
            response = self._session().get(
                url,
                params=params,
                timeout=(self.connect_timeout, self.read_timeout),
                allow_redirects=True,
            )
            return ResponseResult(
                status=response.status_code,
                content=response.content or None,
                final_url=response.url,
                elapsed_s=time.perf_counter() - started,
                error=None,
                physical_requests=1 + len(response.history),
            )
        except requests.exceptions.RequestException as exc:
            return ResponseResult(
                status=None,
                content=None,
                final_url=None,
                elapsed_s=time.perf_counter() - started,
                error=type(exc).__name__,
                physical_requests=1,
            )


def _sample(source: Path, controls: int, pending: int, seed: int) -> list[dict[str, Any]]:
    lazy = pl.scan_parquet(source)

    def take(frame: pl.LazyFrame, count: int, salt: int, group: str) -> pl.DataFrame:
        return (
            frame.with_columns(
                pl.col("id").cast(pl.String).hash(seed=seed + salt).alias("__sample")
            )
            .sort("__sample")
            .head(count)
            .drop("__sample")
            .collect()
            .with_columns(pl.lit(group).alias("__group"))
        )

    known = take(
        lazy.filter(
            (pl.col("fetch_method") == "wayback")
            & pl.col("text").fill_null("").str.strip_chars().ne("")
        ),
        controls,
        0,
        "known_wayback",
    )
    queued = take(
        lazy.filter(pl.col("needs_archive").fill_null(False)),
        pending,
        1,
        "pending",
    )
    if known.height != controls or queued.height != pending:
        raise RuntimeError(
            f"Requested {controls} controls/{pending} pending but found "
            f"{known.height}/{queued.height}"
        )
    return pl.concat([known, queued], how="diagonal_relaxed").to_dicts()


def _raw_replay_url(original: str, timestamp: str) -> str:
    encoded = quote(original, safe=":/?&=%;,+#@")
    return f"https://web.archive.org/web/{timestamp}id_/{encoded}"


def _availability_snapshot(content: bytes | None) -> tuple[str | None, str | None, str | None]:
    if not content:
        return None, None, "empty_availability_payload"
    try:
        payload = json.loads(content.decode("utf-8-sig"))
        closest = payload.get("archived_snapshots", {}).get("closest", {})
        if not closest or not closest.get("available") or str(closest.get("status")) != "200":
            return None, None, None
        timestamp = str(closest["timestamp"])
        replay = str(closest["url"])
        match = re.search(r"/web/(\d{1,14})(?:id_)?/(.+)$", replay)
        if not match:
            return None, None, "invalid_availability_url"
        original = match.group(2)
        return _raw_replay_url(original, timestamp), timestamp, None
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None, None, "invalid_availability_payload"


def _cdx_snapshot(content: bytes | None) -> tuple[str | None, str | None, str | None]:
    if not content:
        return None, None, "empty_cdx_payload"
    try:
        data = json.loads(content.decode("utf-8-sig"))
        if not isinstance(data, list) or not data:
            raise ValueError("CDX response is not a list")
        if len(data) < 2:
            return None, None, None
        header = data[0]
        timestamp_i = header.index("timestamp")
        original_i = header.index("original")
        timestamp = str(data[1][timestamp_i])
        original = str(data[1][original_i])
        return _raw_replay_url(original, timestamp), timestamp, None
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, IndexError, TypeError):
        return None, None, "invalid_cdx_payload"


def _extract(row: dict[str, Any], response: ResponseResult, timestamp: str | None) -> dict[str, Any]:
    if not (
        response.status is not None
        and 200 <= response.status < 300
        and response.content
    ):
        return {"text_length": 0, "verification_level": "no_text", "terminal": False}
    result = scraper._build_result(
        dict(row), response.content, str(row.get("url") or ""), "wayback",
        http_status=response.status, capture_time=timestamp,
    )
    level = str(result.get("verification_level") or "no_text")
    return {
        "text_length": len(str(result.get("text") or "")),
        "verification_level": level,
        "terminal": level in {"strict_verified", "page_verified"},
    }


def run_strategy(
    row: dict[str, Any], strategy: str, client: BenchmarkClient
) -> dict[str, Any]:
    original = str(row.get("url") or "")
    target = scraper._publication_timestamp(row.get("publish_date")) or ""
    started = time.perf_counter()
    index: ResponseResult | None = None
    snapshot: ResponseResult | None = None
    timestamp: str | None = None
    lookup_error: str | None = None

    if strategy == "direct":
        snapshot = client.get(_raw_replay_url(original, target))
        final_match = re.search(r"/web/(\d{14})", str(snapshot.final_url or ""))
        timestamp = final_match.group(1) if final_match else None
    elif strategy == "availability":
        index = client.get(
            "https://archive.org/wayback/available",
            params={"url": original, "timestamp": target},
        )
        if index.status == 200 and index.error is None:
            replay_url, timestamp, lookup_error = _availability_snapshot(index.content)
            if replay_url:
                snapshot = client.get(replay_url)
    elif strategy == "cdx":
        index = client.get(
            "https://web.archive.org/cdx/search/cdx",
            params=[
                ("url", original),
                ("output", "json"),
                ("fl", "timestamp,original,statuscode,mimetype,digest"),
                ("filter", "statuscode:200"),
                ("filter", "mimetype:text/html"),
                ("collapse", "digest"),
                ("limit", "1"),
                ("sort", "closest"),
                ("closest", target),
            ],
        )
        if index.status == 200 and index.error is None:
            replay_url, timestamp, lookup_error = _cdx_snapshot(index.content)
            if replay_url:
                snapshot = client.get(replay_url)
    else:
        raise ValueError(strategy)

    extraction = _extract(row, snapshot, timestamp) if snapshot else {
        "text_length": 0, "verification_level": "no_text", "terminal": False
    }
    return {
        "id": str(row.get("id")),
        "group": row.get("__group"),
        "strategy": strategy,
        "index_status": index.status if index else None,
        "index_error": index.error if index else None,
        "index_seconds": index.elapsed_s if index else 0.0,
        "capture_found": (
            snapshot is not None
            and (
                strategy != "direct"
                or (
                    snapshot.status is not None
                    and 200 <= snapshot.status < 300
                    and bool(snapshot.content)
                    and bool(timestamp)
                )
            )
        ),
        "snapshot_status": snapshot.status if snapshot else None,
        "snapshot_error": snapshot.error if snapshot else lookup_error,
        "snapshot_seconds": snapshot.elapsed_s if snapshot else 0.0,
        "physical_requests": (
            (index.physical_requests if index else 0)
            + (snapshot.physical_requests if snapshot else 0)
        ),
        "logical_requests": 1 + int(index is not None and snapshot is not None),
        "total_seconds": time.perf_counter() - started,
        **extraction,
    }


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(fraction * len(ordered)) - 1))
    return ordered[index]


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for strategy in STRATEGIES:
        selected = [row for row in rows if row["strategy"] == strategy]
        totals = [float(row["total_seconds"]) for row in selected]
        indexes = [float(row["index_seconds"]) for row in selected if row["index_seconds"]]
        snapshots = [
            float(row["snapshot_seconds"]) for row in selected if row["snapshot_seconds"]
        ]
        errors = Counter(
            error
            for row in selected
            for error in (row.get("index_error"), row.get("snapshot_error"))
            if error
        )
        terminal = sum(bool(row["terminal"]) for row in selected)
        summary[strategy] = {
            "rows": len(selected),
            "captures_found": sum(bool(row["capture_found"]) for row in selected),
            "snapshot_http_successes": sum(
                row.get("snapshot_status") is not None
                and 200 <= int(row["snapshot_status"]) < 300
                for row in selected
            ),
            "terminal_extractions": terminal,
            "any_text": sum(int(row["text_length"]) > 0 for row in selected),
            "physical_requests": sum(int(row["physical_requests"]) for row in selected),
            "logical_requests": sum(int(row["logical_requests"]) for row in selected),
            "requests_per_terminal": (
                sum(int(row["physical_requests"]) for row in selected) / terminal
                if terminal else None
            ),
            "total_seconds_mean": statistics.fmean(totals) if totals else 0.0,
            "total_seconds_p50": _percentile(totals, 0.50),
            "total_seconds_p90": _percentile(totals, 0.90),
            "total_seconds_p95": _percentile(totals, 0.95),
            "index_seconds_p50": _percentile(indexes, 0.50),
            "index_seconds_p95": _percentile(indexes, 0.95),
            "snapshot_seconds_p50": _percentile(snapshots, 0.50),
            "snapshot_seconds_p95": _percentile(snapshots, 0.95),
            "errors": dict(errors),
            "by_group": {
                group: {
                    "rows": sum(row["group"] == group for row in selected),
                    "terminal": sum(
                        row["group"] == group and bool(row["terminal"])
                        for row in selected
                    ),
                    "any_text": sum(
                        row["group"] == group and int(row["text_length"]) > 0
                        for row in selected
                    ),
                }
                for group in ("known_wayback", "pending")
            },
        }
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=ROOT / "trafilatura_scraped.parquet")
    parser.add_argument("--controls", type=int, default=20)
    parser.add_argument("--pending", type=int, default=40)
    parser.add_argument("--threads", type=int, default=12)
    parser.add_argument("--request-rate", type=float, default=1.0)
    parser.add_argument("--connect-timeout", type=float, default=5.0)
    parser.add_argument("--read-timeout", type=float, default=60.0)
    parser.add_argument("--seed", type=int, default=20260808)
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "tests" / "wayback_strategy_benchmark.json",
    )
    args = parser.parse_args()
    if args.output.exists():
        raise SystemExit(f"Refusing to overwrite {args.output}")
    if args.request_rate <= 0 or args.threads <= 0:
        raise SystemExit("request rate and threads must be positive")

    rows = _sample(args.source, args.controls, args.pending, args.seed)
    jobs = [(row, strategy) for row in rows for strategy in STRATEGIES]
    random.Random(args.seed).shuffle(jobs)
    gate = RateGate(args.request_rate)
    client = BenchmarkClient(gate, args.connect_timeout, args.read_timeout)
    results: list[dict[str, Any]] = []
    started = time.perf_counter()
    last_heartbeat = started

    print(
        f"Starting real Wayback comparison: rows={len(rows)} jobs={len(jobs)} "
        f"threads={args.threads} rate={args.request_rate:.2f}/s "
        f"timeouts=({args.connect_timeout:.1f},{args.read_timeout:.1f})s",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        futures = {
            pool.submit(run_strategy, row, strategy, client): (row, strategy)
            for row, strategy in jobs
        }
        for future in as_completed(futures):
            results.append(future.result())
            now = time.perf_counter()
            if now - last_heartbeat >= 30 or len(results) == len(jobs):
                elapsed = now - started
                errors = sum(
                    bool(row.get("index_error") or row.get("snapshot_error"))
                    for row in results
                )
                print(
                    f"Heartbeat completed={len(results)}/{len(jobs)} "
                    f"logical_requests={client.request_count} errors={errors} "
                    f"jobs_per_second={len(results) / elapsed:.3f}",
                    flush=True,
                )
                last_heartbeat = now

    elapsed = time.perf_counter() - started
    payload = {
        "source": str(args.source.resolve()),
        "controls": args.controls,
        "pending": args.pending,
        "threads": args.threads,
        "request_rate": args.request_rate,
        "connect_timeout": args.connect_timeout,
        "read_timeout": args.read_timeout,
        "wall_seconds": elapsed,
        "logical_requests": client.request_count,
        "summary": summarize(results),
        "rows": sorted(results, key=lambda row: (row["id"], row["strategy"])),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temp = args.output.with_suffix(args.output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    temp.replace(args.output)
    print(json.dumps(payload["summary"], indent=2), flush=True)
    print(f"Saved {args.output}", flush=True)


if __name__ == "__main__":
    main()
