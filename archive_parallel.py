#!/usr/bin/env python3
"""Crash-resilient, sharded Internet Archive recovery.

This is deliberately separate from ``textile_waste_p2_scrape.py``: the live
``requests -> Playwright`` pass is already complete and is never repeated here.

The default ``all`` command performs four safe phases:

1. Freeze the canonical Parquet plus any unmerged root checkpoints into an
   immutable run-local ``base.parquet`` without modifying those sources.
2. Split only ``needs_archive=True`` rows into deterministic input shards.
3. Distribute shards across isolated Python processes.  Each process uses a
   small thread pool for network waits; every process shares one aggregate
   Wayback request gate plus independent replay, Availability and CDX circuit
   breakers.
4. Reduce verified shard results into a validated replacement Parquet.  The
   canonical file is replaced atomically only after row/ID/text checks pass.

Typical COSMOS invocation (normally use ``run_archive_parallel.sh``)::

    python -u archive_parallel.py all \
      --output-dir "$HOME/textile_waste_outputs" \
      --processes 8 --threads-per-process 4 --shards 64

Re-running the same command resumes the existing run directory.  Use ``status``
to inspect it.  No root checkpoint or shard result is automatically deleted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import multiprocessing as mp
import os
import queue
import re
import signal
import threading
import time
import traceback
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urljoin, urlparse

import polars as pl
import requests
from tqdm import tqdm

import textile_waste_p2_scrape as scraper


ARCHIVE_PIPELINE_VERSION = "2026-08-08-v9-availability-supervised"
DEFAULT_RUN_NAME = "archive_parallel_v9"
DEFAULT_SHARDS = 64
DEFAULT_PROCESSES = 8
DEFAULT_THREADS_PER_PROCESS = 4
DEFAULT_SAVE_EVERY = 1000
DEFAULT_CHECKPOINT_MAX_AGE_S = 300
DEFAULT_REQUEST_RATE = 1.0
DEFAULT_CONNECT_TIMEOUT_S = 3.0
DEFAULT_REPLAY_READ_TIMEOUT_S = 20.0
DEFAULT_AVAILABILITY_READ_TIMEOUT_S = 20.0
DEFAULT_CDX_READ_TIMEOUT_S = 60.0
DEFAULT_BREAKER_FAILURES = 5
DEFAULT_BREAKER_PAUSE_S = 30.0
DEFAULT_MAX_RETRY_AFTER_S = 120.0
DEFAULT_MAX_PROCESS_RESTARTS = 2
DEFAULT_MAX_REPLAYS = 2
DEFAULT_HEARTBEAT_INTERVAL_S = 60.0
DEFAULT_TIMEOUT_WARNING_MIN_ATTEMPTS = 10
DEFAULT_TIMEOUT_WARNING_RATE = 0.20

WAYBACK_HOST = "web.archive.org"
WAYBACK_REPLAY_ROOT = "https://web.archive.org/web"
WAYBACK_AVAILABILITY_URL = "https://archive.org/wayback/available"
WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
# The Availability endpoint rejects some otherwise valid descriptive User-Agent
# syntaxes with HTTP 400.  This stable project identifier worked in the live
# endpoint comparison and remains easy for Internet Archive to identify.
WAYBACK_USER_AGENT = "textile-waste-media-collection/2026.08"

log = logging.getLogger("textile.archive")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.parent / (
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        temp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        # Windows virus scanners and indexers can briefly hold a just-written
        # file.  A bounded retry keeps the metadata update atomic without ever
        # turning a transient local lock into a failed archive shard.
        for attempt in range(10):
            try:
                os.replace(temp, path)
                return
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        temp.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _quality(row: Mapping[str, Any]) -> int:
    return {
        "no_text": 0,
        "unverified_text": 1,
        "page_verified": 2,
        "strict_verified": 3,
    }.get(str(row.get("verification_level") or "no_text"), 0)


def _setup_logging(path: Path) -> None:
    log.setLevel(logging.DEBUG)
    log.propagate = False
    for handler in log.handlers[:]:
        handler.close()
        log.removeHandler(handler)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    log.addHandler(console)
    file_handler = logging.FileHandler(path, mode="a", encoding="utf-8-sig")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)


@dataclass(frozen=True)
class ArchiveConfig:
    request_rate: float = DEFAULT_REQUEST_RATE
    connect_timeout_s: float = DEFAULT_CONNECT_TIMEOUT_S
    replay_read_timeout_s: float = DEFAULT_REPLAY_READ_TIMEOUT_S
    availability_read_timeout_s: float = DEFAULT_AVAILABILITY_READ_TIMEOUT_S
    cdx_read_timeout_s: float = DEFAULT_CDX_READ_TIMEOUT_S
    breaker_failures: int = DEFAULT_BREAKER_FAILURES
    breaker_pause_s: float = DEFAULT_BREAKER_PAUSE_S
    max_retry_after_s: float = DEFAULT_MAX_RETRY_AFTER_S
    max_replays: int = DEFAULT_MAX_REPLAYS
    replay_root: str = WAYBACK_REPLAY_ROOT
    availability_url: str = WAYBACK_AVAILABILITY_URL
    cdx_url: str = WAYBACK_CDX_URL
    archive_host: str = WAYBACK_HOST
    user_agent: str = WAYBACK_USER_AGENT


@dataclass(frozen=True)
class HttpOutcome:
    status_code: int | None = None
    content: bytes | None = None
    final_url: str | None = None
    error: str | None = None
    elapsed_s: float = 0.0
    retry_count: int = 0
    retry_successes: int = 0
    probe: bool = False


@dataclass(frozen=True)
class Capture:
    timestamp: str
    original_url: str
    digest: str | None = None


class ProviderUnavailable(RuntimeError):
    """Raised after the half-open replay recovery probe also fails."""


class CdxUnavailable(RuntimeError):
    """Raised when CDX is disabled after its recovery probe fails."""


class AvailabilityUnavailable(RuntimeError):
    """Raised when Availability is disabled after its recovery probe fails."""


class SharedArchiveControl:
    """One request gate plus independent provider circuit breakers.

    Holding the lock during the short start-to-start wait prevents dozens of
    threads from reserving future request slots that would bypass a newly-opened
    breaker.  Slow HTTP I/O occurs after the lock is released.
    """

    def __init__(
        self,
        context: mp.context.BaseContext,
        config: ArchiveConfig,
        event_queue: Any | None = None,
    ):
        self.lock = context.Lock()
        self.telemetry_lock = context.Lock()
        self.next_request_at = context.Value("d", 0.0)
        self.replay_consecutive_failures = context.Value("i", 0)
        self.replay_pause_until = context.Value("d", 0.0)
        self.replay_probe_required = context.Value("i", 0)
        self.replay_probe_active = context.Value("i", 0)
        self.cdx_consecutive_failures = context.Value("i", 0)
        self.cdx_pause_until = context.Value("d", 0.0)
        self.cdx_probe_required = context.Value("i", 0)
        self.cdx_probe_active = context.Value("i", 0)
        self.availability_consecutive_failures = context.Value("i", 0)
        self.availability_pause_until = context.Value("d", 0.0)
        self.availability_probe_required = context.Value("i", 0)
        self.availability_probe_active = context.Value("i", 0)
        self.fatal_provider = context.Event()
        self.cdx_unavailable = context.Event()
        self.availability_unavailable = context.Event()
        self.shutdown = context.Event()
        self.http_attempts = context.Value("q", 0)
        self.replay_healthy = context.Value("q", 0)
        self.replay_failures = context.Value("q", 0)
        self.replay_breaker_openings = context.Value("q", 0)
        self.cdx_healthy = context.Value("q", 0)
        self.cdx_failures = context.Value("q", 0)
        self.cdx_breaker_openings = context.Value("q", 0)
        self.availability_healthy = context.Value("q", 0)
        self.availability_failures = context.Value("q", 0)
        self.availability_breaker_openings = context.Value("q", 0)
        self.live_attempts = {
            kind: context.Value("q", 0)
            for kind in ("direct", "availability", "cdx", "snapshot")
        }
        self.live_successes = {
            kind: context.Value("q", 0)
            for kind in ("direct", "availability", "cdx", "snapshot")
        }
        self.live_timeouts = {
            kind: context.Value("q", 0)
            for kind in ("direct", "availability", "cdx", "snapshot")
        }
        self.live_errors = {
            kind: context.Value("q", 0)
            for kind in ("direct", "availability", "cdx", "snapshot")
        }
        self.live_seconds = {
            kind: context.Value("d", 0.0)
            for kind in ("direct", "availability", "cdx", "snapshot")
        }
        self.event_queue = event_queue
        self.config = config

    @property
    def gap_s(self) -> float:
        return 1.0 / self.config.request_rate

    def _state(self, endpoint: str):
        if endpoint == "availability":
            return (
                self.availability_consecutive_failures,
                self.availability_pause_until,
                self.availability_probe_required,
                self.availability_probe_active,
                self.availability_healthy,
                self.availability_failures,
                self.availability_breaker_openings,
            )
        if endpoint == "cdx":
            return (
                self.cdx_consecutive_failures,
                self.cdx_pause_until,
                self.cdx_probe_required,
                self.cdx_probe_active,
                self.cdx_healthy,
                self.cdx_failures,
                self.cdx_breaker_openings,
            )
        return (
            self.replay_consecutive_failures,
            self.replay_pause_until,
            self.replay_probe_required,
            self.replay_probe_active,
            self.replay_healthy,
            self.replay_failures,
            self.replay_breaker_openings,
        )

    def acquire(self, endpoint: str, *, probe_owner: bool = False) -> bool:
        """Wait for a global request token and return whether it is a probe."""
        while True:
            if self.shutdown.is_set():
                raise InterruptedError("archive run is stopping")
            if endpoint == "cdx" and self.cdx_unavailable.is_set():
                raise CdxUnavailable("Wayback CDX recovery probe failed")
            if endpoint == "availability" and self.availability_unavailable.is_set():
                raise AvailabilityUnavailable("Wayback Availability recovery probe failed")
            if endpoint == "replay" and self.fatal_provider.is_set():
                raise ProviderUnavailable("Wayback replay recovery probe failed")
            with self.lock:
                _, pause_until, probe_required, probe_active, _, _, _ = self._state(endpoint)
                now = time.monotonic()
                if pause_until.value > now:
                    sleep_for = min(1.0, pause_until.value - now)
                elif probe_required.value and not probe_owner:
                    if probe_active.value:
                        sleep_for = 0.25
                    else:
                        probe_active.value = 1
                        is_probe = True
                        sleep_for = 0.0
                else:
                    is_probe = probe_owner
                    sleep_for = 0.0

                if sleep_for == 0.0:
                    now = time.monotonic()
                    rate_wait = max(0.0, self.next_request_at.value - now)
                    if rate_wait:
                        time.sleep(rate_wait)
                    self.next_request_at.value = time.monotonic() + self.gap_s
                    return is_probe
            time.sleep(sleep_for)

    def defer(self, endpoint: str, seconds: float) -> None:
        """Honor a bounded provider Retry-After across every process."""
        if seconds <= 0:
            return
        with self.lock:
            _, pause_until, _, _, _, _, _ = self._state(endpoint)
            pause_until.value = max(
                pause_until.value, time.monotonic() + seconds
            )

    def record_http_attempt(self) -> None:
        with self.http_attempts.get_lock():
            self.http_attempts.value += 1

    def observe_live(self, kind: str, outcome: HttpOutcome) -> None:
        """Publish low-cost cross-process counters for parent heartbeats."""
        if kind not in self.live_attempts:
            return
        with self.telemetry_lock:
            self.live_attempts[kind].value += 1
            self.live_seconds[kind].value += float(outcome.elapsed_s)
            if (
                outcome.error is None
                and outcome.status_code is not None
                and 200 <= outcome.status_code < 300
            ):
                self.live_successes[kind].value += 1
            if outcome.error:
                self.live_errors[kind].value += 1
                if "timeout" in outcome.error.lower():
                    self.live_timeouts[kind].value += 1

    def _emit(self, action: str, endpoint: str, **values: Any) -> None:
        if self.event_queue is None:
            return
        try:
            self.event_queue.put_nowait((
                "provider", os.getpid(), -1,
                {"action": action, "endpoint": endpoint, **values},
            ))
        except (AttributeError, OSError, ValueError):
            # Telemetry must never make a worker fail during shutdown.
            return

    def record_healthy(self, endpoint: str, *, probe: bool) -> None:
        with self.lock:
            consecutive, pause_until, probe_required, probe_active, healthy, _, _ = self._state(endpoint)
            healthy.value += 1
            consecutive.value = 0
            if probe:
                probe_required.value = 0
                probe_active.value = 0
                pause_until.value = 0.0
        if probe:
            self._emit("probe_recovered", endpoint)

    def record_failure(self, endpoint: str, *, probe: bool) -> None:
        with self.lock:
            consecutive, pause_until, probe_required, probe_active, _, failures, openings = self._state(endpoint)
            failures.value += 1
            if probe:
                probe_active.value = 0
                if endpoint == "cdx":
                    self.cdx_unavailable.set()
                elif endpoint == "availability":
                    self.availability_unavailable.set()
                else:
                    self.fatal_provider.set()
                self._emit("probe_failed", endpoint)
                return
            consecutive.value += 1
            if consecutive.value >= self.config.breaker_failures:
                openings.value += 1
                consecutive.value = 0
                probe_required.value = 1
                pause_until.value = max(
                    pause_until.value,
                    time.monotonic() + self.config.breaker_pause_s,
                )
                self._emit(
                    "breaker_open", endpoint,
                    pause_seconds=self.config.breaker_pause_s,
                    failures=self.config.breaker_failures,
                )

    def snapshot(self) -> dict[str, Any]:
        live: dict[str, Any] = {}
        with self.telemetry_lock:
            for kind in self.live_attempts:
                attempts = self.live_attempts[kind].value
                seconds = self.live_seconds[kind].value
                live[kind] = {
                    "attempts": attempts,
                    "successes": self.live_successes[kind].value,
                    "timeouts": self.live_timeouts[kind].value,
                    "errors": self.live_errors[kind].value,
                    "seconds": seconds,
                    "average_seconds": seconds / attempts if attempts else 0.0,
                }
        return {
            "http_attempts": self.http_attempts.value,
            "replay": {
                "healthy_requests": self.replay_healthy.value,
                "provider_failures": self.replay_failures.value,
                "breaker_openings": self.replay_breaker_openings.value,
                "fatal": self.fatal_provider.is_set(),
            },
            "cdx": {
                "healthy_requests": self.cdx_healthy.value,
                "provider_failures": self.cdx_failures.value,
                "breaker_openings": self.cdx_breaker_openings.value,
                "unavailable": self.cdx_unavailable.is_set(),
            },
            "availability": {
                "healthy_requests": self.availability_healthy.value,
                "provider_failures": self.availability_failures.value,
                "breaker_openings": self.availability_breaker_openings.value,
                "unavailable": self.availability_unavailable.is_set(),
            },
            "live": live,
            "fatal_provider": self.fatal_provider.is_set(),
        }


class Metrics:
    def __init__(self, initial: Mapping[str, Any] | None = None) -> None:
        initial = initial or {}
        self.lock = threading.Lock()
        self.counts: Counter[str] = Counter(initial.get("counts", {}))
        self.seconds: Counter[str] = Counter(initial.get("seconds", {}))
        self.statuses: Counter[str] = Counter(initial.get("statuses", {}))
        self.errors: Counter[str] = Counter(initial.get("errors", {}))

    def add(self, key: str, value: int = 1) -> None:
        with self.lock:
            self.counts[key] += value

    def observe_http(self, kind: str, outcome: HttpOutcome) -> None:
        with self.lock:
            self.counts[f"{kind}_requests"] += 1
            self.counts["logical_requests"] += 1
            self.counts["retry_count"] += outcome.retry_count
            self.counts["retry_successes"] += outcome.retry_successes
            self.seconds[f"{kind}_seconds"] += outcome.elapsed_s
            self.seconds["http_seconds"] += outcome.elapsed_s
            if outcome.status_code is not None:
                self.statuses[str(outcome.status_code)] += 1
            if outcome.error:
                self.errors[outcome.error] += 1

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "counts": dict(self.counts),
                "seconds": dict(self.seconds),
                "statuses": dict(self.statuses),
                "errors": dict(self.errors),
            }


def _retry_after_seconds(value: str | None, maximum: float) -> float:
    if not value:
        return 0.0
    try:
        return min(maximum, max(0.0, float(value)))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return min(maximum, max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds()))
        except (TypeError, ValueError, OverflowError):
            return 0.0


class ArchiveHttpClient:
    """Thread-safe facade backed by one requests Session per thread."""

    transient_statuses = {429, 502, 503, 504}
    provider_failure_statuses = {429, 500, 502, 503, 504}

    def __init__(self, config: ArchiveConfig, control: SharedArchiveControl, metrics: Metrics):
        self.config = config
        self.control = control
        self.metrics = metrics
        self.local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self.local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({
                "User-Agent": self.config.user_agent,
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.1",
                "Accept-Encoding": "gzip, deflate",
            })
            adapter = requests.adapters.HTTPAdapter(
                max_retries=0, pool_connections=4, pool_maxsize=4
            )
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self.local.session = session
        return session

    def _request(
        self,
        url: str,
        *,
        endpoint: str,
        params: list[tuple[str, str]] | None,
        read_timeout_s: float,
    ) -> HttpOutcome:
        network_elapsed = 0.0
        retries = 0
        probe_owner = False
        current_url = url
        current_params = params
        redirects = 0
        needs_token = True

        while True:
            if needs_token:
                probe = self.control.acquire(endpoint, probe_owner=probe_owner)
                probe_owner = probe_owner or probe
            self.control.record_http_attempt()
            request_started = time.perf_counter()
            try:
                response = self._session().get(
                    current_url,
                    params=current_params,
                    timeout=(self.config.connect_timeout_s, read_timeout_s),
                    allow_redirects=False,
                )
            except requests.exceptions.RequestException as exc:
                network_elapsed += time.perf_counter() - request_started
                # One connection-level retry was added after the real endpoint
                # benchmark showed transient ConnectionError/ProxyError events.
                # ReadTimeout is deliberately not retried: it already consumed
                # the full endpoint read allowance.
                if (
                    isinstance(exc, (requests.exceptions.ConnectionError,
                                     requests.exceptions.ConnectTimeout))
                    and not isinstance(exc, requests.exceptions.ReadTimeout)
                    and retries == 0
                    and not probe_owner
                ):
                    retries = 1
                    self.control.defer(endpoint, 2.0)
                    needs_token = True
                    continue
                return HttpOutcome(
                    error=type(exc).__name__, elapsed_s=network_elapsed,
                    retry_count=retries, probe=probe_owner,
                )
            network_elapsed += time.perf_counter() - request_started

            status = response.status_code
            if status in {301, 302, 303, 307, 308} and response.headers.get("Location"):
                redirects += 1
                if redirects > 5:
                    return HttpOutcome(
                        status_code=status, final_url=response.url,
                        error="too_many_redirects", elapsed_s=network_elapsed,
                        retry_count=retries, probe=probe_owner,
                    )
                target = urljoin(response.url, response.headers["Location"])
                if urlparse(target).hostname != self.config.archive_host:
                    return HttpOutcome(
                        status_code=status, final_url=target, error="offsite_redirect",
                        elapsed_s=network_elapsed,
                        retry_count=retries, probe=probe_owner,
                    )
                current_url, current_params = target, None
                # A same-host redirect is part of one logical Wayback request,
                # just as with a normal HTTP client.  Follow it immediately;
                # CDX-to-replay and independent row requests still need tokens.
                needs_token = False
                continue

            if status in self.transient_statuses and retries == 0 and not probe_owner:
                retries = 1
                delay = _retry_after_seconds(
                    response.headers.get("Retry-After"), self.config.max_retry_after_s
                )
                self.control.defer(endpoint, delay or 2.0)
                needs_token = True
                continue

            return HttpOutcome(
                status_code=status,
                content=response.content if response.content else None,
                final_url=response.url,
                elapsed_s=network_elapsed,
                retry_count=retries,
                retry_successes=(1 if retries and 200 <= status < 300 else 0),
                probe=probe_owner,
            )

    def _record_health(
        self, outcome: HttpOutcome, *, endpoint: str, valid_payload: bool = True
    ) -> bool:
        failed = (
            outcome.error not in {None, "offsite_redirect", "too_many_redirects"}
            or outcome.status_code in self.provider_failure_statuses
            or (outcome.status_code == 200 and not valid_payload)
        )
        if failed:
            self.control.record_failure(endpoint, probe=outcome.probe)
            return False
        self.control.record_healthy(endpoint, probe=outcome.probe)
        return True

    def replay(self, original_url: str, timestamp: str, *, kind: str) -> HttpOutcome:
        encoded = quote(original_url, safe=":/?&=%;,+#@")
        replay_url = f"{self.config.replay_root}/{timestamp}id_/{encoded}"
        outcome = self._request(
            replay_url, endpoint="replay", params=None,
            read_timeout_s=self.config.replay_read_timeout_s,
        )
        self.metrics.observe_http(kind, outcome)
        self.control.observe_live(kind, outcome)
        self._record_health(outcome, endpoint="replay")
        return outcome

    def availability(
        self, original_url: str, target: str | None
    ) -> tuple[HttpOutcome, Capture | None]:
        params = [("url", original_url)]
        if target:
            params.append(("timestamp", target))
        outcome = self._request(
            self.config.availability_url,
            endpoint="availability",
            params=params,
            read_timeout_s=self.config.availability_read_timeout_s,
        )
        capture: Capture | None = None
        valid_payload = outcome.status_code != 200
        if outcome.status_code == 200 and outcome.content is not None:
            try:
                data = json.loads(outcome.content.decode("utf-8-sig"))
                closest = data.get("archived_snapshots", {}).get("closest")
                if not isinstance(data, dict):
                    raise ValueError("Availability response is not an object")
                valid_payload = True
                if isinstance(closest, dict) and closest.get("available"):
                    timestamp = str(closest.get("timestamp") or "")
                    if not re.fullmatch(r"\d{14}", timestamp):
                        match = re.search(r"/web/(\d{14})", str(closest.get("url") or ""))
                        timestamp = match.group(1) if match else ""
                    if timestamp:
                        capture = Capture(timestamp=timestamp, original_url=original_url)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError, AttributeError):
                valid_payload = False
        if outcome.status_code == 200 and not valid_payload:
            outcome = replace(outcome, error="invalid_availability_payload")
        self.metrics.observe_http("availability", outcome)
        self.control.observe_live("availability", outcome)
        self._record_health(
            outcome, endpoint="availability", valid_payload=valid_payload
        )
        return outcome, capture

    def cdx(self, original_url: str, target: str | None) -> tuple[HttpOutcome, list[Capture]]:
        params: list[tuple[str, str]] = [
            ("url", original_url),
            ("output", "json"),
            ("fl", "timestamp,original,statuscode,mimetype,digest"),
            ("filter", "statuscode:200"),
            ("filter", "mimetype:text/html"),
            ("collapse", "digest"),
            ("limit", "4"),
        ]
        if target:
            params.extend((("sort", "closest"), ("closest", target)))
        outcome = self._request(
            self.config.cdx_url, endpoint="cdx", params=params,
            read_timeout_s=self.config.cdx_read_timeout_s,
        )
        captures: list[Capture] = []
        valid_payload = outcome.status_code != 200
        if outcome.status_code == 200 and outcome.content is not None:
            try:
                payload = outcome.content.decode("utf-8-sig")
                data = json.loads(payload)
                if not isinstance(data, list):
                    raise ValueError("CDX response is not a list")
                valid_payload = True
                if len(data) >= 2:
                    header = data[0]
                    timestamp_i = header.index("timestamp")
                    original_i = header.index("original")
                    digest_i = header.index("digest") if "digest" in header else None
                    for item in data[1:]:
                        timestamp = str(item[timestamp_i])
                        original = str(item[original_i])
                        captures.append(Capture(
                            timestamp=timestamp,
                            original_url=original,
                            digest=(str(item[digest_i]) if digest_i is not None else None),
                        ))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError, IndexError, TypeError):
                valid_payload = False
        if outcome.status_code == 200 and not valid_payload:
            outcome = replace(outcome, error="invalid_cdx_payload")
        self.metrics.observe_http("cdx", outcome)
        self.control.observe_live("cdx", outcome)
        self._record_health(outcome, endpoint="cdx", valid_payload=valid_payload)
        return outcome, captures


def _capture_timestamp(url: str | None) -> str | None:
    match = re.search(r"/web/(\d{14})", str(url or ""))
    return match.group(1) if match else None


def _distance(timestamp: str, target: str | None) -> float:
    return scraper._timestamp_distance(timestamp, target)


def _append_trace(row: Mapping[str, Any], trace: list[dict[str, Any]]) -> str:
    prior: list[Any] = []
    raw = row.get("attempt_trace")
    if isinstance(raw, str) and raw.strip():
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, list):
                prior = loaded
        except json.JSONDecodeError:
            pass
    return json.dumps([*prior, *trace], ensure_ascii=False, separators=(",", ":"))


def _attempted_methods(row: Mapping[str, Any]) -> str:
    prior = [part for part in str(row.get("attempted_methods") or "").split(",") if part]
    return ",".join([*prior, "wayback"])


def _fallback_candidate(row: Mapping[str, Any]) -> dict[str, Any] | None:
    if not _has_text(row.get("fallback_text")):
        return None
    return {
        "text": row["fallback_text"],
        "text_length": row.get("fallback_text_length") or len(str(row["fallback_text"])),
        "fetch_method": row.get("fallback_fetch_method"),
        "verification_level": "unverified_text",
        "target_verified": False,
        "scrape_status": "ok",
    }


def _preserve_longest_unverified(result: dict[str, Any], alternatives: list[Mapping[str, Any]]) -> None:
    candidates = [candidate for candidate in alternatives if scraper._is_unverified_result(candidate)]
    for alternative in alternatives:
        saved_alternative = _fallback_candidate(alternative)
        if saved_alternative is not None:
            candidates.append(saved_alternative)
    saved = _fallback_candidate(result)
    if saved is not None:
        candidates.append(saved)
    if not candidates:
        return
    longest = max(candidates, key=lambda item: len(str(item.get("text") or "")))
    if len(str(longest.get("text") or "")) > len(str(result.get("text") or "")):
        result["fallback_text"] = longest.get("text")
        result["fallback_text_length"] = len(str(longest.get("text") or ""))
        result["fallback_fetch_method"] = longest.get("fetch_method")


def _result_from_html(
    row: Mapping[str, Any],
    original_url: str,
    capture_time: str | None,
    outcome: HttpOutcome,
    run_id: str,
) -> dict[str, Any] | None:
    if not (outcome.status_code and 200 <= outcome.status_code < 300 and outcome.content):
        return None
    result = scraper._build_result(
        dict(row), outcome.content, original_url, "wayback",
        http_status=outcome.status_code,
        capture_time=capture_time,
    )
    result["pipeline_version"] = ARCHIVE_PIPELINE_VERSION
    result["pipeline_run_id"] = run_id
    result["playwright_outcome"] = row.get("playwright_outcome")
    return result


def process_archive_row(
    row: Mapping[str, Any],
    client: ArchiveHttpClient,
    run_id: str,
) -> dict[str, Any]:
    """Try direct, Availability, then CDX while preserving the best text."""
    original = str(row.get("url") or "").strip()
    target = scraper._publication_timestamp(row.get("publish_date"))
    variants = scraper._archive_url_variants([original])
    trace: list[dict[str, Any]] = []
    archive_candidates: list[dict[str, Any]] = []
    replayed_timestamps: set[str] = set()
    replay_attempts = 0
    deferred = False
    availability_definitive = False

    def remember(kind: str, variant: str, outcome: HttpOutcome) -> dict[str, Any] | None:
        nonlocal deferred
        capture_time = _capture_timestamp(outcome.final_url)
        trace.append({key: value for key, value in {
            "method": "wayback",
            "phase": kind,
            "status": outcome.status_code,
            "error": outcome.error,
            "source_url": outcome.final_url,
            "capture_time": capture_time,
        }.items() if value is not None})
        if (
            outcome.error not in {None, "offsite_redirect", "too_many_redirects"}
            or outcome.status_code in client.provider_failure_statuses
        ):
            deferred = True
            return None
        result = _result_from_html(row, variant, capture_time, outcome, run_id)
        if result is not None and _has_text(result.get("text")):
            archive_candidates.append(result)
        return result

    terminal: dict[str, Any] | None = None
    if variants and target:
        direct = client.replay(variants[0], target, kind="direct")
        replay_attempts += 1
        actual_timestamp = _capture_timestamp(direct.final_url)
        if (
            actual_timestamp
            and direct.status_code is not None
            and 200 <= direct.status_code < 300
            and direct.content
        ):
            replayed_timestamps.add(actual_timestamp)
        terminal = remember("direct", variants[0], direct)
        if scraper._is_terminal_result(terminal):
            client.metrics.add("terminal_acceptances")
        else:
            terminal = None

    if terminal is None and variants and replay_attempts < client.config.max_replays:
        # Availability is much faster and more reliable than CDX in the real
        # comparison.  Variants advance only after a valid empty response.
        for variant in variants:
            try:
                availability_outcome, capture = client.availability(variant, target)
            except AvailabilityUnavailable:
                trace.append({
                    "method": "wayback", "phase": "availability",
                    "variant": variant, "error": "availability_unavailable",
                })
                deferred = True
                break
            trace.append({key: value for key, value in {
                "method": "wayback",
                "phase": "availability",
                "variant": variant,
                "status": availability_outcome.status_code,
                "error": availability_outcome.error,
                "capture_time": capture.timestamp if capture else None,
            }.items() if value is not None})
            if (
                availability_outcome.error
                not in {None, "offsite_redirect", "too_many_redirects"}
                or availability_outcome.status_code in client.provider_failure_statuses
            ):
                deferred = True
                break
            if availability_outcome.status_code == 200:
                availability_definitive = True
            if capture is not None:
                if (
                    capture.timestamp not in replayed_timestamps
                    and replay_attempts < client.config.max_replays
                ):
                    replayed_timestamps.add(capture.timestamp)
                    replay = client.replay(
                        capture.original_url, capture.timestamp, kind="snapshot"
                    )
                    replay_attempts += 1
                    candidate = remember("snapshot", variant, replay)
                    if scraper._is_terminal_result(candidate):
                        terminal = candidate
                        client.metrics.add("terminal_acceptances")
                # A capture ends URL-variant lookup whether its extraction is
                # accepted or not.  HTTP/query-free are only no-capture fallbacks.
                break
            if (
                availability_outcome.status_code != 200
                or availability_outcome.error is not None
            ):
                break

    # CDX is service redundancy, not a duplicate lookup after a valid
    # Availability answer.  In the real comparison it added no unique terminal
    # recovery beyond direct+Availability and had a 60-second p95 lookup.
    if (
        terminal is None
        and not availability_definitive
        and variants
        and replay_attempts < client.config.max_replays
    ):
        captures: list[Capture] = []
        selected_variant: str | None = None
        for variant in variants:
            try:
                cdx_outcome, found = client.cdx(variant, target)
            except CdxUnavailable:
                trace.append({
                    "method": "wayback", "phase": "cdx",
                    "variant": variant, "error": "cdx_unavailable",
                })
                deferred = True
                break
            trace.append({key: value for key, value in {
                "method": "wayback",
                "phase": "cdx",
                "variant": variant,
                "status": cdx_outcome.status_code,
                "error": cdx_outcome.error,
            }.items() if value is not None})
            if (
                cdx_outcome.error not in {None, "offsite_redirect", "too_many_redirects"}
                or cdx_outcome.status_code in client.provider_failure_statuses
            ):
                deferred = True
                break
            if found:
                captures = found
                selected_variant = variant
                break
            # Only a valid 200 response with an empty capture list advances
            # exact -> HTTP -> query-free.  A 403/404/redirect is an attempted
            # variant, not proof that its capture list is empty.
            if cdx_outcome.status_code != 200 or cdx_outcome.error is not None:
                break

        if captures and selected_variant is not None:
            captures.sort(key=lambda item: _distance(item.timestamp, target))
            seen_digests: set[str] = set()
            for capture in captures:
                if replay_attempts >= client.config.max_replays:
                    break
                if capture.timestamp in replayed_timestamps:
                    continue
                if capture.digest and capture.digest in seen_digests:
                    continue
                if capture.digest:
                    seen_digests.add(capture.digest)
                replayed_timestamps.add(capture.timestamp)
                replay = client.replay(capture.original_url, capture.timestamp, kind="snapshot")
                replay_attempts += 1
                candidate = remember("snapshot", selected_variant, replay)
                if scraper._is_terminal_result(candidate):
                    terminal = candidate
                    client.metrics.add("terminal_acceptances")
                    break
                if deferred:
                    break

    if terminal is not None:
        result = dict(terminal)
        _preserve_longest_unverified(result, [dict(row), *archive_candidates])
        result["needs_archive"] = False
        client.metrics.add("rows_recovered")
    else:
        best = dict(row)
        for candidate in archive_candidates:
            if (
                _quality(candidate) > _quality(best)
                or (
                    _quality(candidate) == _quality(best) == 1
                    and len(str(candidate.get("text") or "")) > len(str(best.get("text") or ""))
                )
            ):
                best = candidate
        result = dict(best)
        _preserve_longest_unverified(result, [dict(row), *archive_candidates])
        result["needs_archive"] = deferred
        if deferred:
            client.metrics.add("rows_deferred")
        elif _has_text(result.get("text")):
            client.metrics.add("rows_unverified")
        else:
            client.metrics.add("rows_no_text")

    result.update({
        "id": str(row.get("id") or ""),
        "attempted_methods": _attempted_methods(row),
        "attempt_trace": _append_trace(row, trace),
        "pipeline_version": ARCHIVE_PIPELINE_VERSION,
        "pipeline_run_id": run_id,
        "completed_at": _utc_now(),
        "playwright_outcome": row.get("playwright_outcome"),
    })
    client.metrics.add("rows_completed")
    return result


def _source_files(output_dir: Path) -> list[Path]:
    canonical = output_dir / "trafilatura_scraped.parquet"
    checkpoints = sorted(output_dir.glob("_ckpt_*.parquet"))
    sources = ([canonical] if canonical.exists() else []) + checkpoints
    if not sources:
        raise FileNotFoundError(f"No canonical Parquet or checkpoints found in {output_dir}")
    return sources


def _source_snapshot(output_dir: Path) -> list[dict[str, Any]]:
    return [
        {"name": path.name, "size": path.stat().st_size, "mtime_ns": path.stat().st_mtime_ns}
        for path in _source_files(output_dir)
    ]


def _freeze_base(output_dir: Path, base_path: Path) -> dict[str, Any]:
    """Merge sources into an immutable base without changing the originals."""
    sources = _source_files(output_dir)
    lazy_frames = []
    for rank, path in enumerate(sources):
        lazy_frames.append(
            pl.scan_parquet(path).with_columns([
                pl.col("id").cast(pl.String),
                pl.lit(rank, dtype=pl.Int32).alias("__source_rank"),
                pl.lit(path.stat().st_mtime_ns, dtype=pl.Int64).alias("__source_mtime"),
                pl.lit(path.name == "trafilatura_scraped.parquet").alias("__canonical"),
            ])
        )
    merged = pl.concat(lazy_frames, how="diagonal_relaxed")
    schema = set(merged.collect_schema().names())
    if "text" not in schema:
        merged = merged.with_columns(pl.lit(None, dtype=pl.String).alias("text"))
    schema = set(merged.collect_schema().names())
    if "target_verified" not in schema:
        merged = merged.with_columns(pl.lit(False).alias("target_verified"))
    else:
        merged = merged.with_columns(pl.col("target_verified").fill_null(False).cast(pl.Boolean))
    schema = set(merged.collect_schema().names())
    if "verification_level" not in schema:
        merged = merged.with_columns(
            pl.when(pl.col("target_verified")).then(pl.lit("strict_verified"))
            .when(pl.col("text").cast(pl.String, strict=False).fill_null("").str.strip_chars().ne(""))
            .then(pl.lit("unverified_text")).otherwise(pl.lit("no_text"))
            .alias("verification_level")
        )
    else:
        merged = merged.with_columns(
            pl.col("verification_level").fill_null(
                pl.when(pl.col("target_verified")).then(pl.lit("strict_verified"))
                .when(pl.col("text").cast(pl.String, strict=False).fill_null("").str.strip_chars().ne(""))
                .then(pl.lit("unverified_text")).otherwise(pl.lit("no_text"))
            )
        )
    merged = merged.with_columns(
        pl.when(pl.col("verification_level") == "strict_verified").then(pl.lit(3))
        .when(pl.col("verification_level") == "page_verified").then(pl.lit(2))
        .when(pl.col("verification_level") == "unverified_text").then(pl.lit(1))
        .when(pl.col("target_verified")).then(pl.lit(3))
        .when(pl.col("text").cast(pl.String, strict=False).fill_null("").str.strip_chars().ne(""))
        .then(pl.lit(1)).otherwise(pl.lit(0)).cast(pl.Int8).alias("__quality")
    ).with_columns(
        pl.when(pl.col("__quality") == 1)
        .then(pl.col("text").cast(pl.String, strict=False).fill_null("").str.len_chars())
        .otherwise(pl.lit(0)).alias("__unverified_length")
    )
    # Quality is authoritative.  Among unverified rows keep the longest text;
    # otherwise prefer the newest durable source, with the canonical Parquet
    # winning a timestamp tie over an older root checkpoint.
    merged = (
        merged.sort([
            "__quality", "__unverified_length", "__source_mtime",
            "__canonical", "__source_rank",
        ])
        .unique(subset=["id"], keep="last", maintain_order=False)
        .drop([
            "__quality", "__unverified_length", "__source_mtime",
            "__canonical", "__source_rank",
        ])
    )
    schema = set(merged.collect_schema().names())
    if "needs_archive" not in schema:
        merged = merged.with_columns(
            ((pl.col("scrape_status") == "fetch_error") &
             pl.col("text").cast(pl.String, strict=False).fill_null("").str.strip_chars().eq(""))
            .alias("needs_archive")
        )
    else:
        merged = merged.with_columns(pl.col("needs_archive").fill_null(False).cast(pl.Boolean))

    temp = base_path.with_suffix(".parquet.tmp")
    temp.unlink(missing_ok=True)
    merged.sink_parquet(temp, compression="zstd", statistics=True)
    stats = pl.scan_parquet(temp).select([
        pl.len().alias("rows"),
        pl.col("id").n_unique().alias("unique_ids"),
        pl.col("text").fill_null("").str.strip_chars().ne("").sum().alias("text_rows"),
        pl.col("needs_archive").sum().alias("pending_rows"),
        (pl.col("fetch_method") == "wayback").sum().alias("wayback_rows"),
    ]).collect().row(0, named=True)
    if stats["rows"] != stats["unique_ids"]:
        raise RuntimeError(
            f"Frozen base has {stats['rows']:,} rows but {stats['unique_ids']:,} unique IDs"
        )
    canonical = output_dir / "trafilatura_scraped.parquet"
    if canonical.exists():
        canonical_text = pl.scan_parquet(canonical).select(
            pl.col("text").fill_null("").str.strip_chars().ne("").sum()
        ).collect().item()
        if stats["text_rows"] < canonical_text:
            raise RuntimeError("Frozen base would lose text relative to the canonical Parquet")
    os.replace(temp, base_path)
    return {**stats, "sha256": _sha256(base_path), "source_count": len(sources)}


def _write_input_shards(base_path: Path, input_dir: Path, shard_count: int) -> list[int]:
    input_dir.mkdir(parents=True, exist_ok=True)
    base = pl.read_parquet(base_path)
    pending = base.filter(pl.col("needs_archive").fill_null(False)).with_columns(
        (pl.col("id").cast(pl.String).hash(seed=20260807) % shard_count)
        .cast(pl.Int32).alias("__shard")
    )
    counts: list[int] = []
    for shard in range(shard_count):
        target = input_dir / f"shard_{shard:03d}.parquet"
        part = pending.filter(pl.col("__shard") == shard).drop("__shard")
        temp = target.with_suffix(".parquet.tmp")
        part.write_parquet(temp, compression="zstd", statistics=True)
        check = pl.read_parquet(temp, columns=["id"])
        if check.height != check.get_column("id").n_unique():
            raise RuntimeError(f"Input shard {shard} contains duplicate IDs")
        os.replace(temp, target)
        counts.append(part.height)
    if sum(counts) != pending.height:
        raise RuntimeError("Input shard row counts do not equal the archive queue")
    return counts


def prepare_run(output_dir: Path, run_dir: Path, shard_count: int) -> dict[str, Any]:
    manifest_path = run_dir / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("shard_count") != shard_count:
            raise RuntimeError(
                f"Existing run uses {manifest.get('shard_count')} shards; requested {shard_count}"
            )
        base_path = run_dir / "base.parquet"
        if not base_path.exists() or _sha256(base_path) != manifest.get("base", {}).get("sha256"):
            raise RuntimeError("Existing immutable base is missing or has changed")
        return manifest

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "shards").mkdir(exist_ok=True)
    base_path = run_dir / "base.parquet"
    base_stats = _freeze_base(output_dir, base_path)
    shard_counts = _write_input_shards(base_path, run_dir / "input", shard_count)
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{os.getpid()}"
    sources = _source_files(output_dir)
    manifest = {
        "run_id": run_id,
        "pipeline_version": ARCHIVE_PIPELINE_VERSION,
        "status": "prepared",
        "created_at": _utc_now(),
        "output_dir": str(output_dir.resolve()),
        "shard_count": shard_count,
        "shard_rows": shard_counts,
        "base": base_stats,
        "sources": _source_snapshot(output_dir),
    }
    _atomic_json(manifest_path, manifest)
    log.info(
        "Prepared immutable base: %s rows, %s pending, %s shards, %s source files",
        f"{base_stats['rows']:,}", f"{base_stats['pending_rows']:,}",
        shard_count, len(sources),
    )
    return manifest


def _align_row(row: Mapping[str, Any], columns: list[str]) -> dict[str, Any]:
    return {column: row.get(column) for column in columns}


def _concrete_schema(schema: Mapping[str, pl.DataType]) -> dict[str, pl.DataType]:
    """Give all-null legacy columns their intended stable output type."""
    booleans = {"target_verified", "needs_archive", "fundus_free_access"}
    integers = {"html_length", "text_length", "http_status", "fallback_text_length"}
    return {
        column: (
            dtype if dtype != pl.Null else
            pl.Boolean if column in booleans else
            pl.Int64 if column in integers else
            pl.String
        )
        for column, dtype in schema.items()
    }


def _part_files(shard_dir: Path) -> list[Path]:
    return sorted(shard_dir.glob("part_*.parquet"))


def _completed_ids(shard_dir: Path) -> set[str]:
    result = shard_dir / "result.parquet"
    paths = [result] if result.exists() else _part_files(shard_dir)
    completed: set[str] = set()
    for path in paths:
        schema = pl.read_parquet_schema(path)
        columns = ["id", "needs_archive"] if "needs_archive" in schema else ["id"]
        frame = pl.read_parquet(path, columns=columns)
        # Provider failures remain resumable inside an interrupted shard.  A
        # later successful part for the same ID safely supersedes this row.
        if "needs_archive" in frame.columns:
            frame = frame.filter(~pl.col("needs_archive").fill_null(False))
        completed.update(frame.get_column("id").cast(pl.String).to_list())
    return completed


def _resume_rows(frame: pl.DataFrame, shard_dir: Path) -> list[dict[str, Any]]:
    """Return unfinished rows enriched by their best durable deferred result."""
    completed = _completed_ids(shard_dir)
    saved: dict[str, dict[str, Any]] = {}
    for path in _part_files(shard_dir):
        for candidate in pl.read_parquet(path).to_dicts():
            row_id = str(candidate.get("id") or "")
            if row_id in completed or not bool(candidate.get("needs_archive")):
                continue
            previous = saved.get(row_id)
            candidate_quality = _quality(candidate)
            previous_quality = _quality(previous or {})
            candidate_length = len(str(candidate.get("text") or ""))
            previous_length = len(str((previous or {}).get("text") or ""))
            if (
                previous is None
                or candidate_quality > previous_quality
                or (
                    candidate_quality == previous_quality
                    and candidate_length >= previous_length
                )
            ):
                saved[row_id] = candidate

    rows: list[dict[str, Any]] = []
    for original in frame.to_dicts():
        row_id = str(original.get("id") or "")
        if row_id in completed:
            continue
        prior = saved.get(row_id)
        if prior is None:
            rows.append(original)
            continue
        original_quality = _quality(original)
        prior_quality = _quality(prior)
        original_length = len(str(original.get("text") or ""))
        prior_length = len(str(prior.get("text") or ""))
        best = dict(
            prior if (
                prior_quality > original_quality
                or (prior_quality == original_quality and prior_length >= original_length)
            ) else original
        )
        _preserve_longest_unverified(best, [original, prior])
        rows.append(best)
    return rows


def _write_part(
    shard_dir: Path,
    rows: list[dict[str, Any]],
    schema: Mapping[str, pl.DataType],
    part_index: int,
) -> Path:
    columns = list(schema)
    frame = pl.from_dicts(
        [_align_row(row, columns) for row in rows], schema=schema, strict=False
    )
    target = shard_dir / f"part_{part_index:06d}.parquet"
    temp = target.with_suffix(".parquet.tmp")
    frame.write_parquet(temp, compression="zstd", statistics=True)
    verified = pl.read_parquet(temp, columns=["id"])
    if verified.height != len(rows) or verified.get_column("id").n_unique() != len(rows):
        raise RuntimeError(f"Checkpoint verification failed for {target}")
    os.replace(temp, target)
    return target


def _finalize_shard(shard: int, input_path: Path, shard_dir: Path) -> dict[str, Any]:
    expected = pl.read_parquet(input_path, columns=["id"]).height
    parts = _part_files(shard_dir)
    if not parts and expected:
        raise RuntimeError(f"Shard {shard} has no result parts")
    if expected:
        merged = pl.concat([pl.scan_parquet(path) for path in parts], how="diagonal_relaxed")
        # Later parts are retries and therefore supersede an earlier deferred
        # result for the same ID.  Preserve concatenation order explicitly.
        merged = merged.unique(subset=["id"], keep="last", maintain_order=True)
        temp = shard_dir / "result.parquet.tmp"
        merged.sink_parquet(temp, compression="zstd", statistics=True)
        check = pl.read_parquet(temp, columns=["id"])
        if check.height != expected or check.get_column("id").n_unique() != expected:
            raise RuntimeError(
                f"Shard {shard} expected {expected} unique rows; got {check.height}"
            )
        os.replace(temp, shard_dir / "result.parquet")
    else:
        pl.read_parquet(input_path).write_parquet(shard_dir / "result.parquet")
    marker = {"shard": shard, "rows": expected, "completed_at": _utc_now()}
    _atomic_json(shard_dir / "done.json", marker)
    return marker


def _process_shard(
    shard: int,
    run_dir: Path,
    run_id: str,
    threads: int,
    save_every: int,
    checkpoint_age_s: int,
    config: ArchiveConfig,
    control: SharedArchiveControl,
    status_queue,
) -> dict[str, Any]:
    input_path = run_dir / "input" / f"shard_{shard:03d}.parquet"
    shard_dir = run_dir / "shards" / f"shard_{shard:03d}"
    shard_dir.mkdir(parents=True, exist_ok=True)
    if (shard_dir / "done.json").exists():
        return json.loads((shard_dir / "done.json").read_text(encoding="utf-8"))

    schema = _concrete_schema(pl.read_parquet_schema(input_path))
    frame = pl.read_parquet(input_path)
    rows = _resume_rows(frame, shard_dir)
    part_index = max(
        (int(path.stem.rsplit("_", 1)[1]) for path in _part_files(shard_dir)),
        default=-1,
    ) + 1
    state_path = shard_dir / "state.json"
    prior_state: dict[str, Any] = {}
    if state_path.exists():
        try:
            prior_state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            # Parts are authoritative for resume.  A missing/corrupt optional
            # metrics snapshot must never invalidate durable result rows.
            prior_state = {}
    metrics = Metrics(prior_state.get("metrics"))
    prior_extract = prior_state.get("extract_metrics", {})
    client = ArchiveHttpClient(config, control, metrics)
    scraper._reset_metrics()
    buffer: list[dict[str, Any]] = []
    last_checkpoint = time.monotonic()
    progress_pending = 0

    def extraction_metrics() -> dict[str, dict[str, int | float]]:
        current = scraper._metrics_snapshot().get("extract", {})
        methods = set(prior_extract) | set(current)
        return {
            method: dict(Counter(prior_extract.get(method, {})) +
                         Counter(current.get(method, {})))
            for method in methods
        }

    def checkpoint() -> None:
        nonlocal part_index, last_checkpoint
        if not buffer:
            return
        _write_part(shard_dir, buffer, schema, part_index)
        buffer.clear()
        part_index += 1
        last_checkpoint = time.monotonic()
        _atomic_json(shard_dir / "state.json", {
            "shard": shard,
            "saved_rows": len(_completed_ids(shard_dir)),
            "updated_at": _utc_now(),
            "metrics": metrics.snapshot(),
            "extract_metrics": extraction_metrics(),
        })

    row_iter = iter(rows)
    futures: dict[Future, str] = {}
    with ThreadPoolExecutor(max_workers=threads, thread_name_prefix=f"archive-{shard}") as pool:
        while True:
            while (
                len(futures) < threads * 2
                and not control.shutdown.is_set()
                and not control.fatal_provider.is_set()
            ):
                try:
                    row = next(row_iter)
                except StopIteration:
                    break
                future = pool.submit(process_archive_row, row, client, run_id)
                futures[future] = str(row["id"])

            if not futures:
                break
            done, _ = wait(futures, timeout=1.0, return_when=FIRST_COMPLETED)
            if not done:
                if time.monotonic() - last_checkpoint >= checkpoint_age_s:
                    checkpoint()
                continue
            for future in done:
                row_id = futures.pop(future)
                try:
                    result = future.result()
                except (InterruptedError, ProviderUnavailable):
                    continue
                except BaseException as exc:
                    raise RuntimeError(f"row {row_id} failed: {type(exc).__name__}") from exc
                buffer.append(result)
                progress_pending += 1
                if progress_pending >= 10:
                    status_queue.put(("progress", os.getpid(), shard, progress_pending))
                    progress_pending = 0
                if len(buffer) >= save_every or time.monotonic() - last_checkpoint >= checkpoint_age_s:
                    checkpoint()

    checkpoint()
    if progress_pending:
        status_queue.put(("progress", os.getpid(), shard, progress_pending))
    if control.shutdown.is_set() or control.fatal_provider.is_set():
        return {"shard": shard, "interrupted": True, "metrics": metrics.snapshot()}
    marker = _finalize_shard(shard, input_path, shard_dir)
    marker["metrics"] = metrics.snapshot()
    marker["extract_metrics"] = extraction_metrics()
    _atomic_json(shard_dir / "metrics.json", marker)
    return marker


def _worker_main(
    shard: int,
    status_queue,
    run_dir: str,
    run_id: str,
    threads: int,
    save_every: int,
    checkpoint_age_s: int,
    config: ArchiveConfig,
    control: SharedArchiveControl,
) -> None:
    status_queue.put(("started", os.getpid(), shard, 0))
    try:
        result = _process_shard(
            shard, Path(run_dir), run_id, threads, save_every,
            checkpoint_age_s, config, control, status_queue,
        )
        if result.get("interrupted"):
            status_queue.put(("interrupted", os.getpid(), shard, 0))
            return
        status_queue.put(("completed", os.getpid(), shard, result))
    except BaseException:
        status_queue.put(("failed", os.getpid(), shard, traceback.format_exc()))
        raise


def _done_shards(run_dir: Path, shard_count: int) -> set[int]:
    return {
        shard for shard in range(shard_count)
        if (run_dir / "shards" / f"shard_{shard:03d}" / "done.json").exists()
    }


def _saved_rows(run_dir: Path, shard_count: int) -> int:
    total = 0
    for shard in range(shard_count):
        directory = run_dir / "shards" / f"shard_{shard:03d}"
        if (directory / "done.json").exists():
            total += int(json.loads((directory / "done.json").read_text(encoding="utf-8"))["rows"])
        elif directory.exists():
            total += len(_completed_ids(directory))
    return total


def _aggregate_run_metrics(
    run_dir: Path,
    shard_count: int,
    provider: Mapping[str, Any],
) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    seconds: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    errors: Counter[str] = Counter()
    extract: dict[str, Counter[str]] = {}
    metric_files = 0
    for shard in range(shard_count):
        shard_dir = run_dir / "shards" / f"shard_{shard:03d}"
        path = shard_dir / "metrics.json"
        if not path.exists():
            path = shard_dir / "state.json"
        if not path.exists():
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        metrics = payload.get("metrics", {})
        counts.update(metrics.get("counts", {}))
        seconds.update(metrics.get("seconds", {}))
        statuses.update(metrics.get("statuses", {}))
        errors.update(metrics.get("errors", {}))
        for method, values in payload.get("extract_metrics", {}).items():
            target = extract.setdefault(method, Counter())
            target.update(values)
        metric_files += 1
    logical = counts.get("logical_requests", 0)
    rows = counts.get("rows_completed", 0)
    summary = {
        "shard_metric_files": metric_files,
        "counts": dict(counts),
        "seconds": dict(seconds),
        "statuses": dict(statuses),
        "errors": dict(errors),
        "extract": {method: dict(values) for method, values in extract.items()},
        "provider": dict(provider),
        "average_http_seconds": (seconds.get("http_seconds", 0.0) / logical if logical else 0.0),
        "average_seconds_per_completed_row": (
            seconds.get("http_seconds", 0.0) / rows if rows else 0.0
        ),
    }
    _atomic_json(run_dir / "run_metrics.json", summary)
    return summary


def run_supervisor(
    run_dir: Path,
    manifest: dict[str, Any],
    *,
    processes: int,
    threads: int,
    save_every: int,
    checkpoint_age_s: int,
    max_process_restarts: int,
    config: ArchiveConfig,
) -> dict[str, Any]:
    shard_count = int(manifest["shard_count"])
    completed = _done_shards(run_dir, shard_count)
    remaining = [shard for shard in range(shard_count) if shard not in completed]
    total_rows = int(manifest["base"]["pending_rows"])
    initial_rows = _saved_rows(run_dir, shard_count)
    if not remaining:
        log.info("All %s shards are already complete", shard_count)
        return manifest

    context = mp.get_context("spawn")
    status_queue = context.Queue()
    control = SharedArchiveControl(context, config, event_queue=status_queue)
    pending = deque(remaining)

    active: dict[int, int] = {}
    failures: Counter[int] = Counter()
    workers: dict[int, mp.Process] = {}
    stopping_reason: str | None = None

    def spawn_worker(shard: int) -> None:
        process = context.Process(
            target=_worker_main,
            args=(
                shard, status_queue, str(run_dir), manifest["run_id"], threads,
                save_every, checkpoint_age_s, config, control,
            ),
        )
        process.start()
        workers[process.pid] = process
        # The parent owns the assignment before the child starts.  Therefore
        # even a native crash before the child's first queue message cannot
        # lose a shard or leave the supervisor waiting forever.
        active[process.pid] = shard

    def request_stop(signum=None, frame=None) -> None:
        nonlocal stopping_reason
        stopping_reason = f"signal_{signum}" if signum is not None else "requested"
        control.shutdown.set()

    previous_handlers: dict[int, Any] = {}
    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.signal(signum, request_stop)

    for _ in range(min(processes, len(pending))):
        spawn_worker(pending.popleft())

    manifest.update({
        "status": "running",
        "started_at": manifest.get("started_at") or _utc_now(),
        "processes": processes,
        "threads_per_process": threads,
        "request_rate": config.request_rate,
    })
    _atomic_json(run_dir / "manifest.json", manifest)
    log.info(
        "Archive execution: %s shards (%s active processes x %s HTTP threads), "
        "global rate %.2f requests/s",
        shard_count, processes, threads, config.request_rate,
    )

    try:
        last_manifest_rows = -1
        last_manifest_at = 0.0
        last_heartbeat_at = time.monotonic()
        last_heartbeat_rows = initial_rows
        last_heartbeat_live = control.snapshot()["live"]
        with tqdm(total=total_rows, initial=initial_rows, desc="Archive", unit="url") as pbar:
            while len(completed) < shard_count and not control.shutdown.is_set():
                if control.fatal_provider.is_set():
                    stopping_reason = "provider_recovery_probe_failed"
                    control.shutdown.set()
                    break
                try:
                    kind, pid, shard, payload = status_queue.get(timeout=0.5)
                    if kind == "started":
                        active[pid] = shard
                        log.info("Process %s started shard %03d", pid, shard)
                    elif kind == "progress":
                        # A process can die after reporting progress but before
                        # its next durable checkpoint.  Its replacement then
                        # legitimately reports those rows again; cap the visual
                        # counter so crash recovery can never show >100%.
                        remaining = max(0, total_rows - int(pbar.n))
                        pbar.update(min(int(payload), remaining))
                    elif kind == "completed":
                        completed.add(shard)
                        active.pop(pid, None)
                        log.info(
                            "Process %s completed shard %03d (%s/%s shards)",
                            pid, shard, len(completed), shard_count,
                        )
                    elif kind == "failed":
                        log.error("Process %s failed shard %03d\n%s", pid, shard, payload)
                    elif kind == "interrupted":
                        active.pop(pid, None)
                    elif kind == "provider":
                        action = payload.get("action")
                        endpoint = payload.get("endpoint")
                        if action == "breaker_open":
                            log.warning(
                                "Wayback %s circuit breaker opened after %s provider "
                                "failures; queued %s calls pause for %.0fs",
                                endpoint, payload.get("failures"), endpoint,
                                float(payload.get("pause_seconds") or 0),
                            )
                        elif action == "probe_failed":
                            if endpoint == "replay":
                                log.error(
                                    "Wayback replay recovery probe failed; stopping "
                                    "safely so pending rows remain resumable"
                                )
                            else:
                                log.warning(
                                    "Wayback %s became unavailable after its recovery "
                                    "probe failed; this endpoint is disabled for the run",
                                    endpoint,
                                )
                        elif action == "probe_recovered":
                            log.info("Wayback %s recovery probe succeeded; work resumed", endpoint)
                except queue.Empty:
                    pass

                for pid, process in list(workers.items()):
                    if process.is_alive():
                        continue
                    process.join(timeout=0)
                    workers.pop(pid)
                    shard = active.pop(pid, None)
                    if shard is not None and shard not in completed and not control.shutdown.is_set():
                        if (run_dir / "shards" / f"shard_{shard:03d}" / "done.json").exists():
                            completed.add(shard)
                        else:
                            failures[shard] += 1
                            if failures[shard] > max_process_restarts:
                                stopping_reason = f"shard_{shard:03d}_repeated_process_failure"
                                control.shutdown.set()
                                break
                            log.warning(
                                "Restarting shard %03d after worker exit (%s/%s)",
                                shard, failures[shard], max_process_restarts,
                            )
                            pending.appendleft(shard)
                while (
                    not control.shutdown.is_set()
                    and pending
                    and len(workers) < processes
                ):
                    spawn_worker(pending.popleft())

                now = time.monotonic()
                if now - last_heartbeat_at >= DEFAULT_HEARTBEAT_INTERVAL_S:
                    provider = control.snapshot()
                    live = provider["live"]
                    elapsed = max(0.001, now - last_heartbeat_at)
                    completed_now = int(pbar.n)
                    current_rate = max(
                        0, completed_now - last_heartbeat_rows
                    ) / elapsed
                    endpoint_parts: list[str] = []
                    recent_attempts = 0
                    recent_timeouts = 0
                    for endpoint in ("direct", "availability", "cdx", "snapshot"):
                        values = live[endpoint]
                        previous = last_heartbeat_live[endpoint]
                        delta_attempts = int(values["attempts"]) - int(previous["attempts"])
                        delta_timeouts = int(values["timeouts"]) - int(previous["timeouts"])
                        recent_attempts += max(0, delta_attempts)
                        recent_timeouts += max(0, delta_timeouts)
                        endpoint_parts.append(
                            f"{endpoint}: attempts={values['attempts']} "
                            f"successes={values['successes']} "
                            f"timeouts={values['timeouts']} avg={values['average_seconds']:.2f}s"
                        )
                    log.info(
                        "Heartbeat rows=%s/%s current_rate=%.2f rows/s active=%s "
                        "shards_done=%s/%s | %s",
                        f"{completed_now:,}", f"{total_rows:,}", current_rate,
                        len(workers), len(completed), shard_count,
                        " | ".join(endpoint_parts),
                    )
                    if (
                        recent_attempts >= DEFAULT_TIMEOUT_WARNING_MIN_ATTEMPTS
                        and recent_timeouts / recent_attempts
                        >= DEFAULT_TIMEOUT_WARNING_RATE
                    ):
                        log.warning(
                            "Abnormal Wayback timeout frequency: %s/%s requests "
                            "(%.1f%%) during the last heartbeat interval",
                            recent_timeouts, recent_attempts,
                            100.0 * recent_timeouts / recent_attempts,
                        )
                    last_heartbeat_at = now
                    last_heartbeat_rows = completed_now
                    last_heartbeat_live = live

                if (
                    int(pbar.n) - last_manifest_rows >= 1000
                    or time.monotonic() - last_manifest_at >= 300
                ):
                    manifest["completed_shards"] = sorted(completed)
                    manifest["updated_at"] = _utc_now()
                    manifest["provider"] = control.snapshot()
                    _atomic_json(run_dir / "manifest.json", manifest)
                    last_manifest_rows = int(pbar.n)
                    last_manifest_at = time.monotonic()

        if len(completed) != shard_count:
            _aggregate_run_metrics(run_dir, shard_count, control.snapshot())
            manifest.update({
                "status": "interrupted" if stopping_reason and stopping_reason.startswith("signal_") else "failed",
                "stopped_at": _utc_now(),
                "reason": stopping_reason or "shutdown",
                "completed_shards": sorted(completed),
                "provider": control.snapshot(),
                "metrics_path": "run_metrics.json",
            })
            _atomic_json(run_dir / "manifest.json", manifest)
            raise RuntimeError(
                f"Archive run stopped safely: {manifest['reason']}; committed shard checkpoints will resume"
            )
        final_metrics = _aggregate_run_metrics(run_dir, shard_count, control.snapshot())
        manifest.update({
            "status": "processed",
            "processed_at": _utc_now(),
            "completed_shards": sorted(completed),
            "provider": control.snapshot(),
            "metrics_path": "run_metrics.json",
        })
        _atomic_json(run_dir / "manifest.json", manifest)
        log.info(
            "Archive metrics: rows=%s recovered=%s deferred=%s logical_requests=%s "
            "average_http=%.2fs",
            f"{final_metrics['counts'].get('rows_completed', 0):,}",
            f"{final_metrics['counts'].get('rows_recovered', 0):,}",
            f"{final_metrics['counts'].get('rows_deferred', 0):,}",
            f"{final_metrics['counts'].get('logical_requests', 0):,}",
            final_metrics["average_http_seconds"],
        )
        return manifest
    finally:
        control.shutdown.set()
        deadline = time.monotonic() + 30
        for process in workers.values():
            process.join(timeout=max(0.0, deadline - time.monotonic()))
        for process in workers.values():
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def reduce_run(output_dir: Path, run_dir: Path, manifest: dict[str, Any]) -> Path:
    shard_count = int(manifest["shard_count"])
    completed = _done_shards(run_dir, shard_count)
    if len(completed) != shard_count:
        raise RuntimeError(
            f"Refusing reduction: {len(completed)}/{shard_count} shards are complete"
        )
    base_path = run_dir / "base.parquet"
    if _sha256(base_path) != manifest["base"]["sha256"]:
        raise RuntimeError("Immutable base checksum changed; refusing reduction")
    if _source_snapshot(output_dir) != manifest.get("sources"):
        raise RuntimeError(
            "Canonical Parquet/checkpoints changed after preparation; refusing to overwrite newer work"
        )
    results = [
        run_dir / "shards" / f"shard_{shard:03d}" / "result.parquet"
        for shard in range(shard_count)
    ]
    delta = pl.concat([pl.scan_parquet(path) for path in results], how="diagonal_relaxed")
    delta_check = delta.select([
        pl.len().alias("rows"), pl.col("id").cast(pl.String).n_unique().alias("unique_ids")
    ]).collect().row(0, named=True)
    if delta_check["rows"] != manifest["base"]["pending_rows"]:
        raise RuntimeError("Shard result count does not equal the frozen archive queue")
    if delta_check["rows"] != delta_check["unique_ids"]:
        raise RuntimeError("Shard results contain duplicate IDs")

    combined = pl.concat([
        pl.scan_parquet(base_path).with_columns(pl.lit(0).alias("__rank")),
        delta.with_columns(pl.lit(1).alias("__rank")),
    ], how="diagonal_relaxed")
    merged = (
        combined.sort("__rank")
        .unique(subset=["id"], keep="last", maintain_order=False)
        .drop("__rank")
    )
    temp = output_dir / ".trafilatura_scraped.archive-new.parquet.tmp"
    temp.unlink(missing_ok=True)
    merged.sink_parquet(temp, compression="zstd", statistics=True)
    final_stats = pl.scan_parquet(temp).select([
        pl.len().alias("rows"),
        pl.col("id").cast(pl.String).n_unique().alias("unique_ids"),
        pl.col("text").fill_null("").str.strip_chars().ne("").sum().alias("text_rows"),
        pl.col("needs_archive").fill_null(False).sum().alias("pending_rows"),
        (pl.col("fetch_method") == "wayback").sum().alias("wayback_rows"),
    ]).collect().row(0, named=True)
    if final_stats["rows"] != manifest["base"]["rows"]:
        raise RuntimeError("Reduced row count differs from the immutable base")
    if final_stats["rows"] != final_stats["unique_ids"]:
        raise RuntimeError("Reduced output does not contain unique IDs")
    if final_stats["text_rows"] < manifest["base"]["text_rows"]:
        raise RuntimeError("Reduced output would lose recovered text")

    canonical = output_dir / "trafilatura_scraped.parquet"
    backup = output_dir / "trafilatura_scraped.previous.parquet"
    if canonical.exists():
        os.replace(canonical, backup)
    try:
        os.replace(temp, canonical)
    except BaseException:
        if backup.exists() and not canonical.exists():
            os.replace(backup, canonical)
        raise
    manifest.update({
        "status": "completed",
        "completed_at": _utc_now(),
        "final": final_stats,
        "canonical_path": str(canonical.resolve()),
        "backup_path": str(backup.resolve()) if backup.exists() else None,
    })
    _atomic_json(run_dir / "manifest.json", manifest)
    log.info(
        "Atomic reduction complete: %s rows, %s text, %s still pending",
        f"{final_stats['rows']:,}", f"{final_stats['text_rows']:,}",
        f"{final_stats['pending_rows']:,}",
    )
    return canonical


def print_status(run_dir: Path) -> None:
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        print(f"No prepared archive run at {run_dir}")
        return
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    shard_count = int(manifest["shard_count"])
    done = _done_shards(run_dir, shard_count)
    saved = _saved_rows(run_dir, shard_count)
    pending = int(manifest["base"]["pending_rows"])
    print(f"Run:               {manifest['run_id']}")
    print(f"Status:            {manifest.get('status')}")
    print(f"Base rows:         {manifest['base']['rows']:,}")
    print(f"Archive queue:     {pending:,}")
    print(f"Completed shards:  {len(done):,}/{shard_count:,}")
    print(f"Checkpointed rows: {saved:,}/{pending:,} ({(100 * saved / pending if pending else 100):.2f}%)")
    if manifest.get("provider"):
        print("Provider metrics:  " + json.dumps(manifest["provider"], sort_keys=True))


def _validate_args(args, parser: argparse.ArgumentParser) -> None:
    positive = {
        "processes": args.processes,
        "threads-per-process": args.threads_per_process,
        "shards": args.shards,
        "save-every": args.save_every,
        "checkpoint-max-age": args.checkpoint_max_age,
        "request-rate": args.request_rate,
        "connect-timeout": args.connect_timeout,
        "replay-read-timeout": args.replay_read_timeout,
        "availability-read-timeout": args.availability_read_timeout,
        "cdx-read-timeout": args.cdx_read_timeout,
        "breaker-failures": args.breaker_failures,
        "breaker-pause": args.breaker_pause,
    }
    for name, value in positive.items():
        if value <= 0:
            parser.error(f"--{name} must be greater than zero")
    if args.processes > args.shards:
        parser.error("--processes cannot exceed --shards")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", args.run_name):
        parser.error("--run-name may contain only letters, digits, dot, underscore, and hyphen")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sharded, multiprocessing archive recovery without live refetching.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("command", nargs="?", choices=("all", "prepare", "run", "reduce", "status"), default="all")
    parser.add_argument("--output-dir", type=Path, default=Path("."))
    parser.add_argument("--run-name", default=DEFAULT_RUN_NAME)
    parser.add_argument("--shards", type=int, default=DEFAULT_SHARDS)
    parser.add_argument("--processes", type=int, default=DEFAULT_PROCESSES)
    parser.add_argument("--threads-per-process", type=int, default=DEFAULT_THREADS_PER_PROCESS)
    parser.add_argument("--save-every", type=int, default=DEFAULT_SAVE_EVERY)
    parser.add_argument("--checkpoint-max-age", type=int, default=DEFAULT_CHECKPOINT_MAX_AGE_S)
    parser.add_argument("--request-rate", type=float, default=DEFAULT_REQUEST_RATE,
                        help="Aggregate Wayback request starts per second across every process")
    parser.add_argument("--connect-timeout", type=float, default=DEFAULT_CONNECT_TIMEOUT_S)
    parser.add_argument("--replay-read-timeout", type=float, default=DEFAULT_REPLAY_READ_TIMEOUT_S)
    parser.add_argument(
        "--availability-read-timeout",
        type=float,
        default=DEFAULT_AVAILABILITY_READ_TIMEOUT_S,
    )
    parser.add_argument("--cdx-read-timeout", type=float, default=DEFAULT_CDX_READ_TIMEOUT_S)
    parser.add_argument("--breaker-failures", type=int, default=DEFAULT_BREAKER_FAILURES)
    parser.add_argument("--breaker-pause", type=float, default=DEFAULT_BREAKER_PAUSE_S)
    parser.add_argument("--max-process-restarts", type=int, default=DEFAULT_MAX_PROCESS_RESTARTS)
    args = parser.parse_args()
    _validate_args(args, parser)

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = output_dir / "archive_runs" / args.run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    _setup_logging(run_dir / "archive_parallel.log")
    log.info("Archive pipeline %s command=%s", ARCHIVE_PIPELINE_VERSION, args.command)

    if args.command == "status":
        print_status(run_dir)
        return

    config = ArchiveConfig(
        request_rate=args.request_rate,
        connect_timeout_s=args.connect_timeout,
        replay_read_timeout_s=args.replay_read_timeout,
        availability_read_timeout_s=args.availability_read_timeout,
        cdx_read_timeout_s=args.cdx_read_timeout,
        breaker_failures=args.breaker_failures,
        breaker_pause_s=args.breaker_pause,
    )
    log.info(
        "Archive routing: direct replay -> Availability -> snapshot; CDX -> "
        "snapshot only when Availability is unavailable; exact -> "
        "HTTP-for-HTTPS -> query-free only after no capture"
    )
    log.info(
        "Archive network settings: aggregate_rate=%.2f requests/s connect=%.1fs "
        "replay_read=%.1fs availability_read=%.1fs cdx_read=%.1fs "
        "max_snapshot_downloads=%s retry=one(connection/connect-timeout or "
        "429/502/503/504; never read-timeout)",
        config.request_rate, config.connect_timeout_s,
        config.replay_read_timeout_s, config.availability_read_timeout_s,
        config.cdx_read_timeout_s, config.max_replays,
    )
    log.info(
        "Circuit breakers: %s consecutive failures -> %.0fs pause -> one probe; "
        "heartbeat=%.0fs timeout_warning=%s attempts at >=%.0f%%",
        config.breaker_failures, config.breaker_pause_s,
        DEFAULT_HEARTBEAT_INTERVAL_S, DEFAULT_TIMEOUT_WARNING_MIN_ATTEMPTS,
        100 * DEFAULT_TIMEOUT_WARNING_RATE,
    )
    lock = scraper._RunLock(output_dir / "scrape_run.lock")
    lock.acquire()
    try:
        manifest = prepare_run(output_dir, run_dir, args.shards)
        if args.command == "prepare":
            return
        if manifest.get("status") == "completed":
            log.info(
                "Archive run %s is already reduced and complete; canonical output is unchanged",
                manifest["run_id"],
            )
            return
        if args.command in {"all", "run"}:
            manifest = run_supervisor(
                run_dir, manifest,
                processes=args.processes,
                threads=args.threads_per_process,
                save_every=args.save_every,
                checkpoint_age_s=args.checkpoint_max_age,
                max_process_restarts=args.max_process_restarts,
                config=config,
            )
        if args.command in {"all", "reduce"}:
            reduce_run(output_dir, run_dir, manifest)
    finally:
        lock.release()


if __name__ == "__main__":
    mp.freeze_support()
    main()
