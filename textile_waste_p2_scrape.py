#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recover full article text from URLs in ``mediacloud_combined.csv``.

Requests, Playwright, and Wayback provide a bounded three-source fetch cascade.
Structured article bodies, optional publisher-specific Fundus parsers, scoped
Trafilatura, and whole-page Trafilatura extract every nonempty candidate without
length or phrase filtering. Atomic checkpoints preserve progress across crashes,
and final UTF-8 results are written to Parquet, CSV, and JSON without text limits.

Run ``python textile_waste_p2_scrape.py`` or use ``--help`` for options.
"""

# ── Imports ────────────────────────────────────────────────────────────────

import argparse
import ctypes
import html as html_lib
import json
import logging
import os
import queue
import re
import sys
import threading
import time
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, TypedDict
from urllib.parse import quote, urljoin, urlparse, urlunparse

import pandas as pd
import polars as pl
import pyarrow.parquet as pq
import requests
import trafilatura
from lxml import html as lxml_html
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from trafilatura.metadata import extract_metadata as _traf_extract_metadata
from trafilatura.settings import use_config
from urllib3.util.retry import Retry

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

try:
    from fundus import PublisherCollection
    FUNDUS_AVAILABLE = True
except ImportError:
    PublisherCollection = None
    FUNDUS_AVAILABLE = False


# ── Configuration ──────────────────────────────────────────────────────────

PIPELINE_VERSION = "2026-08-04-v6-three-source"
_RUN_ID = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"

def _system_resources() -> tuple[int, float, float]:
    """Return logical CPUs plus total/available physical memory in GiB."""
    cpus = os.cpu_count() or 4
    if sys.platform == "win32":
        class _MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = _MemoryStatusEx()
        status.dwLength = ctypes.sizeof(status)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return cpus, status.ullTotalPhys / 2**30, status.ullAvailPhys / 2**30
    try:
        # ``sysconf`` is absent from the Windows type stubs and from Windows
        # itself.  Resolve it only for the POSIX fallback.
        sysconf = getattr(os, "sysconf", None)
        if not callable(sysconf):
            return cpus, 8.0, 4.0
        page_size = sysconf("SC_PAGE_SIZE")
        total = page_size * sysconf("SC_PHYS_PAGES") / 2**30
        available = page_size * sysconf("SC_AVPHYS_PAGES") / 2**30
        return cpus, total, available
    except (AttributeError, OSError, ValueError):
        return cpus, 8.0, 4.0


_CPU_COUNT, _RAM_TOTAL_GIB, _RAM_AVAILABLE_GIB = _system_resources()

# Playwright stays memory-aware because every worker owns an independent browser
# process. Requests and Wayback are I/O-bound.
WORKERS_L1 = min(32, max(8, round(_CPU_COUNT * 2.25)))
WORKERS_L3 = 2 if _RAM_TOTAL_GIB >= 12 and _RAM_AVAILABLE_GIB >= 2 else 1
WORKERS_L4 = 4 if _CPU_COUNT >= 8 else 2

SCRAPE_SAVE_EVERY = 1000
CHECKPOINT_MAX_AGE_S = 300
REQUEST_CONNECT_TIMEOUT_S = 3
REQUEST_READ_TIMEOUT_S = 7
PLAYWRIGHT_TIMEOUT_S = 15
WAYBACK_CDX_READ_TIMEOUT_S = 10
WAYBACK_SNAPSHOT_READ_TIMEOUT_S = 15
LIVE_DOMAIN_MIN_GAP_S = 0.5
WAYBACK_MIN_GAP_S = 2.5
WAYBACK_MAX_SNAPSHOTS = 2
WAYBACK_BREAKER_FAILURES = 5
WAYBACK_BREAKER_PAUSE_S = 60

# Corporate Windows policies commonly block Playwright executables under
# AppData. Prefer an installed Chrome/Edge channel there, then fall back to the
# bundled Chromium. Other platforms use bundled Chromium directly.
PLAYWRIGHT_CHANNELS: tuple[str | None, ...] = (
    ("chrome", "msedge", None) if sys.platform == "win32" else (None,)
)

# Set to True by --fetch-only; read (never written) inside threads → module-level is safe.
_FETCH_ONLY: bool = False

# Full Chrome-like header set for layer 1 (requests).
# Missing or inconsistent Sec-Fetch-* / Accept headers are a primary bot signal.
_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language":           "en-US,en;q=0.9",
    "Accept-Encoding":           "gzip, deflate, br",
    "Sec-Fetch-Site":            "none",
    "Sec-Fetch-Mode":            "navigate",
    "Sec-Fetch-User":            "?1",
    "Sec-Fetch-Dest":            "document",
    "Upgrade-Insecure-Requests": "1",
    "sec-ch-ua":                 '"Chromium";v="149", "Google Chrome";v="149", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile":          "?0",
    "sec-ch-ua-platform":        '"Windows"',
}

# HTTP responses are already loaded fully into memory by the fetch layers, so
# keep Trafilatura from applying its separate default 20 MB input threshold.
_TRAF_CONFIG = use_config()
_TRAF_CONFIG.set("DEFAULT", "MAX_FILE_SIZE", str(sys.maxsize))

_CONSENT_BUTTON_RE = re.compile(
    r"^(?:accept(?:\s+all)?|agree|allow\s+all|i\s+agree|"
    r"alle\s+akzeptieren|akzeptieren|zustimmen|"
    r"acceptera\s+alla|godkänn\s+alla|tillåt\s+alla)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class _FetchOutcome:
    """One observable fetch attempt, including failures and archive dates."""

    content: bytes | str | None = None
    final_url: str | None = None
    status_code: int | None = None
    error: str | None = None
    capture_time: str | None = None
    source_url: str | None = None
    retry_count: int = 0
    retry_successes: int = 0

    def __iter__(self):
        """Keep two-value unpacking compatible with the original fetch helpers."""
        yield self.content
        yield self.final_url


class _ScrapeJob(TypedDict):
    """Mutable state for one ID as it moves through the bounded cascade."""

    row: dict[str, Any]
    attempts: list[str]
    trace: list[dict[str, Any]]
    archive_urls: list[str]
    fallback: dict[str, Any] | None
    had_fetch: bool
    last_outcome: _FetchOutcome | None
    needs_archive: bool


@dataclass(frozen=True)
class _ArchiveCapture:
    timestamp: str
    original_url: str
    fetch_url: str
    digest: str | None = None


def _error_name(exc: BaseException) -> str:
    """Return a compact, stable diagnostic without storing exception messages."""
    return type(exc).__name__


def _coerce_fetch_outcome(value) -> _FetchOutcome:
    """Accept legacy two-tuples used by callers/tests as well as rich outcomes."""
    if isinstance(value, _FetchOutcome):
        return value
    if isinstance(value, tuple) and len(value) == 2:
        return _FetchOutcome(content=value[0], final_url=value[1])
    raise TypeError(f"Unexpected fetch result: {type(value).__name__}")


# ── Logging ────────────────────────────────────────────────────────────────

def setup_logging(log_path: Path) -> None:
    """Append console/file logs without duplicating handlers on repeated calls."""
    logger = logging.getLogger("textile")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8-sig", delay=False)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.info(
        "\n%s\nStarting scraper run %s (pipeline %s)\n%s",
        "=" * 72,
        _RUN_ID,
        PIPELINE_VERSION,
        "=" * 72,
    )


log = logging.getLogger("textile")


# ── Part 2: Trafilatura scraping ───────────────────────────────────────────

# One Session per thread — requests.Session is not thread-safe.
_thread_local = threading.local()


def _get_session(source: str = "requests") -> requests.Session:
    """Return one source-specific requests session per worker thread."""
    sessions = getattr(_thread_local, "sessions", None)
    if sessions is None:
        sessions = {}
        _thread_local.sessions = sessions
    if source not in sessions:
        s = requests.Session()
        s.headers.update(_BROWSER_HEADERS)
        retry = Retry(
            total=1,
            connect=1 if source == "requests" else 0,
            read=0,
            status=1,
            backoff_factor=0,
            status_forcelist=(429, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=32,
            pool_maxsize=32,
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        sessions[source] = s
    return sessions[source]


# Per-domain rate limiting: one Lock per domain + shared last-access timestamp.
_domain_locks:    dict[str, threading.Lock] = {}
_domain_last_hit: dict[str, float]          = {}
_registry_lock = threading.Lock()


def _get_domain_lock(domain: str) -> threading.Lock:
    with _registry_lock:
        if domain not in _domain_locks:
            _domain_locks[domain] = threading.Lock()
        return _domain_locks[domain]


# ── Per-layer fetch functions ──────────────────────────────────────────────

def _enforce_rate_limit(domain: str, gap_s: float = LIVE_DOMAIN_MIN_GAP_S) -> None:
    """Acquire a domain lock and enforce a shared start-to-start request gap."""
    with _get_domain_lock(domain):
        elapsed = time.monotonic() - _domain_last_hit.get(domain, 0.0)
        if elapsed < gap_s:
            time.sleep(gap_s - elapsed)
        _domain_last_hit[domain] = time.monotonic()


def _response_retries(resp: requests.Response) -> tuple[int, int]:
    """Return retries used and retries that preceded a successful response."""
    history = getattr(getattr(resp, "raw", None), "retries", None)
    count = len(getattr(history, "history", ()) or ())
    return count, count if 200 <= resp.status_code < 300 else 0


def _l1_fetch(url: str) -> _FetchOutcome:
    """Layer 1 — requests with full browser headers and undecoded response bytes."""
    domain = urlparse(url).netloc
    with _get_domain_lock(domain):
        elapsed = time.monotonic() - _domain_last_hit.get(domain, 0.0)
        if elapsed < LIVE_DOMAIN_MIN_GAP_S:
            time.sleep(LIVE_DOMAIN_MIN_GAP_S - elapsed)
        try:
            resp = _get_session("requests").get(
                url,
                timeout=(REQUEST_CONNECT_TIMEOUT_S, REQUEST_READ_TIMEOUT_S),
                allow_redirects=True,
            )
            status = resp.status_code
            retry_count, retry_successes = _response_retries(resp)
            if 200 <= status < 300 and resp.content:
                outcome = _FetchOutcome(
                    content=resp.content,
                    final_url=str(resp.url),
                    status_code=status,
                    source_url=url,
                    retry_count=retry_count,
                    retry_successes=retry_successes,
                )
            else:
                outcome = _FetchOutcome(
                    final_url=str(resp.url),
                    status_code=status,
                    error=f"http_{status}" if status else "empty_response",
                    source_url=url,
                    retry_count=retry_count,
                )
        except requests.exceptions.RequestException as exc:
            outcome = _FetchOutcome(error=_error_name(exc), source_url=url)
        _domain_last_hit[domain] = time.monotonic()
    return outcome


# Playwright is not thread-safe. Each L3 worker therefore owns one lazily
# started Playwright instance and browser for the lifetime of that thread.
# The domain lock is released before page.goto() so a slow browser render
# does not block other threads from hitting the same domain.
_pw_thread_local = threading.local()
_pw_launch_lock = threading.Lock()
_pw_launch_failure: str | None = None


def _get_pw_browser():
    global _pw_launch_failure
    if not hasattr(_pw_thread_local, "browser"):
        # A sandbox/policy launch denial is process-wide, not URL-specific.
        # Remember it so every queued URL does not repeatedly start Playwright.
        with _pw_launch_lock:
            if _pw_launch_failure is not None:
                raise RuntimeError(_pw_launch_failure)
            instance = None
            browser = None
            launch_errors = []
            try:
                instance = sync_playwright().start()
                for channel in PLAYWRIGHT_CHANNELS:
                    try:
                        browser = instance.chromium.launch(
                            headless=True, channel=channel
                        )
                        break
                    except Exception as exc:
                        launch_errors.append(f"{channel or 'bundled chromium'}: {exc}")
                if browser is None:
                    raise RuntimeError(
                        "No Playwright browser could be launched: " + " | ".join(launch_errors)
                    )
            except Exception as exc:
                if instance is not None:
                    try:
                        instance.stop()
                    except Exception:
                        pass
                _pw_launch_failure = f"Playwright launch unavailable: {_error_name(exc)}"
                raise RuntimeError(_pw_launch_failure) from exc
            _pw_thread_local.instance = instance
            _pw_thread_local.browser = browser
    return _pw_thread_local.browser


def _get_pw_context():
    """Return one isolated, cookie-preserving context owned by this worker."""
    if not hasattr(_pw_thread_local, "context"):
        context = _get_pw_browser().new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=_BROWSER_HEADERS["User-Agent"],
            locale="en-US",
            service_workers="block",
        )
        context.route(
            "**/*",
            lambda route: (
                route.abort()
                if route.request.resource_type in {"image", "media", "font"}
                else route.continue_()
            ),
        )
        _pw_thread_local.context = context
    return _pw_thread_local.context


def _close_pw_browser() -> None:
    """Close the Playwright objects owned by the current worker thread."""
    context = getattr(_pw_thread_local, "context", None)
    browser = getattr(_pw_thread_local, "browser", None)
    instance = getattr(_pw_thread_local, "instance", None)
    try:
        if context is not None:
            context.close()
    except Exception as exc:
        log.debug(f"playwright context cleanup failed: {exc!r}")
    finally:
        if hasattr(_pw_thread_local, "context"):
            del _pw_thread_local.context
    try:
        if browser is not None:
            browser.close()
    except Exception as exc:
        log.debug(f"playwright browser cleanup failed: {exc!r}")
    finally:
        if hasattr(_pw_thread_local, "browser"):
            del _pw_thread_local.browser
    try:
        if instance is not None:
            instance.stop()
    except Exception as exc:
        log.debug(f"playwright instance cleanup failed: {exc!r}")
    finally:
        if hasattr(_pw_thread_local, "instance"):
            del _pw_thread_local.instance


def _l3_fetch(url: str) -> _FetchOutcome:
    """Layer 3 — headless Chromium. Rate limit enforced before fetch (lock not held during render)."""
    _enforce_rate_limit(urlparse(url).netloc)
    if not PLAYWRIGHT_AVAILABLE:
        return _FetchOutcome(error="dependency_unavailable", source_url=url)
    page = None
    try:
        page = _get_pw_context().new_page()
        resp = page.goto(url, timeout=PLAYWRIGHT_TIMEOUT_S * 1000, wait_until="domcontentloaded")
        if not resp:
            return _FetchOutcome(error="no_navigation_response", source_url=url)
        if not resp.ok:
            return _FetchOutcome(
                final_url=page.url,
                status_code=resp.status,
                error=f"http_{resp.status}",
                source_url=url,
            )

        # Handle common consent-management platforms without attempting to
        # defeat authentication, subscriptions, CAPTCHAs, or access controls.
        consent_selectors = (
            "#onetrust-accept-btn-handler",
            "#didomi-notice-agree-button",
            "button[mode='primary'][data-testid='uc-accept-all-button']",
            "button.sp_choice_type_11",
        )
        consent_clicked = False
        for frame in page.frames:
            for selector in consent_selectors:
                try:
                    locator = frame.locator(selector).first
                    if locator.count() and locator.is_visible(timeout=300):
                        locator.click(timeout=1_500)
                        consent_clicked = True
                        break
                except Exception:
                    continue
            if consent_clicked:
                break
            try:
                locator = frame.get_by_role("button", name=_CONSENT_BUTTON_RE).first
                if locator.count() and locator.is_visible(timeout=300):
                    locator.click(timeout=1_500)
                    consent_clicked = True
                    break
            except Exception:
                continue
        # Wait briefly for an observable article structure without waiting for
        # every ad/analytics request on the page to become idle.
        try:
            page.wait_for_function(
                """() => Boolean(
                    document.querySelector('article, main, [itemprop="articleBody"]') ||
                    document.querySelector('script[type*="ld+json"]')
                )""",
                timeout=2_000,
            )
        except Exception:
            pass
        content = page.content()
        return _FetchOutcome(
            content=content or None,
            final_url=page.url,
            status_code=resp.status,
            error=None if content else "empty_response",
            source_url=url,
        )
    except Exception as exc:
        log.debug(f"playwright failed on {url}: {exc!r}")
        return _FetchOutcome(error=_error_name(exc), source_url=url)
    finally:
        if page is not None:
            try:
                page.close()
            except Exception as exc:
                log.debug(f"playwright page cleanup failed for {url}: {exc!r}")


def _publication_timestamp(value) -> str | None:
    """Convert a MediaCloud date to a Wayback/Common Crawl timestamp."""
    if value is None:
        return None
    match = re.search(
        r"(?P<year>\d{4})[-/]?(?P<month>\d{2})[-/]?(?P<day>\d{2})"
        r"(?:[T\s]?(?P<hour>\d{2}):?(?P<minute>\d{2})?:?(?P<second>\d{2})?)?",
        str(value),
    )
    if not match:
        return None
    parts = match.groupdict(default="00")
    try:
        parsed = datetime(
            int(parts["year"]), int(parts["month"]), int(parts["day"]),
            int(parts["hour"]), int(parts["minute"]), int(parts["second"]),
            tzinfo=timezone.utc,
        )
    except ValueError:
        return None
    return parsed.strftime("%Y%m%d%H%M%S")


def _timestamp_distance(timestamp: str, target: str | None) -> float:
    if target is None:
        return -float(timestamp or "0")
    try:
        captured = datetime.strptime(timestamp[:14], "%Y%m%d%H%M%S")
        wanted = datetime.strptime(target[:14], "%Y%m%d%H%M%S")
        return abs((captured - wanted).total_seconds())
    except (TypeError, ValueError):
        return float("inf")


def _archive_url_variants(urls) -> list[str]:
    """Return the three approved, ordered Wayback URL forms for one target."""
    raw = urls[0] if isinstance(urls, (list, tuple)) and urls else urls
    if not raw:
        return []
    try:
        base = urlparse(str(raw))._replace(fragment="")
    except ValueError:
        return []
    if not base.netloc:
        return []
    candidates = [base]
    if base.scheme.lower() == "https":
        candidates.append(base._replace(scheme="http"))
    if base.query:
        candidates.append(base._replace(query=""))
    variants: list[str] = []
    for candidate in candidates:
        value = urlunparse(candidate)
        if value and value not in variants:
            variants.append(value)
    return variants


def _query_wayback_cdx(url: str, target: str | None) -> list[_ArchiveCapture]:
    """Query exact Wayback captures, preferably ordered around publication."""
    cdx_url = "https://web.archive.org/cdx/search/cdx"
    params = [
        ("url", url),
        ("output", "json"),
        ("fl", "timestamp,original,statuscode,mimetype,digest"),
        ("filter", "statuscode:200"),
        ("filter", "mimetype:text/html"),
        ("collapse", "digest"),
        ("limit", str(max(WAYBACK_MAX_SNAPSHOTS * 2, 4))),
    ]
    if target:
        params.extend([("sort", "closest"), ("closest", target)])
    _enforce_rate_limit("web.archive.org", WAYBACK_MIN_GAP_S)
    try:
        resp = _get_session("wayback").get(
            cdx_url,
            params=params,
            timeout=(REQUEST_CONNECT_TIMEOUT_S, WAYBACK_CDX_READ_TIMEOUT_S),
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            return []
        header = payload[0]
        timestamp_i = header.index("timestamp")
        original_i = header.index("original")
        digest_i = header.index("digest") if "digest" in header else None
        snapshots: list[_ArchiveCapture] = []
        for row in payload[1:]:
            timestamp = row[timestamp_i]
            original_raw = row[original_i]
            original = quote(original_raw, safe=":/?&=%;,+#@")
            snapshots.append(_ArchiveCapture(
                timestamp=timestamp,
                original_url=original_raw,
                fetch_url=f"https://web.archive.org/web/{timestamp}id_/{original}",
                digest=row[digest_i] if digest_i is not None else None,
            ))
        return snapshots
    except (requests.exceptions.RequestException, ValueError, KeyError, IndexError, TypeError) as exc:
        log.debug(f"Wayback CDX lookup failed for {url}: {exc!r}")
        raise


def _wayback_snapshot_refs(urls, publish_date=None) -> list[_ArchiveCapture]:
    """Return unique snapshots closest to the MediaCloud publication date."""
    target = _publication_timestamp(publish_date)
    captures: list[_ArchiveCapture] = []
    for variant in _archive_url_variants(urls):
        captures.extend(_query_wayback_cdx(variant, target))
    captures.sort(key=lambda item: _timestamp_distance(item.timestamp, target))
    selected: list[_ArchiveCapture] = []
    seen_digests: set[str] = set()
    seen_fetch_urls: set[str] = set()
    for capture in captures:
        if capture.fetch_url in seen_fetch_urls:
            continue
        if capture.digest and capture.digest in seen_digests:
            continue
        seen_fetch_urls.add(capture.fetch_url)
        if capture.digest:
            seen_digests.add(capture.digest)
        selected.append(capture)
        if len(selected) >= WAYBACK_MAX_SNAPSHOTS:
            break
    return selected


def _l4_fetch_candidates(urls, publish_date=None):
    """Yield raw bytes from publication-aware Wayback snapshots."""
    variants = _archive_url_variants(urls)
    try:
        snapshots = _wayback_snapshot_refs(variants, publish_date)
    except (requests.exceptions.RequestException, ValueError, KeyError, IndexError, TypeError) as exc:
        # A CDX outage is a provider failure. Surface it to the worker so the
        # circuit breaker can protect the remaining queue immediately.
        yield _FetchOutcome(error=_error_name(exc))
        return
    if not snapshots and variants:
        target = _publication_timestamp(publish_date)
        original = variants[0]
        quoted = quote(original, safe=":/?&=%;,+#@")
        fetch_url = (
            f"https://web.archive.org/web/{target}id_/{quoted}"
            if target else f"https://web.archive.org/web/{quoted}"
        )
        snapshots = [_ArchiveCapture(target or "", original, fetch_url)]
    if not snapshots:
        yield _FetchOutcome(error="no_archive_url")
        return
    for snapshot in snapshots:
        _enforce_rate_limit("web.archive.org", WAYBACK_MIN_GAP_S)
        try:
            resp = _get_session("wayback").get(
                snapshot.fetch_url,
                timeout=(REQUEST_CONNECT_TIMEOUT_S, WAYBACK_SNAPSHOT_READ_TIMEOUT_S),
                allow_redirects=True,
            )
            retry_count, retry_successes = _response_retries(resp)
            if 200 <= resp.status_code < 300 and resp.content:
                yield _FetchOutcome(
                    content=resp.content,
                    final_url=snapshot.original_url,
                    status_code=resp.status_code,
                    capture_time=snapshot.timestamp or None,
                    source_url=snapshot.fetch_url,
                    retry_count=retry_count,
                    retry_successes=retry_successes,
                )
            else:
                yield _FetchOutcome(
                    final_url=snapshot.original_url,
                    status_code=resp.status_code,
                    error=f"http_{resp.status_code}" if resp.status_code else "empty_response",
                    capture_time=snapshot.timestamp or None,
                    source_url=snapshot.fetch_url,
                    retry_count=retry_count,
                )
        except requests.exceptions.RequestException as exc:
            log.debug(f"Wayback snapshot failed for {snapshot.original_url}: {exc!r}")
            yield _FetchOutcome(
                final_url=snapshot.original_url,
                error=_error_name(exc),
                capture_time=snapshot.timestamp or None,
                source_url=snapshot.fetch_url,
            )




# ── Fetch-method statistics (thread-safe) ─────────────────────────────────

_FETCH_METHODS = ("requests", "playwright", "wayback")
_EXTRACT_METHODS = ("jsonld", "itemprop", "fundus", "trafilatura_scoped", "trafilatura")
_metrics_lock = threading.Lock()
_run_metrics: dict[str, dict[str, dict[str, float | int]]] = {}


def _reset_metrics() -> None:
    global _run_metrics
    with _metrics_lock:
        _run_metrics = {
            "fetch": {method: {"attempts": 0, "successful_fetches": 0, "terminal_acceptances": 0,
                                "retry_count": 0, "retry_successes": 0, "seconds": 0.0}
                      for method in _FETCH_METHODS},
            "extract": {method: {"attempts": 0, "successful_fetches": 0, "terminal_acceptances": 0,
                                  "seconds": 0.0}
                        for method in _EXTRACT_METHODS},
        }


def _metric(group: str, method: str, *, seconds: float = 0.0, success: bool = False,
            terminal: bool = False, retry_count: int = 0, retry_successes: int = 0,
            attempt: bool = True) -> None:
    with _metrics_lock:
        entry = _run_metrics[group][method]
        if attempt:
            entry["attempts"] += 1
        entry["seconds"] += seconds
        if success:
            entry["successful_fetches"] += 1
        if terminal:
            entry["terminal_acceptances"] += 1
        if group == "fetch":
            entry["retry_count"] += retry_count
            entry["retry_successes"] += retry_successes


def _metrics_snapshot() -> dict:
    with _metrics_lock:
        return json.loads(json.dumps(_run_metrics))


_reset_metrics()


def _extract_l1(tree, url: str) -> dict | None:
    """Layer 1 — trafilatura.extract() with favor_recall. Returns full metadata dict or None."""
    raw = trafilatura.extract(
        tree,
        url=url,
        include_comments=False,
        include_tables=True,
        deduplicate=False,
        output_format="json",
        with_metadata=True,
        favor_recall=True,
        config=_TRAF_CONFIG,
    )
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _clean_structured_text(value: str) -> str:
    """Normalize JSON-LD articleBody text without imposing a length limit."""
    text = html_lib.unescape(value).strip()
    if "<" in text and ">" in text:
        try:
            text = lxml_html.fromstring(f"<div>{text}</div>").text_content()
        except Exception:
            pass
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def _walk_jsonld(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            if isinstance(child, (dict, list)):
                yield from _walk_jsonld(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk_jsonld(child)


def _jsonld_objects(tree) -> list[dict]:
    objects: list[dict] = []
    scripts = tree.xpath(
        "//script[contains(translate(@type, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
        "'abcdefghijklmnopqrstuvwxyz'), 'ld+json')]/text()"
    )
    for raw in scripts:
        raw = raw.strip().removeprefix("<!--").removesuffix("-->").strip()
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue
        objects.extend(item for item in _walk_jsonld(parsed) if isinstance(item, dict))
    return objects


def _string_value(value) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, dict):
        for key in ("@id", "url", "name", "headline"):
            if isinstance(value.get(key), str) and value[key].strip():
                return value[key].strip()
    return None


def _jsonld_article_candidates(tree) -> list[dict]:
    candidates: list[dict] = []
    for value in _jsonld_objects(tree):
        body = value.get("articleBody")
        if not isinstance(body, str):
            continue
        cleaned = _clean_structured_text(body)
        if not cleaned:
            continue
        urls = []
        for key in ("url", "mainEntityOfPage", "@id"):
            found = _string_value(value.get(key))
            if found:
                urls.append(found)
        candidates.append({
            "text": cleaned,
            "headline": _string_value(value.get("headline")) or _string_value(value.get("name")),
            "urls": urls,
            "date": _string_value(value.get("datePublished")),
        })
    return candidates


def _extract_jsonld_article_body(tree) -> str | None:
    """Return the first nonempty schema.org articleBody embedded in JSON-LD."""
    candidates = _jsonld_article_candidates(tree)
    return candidates[0]["text"] if candidates else None


def _normalize_title(value) -> str:
    if value is None:
        return ""
    value = unicodedata.normalize("NFC", html_lib.unescape(str(value)))
    return " ".join(value.split()).casefold()


def _normalize_url_identity(value: str, base_url: str | None = None):
    if not value:
        return None
    try:
        absolute = urljoin(base_url or "", str(value).strip())
        parsed = urlparse(absolute)
        host = (parsed.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        port = parsed.port
        if port and not (
            (parsed.scheme.lower() == "http" and port == 80)
            or (parsed.scheme.lower() == "https" and port == 443)
        ):
            host = f"{host}:{port}"
        path = parsed.path or "/"
        if path != "/":
            path = path.rstrip("/")
        return host, path, parsed.query
    except (TypeError, ValueError):
        return None


def _urls_equivalent(left: str, right: str, base_url: str | None = None) -> bool:
    normalized_left = _normalize_url_identity(left, base_url)
    normalized_right = _normalize_url_identity(right, base_url)
    return bool(normalized_left and normalized_left == normalized_right)


def _first_xpath_value(tree, xpath: str) -> str | None:
    try:
        values = tree.xpath(xpath)
    except Exception:
        return None
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _page_metadata(tree, base_url: str) -> dict:
    canonical = _first_xpath_value(
        tree,
        "//link[contains(concat(' ', translate(normalize-space(@rel), "
        "'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), ' '), "
        "' canonical ')]/@href",
    )
    og_url = _first_xpath_value(
        tree,
        "//meta[translate(@property, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
        "'abcdefghijklmnopqrstuvwxyz')='og:url']/@content",
    )
    og_title = _first_xpath_value(
        tree,
        "//meta[translate(@property, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', "
        "'abcdefghijklmnopqrstuvwxyz')='og:title']/@content",
    )
    return {
        "canonical": urljoin(base_url, canonical) if canonical else None,
        "og_url": urljoin(base_url, og_url) if og_url else None,
        "og_title": og_title,
    }


def _identity_sources(
    row: dict,
    base_url: str,
    *,
    urls=(),
    titles=(),
) -> list[str]:
    """Return exact page signals tying a body to the requested MediaCloud row."""
    target_url = str(row.get("url") or "")
    target_title = _normalize_title(row.get("title"))
    sources: list[str] = []
    for label, value in urls:
        if value and _urls_equivalent(str(value), target_url, base_url):
            sources.append(label)
    if target_title:
        for label, value in titles:
            if value and _normalize_title(value) == target_title:
                sources.append(label)
    return list(dict.fromkeys(sources))


def _element_text(node) -> str | None:
    try:
        text = node.text_content()
    except Exception:
        return None
    if not _has_text(text):
        return None
    return "\n".join(line.strip() for line in text.splitlines() if line.strip())


def _extract_itemprop_bodies(tree) -> list[str]:
    nodes = tree.xpath(
        "//*[@itemprop and contains(concat(' ', normalize-space(@itemprop), ' '), "
        "' articleBody ')]"
    )
    texts = []
    for node in nodes:
        text = _element_text(node)
        if _has_text(text) and text not in texts:
            texts.append(text)
    return texts


def _semantic_article_nodes(tree) -> list:
    articles = tree.xpath("//article")
    if len(articles) == 1:
        return articles
    mains = tree.xpath("//main")
    if not articles and len(mains) == 1:
        return mains
    return []


def _node_has_target_headline(node, row: dict) -> bool:
    target = _normalize_title(row.get("title"))
    if not target:
        return False
    for heading in node.xpath(".//h1 | .//h2"):
        try:
            if _normalize_title(heading.text_content()) == target:
                return True
        except Exception:
            continue
    return False


_fundus_specs_cache: list[tuple[str, str, type]] | None = None
_fundus_specs_lock = threading.Lock()


def _fundus_specs() -> list[tuple[str, str, type]]:
    global _fundus_specs_cache
    if not FUNDUS_AVAILABLE:
        return []
    with _fundus_specs_lock:
        if _fundus_specs_cache is None:
            specs = []
            for publisher in PublisherCollection:
                host = (urlparse(publisher.domain).hostname or "").lower().removeprefix("www.")
                if host:
                    specs.append((host, publisher.name, type(publisher.parser)))
            specs.sort(key=lambda item: len(item[0]), reverse=True)
            _fundus_specs_cache = specs
        return _fundus_specs_cache


def _fundus_spec_for_url(url: str):
    host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    for publisher_host, name, parser_type in _fundus_specs():
        if host == publisher_host or host.endswith(f".{publisher_host}"):
            return name, parser_type
    return None


def _fundus_parse(tree, url: str, capture_time: str | None = None) -> dict | None:
    """Parse already-fetched HTML with one thread-owned Fundus parser."""
    spec = _fundus_spec_for_url(url)
    if spec is None:
        return None
    publisher_name, parser_type = spec
    parsers = getattr(_thread_local, "fundus_parsers", None)
    if parsers is None:
        parsers = {}
        _thread_local.fundus_parsers = parsers
    proxy = parsers.get(parser_type)
    if proxy is None:
        proxy = parser_type()
        parsers[parser_type] = proxy
    crawl_date = None
    if capture_time:
        try:
            crawl_date = datetime.strptime(capture_time[:8], "%Y%m%d").date()
        except ValueError:
            pass
    try:
        try:
            parser = proxy(crawl_date)
        except ValueError:
            parser = proxy()
        html_text = lxml_html.tostring(tree, encoding="unicode", method="html")
        parsed = parser.parse(html_text, error_handling="suppress")
        body = parsed.get("body")
        text = str(body).strip() if body is not None and not isinstance(body, Exception) else None
        if not _has_text(text):
            return None
        title = parsed.get("title")
        if not isinstance(title, str):
            title = None
        return {
            "text": text,
            "title": title,
            "publishing_date": str(parsed.get("publishing_date") or "") or None,
            "free_access": parsed.get("free_access"),
            "publisher": publisher_name,
        }
    except Exception as exc:
        log.debug("Fundus parse failed for %s: %r", url, exc)
        return None


def _has_text(value) -> bool:
    """Return whether an extractor produced non-whitespace text."""
    return isinstance(value, str) and bool(value.strip())


def _needs_archive_attempt(row: Mapping[str, Any]) -> bool:
    """Return whether a row remains in the Archive-only work queue.

    v5 rows did not carry the flag, so legacy no-text fetch failures are the
    sole inferred pending case. Once a v6 row has a flag, it is authoritative:
    a completed Wayback search must not be requeued indefinitely.
    """
    if "needs_archive" in row:
        return bool(row.get("needs_archive"))
    return row.get("scrape_status") == "fetch_error" and not _has_text(row.get("text"))


def _load_html_document(html: bytes | str):
    """Parse HTML using strict UTF-8 or the document's declared charset."""
    if isinstance(html, bytes):
        try:
            html = html.decode("utf-8")
        except UnicodeDecodeError:
            # Passing bytes directly lets libxml2 honor BOM/XML/HTML charset
            # declarations while tolerating isolated invalid byte sequences.
            return lxml_html.fromstring(html)
    return trafilatura.load_html(html)


def _is_terminal_result(result: dict | None) -> bool:
    if not result:
        return False
    if result.get("scrape_status") == "fetch_ok":
        return True
    return (
        result.get("scrape_status") == "ok"
        and result.get("verification_level") in {"strict_verified", "page_verified"}
        and _has_text(result.get("text"))
    )


# Pre-declare every field the pipeline may write so all result dictionaries
# share a stable schema across fetch failures, old outputs, and checkpoints.
_SCRAPE_DEFAULTS: dict = {
    "scrape_status": None, "fetch_method": None, "extract_method": None,
    "final_url": None, "html_length": None, "text": None, "text_length": None,
    "attempted_methods": None, "attempt_trace": None,
    "http_status": None, "fetch_error": None, "capture_time": None,
    "target_verified": False, "verified_by": None,
    "verification_level": "no_text", "needs_archive": False,
    "canonical_url": None, "structured_url": None, "structured_title": None,
    "fundus_publisher": None, "fundus_free_access": None,
    "pipeline_version": PIPELINE_VERSION,
    "pipeline_run_id": _RUN_ID, "completed_at": None,
    "traf_title": None, "traf_author": None, "traf_date": None,
    "traf_description": None, "traf_sitename": None, "traf_hostname": None,
    "traf_categories": None, "traf_tags": None, "traf_language": None,
    "traf_image": None, "traf_pagetype": None,
}


def _apply_traf_metadata(result: dict, data) -> None:
    """Copy metadata without requiring a full page-text extraction."""
    for target, source in (
        ("traf_title", "title"), ("traf_author", "author"),
        ("traf_date", "date"), ("traf_description", "description"),
        ("traf_sitename", "sitename"), ("traf_hostname", "hostname"),
        ("traf_categories", "categories"), ("traf_tags", "tags"),
        ("traf_language", "language"), ("traf_image", "image"),
        ("traf_pagetype", "pagetype"),
    ):
        value = data.get(source) if isinstance(data, dict) else getattr(data, source, None)
        if isinstance(value, (list, tuple, set)):
            value = ", ".join(str(item) for item in value if item is not None)
        if value is not None:
            result[target] = value


def _article_jsonld_identity(tree) -> tuple[list[tuple[str, str]], list[tuple[str, str]], str | None, str | None]:
    """Return URL/title signals only from article-like JSON-LD objects."""
    for value in _jsonld_objects(tree):
        raw_type = value.get("@type", "")
        types = raw_type if isinstance(raw_type, list) else [raw_type]
        article_like = any("article" in str(item).casefold() for item in types)
        if not article_like and not isinstance(value.get("articleBody"), str):
            continue
        urls = [("structured_url", found) for key in ("url", "mainEntityOfPage", "@id")
                if (found := _string_value(value.get(key)))]
        title = _string_value(value.get("headline")) or _string_value(value.get("name"))
        titles = [("structured_title", title)] if title else []
        return urls, titles, (urls[0][1] if urls else None), title
    return [], [], None, None


def _build_result(
    row: dict,
    html: bytes | str,
    final_url: str | None,
    method: str,
    *,
    http_status: int | None = None,
    fetch_error: str | None = None,
    capture_time: str | None = None,
) -> dict:
    """Apply the fixed extraction order without length or phrase decisions."""
    result = {
        **row,
        **_SCRAPE_DEFAULTS,
        "fetch_method": method,
        "final_url": final_url,
        "html_length": len(html),
        "http_status": http_status,
        "fetch_error": fetch_error,
        "capture_time": capture_time,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    if _FETCH_ONLY:
        result["scrape_status"] = "fetch_ok"
        return result

    url = final_url or row.get("url", "")

    # Keep raw HTTP bytes until strict UTF-8/document-declared decoding here.
    try:
        tree = _load_html_document(html)
    except Exception as exc:
        log.debug(f"HTML decoding/parsing failed for {url}: {exc!r}")
        tree = None
    if tree is None:
        result["scrape_status"] = "extract_error"
        result["text_length"] = 0
        return result

    page_meta = _page_metadata(tree, url)
    result["canonical_url"] = page_meta["canonical"]
    try:
        _apply_traf_metadata(result, _traf_extract_metadata(tree, default_url=url))
    except Exception as exc:
        log.debug("Trafilatura metadata failed for %s: %r", url, exc)
    base_urls = [
        ("canonical", page_meta.get("canonical")),
        ("og_url", page_meta.get("og_url")),
    ]
    base_titles = [
        ("og_title", page_meta.get("og_title")),
        ("trafilatura_title", result.get("traf_title")),
    ]
    provisional = None

    def _rank(candidate: dict | None) -> int:
        if candidate is None:
            return 0
        return {
            "no_text": 0,
            "unverified_text": 1,
            "page_verified": 2,
            "strict_verified": 3,
        }.get(candidate.get("verification_level"), 0)

    def _consider(
        text: str | None,
        method_name: str,
        body_source: str,
        *,
        urls=(),
        titles=(),
        structured_url: str | None = None,
        structured_title: str | None = None,
        extra: dict | None = None,
        strict_body: bool = True,
    ) -> dict | None:
        nonlocal provisional
        if not _has_text(text):
            return None
        sources = _identity_sources(
            row,
            url,
            urls=[*base_urls, *urls],
            titles=[*base_titles, *titles],
        )
        level = "strict_verified" if sources and strict_body else (
            "page_verified" if sources else "unverified_text"
        )
        candidate = {
            **result,
            "scrape_status": "ok",
            "extract_method": method_name,
            "text": text,
            "text_length": len(text),
            "target_verified": level == "strict_verified",
            "verification_level": level,
            "verified_by": (
                "+".join([*sources, body_source])
                if sources else None
            ),
            "structured_url": structured_url,
            "structured_title": structured_title,
            **(extra or {}),
        }
        if _rank(candidate) > _rank(provisional):
            provisional = candidate
        if level in {"strict_verified", "page_verified"}:
            _metric("extract", method_name, terminal=True, attempt=False)
            return candidate
        return None

    # 1. schema.org JSON-LD articleBody.
    started = time.perf_counter()
    jsonld_candidates = _jsonld_article_candidates(tree)
    _metric("extract", "jsonld", seconds=time.perf_counter() - started,
            success=bool(jsonld_candidates))
    for jsonld in jsonld_candidates:
        jsonld_urls = [("jsonld_url", value) for value in jsonld["urls"]]
        accepted = _consider(
            jsonld["text"],
            "jsonld",
            "jsonld_body",
            urls=jsonld_urls,
            titles=[("jsonld_headline", jsonld["headline"])],
            structured_url=jsonld["urls"][0] if jsonld["urls"] else None,
            structured_title=jsonld["headline"],
        )
        if accepted is not None:
            return accepted

    # 2. Explicit microdata articleBody containers.
    started = time.perf_counter()
    itemprop_bodies = _extract_itemprop_bodies(tree)
    _metric("extract", "itemprop", seconds=time.perf_counter() - started,
            success=bool(itemprop_bodies))
    for itemprop_text in itemprop_bodies:
        accepted = _consider(
            itemprop_text,
            "itemprop",
            "itemprop_body",
            strict_body=len(itemprop_bodies) == 1,
        )
        if accepted is not None:
            return accepted

    # 3. Publisher-specific Fundus parser, on supported domains only.
    started = time.perf_counter()
    fundus_data = _fundus_parse(tree, url, capture_time)
    _metric("extract", "fundus", seconds=time.perf_counter() - started,
            success=bool(fundus_data and _has_text(fundus_data.get("text"))))
    if fundus_data is not None:
        accepted = _consider(
            fundus_data["text"],
            "fundus",
            "fundus_body",
            titles=[("fundus_title", fundus_data.get("title"))],
            structured_title=fundus_data.get("title"),
            extra={
                "fundus_publisher": fundus_data.get("publisher"),
                "fundus_free_access": fundus_data.get("free_access"),
            },
        )
        if accepted is not None:
            return accepted

    # 4. Trafilatura scoped to one semantic article/main container.
    started = time.perf_counter()
    scoped_success = False
    for node in _semantic_article_nodes(tree):
        try:
            scoped_data = _extract_l1(node, url)
        except Exception as exc:
            log.debug("scoped Trafilatura failed for %s: %r", url, exc)
            scoped_data = None
        scoped_text = scoped_data.get("text") if scoped_data else None
        if not _has_text(scoped_text):
            scoped_text = _element_text(node)
        scoped_success |= _has_text(scoped_text)
        scoped_title = scoped_data.get("title") if scoped_data else None
        accepted = _consider(
            scoped_text,
            "trafilatura_scoped",
            "semantic_body",
            titles=[("scoped_title", scoped_title)],
            structured_title=scoped_title,
            strict_body=_node_has_target_headline(node, row),
        )
        if accepted is not None:
            _metric("extract", "trafilatura_scoped", seconds=time.perf_counter() - started,
                    success=scoped_success)
            return accepted
    _metric("extract", "trafilatura_scoped", seconds=time.perf_counter() - started,
            success=scoped_success)

    # 5. Full whole-page Trafilatura runs only after the body-specific methods.
    started = time.perf_counter()
    try:
        data = _extract_l1(tree, url)
    except Exception as exc:
        log.debug("whole-page Trafilatura failed for %s: %r", url, exc)
        data = None
    if data is not None:
        _apply_traf_metadata(result, data)
    structured_urls, structured_titles, structured_url, structured_title = _article_jsonld_identity(tree)
    whole_text = data.get("text") if data else None
    _metric("extract", "trafilatura", seconds=time.perf_counter() - started,
            success=_has_text(whole_text))
    accepted = _consider(
        whole_text,
        "trafilatura",
        "whole_page",
        urls=structured_urls,
        titles=structured_titles,
        structured_url=structured_url,
        structured_title=structured_title,
        strict_body=False,
    )
    if accepted is not None:
        return accepted

    if provisional is not None:
        return provisional

    result["scrape_status"] = "extract_error"
    result["text_length"] = 0
    return result


def _atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    """Write a small text artifact completely before replacing its old version."""
    temp = path.with_name(f".{path.name}.tmp")
    temp.write_text(text, encoding=encoding)
    os.replace(temp, path)


class _RunLock:
    """Hold an OS-level lock so two scraper processes cannot merge the same files."""

    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def acquire(self) -> None:
        self.handle = self.path.open("a+b")
        try:
            if self.path.stat().st_size == 0:
                self.handle.write(b"\0")
                self.handle.flush()
            self.handle.seek(0)
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, BlockingIOError) as exc:
            self.handle.close()
            self.handle = None
            raise RuntimeError(
                f"Another scraper process is already using {self.path.parent}"
            ) from exc

        metadata = json.dumps({
            "pid": os.getpid(),
            "run_id": _RUN_ID,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }).encode("utf-8")
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(metadata)
        self.handle.flush()
        os.fsync(self.handle.fileno())

    def release(self) -> None:
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            if sys.platform == "win32":
                import msvcrt
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def _recover_primary_output(out_parquet: Path) -> None:
    """Recover the last valid Parquet if a crash occurred during final commit."""
    backup = out_parquet.with_name("trafilatura_scraped.previous.parquet")
    if not out_parquet.exists() and backup.exists():
        os.replace(backup, out_parquet)
        log.warning("Recovered primary output from %s", backup.name)
    if not out_parquet.exists():
        return
    try:
        pl.scan_parquet(out_parquet).select(pl.len()).collect()
    except Exception as exc:
        if not backup.exists():
            raise RuntimeError(f"Primary output is unreadable: {out_parquet}") from exc
        corrupt = out_parquet.with_name(
            f"trafilatura_scraped.corrupt.{datetime.now():%Y%m%dT%H%M%S}.parquet"
        )
        os.replace(out_parquet, corrupt)
        os.replace(backup, out_parquet)
        log.warning("Moved unreadable output to %s and restored backup", corrupt.name)


def _commit_primary_output(temp: Path, target: Path) -> None:
    """Commit a verified Parquet while retaining one recoverable prior version."""
    backup = target.with_name("trafilatura_scraped.previous.parquet")
    if target.exists():
        os.replace(target, backup)
    try:
        os.replace(temp, target)
    except Exception:
        if backup.exists() and not target.exists():
            os.replace(backup, target)
        raise


def _stream_json_array(parquet_path: Path, json_path: Path) -> None:
    """Export JSON in bounded batches so article text is never loaded all at once."""
    temp = json_path.with_name(f".{json_path.name}.tmp")
    with temp.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write("[\n")
        first = True
        for batch in pq.ParquetFile(parquet_path).iter_batches(batch_size=256):
            for row in batch.to_pylist():
                if not first:
                    handle.write(",\n")
                json.dump(row, handle, ensure_ascii=False, default=str)
                first = False
        handle.write("\n]\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, json_path)


def _merge_outputs(out_dir: Path, export_csv: bool, export_json: bool) -> Path:
    """Deduplicate and atomically merge the primary output plus checkpoints."""
    out_parquet = out_dir / "trafilatura_scraped.parquet"
    out_csv = out_dir / "trafilatura_scraped.csv"
    out_json = out_dir / "trafilatura_scraped.json"
    _recover_primary_output(out_parquet)
    checkpoint_files = sorted(out_dir.glob("_ckpt_*.parquet"))
    if out_parquet.exists() and not checkpoint_files:
        parquet_mtime = out_parquet.stat().st_mtime_ns
        csv_has_bom = False
        if out_csv.exists():
            with out_csv.open("rb") as handle:
                csv_has_bom = handle.read(3) == b"\xef\xbb\xbf"
        csv_current = (
            out_csv.exists()
            and out_csv.stat().st_mtime_ns >= parquet_mtime
            and csv_has_bom
        )
        json_current = out_json.exists() and out_json.stat().st_mtime_ns >= parquet_mtime
        if export_csv and not csv_current:
            temp_csv = out_dir / ".trafilatura_scraped.csv.tmp"
            if temp_csv.exists():
                temp_csv.unlink()
            pl.scan_parquet(out_parquet).sink_csv(temp_csv, include_bom=True)
            os.replace(temp_csv, out_csv)
            log.info("Regenerated missing or stale CSV export")
        if export_json and not json_current:
            _stream_json_array(out_parquet, out_json)
            log.info("Regenerated missing or stale JSON export")
        log.info("No new checkpoints; canonical Parquet remains unchanged")
        return out_parquet
    sources = ([out_parquet] if out_parquet.exists() else []) + checkpoint_files
    if not sources:
        return out_parquet

    legacy_quality_columns = {
        "content_quality", "content_quality_score", "quality_flags",
    }
    lazy_frames = []
    for rank, path in enumerate(sources):
        lazy_frames.append(
            pl.scan_parquet(path)
            .with_columns([
                pl.col("id").cast(pl.String),
                pl.lit(rank, dtype=pl.Int32).alias("__source_rank"),
            ])
        )
    merged = pl.concat(lazy_frames, how="diagonal_relaxed").with_columns(
        pl.col("text")
        .cast(pl.String, strict=False)
        .fill_null("")
        .str.strip_chars()
        .ne("")
        .cast(pl.Int8)
        .alias("__has_text")
    )
    merged = (
        merged
        # A newer failed retry must not erase previously recovered article text.
        # When both rows contain text or both contain none, the newest wins.
        .sort(["__has_text", "__source_rank"])
        .unique(subset=["id"], keep="last", maintain_order=False)
        .with_columns(
            pl.when(
                (pl.col("__has_text") == 1)
                & pl.col("scrape_status").is_in(["ok_low_confidence", "false_positive"])
            )
            .then(pl.lit("ok"))
            .otherwise(pl.col("scrape_status"))
            .alias("scrape_status")
        )
        .drop(["__source_rank", "__has_text"])
    )
    if "target_verified" in merged.collect_schema().names():
        merged = merged.with_columns(
            pl.col("target_verified").fill_null(False).cast(pl.Boolean)
        )
    else:
        merged = merged.with_columns(pl.lit(False).alias("target_verified"))
    schema = set(merged.collect_schema().names())
    if "verification_level" not in schema:
        merged = merged.with_columns(
            pl.when(pl.col("target_verified"))
            .then(pl.lit("strict_verified"))
            .when(pl.col("text").cast(pl.String, strict=False).fill_null("").str.strip_chars().ne(""))
            .then(pl.lit("unverified_text"))
            .otherwise(pl.lit("no_text"))
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
    if "needs_archive" not in schema:
        merged = merged.with_columns(
            ((pl.col("scrape_status") == "fetch_error") &
             pl.col("text").cast(pl.String, strict=False).fill_null("").str.strip_chars().eq(""))
            .alias("needs_archive")
        )
    else:
        merged = merged.with_columns(pl.col("needs_archive").fill_null(False).cast(pl.Boolean))
    drop_columns = sorted(legacy_quality_columns & set(merged.collect_schema().names()))
    if drop_columns:
        merged = merged.drop(drop_columns)
    temp_parquet = out_dir / ".trafilatura_scraped.parquet.tmp"
    if temp_parquet.exists():
        temp_parquet.unlink()
    merged.sink_parquet(temp_parquet, compression="zstd", statistics=True)
    verification = (
        pl.scan_parquet(temp_parquet)
        .select([
            pl.len().alias("rows"),
            pl.col("id").cast(pl.String).n_unique().alias("unique_ids"),
        ])
        .collect()
    )
    rows = verification["rows"][0]
    unique_ids = verification["unique_ids"][0]
    if rows != unique_ids:
        raise RuntimeError(
            f"Refusing final commit: {rows:,} rows but {unique_ids:,} unique IDs"
        )
    _commit_primary_output(temp_parquet, out_parquet)

    if export_csv:
        temp_csv = out_dir / ".trafilatura_scraped.csv.tmp"
        if temp_csv.exists():
            temp_csv.unlink()
        pl.scan_parquet(out_parquet).sink_csv(temp_csv, include_bom=True)
        os.replace(temp_csv, out_csv)
    if export_json:
        _stream_json_array(out_parquet, out_json)

    # Checkpoints are deleted only after every requested final export succeeds.
    for checkpoint in checkpoint_files:
        checkpoint.unlink()
    log.info("Final atomic merge — %s unique rows saved", f"{rows:,}")
    return out_parquet




# ── Stats report ───────────────────────────────────────────────────────────

_wayback_breaker_lock = threading.Lock()
_wayback_consecutive_failures = 0
_wayback_open_until = 0.0


def _wayback_allowed() -> bool:
    with _wayback_breaker_lock:
        return time.monotonic() >= _wayback_open_until


def _record_wayback_health(outcome: _FetchOutcome) -> bool:
    """Update the small provider circuit breaker and return provider failure."""
    global _wayback_consecutive_failures, _wayback_open_until
    provider_failure = bool(outcome.error) and not str(outcome.error).startswith("http_") and outcome.error not in {"no_capture", "no_archive_url"}
    provider_failure |= outcome.status_code in {429, 502, 503, 504}
    with _wayback_breaker_lock:
        if provider_failure:
            _wayback_consecutive_failures += 1
            if _wayback_consecutive_failures >= WAYBACK_BREAKER_FAILURES:
                _wayback_open_until = time.monotonic() + WAYBACK_BREAKER_PAUSE_S
                _wayback_consecutive_failures = 0
                log.warning("Wayback circuit breaker open for %ss", WAYBACK_BREAKER_PAUSE_S)
        elif outcome.content is not None:
            _wayback_consecutive_failures = 0
        return provider_failure


def scrape_all(
    df_mc: pd.DataFrame,
    out_dir: Path,
    workers_l1: int,
    workers_l3: int,
    workers_l4: int,
    save_every: int,
    *,
    mode: str = "complete",
    limit: int | None = None,
    export_csv: bool = True,
    export_json: bool = True,
) -> pd.DataFrame:
    """Run the requests -> Playwright -> Wayback three-source cascade."""
    if mode not in {"complete", "live-only", "archive-only"}:
        raise ValueError("mode must be complete, live-only, or archive-only")
    if any(value < 1 for value in (workers_l1, workers_l3, workers_l4)):
        raise ValueError("workers must be at least 1")
    if save_every < 1:
        raise ValueError("save_every must be at least 1")

    out_dir.mkdir(parents=True, exist_ok=True)
    out_parquet = out_dir / "trafilatura_scraped.parquet"
    _recover_primary_output(out_parquet)
    checkpoint_files = sorted(out_dir.glob("_ckpt_*.parquet"))
    sources = ([out_parquet] if out_parquet.exists() else []) + checkpoint_files
    existing: dict[str, dict] = {}
    for path in sources:
        for row in pl.read_parquet(path).to_dicts():
            existing[str(row.get("id", ""))] = row

    def pending_archive(row: dict) -> bool:
        return _needs_archive_attempt(row)

    input_rows = {str(row["id"]): row for row in df_mc.to_dict("records")}
    live_rows: list[dict] = []
    archive_rows: list[dict] = []
    if mode == "archive-only":
        archive_rows = [row for row in existing.values() if pending_archive(row)]
    else:
        for story_id, row in input_rows.items():
            saved = existing.get(story_id)
            if saved is None:
                live_rows.append(row)
            elif mode == "complete" and pending_archive(saved):
                archive_rows.append(saved)
    if limit is not None:
        remaining = limit
        live_rows, archive_rows = live_rows[:remaining], archive_rows[:max(0, remaining - len(live_rows))]
    log.info("Three-source mode=%s: %s live rows, %s archive-only rows, %s preserved rows",
             mode, len(live_rows), len(archive_rows), len(existing) - len(archive_rows))

    _reset_metrics()
    q1: queue.Queue = queue.Queue(maxsize=max(1000, workers_l1 * 20))
    q3: queue.Queue = queue.Queue(maxsize=max(500, workers_l3 * 100))
    q4: queue.Queue = queue.Queue(maxsize=max(1000, workers_l4 * 100))
    new_results: list[dict] = []
    save_lock = threading.Lock()
    last_checkpoint_at = time.monotonic()
    checkpoint_index = max((int(p.stem.rsplit("_", 1)[1]) for p in checkpoint_files
                            if p.stem.rsplit("_", 1)[1].isdigit()), default=-1) + 1

    def checkpoint() -> None:
        nonlocal checkpoint_index, last_checkpoint_at
        if not new_results:
            return
        batch = pl.from_dicts(new_results, infer_schema_length=len(new_results))
        target = out_dir / f"_ckpt_{checkpoint_index:06d}.parquet"
        temp = target.with_suffix(".parquet.tmp")
        batch.write_parquet(temp, compression="zstd", statistics=True)
        if pl.read_parquet(temp).height != len(batch):
            raise RuntimeError(f"Checkpoint verification failed: {temp.name}")
        os.replace(temp, target)
        new_results.clear()
        checkpoint_index += 1
        last_checkpoint_at = time.monotonic()

    def save(result: dict, pbar) -> None:
        nonlocal last_checkpoint_at
        result["id"] = str(result.get("id", ""))
        with save_lock:
            new_results.append(result)
            if len(new_results) >= save_every or time.monotonic() - last_checkpoint_at >= CHECKPOINT_MAX_AGE_S:
                checkpoint()
        pbar.update(1)

    def rank(result: dict | None) -> int:
        return {"no_text": 0, "unverified_text": 1, "page_verified": 2,
                "strict_verified": 3}.get((result or {}).get("verification_level"), 0)

    def new_job(row: dict, fallback: dict | None = None) -> _ScrapeJob:
        return {"row": {**row, "id": str(row.get("id", ""))}, "attempts": [], "trace": [],
                "archive_urls": [row.get("url")], "fallback": fallback, "had_fetch": False,
                "last_outcome": None, "needs_archive": False}

    def trace(job: _ScrapeJob, method: str, outcome: _FetchOutcome) -> None:
        job["trace"].append({key: value for key, value in {
            "method": method, "status": outcome.status_code, "error": outcome.error,
            "final_url": outcome.final_url, "capture_time": outcome.capture_time,
            "source_url": outcome.source_url}.items() if value is not None})
        job["last_outcome"] = outcome

    def process(job: _ScrapeJob, method: str, outcome: _FetchOutcome) -> dict | None:
        job["had_fetch"] = True
        candidate = _build_result(job["row"], outcome.content, outcome.final_url, method,
                                  http_status=outcome.status_code, fetch_error=outcome.error,
                                  capture_time=outcome.capture_time)
        terminal = _is_terminal_result(candidate)
        candidate["attempted_methods"] = ",".join(job["attempts"])
        candidate["attempt_trace"] = json.dumps(job["trace"], ensure_ascii=False, separators=(",", ":"))
        if terminal:
            return candidate
        if candidate.get("scrape_status") == "ok" and rank(candidate) > rank(job["fallback"]):
            job["fallback"] = candidate
        return None

    def finalize(job: _ScrapeJob, *, needs_archive: bool) -> dict:
        if job["fallback"] is not None:
            result = dict(job["fallback"])
        else:
            last = job["last_outcome"]
            result = {**job["row"], **_SCRAPE_DEFAULTS,
                      "scrape_status": "extract_error" if job["had_fetch"] else "fetch_error",
                      "text_length": 0, "http_status": last.status_code if last else None,
                      "fetch_error": last.error if last else None}
        result["needs_archive"] = needs_archive
        result["attempted_methods"] = ",".join(job["attempts"])
        result["attempt_trace"] = json.dumps(job["trace"], ensure_ascii=False, separators=(",", ":"))
        result["completed_at"] = datetime.now(timezone.utc).isoformat()
        return result

    def timed_fetch(job: _ScrapeJob, method: str, fetcher) -> _FetchOutcome:
        job["attempts"].append(method)
        started = time.perf_counter()
        try:
            outcome = _coerce_fetch_outcome(fetcher(job["row"].get("url", "")))
        except Exception as exc:
            outcome = _FetchOutcome(error=_error_name(exc), source_url=job["row"].get("url", ""))
        _metric("fetch", method, seconds=time.perf_counter() - started,
                success=outcome.content is not None, retry_count=outcome.retry_count,
                retry_successes=outcome.retry_successes)
        trace(job, method, outcome)
        return outcome

    def requests_worker(pbar) -> None:
        while True:
            job = q1.get()
            try:
                if job is None:
                    return
                outcome = timed_fetch(job, "requests", _l1_fetch)
                if outcome.content is not None:
                    accepted = process(job, "requests", outcome)
                    if accepted:
                        _metric("fetch", "requests", terminal=True, attempt=False)
                        save(accepted, pbar)
                    else:
                        q3.put(job)
                elif outcome.status_code in {404, 410}:
                    if mode == "live-only":
                        save(finalize(job, needs_archive=True), pbar)
                    else:
                        q4.put(job)
                elif mode == "live-only":
                    save(finalize(job, needs_archive=True), pbar)
                else:
                    q3.put(job)
            finally:
                q1.task_done()

    def playwright_worker(pbar) -> None:
        try:
            while True:
                job = q3.get()
                try:
                    if job is None:
                        return
                    outcome = timed_fetch(job, "playwright", _l3_fetch)
                    if outcome.content is not None:
                        accepted = process(job, "playwright", outcome)
                        if accepted:
                            _metric("fetch", "playwright", terminal=True, attempt=False)
                            save(accepted, pbar)
                        elif mode == "live-only":
                            save(finalize(job, needs_archive=True), pbar)
                        else:
                            q4.put(job)
                    elif mode == "live-only":
                        save(finalize(job, needs_archive=True), pbar)
                    else:
                        q4.put(job)
                finally:
                    q3.task_done()
        finally:
            _close_pw_browser()

    def wayback_worker(pbar) -> None:
        while True:
            job = q4.get()
            try:
                if job is None:
                    return
                if not _wayback_allowed():
                    trace(job, "wayback", _FetchOutcome(error="circuit_open"))
                    save(finalize(job, needs_archive=True), pbar)
                    continue
                job["attempts"].append("wayback")
                accepted = None
                deferred = False
                wayback_fetched = False
                retry_count = 0
                retry_successes = 0
                started = time.perf_counter()
                for outcome in _l4_fetch_candidates(job["archive_urls"], job["row"].get("publish_date")):
                    trace(job, "wayback", outcome)
                    deferred |= _record_wayback_health(outcome)
                    retry_count += outcome.retry_count
                    retry_successes += outcome.retry_successes
                    if outcome.content is not None:
                        wayback_fetched = True
                        accepted = process(job, "wayback", outcome)
                        if accepted:
                            break
                _metric("fetch", "wayback", seconds=time.perf_counter() - started,
                        success=wayback_fetched, terminal=accepted is not None,
                        retry_count=retry_count, retry_successes=retry_successes)
                if accepted is not None:
                    accepted["needs_archive"] = False
                    save(accepted, pbar)
                else:
                    save(finalize(job, needs_archive=deferred), pbar)
            finally:
                q4.task_done()

    total = len(live_rows) + len(archive_rows)
    log.info("Three-source cascade: requests=%s, Playwright=%s, Wayback=%s", workers_l1, workers_l3, workers_l4)
    threads = []
    with tqdm(total=total, desc="Scraping", unit="url") as pbar:
        for _ in range(workers_l1):
            threads.append(threading.Thread(target=requests_worker, args=(pbar,), daemon=True))
        if mode != "archive-only":
            for _ in range(workers_l3):
                threads.append(threading.Thread(target=playwright_worker, args=(pbar,), daemon=True))
        for _ in range(workers_l4):
            threads.append(threading.Thread(target=wayback_worker, args=(pbar,), daemon=True))
        for thread in threads:
            thread.start()
        for row in live_rows:
            q1.put(new_job(row))
        for row in archive_rows:
            fallback = row if _has_text(row.get("text")) else None
            q4.put(new_job(row, fallback=fallback))
        for _ in range(workers_l1): q1.put(None)
        q1.join()
        if mode != "archive-only":
            for _ in range(workers_l3): q3.put(None)
            q3.join()
        for _ in range(workers_l4): q4.put(None)
        q4.join()
        for thread in threads:
            thread.join(timeout=10)
    with save_lock:
        checkpoint()
    final_path = _merge_outputs(out_dir, export_csv=export_csv, export_json=export_json)
    metrics = _metrics_snapshot()
    _atomic_write_text(out_dir / "run_metrics.json", json.dumps(metrics, indent=2), encoding="utf-8")
    for group, rows in metrics.items():
        for method, values in rows.items():
            attempts = int(values["attempts"])
            average = values["seconds"] / attempts if attempts else 0.0
            log.info("%s %-18s attempts=%s fetched=%s accepted=%s seconds=%.2f average=%.2f retries=%s retry_successes=%s",
                     group, method, attempts, int(values["successful_fetches"]),
                     int(values["terminal_acceptances"]), values["seconds"], average,
                     int(values.get("retry_count", 0)), int(values.get("retry_successes", 0)))
    if not final_path.exists():
        return pd.DataFrame()
    return pl.read_parquet(final_path).to_pandas()


def _country_by_id(df_mc: pd.DataFrame, input_dir: Path) -> pd.Series:
    """Return the exact Sweden/Germany label for every input MediaCloud ID."""
    if "media_name" not in df_mc.columns:
        raise ValueError("Input is missing media_name, required for country statistics")
    source_path = input_dir / "collections_sources.csv"
    if not source_path.exists():
        raise FileNotFoundError(
            f"{source_path} not found. Country statistics require collections_sources.csv."
        )
    sources = pd.read_csv(
        source_path, encoding="utf-8-sig", usecols=["country", "source_name"]
    ).dropna(subset=["country", "source_name"])
    source_counts = sources.groupby("source_name")["country"].nunique()
    ambiguous = source_counts[source_counts > 1]
    if not ambiguous.empty:
        raise ValueError("collections_sources.csv maps some source names to multiple countries")
    source_country = sources.drop_duplicates("source_name").set_index("source_name")["country"]
    countries = df_mc["media_name"].map(source_country)
    if countries.isna().any():
        examples = ", ".join(df_mc.loc[countries.isna(), "media_name"].drop_duplicates().head(5))
        raise ValueError(f"No country mapping for input source(s): {examples}")
    unexpected = sorted(set(countries) - {"Sweden", "Germany"})
    if unexpected:
        raise ValueError(f"Unexpected country label(s): {', '.join(unexpected)}")
    return pd.Series(countries.to_numpy(), index=df_mc["id"].astype(str), dtype="string")


def _stats_metrics(df: pd.DataFrame) -> dict[str, int]:
    """Return descriptive result counts; never alter scraping results."""
    statuses = df["scrape_status"].value_counts(dropna=False) if not df.empty else {}
    recovered = int(statuses.get("ok", 0))
    levels = (
        df["verification_level"].fillna("no_text").value_counts()
        if not df.empty and "verification_level" in df.columns else pd.Series(dtype="int64")
    )
    verified = (
        int(levels.get("strict_verified", 0)) + int(levels.get("page_verified", 0))
        if not levels.empty else int(df["target_verified"].fillna(False).astype(bool).sum())
        if not df.empty and "target_verified" in df.columns else 0
    )
    short_text = 0
    if not df.empty and "text_length" in df.columns:
        lengths = pd.to_numeric(df["text_length"], errors="coerce")
        short_text = int(((df["scrape_status"] == "ok") & (lengths < 500)).sum())
    return {
        "final": len(df),
        "recovered": recovered,
        "verified": verified,
        "strict_verified": int(levels.get("strict_verified", 0)),
        "page_verified": int(levels.get("page_verified", 0)),
        "unverified_text": int(levels.get("unverified_text", 0)),
        "no_text": int(levels.get("no_text", 0)),
        "short_text": short_text,
        "fetch_error": int(statuses.get("fetch_error", 0)),
        "extract_error": int(statuses.get("extract_error", 0)),
        "exception": int(statuses.get("exception", 0)),
    }


def write_stats(
    df_mc: pd.DataFrame,
    df_sc: pd.DataFrame,
    out_dir: Path,
    country_by_id: pd.Series,
) -> None:
    """Write a compact, country-aware description of current scraper outcomes."""
    overall = _stats_metrics(df_sc)
    results = df_sc.copy()
    if not results.empty:
        results["__country"] = results["id"].astype(str).map(country_by_id)
    recovered_by_method = (
        results.loc[results["scrape_status"] == "ok", "fetch_method"].value_counts()
        if not results.empty and "fetch_method" in results.columns else pd.Series(dtype="int64")
    )

    lines = [
        "=" * 72,
        "Textile Waste Collection - Run Statistics",
        "=" * 72,
        "",
        "Input and results",
        f"  Total input URLs:               {len(df_mc):>8,}",
        f"  URLs with final result:         {overall['final']:>8,}",
        f"  Text recovered:                 {overall['recovered']:>8,}",
        f"  Target verified:                {overall['verified']:>8,}",
        f"  Strict verified:                {overall['strict_verified']:>8,}",
        f"  Page verified:                  {overall['page_verified']:>8,}",
        f"  Unverified text:                {overall['unverified_text']:>8,}",
        f"  No text:                        {overall['no_text']:>8,}",
        f"  Text under 500 characters:      {overall['short_text']:>8,}",
        f"  Fetch errors:                   {overall['fetch_error']:>8,}",
        f"  Extraction errors:              {overall['extract_error']:>8,}",
        f"  Exceptions:                     {overall['exception']:>8,}",
        "",
        "Recovered text by final fetch layer",
    ]
    for method in _FETCH_METHODS:
        lines.append(f"  {method:<14} {int(recovered_by_method.get(method, 0)):>8,}")

    lines += [
        "",
        "Country results",
        "  Country  Input URLs    Final  Recovered  Verified   <500  Fetch err  Extract err  Exception",
    ]
    input_country = df_mc["id"].astype(str).map(country_by_id)
    for country in ("Sweden", "Germany"):
        metrics = _stats_metrics(results[results["__country"] == country]) if not results.empty else _stats_metrics(pd.DataFrame())
        lines.append(
            f"  {country:<8} {int((input_country == country).sum()):>10,} "
            f"{metrics['final']:>8,} {metrics['recovered']:>10,} {metrics['verified']:>9,} "
            f"{metrics['short_text']:>6,} {metrics['fetch_error']:>10,} "
            f"{metrics['extract_error']:>12,} {metrics['exception']:>10,}"
        )

    lines += [
        "",
        f"Output directory: {out_dir.resolve()}",
        "=" * 72,
    ]

    report = "\n".join(lines)
    log.info("\n" + report)
    _atomic_write_text(out_dir / "run_statistics.txt", report + "\n", encoding="utf-8-sig")
    log.info(f"Stats written → run_statistics.txt")


# ── main ───────────────────────────────────────────────────────────────────

def main():
    global _FETCH_ONLY

    parser = argparse.ArgumentParser(
        description="Recover article text from MediaCloud URLs with a three-source fetch cascade.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--workers-l1", type=int, default=WORKERS_L1,
                        help="Layer 1 (requests) threads")
    parser.add_argument("--workers-l3", type=int, default=WORKERS_L3,
                        help="Playwright threads")
    parser.add_argument("--workers-l4", type=int, default=WORKERS_L4,
                        help="Wayback Machine threads")
    parser.add_argument("--mode", choices=("complete", "live-only", "archive-only"), default="complete",
                        help="complete: all sources; live-only: requests/Playwright; archive-only: pending Wayback rows")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip scraping; load existing output and print stats only")
    parser.add_argument("--input-dir", default=".",
                        help="Directory containing mediacloud_combined.csv and collections_sources.csv")
    parser.add_argument("--output-dir",  default=".",
                        help="Directory for all generated outputs, checkpoints, and logs")
    parser.add_argument("--fetch-only",  action="store_true",
                        help="Stop after fetching; skip extraction.")
    parser.add_argument("--date-from",   default=None, metavar="YYYY-MM-DD",
                        help="Only scrape articles published on or after this date")
    parser.add_argument("--date-to",     default=None, metavar="YYYY-MM-DD",
                        help="Only scrape articles published on or before this date")
    parser.add_argument("--limit", type=int, default=None, metavar="N",
                        help="Process at most N pending URLs (for a small validation run)")
    parser.add_argument("--no-csv", action="store_true",
                        help="Skip the final CSV export; Parquet remains canonical")
    parser.add_argument("--no-json", action="store_true",
                        help="Skip the final JSON export; Parquet remains canonical")
    args = parser.parse_args()

    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")
    for name in ("workers_l1", "workers_l3", "workers_l4"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be at least 1")

    _FETCH_ONLY = args.fetch_only

    input_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_lock = _RunLock(out_dir / "scrape_run.lock")
    run_lock.acquire()
    setup_logging(out_dir / "run.log")
    state_path = out_dir / "scrape_run_state.json"
    if state_path.exists():
        try:
            previous_state = json.loads(state_path.read_text(encoding="utf-8"))
            if previous_state.get("status") == "running":
                log.warning(
                    "Previous invocation %s did not finish cleanly; atomic checkpoints will resume it",
                    previous_state.get("run_id", "unknown"),
                )
        except (OSError, json.JSONDecodeError):
            log.warning("Previous run-state file is unreadable; checkpoint discovery will be used")

    run_state = {
        "run_id": _RUN_ID,
        "pipeline_version": PIPELINE_VERSION,
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "workers": {
            "requests": args.workers_l1,
            "playwright": args.workers_l3,
            "wayback": args.workers_l4,
        },
        "mode": args.mode,
    }
    _atomic_write_text(state_path, json.dumps(run_state, indent=2), encoding="utf-8")
    log.info(
        "Resources detected: %s logical CPUs, %.1f GiB RAM (%.1f GiB currently available)",
        _CPU_COUNT,
        _RAM_TOTAL_GIB,
        _RAM_AVAILABLE_GIB,
    )
    log.info(
        "Three-source dependencies: Playwright=%s, Fundus=%s",
        PLAYWRIGHT_AVAILABLE,
        FUNDUS_AVAILABLE,
    )

    try:
        combined_csv = input_dir / "mediacloud_combined.csv"
        if not combined_csv.exists():
            raise FileNotFoundError(
                f"{combined_csv} not found. Run textile_waste_p1_mediacloud.py first."
            )

        df_mc = pd.read_csv(combined_csv, encoding="utf-8-sig")
        required = {"id", "url", "publish_date"}
        missing = sorted(required - set(df_mc.columns))
        if missing:
            raise ValueError(f"Input is missing required columns: {', '.join(missing)}")
        if df_mc["id"].astype(str).duplicated().any():
            raise ValueError("Input contains duplicate MediaCloud IDs; refusing ambiguous resume")
        log.info(f"Loaded MC data: {len(df_mc):,} stories from {combined_csv}")

        if args.date_from or args.date_to:
            dates = pd.to_datetime(df_mc["publish_date"], errors="coerce")
            mask = pd.Series(True, index=df_mc.index)
            if args.date_from:
                mask &= dates >= pd.Timestamp(args.date_from)
            if args.date_to:
                mask &= dates <= pd.Timestamp(args.date_to)
            df_mc = df_mc[mask].reset_index(drop=True)
            log.info(
                "Date filter %s → %s: %s stories",
                args.date_from or "*",
                args.date_to or "*",
                f"{len(df_mc):,}",
            )
        country_by_id = _country_by_id(df_mc, input_dir)

        if not args.skip_scrape:
            df_sc = scrape_all(
                df_mc,
                out_dir,
                args.workers_l1,
                args.workers_l3,
                args.workers_l4,
                SCRAPE_SAVE_EVERY,
                mode=args.mode,
                limit=args.limit,
                export_csv=not args.no_csv,
                export_json=not args.no_json,
            )
            if args.limit is None:
                expected_ids = set(df_mc["id"].astype(str))
                output_ids = set(df_sc["id"].astype(str)) if not df_sc.empty else set()
                missing_ids = expected_ids - output_ids
                if missing_ids:
                    raise RuntimeError(
                        "Full invocation did not produce one result for "
                        f"{len(missing_ids):,} input IDs; refusing to report completion"
                    )
        else:
            scrape_parquet = out_dir / "trafilatura_scraped.parquet"
            if scrape_parquet.exists():
                columns = [
                    "id", "url", "scrape_status", "fetch_method", "text_length",
                    "target_verified",
                ]
                names = pl.scan_parquet(scrape_parquet).collect_schema().names()
                summary = pl.scan_parquet(scrape_parquet).select(
                    [column for column in columns if column in names]
                )
                if "text_length" not in names and "text" in names:
                    summary = summary.with_columns(
                        pl.col("text").fill_null("").str.len_chars().alias("text_length")
                    )
                df_sc = summary.collect().to_pandas()
            else:
                df_sc = pd.DataFrame()
            log.info(f"Loaded existing scrape summary: {len(df_sc):,} rows")

        failed = (
            df_sc[df_sc["scrape_status"].isin(["fetch_error", "extract_error", "exception"])]
            if not df_sc.empty else pd.DataFrame()
        )
        failed_urls = failed["url"].dropna().astype(str).tolist() if not failed.empty else []
        failed_text = (
            f"# {len(failed_urls)} URLs that could not be fetched or extracted\n"
            + "".join(f"{url}\n" for url in failed_urls)
        )
        _atomic_write_text(out_dir / "failed_urls.txt", failed_text, encoding="utf-8-sig")
        _atomic_write_text(
            out_dir / "failed_urls.json",
            json.dumps(failed_urls, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        log.info("Failure lists updated atomically: %s failed", f"{len(failed_urls):,}")

        write_stats(df_mc, df_sc, out_dir, country_by_id)
        run_state.update({
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "output_rows": len(df_sc),
            "metrics_path": "run_metrics.json",
        })
        _atomic_write_text(state_path, json.dumps(run_state, indent=2), encoding="utf-8")
    except KeyboardInterrupt:
        run_state.update({
            "status": "interrupted",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })
        _atomic_write_text(state_path, json.dumps(run_state, indent=2), encoding="utf-8")
        log.warning("Interrupted; committed checkpoints will resume on the next invocation")
        raise
    except Exception as exc:
        run_state.update({
            "status": "failed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error": repr(exc),
        })
        _atomic_write_text(state_path, json.dumps(run_state, indent=2), encoding="utf-8")
        log.exception("Scraper run failed; existing outputs and committed checkpoints were preserved")
        raise
    finally:
        run_lock.release()


if __name__ == "__main__":
    main()
