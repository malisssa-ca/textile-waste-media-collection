#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recover full article text from URLs in ``mediacloud_combined.csv``.

Requests, curl_cffi, Playwright, Wayback, and Common Crawl provide a bounded
fetch cascade. Structured article bodies, optional publisher-specific Fundus
parsers, scoped Trafilatura, whole-page Trafilatura, baseline, and html2txt then
extract every nonempty candidate without length or phrase filtering. Atomic
checkpoints preserve progress across crashes, and final UTF-8 results are
written to Parquet, CSV, and JSON without text limits.

Run ``python textile_waste_p2_scrape.py`` or use ``--help`` for options.
"""

# ── Imports ────────────────────────────────────────────────────────────────

import argparse
import ctypes
import gzip
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
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import quote, urljoin, urlparse, urlunparse

import pandas as pd
import polars as pl
import pyarrow.parquet as pq
import requests
import trafilatura
from lxml import html as lxml_html
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from trafilatura.settings import use_config
from urllib3.util.retry import Retry

try:
    import brotli
    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False

try:
    from curl_cffi import requests as cffi_requests
    CURL_CFFI_AVAILABLE = True
except ImportError:
    CURL_CFFI_AVAILABLE = False

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

PIPELINE_VERSION = "2026-07-22-v5-target-archives"
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

# Adaptive defaults for this 14-thread/16-GiB machine. Benchmarks with article
# extraction plus realistic network latency favor the package caps of 32
# Requests threads and 16 curl_cffi threads. Playwright stays memory-aware
# because every worker owns an independent browser process.
WORKERS_L1 = min(32, max(8, round(_CPU_COUNT * 2.25)))
WORKERS_L2 = min(16, max(4, _CPU_COUNT + 2))
WORKERS_L3 = 2 if _RAM_TOTAL_GIB >= 12 and _RAM_AVAILABLE_GIB >= 2 else 1
WORKERS_L4 = 4 if _CPU_COUNT >= 8 else 2
WORKERS_L5 = 4 if _CPU_COUNT >= 8 else 2

SCRAPE_SAVE_EVERY = 250   # checkpoint after this many completed rows
CHECKPOINT_MAX_AGE_S = 30 # also checkpoint after this many seconds
TIMEOUT_L1 = 12           # seconds per read after a 5-second connect timeout
TIMEOUT_L2 = 18           # curl_cffi request timeout
TIMEOUT_L3 = 35           # Chromium navigation timeout
TIMEOUT_L4 = 25           # Internet Archive request timeout
TIMEOUT_L5 = 30           # Common Crawl index/range request timeout
DOMAIN_MIN_GAP_S = 1.0    # minimum start gap for requests to the same domain
WAYBACK_MAX_SNAPSHOTS = 5 # closest unique captures to MediaCloud publication
COMMON_CRAWL_MAX_INDEXES = 4
COMMON_CRAWL_MAX_CAPTURES = 5
COMMON_CRAWL_MAX_URL_VARIANTS = 2

# Corporate Windows policies commonly block Playwright executables under
# AppData. Prefer an installed Chrome/Edge channel there, then fall back to the
# bundled Chromium. Other platforms use bundled Chromium directly.
PLAYWRIGHT_CHANNELS: tuple[str | None, ...] = (
    ("chrome", "msedge", None) if sys.platform == "win32" else (None,)
)

# Set to True by --fetch-only; read (never written) inside threads → module-level is safe.
_FETCH_ONLY: bool = False

# Layer 2 — curl_cffi retry behaviour
CURL_CFFI_IMPERSONATE = "chrome"     # current browser TLS profile
CURL_TIMEOUT_RETRIES  = 2            # extra attempts for transient status/timeout failures
CURL_RETRY_SLEEP_S    = 3.0          # base seconds for bounded linear backoff
CURL_RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}

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


def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(_BROWSER_HEADERS)
        retry = Retry(
            total=2,
            connect=2,
            read=1,
            status=2,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
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
        _thread_local.session = s
    return _thread_local.session


def _get_cffi_session():
    """Return one curl_cffi session per worker so cookies/connections persist."""
    if not hasattr(_thread_local, "cffi_session"):
        _thread_local.cffi_session = cffi_requests.Session()
    return _thread_local.cffi_session


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

def _enforce_rate_limit(domain: str) -> None:
    """Acquire domain lock, sleep if within DOMAIN_MIN_GAP_S, stamp timestamp, release."""
    with _get_domain_lock(domain):
        elapsed = time.monotonic() - _domain_last_hit.get(domain, 0.0)
        if elapsed < DOMAIN_MIN_GAP_S:
            time.sleep(DOMAIN_MIN_GAP_S - elapsed)
        _domain_last_hit[domain] = time.monotonic()


def _l1_fetch(url: str) -> _FetchOutcome:
    """Layer 1 — requests with full browser headers and undecoded response bytes."""
    domain = urlparse(url).netloc
    with _get_domain_lock(domain):
        elapsed = time.monotonic() - _domain_last_hit.get(domain, 0.0)
        if elapsed < DOMAIN_MIN_GAP_S:
            time.sleep(DOMAIN_MIN_GAP_S - elapsed)
        try:
            resp = _get_session().get(
                url,
                timeout=(5, TIMEOUT_L1),
                allow_redirects=True,
            )
            status = resp.status_code
            if 200 <= status < 300 and resp.content:
                outcome = _FetchOutcome(
                    content=resp.content,
                    final_url=str(resp.url),
                    status_code=status,
                    source_url=url,
                )
            else:
                outcome = _FetchOutcome(
                    final_url=str(resp.url),
                    status_code=status,
                    error=f"http_{status}" if status else "empty_response",
                    source_url=url,
                )
        except requests.exceptions.RequestException as exc:
            outcome = _FetchOutcome(error=_error_name(exc), source_url=url)
        _domain_last_hit[domain] = time.monotonic()
    return outcome


def _l2_fetch(url: str) -> _FetchOutcome:
    """Layer 2 — browser TLS fingerprint returning undecoded response bytes."""
    _enforce_rate_limit(urlparse(url).netloc)
    if not CURL_CFFI_AVAILABLE:
        return _FetchOutcome(error="dependency_unavailable", source_url=url)
    for attempt in range(CURL_TIMEOUT_RETRIES + 1):
        try:
            resp = _get_cffi_session().get(
                url, impersonate=CURL_CFFI_IMPERSONATE,
                timeout=TIMEOUT_L2, allow_redirects=True,
            )
            if (
                resp.status_code in CURL_RETRYABLE_STATUS
                and attempt < CURL_TIMEOUT_RETRIES
            ):
                retry_after = resp.headers.get("Retry-After", "")
                try:
                    retry_after_s = max(0.0, float(retry_after))
                except (TypeError, ValueError):
                    retry_after_s = 0.0
                delay = min(
                    60.0,
                    max(CURL_RETRY_SLEEP_S * (attempt + 1), retry_after_s),
                )
                log.debug(
                    "curl_cffi HTTP %s for %s, retry %s/%s in %.1fs",
                    resp.status_code,
                    url,
                    attempt + 1,
                    CURL_TIMEOUT_RETRIES,
                    delay,
                )
                time.sleep(delay)
                continue
            if 200 <= resp.status_code < 300 and resp.content:
                return _FetchOutcome(
                    content=resp.content,
                    final_url=str(resp.url),
                    status_code=resp.status_code,
                    source_url=url,
                )
            return _FetchOutcome(
                final_url=str(resp.url),
                status_code=resp.status_code,
                error=f"http_{resp.status_code}" if resp.status_code else "empty_response",
                source_url=url,
            )
        except Exception as exc:
            is_timeout = "timeout" in str(exc).lower() or "28" in str(exc)
            if is_timeout and attempt < CURL_TIMEOUT_RETRIES:
                log.debug(f"curl_cffi timeout {url}, retry {attempt + 1}/{CURL_TIMEOUT_RETRIES}")
                time.sleep(CURL_RETRY_SLEEP_S * (attempt + 1))
            else:
                return _FetchOutcome(error=_error_name(exc), source_url=url)
    return _FetchOutcome(error="retry_exhausted", source_url=url)


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
        resp = page.goto(url, timeout=TIMEOUT_L3 * 1000, wait_until="domcontentloaded")
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
        if consent_clicked:
            page.wait_for_timeout(500)

        # Wait briefly for an observable article structure without waiting for
        # every ad/analytics request on the page to become idle.
        try:
            page.wait_for_function(
                """() => Boolean(
                    document.querySelector('article, main, [itemprop="articleBody"]') ||
                    document.querySelector('script[type*="ld+json"]')
                )""",
                timeout=2_500,
            )
        except Exception:
            pass

        # Recalculate height while scrolling so article sections appended by
        # IntersectionObserver/lazy loading also enter the DOM.
        page.evaluate(
            """async () => {
                const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
                let previousHeight = 0;
                let stablePasses = 0;
                for (let pass = 0; pass < 12; pass++) {
                    const height = Math.max(
                        document.body.scrollHeight,
                        document.documentElement.scrollHeight
                    );
                    window.scrollTo(0, height);
                    await pause(250);
                    const newHeight = Math.max(
                        document.body.scrollHeight,
                        document.documentElement.scrollHeight
                    );
                    stablePasses = newHeight === height && height === previousHeight
                        ? stablePasses + 1
                        : 0;
                    if (stablePasses >= 1) break;
                    previousHeight = height;
                }
                window.scrollTo(0, 0);
                await pause(300);
            }"""
        )
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
    """Return bounded, non-destructive URL forms for exact archive lookups."""
    if isinstance(urls, str):
        urls = [urls]
    variants: list[str] = []
    for raw in urls:
        if not raw:
            continue
        try:
            parsed = urlparse(str(raw))
        except ValueError:
            continue
        if not parsed.netloc:
            continue
        base = parsed._replace(fragment="")
        candidates = [base]
        if base.query:
            candidates.append(base._replace(query=""))
        candidates.append(base._replace(scheme="http" if base.scheme == "https" else "https"))
        host = base.hostname or ""
        if host.lower().startswith("www."):
            toggled_host = host[4:]
        else:
            toggled_host = f"www.{host}"
        try:
            port = base.port
        except ValueError:
            continue
        if port:
            toggled_host = f"{toggled_host}:{port}"
        candidates.append(base._replace(netloc=toggled_host))
        for candidate in candidates:
            value = urlunparse(candidate)
            if value and value not in variants:
                variants.append(value)
            if len(variants) >= 9:
                return variants
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
        ("limit", str(max(WAYBACK_MAX_SNAPSHOTS * 2, 10))),
    ]
    if target:
        params.extend([("sort", "closest"), ("closest", target)])
    _enforce_rate_limit("web.archive.org")
    try:
        resp = _get_session().get(cdx_url, params=params, timeout=(5, TIMEOUT_L4))
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
        return []


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


def _wayback_snapshot_urls(url: str, publish_date=None) -> list[str]:
    """Compatibility helper returning publication-aware Wayback replay URLs."""
    return [item.fetch_url for item in _wayback_snapshot_refs(url, publish_date)]


def _l4_fetch_candidates(urls, publish_date=None):
    """Yield raw bytes from publication-aware Wayback snapshots."""
    variants = _archive_url_variants(urls)
    snapshots = _wayback_snapshot_refs(variants, publish_date)
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
        _enforce_rate_limit("web.archive.org")
        try:
            resp = _get_session().get(
                snapshot.fetch_url,
                timeout=(5, TIMEOUT_L4),
                allow_redirects=True,
            )
            if 200 <= resp.status_code < 300 and resp.content:
                yield _FetchOutcome(
                    content=resp.content,
                    final_url=snapshot.original_url,
                    status_code=resp.status_code,
                    capture_time=snapshot.timestamp or None,
                    source_url=snapshot.fetch_url,
                )
            else:
                yield _FetchOutcome(
                    final_url=snapshot.original_url,
                    status_code=resp.status_code,
                    error=f"http_{resp.status_code}" if resp.status_code else "empty_response",
                    capture_time=snapshot.timestamp or None,
                    source_url=snapshot.fetch_url,
                )
        except requests.exceptions.RequestException as exc:
            log.debug(f"Wayback snapshot failed for {snapshot.original_url}: {exc!r}")
            yield _FetchOutcome(
                final_url=snapshot.original_url,
                error=_error_name(exc),
                capture_time=snapshot.timestamp or None,
                source_url=snapshot.fetch_url,
            )


def _l4_fetch(url: str, publish_date=None) -> tuple[bytes | None, str | None]:
    """Compatibility wrapper returning the first available archived snapshot."""
    for outcome in _l4_fetch_candidates(url, publish_date):
        if outcome.content is not None:
            return outcome.content, outcome.final_url
    return None, None


_common_crawl_indexes_cache: list[dict] | None = None
_common_crawl_indexes_lock = threading.Lock()


def _common_crawl_indexes(publish_date=None) -> list[dict]:
    """Return Common Crawl indexes ordered by proximity to publication."""
    global _common_crawl_indexes_cache
    with _common_crawl_indexes_lock:
        if _common_crawl_indexes_cache is None:
            _enforce_rate_limit("index.commoncrawl.org")
            try:
                response = _get_session().get(
                    "https://index.commoncrawl.org/collinfo.json",
                    timeout=(5, TIMEOUT_L5),
                )
                response.raise_for_status()
                payload = response.json()
                _common_crawl_indexes_cache = payload if isinstance(payload, list) else []
            except (requests.exceptions.RequestException, ValueError, TypeError) as exc:
                log.debug("Common Crawl index-list lookup failed: %r", exc)
                _common_crawl_indexes_cache = []
        indexes = list(_common_crawl_indexes_cache)

    target = _publication_timestamp(publish_date)
    target_dt = None
    if target:
        try:
            target_dt = datetime.strptime(target[:8], "%Y%m%d")
        except ValueError:
            pass

    def _distance(item: dict) -> float:
        match = re.search(r"CC-MAIN-(\d{4})-(\d{2})", str(item.get("id", "")))
        if not match or target_dt is None:
            return 0.0
        try:
            crawl_dt = datetime.fromisocalendar(int(match.group(1)), int(match.group(2)), 1)
            return abs((crawl_dt - target_dt).total_seconds())
        except ValueError:
            return float("inf")

    if target_dt is not None:
        indexes.sort(key=_distance)
    return indexes[:COMMON_CRAWL_MAX_INDEXES]


def _query_common_crawl_index(index: dict, url: str, publish_date=None) -> list[dict]:
    api = index.get("cdx-api")
    if not api:
        return []
    params = [
        ("url", url),
        ("output", "json"),
        ("filter", "status:200"),
        ("collapse", "digest"),
        ("limit", str(max(COMMON_CRAWL_MAX_CAPTURES * 2, 10))),
    ]
    _enforce_rate_limit("index.commoncrawl.org")
    try:
        response = _get_session().get(api, params=params, timeout=(5, TIMEOUT_L5))
        if response.status_code == 404:
            return []
        response.raise_for_status()
        records = []
        for line in response.text.splitlines():
            if not line.strip():
                continue
            record = json.loads(line)
            if isinstance(record, dict) and all(
                key in record for key in ("filename", "offset", "length")
            ):
                records.append(record)
        target = _publication_timestamp(publish_date)
        records.sort(key=lambda item: _timestamp_distance(str(item.get("timestamp", "")), target))
        return records
    except (requests.exceptions.RequestException, json.JSONDecodeError, TypeError, ValueError) as exc:
        log.debug("Common Crawl CDX lookup failed for %s in %s: %r", url, index.get("id"), exc)
        return []


def _common_crawl_url_variants(urls) -> list[str]:
    """Keep CDX lookups bounded to the requested URL and its query-free form.

    Common Crawl's CDX API asks clients not to issue concurrent/bulk queries.
    The original exact URL is therefore always tried first; a query-free version
    is the only additional form because publishers commonly canonicalize tracking
    parameters away. Scheme/www permutations remain available to Wayback but are
    deliberately not multiplied across every Common Crawl index.
    """
    if isinstance(urls, str):
        urls = [urls]
    variants: list[str] = []
    for raw in urls:
        if not raw:
            continue
        try:
            parsed = urlparse(str(raw))
        except ValueError:
            continue
        if not parsed.netloc:
            continue
        base = parsed._replace(fragment="")
        for candidate in (base, base._replace(query="") if base.query else None):
            if candidate is None:
                continue
            value = urlunparse(candidate)
            if value and value not in variants:
                variants.append(value)
            if len(variants) >= COMMON_CRAWL_MAX_URL_VARIANTS:
                return variants
    return variants


def _decode_http_payload(payload: bytes, headers: dict[str, str]) -> bytes | None:
    encodings = [
        value.strip().lower()
        for value in headers.get("content-encoding", "").split(",")
        if value.strip() and value.strip().lower() != "identity"
    ]
    try:
        for encoding in reversed(encodings):
            if encoding == "br":
                if not BROTLI_AVAILABLE:
                    return None
                payload = brotli.decompress(payload)
            elif encoding in {"gzip", "x-gzip"}:
                payload = gzip.decompress(payload)
            elif encoding == "deflate":
                try:
                    payload = zlib.decompress(payload)
                except zlib.error:
                    payload = zlib.decompress(payload, -zlib.MAX_WBITS)
            else:
                return None
        return payload
    except Exception:
        return None


def _header_map(raw_headers: bytes) -> dict[str, str]:
    headers: dict[str, str] = {}
    for line in raw_headers.decode("iso-8859-1", errors="replace").splitlines()[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key.strip().lower()] = value.strip()
    return headers


def _decode_chunked_payload(payload: bytes) -> bytes | None:
    """Decode RFC 9112 chunked transfer framing from an archived response."""
    chunks: list[bytes] = []
    position = 0
    try:
        while True:
            line_end = payload.index(b"\r\n", position)
            size_token = payload[position:line_end].split(b";", 1)[0].strip()
            size = int(size_token, 16)
            position = line_end + 2
            if size == 0:
                return b"".join(chunks)
            chunk_end = position + size
            if chunk_end + 2 > len(payload) or payload[chunk_end:chunk_end + 2] != b"\r\n":
                return None
            chunks.append(payload[position:chunk_end])
            position = chunk_end + 2
    except (ValueError, IndexError):
        return None


def _extract_warc_http_body(record: bytes) -> bytes | None:
    """Extract the embedded HTTP entity from one gzip-compressed WARC record."""
    try:
        decompressed = gzip.decompress(record)
        raw_warc_headers, http_message = decompressed.split(b"\r\n\r\n", 1)
        warc_headers = _header_map(raw_warc_headers)
        if warc_headers.get("content-length"):
            http_message = http_message[:int(warc_headers["content-length"])]
        raw_headers, body = http_message.split(b"\r\n\r\n", 1)
    except (OSError, TypeError, ValueError):
        return None
    headers = _header_map(raw_headers)
    if "chunked" in headers.get("transfer-encoding", "").lower():
        body = _decode_chunked_payload(body)
        if body is None:
            return None
    elif headers.get("content-length"):
        try:
            body = body[:int(headers["content-length"])]
        except ValueError:
            return None
    return _decode_http_payload(body, headers)


def _fetch_common_crawl_record(record: dict) -> _FetchOutcome:
    try:
        offset = int(record["offset"])
        length = int(record["length"])
        filename = str(record["filename"])
    except (KeyError, TypeError, ValueError):
        return _FetchOutcome(error="invalid_index_record")
    data_url = f"https://data.commoncrawl.org/{filename}"
    end = offset + length - 1
    _enforce_rate_limit("data.commoncrawl.org")
    try:
        with _get_session().get(
            data_url,
            headers={"Range": f"bytes={offset}-{end}", "Accept-Encoding": "identity"},
            timeout=(5, TIMEOUT_L5),
            stream=True,
        ) as response:
            if response.status_code != 206:
                return _FetchOutcome(
                    status_code=response.status_code,
                    error=f"http_{response.status_code}",
                    capture_time=str(record.get("timestamp") or "") or None,
                    source_url=data_url,
                )
            compressed = response.content
        body = _extract_warc_http_body(compressed)
        if not body:
            return _FetchOutcome(
                status_code=206,
                error="empty_warc_payload",
                capture_time=str(record.get("timestamp") or "") or None,
                source_url=data_url,
            )
        return _FetchOutcome(
            content=body,
            final_url=str(record.get("url") or "") or None,
            status_code=int(record.get("status") or 200),
            capture_time=str(record.get("timestamp") or "") or None,
            source_url=data_url,
        )
    except requests.exceptions.RequestException as exc:
        return _FetchOutcome(
            error=_error_name(exc),
            capture_time=str(record.get("timestamp") or "") or None,
            source_url=data_url,
        )


def _l5_fetch_candidates(urls, publish_date=None):
    """Yield up to five exact Common Crawl captures around publication."""
    variants = _common_crawl_url_variants(urls)
    if not variants:
        yield _FetchOutcome(error="no_archive_url")
        return
    seen: set[str] = set()
    yielded = 0
    for index in _common_crawl_indexes(publish_date):
        records: list[dict] = []
        for variant in variants:
            records.extend(_query_common_crawl_index(index, variant, publish_date))
        target = _publication_timestamp(publish_date)
        records.sort(key=lambda item: _timestamp_distance(str(item.get("timestamp", "")), target))
        for record in records:
            identity = str(record.get("digest") or "") or (
                f"{record.get('filename')}:{record.get('offset')}:{record.get('length')}"
            )
            if identity in seen:
                continue
            seen.add(identity)
            yield _fetch_common_crawl_record(record)
            yielded += 1
            if yielded >= COMMON_CRAWL_MAX_CAPTURES:
                return
    if yielded == 0:
        yield _FetchOutcome(error="no_capture")


# ── Fetch-method statistics (thread-safe) ─────────────────────────────────

_FETCH_METHODS = ("requests", "curl_cffi", "playwright", "wayback", "common_crawl")
_fetch_attempts: dict[str, int] = {method: 0 for method in _FETCH_METHODS}
_fetch_counts: dict[str, int] = {method: 0 for method in _FETCH_METHODS}
_fetch_counts["failed"] = 0
_accepted_counts: dict[str, int] = {method: 0 for method in _FETCH_METHODS}
_fetch_counts_lock = threading.Lock()


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


def _extract_l2(tree) -> str | None:
    """Layer 2 — trafilatura.baseline(). More permissive than extract(); no metadata.
    Official trafilatura fallback for pages where extract() returns nothing."""
    try:
        _, text, _ = trafilatura.baseline(tree)
        return text if text and text.strip() else None
    except Exception:
        return None


def _extract_l3(tree) -> str | None:
    """Layer 3 — trafilatura.html2txt(). Strips all tags, returns all visible text.
    Maximum recall; use only when both extract() and baseline() return nothing."""
    try:
        text = trafilatura.html2txt(tree)
        return text if text and text.strip() else None
    except Exception:
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
        text = trafilatura.html2txt(node)
    except Exception:
        text = None
    if not _has_text(text):
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
        and result.get("target_verified") is True
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
    "canonical_url": None, "structured_url": None, "structured_title": None,
    "fundus_publisher": None, "fundus_free_access": None,
    "pipeline_version": PIPELINE_VERSION,
    "pipeline_run_id": _RUN_ID, "completed_at": None,
    "traf_title": None, "traf_author": None, "traf_date": None,
    "traf_description": None, "traf_sitename": None, "traf_hostname": None,
    "traf_categories": None, "traf_tags": None, "traf_language": None,
    "traf_image": None, "traf_pagetype": None,
}


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

    # Run whole-page Trafilatura once for metadata and its later text stage.
    try:
        data = _extract_l1(tree, url)
    except Exception as exc:
        log.debug(f"extract layer 1 failed for {url}: {exc!r}")
        data = None

    if data is not None:
        result.update({
            "traf_title":       data.get("title"),
            "traf_author":      data.get("author"),
            "traf_date":        data.get("date"),
            "traf_description": data.get("description"),
            "traf_sitename":    data.get("sitename"),
            "traf_hostname":    data.get("hostname"),
            "traf_categories":  data.get("categories"),
            "traf_tags":        data.get("tags"),
            "traf_language":    data.get("language"),
            "traf_image":       data.get("image"),
            "traf_pagetype":    data.get("pagetype"),
        })
    base_urls = [
        ("canonical", page_meta.get("canonical")),
        ("og_url", page_meta.get("og_url")),
    ]
    base_titles = [
        ("og_title", page_meta.get("og_title")),
        ("trafilatura_title", data.get("title") if data else None),
    ]
    provisional = None

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
        verifiable: bool = True,
        include_page_identity: bool = True,
    ) -> dict | None:
        nonlocal provisional
        if not _has_text(text):
            return None
        sources = _identity_sources(
            row,
            url,
            urls=[*(base_urls if include_page_identity else []), *urls],
            titles=[*(base_titles if include_page_identity else []), *titles],
        )
        candidate = {
            **result,
            "scrape_status": "ok",
            "extract_method": method_name,
            "text": text,
            "text_length": len(text),
            "target_verified": bool(sources) and verifiable,
            "verified_by": (
                "+".join([*sources, body_source])
                if sources and verifiable else None
            ),
            "structured_url": structured_url,
            "structured_title": structured_title,
            **(extra or {}),
        }
        if provisional is None:
            provisional = candidate
        return candidate if candidate["target_verified"] else None

    # 1. schema.org JSON-LD articleBody.
    for jsonld in _jsonld_article_candidates(tree):
        jsonld_urls = [("jsonld_url", value) for value in jsonld["urls"]]
        accepted = _consider(
            jsonld["text"],
            "jsonld",
            "jsonld_body",
            urls=jsonld_urls,
            titles=[("jsonld_headline", jsonld["headline"])],
            structured_url=jsonld["urls"][0] if jsonld["urls"] else None,
            structured_title=jsonld["headline"],
            include_page_identity=not bool(jsonld["urls"] or jsonld["headline"]),
        )
        if accepted is not None:
            return accepted

    # 2. Explicit microdata articleBody containers.
    itemprop_bodies = _extract_itemprop_bodies(tree)
    for itemprop_text in itemprop_bodies:
        accepted = _consider(
            itemprop_text,
            "itemprop",
            "itemprop_body",
            verifiable=len(itemprop_bodies) == 1,
        )
        if accepted is not None:
            return accepted

    # 3. Publisher-specific Fundus parser, on supported domains only.
    fundus_data = _fundus_parse(tree, url, capture_time)
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
            include_page_identity=not bool(fundus_data.get("title")),
        )
        if accepted is not None:
            return accepted

    # 4. Trafilatura scoped to one semantic article/main container.
    for node in _semantic_article_nodes(tree):
        try:
            scoped_data = _extract_l1(node, url)
        except Exception as exc:
            log.debug("scoped Trafilatura failed for %s: %r", url, exc)
            scoped_data = None
        scoped_text = scoped_data.get("text") if scoped_data else None
        if not _has_text(scoped_text):
            scoped_text = _element_text(node)
        scoped_title = scoped_data.get("title") if scoped_data else None
        accepted = _consider(
            scoped_text,
            "trafilatura_scoped",
            "semantic_body",
            titles=[("scoped_title", scoped_title)],
            structured_title=scoped_title,
            verifiable=_node_has_target_headline(node, row),
        )
        if accepted is not None:
            return accepted

    # 5. Whole-page Trafilatura remains the generic recall-oriented extractor.
    whole_text = data.get("text") if data else None
    accepted = _consider(
        whole_text, "trafilatura", "whole_page", verifiable=False
    )
    if accepted is not None:
        return accepted

    # 6. Trafilatura baseline.
    try:
        text = _extract_l2(tree)
    except Exception as exc:
        log.debug(f"extract layer 2 failed for {url}: {exc!r}")
        text = None
    accepted = _consider(text, "baseline", "baseline", verifiable=False)
    if accepted is not None:
        return accepted

    # 7. Maximum-recall html2txt fallback.
    try:
        text = _extract_l3(tree)
    except Exception as exc:
        log.debug(f"extract layer 3 failed for {url}: {exc!r}")
        text = None
    accepted = _consider(text, "html2txt", "html2txt", verifiable=False)
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


def scrape_all(
    df_mc: pd.DataFrame,
    out_dir: Path,
    workers_l1: int,
    workers_l2: int,
    workers_l3: int,
    workers_l4: int,
    save_every: int,
    *,
    workers_l5: int = WORKERS_L5,
    limit: int | None = None,
    export_csv: bool = True,
    export_json: bool = True,
) -> pd.DataFrame:
    """Run a bounded, recall-first five-layer cascade with atomic resume."""
    worker_counts = {
        "workers_l1": workers_l1,
        "workers_l2": workers_l2,
        "workers_l3": workers_l3,
        "workers_l4": workers_l4,
        "workers_l5": workers_l5,
    }
    invalid = [name for name, value in worker_counts.items() if value < 1]
    if invalid:
        raise ValueError(f"Worker counts must be at least 1: {', '.join(invalid)}")
    if save_every < 1:
        raise ValueError("save_every must be at least 1")

    out_dir.mkdir(parents=True, exist_ok=True)
    out_parquet = out_dir / "trafilatura_scraped.parquet"
    _recover_primary_output(out_parquet)
    existing_ckpt_files = sorted(out_dir.glob("_ckpt_*.parquet"))
    resume_sources = ([out_parquet] if out_parquet.exists() else []) + existing_ckpt_files

    terminal_statuses = {"ok"}
    if _FETCH_ONLY:
        terminal_statuses.add("fetch_ok")

    done_ids: set[str] = set()
    resume_frames = []
    has_pipeline_version = False
    for path in resume_sources:
        frame = pl.scan_parquet(path)
        names = frame.collect_schema().names()
        has_pipeline_version |= "pipeline_version" in names
        resume_frames.append(frame)
    if resume_frames and has_pipeline_version:
        existing = pl.concat(resume_frames, how="diagonal_relaxed")
        done_ids = set(
            existing.filter(
                (pl.col("pipeline_version") == PIPELINE_VERSION)
                & pl.col("scrape_status").is_in(sorted(terminal_statuses))
            )
            .select(pl.col("id").cast(pl.String).unique())
            .collect()["id"]
            .to_list()
        )
    log.info(
        "Resume audit — %s terminal rows from pipeline %s; failures will retry",
        f"{len(done_ids):,}",
        PIPELINE_VERSION,
    )

    to_scrape = df_mc.loc[~df_mc["id"].astype(str).isin(done_ids)]
    if limit is not None:
        to_scrape = to_scrape.head(limit)
    total = len(to_scrape)
    log.info("URLs to scrape this invocation: %s", f"{total:,}")

    q1 = queue.Queue(maxsize=max(1_000, workers_l1 * 20))
    q2 = queue.Queue(maxsize=max(1_000, workers_l2 * 30))
    q3 = queue.Queue(maxsize=max(500, workers_l3 * 100))
    q4 = queue.Queue(maxsize=max(2_000, workers_l4 * 250))
    q5 = queue.Queue(maxsize=max(2_000, workers_l5 * 250))

    with _fetch_counts_lock:
        for method in _FETCH_METHODS:
            _fetch_attempts[method] = 0
            _fetch_counts[method] = 0
            _accepted_counts[method] = 0
        _fetch_counts["failed"] = 0

    new_results: list[dict] = []
    save_lock = threading.Lock()
    rows_saved = 0
    last_checkpoint_at = time.monotonic()
    checkpoint_numbers = [
        int(path.stem.rsplit("_", 1)[1])
        for path in existing_ckpt_files
        if path.stem.rsplit("_", 1)[1].isdigit()
    ]
    checkpoint_index = max(checkpoint_numbers, default=-1) + 1

    def _checkpoint() -> None:
        nonlocal checkpoint_index, last_checkpoint_at
        if not new_results:
            return
        batch = pl.from_dicts(new_results, infer_schema_length=len(new_results))
        checkpoint = out_dir / f"_ckpt_{checkpoint_index:06d}.parquet"
        temp = checkpoint.with_suffix(".parquet.tmp")
        if temp.exists():
            temp.unlink()
        batch.write_parquet(temp, compression="zstd", statistics=True)
        if pl.read_parquet(temp).height != len(new_results):
            raise RuntimeError(f"Checkpoint verification failed: {temp.name}")
        os.replace(temp, checkpoint)
        log.info(
            "Checkpoint %s — %s rows committed atomically",
            checkpoint.name,
            f"{len(batch):,}",
        )
        checkpoint_index += 1
        new_results.clear()
        last_checkpoint_at = time.monotonic()

    def _save(result: dict, pbar) -> None:
        nonlocal rows_saved
        result["id"] = str(result.get("id", ""))
        with save_lock:
            new_results.append(result)
            rows_saved += 1
            checkpoint_due = (
                len(new_results) >= save_every
                or time.monotonic() - last_checkpoint_at >= CHECKPOINT_MAX_AGE_S
            )
            if checkpoint_due:
                try:
                    _checkpoint()
                except Exception as exc:
                    log.error(
                        "Checkpoint write failed; rows remain in memory and will retry: %r",
                        exc,
                    )
        pbar.update(1)

    def _new_job(row: dict[str, Any]) -> _ScrapeJob:
        row["id"] = str(row.get("id", ""))
        return {
            "row": row,
            "attempts": [],
            "trace": [],
            "archive_urls": [row.get("url")],
            "fallback": None,
            "had_fetch": False,
            "last_outcome": None,
        }

    def _job_url(job: _ScrapeJob) -> str:
        """Return the input URL as a string for fetchers and error traces."""
        value = job["row"].get("url")
        return value if isinstance(value, str) else ""

    def _job_publish_date(job: _ScrapeJob) -> str | None:
        value = job["row"].get("publish_date")
        return value if isinstance(value, str) else None

    def _mark_attempt(job: _ScrapeJob, method: str) -> None:
        job["attempts"].append(method)
        with _fetch_counts_lock:
            _fetch_attempts[method] += 1

    def _record_outcome(job: _ScrapeJob, method: str, outcome: _FetchOutcome) -> None:
        entry = {"method": method}
        for key, value in (
            ("status", outcome.status_code),
            ("error", outcome.error),
            ("final_url", outcome.final_url),
            ("capture_time", outcome.capture_time),
            ("source_url", outcome.source_url),
        ):
            if value is not None:
                entry[key] = value
        job["trace"].append(entry)
        job["last_outcome"] = outcome
        for value in (outcome.final_url,):
            if value and value not in job["archive_urls"]:
                job["archive_urls"].append(value)

    def _trace_json(job: _ScrapeJob) -> str:
        return json.dumps(job["trace"], ensure_ascii=False, separators=(",", ":"))

    def _process_html(
        job: _ScrapeJob,
        outcome: _FetchOutcome,
        method: str,
        content: bytes | str,
    ) -> dict | None:
        job["had_fetch"] = True
        with _fetch_counts_lock:
            _fetch_counts[method] += 1
        candidate = _build_result(
            job["row"],
            content,
            outcome.final_url,
            method,
            http_status=outcome.status_code,
            fetch_error=outcome.error,
            capture_time=outcome.capture_time,
        )
        candidate["attempted_methods"] = ",".join(job["attempts"])
        candidate["attempt_trace"] = _trace_json(job)
        for value in (candidate.get("canonical_url"),):
            if value and value not in job["archive_urls"]:
                job["archive_urls"].append(value)
        if _is_terminal_result(candidate):
            with _fetch_counts_lock:
                _accepted_counts[method] += 1
            return candidate
        if (
            job["fallback"] is None
            and candidate.get("scrape_status") == "ok"
            and _has_text(candidate.get("text"))
        ):
            job["fallback"] = candidate
        return None

    def _finalize_job(job: _ScrapeJob) -> dict[str, Any]:
        attempts = ",".join(job["attempts"])
        if job["fallback"] is not None:
            result = job["fallback"]
            result["attempted_methods"] = attempts
            result["attempt_trace"] = _trace_json(job)
            result["completed_at"] = datetime.now(timezone.utc).isoformat()
            with _fetch_counts_lock:
                _accepted_counts[result["fetch_method"]] += 1
            return result
        status = "extract_error" if job["had_fetch"] else "fetch_error"
        with _fetch_counts_lock:
            _fetch_counts["failed"] += 1
        last_outcome = job.get("last_outcome")
        return {
            **job["row"],
            **_SCRAPE_DEFAULTS,
            "scrape_status": status,
            "text_length": 0,
            "attempted_methods": attempts,
            "attempt_trace": _trace_json(job),
            "http_status": last_outcome.status_code if last_outcome else None,
            "fetch_error": last_outcome.error if last_outcome else None,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _route_after_failure(method: str, outcome: _FetchOutcome, default_queue):
        if method == "requests":
            if outcome.status_code == 410:
                return q4
            if outcome.status_code in {403, 404}:
                return q3
        if method == "curl_cffi" and outcome.status_code == 410:
            return q4
        return default_queue

    def _make_worker(input_queue, output_queue, method, fetcher, pbar, cleanup=None):
        def _worker():
            try:
                while True:
                    job = input_queue.get()
                    try:
                        if job is None:
                            return
                        _mark_attempt(job, method)
                        outcome = _coerce_fetch_outcome(fetcher(_job_url(job)))
                        _record_outcome(job, method, outcome)
                        content = outcome.content
                        if content is not None:
                            accepted = _process_html(job, outcome, method, content)
                            if accepted is not None:
                                _save(accepted, pbar)
                            else:
                                output_queue.put(job)
                        else:
                            _route_after_failure(method, outcome, output_queue).put(job)
                    except Exception as exc:
                        log.warning("%s exception for %s: %r", method, _job_url(job), exc)
                        _record_outcome(
                            job,
                            method,
                            _FetchOutcome(error=_error_name(exc), source_url=_job_url(job)),
                        )
                        output_queue.put(job)
                    finally:
                        input_queue.task_done()
            finally:
                if cleanup is not None:
                    cleanup()
        return _worker

    def _make_wayback_worker(pbar):
        def _worker():
            while True:
                job = q4.get()
                try:
                    if job is None:
                        return
                    _mark_attempt(job, "wayback")
                    accepted = None
                    for outcome in _l4_fetch_candidates(
                        job["archive_urls"], _job_publish_date(job)
                    ):
                        _record_outcome(job, "wayback", outcome)
                        if outcome.content is None:
                            continue
                        accepted = _process_html(job, outcome, "wayback", outcome.content)
                        if accepted is not None:
                            break
                    if accepted is not None:
                        _save(accepted, pbar)
                    else:
                        q5.put(job)
                except Exception as exc:
                    log.warning("wayback exception for %s: %r", _job_url(job), exc)
                    _record_outcome(job, "wayback", _FetchOutcome(error=_error_name(exc)))
                    q5.put(job)
                finally:
                    q4.task_done()
        return _worker

    def _make_common_crawl_worker(pbar):
        def _worker():
            while True:
                job = q5.get()
                try:
                    if job is None:
                        return
                    _mark_attempt(job, "common_crawl")
                    accepted = None
                    for outcome in _l5_fetch_candidates(
                        job["archive_urls"], _job_publish_date(job)
                    ):
                        _record_outcome(job, "common_crawl", outcome)
                        if outcome.content is None:
                            continue
                        accepted = _process_html(job, outcome, "common_crawl", outcome.content)
                        if accepted is not None:
                            break
                    _save(accepted or _finalize_job(job), pbar)
                except Exception as exc:
                    log.warning("common_crawl exception for %s: %r", _job_url(job), exc)
                    _record_outcome(job, "common_crawl", _FetchOutcome(error=_error_name(exc)))
                    _save(_finalize_job(job), pbar)
                finally:
                    q5.task_done()
        return _worker

    log.info(
        "Staged recall-first cascade: L1=%s requests, L2=%s curl_cffi, "
        "L3=%s Playwright, L4=%s Wayback, L5=%s Common Crawl; "
        "queue memory is bounded",
        workers_l1,
        workers_l2,
        workers_l3,
        workers_l4,
        workers_l5,
    )

    threads: list[threading.Thread] = []
    with tqdm(total=total, desc="Scraping", unit="url") as pbar:
        specs = [
            (workers_l1, _make_worker(q1, q2, "requests", _l1_fetch, pbar)),
            (workers_l2, _make_worker(q2, q3, "curl_cffi", _l2_fetch, pbar)),
            (workers_l3, _make_worker(q3, q4, "playwright", _l3_fetch, pbar, _close_pw_browser)),
            (workers_l4, _make_wayback_worker(pbar)),
            (workers_l5, _make_common_crawl_worker(pbar)),
        ]
        for count, target in specs:
            for _ in range(count):
                thread = threading.Thread(target=target, daemon=True)
                thread.start()
                threads.append(thread)

        columns = list(to_scrape.columns)
        for values in to_scrape.itertuples(index=False, name=None):
            row = {}
            for column, value in zip(columns, values):
                try:
                    row[column] = None if pd.isna(value) else value
                except (TypeError, ValueError):
                    row[column] = value
            q1.put(_new_job(row))
        for _ in range(workers_l1):
            q1.put(None)

        q1.join()
        for _ in range(workers_l2):
            q2.put(None)
        q2.join()
        for _ in range(workers_l3):
            q3.put(None)
        q3.join()
        for _ in range(workers_l4):
            q4.put(None)
        q4.join()
        for _ in range(workers_l5):
            q5.put(None)
        q5.join()
        for thread in threads:
            thread.join(timeout=10)

    with save_lock:
        _checkpoint()

    final_path = _merge_outputs(out_dir, export_csv=export_csv, export_json=export_json)

    with _fetch_counts_lock:
        attempts = dict(_fetch_attempts)
        fetched = dict(_fetch_counts)
        accepted = dict(_accepted_counts)
    log.info(
        "\n── Fetch cascade results ────────────────────────────\n"
        "  method          attempted   fetched   accepted\n"
        f"  requests       {attempts['requests']:>9,} {fetched['requests']:>9,} {accepted['requests']:>10,}\n"
        f"  curl_cffi      {attempts['curl_cffi']:>9,} {fetched['curl_cffi']:>9,} {accepted['curl_cffi']:>10,}\n"
        f"  playwright     {attempts['playwright']:>9,} {fetched['playwright']:>9,} {accepted['playwright']:>10,}\n"
        f"  wayback        {attempts['wayback']:>9,} {fetched['wayback']:>9,} {accepted['wayback']:>10,}\n"
        f"  common_crawl   {attempts['common_crawl']:>9,} {fetched['common_crawl']:>9,} {accepted['common_crawl']:>10,}\n"
        f"  unresolved     {fetched['failed']:>9,}\n"
        "────────────────────────────────────────────────────"
    )

    if not final_path.exists():
        return pd.DataFrame()
    summary_columns = [
        "id", "url", "scrape_status", "fetch_method", "extract_method",
        "text_length", "target_verified", "verified_by", "pipeline_version",
    ]
    schema_names = pl.scan_parquet(final_path).collect_schema().names()
    available = [column for column in summary_columns if column in schema_names]
    return pl.scan_parquet(final_path).select(available).collect().to_pandas()


# ── Stats report ───────────────────────────────────────────────────────────

def write_stats(df_mc: pd.DataFrame, df_sc: pd.DataFrame, out_dir: Path) -> None:
    """Write a P002-style funnel report to the console and run_statistics.txt."""
    mc_total  = len(df_mc)
    s = df_sc["scrape_status"].value_counts(dropna=False) if not df_sc.empty else {}
    n_ok = s.get("ok", 0)
    n_tot = len(df_sc) if not df_sc.empty else 0
    n_verified = (
        int(df_sc["target_verified"].fillna(False).astype(bool).sum())
        if not df_sc.empty and "target_verified" in df_sc.columns else 0
    )

    lines = [
        "=" * 60,
        "Textile Waste Collection — Run Statistics",
        "=" * 60,
        "",
        "── Part 1: MediaCloud fetch ─────────────────────────",
        f"  Stories fetched (total):        {mc_total:>7,}",
    ]
    if "country" in df_mc.columns:
        for country, n in df_mc["country"].value_counts().items():
            lines.append(f"    {country:<24}      {n:>7,}")

    lines += [
        "",
        "── Part 2: Scraping funnel ──────────────────────────",
        f"  URLs attempted:                 {n_tot:>7,}",
        f"  fetch_ok (fetch-only mode):     {s.get('fetch_ok', 0):>7,}",
        f"  fetch_error:                    {s.get('fetch_error',  0):>7,}",
        f"  extract_error:                  {s.get('extract_error', 0):>7,}",
        f"  exception:                      {s.get('exception', 0):>7,}",
        f"  {'─' * 42}",
        f"  Articles with recovered text:   {n_ok:>7,}  "
        f"({n_ok / max(n_tot, 1) * 100:.1f}%)",
        f"  Target article verified:        {n_verified:>7,}",
        "",
        f"Output directory: {out_dir.resolve()}",
        "=" * 60,
    ]

    report = "\n".join(lines)
    log.info("\n" + report)
    _atomic_write_text(out_dir / "run_statistics.txt", report + "\n", encoding="utf-8-sig")
    log.info(f"Stats written → run_statistics.txt")


# ── main ───────────────────────────────────────────────────────────────────

def main():
    global _FETCH_ONLY

    parser = argparse.ArgumentParser(
        description="Recover article text from MediaCloud URLs with a five-layer fetch cascade.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--workers-l1", type=int, default=WORKERS_L1,
                        help="Layer 1 (requests) threads")
    parser.add_argument("--workers-l2", type=int, default=WORKERS_L2,
                        help="Layer 2 (curl_cffi) threads")
    parser.add_argument("--workers-l3", type=int, default=WORKERS_L3,
                        help="Layer 3 (playwright) threads")
    parser.add_argument("--workers-l4", type=int, default=WORKERS_L4,
                        help="Layer 4 (Wayback Machine) threads")
    parser.add_argument("--workers-l5", type=int, default=WORKERS_L5,
                        help="Layer 5 (Common Crawl) threads")
    parser.add_argument("--skip-scrape", action="store_true",
                        help="Skip scraping; load existing output and print stats only")
    parser.add_argument("--output-dir",  default=".",
                        help="Directory containing mediacloud_combined.csv and all outputs")
    parser.add_argument("--fetch-only",  action="store_true",
                        help="Stop after fetching — skip trafilatura extraction. "
                             "Useful for benchmarking the five-layer fetch cascade.")
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
    for name in ("workers_l1", "workers_l2", "workers_l3", "workers_l4", "workers_l5"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be at least 1")

    _FETCH_ONLY = args.fetch_only

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
            "curl_cffi": args.workers_l2,
            "playwright": args.workers_l3,
            "wayback": args.workers_l4,
            "common_crawl": args.workers_l5,
        },
    }
    _atomic_write_text(state_path, json.dumps(run_state, indent=2), encoding="utf-8")
    log.info(
        "Resources detected: %s logical CPUs, %.1f GiB RAM (%.1f GiB currently available)",
        _CPU_COUNT,
        _RAM_TOTAL_GIB,
        _RAM_AVAILABLE_GIB,
    )
    log.info(
        "Layer dependencies: curl_cffi=%s, Playwright=%s, Fundus=%s, Brotli=%s",
        CURL_CFFI_AVAILABLE,
        PLAYWRIGHT_AVAILABLE,
        FUNDUS_AVAILABLE,
        BROTLI_AVAILABLE,
    )

    try:
        combined_csv = out_dir / "mediacloud_combined.csv"
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
        log.info(f"Loaded MC data: {len(df_mc):,} stories from {combined_csv.name}")

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

        if not args.skip_scrape:
            df_sc = scrape_all(
                df_mc,
                out_dir,
                args.workers_l1,
                args.workers_l2,
                args.workers_l3,
                args.workers_l4,
                SCRAPE_SAVE_EVERY,
                workers_l5=args.workers_l5,
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
                columns = ["id", "url", "scrape_status"]
                names = pl.scan_parquet(scrape_parquet).collect_schema().names()
                df_sc = (
                    pl.scan_parquet(scrape_parquet)
                    .select([column for column in columns if column in names])
                    .collect()
                    .to_pandas()
                )
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

        write_stats(df_mc, df_sc, out_dir)
        run_state.update({
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "output_rows": len(df_sc),
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
