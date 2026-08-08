import html
import json
import os
import shutil
import sys
import threading
import time
import unittest
import uuid
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, unquote, urlparse

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import archive_parallel as archive
import textile_waste_p2_scrape as scraper


REAL_WORKER_MAIN = archive._worker_main


def crash_exactly_one_worker(*args):
    """Spawn-safe test helper that simulates one abrupt native-process exit."""
    run_dir = Path(args[2])
    marker = run_dir / ".test-worker-crashed"
    try:
        descriptor = os.open(marker, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        REAL_WORKER_MAIN(*args)
    else:
        os.close(descriptor)
        os._exit(73)


@contextmanager
def workspace_temp():
    path = ROOT / "tests" / f"_archive_tmp_{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


def article_html(url: str) -> bytes:
    escaped = html.escape(url, quote=True)
    body = ("This archived article contains a complete reported passage for extraction. " * 35)
    return (
        f"<html><head><link rel='canonical' href='{escaped}'></head>"
        f"<body><article><h1>Archived article</h1><p>{body}</p></article></body></html>"
    ).encode("utf-8")


class FakeClient:
    provider_failure_statuses = archive.ArchiveHttpClient.provider_failure_statuses

    def __init__(
        self, replay_outcomes, cdx_outcomes, max_replays=2,
        availability_outcomes=None,
    ):
        self.replay_outcomes = list(replay_outcomes)
        self.cdx_outcomes = list(cdx_outcomes)
        self.availability_outcomes = list(availability_outcomes or [])
        self.replay_calls = []
        self.cdx_calls = []
        self.availability_calls = []
        self.config = archive.ArchiveConfig(max_replays=max_replays)
        self.metrics = archive.Metrics()

    def replay(self, url, timestamp, *, kind):
        self.replay_calls.append((url, timestamp, kind))
        return self.replay_outcomes.pop(0)

    def cdx(self, url, target):
        self.cdx_calls.append((url, target))
        return self.cdx_outcomes.pop(0) if self.cdx_outcomes else cdx_empty()

    def availability(self, url, target):
        self.availability_calls.append((url, target))
        if self.availability_outcomes:
            outcome = self.availability_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return archive.HttpOutcome(status_code=200, content=b"{}"), None


class ImmediateControl:
    def __init__(self):
        self.acquires = 0
        self.http_attempts = 0
        self.healthy = 0
        self.failures = 0

    def acquire(self, endpoint, *, probe_owner=False):
        self.acquires += 1
        return False

    def record_http_attempt(self):
        self.http_attempts += 1

    def record_healthy(self, endpoint, *, probe):
        self.healthy += 1

    def record_failure(self, endpoint, *, probe):
        self.failures += 1

    def defer(self, endpoint, seconds):
        return

    def observe_live(self, kind, outcome):
        return


def base_row(row_id="1", url="https://example.com/story?ref=mc"):
    return {
        "id": row_id,
        "url": url,
        "title": "Archived article",
        "publish_date": "2020-01-01",
        **scraper._SCRAPE_DEFAULTS,
        "scrape_status": "fetch_error",
        "verification_level": "no_text",
        "needs_archive": True,
    }


def replay_ok(url: str, timestamp="20200102000000"):
    return archive.HttpOutcome(
        status_code=200,
        content=article_html(url),
        final_url=f"https://web.archive.org/web/{timestamp}id_/{url}",
    )


def replay_missing(url: str):
    return archive.HttpOutcome(
        status_code=404,
        final_url=f"https://web.archive.org/web/20200101000000id_/{url}",
    )


def cdx_empty():
    return archive.HttpOutcome(status_code=200, content=b"[]"), []


def cdx_capture(url: str, timestamp="20190101000000"):
    capture = archive.Capture(
        timestamp=timestamp,
        original_url=url,
        digest="digest-1",
    )
    return archive.HttpOutcome(status_code=200, content=b"[]"), [capture]


def availability_empty():
    return archive.HttpOutcome(status_code=200, content=b"{}"), None


def availability_capture(url: str, timestamp="20190101000000"):
    return (
        archive.HttpOutcome(status_code=200, content=b"{}"),
        archive.Capture(timestamp=timestamp, original_url=url),
    )


class ArchiveRoutingTests(unittest.TestCase):
    def test_failed_recovery_probes_keep_every_endpoint_alive(self):
        config = archive.ArchiveConfig(
            request_rate=1_000,
            breaker_failures=5,
            breaker_pause_s=0.01,
        )
        for endpoint in ("replay", "availability", "cdx"):
            control = archive.SharedArchiveControl(
                archive.mp.get_context("spawn"), config
            )
            for _ in range(5):
                control.record_failure(endpoint, probe=False)

            time.sleep(0.02)
            self.assertTrue(control.acquire(endpoint))
            control.record_failure(endpoint, probe=True)
            self.assertTrue(control.snapshot()[endpoint]["probe_pending"])

            time.sleep(0.02)
            self.assertTrue(control.acquire(endpoint))
            control.record_healthy(endpoint, probe=True)
            self.assertFalse(control.snapshot()[endpoint]["probe_pending"])

    def test_direct_terminal_result_stops_without_cdx(self):
        row = base_row()
        client = FakeClient([replay_ok(row["url"])], [])
        result = archive.process_archive_row(row, client, "test-run")
        self.assertIn(result["verification_level"], {"strict_verified", "page_verified"})
        self.assertFalse(result["needs_archive"])
        self.assertEqual(len(client.replay_calls), 1)
        self.assertEqual(client.cdx_calls, [])

    def test_resume_after_reliable_direct_starts_at_availability(self):
        row = base_row()
        row["attempt_trace"] = json.dumps([
            {"method": "wayback", "phase": "direct", "status": 404},
        ])
        client = FakeClient(
            [replay_ok(row["url"], "20190101000000")], [],
            availability_outcomes=[availability_capture(row["url"])],
        )
        result = archive.process_archive_row(row, client, "resume-test")
        self.assertFalse(result["needs_archive"])
        self.assertEqual(client.replay_calls, [
            (row["url"], "20190101000000", "snapshot"),
        ])
        self.assertEqual(len(client.availability_calls), 1)

    def test_resume_after_direct_provider_error_retries_direct(self):
        row = base_row()
        row["attempt_trace"] = json.dumps([
            {"method": "wayback", "phase": "direct", "error": "ReadTimeout"},
        ])
        client = FakeClient([replay_ok(row["url"])], [])
        result = archive.process_archive_row(row, client, "resume-test")
        self.assertFalse(result["needs_archive"])
        self.assertEqual(len(client.replay_calls), 1)
        self.assertEqual(client.replay_calls[0][2], "direct")

    def test_resume_after_reliable_availability_starts_at_cdx(self):
        row = base_row()
        row["attempt_trace"] = json.dumps([
            {"method": "wayback", "phase": "direct", "status": 404},
            {"method": "wayback", "phase": "availability", "status": 200},
        ])
        client = FakeClient(
            [replay_ok(row["url"], "20190101000000")],
            [cdx_capture(row["url"])],
        )
        result = archive.process_archive_row(row, client, "resume-test")
        self.assertFalse(result["needs_archive"])
        self.assertEqual(len(client.availability_calls), 0)
        self.assertEqual(len(client.cdx_calls), 1)
        self.assertEqual(client.replay_calls[0][2], "snapshot")

    def test_availability_snapshot_is_used_before_cdx(self):
        row = base_row()
        client = FakeClient(
            [replay_missing(row["url"]), replay_ok(row["url"], "20190101000000")],
            [],
            availability_outcomes=[availability_capture(row["url"])],
        )
        result = archive.process_archive_row(row, client, "test-run")
        self.assertFalse(result["needs_archive"])
        self.assertEqual(len(client.availability_calls), 1)
        self.assertEqual(client.cdx_calls, [])
        self.assertEqual(len(client.replay_calls), 2)

    def test_valid_empty_availability_variants_advance_to_cdx(self):
        row = base_row()
        variants = scraper._archive_url_variants([row["url"]])
        client = FakeClient(
            [replay_missing(row["url"])],
            [],
            availability_outcomes=[availability_empty() for _ in variants],
        )
        result = archive.process_archive_row(row, client, "test-run")
        self.assertFalse(result["needs_archive"])
        self.assertEqual([call[0] for call in client.availability_calls], variants)
        self.assertEqual([call[0] for call in client.cdx_calls], variants)

    def test_cdx_variants_are_conditional_and_ordered(self):
        row = base_row()
        http_variant = "http://example.com/story?ref=mc"
        client = FakeClient(
            [replay_missing(row["url"]), replay_ok(http_variant, "20190101000000")],
            [cdx_empty(), cdx_capture(http_variant)],
            availability_outcomes=[archive.AvailabilityUnavailable("test")],
        )
        result = archive.process_archive_row(row, client, "test-run")
        self.assertFalse(result["needs_archive"])
        self.assertEqual([call[0] for call in client.cdx_calls], [row["url"], http_variant])
        self.assertNotIn("https://example.com/story", [call[0] for call in client.cdx_calls])
        self.assertEqual(len(client.replay_calls), 2)

    def test_exact_captures_prevent_alternative_cdx_queries(self):
        row = base_row()
        client = FakeClient(
            [replay_missing(row["url"]), replay_ok(row["url"], "20180101000000")],
            [cdx_capture(row["url"], "20180101000000")],
            availability_outcomes=[archive.AvailabilityUnavailable("test")],
        )
        result = archive.process_archive_row(row, client, "test-run")
        self.assertFalse(result["needs_archive"])
        self.assertEqual(len(client.cdx_calls), 1)
        self.assertEqual(len(client.replay_calls), 2)

    def test_non_200_cdx_does_not_claim_an_empty_capture_list(self):
        row = base_row()
        client = FakeClient(
            [replay_missing(row["url"])],
            [(archive.HttpOutcome(status_code=403), [])],
            availability_outcomes=[archive.AvailabilityUnavailable("test")],
        )
        result = archive.process_archive_row(row, client, "test-run")
        self.assertTrue(result["needs_archive"])
        self.assertEqual(len(client.cdx_calls), 1)
        self.assertEqual(len(client.replay_calls), 1)

    def test_longer_prior_unverified_text_is_preserved(self):
        row = base_row()
        row.update({
            "scrape_status": "ok",
            "text": "long prior requests text " * 300,
            "text_length": len("long prior requests text " * 300),
            "fetch_method": "requests",
            "verification_level": "unverified_text",
        })
        client = FakeClient([replay_ok(row["url"])], [])
        result = archive.process_archive_row(row, client, "test-run")
        self.assertEqual(result["fetch_method"], "wayback")
        self.assertEqual(result["fallback_text"], row["text"])
        self.assertEqual(result["fallback_fetch_method"], "requests")

    def test_provider_failure_stays_pending(self):
        row = base_row()
        failed = archive.HttpOutcome(error="ReadTimeout", probe=False)
        client = FakeClient([failed], [])
        result = archive.process_archive_row(row, client, "test-run")
        self.assertTrue(result["needs_archive"])

    def test_403_is_a_valid_url_outcome_not_a_provider_outage(self):
        control = ImmediateControl()
        client = archive.ArchiveHttpClient(archive.ArchiveConfig(), control, archive.Metrics())
        self.assertTrue(client._record_health(
            archive.HttpOutcome(status_code=403), endpoint="replay"
        ))
        self.assertEqual(control.healthy, 1)
        self.assertEqual(control.failures, 0)

    def test_same_host_replay_redirect_uses_one_rate_token(self):
        endpoint = MockWayback()
        endpoint.start()
        try:
            control = ImmediateControl()
            config = archive.ArchiveConfig(
                replay_root=f"http://127.0.0.1:{endpoint.port}/redirect",
                availability_url=f"http://127.0.0.1:{endpoint.port}/available",
                cdx_url=f"http://127.0.0.1:{endpoint.port}/cdx",
                archive_host="127.0.0.1",
            )
            client = archive.ArchiveHttpClient(config, control, archive.Metrics())
            outcome = client.replay(
                "https://example.com/article-1", "20200101000000", kind="direct"
            )
            self.assertEqual(outcome.status_code, 200)
            self.assertEqual(control.acquires, 1)
            self.assertEqual(control.http_attempts, 2)
        finally:
            endpoint.close()

    def test_metrics_continue_from_a_durable_checkpoint(self):
        metrics = archive.Metrics({
            "counts": {"logical_requests": 4},
            "seconds": {"http_seconds": 2.5},
            "statuses": {"200": 3},
            "errors": {"ReadTimeout": 1},
        })
        metrics.observe_http(
            "direct", archive.HttpOutcome(status_code=200, elapsed_s=0.5)
        )
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["counts"]["logical_requests"], 5)
        self.assertEqual(snapshot["seconds"]["http_seconds"], 3.0)
        self.assertEqual(snapshot["statuses"]["200"], 4)
        self.assertEqual(snapshot["errors"]["ReadTimeout"], 1)

    def test_shared_heartbeat_metrics_separate_endpoints_and_timeouts(self):
        context = archive.mp.get_context("spawn")
        control = archive.SharedArchiveControl(context, archive.ArchiveConfig())
        control.observe_live(
            "direct", archive.HttpOutcome(status_code=200, elapsed_s=0.5)
        )
        control.observe_live(
            "cdx", archive.HttpOutcome(error="ReadTimeout", elapsed_s=60.0)
        )
        live = control.snapshot()["live"]
        self.assertEqual(live["direct"]["attempts"], 1)
        self.assertEqual(live["direct"]["successes"], 1)
        self.assertEqual(live["direct"]["average_seconds"], 0.5)
        self.assertEqual(live["cdx"]["attempts"], 1)
        self.assertEqual(live["cdx"]["timeouts"], 1)
        self.assertEqual(live["cdx"]["successes"], 0)


class MockWayback:
    """Local deterministic endpoint used by multiprocessing integration tests."""

    def __init__(self):
        self.mode = "normal"
        self.requests = 0
        self.lock = threading.Lock()
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format, *args):
                return

            def do_GET(self):
                with parent.lock:
                    parent.requests += 1
                    request_number = parent.requests
                    mode = parent.mode
                if mode == "cdx_fail" and urlparse(self.path).path in {"/available", "/cdx"}:
                    self.send_response(503)
                    self.end_headers()
                    return
                if mode == "fail" and request_number > 30:
                    self.send_response(503)
                    self.end_headers()
                    return
                parsed = urlparse(self.path)
                if parsed.path == "/cdx":
                    original = parse_qs(parsed.query).get("url", [""])[0]
                    key = sum(original.encode("utf-8")) % 4
                    data = [["timestamp", "original", "statuscode", "mimetype", "digest"]]
                    if key == 2:
                        data.append(["20190101000000", original, "200", "text/html", "digest"])
                    payload = json.dumps(data).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                if parsed.path == "/available":
                    payload = b'{"archived_snapshots": {}}'
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                if parsed.path.startswith("/redirect/"):
                    self.send_response(302)
                    self.send_header("Location", self.path.replace("/redirect/", "/web/", 1))
                    self.end_headers()
                    return
                if parsed.path.startswith("/web/"):
                    match = archive.re.search(r"/web/(\d{14})id_/(.*)", self.path)
                    timestamp = match.group(1) if match else ""
                    original = unquote(match.group(2)) if match else ""
                    key = sum(original.encode("utf-8")) % 4
                    # CDX snapshots use the fixed 2019 timestamp below.  Every
                    # other timestamp is a publication-date direct replay.
                    if timestamp != "20190101000000" and key >= 2:
                        self.send_response(404)
                        self.end_headers()
                        return
                    payload = article_html(original)
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return
                self.send_response(404)
                self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def port(self):
        return self.server.server_port

    def start(self):
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def sample_frame(count: int) -> pl.DataFrame:
    rows = []
    for index in range(count):
        row = base_row(str(index), f"https://example.com/article-{index}")
        rows.append(row)
    return pl.from_dicts(rows, infer_schema_length=count)


class ArchivePersistenceTests(unittest.TestCase):
    def test_supervisor_emits_periodic_endpoint_heartbeat(self):
        endpoint = MockWayback()
        endpoint.start()
        try:
            with workspace_temp() as output:
                sample_frame(20).write_parquet(output / "trafilatura_scraped.parquet")
                run_dir = output / "archive_runs" / "heartbeat-test"
                manifest = archive.prepare_run(output, run_dir, 2)
                config = archive.ArchiveConfig(
                    request_rate=20,
                    breaker_pause_s=0.05,
                    replay_root=f"http://127.0.0.1:{endpoint.port}/web",
                    availability_url=f"http://127.0.0.1:{endpoint.port}/available",
                    cdx_url=f"http://127.0.0.1:{endpoint.port}/cdx",
                    archive_host="127.0.0.1",
                )
                with patch.object(archive, "DEFAULT_HEARTBEAT_INTERVAL_S", 0.05):
                    with self.assertLogs("textile.archive", level="INFO") as captured:
                        archive.run_supervisor(
                            run_dir, manifest, processes=1, threads=2,
                            save_every=10, checkpoint_age_s=1,
                            max_process_restarts=1, config=config,
                        )
                joined = "\n".join(captured.output)
                self.assertIn("Heartbeat rows=", joined)
                self.assertIn("direct: attempts=", joined)
                self.assertIn("availability: attempts=", joined)
                self.assertIn("cdx: attempts=", joined)
                self.assertIn("snapshot: attempts=", joined)
        finally:
            endpoint.close()

    def test_freeze_preserves_text_when_newer_checkpoint_failed(self):
        with workspace_temp() as output:
            canonical = sample_frame(3).with_columns([
                pl.when(pl.col("id") == "0").then(pl.lit("saved text")).otherwise(pl.col("text")).alias("text"),
                pl.when(pl.col("id") == "0").then(pl.lit(10)).otherwise(pl.col("text_length")).alias("text_length"),
            ])
            canonical.write_parquet(output / "trafilatura_scraped.parquet")
            failed = canonical.filter(pl.col("id") == "0").with_columns([
                pl.lit(None, dtype=pl.String).alias("text"),
                pl.lit(None, dtype=pl.Int64).alias("text_length"),
            ])
            failed.write_parquet(output / "_ckpt_000001.parquet")
            stats = archive._freeze_base(output, output / "base.parquet")
            frozen = pl.read_parquet(output / "base.parquet")
            self.assertEqual(stats["rows"], 3)
            self.assertEqual(frozen.filter(pl.col("id") == "0")["text"][0], "saved text")

    def test_freeze_prefers_quality_then_recency_across_old_checkpoints(self):
        with workspace_temp() as output:
            checkpoint_path = output / "_ckpt_000001.parquet"
            canonical_path = output / "trafilatura_scraped.parquet"
            checkpoint = sample_frame(1).with_columns([
                pl.lit("older verified checkpoint").alias("text"),
                pl.lit(25).alias("text_length"),
                pl.lit("strict_verified").alias("verification_level"),
                pl.lit(True).alias("target_verified"),
                pl.lit("wayback").alias("fetch_method"),
            ])
            canonical = checkpoint.with_columns([
                pl.lit("newer verified canonical").alias("text"),
                pl.lit(24).alias("text_length"),
                pl.lit("requests").alias("fetch_method"),
            ])
            checkpoint.write_parquet(checkpoint_path)
            canonical.write_parquet(canonical_path)
            os.utime(checkpoint_path, ns=(1_700_000_000_000_000_000,) * 2)
            os.utime(canonical_path, ns=(1_800_000_000_000_000_000,) * 2)
            archive._freeze_base(output, output / "base.parquet")
            frozen = pl.read_parquet(output / "base.parquet")
            self.assertEqual(frozen["text"][0], "newer verified canonical")

            # A newer unverified checkpoint still cannot displace a verified
            # canonical row: verification_level remains authoritative.
            checkpoint.with_columns([
                pl.lit("newest but unverified checkpoint " * 10).alias("text"),
                pl.lit("unverified_text").alias("verification_level"),
                pl.lit(False).alias("target_verified"),
            ]).write_parquet(checkpoint_path)
            os.utime(checkpoint_path, ns=(1_900_000_000_000_000_000,) * 2)
            archive._freeze_base(output, output / "base-quality.parquet")
            frozen = pl.read_parquet(output / "base-quality.parquet")
            self.assertEqual(frozen["text"][0], "newer verified canonical")

    def test_freeze_keeps_longest_unverified_text(self):
        with workspace_temp() as output:
            checkpoint_path = output / "_ckpt_000001.parquet"
            canonical_path = output / "trafilatura_scraped.parquet"
            older_long = sample_frame(1).with_columns([
                pl.lit("older long text " * 100).alias("text"),
                pl.lit("unverified_text").alias("verification_level"),
            ])
            newer_short = older_long.with_columns(
                pl.lit("newer short text").alias("text")
            )
            older_long.write_parquet(checkpoint_path)
            newer_short.write_parquet(canonical_path)
            os.utime(checkpoint_path, ns=(1_700_000_000_000_000_000,) * 2)
            os.utime(canonical_path, ns=(1_800_000_000_000_000_000,) * 2)
            archive._freeze_base(output, output / "base.parquet")
            frozen = pl.read_parquet(output / "base.parquet")
            self.assertEqual(frozen["text"][0], "older long text " * 100)

    def test_resume_keeps_unverified_text_from_a_deferred_part(self):
        with workspace_temp() as output:
            frame = sample_frame(1)
            shard_dir = output / "shard_000"
            shard_dir.mkdir()
            recovered = frame.with_columns([
                pl.lit("useful archive text " * 100).alias("text"),
                pl.lit(len("useful archive text " * 100)).alias("text_length"),
                pl.lit("unverified_text").alias("verification_level"),
                pl.lit("wayback").alias("fetch_method"),
                pl.lit("ok").alias("scrape_status"),
                pl.lit(True).alias("needs_archive"),
            ])
            recovered.write_parquet(shard_dir / "part_000000.parquet")
            resumed = archive._resume_rows(frame, shard_dir)
            self.assertEqual(resumed[0]["text"], "useful archive text " * 100)

            client = FakeClient(
                [replay_missing(resumed[0]["url"])],
                [cdx_empty(), cdx_empty()],
            )
            final = archive.process_archive_row(resumed[0], client, "resume-test")
            self.assertFalse(final["needs_archive"])
            self.assertEqual(final["text"], "useful archive text " * 100)

    def test_end_to_end_multiprocess_shards_and_atomic_reduction(self):
        endpoint = MockWayback()
        endpoint.start()
        try:
            with workspace_temp() as output:
                original = sample_frame(120)
                original.write_parquet(output / "trafilatura_scraped.parquet")
                run_dir = output / "archive_runs" / "test"
                manifest = archive.prepare_run(output, run_dir, 8)
                config = archive.ArchiveConfig(
                    request_rate=500,
                    breaker_pause_s=0.05,
                    replay_root=f"http://127.0.0.1:{endpoint.port}/web",
                    availability_url=f"http://127.0.0.1:{endpoint.port}/available",
                    cdx_url=f"http://127.0.0.1:{endpoint.port}/cdx",
                    archive_host="127.0.0.1",
                )
                manifest = archive.run_supervisor(
                    run_dir, manifest, processes=2, threads=4, save_every=20,
                    checkpoint_age_s=5, max_process_restarts=1, config=config,
                )
                final_path = archive.reduce_run(output, run_dir, manifest)
                final = pl.read_parquet(final_path)
                self.assertEqual(final.height, 120)
                self.assertEqual(final["id"].n_unique(), 120)
                self.assertGreater(final.filter(pl.col("fetch_method") == "wayback").height, 0)
                self.assertEqual(final.filter(pl.col("needs_archive")).height, 0)
                self.assertTrue((output / "trafilatura_scraped.previous.parquet").exists())
                self.assertEqual(len(list((run_dir / "shards").glob("*/result.parquet"))), 8)
        finally:
            endpoint.close()

    def test_abrupt_worker_exit_restarts_its_shard(self):
        endpoint = MockWayback()
        endpoint.start()
        try:
            with workspace_temp() as output:
                sample_frame(120).write_parquet(output / "trafilatura_scraped.parquet")
                run_dir = output / "archive_runs" / "crash-test"
                manifest = archive.prepare_run(output, run_dir, 8)
                config = archive.ArchiveConfig(
                    request_rate=500,
                    breaker_pause_s=0.05,
                    replay_root=f"http://127.0.0.1:{endpoint.port}/web",
                    availability_url=f"http://127.0.0.1:{endpoint.port}/available",
                    cdx_url=f"http://127.0.0.1:{endpoint.port}/cdx",
                    archive_host="127.0.0.1",
                )
                with patch.object(archive, "_worker_main", crash_exactly_one_worker):
                    manifest = archive.run_supervisor(
                        run_dir, manifest, processes=2, threads=4, save_every=20,
                        checkpoint_age_s=2, max_process_restarts=1, config=config,
                    )
                final = pl.read_parquet(archive.reduce_run(output, run_dir, manifest))
                self.assertTrue((run_dir / ".test-worker-crashed").exists())
                self.assertEqual(final.height, 120)
                self.assertEqual(final["id"].n_unique(), 120)
        finally:
            endpoint.close()

    def test_provider_outage_does_not_terminate_run_and_preserves_deferred_rows(self):
        endpoint = MockWayback()
        endpoint.mode = "fail"
        endpoint.start()
        try:
            with workspace_temp() as output:
                sample_frame(240).write_parquet(output / "trafilatura_scraped.parquet")
                run_dir = output / "archive_runs" / "resume-test"
                manifest = archive.prepare_run(output, run_dir, 8)
                config = archive.ArchiveConfig(
                    request_rate=500,
                    breaker_failures=5,
                    breaker_pause_s=0.05,
                    replay_root=f"http://127.0.0.1:{endpoint.port}/web",
                    availability_url=f"http://127.0.0.1:{endpoint.port}/available",
                    cdx_url=f"http://127.0.0.1:{endpoint.port}/cdx",
                    archive_host="127.0.0.1",
                )
                def restore_provider():
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        with endpoint.lock:
                            if endpoint.requests >= 45:
                                endpoint.mode = "normal"
                                return
                        time.sleep(0.01)
                    raise RuntimeError("test provider did not receive its planned outage calls")

                restorer = threading.Thread(target=restore_provider)
                restorer.start()
                manifest = archive.run_supervisor(
                    run_dir, manifest, processes=2, threads=4, save_every=10,
                    checkpoint_age_s=2, max_process_restarts=1, config=config,
                )
                restorer.join()
                self.assertGreaterEqual(endpoint.requests, 45)
                final = pl.read_parquet(archive.reduce_run(output, run_dir, manifest))
                self.assertEqual(final.height, 240)
                self.assertEqual(final["id"].n_unique(), 240)
                self.assertGreater(final.filter(pl.col("needs_archive")).height, 0)
        finally:
            endpoint.close()

    def test_cdx_outage_does_not_abort_remaining_direct_replays(self):
        endpoint = MockWayback()
        endpoint.mode = "cdx_fail"
        endpoint.start()
        try:
            with workspace_temp() as output:
                sample_frame(120).write_parquet(output / "trafilatura_scraped.parquet")
                run_dir = output / "archive_runs" / "cdx-test"
                manifest = archive.prepare_run(output, run_dir, 8)
                config = archive.ArchiveConfig(
                    request_rate=500,
                    breaker_failures=5,
                    breaker_pause_s=0.05,
                    replay_root=f"http://127.0.0.1:{endpoint.port}/web",
                    availability_url=f"http://127.0.0.1:{endpoint.port}/available",
                    cdx_url=f"http://127.0.0.1:{endpoint.port}/cdx",
                    archive_host="127.0.0.1",
                )
                manifest = archive.run_supervisor(
                    run_dir, manifest, processes=2, threads=4, save_every=20,
                    checkpoint_age_s=2, max_process_restarts=1, config=config,
                )
                self.assertTrue(manifest["provider"]["cdx"]["probe_pending"])
                final = pl.read_parquet(archive.reduce_run(output, run_dir, manifest))
                self.assertEqual(final.height, 120)
                self.assertEqual(final["id"].n_unique(), 120)
                self.assertGreater(final.filter(pl.col("fetch_method") == "wayback").height, 0)
                self.assertGreater(final.filter(pl.col("needs_archive")).height, 0)
        finally:
            endpoint.close()


if __name__ == "__main__":
    unittest.main()
