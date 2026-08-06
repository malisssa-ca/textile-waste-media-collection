import importlib.util
import json
import queue
import sys
import threading
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).resolve().parents[1] / "textile_waste_p2_scrape.py"
SPEC = importlib.util.spec_from_file_location("textile_waste_p2_scrape", MODULE_PATH)
module = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(module)


class ScrapePipelineTests(unittest.TestCase):
    def _run_mocked_live_browser_case(self, browser_outcome, browser_result=None):
        row = module.pd.DataFrame([{
            "id": "browser-case", "url": "https://example.com/article",
            "title": "Title", "publish_date": "2020-01-01",
        }])
        requests_result = {
            **row.iloc[0].to_dict(), **module._SCRAPE_DEFAULTS,
            "scrape_status": "ok", "fetch_method": "requests",
            "text": "Requests fallback", "text_length": 17,
            "verification_level": "unverified_text",
        }

        def build_result(*args, **kwargs):
            method = args[3]
            if method == "requests":
                return dict(requests_result)
            return dict(browser_result)

        captured = []
        fake_batch = mock.MagicMock()
        fake_batch.__len__.return_value = 1

        def capture_rows(rows, **kwargs):
            captured.extend(dict(item) for item in rows)
            return fake_batch

        with (
            mock.patch.object(
                module, "_l1_fetch",
                return_value=module._FetchOutcome(
                    content=b"<html></html>", final_url="https://example.com/article",
                    status_code=200,
                ),
            ),
            mock.patch.object(module, "_l3_fetch", return_value=browser_outcome),
            mock.patch.object(module, "_build_result", side_effect=build_result),
            mock.patch.object(module.pl, "from_dicts", side_effect=capture_rows),
            mock.patch.object(module.pl, "read_parquet", return_value=mock.MagicMock(height=1)),
            mock.patch.object(module.os, "replace"),
            mock.patch.object(module, "_merge_outputs", return_value=Path("missing.parquet")),
            mock.patch.object(module, "_atomic_write_text"),
        ):
            module.scrape_all(row, Path("tests"), 1, 1, 0, 10, mode="live-only")
        return captured[0]

    def test_build_result_has_no_html2txt_fallback(self):
        module._USE_KEYWORD_FILTER = False
        with mock.patch.object(module, "_extract_l1", side_effect=RuntimeError("boom")):
            result = module._build_result(
                {"id": "1", "url": "https://example.com/article"},
                "<html><body>sample</body></html>",
                "https://example.com/article",
                "requests",
            )

        self.assertEqual(result["scrape_status"], "extract_error")
        self.assertEqual(result["verification_level"], "no_text")

    def test_build_result_skips_keyword_filter_when_disabled(self):
        module._USE_KEYWORD_FILTER = False
        with (
            mock.patch.object(module, "_extract_l1", return_value={"text": "some unrelated page text", "title": ""}),
        ):
            result = module._build_result(
                {"id": "2", "url": "https://example.com/article"},
                "<html><body>sample</body></html>",
                "https://example.com/article",
                "requests",
            )

        self.assertEqual(result["scrape_status"], "ok")
        self.assertEqual(result["extract_method"], "trafilatura")

    def test_build_result_does_not_truncate_long_text(self):
        module._USE_KEYWORD_FILTER = False
        long_text = "article text " * 20_000
        with mock.patch.object(
            module,
            "_extract_l1",
            return_value={"text": long_text, "title": "Long article"},
        ):
            result = module._build_result(
                {"id": "3", "url": "https://example.com/long-article"},
                "<html><body>sample</body></html>",
                "https://example.com/long-article",
                "requests",
            )

        self.assertEqual(result["text"], long_text)
        self.assertEqual(len(result["text"]), len(long_text))

    def test_extract_l1_preserves_tables_and_repeated_passages(self):
        expected = json.dumps({"text": "full article"})
        with mock.patch.object(module.trafilatura, "extract", return_value=expected) as extract:
            result = module._extract_l1(object(), "https://example.com/article")

        self.assertEqual(result, {"text": "full article"})
        self.assertTrue(extract.call_args.kwargs["include_tables"])
        self.assertFalse(extract.call_args.kwargs["deduplicate"])

    def test_trafilatura_has_no_practical_input_size_ceiling(self):
        self.assertEqual(
            module._TRAF_CONFIG.getint("DEFAULT", "MAX_FILE_SIZE"),
            sys.maxsize,
        )

    def test_playwright_instance_is_owned_and_closed_per_thread(self):
        created = []

        class FakeBrowser:
            def __init__(self, owner):
                self.owner = owner
                self.closed = False

            def close(self):
                self.closed = True

        class FakeChromium:
            def __init__(self, owner):
                self.owner = owner

            def launch(self, *, headless, channel=None, timeout=None):
                self.headless = headless
                self.channel = channel
                self.timeout = timeout
                return FakeBrowser(self.owner)

        class FakeInstance:
            def __init__(self, owner):
                self.owner = owner
                self.chromium = FakeChromium(owner)
                self.stopped = False

            def stop(self):
                self.stopped = True

        class FakeStarter:
            def start(self):
                instance = FakeInstance(threading.get_ident())
                created.append(instance)
                return instance

        results = queue.Queue()

        def worker():
            first = module._get_pw_browser()
            second = module._get_pw_browser()
            results.put((first.owner, first is second))
            module._close_pw_browser()

        module._pw_launch_failure = None
        with mock.patch.object(module, "sync_playwright", side_effect=FakeStarter):
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        ownership = [results.get_nowait() for _ in threads]
        self.assertEqual(len(created), 2)
        self.assertEqual(len({owner for owner, _ in ownership}), 2)
        self.assertTrue(all(reused for _, reused in ownership))
        self.assertTrue(all(instance.stopped for instance in created))
        self.assertTrue(all(instance.chromium.headless for instance in created))
        expected_channel = "chrome" if sys.platform == "win32" else None
        self.assertTrue(all(instance.chromium.channel == expected_channel for instance in created))
        self.assertTrue(all(
            instance.chromium.timeout == module.PLAYWRIGHT_TIMEOUT_S * 1000
            for instance in created
        ))

    def test_playwright_hard_deadline_discards_stalled_process(self):
        class FakeConnection:
            def send(self, message):
                self.message = message

            def poll(self, timeout):
                return False

        class FakeProcess:
            def is_alive(self):
                return True

        supervisor = module._PlaywrightSupervisor()
        supervisor._connection = FakeConnection()
        supervisor._process = FakeProcess()
        with (
            mock.patch.object(module, "PLAYWRIGHT_HARD_TIMEOUT_S", 0.01),
            mock.patch.object(supervisor, "_discard") as discard,
        ):
            outcome = supervisor.fetch("https://example.com/stalled")

        self.assertEqual(outcome.error, "playwright_hard_timeout")
        discard.assert_called_once_with(force=True)

    def test_playwright_outcome_distinguishes_browser_results(self):
        base = {
            "id": "browser-case", "url": "https://example.com/article",
            **module._SCRAPE_DEFAULTS, "fetch_method": "playwright",
        }
        cases = (
            (
                module._FetchOutcome(error="playwright_hard_timeout"),
                None, "hard_timeout", "Requests fallback",
            ),
            (
                module._FetchOutcome(error="TimeoutError"),
                None, "fetch_failed", "Requests fallback",
            ),
            (
                module._FetchOutcome(
                    content=b"<html></html>", final_url="https://example.com/article",
                    status_code=200,
                ),
                {**base, "scrape_status": "extract_error", "verification_level": "no_text"},
                "no_text", "Requests fallback",
            ),
            (
                module._FetchOutcome(
                    content=b"<html></html>", final_url="https://example.com/article",
                    status_code=200,
                ),
                {
                    **base, "scrape_status": "ok", "text": "Browser text",
                    "text_length": 12, "verification_level": "unverified_text",
                },
                "unverified_text", "Requests fallback",
            ),
            (
                module._FetchOutcome(
                    content=b"<html></html>", final_url="https://example.com/article",
                    status_code=200,
                ),
                {
                    **base, "scrape_status": "ok", "text": "Accepted browser text",
                    "text_length": 21, "verification_level": "page_verified",
                },
                "accepted", "Accepted browser text",
            ),
        )
        for browser_fetch, browser_result, expected_outcome, expected_text in cases:
            with self.subTest(playwright_outcome=expected_outcome):
                result = self._run_mocked_live_browser_case(browser_fetch, browser_result)
                self.assertEqual(result["playwright_outcome"], expected_outcome)
                self.assertEqual(result["text"], expected_text)
                self.assertEqual(result["needs_archive"], expected_outcome != "accepted")

    def test_verified_result_preserves_longest_unverified_from_any_fetch_layer(self):
        row = module.pd.DataFrame([{
            "id": "fallback-case", "url": "https://example.com/article",
            "title": "Title", "publish_date": "2020-01-01",
        }])
        cases = (
            ({"requests": 500, "playwright": 300, "wayback": 200}, "requests"),
            ({"requests": 300, "playwright": 500, "wayback": 200}, "playwright"),
            ({"requests": 300, "playwright": 200, "wayback": 500}, "wayback"),
        )
        for lengths, expected_method in cases:
            with self.subTest(expected_method=expected_method):
                wayback_builds = 0

                def build_result(*args, **kwargs):
                    nonlocal wayback_builds
                    method = args[3]
                    if method == "wayback":
                        wayback_builds += 1
                    verified = method == "wayback" and wayback_builds == 2
                    text = "V" * 50 if verified else method[0].upper() * lengths[method]
                    return {
                        **row.iloc[0].to_dict(), **module._SCRAPE_DEFAULTS,
                        "scrape_status": "ok", "fetch_method": method,
                        "text": text, "text_length": len(text),
                        "target_verified": verified,
                        "verification_level": "strict_verified" if verified else "unverified_text",
                    }

                captured = []
                fake_batch = mock.MagicMock()
                fake_batch.__len__.return_value = 1

                def capture_rows(rows, **kwargs):
                    captured.extend(dict(item) for item in rows)
                    return fake_batch

                with (
                    mock.patch.object(
                        module, "_l1_fetch",
                        return_value=module._FetchOutcome(
                            content=b"requests", final_url="https://example.com/article",
                            status_code=200,
                        ),
                    ),
                    mock.patch.object(
                        module, "_l3_fetch",
                        return_value=module._FetchOutcome(
                            content=b"playwright", final_url="https://example.com/article",
                            status_code=200,
                        ),
                    ),
                    mock.patch.object(
                        module, "_l4_fetch_candidates",
                        return_value=[
                            module._FetchOutcome(
                                content=b"archive-one", final_url="https://example.com/article",
                                status_code=200,
                            ),
                            module._FetchOutcome(
                                content=b"archive-two", final_url="https://example.com/article",
                                status_code=200,
                            ),
                        ],
                    ),
                    mock.patch.object(module, "_build_result", side_effect=build_result),
                    mock.patch.object(module.pl, "from_dicts", side_effect=capture_rows),
                    mock.patch.object(
                        module.pl, "read_parquet", return_value=mock.MagicMock(height=1)
                    ),
                    mock.patch.object(module.os, "replace"),
                    mock.patch.object(
                        module, "_merge_outputs", return_value=Path("missing.parquet")
                    ),
                    mock.patch.object(module, "_atomic_write_text"),
                ):
                    module.scrape_all(row, Path("tests"), 1, 1, 1, 10, mode="complete")

                result = captured[0]
                self.assertEqual(result["text"], "V" * 50)
                self.assertEqual(result["fallback_fetch_method"], expected_method)
                self.assertEqual(result["fallback_text_length"], lengths[expected_method])
                self.assertEqual(
                    result["fallback_text"],
                    expected_method[0].upper() * lengths[expected_method],
                )

    def test_longest_unverified_is_primary_when_nothing_is_verified(self):
        browser_text = "Longer browser candidate " * 10
        browser_result = {
            "id": "browser-case", "url": "https://example.com/article",
            **module._SCRAPE_DEFAULTS, "scrape_status": "ok",
            "fetch_method": "playwright", "text": browser_text,
            "text_length": len(browser_text), "verification_level": "unverified_text",
        }
        result = self._run_mocked_live_browser_case(
            module._FetchOutcome(
                content=b"<html></html>", final_url="https://example.com/article",
                status_code=200,
            ),
            browser_result,
        )

        self.assertEqual(result["text"], browser_text)
        self.assertEqual(result["fetch_method"], "playwright")
        self.assertIsNone(result["fallback_text"])
        self.assertIsNone(result["fallback_fetch_method"])
        self.assertIsNone(result["fallback_text_length"])

    def test_verified_extraction_preserves_longer_unverified_extractor_text(self):
        long_unverified = "L" * 500
        short_verified = "S" * 50
        with (
            mock.patch.object(
                module,
                "_jsonld_article_candidates",
                return_value=[{"text": long_unverified, "urls": [], "headline": None}],
            ),
            mock.patch.object(module, "_extract_itemprop_bodies", return_value=[short_verified]),
            mock.patch.object(module, "_identity_sources", side_effect=[[], ["canonical"]]),
        ):
            result = module._build_result(
                {"id": "extract-fallback", "url": "https://example.com/article"},
                "<html><body></body></html>",
                "https://example.com/article",
                "requests",
            )

        self.assertEqual(result["text"], short_verified)
        self.assertEqual(result["verification_level"], "strict_verified")
        self.assertEqual(result["fallback_text"], long_unverified)
        self.assertEqual(result["fallback_fetch_method"], "requests")
        self.assertEqual(result["fallback_text_length"], len(long_unverified))

    def test_http_retry_does_not_honor_unbounded_retry_after(self):
        module._thread_local.sessions = {}
        session = module._get_session("requests")
        retry = session.get_adapter("https://").max_retries

        self.assertFalse(retry.respect_retry_after_header)
        self.assertEqual(retry.total, 1)

    def test_unexpected_worker_failure_aborts_instead_of_waiting_forever(self):
        row = module.pd.DataFrame([{
            "id": "broken", "url": "https://example.com/article",
            "title": "Title", "publish_date": "2020-01-01",
        }])
        with (
            self.subTest("requests worker"),
            mock.patch.object(
                module,
                "_l1_fetch",
                return_value=module._FetchOutcome(
                    content=b"<html></html>",
                    final_url="https://example.com/article",
                    status_code=200,
                ),
            ),
            mock.patch.object(module, "_build_result", side_effect=RuntimeError("broken extractor")),
        ):
            with self.assertRaisesRegex(RuntimeError, "requests worker failed"):
                module.scrape_all(
                    row, Path("tests"), 1, 1, 0, 10, mode="live-only"
                )

    def test_live_only_requests_failure_still_reaches_playwright(self):
        row = module.pd.DataFrame([{
            "id": "live", "url": "https://example.com/article",
            "title": "Title", "publish_date": "2020-01-01",
        }])
        browser_result = {
            **row.iloc[0].to_dict(),
            **module._SCRAPE_DEFAULTS,
            "scrape_status": "ok",
            "fetch_method": "playwright",
            "text": "Recovered article",
            "text_length": 17,
            "verification_level": "page_verified",
        }
        fake_batch = mock.MagicMock()
        fake_batch.__len__.return_value = 1
        fake_readback = mock.MagicMock(height=1)
        with (
            mock.patch.object(
                module, "_l1_fetch",
                return_value=module._FetchOutcome(error="ConnectTimeout"),
            ),
            mock.patch.object(
                module, "_l3_fetch",
                return_value=module._FetchOutcome(
                    content=b"<html></html>", final_url="https://example.com/article",
                    status_code=200,
                ),
            ) as browser_fetch,
            mock.patch.object(module, "_build_result", return_value=browser_result),
            mock.patch.object(module, "_close_pw_browser"),
            mock.patch.object(module.pl, "from_dicts", return_value=fake_batch),
            mock.patch.object(module.pl, "read_parquet", return_value=fake_readback),
            mock.patch.object(module.os, "replace"),
            mock.patch.object(module, "_merge_outputs", return_value=Path("missing.parquet")),
            mock.patch.object(module, "_atomic_write_text"),
        ):
            result = module.scrape_all(
                row, Path("tests"), 1, 1, 0, 10, mode="live-only"
            )

        browser_fetch.assert_called_once_with("https://example.com/article")
        self.assertTrue(result.empty)

    def test_complete_mode_uses_both_conditional_fetch_routes(self):
        row = module.pd.DataFrame([{
            "id": "route", "url": "https://example.com/article",
            "title": "Title", "publish_date": "2020-01-01",
        }])
        terminal = {
            **row.iloc[0].to_dict(),
            **module._SCRAPE_DEFAULTS,
            "scrape_status": "ok",
            "fetch_method": "wayback",
            "text": "Recovered article",
            "text_length": 17,
            "verification_level": "page_verified",
        }

        for first_outcome, expected_browser_calls, expected_browser_outcome in (
            (module._FetchOutcome(status_code=404, error="http_404"), 0, None),
            (module._FetchOutcome(error="ConnectTimeout"), 1, "hard_timeout"),
        ):
            fake_batch = mock.MagicMock()
            fake_batch.__len__.return_value = 1
            with (
                self.subTest(first_outcome=first_outcome),
                mock.patch.object(module, "_l1_fetch", return_value=first_outcome),
                mock.patch.object(
                    module, "_l3_fetch",
                    return_value=module._FetchOutcome(error="playwright_hard_timeout"),
                ) as browser_fetch,
                mock.patch.object(
                    module, "_l4_fetch_candidates",
                    return_value=[module._FetchOutcome(
                        content=b"<html></html>",
                        final_url="https://example.com/article",
                        status_code=200,
                    )],
                ) as archive_fetch,
                mock.patch.object(module, "_build_result", return_value=terminal),
                mock.patch.object(module, "_close_pw_browser"),
                mock.patch.object(module.pl, "from_dicts", return_value=fake_batch),
                mock.patch.object(
                    module.pl, "read_parquet", return_value=mock.MagicMock(height=1)
                ),
                mock.patch.object(module.os, "replace"),
                mock.patch.object(module, "_merge_outputs", return_value=Path("missing.parquet")),
                mock.patch.object(module, "_atomic_write_text"),
            ):
                result = module.scrape_all(
                    row, Path("tests"), 1, 1, 1, 10, mode="complete"
                )

            self.assertEqual(browser_fetch.call_count, expected_browser_calls)
            archive_fetch.assert_called_once()
            self.assertEqual(terminal["playwright_outcome"], expected_browser_outcome)
            self.assertTrue(result.empty)

    def test_worker_counts_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "workers"):
            module.scrape_all(
                module.pd.DataFrame(), Path("."), 0, 1, 1, 10
            )

    def test_all_modes_finish_when_there_are_no_pending_rows(self):
        empty = module.pd.DataFrame(columns=["id", "url", "title", "publish_date"])
        worker_counts = {
            "complete": (1, 1, 1),
            "live-only": (1, 1, 0),
            "archive-only": (0, 0, 1),
        }
        for mode, workers in worker_counts.items():
            with (
                self.subTest(mode=mode),
                mock.patch.object(module, "_atomic_write_text"),
                mock.patch.object(
                    module, "_merge_outputs", return_value=Path("missing-test-output.parquet")
                ),
            ):
                result = module.scrape_all(
                    empty, Path("tests"), *workers, 10, mode=mode
                )
            self.assertTrue(result.empty)

    def test_wayback_variants_have_approved_order(self):
        self.assertEqual(
            module._archive_url_variants("https://example.com/a?utm=x#part"),
            ["https://example.com/a?utm=x", "http://example.com/a?utm=x", "https://example.com/a"],
        )

    def test_wayback_selects_at_most_two_snapshots(self):
        captures = [
            module._ArchiveCapture("20200101000000", "https://example.com/a", "one"),
            module._ArchiveCapture("20200201000000", "https://example.com/a", "two"),
            module._ArchiveCapture("20200301000000", "https://example.com/a", "three"),
        ]
        with mock.patch.object(module, "_query_wayback_cdx", return_value=captures):
            selected = module._wayback_snapshot_refs(["https://example.com/a"], "2020-01-15")

        self.assertEqual([capture.fetch_url for capture in selected], ["one", "two"])

    def test_wayback_stops_url_lookup_after_exact_capture(self):
        capture = module._ArchiveCapture("20200101000000", "https://example.com/a", "exact")
        with mock.patch.object(module, "_query_wayback_cdx", return_value=[capture]) as lookup:
            selected = module._wayback_snapshot_refs(
                ["https://example.com/a?utm=x"], "2020-01-15"
            )

        self.assertEqual([item.fetch_url for item in selected], ["exact"])
        lookup.assert_called_once_with("https://example.com/a?utm=x", "20200115000000")

    def test_wayback_uses_url_forms_only_after_prior_form_has_no_capture(self):
        http_capture = module._ArchiveCapture("20200101000000", "http://example.com/a", "http")
        with mock.patch.object(module, "_query_wayback_cdx", side_effect=[[], [http_capture]]) as lookup:
            selected = module._wayback_snapshot_refs(
                ["https://example.com/a?utm=x"], "2020-01-15"
            )

        self.assertEqual([item.fetch_url for item in selected], ["http"])
        self.assertEqual(
            [call.args[0] for call in lookup.call_args_list],
            ["https://example.com/a?utm=x", "http://example.com/a?utm=x"],
        )

    def test_page_verified_is_terminal_without_strict_boolean(self):
        result = {"scrape_status": "ok", "target_verified": False,
                  "verification_level": "page_verified", "text": "article"}
        self.assertTrue(module._is_terminal_result(result))

    def test_needs_archive_is_a_queue_flag_with_v5_migration(self):
        legacy_failure = {"scrape_status": "fetch_error", "text": ""}
        completed_archive_failure = {
            "scrape_status": "fetch_error", "text": "", "needs_archive": False,
        }
        temporary_archive_failure = {
            "scrape_status": "fetch_error", "text": "", "needs_archive": True,
        }

        self.assertTrue(module._needs_archive_attempt(legacy_failure))
        self.assertFalse(module._needs_archive_attempt(completed_archive_failure))
        self.assertTrue(module._needs_archive_attempt(temporary_archive_failure))

    def test_wayback_circuit_breaker_opens_after_five_provider_errors(self):
        module._wayback_consecutive_failures = 0
        module._wayback_open_until = 0.0
        for _ in range(5):
            module._record_wayback_health(module._FetchOutcome(error="ConnectionError"))
        self.assertFalse(module._wayback_allowed())
        module._wayback_open_until = 0.0

    def test_wayback_breaker_waits_instead_of_skipping_queued_work(self):
        module._wayback_open_until = 160.0
        try:
            with (
                mock.patch.object(module.time, "monotonic", side_effect=[100.0, 160.0]),
                mock.patch.object(module.time, "sleep") as sleep,
            ):
                self.assertTrue(module._wait_for_wayback())
            sleep.assert_called_once_with(60.0)
        finally:
            module._wayback_open_until = 0.0

    def test_wayback_breaker_wait_honors_clean_shutdown(self):
        stop = threading.Event()
        stop.set()
        module._wayback_open_until = 160.0
        try:
            with mock.patch.object(module.time, "monotonic", return_value=100.0):
                self.assertFalse(module._wait_for_wayback(stop))
        finally:
            module._wayback_open_until = 0.0

    def test_wayback_cdx_failure_reaches_circuit_breaker(self):
        with mock.patch.object(
            module, "_wayback_snapshot_refs", side_effect=module.requests.ConnectionError("down")
        ):
            outcomes = list(module._l4_fetch_candidates("https://example.com/article"))

        self.assertEqual(len(outcomes), 1)
        self.assertEqual(outcomes[0].error, "ConnectionError")

    def test_trafilatura_list_metadata_is_normalized_to_existing_string_schema(self):
        result = {}
        module._apply_traf_metadata(result, {"categories": ["Kultur"], "tags": ["news", "local"]})

        self.assertEqual(result["traf_categories"], "Kultur")
        self.assertEqual(result["traf_tags"], "news, local")

    def test_source_article_url_keeps_only_explicit_external_read_more_link(self):
        html = """<html><body>
        <a href="/latest">More news</a>
        <a href="https://www.dn.se/article">Läs mer på DN</a>
        </body></html>"""
        with mock.patch.object(module, "_extract_l1", return_value={"text": "Recovered text"}):
            result = module._build_result(
                {"id": "source", "url": "https://www.pressen.se/123.html"},
                html, "https://www.pressen.se/123.html", "requests",
            )

        self.assertEqual(result["source_article_url"], "https://www.dn.se/article")

    def test_source_article_url_keeps_observed_focus_original_link(self):
        html = """<html><body><p>Das Original zu diesem Beitrag
        &quot;<a href="https://www.swyrl.tv/article">Original headline</a>&quot;
        stammt von teleschau.</p></body></html>"""
        with mock.patch.object(module, "_extract_l1", return_value={"text": "Recovered text"}):
            result = module._build_result(
                {"id": "focus", "url": "https://www.focus.de/article"},
                html, "https://www.focus.de/article", "requests",
            )

        self.assertEqual(result["source_article_url"], "https://www.swyrl.tv/article")

    def test_source_article_url_rejects_observed_paywall_continue_label(self):
        html = """<html><body>
        <a href="https://abo.die-glocke.de/subscribe">Jetzt weiterlesen mit G+</a>
        </body></html>"""
        with mock.patch.object(module, "_extract_l1", return_value={"text": "Recovered text"}):
            result = module._build_result(
                {"id": "paywall", "url": "https://www.die-glocke.de/article"},
                html, "https://www.die-glocke.de/article", "requests",
            )

        self.assertIsNone(result["source_article_url"])

    def test_jsonld_acceptance_does_not_run_full_page_trafilatura(self):
        html = '''<html><head><script type="application/ld+json">
        {"@type":"NewsArticle","url":"https://example.com/article",
         "headline":"Target title","articleBody":"Recovered text"}
        </script></head><body></body></html>'''
        with mock.patch.object(module, "_extract_l1", side_effect=AssertionError("must stay lazy")) as extract:
            result = module._build_result(
                {"id": "lazy", "url": "https://example.com/article", "title": "Target title"},
                html, "https://example.com/article", "requests",
            )
        self.assertEqual(result["verification_level"], "strict_verified")
        extract.assert_not_called()



if __name__ == "__main__":
    unittest.main()
