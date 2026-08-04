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

            def launch(self, *, headless, channel=None):
                self.headless = headless
                self.channel = channel
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

    def test_worker_counts_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "workers"):
            module.scrape_all(
                module.pd.DataFrame(), Path("."), 0, 1, 1, 10
            )

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
