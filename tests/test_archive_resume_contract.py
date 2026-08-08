"""Regression tests for durable trace-aware archive resumption."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import archive_parallel as archive


class ArchiveResumeContractTests(unittest.TestCase):
    def test_reliable_direct_nonterminal_resumes_at_availability(self):
        row = {
            "needs_archive": True,
            "attempt_trace": [{"method": "wayback", "phase": "direct", "status": 404}],
        }
        self.assertEqual(archive._resume_stage(row), "availability")

    def test_direct_provider_error_retries_direct(self):
        row = {
            "needs_archive": True,
            "attempt_trace": [{
                "method": "wayback", "phase": "direct", "error": "ReadTimeout"
            }],
        }
        self.assertEqual(archive._resume_stage(row), "direct")

    def test_unavailable_fallback_is_retried_not_treated_as_no_capture(self):
        row = {
            "needs_archive": True,
            "attempt_trace": [
                {"method": "wayback", "phase": "direct", "status": 404},
                {
                    "method": "wayback",
                    "phase": "availability",
                    "error": "availability_unavailable",
                },
            ],
        }
        self.assertEqual(archive._resume_stage(row), "availability")

    def test_reliable_availability_nonterminal_advances_to_cdx(self):
        row = {
            "needs_archive": True,
            "attempt_trace": [
                {"method": "wayback", "phase": "direct", "status": 404},
                {"method": "wayback", "phase": "availability", "status": 200},
            ],
        }
        self.assertEqual(archive._resume_stage(row), "cdx")

    def test_all_reliable_discovery_stages_exhausted_is_no_longer_pending(self):
        row = {
            "needs_archive": True,
            "attempt_trace": [
                {"method": "wayback", "phase": "direct", "status": 404},
                {"method": "wayback", "phase": "availability", "status": 200},
                {"method": "wayback", "phase": "cdx", "status": 200},
            ],
        }
        self.assertEqual(archive._resume_stage(row), "done")

    def test_pending_false_is_always_done(self):
        self.assertEqual(archive._resume_stage({"needs_archive": False}), "done")

if __name__ == "__main__":
    unittest.main()
