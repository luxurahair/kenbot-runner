"""Offline archive integrity tests: never extracts or restores content."""
import io
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch
from monitoring.backup_guard import inspect_archive, safe_member, latest_archive, main

class BackupGuardTests(unittest.TestCase):
    def make_archive(self, location, members=None):
        filename = Path(location) / "locked-features-2026-10-22T12-00-00Z.tar.gz"
        if members is None:
            members = {"kenbot-dashboard/frontend/src/features/reprise-evaluations-grossistes/verified.js": b"ok"}
        with tarfile.open(filename, "w:gz") as dst:
            for name, data in members.items():
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                dst.addfile(entry, io.BytesIO(data))
        return filename

    def test_safe_feature(self):
        self.assertTrue(safe_member("kenbot-dashboard/frontend/src/features/reprise-evaluations-grossistes/foo.js"))
    def test_safe_regression(self):
        self.assertTrue(safe_member("tests/test_critical_regression.py"))
    def test_safe_document(self):
        self.assertTrue(safe_member("REPRISE_EVALUATIONS_LOCKED.md"))
    def test_reject_absolute(self):
        self.assertFalse(safe_member("/etc/passwd"))
    def test_reject_parent(self):
        self.assertFalse(safe_member("kenbot-dashboard/frontend/src/features/reprise-evaluations-grossistes/../../secret"))
    def test_reject_unrelated(self):
        self.assertFalse(safe_member("server.py"))
    def test_reject_empty(self):
        self.assertFalse(safe_member(""))
    def test_missing(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(inspect_archive(str(Path(d)/"missing.tar.gz"))["status"], "MISSING")
    def test_verified(self):
        with tempfile.TemporaryDirectory() as d:
            p=self.make_archive(d)
            self.assertEqual(inspect_archive(str(p))["status"], "VERIFIED")
    def test_hash_present(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(len(inspect_archive(str(self.make_archive(d)))["sha256"]),64)
    def test_stale(self):
        with tempfile.TemporaryDirectory() as d:
            p=self.make_archive(d)
            os.utime(p,(1000000000,1000000000))
            self.assertEqual(inspect_archive(str(p))["status"], "STALE")
    def test_future_timestamp(self):
        with tempfile.TemporaryDirectory() as d:
            p=self.make_archive(d)
            os.utime(p,(2000000000,2000000000))
            self.assertEqual(inspect_archive(str(p))["status"], "INVALID_TIMESTAMP")
    def test_unsafe_member(self):
        with tempfile.TemporaryDirectory() as d:
            p=self.make_archive(d,{"../../etc/passwd":b"x"})
            self.assertEqual(inspect_archive(str(p))["status"], "UNSAFE")
    def test_incomplete(self):
        with tempfile.TemporaryDirectory() as d:
            p=self.make_archive(d,{"tests/test_critical_regression.py":b"x"})
            self.assertEqual(inspect_archive(str(p))["status"], "INCOMPLETE")
    def test_corrupt(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"locked-features-x.tar.gz"
            p.write_bytes(b"not a tar")
            self.assertEqual(inspect_archive(str(p))["status"], "CORRUPT")
    def test_latest_missing(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(latest_archive(d))
    def test_latest_present(self):
        with tempfile.TemporaryDirectory() as d:
            p=self.make_archive(d)
            self.assertEqual(latest_archive(d),p)
    def test_main_missing(self):
        with tempfile.TemporaryDirectory() as d:
            with patch.dict(os.environ,{"KENBOT_BACKUP_DIR":d}):
                with patch("builtins.print"):
                    self.assertEqual(main(),2)
    def test_main_verified(self):
        with tempfile.TemporaryDirectory() as d:
            self.make_archive(d)
            with patch.dict(os.environ,{"KENBOT_BACKUP_DIR":d}):
                with patch("builtins.print"):
                    self.assertEqual(main(),0)
    def test_main_stale(self):
        with tempfile.TemporaryDirectory() as d:
            p=self.make_archive(d)
            os.utime(p,(1000000000,1000000000))
            with patch.dict(os.environ,{"KENBOT_BACKUP_DIR":d}):
                with patch("builtins.print"):
                    self.assertEqual(main(),1)

if __name__=="__main__": unittest.main()
