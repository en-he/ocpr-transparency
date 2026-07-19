import hashlib
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import config  # noqa: E402
import download  # noqa: E402


class DownloadTests(unittest.TestCase):
    def test_bulk_csv_years_extend_through_current_fiscal_year(self):
        fiscal_years = config.bulk_csv_years_through_current(date(2026, 4, 13))

        self.assertEqual(fiscal_years[0], "2025-2026")
        self.assertEqual(fiscal_years[-1], "2010-2011")
        self.assertIn("2023-2024", fiscal_years)

    def test_discover_live_refresh_years_starts_after_latest_local_csv(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            (tmp / "contratos_2022-2023.csv").write_text("", encoding="utf-8")
            (tmp / "contratos_2010-2011.csv").write_text("", encoding="utf-8")

            fiscal_years = download.discover_live_refresh_years(tmp, today=date(2026, 4, 13))

            self.assertEqual(fiscal_years, ["2022-2023", "2023-2024", "2024-2025", "2025-2026"])

    def test_discover_live_refresh_years_returns_empty_when_local_raw_is_current(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            (tmp / "contratos_2025-2026.csv").write_text("", encoding="utf-8")

            fiscal_years = download.discover_live_refresh_years(tmp, today=date(2026, 4, 13))

            self.assertEqual(fiscal_years, ["2025-2026"])

    def test_discover_live_refresh_years_skips_archive_only_years_as_refresh_base(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            (tmp / "contratos_2011-2012.csv").write_text("", encoding="utf-8")
            (tmp / "contratos_2010-2011.csv").write_text("", encoding="utf-8")

            fiscal_years = download.discover_live_refresh_years(tmp, today=date(2026, 4, 13))

            self.assertEqual(fiscal_years[0], "2010-2011")
            self.assertIn("2025-2026", fiscal_years)

    def _observation(self, year, body):
        url = (
            "https://consultacontratos.ocpr.gov.pr/contract/"
            f"downloadfrequentsearchfiscalyeardocument?q={year}"
        )
        return download.SourceObservation(
            fiscal_year=year,
            status="unlisted_available",
            advertised=False,
            requested_url=url,
            final_url=url,
            redirect_chain=(url,),
            http_status=200,
            content_type="text/csv",
            content_disposition=f'attachment; filename="contratos_{year}.csv"',
            byte_length=len(body),
            sha256=hashlib.sha256(body).hexdigest(),
            eligible=True,
            review_required=False,
            reason=None,
        )

    def test_identical_hash_does_not_bypass_active_symlink_rejection(self):
        body = (Path(__file__).parent / "fixtures" / "bulk" / "ocpr-bulk-v1.csv").read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            target = root / "outside.csv"
            target.write_bytes(body)
            raw = root / "raw"
            raw.mkdir()
            (raw / "contratos_2012-2013.csv").symlink_to(target)
            self.assertFalse(
                download.download_year(
                    "2012-2013",
                    raw,
                    observation=self._observation("2012-2013", body),
                    http_get=lambda *_args, **_kwargs: self.fail("must not fetch"),
                    quarantine_dir=root / "quarantine",
                    evidence_dir=root / "evidence",
                )
            )
            self.assertEqual(target.read_bytes(), body)

    def test_identical_discovered_hash_is_noop_without_second_get(self):
        body = (Path(__file__).parent / "fixtures" / "bulk" / "ocpr-bulk-v1.csv").read_bytes()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "raw"
            raw.mkdir()
            active = raw / "contratos_2012-2013.csv"
            active.write_bytes(body)

            def forbidden_get(*_args, **_kwargs):
                self.fail("unchanged hash must not be fetched a second time")

            self.assertTrue(
                download.download_year(
                    "2012-2013",
                    raw,
                    observation=self._observation("2012-2013", body),
                    http_get=forbidden_get,
                    quarantine_dir=root / "quarantine",
                    evidence_dir=root / "evidence",
                )
            )
            self.assertEqual(active.read_bytes(), body)
            self.assertFalse((root / "evidence").exists())

    def test_changed_valid_bytes_retain_prior_version_then_promote(self):
        old_body = (Path(__file__).parent / "fixtures" / "bulk" / "ocpr-bulk-v1.csv").read_bytes()
        new_body = old_body.replace(b"1500.00", b"1501.00", 1)
        year = "2012-2013"

        class Response:
            status_code = 200
            history = ()
            headers = {
                "Content-Type": "text/csv; charset=latin-1",
                "Content-Disposition": f'attachment; filename="contratos_{year}.csv"',
            }

            def __init__(self, url):
                self.url = url

            def iter_content(self, chunk_size=None):
                del chunk_size
                yield new_body

        calls = []

        def http_get(url, **kwargs):
            calls.append((url, kwargs))
            return Response(url)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "raw"
            raw.mkdir()
            active = raw / f"contratos_{year}.csv"
            active.write_bytes(old_body)
            evidence = root / "evidence"
            self.assertTrue(
                download.download_year(
                    year,
                    raw,
                    observation=self._observation(year, new_body),
                    http_get=http_get,
                    quarantine_dir=root / "quarantine",
                    evidence_dir=evidence,
                    captured_at="2026-07-19T00:00:00+00:00",
                )
            )
            self.assertEqual(active.read_bytes(), new_body)
            old_hash = hashlib.sha256(old_body).hexdigest()
            new_hash = hashlib.sha256(new_body).hexdigest()
            self.assertEqual((evidence / year / f"{old_hash}.csv").read_bytes(), old_body)
            self.assertEqual((evidence / year / f"{new_hash}.csv").read_bytes(), new_body)
            self.assertFalse(calls[0][1]["allow_redirects"])
            self.assertTrue(calls[0][1]["stream"])

    def test_invalid_capture_never_replaces_active_snapshot(self):
        year = "2012-2013"
        old_body = b"already-certified-active"
        invalid_body = b"<html>gateway error</html>"

        class Response:
            status_code = 200
            history = ()
            headers = {
                "Content-Type": "text/csv",
                "Content-Disposition": f'attachment; filename="contratos_{year}.csv"',
            }

            def __init__(self, url):
                self.url = url

            def iter_content(self, chunk_size=None):
                del chunk_size
                yield invalid_body

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw = root / "raw"
            raw.mkdir()
            active = raw / f"contratos_{year}.csv"
            active.write_bytes(old_body)
            observation = self._observation(year, invalid_body)
            self.assertFalse(
                download.download_year(
                    year,
                    raw,
                    observation=observation,
                    http_get=lambda url, **_kwargs: Response(url),
                    quarantine_dir=root / "quarantine",
                    evidence_dir=root / "evidence",
                    captured_at="2026-07-19T00:00:00+00:00",
                )
            )
            self.assertEqual(active.read_bytes(), old_body)
            self.assertTrue(any((root / "quarantine").iterdir()))
            self.assertFalse((root / "evidence").exists())


if __name__ == "__main__":
    unittest.main()
