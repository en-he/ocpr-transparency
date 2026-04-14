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


if __name__ == "__main__":
    unittest.main()
