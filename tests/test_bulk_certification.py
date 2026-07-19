import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = REPO_ROOT / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import generate_bulk_certification as generator  # noqa: E402


class BulkCertificationArtifactTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.generated = generator.build_artifacts(REPO_ROOT)
        cls.manifest = json.loads(
            cls.generated[generator.MANIFEST_PATH].decode("utf-8")
        )
        cls.reports = {
            snapshot["fiscal_year"]: json.loads(
                cls.generated[snapshot["report_path"]].decode("utf-8")
            )
            for snapshot in cls.manifest["snapshots"]
        }

    def test_observed_capture_metadata_stabilizes_precommit_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            year = "2024-2025"
            raw = root / "data" / "raw" / f"contratos_{year}.csv"
            raw.parent.mkdir(parents=True)
            raw.write_bytes(b"exact-new-source-bytes")
            digest = hashlib.sha256(raw.read_bytes()).hexdigest()
            sidecar = (
                root
                / "data"
                / "evidence"
                / "bulk"
                / year
                / f"{digest}.csv.json"
            )
            sidecar.parent.mkdir(parents=True)
            sidecar.write_text(
                json.dumps(
                    {
                        "sha256": digest,
                        "captured_at": "2026-07-19T18:55:00+00:00",
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                generator._capture_metadata(root, raw),
                ("2026-07-19T18:55:00+00:00", "observed"),
            )

    def test_tracked_artifacts_match_deterministic_generation(self):
        self.assertTrue(generator.check_artifacts(REPO_ROOT, self.generated))
        for relative_path, expected in self.generated.items():
            with self.subTest(path=relative_path):
                self.assertEqual((REPO_ROOT / relative_path).read_bytes(), expected)

    def test_repeated_generation_is_byte_identical(self):
        regenerated = generator.build_artifacts(REPO_ROOT)
        self.assertEqual(regenerated, self.generated)

    def test_manifest_covers_exactly_thirteen_preserved_snapshots(self):
        snapshots = self.manifest["snapshots"]
        raw_files = sorted((REPO_ROOT / "data" / "raw").glob("contratos_*.csv"))
        self.assertEqual(len(snapshots), 13)
        self.assertEqual(len(self.reports), 13)
        self.assertEqual(
            [snapshot["source_file"] for snapshot in snapshots],
            [path.relative_to(REPO_ROOT).as_posix() for path in raw_files],
        )
        for snapshot in snapshots:
            source_path = REPO_ROOT / snapshot["source_file"]
            with self.subTest(year=snapshot["fiscal_year"]):
                self.assertEqual(
                    snapshot["sha256"],
                    hashlib.sha256(source_path.read_bytes()).hexdigest(),
                )
                self.assertEqual(snapshot["byte_length"], source_path.stat().st_size)
                self.assertEqual(snapshot["status"], "certified")
                self.assertEqual(snapshot["capture_time_status"], "git_first_seen")
                self.assertTrue(snapshot["capture_time"])

    def test_source_channels_and_unavailable_year_are_honest(self):
        channels = {
            snapshot["fiscal_year"]: snapshot["source_channel"]
            for snapshot in self.manifest["snapshots"]
        }
        self.assertEqual(channels["2010-2011"], "archive_bulk")
        self.assertEqual(channels["2011-2012"], "archive_bulk")
        self.assertEqual(
            self.reports["2010-2011"]["archive_url"],
            generator.ARCHIVE_CAPTURE_URLS["2010-2011"],
        )
        self.assertEqual(
            self.reports["2011-2012"]["archive_url"],
            generator.ARCHIVE_CAPTURE_URLS["2011-2012"],
        )
        self.assertEqual(
            {channel for year, channel in channels.items() if year not in {"2010-2011", "2011-2012"}},
            {"official_bulk"},
        )
        self.assertEqual(
            self.manifest["unavailable"],
            [
                {
                    "archive_url": None,
                    "fiscal_year": "2023-2024",
                    "reason": "official bulk export is unavailable; no preserved bytes",
                    "source_channel": "official_bulk",
                    "source_url": (
                        "https://consultacontratos.ocpr.gov.pr/contract/"
                        "downloadfrequentsearchfiscalyeardocument?q=2023-2024"
                    ),
                    "status": "unavailable",
                }
            ],
        )

    def test_independent_report_counts_reconcile_to_manifest(self):
        aggregate = self.manifest["aggregate"]
        self.assertEqual(aggregate["source_file_count"], 13)
        self.assertEqual(aggregate["rows_total"], 1_232_110)
        self.assertEqual(aggregate["rows_certified"], 1_231_603)
        self.assertEqual(aggregate["rows_quarantined"], 507)
        self.assertEqual(aggregate["quarantine_reason_counts"], {"shifted_row": 507})
        self.assertEqual(
            sum(report["rows_total"] for report in self.reports.values()),
            aggregate["rows_total"],
        )
        self.assertEqual(
            sum(report["rows_quarantined"] for report in self.reports.values()),
            aggregate["rows_quarantined"],
        )
        self.assertEqual(
            sum(len(report["quarantined_rows"]) for report in self.reports.values()),
            aggregate["rows_quarantined"],
        )
        for snapshot in self.manifest["snapshots"]:
            report = self.reports[snapshot["fiscal_year"]]
            with self.subTest(year=snapshot["fiscal_year"]):
                self.assertEqual(report["sha256"], snapshot["sha256"])
                self.assertEqual(report["report_hash"], snapshot["report_hash"])
                self.assertEqual(report["rows_total"], snapshot["rows_total"])
                self.assertEqual(
                    len(report["quarantined_rows"]), report["rows_quarantined"]
                )
                for row in report["quarantined_rows"]:
                    self.assertEqual(row["reason"], "shifted_row")
                    self.assertGreaterEqual(row["row_number"], 2)
                    self.assertEqual(len(row["raw_record_sha256"]), 64)
                    self.assertIsInstance(row["raw_fields"], list)

    def test_canonical_exclusions_are_all_traceable_and_separate(self):
        exclusions = self.manifest["canonical_exclusions"]
        aggregate = self.manifest["aggregate"]
        self.assertEqual(len(exclusions), 602)
        self.assertEqual(aggregate["canonical_exclusions"], 602)
        self.assertEqual(aggregate["canonical_rows"], 1_231_508)
        self.assertEqual(aggregate["rows_total"] - aggregate["canonical_rows"], 602)
        per_year = {year: 0 for year in self.reports}
        for exclusion in exclusions:
            per_year[exclusion["fiscal_year"]] += 1
        self.assertEqual(
            per_year,
            {
                "2010-2011": 23,
                "2011-2012": 1,
                "2012-2013": 4,
                "2013-2014": 0,
                "2014-2015": 1,
                "2015-2016": 9,
                "2016-2017": 7,
                "2017-2018": 20,
                "2018-2019": 23,
                "2019-2020": 36,
                "2020-2021": 149,
                "2021-2022": 258,
                "2022-2023": 71,
            },
        )
        for exclusion in exclusions:
            self.assertEqual(exclusion["reason"], "canonical_row_hash_duplicate")
            self.assertRegex(exclusion["row_hash"], r"^[0-9a-f]{16}$")
            self.assertGreaterEqual(exclusion["record_number"], 2)
            self.assertGreaterEqual(exclusion["first_seen"]["record_number"], 2)
            self.assertTrue(exclusion["source_file"].startswith("data/raw/"))
            self.assertTrue(exclusion["first_seen"]["source_file"].startswith("data/raw/"))

    def test_artifacts_contain_no_absolute_machine_paths_or_volatile_clock(self):
        forbidden = (b"/home/", b"/Users/", b"C:\\")
        for relative_path, content in self.generated.items():
            with self.subTest(path=relative_path):
                self.assertFalse(Path(relative_path).is_absolute())
                for marker in forbidden:
                    self.assertNotIn(marker, content)
                self.assertNotIn(b'"certified_at"', content)


if __name__ == "__main__":
    unittest.main()
