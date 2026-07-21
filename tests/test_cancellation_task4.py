import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = REPO_ROOT / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import build_site_artifacts  # noqa: E402
import bulk_observations  # noqa: E402
from contract_utils import (  # noqa: E402
    CONTRACT_INSERT_SQL,
    CANCELLATION_STATUSES,
    create_schema,
    normalize_contract_record,
    parse_bulk_field,
    parse_cancellation,
    records_equivalent,
)
from live_recovery import load_recovered_rows, write_recovered_rows  # noqa: E402


BULK_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "bulk" / "cancellation-values.csv"


class CancellationParsingTests(unittest.TestCase):
    def test_cancellation_status_is_a_closed_set(self):
        self.assertEqual(
            CANCELLATION_STATUSES,
            frozenset({"cancelled", "not_cancelled", "unknown", "malformed"}),
        )

    def test_blank_and_nul_are_unknown_and_retain_raw_value(self):
        for raw in ("", "   ", "\x00"):
            with self.subTest(raw=repr(raw)):
                parsed = parse_cancellation(raw)
                self.assertEqual(parsed.raw_value, raw)
                self.assertIsNone(parsed.date)
                self.assertEqual(parsed.status, "unknown")
                self.assertEqual(parsed.legacy_cancelled, 0)

    def test_bulk_date_is_effective_cancellation_date(self):
        parsed = parse_cancellation("09-30-2011")
        self.assertEqual(parsed.raw_value, "09-30-2011")
        self.assertEqual(parsed.date, "2011-09-30")
        self.assertEqual(parsed.status, "cancelled")
        self.assertEqual(parsed.legacy_cancelled, 1)

    def test_explicit_live_true_and_false_tokens_are_distinct(self):
        true_result = parse_cancellation("true")
        false_result = parse_cancellation("FALSE")
        self.assertEqual(
            (true_result.status, true_result.date, true_result.legacy_cancelled),
            ("cancelled", None, 1),
        )
        self.assertEqual(
            (false_result.status, false_result.date, false_result.legacy_cancelled),
            ("not_cancelled", None, 0),
        )

    def test_live_tokens_are_malformed_in_the_certified_bulk_profile(self):
        for raw in (
            "true",
            "FALSE",
            "unknown",
            "2011-09-30",
            "09/30/2011",
            "9-30-2011",
            "09-3-2011",
        ):
            with self.subTest(raw=raw):
                raw_field = parse_bulk_field("cancellation_raw", raw)
                date_field = parse_bulk_field("cancellation_date", raw)
                status_field = parse_bulk_field("cancellation_status", raw)
                legacy_field = parse_bulk_field("cancelled", raw)
                self.assertEqual(raw_field.raw_value, raw)
                self.assertEqual(raw_field.value, raw)
                self.assertEqual(raw_field.status, "malformed")
                self.assertIsNone(date_field.value)
                self.assertEqual(date_field.status, "malformed")
                self.assertEqual(status_field.value, "malformed")
                self.assertEqual(status_field.status, "malformed")
                self.assertEqual(legacy_field.value, 0)
                self.assertEqual(legacy_field.status, "malformed")

    def test_malformed_and_ambiguous_values_fail_closed_without_losing_raw(self):
        malformed = parse_cancellation("not-a-cancellation")
        ambiguous = parse_cancellation("05-06-14")
        self.assertEqual(malformed.status, "malformed")
        self.assertEqual(malformed.raw_value, "not-a-cancellation")
        self.assertIsNone(malformed.date)
        self.assertEqual(ambiguous.status, "unknown")
        self.assertEqual(ambiguous.raw_value, "05-06-14")
        self.assertIsNone(ambiguous.date)

    def test_live_date_shapes_preserve_raw_and_parse_effective_date(self):
        ajax = parse_cancellation("/Date(1317369600000)/")
        slash = parse_cancellation("09/30/2011")
        iso = parse_cancellation("2011-09-30")
        single_digit_month = parse_cancellation("9-30-2011")
        single_digit_day = parse_cancellation("09-3-2011")
        self.assertEqual(
            (ajax.raw_value, ajax.date, ajax.status),
            ("/Date(1317369600000)/", "2011-09-30", "cancelled"),
        )
        self.assertEqual((slash.date, slash.status), ("2011-09-30", "cancelled"))
        self.assertEqual((iso.date, iso.status), ("2011-09-30", "cancelled"))
        self.assertEqual(
            (single_digit_month.date, single_digit_month.status),
            ("2011-09-30", "cancelled"),
        )
        self.assertEqual(
            (single_digit_day.date, single_digit_day.status),
            ("2011-09-03", "cancelled"),
        )

    def test_normalized_record_derives_legacy_cancelled_from_validated_status(self):
        dated = normalize_contract_record(
            {"cancellation_raw": "09-30-2011", "cancelled": 0},
            inserted_at="2026-07-20T00:00:00+00:00",
        )
        blank = normalize_contract_record(
            {"cancellation_raw": "\x00", "cancelled": 1},
            inserted_at="2026-07-20T00:00:00+00:00",
        )
        explicit_false = normalize_contract_record(
            {"cancellation_raw": "false", "cancelled": 1},
            inserted_at="2026-07-20T00:00:00+00:00",
        )
        self.assertEqual(
            (dated["cancellation_raw"], dated["cancellation_date"], dated["cancellation_status"], dated["cancelled"]),
            ("09-30-2011", "2011-09-30", "cancelled", 1),
        )
        self.assertEqual(
            (blank["cancellation_raw"], blank["cancellation_date"], blank["cancellation_status"], blank["cancelled"]),
            ("\x00", None, "unknown", 0),
        )
        self.assertEqual(
            (explicit_false["cancellation_status"], explicit_false["cancelled"]),
            ("not_cancelled", 0),
        )


class CancellationSchemaAndPipelineTests(unittest.TestCase):
    def test_schema_migration_and_defaults_are_backward_compatible(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE contracts (id INTEGER PRIMARY KEY AUTOINCREMENT, cancelled INTEGER)"
        )
        conn.executemany("INSERT INTO contracts (cancelled) VALUES (?)", [(1,), (0,)])
        conn.commit()

        create_schema(conn)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(contracts)")}
        self.assertTrue(
            {"cancellation_raw", "cancellation_date", "cancellation_status"}.issubset(columns)
        )
        migrated = conn.execute(
            "SELECT cancelled, cancellation_raw, cancellation_date, cancellation_status "
            "FROM contracts ORDER BY id"
        ).fetchall()
        self.assertEqual(migrated[0], (1, None, None, "cancelled"))
        self.assertEqual(migrated[1], (0, None, None, "unknown"))

        conn.execute("INSERT INTO contracts (contract_number) VALUES ('new-row')")
        self.assertEqual(
            conn.execute(
                "SELECT cancelled, cancellation_raw, cancellation_date, cancellation_status "
                "FROM contracts WHERE contract_number = 'new-row'"
            ).fetchone(),
            (0, None, None, "unknown"),
        )
        conn.close()

    def test_schema_rejects_inconsistent_cancellation_projections(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        hostile_rows = (
            ("unknown", 1, None),
            ("not_cancelled", 1, None),
            ("malformed", 0, "2011-09-30"),
            ("forged", 0, None),
        )
        for status, legacy, date in hostile_rows:
            with self.subTest(status=status, legacy=legacy, date=date):
                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "invalid cancellation projection|CHECK constraint failed",
                ):
                    conn.execute(
                        "INSERT INTO contracts "
                        "(contract_number, cancelled, cancellation_date, cancellation_status) "
                        "VALUES (?, ?, ?, ?)",
                        (f"hostile-{status}", legacy, date, status),
                    )
        conn.close()

    def test_bulk_observation_and_canonical_projection_keep_cancellation_fields(self):
        batch = bulk_observations.generate_bulk_observations(
            BULK_FIXTURE,
            fiscal_year="2010-2011",
            source_channel="official_bulk",
        )
        first_typed = json.loads(batch[0]["parsed_values_json"])
        second_typed = json.loads(batch[1]["parsed_values_json"])
        self.assertEqual(
            (first_typed["cancellation_raw"], first_typed["cancellation_date"], first_typed["cancellation_status"], first_typed["cancelled"]),
            ("\x00", None, "unknown", 0),
        )
        self.assertEqual(
            (second_typed["cancellation_raw"], second_typed["cancellation_date"], second_typed["cancellation_status"], second_typed["cancelled"]),
            ("09-30-2011", "2011-09-30", "cancelled", 1),
        )

        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        bulk_observations.insert_bulk_observations(conn, batch)
        with conn:
            result = bulk_observations.project_bulk_observations(
                conn,
                batch,
                inserted_at="2026-07-20T00:00:00+00:00",
            )
        self.assertEqual(result.rows_new, 2)
        projected = conn.execute(
            "SELECT amendment, cancellation_raw, cancellation_date, cancellation_status, cancelled "
            "FROM contracts ORDER BY amendment"
        ).fetchall()
        self.assertEqual(projected[0], ("", "\x00", None, "unknown", 0))
        self.assertEqual(projected[1], ("A", "09-30-2011", "2011-09-30", "cancelled", 1))
        conn.close()

    def test_recovery_csv_round_trip_preserves_cancellation_compatibility(self):
        record = normalize_contract_record(
            {
                "contract_number": "2022-000019",
                "entity": "Entidad Demo",
                "contractor": "Contractor Demo",
                "cancelled": 0,
                "source_type": "live_recovery",
            },
            default_source_type="live_recovery",
            inserted_at="2026-07-20T00:00:00+00:00",
        )
        self.assertEqual(record["cancellation_status"], "not_cancelled")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "recovered.csv"
            write_recovered_rows(path, [record])
            reloaded = load_recovered_rows(path)[0]
        self.assertTrue(records_equivalent(record, reloaded))
        self.assertEqual(reloaded["cancellation_status"], "not_cancelled")

        unknown = normalize_contract_record(
            {
                "contract_number": "2022-000020",
                "cancellation_raw": None,
                "source_type": "live_recovery",
            },
            default_source_type="live_recovery",
            inserted_at="2026-07-20T00:00:00+00:00",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "unknown.csv"
            write_recovered_rows(path, [unknown])
            unknown_reloaded = load_recovered_rows(path)[0]
        self.assertTrue(records_equivalent(unknown, unknown_reloaded))
        self.assertIsNone(unknown_reloaded["cancellation_raw"])
        self.assertEqual(unknown_reloaded["cancellation_status"], "unknown")

    def test_browser_projection_contains_and_copies_cancellation_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source_db = Path(tmpdir) / "source.db"
            browser_db = Path(tmpdir) / "browser.db"
            conn = sqlite3.connect(source_db)
            create_schema(conn)
            record = normalize_contract_record(
                {
                    "contract_number": "1995-000444",
                    "entity": "Entidad Demo",
                    "contractor": "Contractor Demo",
                    "cancellation_raw": "09-30-2011",
                    "inserted_at": "2026-07-20T00:00:00+00:00",
                },
                inserted_at="2026-07-20T00:00:00+00:00",
            )
            conn.execute(CONTRACT_INSERT_SQL, record)
            conn.commit()
            conn.close()

            build_site_artifacts.build_browser_db(source_db, browser_db)
            browser = sqlite3.connect(browser_db)
            columns = {row[1] for row in browser.execute("PRAGMA table_info(contracts)")}
            self.assertTrue(
                {"cancellation_raw", "cancellation_date", "cancellation_status"}.issubset(columns)
            )
            self.assertEqual(
                browser.execute(
                    "SELECT cancellation_raw, cancellation_date, cancellation_status, cancelled "
                    "FROM contracts"
                ).fetchone(),
                ("09-30-2011", "2011-09-30", "cancelled", 1),
            )
            browser.close()

    def test_browser_schema_rejects_inconsistent_cancellation_projections(self):
        browser = sqlite3.connect(":memory:")
        build_site_artifacts.create_browser_schema(browser)
        base = {
            "id": 1,
            "canonical_id": "canonical:v1:" + ("a" * 64),
            "family_id": "family:v1:" + ("b" * 64),
            "canonical_identity_version": "canonical-record-v1",
            "family_identity_version": "contract-family-v1",
        }
        with self.assertRaises(sqlite3.IntegrityError):
            browser.execute(
                """
                INSERT INTO contracts (
                    id, cancelled, cancellation_status, canonical_id, family_id,
                    canonical_identity_version, family_identity_version
                ) VALUES (:id, 0, 'cancelled', :canonical_id, :family_id,
                          :canonical_identity_version, :family_identity_version)
                """,
                base,
            )
        with self.assertRaises(sqlite3.IntegrityError):
            browser.execute(
                """
                INSERT INTO contracts (
                    id, cancelled, cancellation_status, canonical_id, family_id,
                    canonical_identity_version, family_identity_version
                ) VALUES (:id, 0, 'invented', :canonical_id, :family_id,
                          :canonical_identity_version, :family_identity_version)
                """,
                base,
            )
        with self.assertRaises(sqlite3.IntegrityError):
            browser.execute(
                """
                INSERT INTO contracts (
                    id, cancelled, cancellation_date, cancellation_status,
                    canonical_id, family_id, canonical_identity_version,
                    family_identity_version
                ) VALUES (:id, 0, '2011-09-30', 'unknown',
                          :canonical_id, :family_id,
                          :canonical_identity_version, :family_identity_version)
                """,
                base,
            )
        browser.close()


class CancellationDetailRenderingContractTests(unittest.TestCase):
    def test_detail_page_and_locales_name_source_status_and_effective_date(self):
        html = (REPO_ROOT / "site" / "contract.html").read_text(encoding="utf-8")
        javascript = (REPO_ROOT / "site" / "js" / "contract.js").read_text(encoding="utf-8")
        i18n = (REPO_ROOT / "site" / "js" / "i18n.js").read_text(encoding="utf-8")

        self.assertIn('data-i18n="detail.effectiveCancellationDate"', html)
        self.assertIn('id="d-cancellation-date"', html)
        self.assertIn('data-i18n="detail.sourceCancellationStatus"', html)
        self.assertIn('id="d-cancellation-status"', html)
        self.assertIn('data-i18n="detail.cancellationRaw"', html)
        self.assertIn('id="d-cancellation-raw"', html)
        self.assertIn("c.cancellation_status", javascript)
        self.assertIn('d-cancellation-date', javascript)
        self.assertIn('d-cancellation-status', javascript)
        self.assertIn('detail.effectiveCancellationDate', i18n)
        self.assertIn('detail.sourceCancellationStatus', i18n)
        self.assertIn('Effective cancellation date', i18n)
        self.assertIn('Estado de cancelación de la fuente', i18n)
        self.assertIn('detail.cancellationStatus.cancelled', i18n)
        self.assertIn('detail.cancellationStatus.not_cancelled', i18n)
        self.assertIn('detail.cancellationStatus.unknown', i18n)
        self.assertIn('detail.cancellationStatus.malformed', i18n)


if __name__ == "__main__":
    unittest.main()
