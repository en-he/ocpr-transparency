import csv
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import bulk_observations  # noqa: E402
import certify_bulk  # noqa: E402
from contract_utils import (  # noqa: E402
    CONTRACT_INSERT_SQL,
    RAW_SOURCE_TYPE,
    create_schema,
    normalize_contract_record,
    parse_bulk_field,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "bulk"
ENCODING = "latin-1"


class BulkObservationGenerationTests(unittest.TestCase):
    def test_reordered_known_header_profile_uses_actual_source_column_order(self):
        original = FIXTURES / "ocpr-bulk-v1.csv"
        with open(original, encoding=ENCODING, newline="") as handle:
            rows = list(csv.reader(handle))
        order = [1, 0, *range(2, len(rows[0]))]
        with tempfile.TemporaryDirectory() as tmpdir:
            reordered = Path(tmpdir) / "reordered-v1.csv"
            with open(reordered, "w", encoding=ENCODING, newline="") as handle:
                writer = csv.writer(handle, lineterminator="\r\n")
                writer.writerows([[row[index] for index in order] for row in rows])
            expected = bulk_observations.generate_bulk_observations(
                original,
                fiscal_year="2010-2011",
                source_channel="archive_bulk",
            )
            actual = bulk_observations.generate_bulk_observations(
                reordered,
                fiscal_year="2010-2011",
                source_channel="archive_bulk",
            )

        self.assertEqual(actual.headers, tuple(rows[0][index] for index in order))
        self.assertEqual(
            json.loads(actual[0]["parsed_values_json"]),
            json.loads(expected[0]["parsed_values_json"]),
        )

    def test_observation_id_has_a_stable_known_vector(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "ocpr-bulk-v1.csv",
            fiscal_year="2010-2011",
            source_channel="archive_bulk",
        )
        self.assertEqual(
            batch[0]["observation_id"],
            "sha256:1ad59f9fdf1c5af7fb1343086e18fd71e0308a6072beeaddb9bce3d83cecd0dd",
        )

    def test_v3_generation_preserves_identity_raw_json_and_typed_statuses(self):
        source = FIXTURES / "ocpr-bulk-v3.csv"
        batch = bulk_observations.generate_bulk_observations(
            source,
            fiscal_year="2013-2014",
            source_channel="official_bulk",
        )

        self.assertEqual(len(batch), 2)
        self.assertEqual(batch.evidence.file_sha256, hashlib.sha256(source.read_bytes()).hexdigest())
        self.assertEqual(batch.evidence.evidence_id, f"sha256:{batch.evidence.file_sha256}")

        row = batch[0]
        certified = certify_bulk.certify_bulk_file(
            source,
            source_channel="official_bulk",
            fiscal_year="2013-2014",
        )
        raw_fields = list(certified.row_outcomes[0].raw_fields)
        self.assertEqual(json.loads(row["raw_values_json"]), raw_fields)
        self.assertEqual(
            row["raw_row_hash"],
            hashlib.sha256(
                source.read_text(encoding=ENCODING).splitlines()[1].encode(ENCODING)
            ).hexdigest(),
        )
        self.assertEqual(row["source_row_number"], 2)
        self.assertEqual(row["parser_profile"], "v3")
        self.assertEqual(row["parser_version"], certify_bulk.PARSER_VERSION)
        self.assertEqual(row["normalizer_version"], certify_bulk.NORMALIZER_VERSION)
        self.assertEqual(row["observation_status"], "certified")
        self.assertEqual(row["canonical_eligible"], 1)

        typed = json.loads(row["parsed_values_json"])
        statuses = json.loads(row["field_status_json"])
        self.assertEqual(typed["amount"], 600.0)
        self.assertEqual(typed["award_date"], "2014-02-15")
        self.assertEqual(statuses["amount"], "valid")
        self.assertEqual(statuses["award_date"], "valid")
        self.assertEqual(statuses["amendment"], "missing")
        self.assertEqual(statuses["procurement_method"], "missing")
        self.assertEqual(json.loads(row["warnings_json"]), [])
        self.assertEqual(json.loads(row["raw_coordinates_json"]), [])

    def test_ambiguous_date_is_quarantined_with_raw_value_and_exclusion(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "ocpr-bulk-v3.csv",
            fiscal_year="2013-2014",
            source_channel="official_bulk",
        )
        row = batch[1]
        statuses = json.loads(row["field_status_json"])
        raw_values = json.loads(row["raw_values_json"])
        headers = list(certify_bulk.HEADER_PROFILES["v3"])

        self.assertEqual(row["source_row_number"], 3)
        self.assertEqual(raw_values[headers.index("Otorgado en")], "05-06-14")
        self.assertEqual(statuses["award_date"], "ambiguous")
        self.assertEqual(row["observation_status"], "quarantined")
        self.assertEqual(row["canonical_eligible"], 0)
        self.assertEqual(row["canonical_exclusion_reason"], "parser_ambiguous_date")
        self.assertIn("ambiguous_date", json.loads(row["warnings_json"]))

    def test_shifted_rows_keep_source_coordinates_and_are_never_projected(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "malformed-shifted-row.csv",
            fiscal_year="2014-2015",
            source_channel="official_bulk",
        )

        self.assertEqual(len(batch), 3)
        self.assertEqual(
            [row["observation_status"] for row in batch],
            ["certified", "quarantined", "quarantined"],
        )
        self.assertEqual([row["source_row_number"] for row in batch], [2, 3, 4])
        self.assertEqual(len(json.loads(batch[1]["raw_values_json"])), 14)
        self.assertEqual(len(json.loads(batch[2]["raw_values_json"])), 12)
        coordinates = json.loads(batch[1]["raw_coordinates_json"])
        self.assertEqual(coordinates[12]["raw_value"], "CONTRATISTA EJEMPLO SEIS")
        self.assertEqual(coordinates[13]["column_index"], 13)
        self.assertEqual(coordinates[13]["header"], None)
        self.assertEqual(batch[1]["canonical_exclusion_reason"], "parser_shifted_row")
        self.assertEqual(batch[2]["canonical_exclusion_reason"], "parser_shifted_row")
        self.assertEqual(sum(row["canonical_eligible"] for row in batch), 1)

    def test_quarantined_repeated_records_do_not_inflate_certified_duplicate_count(self):
        records = (FIXTURES / "malformed-shifted-row.csv").read_bytes().splitlines()
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "repeated-shifted.csv"
            source.write_bytes(
                b"\r\n".join((records[0], records[2], records[2], b""))
            )
            batch = bulk_observations.generate_bulk_observations(
                source,
                fiscal_year="2014-2015",
                source_channel="official_bulk",
            )
        self.assertEqual(batch.report.duplicate_count, 0)
        self.assertEqual(
            [row["duplicate_status"] for row in batch],
            ["unique", "unique"],
        )
        self.assertEqual(
            [row["observation_status"] for row in batch],
            ["quarantined", "quarantined"],
        )

    def test_strict_known_profiles_reject_duplicate_missing_and_unknown_headers(self):
        cases = (
            ("duplicate-header.csv", bulk_observations.DuplicateHeaderError),
            ("missing-required-header.csv", bulk_observations.MissingRequiredHeaderError),
            ("unknown-header.csv", bulk_observations.UnknownHeaderProfileError),
        )
        for filename, error_type in cases:
            with self.subTest(filename=filename):
                with self.assertRaises(error_type):
                    bulk_observations.generate_bulk_observations(
                        FIXTURES / filename,
                        fiscal_year="2010-2011",
                        source_channel="official_bulk",
                    )

    def test_profile_date_parser_uses_mm_dd_yyyy_without_trying_both_orders(self):
        parsed = parse_bulk_field("award_date", "05-06-2014", profile="v3")
        self.assertEqual(parsed.value, "2014-05-06")
        self.assertEqual(parsed.status, "valid")

        ambiguous = parse_bulk_field("award_date", "05-06-14", profile="v3")
        self.assertIsNone(ambiguous.value)
        self.assertEqual(ambiguous.status, "ambiguous")


class BulkObservationPersistenceTests(unittest.TestCase):
    def _duplicate_batch(self, tmp: Path):
        records = (FIXTURES / "ocpr-bulk-v1.csv").read_bytes().splitlines()
        source = tmp / "duplicate-v1.csv"
        source.write_bytes(b"\r\n".join((records[0], records[1], records[1], b"")))
        return bulk_observations.generate_bulk_observations(
            source,
            fiscal_year="2010-2011",
            source_channel="archive_bulk",
        )

    def test_schema_enforces_foreign_keys_checks_and_restricts_evidence_delete(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        self.assertEqual(conn.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        self.assertEqual(conn.execute("PRAGMA recursive_triggers").fetchone()[0], 1)
        valid_evidence = (
            "sha256:" + ("1" * 64),
            "official_bulk", "2098-2099", None, None, None, "unknown",
            "2" * 64, 1, "latin-1", "text/csv", "v1", "3" * 64,
            "certified", "{}",
        )
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO evidence_objects (
                    evidence_id, source_channel, fiscal_year, source_url, archive_url,
                    captured_at, capture_time_status, sha256, byte_length, encoding,
                    media_type, header_profile, header_fingerprint, status, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                valid_evidence,
            )
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO evidence_objects (
                    evidence_id, source_channel, fiscal_year, source_url, archive_url,
                    captured_at, capture_time_status, sha256, byte_length, encoding,
                    media_type, header_profile, header_fingerprint, status, metadata_json
                ) VALUES (
                    'sha256:bad', 'unknown', '2010-2011', NULL, NULL,
                    NULL, 'unknown', 'bad', -1, 'latin-1', NULL, 'v1', 'bad',
                    'certified', '{}'
                )
                """
            )

        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "ocpr-bulk-v1.csv",
            fiscal_year="2010-2011",
            source_channel="archive_bulk",
        )
        bulk_observations.insert_bulk_observations(conn, batch)
        evidence = batch.evidence
        replacement = (
            evidence.evidence_id, evidence.source_channel, evidence.fiscal_year,
            "https://hostile.test", evidence.archive_url, evidence.captured_at,
            evidence.capture_time_status, evidence.file_sha256,
            evidence.byte_length, evidence.encoding, evidence.media_type,
            evidence.header_profile, evidence.header_fingerprint,
            evidence.status, evidence.metadata_json,
        )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "evidence objects are append-only",
        ):
            conn.execute(
                """
                INSERT OR REPLACE INTO evidence_objects (
                    evidence_id, source_channel, fiscal_year, source_url, archive_url,
                    captured_at, capture_time_status, sha256, byte_length, encoding,
                    media_type, header_profile, header_fingerprint, status, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                replacement,
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "evidence objects are append-only"):
            conn.execute(
                "UPDATE evidence_objects SET source_url = 'https://hostile.test' WHERE evidence_id = ?",
                (batch.evidence.evidence_id,),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "bulk observations are append-only"):
            conn.execute(
                "UPDATE bulk_observations SET raw_record = 'mutated' WHERE observation_id = ?",
                (batch[0]["observation_id"],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "bulk observations are append-only"):
            conn.execute(
                "DELETE FROM bulk_observations WHERE observation_id = ?",
                (batch[0]["observation_id"],),
            )
        observation = batch[0]
        columns = bulk_observations._OBSERVATION_COLUMNS
        replacement_observation = dict(observation)
        replacement_observation["raw_record"] = "mutated by replace"
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "bulk observations are append-only|invalid bulk observation identity",
        ):
            conn.execute(
                f"INSERT OR REPLACE INTO bulk_observations ({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})",
                tuple(replacement_observation[column] for column in columns),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bulk_projection_exclusions (
                    exclusion_id, observation_id, evidence_id,
                    source_row_number, reason, details_json
                ) VALUES (?, ?, ?, ?, 'hostile_coordinate_mismatch', '{}')
                """,
                (
                    "sha256:" + ("a" * 64),
                    batch[0]["observation_id"],
                    batch.evidence.evidence_id,
                    batch[0]["source_row_number"] + 999,
                ),
            )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "invalid projection exclusion semantics",
        ):
            conn.execute(
                """
                INSERT INTO bulk_projection_exclusions (
                    exclusion_id, observation_id, evidence_id,
                    source_row_number, reason, details_json
                ) VALUES (?, ?, ?, ?, 'forged_eligible_exclusion', '{}')
                """,
                (
                    "sha256:" + ("b" * 64),
                    batch[0]["observation_id"],
                    batch.evidence.evidence_id,
                    batch[0]["source_row_number"],
                ),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO bulk_projection_results (
                    observation_id, row_hash, contract_id,
                    projection_status, reason
                ) VALUES (?, ?, NULL, 'excluded', 'forged_hash')
                """,
                (batch[0]["observation_id"], "f" * 64),
            )
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "invalid projection result semantics",
        ):
            conn.execute(
                """
                INSERT INTO bulk_projection_results (
                    observation_id, row_hash, contract_id,
                    projection_status, reason
                ) VALUES (?, NULL, NULL, 'excluded', 'forged_reason')
                """,
                (batch[0]["observation_id"],),
            )

        quarantined = bulk_observations.generate_bulk_observations(
            FIXTURES / "malformed-shifted-row.csv",
            fiscal_year="2014-2015",
            source_channel="official_bulk",
        )[1]
        forged = dict(quarantined)
        forged["observation_id"] = "sha256:" + ("c" * 64)
        forged["evidence_id"] = batch.evidence.evidence_id
        forged["source_row_number"] = 999
        forged["canonical_eligible"] = 1
        forged["canonical_exclusion_reason"] = None
        columns = bulk_observations._OBSERVATION_COLUMNS
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "only certified observations may be canonical eligible|invalid bulk observation identity",
        ):
            conn.execute(
                f"INSERT INTO bulk_observations ({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})",
                tuple(forged[column] for column in columns),
            )
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                "DELETE FROM evidence_objects WHERE evidence_id = ?",
                (batch.evidence.evidence_id,),
            )
        conn.close()

    def test_schema_recomputes_observation_identity_and_duplicate_lineage(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "ocpr-bulk-v1.csv",
            fiscal_year="2010-2011",
            source_channel="archive_bulk",
        )
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        bulk_observations.insert_bulk_observations(conn, batch)
        columns = bulk_observations._OBSERVATION_COLUMNS

        forged_identity = dict(batch[0])
        forged_identity["observation_id"] = "sha256:" + ("f" * 64)
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "invalid bulk observation identity",
        ):
            conn.execute(
                f"INSERT INTO bulk_observations ({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})",
                tuple(forged_identity[column] for column in columns),
            )

        forged_duplicate = dict(batch[0])
        forged_duplicate["source_row_number"] = 999
        forged_duplicate["raw_record"] = "forged non-duplicate record"
        forged_duplicate["raw_row_hash"] = hashlib.sha256(
            forged_duplicate["raw_record"].encode(ENCODING)
        ).hexdigest()
        forged_duplicate["observation_id"] = bulk_observations.observation_id(
            evidence_id=forged_duplicate["evidence_id"],
            source_row_number=forged_duplicate["source_row_number"],
            raw_row_hash=forged_duplicate["raw_row_hash"],
            parser_version=forged_duplicate["parser_version"],
            normalizer_version=forged_duplicate["normalizer_version"],
        )
        forged_duplicate["duplicate_status"] = "exact_duplicate"
        forged_duplicate["duplicate_of_observation_id"] = batch[0]["observation_id"]
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "invalid bulk observation identity",
        ):
            conn.execute(
                f"INSERT INTO bulk_observations ({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})",
                tuple(forged_duplicate[column] for column in columns),
            )
        conn.close()

    def test_duplicate_exclusion_requires_projection_and_contributor_edges(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "ocpr-bulk-v1.csv",
            fiscal_year="2010-2011",
            source_channel="archive_bulk",
        )
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        bulk_observations.insert_bulk_observations(conn, batch)
        observation = batch[0]
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "invalid projection exclusion semantics",
        ):
            conn.execute(
                """
                INSERT INTO bulk_projection_exclusions (
                    exclusion_id, observation_id, evidence_id,
                    source_row_number, reason, details_json
                ) VALUES (?, ?, ?, ?, 'canonical_row_hash_duplicate', '{}')
                """,
                (
                    "sha256:" + ("d" * 64),
                    observation["observation_id"],
                    observation["evidence_id"],
                    observation["source_row_number"],
                ),
            )
        conn.close()

    def test_exact_duplicate_rows_remain_distinct_observations_and_project_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            batch = self._duplicate_batch(Path(tmpdir))
            self.assertEqual(len(batch), 2)
            self.assertEqual(
                [row["duplicate_status"] for row in batch],
                ["unique", "exact_duplicate"],
            )
            self.assertNotEqual(batch[0]["observation_id"], batch[1]["observation_id"])

            conn = sqlite3.connect(":memory:")
            create_schema(conn)
            bulk_observations.insert_bulk_observations(conn, batch)
            projected = bulk_observations.project_bulk_observations(
                conn,
                batch,
                inserted_at="1970-01-01T00:00:00+00:00",
            )
            self.assertEqual(projected.rows_new, 1)
            self.assertEqual(projected.rows_duplicate, 1)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM bulk_observations").fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0],
                1,
            )
            representative = min(batch, key=lambda row: row["observation_id"])
            duplicate = max(batch, key=lambda row: row["observation_id"])
            contract = conn.execute(
                """
                SELECT id, row_hash, representative_observation_id,
                       canonicalization_status, normalizer_version
                FROM contracts
                """
            ).fetchone()
            self.assertEqual(contract[2], representative["observation_id"])
            self.assertEqual(contract[3], "selected_observation")
            self.assertEqual(contract[4], representative["normalizer_version"])
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "projection results are append-only",
            ):
                conn.execute(
                    "DELETE FROM bulk_projection_results WHERE observation_id = ?",
                    (batch[0]["observation_id"],),
                )
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "projection exclusions are append-only",
            ):
                conn.execute(
                    "DELETE FROM bulk_projection_exclusions WHERE observation_id = ?",
                    (duplicate["observation_id"],),
                )
            projection_result = conn.execute(
                """
                SELECT observation_id, row_hash, contract_id,
                       projection_status, reason
                FROM bulk_projection_results
                WHERE observation_id = ?
                """,
                (batch[0]["observation_id"],),
            ).fetchone()
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "projection results are append-only",
            ):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO bulk_projection_results (
                        observation_id, row_hash, contract_id,
                        projection_status, reason
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    projection_result,
                )
            projection_exclusion = conn.execute(
                """
                SELECT exclusion_id, observation_id, evidence_id,
                       source_row_number, reason, details_json
                FROM bulk_projection_exclusions
                WHERE observation_id = ?
                """,
                (duplicate["observation_id"],),
            ).fetchone()
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "projection exclusions are append-only",
            ):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO bulk_projection_exclusions (
                        exclusion_id, observation_id, evidence_id,
                        source_row_number, reason, details_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    projection_exclusion,
                )
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "canonical contract/projection mismatch",
            ):
                conn.execute(
                    "UPDATE contracts SET row_hash = ? WHERE id = ?",
                    ("f" * 64, contract[0]),
                )
            with self.assertRaisesRegex(
                sqlite3.IntegrityError,
                "canonical contract/projection mismatch",
            ):
                conn.execute(
                    """
                    UPDATE contracts
                    SET representative_observation_id = ?
                    WHERE id = ?
                    """,
                    (duplicate["observation_id"], contract[0]),
                )
            exclusion_count = conn.execute(
                "SELECT COUNT(*) FROM bulk_projection_exclusions"
            ).fetchone()[0]
            repeated = bulk_observations.project_bulk_observations(
                conn,
                batch,
                inserted_at="2099-01-01T00:00:00+00:00",
            )
            self.assertEqual(repeated.rows_existing, 2)
            self.assertEqual(repeated.rows_new, 0)
            self.assertEqual(repeated.rows_duplicate, 0)
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM bulk_projection_exclusions"
                ).fetchone()[0],
                exclusion_count,
            )
            conn.close()

    def test_selected_projection_rejects_legacy_unlinked_contract(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "ocpr-bulk-v1.csv",
            fiscal_year="2010-2011",
            source_channel="archive_bulk",
        )
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        bulk_observations.insert_bulk_observations(conn, batch)
        parsed = json.loads(batch[0]["parsed_values_json"])
        parsed.update(
            {
                "source_type": RAW_SOURCE_TYPE,
                "representative_observation_id": batch[0]["observation_id"],
                "canonicalization_status": "legacy_unlinked",
                "normalizer_version": batch[0]["normalizer_version"],
            }
        )
        normalized = normalize_contract_record(
            parsed,
            default_source_type=RAW_SOURCE_TYPE,
            inserted_at="1970-01-01T00:00:00+00:00",
        )
        contract_id = conn.execute(CONTRACT_INSERT_SQL, normalized).lastrowid
        with self.assertRaisesRegex(
            sqlite3.IntegrityError,
            "selected projection lineage mismatch",
        ):
            conn.execute(
                """
                INSERT INTO bulk_projection_results (
                    observation_id, row_hash, contract_id,
                    projection_status, reason
                ) VALUES (?, ?, ?, 'selected', NULL)
                """,
                (batch[0]["observation_id"], normalized["row_hash"], contract_id),
            )
        conn.close()

    def test_parser_version_change_appends_observations_for_same_evidence(self):
        first = bulk_observations.generate_bulk_observations(
            FIXTURES / "ocpr-bulk-v1.csv",
            fiscal_year="2010-2011",
            source_channel="archive_bulk",
        )
        second = replace(
            first,
            evidence=replace(first.evidence, parser_version="bulk-certify-parser-2.0.0"),
        )
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        bulk_observations.insert_bulk_observations(conn, first)
        result = bulk_observations.insert_bulk_observations(conn, second)
        self.assertEqual(result.evidence_inserted, 0)
        self.assertEqual(result.observations_inserted, 2)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM evidence_objects").fetchone()[0],
            1,
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM bulk_observations").fetchone()[0],
            4,
        )
        conn.close()

    def test_failed_batch_rolls_back_evidence_and_prior_observations(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "ocpr-bulk-v1.csv",
            fiscal_year="2010-2011",
            source_channel="archive_bulk",
        )
        rows = list(batch)
        broken = dict(rows[1])
        broken["canonical_eligible"] = 1
        broken["canonical_exclusion_reason"] = "forbidden_with_eligible"

        class BrokenBatch:
            evidence = batch.evidence

            def __iter__(self):
                return iter((rows[0], broken))

        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        with self.assertRaises(sqlite3.IntegrityError):
            bulk_observations.insert_bulk_observations(conn, BrokenBatch())
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM evidence_objects").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM bulk_observations").fetchone()[0], 0)
        conn.close()

    def test_generation_and_insertion_are_idempotent_and_append_only(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "ocpr-bulk-v1.csv",
            fiscal_year="2010-2011",
            source_channel="archive_bulk",
        )
        conn = sqlite3.connect(":memory:")
        create_schema(conn)

        first = bulk_observations.insert_bulk_observations(conn, batch)
        second = bulk_observations.insert_bulk_observations(conn, batch)

        self.assertEqual(first.observations_inserted, 2)
        self.assertEqual(second.observations_inserted, 0)
        self.assertEqual(second.observations_existing, 2)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM evidence_objects").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM bulk_observations").fetchone()[0], 2)
        self.assertEqual(
            conn.execute(
                "SELECT COUNT(*) FROM bulk_projection_exclusions"
            ).fetchone()[0],
            0,
        )

        before = conn.execute(
            "SELECT observation_id, raw_values_json FROM bulk_observations ORDER BY source_row_number"
        ).fetchall()
        bulk_observations.insert_bulk_observations(conn, batch)
        after = conn.execute(
            "SELECT observation_id, raw_values_json FROM bulk_observations ORDER BY source_row_number"
        ).fetchall()
        self.assertEqual(before, after)
        conn.close()

    def test_quarantined_observations_record_projection_exclusions(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURES / "malformed-shifted-row.csv",
            fiscal_year="2014-2015",
            source_channel="official_bulk",
        )
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        result = bulk_observations.insert_bulk_observations(conn, batch)

        self.assertEqual(result.exclusions_inserted, 2)
        rows = conn.execute(
            "SELECT source_row_number, reason FROM bulk_projection_exclusions ORDER BY source_row_number"
        ).fetchall()
        self.assertEqual(rows, [(3, "parser_shifted_row"), (4, "parser_shifted_row")])
        conn.close()


if __name__ == "__main__":
    unittest.main()
