import csv
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
import contract_utils  # noqa: E402
import ingest  # noqa: E402


FIXTURE = REPO_ROOT / "tests" / "fixtures" / "bulk" / "ocpr-bulk-v3.csv"


def write_source(path: Path, newline: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with FIXTURE.open(encoding="latin-1", newline="") as handle:
        header, first_row, *_ = csv.reader(handle)
    with path.open("w", encoding="latin-1", newline="") as handle:
        writer = csv.writer(handle, lineterminator=newline)
        writer.writerows([header, first_row])


class CanonicalIdentityTests(unittest.TestCase):
    def test_canonical_identity_is_full_versioned_and_includes_entity_number(self):
        base = {
            "contract_number": "2021-000001",
            "entity": "Entidad Demo",
            "contractor": "INTEGRA",
            "amount": "10000.00",
            "award_date": "08-01-2021",
        }
        first = contract_utils.normalize_contract_record(
            {**base, "entity_number": "9000"},
            inserted_at="1970-01-01T00:00:00+00:00",
        )
        repeated = contract_utils.normalize_contract_record(
            {**base, "entity_number": "9000"},
            inserted_at="different-clock-value",
        )
        second = contract_utils.normalize_contract_record(
            {**base, "entity_number": "9001"},
            inserted_at="1970-01-01T00:00:00+00:00",
        )
        self.assertRegex(first["canonical_id"], r"^canonical:v1:[0-9a-f]{64}$")
        self.assertEqual(first["canonical_id"], repeated["canonical_id"])
        self.assertNotEqual(first["canonical_id"], second["canonical_id"])
        self.assertEqual(len(first["row_hash"]), 64)
        self.assertEqual(first["canonical_identity_version"], "canonical-record-v1")

    def test_family_identity_is_stable_across_original_and_amendment_rows(self):
        base = {
            "contract_number": "2021-000001",
            "entity": "Entidad Demo",
            "entity_number": "9000",
            "contractor": "INTEGRA",
        }
        original = contract_utils.normalize_contract_record(
            {**base, "amendment": ""}, inserted_at="clock-a"
        )
        amendment = contract_utils.normalize_contract_record(
            {**base, "amendment": "1", "amount": "200"}, inserted_at="clock-b"
        )
        self.assertRegex(original["family_id"], r"^family:v1:[0-9a-f]{64}$")
        self.assertEqual(original["family_id"], amendment["family_id"])
        self.assertNotEqual(original["canonical_id"], amendment["canonical_id"])
        self.assertEqual(original["family_identity_version"], "contract-family-v1")


class CanonicalLineageTests(unittest.TestCase):
    def test_schema_enforces_direct_representative_and_append_only_contributors(self):
        connection = sqlite3.connect(":memory:")
        contract_utils.create_schema(connection)
        contract_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(contracts)")
        }
        self.assertTrue(
            {
                "canonical_id",
                "family_id",
                "canonical_identity_version",
                "family_identity_version",
            }
            <= contract_columns
        )
        contributor_columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info(canonical_observation_contributors)"
            )
        }
        self.assertEqual(
            contributor_columns,
            {
                "canonical_id",
                "family_id",
                "observation_id",
                "representative_observation_id",
                "contribution_role",
                "merge_reason",
                "decision_version",
            },
        )

    def test_contributor_lineage_is_append_only_and_rejects_forgery(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURE,
            fiscal_year="2013-2014",
            source_channel="official_bulk",
        )
        connection = sqlite3.connect(":memory:")
        contract_utils.create_schema(connection)
        bulk_observations.insert_bulk_observations(connection, batch)
        bulk_observations.project_bulk_observations(
            connection,
            batch,
            inserted_at="1970-01-01T00:00:00+00:00",
        )
        row = connection.execute(
            "SELECT * FROM canonical_observation_contributors LIMIT 1"
        ).fetchone()
        self.assertIsNotNone(row)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            connection.execute(
                "DELETE FROM canonical_observation_contributors "
                "WHERE observation_id = ?",
                (row[2],),
            )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            connection.execute(
                "UPDATE canonical_observation_contributors "
                "SET merge_reason = 'canonical_record_duplicate' "
                "WHERE observation_id = ?",
                (row[2],),
            )
        forged = list(row)
        forged[0] = "canonical:v1:" + ("f" * 64)
        with self.assertRaises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO canonical_observation_contributors (
                    canonical_id, family_id, observation_id,
                    representative_observation_id, contribution_role,
                    merge_reason, decision_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                forged,
            )
        connection.executescript(
            """
            DROP TRIGGER validate_canonical_contributor_insert;
            CREATE TRIGGER validate_canonical_contributor_insert
            BEFORE INSERT ON canonical_observation_contributors
            WHEN 0
            BEGIN
                SELECT RAISE(ABORT, 'obsolete permissive trigger');
            END;
            """
        )
        contract_utils.create_schema(connection)
        trigger_sql = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'trigger'
              AND name = 'validate_canonical_contributor_insert'
            """
        ).fetchone()[0]
        self.assertIn("canonical_id_from_parsed_values", trigger_sql)

        with tempfile.TemporaryDirectory() as directory:
            hostile_source = Path(directory) / "contratos_2021-2022.csv"
            write_source(hostile_source, "\n")
            with hostile_source.open(encoding="latin-1", newline="") as handle:
                hostile_rows = list(csv.reader(handle))
            hostile_rows[1][0] = "9999"
            with hostile_source.open("w", encoding="latin-1", newline="") as handle:
                csv.writer(handle, lineterminator="\n").writerows(hostile_rows)
            hostile_batch = bulk_observations.generate_bulk_observations(
                hostile_source,
                fiscal_year="2021-2022",
                source_channel="official_bulk",
            )
            bulk_observations.insert_bulk_observations(connection, hostile_batch)
            hostile_observation_id = hostile_batch[0]["observation_id"]
            contract = connection.execute(
                """
                SELECT canonical_id, family_id, representative_observation_id
                FROM contracts LIMIT 1
                """
            ).fetchone()
            with self.assertRaisesRegex(
                sqlite3.IntegrityError, "invalid canonical contributor lineage"
            ):
                connection.execute(
                    """
                    INSERT INTO canonical_observation_contributors (
                        canonical_id, family_id, observation_id,
                        representative_observation_id, contribution_role,
                        merge_reason, decision_version
                    ) VALUES (?, ?, ?, ?, 'duplicate',
                              'canonical_record_duplicate', 'canonical-decision-v1')
                    """,
                    (*contract[:2], hostile_observation_id, contract[2]),
                )
        connection.close()

    def test_projection_result_requires_contributor_relation(self):
        batch = bulk_observations.generate_bulk_observations(
            FIXTURE,
            fiscal_year="2013-2014",
            source_channel="official_bulk",
        )
        observation = batch[0]
        connection = sqlite3.connect(":memory:")
        contract_utils.create_schema(connection)
        bulk_observations.insert_bulk_observations(connection, batch)
        parsed = json.loads(observation["parsed_values_json"])
        parsed.update(
            {
                "source_type": contract_utils.RAW_SOURCE_TYPE,
                "representative_observation_id": observation["observation_id"],
                "canonicalization_status": "selected_observation",
                "normalizer_version": observation["normalizer_version"],
            }
        )
        normalized = contract_utils.normalize_contract_record(
            parsed,
            inserted_at="1970-01-01T00:00:00+00:00",
        )
        contract_id = connection.execute(
            contract_utils.CONTRACT_INSERT_SQL, normalized
        ).lastrowid
        with self.assertRaisesRegex(
            sqlite3.IntegrityError, "projection contributor lineage mismatch"
        ):
            connection.execute(
                """
                INSERT INTO bulk_projection_results (
                    observation_id, row_hash, contract_id,
                    projection_status, reason
                ) VALUES (?, ?, ?, 'selected', NULL)
                """,
                (
                    observation["observation_id"],
                    normalized["row_hash"],
                    contract_id,
                ),
            )
        connection.close()

    def test_full_set_projection_is_identical_when_source_order_is_reversed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "a" / "contratos_2021-2022.csv"
            second = root / "b" / "contratos_2021-2022.csv"
            write_source(first, "\n")
            write_source(second, "\r\n")

            snapshots = []
            for sources in ([first, second], [second, first]):
                connection = sqlite3.connect(":memory:")
                contract_utils.create_schema(connection)
                ingest.ingest_bulk_csvs(connection, list(sources))
                contract = connection.execute(
                    """
                    SELECT canonical_id, family_id, representative_observation_id
                    FROM contracts
                    """
                ).fetchone()
                contributors = connection.execute(
                    """
                    SELECT canonical_id, family_id, observation_id,
                           representative_observation_id, contribution_role,
                           merge_reason, decision_version
                    FROM canonical_observation_contributors
                    ORDER BY observation_id
                    """
                ).fetchall()
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM contracts"
                ).fetchone()[0], 1)
                self.assertEqual(len(contributors), 2)
                self.assertEqual(
                    [row[4] for row in contributors].count("representative"), 1
                )
                self.assertEqual(contract[2], min(row[2] for row in contributors))
                self.assertTrue(all(row[3] == contract[2] for row in contributors))
                snapshots.append((contract, contributors))
                connection.close()

            self.assertEqual(snapshots[0], snapshots[1])

    def test_incremental_new_source_fails_closed_instead_of_freezing_arrival_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "a" / "contratos_2021-2022.csv"
            second = root / "b" / "contratos_2021-2022.csv"
            write_source(first, "\n")
            write_source(second, "\r\n")

            connection = sqlite3.connect(":memory:")
            contract_utils.create_schema(connection)
            ingest.ingest_raw_csv(connection, first, "2021-2022")
            representative_before = connection.execute(
                "SELECT representative_observation_id FROM contracts"
            ).fetchone()[0]
            counts_before = tuple(
                connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "evidence_objects",
                    "bulk_observations",
                    "bulk_projection_results",
                    "contracts",
                    "canonical_observation_contributors",
                )
            )

            with self.assertRaisesRegex(
                RuntimeError, "use ingest_bulk_csvs with the complete source set"
            ):
                ingest.ingest_raw_csv(connection, second, "2021-2022")

            counts_after = tuple(
                connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "evidence_objects",
                    "bulk_observations",
                    "bulk_projection_results",
                    "contracts",
                    "canonical_observation_contributors",
                )
            )
            self.assertEqual(counts_after, counts_before)
            self.assertEqual(
                connection.execute(
                    "SELECT representative_observation_id FROM contracts"
                ).fetchone()[0],
                representative_before,
            )
            connection.close()

    def test_browser_projection_keeps_identities_but_excludes_lineage_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "full.db"
            browser = Path(directory) / "browser.db"
            connection = sqlite3.connect(source)
            contract_utils.create_schema(connection)
            contract_utils.insert_contract_rows(
                connection,
                [{
                    "contract_number": "2021-000001",
                    "entity": "Entidad Demo",
                    "entity_number": "9000",
                    "contractor": "INTEGRA",
                }],
                rebuild_fts=True,
            )
            connection.close()

            build_site_artifacts.build_browser_db(source, browser)
            connection = sqlite3.connect(browser)
            columns = {
                row[1] for row in connection.execute("PRAGMA table_info(contracts)")
            }
            self.assertTrue({"canonical_id", "family_id"} <= columns)
            self.assertIsNone(connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'canonical_observation_contributors'
                """
            ).fetchone())
            browser_source = (REPO_ROOT / "site" / "js" / "db.js").read_text(
                encoding="utf-8"
            )
            self.assertIn("PARTITION BY f.projection_family_id", browser_source)
            self.assertIn("GROUP BY projection_family_id", browser_source)
            self.assertIn('hasContractColumn("canonical_id")', browser_source)
            self.assertIn('hasContractColumn("family_id")', browser_source)
            self.assertIn('hasContractColumn("contractor_canonical_id")', browser_source)
            self.assertIn("refreshContractColumns(_db)", browser_source)
            self.assertIn("const key = row.family_id || `legacy:${legacyKey}`", browser_source)
            self.assertIn("const canonicalIdA = a.canonical_id ||", browser_source)
            self.assertIn('params.set("family_id", familyId)', browser_source)
            self.assertIn("function isPersistedFamilyId(value)", browser_source)
            self.assertIn(
                "isPersistedFamilyId(row.family_id) ? row.family_id : null",
                browser_source,
            )
            self.assertIn("representative_id AS id,\n            family_id,", browser_source)
            self.assertIn(".filter(row => row.family_id === familyId)", browser_source)
            self.assertIn("!row.family_id && (", browser_source)
            self.assertIn("family_id: firstFamilyRow.family_id || null", browser_source)
            contract_source = (REPO_ROOT / "site" / "js" / "contract.js").read_text(
                encoding="utf-8"
            )
            ui_source = (REPO_ROOT / "site" / "js" / "ui.js").read_text(
                encoding="utf-8"
            )
            self.assertIn('params.get("family_id")', contract_source)
            self.assertIn("c.family_id", contract_source)
            self.assertIn("r.family_id", ui_source)
            self.assertNotIn(
                "PARTITION BY f.contract_number, f.entity, f.contractor_family",
                browser_source,
            )
            connection.close()


if __name__ == "__main__":
    unittest.main()
