import csv
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import build_site_artifacts  # noqa: E402
import ingest  # noqa: E402
import monitor  # noqa: E402
import seed_live_recovery_targets  # noqa: E402
from contract_utils import create_schema  # noqa: E402
from live_recovery import load_recovery_targets, write_recovered_rows, write_recovery_targets  # noqa: E402
from recover_live_parents import process_target, select_targets_for_processing  # noqa: E402


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "recovery"


class PipelineIntegrationTests(unittest.TestCase):
    class FakeRecoveryClient:
        def __init__(self, *, entity_id="3136", search_payload=None, detail_pages=None):
            self.entity_id = entity_id
            self.search_payload = search_payload or {"data": []}
            self.detail_pages = detail_pages or {}

        def resolve_entity_id(self, entity_name):
            return self.entity_id

        def search_contract_rows(self, **kwargs):
            return self.search_payload

        def fetch_detail_html(self, source_url):
            return self.detail_pages[source_url]

    def test_schema_migration_adds_provenance_columns(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE contracts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_number TEXT,
                entity TEXT
            )
            """
        )
        create_schema(conn)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(contracts)").fetchall()}
        self.assertIn("source_type", columns)
        self.assertIn("source_url", columns)
        self.assertIn("source_contract_id", columns)
        conn.close()

    def test_ingest_reads_recovery_csv_with_reset_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            raw_dir = tmp / "raw"
            raw_dir.mkdir()
            raw_csv = raw_dir / "contratos_2021-2022.csv"
            with open(raw_csv, "w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(["Núm. Contrato", "Entidad", "Contratista", "Enmienda", "Cuantía", "Otorgado en"])
                writer.writerow(["2021-000001", "Entidad Demo", "Demo Contractor", "", "$10,000.00", "2021-08-01"])

            recovery_csv = tmp / "live_recovered_contracts.csv"
            with open(recovery_csv, "w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=[
                    "contract_number", "entity", "entity_number", "contractor", "amendment",
                    "service_category", "service_type", "amount", "amount_receivable",
                    "award_date", "valid_from", "valid_to", "procurement_method", "fund_type",
                    "pco_number", "cancelled", "document_url", "fiscal_year",
                    "source_type", "source_url", "source_contract_id"
                ])
                writer.writeheader()
                writer.writerow({
                    "contract_number": "2022-000019",
                    "entity": "Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios",
                    "entity_number": "3136",
                    "contractor": "IEMES, PSC",
                    "amendment": "",
                    "service_category": "SERVICIOS PROFESIONALES",
                    "service_type": "SERVICIOS DE INGENIERÍA",
                    "amount": "16200.0",
                    "amount_receivable": "0.0",
                    "award_date": "2022-03-23",
                    "valid_from": "2022-03-23",
                    "valid_to": "2022-06-30",
                    "procurement_method": "No Aplica",
                    "fund_type": "Fondos Estatales",
                    "pco_number": "2022-06042",
                    "cancelled": "0",
                    "document_url": "",
                    "fiscal_year": "2021-2022",
                    "source_type": "live_recovery",
                    "source_url": "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440",
                    "source_contract_id": "5248440",
                })

            db_path = tmp / "contracts.db"
            conn = sqlite3.connect(db_path)
            create_schema(conn)
            ingest.ingest_raw_csv(conn, raw_csv, "2021-2022")
            ingest.ingest_recovery_csv(conn, recovery_csv)

            total_rows = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
            source_types = {
                row[0]: row[1]
                for row in conn.execute("SELECT contract_number, source_type FROM contracts")
            }

            self.assertEqual(total_rows, 2)
            self.assertEqual(source_types["2021-000001"], "csv")
            self.assertEqual(source_types["2022-000019"], "live_recovery")
            conn.close()

    def test_browser_artifact_build_preserves_provenance_columns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            source_db = tmp / "source.db"
            browser_db = tmp / "browser.db"

            conn = sqlite3.connect(source_db)
            create_schema(conn)
            conn.execute(
                """
                INSERT INTO contracts (
                    row_hash, contract_number, entity, entity_number, contractor, amendment,
                    service_category, service_type, amount, amount_receivable, award_date,
                    valid_from, valid_to, procurement_method, fund_type, pco_number,
                    cancelled, document_url, fiscal_year, source_type, source_url,
                    source_contract_id, inserted_at
                ) VALUES (
                    'abc123', '2022-000019',
                    'Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios',
                    '3136', 'IEMES, PSC', '',
                    'SERVICIOS PROFESIONALES', 'SERVICIOS DE INGENIERÍA',
                    16200.0, 0.0, '2022-03-23', '2022-03-23', '2022-06-30',
                    'No Aplica', 'Fondos Estatales', '2022-06042',
                    0, NULL, '2021-2022', 'live_recovery',
                    'https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440',
                    '5248440', '2026-04-09T00:00:00+00:00'
                )
                """
            )
            conn.commit()
            conn.close()

            build_site_artifacts.build_browser_db(source_db, browser_db)
            browser_conn = sqlite3.connect(browser_db)
            columns = {row[1] for row in browser_conn.execute("PRAGMA table_info(contracts)").fetchall()}
            row = browser_conn.execute(
                "SELECT source_type, source_url, source_contract_id FROM contracts WHERE contract_number = '2022-000019'"
            ).fetchone()
            self.assertIn("source_type", columns)
            self.assertIn("source_url", columns)
            self.assertIn("source_contract_id", columns)
            self.assertEqual(row, ("live_recovery", "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440", "5248440"))
            browser_conn.close()

    def test_manifest_includes_recovery_targets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            recovery_targets_csv = tmp / "live_recovery_targets.csv"
            with open(recovery_targets_csv, "w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=[
                    "contract_number", "entity", "contractor", "recovery_batch", "lookup_mode",
                    "source_url", "status", "notes", "last_checked_at",
                ])
                writer.writeheader()
                writer.writerow({
                    "contract_number": "2008-000669",
                    "entity": "Municipio de Humacao",
                    "contractor": "IEMS & M H, INC.",
                    "recovery_batch": "2011-2012",
                    "lookup_mode": "auto_discover",
                    "source_url": "",
                    "status": "unrecoverable",
                    "notes": "search returned 0 exact candidate(s)",
                    "last_checked_at": "2026-04-09T04:30:46.713070+00:00",
                })

            full_gz = tmp / "contratos-full.db.gz"
            full_gz.write_bytes(b"test full db")
            manifest_path = tmp / "data-manifest.json"

            recovery_targets = build_site_artifacts.load_recovery_targets(recovery_targets_csv)
            build_site_artifacts.write_manifest(
                manifest_path,
                browser_download={
                    "sha256": "browser-sha",
                    "size_bytes": 123,
                    "format": "sqlite+gzip",
                    "url": "contratos.db.gz",
                },
                full_gz=full_gz,
                repo_raw_base="https://example.com/repo",
                full_download_url="https://example.com/full.db.gz",
                stats={
                    "row_count": 1,
                    "total_amount": 0,
                    "fiscal_years": ["2011-2012"],
                    "archived_csv_fiscal_years": ["2011-2012"],
                },
                dashboard={
                    "top_contractors": [],
                    "top_entities": [],
                    "yearly_spending": [],
                },
                recovery_targets=recovery_targets,
            )

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(len(manifest["recovery_targets"]), 1)
            self.assertEqual(manifest["recovery_targets"][0]["status"], "unrecoverable")
            self.assertEqual(manifest["recovery_targets"][0]["contract_number"], "2008-000669")
            self.assertEqual(manifest["recovery_targets"][0]["recovery_batch"], "2011-2012")
            self.assertEqual(manifest["archived_csv_fiscal_years"], ["2011-2012"])

    def test_discover_archived_csv_fiscal_years_uses_raw_csv_files_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            (tmp / "contratos_2022-2023.csv").write_text("", encoding="utf-8")
            (tmp / "contratos_2010-2011.csv").write_text("", encoding="utf-8")
            (tmp / "contratos_1989-1990.backup.csv").write_text("", encoding="utf-8")
            (tmp / "notes.txt").write_text("", encoding="utf-8")

            fiscal_years = build_site_artifacts.discover_archived_csv_fiscal_years(tmp)

            self.assertEqual(fiscal_years, ["2022-2023", "2010-2011"])

    def test_collect_dashboard_can_limit_to_archived_csv_years(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            source_db = tmp / "browser.db"

            conn = sqlite3.connect(source_db)
            create_schema(conn)
            conn.execute(
                """
                INSERT INTO contracts (
                    row_hash, contract_number, entity, entity_number, contractor, amendment,
                    service_category, service_type, amount, amount_receivable, award_date,
                    valid_from, valid_to, procurement_method, fund_type, pco_number,
                    cancelled, document_url, fiscal_year, source_type, source_url,
                    source_contract_id, inserted_at
                ) VALUES (
                    'row-1', '2022-000001', 'Entidad Demo', '1000', 'Demo Contractor', '',
                    'SERVICIOS PROFESIONALES', 'SERVICIOS PROFESIONALES',
                    100.0, 0.0, '2022-07-01', '2022-07-01', '2022-12-31',
                    NULL, NULL, NULL,
                    0, NULL, '2022-2023', 'csv', NULL,
                    NULL, '2026-04-09T00:00:00+00:00'
                )
                """
            )
            conn.execute(
                """
                INSERT INTO contracts (
                    row_hash, contract_number, entity, entity_number, contractor, amendment,
                    service_category, service_type, amount, amount_receivable, award_date,
                    valid_from, valid_to, procurement_method, fund_type, pco_number,
                    cancelled, document_url, fiscal_year, source_type, source_url,
                    source_contract_id, inserted_at
                ) VALUES (
                    'row-2', '1999-000001', 'Entidad Vieja', '1001', 'Legacy Contractor', '',
                    'SERVICIOS PROFESIONALES', 'SERVICIOS PROFESIONALES',
                    200.0, 0.0, '1999-07-01', '1999-07-01', '2000-06-30',
                    NULL, NULL, NULL,
                    0, NULL, '1999-2000', 'live_recovery', NULL,
                    NULL, '2026-04-09T00:00:00+00:00'
                )
                """
            )
            conn.commit()
            conn.close()

            dashboard = build_site_artifacts.collect_dashboard(
                source_db,
                archived_csv_fiscal_years=["2022-2023"],
            )

            self.assertEqual(
                dashboard["yearly_spending"],
                [{"fiscal_year": "2022-2023", "family_count": 1, "total_amount": 100.0}],
            )

    def test_monitor_marks_live_monitor_provenance(self):
        search_payload = json.loads((FIXTURES / "search_one_candidate.json").read_text(encoding="utf-8"))
        detail_html = (FIXTURES / "detail_2022_000019.html").read_text(encoding="utf-8")

        class FakeMonitorClient:
            def iter_recent_contract_rows(self, *, award_date_from, page_size=100):
                yield search_payload["data"]

            def fetch_detail_html(self, source_url):
                return detail_html

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            db_path = tmp / "monitor.db"

            original_db_path = monitor.DB_PATH
            monitor.DB_PATH = db_path
            try:
                with patch.object(monitor, "OCPRContractRegistryClient", return_value=FakeMonitorClient()):
                    result = monitor.run("2022-03-01", dry_run=False, notify=None)
                self.assertTrue(result["success"])

                conn = sqlite3.connect(db_path)
                row = conn.execute(
                    "SELECT source_type, source_url, source_contract_id FROM contracts WHERE contract_number = '2022-000019'"
                ).fetchone()
                self.assertEqual(row[0], "live_monitor")
                self.assertTrue(row[1].endswith("contractid=5248440"))
                self.assertEqual(row[2], "5248440")
                conn.close()
            finally:
                monitor.DB_PATH = original_db_path

    def test_seed_batch_recover_and_rebuild_preserves_original(self):
        search_payload = json.loads((FIXTURES / "search_one_candidate.json").read_text(encoding="utf-8"))
        detail_html = (FIXTURES / "detail_2022_000019.html").read_text(encoding="utf-8")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            raw_dir = tmp / "raw"
            raw_dir.mkdir()
            raw_csv = raw_dir / "contratos_2021-2022.csv"
            with open(raw_csv, "w", encoding="utf-8", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(["Núm. Contrato", "Entidad", "Contratista", "Enmienda", "Cuantía", "Otorgado en"])
                writer.writerow([
                    "2022-000019",
                    "Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios",
                    "IEMES PSC",
                    "A",
                    "$5,000.00",
                    "2022-04-01",
                ])
                writer.writerow([
                    "2022-000019",
                    "Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios",
                    "IEMES PSC",
                    "B",
                    "$6,000.00",
                    "2022-05-01",
                ])

            db_path = tmp / "contracts.db"
            recovery_csv = tmp / "live_recovered_contracts.csv"
            targets_csv = tmp / "live_recovery_targets.csv"

            conn = sqlite3.connect(db_path)
            create_schema(conn)
            ingest.ingest_raw_csv(conn, raw_csv, "2021-2022")

            summaries = seed_live_recovery_targets.scan_contract_family_summaries(conn)
            targets, added_count, total_candidates, removed_count, collapsed_count = seed_live_recovery_targets.merge_seeded_targets([], summaries)
            self.assertEqual(added_count, 1)
            self.assertEqual(total_candidates, 1)
            self.assertEqual(removed_count, 0)
            self.assertEqual(collapsed_count, 0)
            write_recovery_targets(targets_csv, targets)

            loaded_targets = load_recovery_targets(targets_csv)
            selected_targets = select_targets_for_processing(
                loaded_targets,
                contract_number=None,
                batches=["2021-2022"],
                all_pending=False,
            )
            self.assertEqual(len(selected_targets), 1)

            recovered_rows = []
            client = self.FakeRecoveryClient(
                search_payload=search_payload,
                detail_pages={
                    "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440": detail_html
                },
            )
            outcome = process_target(
                selected_targets[0],
                client=client,
                conn=conn,
                recovered_rows=recovered_rows,
                dry_run=False,
            )
            self.assertEqual(outcome, "recovered")
            write_recovered_rows(recovery_csv, recovered_rows)
            write_recovery_targets(targets_csv, loaded_targets)
            conn.close()

            rebuilt_path = tmp / "rebuilt.db"
            rebuilt_conn = sqlite3.connect(rebuilt_path)
            create_schema(rebuilt_conn)
            ingest.ingest_raw_csv(rebuilt_conn, raw_csv, "2021-2022")
            ingest.ingest_recovery_csv(rebuilt_conn, recovery_csv)
            original_row = rebuilt_conn.execute(
                """
                SELECT amendment, source_type
                FROM contracts
                WHERE contract_number = '2022-000019'
                  AND entity = 'Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios'
                  AND amendment = ''
                """
            ).fetchone()
            self.assertEqual(original_row, ("", "live_recovery"))
            rebuilt_conn.close()


if __name__ == "__main__":
    unittest.main()
