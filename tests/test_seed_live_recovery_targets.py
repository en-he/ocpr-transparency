import sqlite3
import sys
import unittest
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from contract_utils import create_schema, normalize_contract_record  # noqa: E402
from live_recovery import RecoveryTarget  # noqa: E402
from seed_live_recovery_targets import merge_seeded_targets, scan_contract_family_summaries  # noqa: E402


def insert_contract(conn: sqlite3.Connection, **overrides):
    record = {
        "contract_number": "2022-000100",
        "entity": "Entidad Demo",
        "entity_number": "1000",
        "contractor": "Demo Contractor LLC",
        "amendment": "A",
        "service_category": "SERVICIOS PROFESIONALES",
        "service_type": "SERVICIOS DE CONSULTORIA",
        "amount": 1000.0,
        "amount_receivable": 0.0,
        "award_date": "2022-07-01",
        "valid_from": "2022-07-01",
        "valid_to": "2022-08-01",
        "procurement_method": None,
        "fund_type": None,
        "pco_number": None,
        "cancelled": 0,
        "document_url": None,
        "fiscal_year": "2022-2023",
        "source_type": "csv",
        "source_url": None,
        "source_contract_id": None,
        "inserted_at": "2026-04-09T00:00:00+00:00",
    }
    record.update(overrides)
    normalized = normalize_contract_record(
        record,
        default_source_type=record["source_type"],
        inserted_at=record["inserted_at"],
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
            :row_hash, :contract_number, :entity, :entity_number, :contractor, :amendment,
            :service_category, :service_type, :amount, :amount_receivable, :award_date,
            :valid_from, :valid_to, :procurement_method, :fund_type, :pco_number,
            :cancelled, :document_url, :fiscal_year, :source_type, :source_url,
            :source_contract_id, :inserted_at
        )
        """,
        normalized,
    )


class SeedLiveRecoveryTargetsTests(unittest.TestCase):
    def test_merge_adds_missing_original_candidate_with_earliest_batch(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        insert_contract(
            conn,
            contract_number="2022-000100",
            entity="Entidad Demo",
            contractor="Demo Contractor LLC",
            amendment="A",
            award_date="2021-08-01",
            valid_from="2021-08-01",
            valid_to="2021-12-31",
            fiscal_year="2021-2022",
        )
        insert_contract(
            conn,
            contract_number="2022-000100",
            entity="Entidad Demo",
            contractor="Demo Contractor, LLC",
            amendment="B",
            award_date="2022-07-10",
            valid_from="2022-07-10",
            valid_to="2022-08-31",
            fiscal_year="2022-2023",
        )

        summaries = scan_contract_family_summaries(conn)
        merged_targets, added_count, total_candidates, removed_count, collapsed_count = merge_seeded_targets([], summaries)

        self.assertEqual(len(merged_targets), 1)
        self.assertEqual(added_count, 1)
        self.assertEqual(total_candidates, 1)
        self.assertEqual(removed_count, 0)
        self.assertEqual(collapsed_count, 0)
        self.assertEqual(merged_targets[0].status, "pending")
        self.assertEqual(merged_targets[0].lookup_mode, "auto_discover")
        self.assertEqual(merged_targets[0].recovery_batch, "2021-2022")
        conn.close()

    def test_merge_preserves_existing_manual_target_and_backfills_batch(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        insert_contract(
            conn,
            contract_number="2022-000100",
            entity="Entidad Demo",
            contractor="Demo Contractor LLC",
            amendment="A",
            award_date="2021-08-01",
            fiscal_year="2021-2022",
        )
        insert_contract(
            conn,
            contract_number="2022-000100",
            entity="Entidad Demo",
            contractor="Demo Contractor, LLC",
            amendment="B",
            award_date="2022-07-10",
            fiscal_year="2022-2023",
        )

        existing = [
            RecoveryTarget(
                contract_number="2022-000100",
                entity="Entidad Demo",
                contractor="Demo Contractor LLC",
                recovery_batch=None,
                lookup_mode="manual_url",
                source_url="https://example.com/detail",
                status="recovered",
                notes="manual recovery complete",
                last_checked_at="2026-04-09T00:00:00+00:00",
            )
        ]

        summaries = scan_contract_family_summaries(conn)
        merged_targets, added_count, total_candidates, removed_count, collapsed_count = merge_seeded_targets(existing, summaries)

        self.assertEqual(len(merged_targets), 1)
        self.assertEqual(added_count, 0)
        self.assertEqual(total_candidates, 1)
        self.assertEqual(removed_count, 0)
        self.assertEqual(collapsed_count, 0)
        self.assertEqual(merged_targets[0].lookup_mode, "manual_url")
        self.assertEqual(merged_targets[0].status, "recovered")
        self.assertEqual(merged_targets[0].recovery_batch, "2021-2022")
        conn.close()

    def test_batch_filter_only_adds_requested_new_candidates(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        insert_contract(
            conn,
            contract_number="2022-000100",
            entity="Entidad Demo",
            contractor="Demo Contractor LLC",
            amendment="A",
            award_date="2022-07-01",
            fiscal_year="2022-2023",
        )
        insert_contract(
            conn,
            contract_number="2022-000100",
            entity="Entidad Demo",
            contractor="Demo Contractor LLC",
            amendment="B",
            award_date="2022-08-01",
            fiscal_year="2022-2023",
        )
        insert_contract(
            conn,
            contract_number="2021-000200",
            entity="Entidad Vieja",
            contractor="Legacy Contractor Inc.",
            amendment="A",
            award_date="2021-08-01",
            fiscal_year="2021-2022",
        )
        insert_contract(
            conn,
            contract_number="2021-000200",
            entity="Entidad Vieja",
            contractor="Legacy Contractor Inc.",
            amendment="B",
            award_date="2021-09-01",
            fiscal_year="2021-2022",
        )

        existing = [
            RecoveryTarget(
                contract_number="2011-000300",
                entity="Entidad Histórica",
                contractor="Historic Contractor",
                recovery_batch="2011-2012",
                lookup_mode="auto_discover",
                source_url=None,
                status="unrecoverable",
                notes="kept for audit",
                last_checked_at="2026-04-09T00:00:00+00:00",
            )
        ]

        summaries = scan_contract_family_summaries(conn)
        merged_targets, added_count, total_candidates, removed_count, collapsed_count = merge_seeded_targets(
            existing,
            summaries,
            selected_batches={"2022-2023"},
        )

        self.assertEqual(len(merged_targets), 2)
        self.assertEqual(added_count, 1)
        self.assertEqual(total_candidates, 1)
        self.assertEqual(removed_count, 0)
        self.assertEqual(collapsed_count, 0)
        self.assertEqual(merged_targets[0].recovery_batch, "2022-2023")
        self.assertEqual(merged_targets[0].contract_number, "2022-000100")
        self.assertEqual(merged_targets[1].contract_number, "2011-000300")
        conn.close()

    def test_merge_prunes_stale_ambiguous_target_when_family_already_has_original(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        insert_contract(
            conn,
            contract_number="2017-000081",
            entity="Autoridad de Carreteras y Transportación de Puerto Rico",
            contractor="CSA ARCHITECTS & ENGINEERSLLP",
            amendment="",
            award_date="2021-08-01",
            fiscal_year="2021-2022",
        )
        insert_contract(
            conn,
            contract_number="2017-000081",
            entity="Autoridad de Carreteras y Transportación de Puerto Rico",
            contractor="CSA ARCHITECTS & ENGINEERS LLP",
            amendment="F",
            award_date="2022-01-10",
            fiscal_year="2021-2022",
        )

        existing = [
            RecoveryTarget(
                contract_number="2017-000081",
                entity="Autoridad de Carreteras y Transportación de Puerto Rico",
                contractor="CSA ARCHITECTS & ENGINEERS LLP",
                recovery_batch="2021-2022",
                lookup_mode="auto_discover",
                source_url=None,
                status="ambiguous",
                notes="stale contractor family conflict",
                last_checked_at="2026-04-10T00:00:00+00:00",
            )
        ]

        summaries = scan_contract_family_summaries(conn)
        merged_targets, added_count, total_candidates, removed_count, collapsed_count = merge_seeded_targets(existing, summaries)

        self.assertEqual(merged_targets, [])
        self.assertEqual(added_count, 0)
        self.assertEqual(total_candidates, 0)
        self.assertEqual(removed_count, 1)
        self.assertEqual(collapsed_count, 0)
        conn.close()

    def test_merge_collapses_duplicate_existing_targets_to_best_status(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        insert_contract(
            conn,
            contract_number="2022-000044",
            entity="Autoridad para el Financiamiento de la Infraestructura de Puerto Rico",
            contractor="L&R ENGINEERING GROUP LLC",
            amendment="A",
            award_date="2021-08-01",
            fiscal_year="2021-2022",
        )
        insert_contract(
            conn,
            contract_number="2022-000044",
            entity="Autoridad para el Financiamiento de la Infraestructura de Puerto Rico",
            contractor="L & R ENGINEERING GROUP LLC",
            amendment="B",
            award_date="2022-07-10",
            fiscal_year="2022-2023",
        )

        existing = [
            RecoveryTarget(
                contract_number="2022-000044",
                entity="Autoridad para el Financiamiento de la Infraestructura de Puerto Rico",
                contractor="L & R ENGINEERING GROUP LLC",
                recovery_batch="2022-2023",
                lookup_mode="auto_discover",
                source_url=None,
                status="unrecoverable",
                notes="search returned 0 exact candidate(s)",
                last_checked_at="2026-04-10T00:00:00+00:00",
            ),
            RecoveryTarget(
                contract_number="2022-000044",
                entity="Autoridad para el Financiamiento de la Infraestructura de Puerto Rico",
                contractor="L&R ENGINEERING GROUP LLC",
                recovery_batch="2021-2022",
                lookup_mode="auto_discover",
                source_url="https://consultacontratos.ocpr.gov.pr/contract/details?contractid=1234567",
                status="recovered",
                notes="search returned 1 exact candidate(s); recovered original row",
                last_checked_at="2026-04-10T01:00:00+00:00",
            ),
        ]

        summaries = scan_contract_family_summaries(conn)
        merged_targets, added_count, total_candidates, removed_count, collapsed_count = merge_seeded_targets(existing, summaries)

        self.assertEqual(len(merged_targets), 1)
        self.assertEqual(added_count, 0)
        self.assertEqual(total_candidates, 1)
        self.assertEqual(removed_count, 0)
        self.assertEqual(collapsed_count, 1)
        self.assertEqual(merged_targets[0].status, "recovered")
        self.assertEqual(merged_targets[0].source_url, "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=1234567")
        conn.close()


if __name__ == "__main__":
    unittest.main()
