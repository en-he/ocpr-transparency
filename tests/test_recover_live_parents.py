import json
import sqlite3
import sys
import unittest
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from contract_utils import create_schema, normalize_contract_record  # noqa: E402
from live_recovery import RecoveryTarget, normalize_search_result_row  # noqa: E402
from recover_live_parents import process_target, select_targets_for_processing  # noqa: E402


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "recovery"


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


def build_target(**overrides):
    data = {
        "contract_number": "2022-000019",
        "entity": "Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios",
        "contractor": "IEMES PSC",
        "recovery_batch": "2021-2022",
        "lookup_mode": "manual_url",
        "source_url": "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440",
        "status": "pending",
        "notes": None,
        "last_checked_at": None,
    }
    data.update(overrides)
    return RecoveryTarget(**data)


class RecoverLiveParentsTests(unittest.TestCase):
    def setUp(self):
        self.detail_html = (FIXTURES / "detail_2022_000019.html").read_text(encoding="utf-8")
        self.search_one = json.loads((FIXTURES / "search_one_candidate.json").read_text(encoding="utf-8"))
        self.search_zero = json.loads((FIXTURES / "search_zero_candidates.json").read_text(encoding="utf-8"))
        self.search_multi = json.loads((FIXTURES / "search_multi_candidates.json").read_text(encoding="utf-8"))

    def test_successful_manual_recovery_appends_row(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        recovered_rows = []
        client = FakeRecoveryClient(
            search_payload=self.search_one,
            detail_pages={
                "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440": self.detail_html
            },
        )
        target = build_target()

        outcome = process_target(
            target,
            client=client,
            conn=conn,
            recovered_rows=recovered_rows,
            dry_run=False,
        )

        self.assertEqual(outcome, "recovered")
        self.assertEqual(target.status, "recovered")
        self.assertEqual(len(recovered_rows), 1)
        self.assertEqual(recovered_rows[0]["source_contract_id"], "5248440")
        self.assertEqual(recovered_rows[0]["source_type"], "live_recovery")
        conn.close()

    def test_idempotent_rerun_does_not_duplicate_recovered_row(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        recovered_rows = []
        client = FakeRecoveryClient(
            search_payload=self.search_one,
            detail_pages={
                "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440": self.detail_html
            },
        )

        first_target = build_target()
        second_target = build_target()

        self.assertEqual(
            process_target(first_target, client=client, conn=conn, recovered_rows=recovered_rows, dry_run=False),
            "recovered",
        )
        self.assertEqual(
            process_target(second_target, client=client, conn=conn, recovered_rows=recovered_rows, dry_run=False),
            "recovered",
        )
        self.assertEqual(len(recovered_rows), 1)
        self.assertIn("already exists", second_target.notes)
        conn.close()

    def test_zero_candidate_auto_discovery_marks_unrecoverable(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        target = build_target(lookup_mode="auto_discover", source_url=None)
        client = FakeRecoveryClient(search_payload=self.search_zero)

        outcome = process_target(
            target,
            client=client,
            conn=conn,
            recovered_rows=[],
            dry_run=False,
        )

        self.assertEqual(outcome, "unrecoverable")
        self.assertEqual(target.status, "unrecoverable")
        conn.close()

    def test_multiple_valid_candidates_marks_ambiguous(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        target = build_target(lookup_mode="auto_discover", source_url=None)
        detail_pages = {
            "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440": self.detail_html,
            "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248441": self.detail_html,
        }
        client = FakeRecoveryClient(search_payload=self.search_multi, detail_pages=detail_pages)

        outcome = process_target(
            target,
            client=client,
            conn=conn,
            recovered_rows=[],
            dry_run=False,
        )

        self.assertEqual(outcome, "ambiguous")
        self.assertEqual(target.status, "ambiguous")
        conn.close()

    def test_existing_live_recovery_row_is_treated_as_idempotent_match(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        existing = normalize_contract_record(
            {
                "contract_number": "2022-000019",
                "entity": "Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios",
                "entity_number": "3136",
                "contractor": "IEMES, PSC",
                "amendment": "",
                "service_category": "SERVICIOS PROFESIONALES",
                "service_type": "SERVICIOS DE INGENIERÍA",
                "amount": 16200.0,
                "amount_receivable": 0.0,
                "award_date": "2022-03-23",
                "valid_from": "2022-03-23",
                "valid_to": "2022-06-30",
                "procurement_method": "No Aplica",
                "fund_type": "Fondos Estatales",
                "pco_number": "2022-06042",
                "cancelled": 0,
                "document_url": None,
                "fiscal_year": "2021-2022",
                "source_type": "live_recovery",
                "source_url": "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=9999999",
                "source_contract_id": "9999999",
                "inserted_at": "2026-04-09T00:00:00+00:00",
            },
            default_source_type="live_recovery",
            inserted_at="2026-04-09T00:00:00+00:00",
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
            existing,
        )
        conn.commit()

        client = FakeRecoveryClient(
            search_payload=self.search_one,
            detail_pages={
                "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440": self.detail_html
            },
        )
        target = build_target()

        outcome = process_target(
            target,
            client=client,
            conn=conn,
            recovered_rows=[],
            dry_run=False,
        )

        self.assertEqual(outcome, "recovered")
        self.assertEqual(target.status, "recovered")
        self.assertIn("already exists", target.notes)
        conn.close()

    def test_existing_csv_original_with_missing_contractor_is_deferred_to_enrichment(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        existing = normalize_contract_record(
            {
                "contract_number": "2022-000019",
                "entity": "Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios",
                "entity_number": "3136",
                "contractor": None,
                "amendment": "",
                "service_category": "SERVICIOS PROFESIONALES",
                "service_type": "SERVICIOS DE INGENIERÍA",
                "amount": 16200.0,
                "amount_receivable": 0.0,
                "award_date": "2022-03-23",
                "valid_from": "2022-03-23",
                "valid_to": "2022-06-30",
                "procurement_method": None,
                "fund_type": None,
                "pco_number": "2022-06042",
                "cancelled": 0,
                "document_url": None,
                "fiscal_year": "2021-2022",
                "source_type": "csv",
                "source_url": None,
                "source_contract_id": None,
                "inserted_at": "2026-04-09T00:00:00+00:00",
            },
            default_source_type="csv",
            inserted_at="2026-04-09T00:00:00+00:00",
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
            existing,
        )
        conn.commit()

        client = FakeRecoveryClient(
            search_payload=self.search_one,
            detail_pages={
                "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440": self.detail_html
            },
        )
        target = build_target()

        outcome = process_target(
            target,
            client=client,
            conn=conn,
            recovered_rows=[],
            dry_run=False,
        )

        self.assertEqual(outcome, "unrecoverable")
        self.assertEqual(target.status, "unrecoverable")
        self.assertIn("enrichment track", target.notes)
        conn.close()

    def test_existing_csv_row_with_same_hash_is_deferred_to_enrichment(self):
        conn = sqlite3.connect(":memory:")
        create_schema(conn)
        existing = normalize_contract_record(
            {
                "contract_number": "2022-000019",
                "entity": "Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios",
                "entity_number": "3136",
                "contractor": "IEMES, PSC",
                "amendment": "",
                "service_category": "SERVICIOS PROFESIONALES",
                "service_type": "SERVICIOS DE INGENIERÍA",
                "amount": 16200.0,
                "amount_receivable": 0.0,
                "award_date": "2022-03-23",
                "valid_from": "2022-03-23",
                "valid_to": "2022-06-30",
                "procurement_method": None,
                "fund_type": None,
                "pco_number": "2022-06042",
                "cancelled": 0,
                "document_url": None,
                "fiscal_year": "2021-2022",
                "source_type": "csv",
                "source_url": None,
                "source_contract_id": None,
                "inserted_at": "2026-04-09T00:00:00+00:00",
            },
            default_source_type="csv",
            inserted_at="2026-04-09T00:00:00+00:00",
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
            existing,
        )
        conn.commit()

        client = FakeRecoveryClient(
            search_payload=self.search_one,
            detail_pages={
                "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440": self.detail_html
            },
        )
        target = build_target()

        outcome = process_target(
            target,
            client=client,
            conn=conn,
            recovered_rows=[],
            dry_run=False,
        )

        self.assertEqual(outcome, "unrecoverable")
        self.assertEqual(target.status, "unrecoverable")
        self.assertIn("enrichment track", target.notes)
        conn.close()

    def test_batch_selection_filters_pending_targets(self):
        targets = [
            build_target(contract_number="2022-000001", recovery_batch="2022-2023"),
            build_target(contract_number="2021-000001", recovery_batch="2021-2022"),
            build_target(contract_number="2011-000001", recovery_batch="2011-2012"),
        ]

        selected = select_targets_for_processing(
            targets,
            contract_number=None,
            batches=["2022-2023", "2021-2022"],
            all_pending=False,
        )

        self.assertEqual(
            [target.contract_number for target in selected],
            ["2022-000001", "2021-000001"],
        )

    def test_retry_status_can_select_ambiguous_targets_by_batch(self):
        targets = [
            build_target(contract_number="2022-000001", recovery_batch="2022-2023", status="ambiguous"),
            build_target(contract_number="2021-000001", recovery_batch="2021-2022", status="ambiguous"),
            build_target(contract_number="2011-000001", recovery_batch="2011-2012", status="unrecoverable"),
        ]

        selected = select_targets_for_processing(
            targets,
            contract_number=None,
            batches=["2021-2022"],
            all_pending=False,
            retry_statuses=["ambiguous"],
        )

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].contract_number, "2021-000001")
        self.assertEqual(selected[0].status, "ambiguous")

    def test_guardrail_requires_batch_for_large_pending_backlog(self):
        targets = [
            build_target(contract_number=f"2022-{index:06d}", recovery_batch="2022-2023")
            for index in range(101)
        ]

        with self.assertRaises(ValueError):
            select_targets_for_processing(
                targets,
                contract_number=None,
                batches=None,
                all_pending=False,
                pending_guard_limit=100,
            )

    def test_contract_number_filter_bypasses_large_pending_guard(self):
        targets = [
            build_target(contract_number=f"2022-{index:06d}", recovery_batch="2022-2023")
            for index in range(101)
        ]

        selected = select_targets_for_processing(
            targets,
            contract_number="2022-000042",
            batches=None,
            all_pending=False,
            pending_guard_limit=100,
        )

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0].contract_number, "2022-000042")


if __name__ == "__main__":
    unittest.main()
