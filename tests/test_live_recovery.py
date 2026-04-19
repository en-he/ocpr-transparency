import json
import sys
import unittest
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from contract_utils import normalize_contract_record  # noqa: E402
from live_recovery import (  # noqa: E402
    ParsedContractDetail,
    RecoveryTarget,
    extract_contract_id,
    filter_search_candidates,
    parse_contract_detail_html,
    validate_detail_match,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "recovery"


class LiveRecoveryTests(unittest.TestCase):
    def test_detail_parser_extracts_canonical_fields(self):
        html_text = (FIXTURES / "detail_2022_000019.html").read_text(encoding="utf-8")
        parsed = parse_contract_detail_html(
            html_text,
            source_url="https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440",
            captured_at="2026-04-09T00:00:00+00:00",
        )
        record = parsed.record

        self.assertEqual(record["contract_number"], "2022-000019")
        self.assertEqual(record["entity"], "Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios")
        self.assertEqual(record["entity_number"], "3136")
        self.assertEqual(record["contractor"], "IEMES, PSC")
        self.assertEqual(record["fiscal_year"], "2021-2022")
        self.assertEqual(record["source_contract_id"], "5248440")
        self.assertEqual(record["source_url"], "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440")

    def test_validate_detail_match_rejects_wrong_entity(self):
        html_text = (FIXTURES / "detail_wrong_entity.html").read_text(encoding="utf-8")
        parsed = parse_contract_detail_html(
            html_text,
            source_url="https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5166880",
            captured_at="2026-04-09T00:00:00+00:00",
        )
        target = RecoveryTarget(
            contract_number="2022-000019",
            entity="Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios",
            contractor="IEMES PSC",
            recovery_batch="2021-2022",
            lookup_mode="manual_url",
            source_url="https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5166880",
            status="pending",
            notes=None,
            last_checked_at=None,
        )
        is_valid, reason = validate_detail_match(target, parsed)
        self.assertFalse(is_valid)
        self.assertIn("entity", reason)

    def test_validate_detail_match_requires_primary_contractor_family_match(self):
        parsed = ParsedContractDetail(
            record=normalize_contract_record(
                {
                    "contract_number": "2022-000044",
                    "entity": "Autoridad para el Financiamiento de la Infraestructura de Puerto Rico",
                    "entity_number": "3090",
                    "contractor": "L&R ENGINEERING GROUP, LLC",
                    "amendment": "",
                    "service_category": "CONSTRUCCION",
                    "service_type": "ESCUELAS",
                    "amount": 5421691.0,
                    "amount_receivable": 0.0,
                    "award_date": "2021-07-19",
                    "valid_from": "2021-07-19",
                    "valid_to": "2021-12-16",
                    "source_type": "live_recovery",
                    "source_url": "https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5178501",
                    "source_contract_id": "5178501",
                    "inserted_at": "2026-04-10T00:00:00+00:00",
                },
                default_source_type="live_recovery",
                inserted_at="2026-04-10T00:00:00+00:00",
            ),
            contractor_names=[
                "L&R ENGINEERING GROUP, LLC",
                "STRUCTURAL CONSULTING SERVICES PSC",
            ],
            entity_display="3090 Autoridad para el Financiamiento de la Infraestructura de Puerto Rico",
        )
        target = RecoveryTarget(
            contract_number="2022-000044",
            entity="Autoridad para el Financiamiento de la Infraestructura de Puerto Rico",
            contractor="STRUCTURAL CONSULTING SERVICES PSC",
            recovery_batch="2021-2022",
            lookup_mode="manual_url",
            source_url="https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5178501",
            status="pending",
            notes=None,
            last_checked_at=None,
        )

        is_valid, reason = validate_detail_match(target, parsed)

        self.assertFalse(is_valid)
        self.assertIn("contractor family", reason)

    def test_filter_search_candidates_requires_exact_entity_and_family(self):
        payload = json.loads((FIXTURES / "search_one_candidate.json").read_text(encoding="utf-8"))
        target = RecoveryTarget(
            contract_number="2022-000019",
            entity="Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios",
            contractor="IEMES PSC",
            recovery_batch="2021-2022",
            lookup_mode="auto_discover",
            source_url=None,
            status="pending",
            notes=None,
            last_checked_at=None,
        )
        candidates = filter_search_candidates(payload["data"], target)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(extract_contract_id("https://consultacontratos.ocpr.gov.pr/contract/details?contractid=5248440"), "5248440")


if __name__ == "__main__":
    unittest.main()
