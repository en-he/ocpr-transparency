import sys
import unittest
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from contract_utils import (  # noqa: E402
    fiscal_year_from_date,
    is_original_amendment,
    normalize_contractor_family,
    normalize_contract_record,
    parse_date,
    row_hash,
)


class ContractUtilsTests(unittest.TestCase):
    def test_parse_date_supports_spanish_detail_dates(self):
        self.assertEqual(parse_date("23 mar. 2022"), "2022-03-23")
        self.assertEqual(parse_date("31 mar. 2022 04:38 p. m."), "2022-03-31")

    def test_contractor_family_normalization_matches_known_variants(self):
        left = normalize_contractor_family("IEMES PSC")
        right = normalize_contractor_family("IEMES, PSC")
        self.assertEqual(left, right)

        nova_a = normalize_contractor_family("Nova Bus a Division of Prevost Car US Inc.")
        nova_b = normalize_contractor_family("NOVA BUS US INC.")
        self.assertEqual(nova_a, nova_b)

        self.assertEqual(
            normalize_contractor_family("CSA ARCHITECTS & ENGINEERSLLP"),
            normalize_contractor_family("CSA ARCHITECTS & ENGINEERS LLP"),
        )
        self.assertEqual(
            normalize_contractor_family("AECOM Caribe L.L.P."),
            normalize_contractor_family("AECOM CARIBE LLP"),
        )
        self.assertEqual(
            normalize_contractor_family("RICOH PUERTO RICOINC"),
            normalize_contractor_family("RICOH PUERTO RICO INC."),
        )
        self.assertEqual(
            normalize_contractor_family("ING. JOSÉ HERNÁNDEZ COLÓN"),
            normalize_contractor_family("JOSÉ HERNÁNDEZ COLÓN"),
        )
        self.assertEqual(
            normalize_contractor_family("C & D INGENIEROS CSP"),
            normalize_contractor_family("C&D INGENIEROS"),
        )
        self.assertEqual(
            normalize_contractor_family("JOY RAMIREZ GONZALEZ H/N/C RAMZ CONTRACTOR SERVICE"),
            normalize_contractor_family("JOY RAMIREZ GONZALEZ"),
        )
        self.assertEqual(
            normalize_contractor_family("FOUNDATION FOR PUERTO RICO INC."),
            normalize_contractor_family("FOUNDATION OF PUERTO RICO INC."),
        )
        self.assertEqual(
            normalize_contractor_family("Autoridadf para el financiamiento de la infraestru"),
            normalize_contractor_family("AUTORIDAD PARA EL FINANCIAMIENTO DE LA INFRAESTRUCTURA DE PUERTO RICO"),
        )
        self.assertEqual(
            normalize_contractor_family("Maglez Engineering & Contractors Corp."),
            normalize_contractor_family("MAGLEZ ENGINEERINGS & CONTRACTORS CORPORATION"),
        )
        self.assertEqual(
            normalize_contractor_family("CONSTRUCCIONES DEL VIVI Y AGREDADO CORP."),
            normalize_contractor_family("CONSTRUCCIONES DEL VIVI Y AGREGADOS CORP."),
        )
        self.assertEqual(
            normalize_contractor_family("BERMUDEZLONGODIAZ-MASSOLLC"),
            normalize_contractor_family("BERMUDEZ LONGO DIAZ-MASSO LLC"),
        )
        self.assertEqual(
            normalize_contractor_family("Desing Build LLC"),
            normalize_contractor_family("DESIGN BUILD LLC"),
        )
        self.assertEqual(
            normalize_contractor_family("JOSEPH HARRISON FLORESDBAHARISON CONSULTING"),
            normalize_contractor_family("JOSEPH HARRISON FLORES"),
        )
        self.assertEqual(
            normalize_contractor_family("MUNICIPIO DE VIEQUES CCD"),
            normalize_contractor_family("MUNICIPIO DE VIEQUES"),
        )
        self.assertEqual(
            normalize_contractor_family("MUNICIPIO DE SAN LOENZO"),
            normalize_contractor_family("MUNICIPIO DE SAN LORENZO"),
        )
        self.assertEqual(
            normalize_contractor_family("AUTORIDAD PARA EL FINANCIAMIENTO DE LA INFRAESTRUC"),
            normalize_contractor_family("AUTORIDAD PARA EL FINANCIAMIENTO DE LA INFRAESTRUCTURA DE PUERTO RICO"),
        )
        self.assertEqual(
            normalize_contractor_family("JF BUILDING LEASE & MAINTENANCE CORP"),
            normalize_contractor_family("J.F. BUILDING LEASE AND MAINTENANCE CORP"),
        )
        self.assertEqual(
            normalize_contractor_family("ISIDRO M. MARTINEZ GILORMINI"),
            normalize_contractor_family("MARTINEZ GILORMINI ISIDRO M"),
        )
        self.assertEqual(
            normalize_contractor_family("LA ADMINISTRACION DE COMPENSACIONES POR ACCIDENTES"),
            normalize_contractor_family("ADMINISTRACIÓN DE COMPENSACIONES POR ACCIDENTES DE AUTOMÓVILES"),
        )
        self.assertEqual(
            normalize_contractor_family("CANCIO NADAL & RIVERA LLC"),
            normalize_contractor_family("CANCIONADAL & RIVERA LLC"),
        )
        self.assertEqual(
            normalize_contractor_family("AQUINO DE CORDOVA ALFARO & CO. LLP."),
            normalize_contractor_family("Aquino de CordovaAlfaro & Co. LLP"),
        )
        self.assertEqual(
            normalize_contractor_family("RICHARD SANTOS GARCIA MA"),
            normalize_contractor_family("Richard Santos GarciaMA"),
        )
        self.assertEqual(
            normalize_contractor_family("UNIVERSITY OF PUERTO RICO PARKING SYSTEM INC."),
            normalize_contractor_family("UNIVERSIDA OF PUERTO RICO PARKING SYSTEM INC"),
        )
        self.assertEqual(
            normalize_contractor_family("NAIOSCALY CRUZ PONCE"),
            normalize_contractor_family("CRUZ PONCE NAIOSCALY"),
        )
        self.assertEqual(
            normalize_contractor_family("GIOVANY RIVERA CARRERO"),
            normalize_contractor_family("RIVERA CARRERO GIOVANY"),
        )
        self.assertEqual(
            normalize_contractor_family("A1 GENERATOR SERVICES INC."),
            normalize_contractor_family("AI GENERATOR SERVICES INC."),
        )
        self.assertEqual(
            normalize_contractor_family("T & P CONSULTING INC."),
            normalize_contractor_family("QUANTUM HEALTH CONSULTING"),
        )
        self.assertEqual(
            normalize_contractor_family("T & P CONSULTING INC. D/B/A QUANTUM HEALTH CONSUL"),
            normalize_contractor_family("QUANTUM HEALTH CONSULTING"),
        )
        self.assertEqual(
            normalize_contractor_family("INTEGRA"),
            normalize_contractor_family("INTEGRA DESIGN GROUP. PSC"),
        )

    def test_contractor_family_normalization_keeps_distinct_manual_review_case_distinct(self):
        self.assertNotEqual(
            normalize_contractor_family("DEPARTAMENTO DE EDUCACIÓN DE PUERTO RICO"),
            normalize_contractor_family("AUTORIDAD DE EDIFICIOS PUBLICOS"),
        )

    def test_original_amendment_classification_accepts_blank_and_original(self):
        self.assertTrue(is_original_amendment(""))
        self.assertTrue(is_original_amendment("   "))
        self.assertTrue(is_original_amendment("Original"))
        self.assertFalse(is_original_amendment("A"))

    def test_fiscal_year_from_date_uses_pr_fiscal_calendar(self):
        self.assertEqual(fiscal_year_from_date("2022-03-23"), "2021-2022")
        self.assertEqual(fiscal_year_from_date("2022-07-01"), "2022-2023")

    def test_row_hash_distinguishes_original_from_amendment_rows(self):
        base = normalize_contract_record(
            {
                "contract_number": "2023-000017",
                "entity": "Guardia Nacional de Puerto Rico",
                "contractor": "GLORIMAR MUÑOZ BERLY",
                "amendment": "",
                "award_date": "2022-10-11",
                "amount": 36100.0,
            },
            inserted_at="2026-04-10T00:00:00+00:00",
        )
        amendment_a = dict(base)
        amendment_a["amendment"] = "A"

        self.assertNotEqual(row_hash(base), row_hash(amendment_a))


if __name__ == "__main__":
    unittest.main()
