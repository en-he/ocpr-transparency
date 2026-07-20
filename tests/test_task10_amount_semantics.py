import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SITE = REPO_ROOT / "site"
JS = SITE / "js"


class ReportedAmountLanguageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.index = (SITE / "index.html").read_text(encoding="utf-8")
        cls.i18n = (JS / "i18n.js").read_text(encoding="utf-8")
        cls.ui = (JS / "ui.js").read_text(encoding="utf-8")
        cls.db = (JS / "db.js").read_text(encoding="utf-8")
        cls.detail = (SITE / "contract.html").read_text(encoding="utf-8")
        cls.provenance = (
            REPO_ROOT / "docs" / "project" / "data-provenance.md"
        ).read_text(encoding="utf-8")

    def test_public_labels_use_reported_contract_amount_language(self):
        required = (
            "Reported contract amounts",
            "Reported source-row amount",
            "Representative original reported amount",
            "Unvalidated family-row sum of reported amounts",
            "Cuantías reportadas de contratos",
            "Cuantía reportada de la fila fuente",
            "Cuantía reportada de la fila original representativa",
            "Suma no validada de cuantías reportadas de las filas de la familia",
        )
        public_copy = self.index + self.i18n
        for phrase in required:
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, public_copy)

    def test_stale_spending_payment_and_current_value_claims_are_absent(self):
        public_copy = (self.index + self.i18n).casefold()
        stale_phrases = (
            "quién recibe y quién gasta",
            "entidades con más gasto",
            "tendencia anual del gasto",
            "who gets paid and who spends",
            "yearly spending trend",
            "amount payable",
            "amount receivable",
            "cuantía a pagar",
            "cuantía a recibir",
            "family total amount",
            "valor total de la familia",
        )
        for phrase in stale_phrases:
            with self.subTest(phrase=phrase):
                self.assertNotIn(phrase, public_copy)

    def test_filter_scope_and_family_sum_warning_are_visible(self):
        self.assertIn('data-i18n="amount.filterScope"', self.index)
        self.assertIn('data-i18n="amount.familyWarning"', self.index)
        self.assertIn('"amount.filterScope"', self.i18n)
        self.assertIn('"amount.familyWarning"', self.i18n)
        self.assertIn("source-row reported amount", self.i18n.casefold())
        self.assertIn("not current contract value or actual payments", self.i18n.casefold())
        self.assertIn("no representa el valor actual del contrato ni pagos reales", self.i18n.casefold())

    def test_amount_filter_remains_source_row_scoped(self):
        self.assertRegex(
            self.db,
            re.compile(r"c\.amount\s*>=\s*\?", re.MULTILINE),
        )
        self.assertRegex(
            self.db,
            re.compile(r"c\.amount\s*<=\s*\?", re.MULTILINE),
        )
        self.assertNotRegex(self.db, r"family_total_amount\s*(?:>=|<=)\s*\?")

    def test_summary_query_retains_representative_vs_family_sum_distinction(self):
        self.assertIn("WHEN family_has_original = 1 THEN representative_amount", self.db)
        self.assertIn("ELSE family_total_amount", self.db)
        self.assertIn("SUM(family_total_amount)", self.db)

    def test_exports_name_the_unvalidated_family_row_sum(self):
        self.assertIn('t("export.familyTotalAmount")', self.ui)
        self.assertIn(
            '"export.familyTotalAmount": "Unvalidated family-row sum of reported amounts"',
            self.i18n,
        )
        self.assertIn(
            '"export.familyTotalAmount": "Suma no validada de cuantías reportadas de las filas de la familia"',
            self.i18n,
        )

    def test_internal_compatibility_keys_remain_machine_only(self):
        self.assertIn("yearly_spending", self.db)
        self.assertIn("yearly_spending", self.ui)
        self.assertIn("family_total_amount", self.db)
        self.assertNotIn("yearly_spending", self.index)
        self.assertNotIn("family_total_amount", self.index)

    def test_provenance_defines_all_three_amount_modes(self):
        required = (
            "Source-row reported amount",
            "Representative original reported amount",
            "Family-row sum",
            "not a payment",
            "not current contract value",
        )
        for phrase in required:
            with self.subTest(phrase=phrase):
                self.assertIn(phrase, self.provenance)


if __name__ == "__main__":
    unittest.main()
