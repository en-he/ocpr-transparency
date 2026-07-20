import csv
import hashlib
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = REPO_ROOT / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

import normalization  # noqa: E402


EXPECTED_OVERRIDES = {
    "AUTORIDADF FINANCIAMIENTO INFRAESTRU": (
        "contractor:autoridad-financiamiento-infraestructura-puerto-rico",
        "AUTORIDAD FINANCIAMIENTO INFRAESTRUCTURA PUERTO RICO",
    ),
    "MAGLEZ ENGINEERINGS CONTRACTORS": (
        "contractor:maglez-engineering-contractors",
        "MAGLEZ ENGINEERING CONTRACTORS",
    ),
    "CONSTRUCCIONES VIVI AGREDADO": (
        "contractor:construcciones-vivi-agregados",
        "CONSTRUCCIONES VIVI AGREGADOS",
    ),
    "CONSTRUCCIONES VIVI AGREGADO": (
        "contractor:construcciones-vivi-agregados",
        "CONSTRUCCIONES VIVI AGREGADOS",
    ),
    "CONSTRUCCIONES VIVI AGRAGADOS": (
        "contractor:construcciones-vivi-agregados",
        "CONSTRUCCIONES VIVI AGREGADOS",
    ),
    "BERMUDEZLONGODIAZ MASSO": (
        "contractor:bermudez-longo-diaz-masso",
        "BERMUDEZ LONGO DIAZ MASSO",
    ),
    "DESING BUILD": (
        "contractor:design-build",
        "DESIGN BUILD",
    ),
    "JOSEPH HARRISON FLORESDBAHARISON CONSULTING": (
        "contractor:joseph-harrison-flores",
        "JOSEPH HARRISON FLORES",
    ),
    "MUNICIPIO VIEQUES CCD": (
        "contractor:municipio-vieques",
        "MUNICIPIO VIEQUES",
    ),
    "MUNICIPIO SAN LOENZO": (
        "contractor:municipio-san-lorenzo",
        "MUNICIPIO SAN LORENZO",
    ),
    "AUTORIDAD FINANCIAMIENTO INFRAESTRUC": (
        "contractor:autoridad-financiamiento-infraestructura-puerto-rico",
        "AUTORIDAD FINANCIAMIENTO INFRAESTRUCTURA PUERTO RICO",
    ),
    "J F BUILDING LEASE MAINTENANCE": (
        "contractor:jf-building-lease-maintenance",
        "JF BUILDING LEASE MAINTENANCE",
    ),
    "ISIDRO M MARTINEZ GILORMINI": (
        "contractor:martinez-gilormini-isidro-m",
        "MARTINEZ GILORMINI ISIDRO M",
    ),
    "ADMINISTRACION COMPENSACIONES POR ACCIDENTES": (
        "contractor:administracion-compensaciones-por-accidentes-automoviles",
        "ADMINISTRACION COMPENSACIONES POR ACCIDENTES AUTOMOVILES",
    ),
    "CANCIO NADAL RIVERA": (
        "contractor:cancionadal-rivera",
        "CANCIONADAL RIVERA",
    ),
    "AQUINO CORDOVA ALFARO": (
        "contractor:aquino-cordovaalfaro",
        "AQUINO CORDOVAALFARO",
    ),
    "RICHARD SANTOS GARCIA MA": (
        "contractor:richard-santos-garciama",
        "RICHARD SANTOS GARCIAMA",
    ),
    "UNIVERSITY PUERTO RICO PARKING SYSTEM": (
        "contractor:universida-puerto-rico-parking-system",
        "UNIVERSIDA PUERTO RICO PARKING SYSTEM",
    ),
    "NAIOSCALY CRUZ PONCE": (
        "contractor:cruz-ponce-naioscaly",
        "CRUZ PONCE NAIOSCALY",
    ),
    "GIOVANY RIVERA CARRERO": (
        "contractor:rivera-carrero-giovany",
        "RIVERA CARRERO GIOVANY",
    ),
    "A1 GENERATOR SERVICES": (
        "contractor:ai-generator-services",
        "AI GENERATOR SERVICES",
    ),
    "T P CONSULTING": (
        "contractor:quantum-health-consulting",
        "QUANTUM HEALTH CONSULTING",
    ),
    "INTEGRA": (
        "contractor:integra-design-group",
        "INTEGRA DESIGN GROUP",
    ),
}


class NormalizationRegistryTests(unittest.TestCase):
    def _copy_registry(self, temporary_root):
        target = Path(temporary_root) / "data" / "normalization"
        target.parent.mkdir(parents=True)
        shutil.copytree(REPO_ROOT / "data" / "normalization", target)
        return target

    def _read_alias_rows(self, domain="contractor", root=REPO_ROOT):
        path = Path(root) / "data" / "normalization" / f"{domain.replace('_', '-')}-aliases.csv"
        with path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def test_all_twenty_three_existing_contractor_overrides_are_reviewed(self):
        rows = self._read_alias_rows()
        self.assertEqual(len(rows), 23)
        self.assertEqual(
            {(row["alias"], row["canonical_id"], row["display_label"]) for row in rows},
            {
                (alias, canonical_id, display_label)
                for alias, (canonical_id, display_label) in EXPECTED_OVERRIDES.items()
            },
        )
        self.assertTrue(all(row["review_status"] == "reviewed" for row in rows))

        for alias, (canonical_id, display_label) in EXPECTED_OVERRIDES.items():
            with self.subTest(alias=alias):
                result = normalization.normalize_value("contractor", alias)
                self.assertEqual(
                    result,
                    {
                        "raw_value": alias,
                        "alias_key": alias,
                        "canonical_id": canonical_id,
                        "display_label": display_label,
                        "status": "resolved",
                        "registry_version": "normalization-registry-1",
                    },
                )

    def test_canonical_ids_are_stable_domain_prefixed_manual_values(self):
        registry = normalization.load_registry(REPO_ROOT)
        contractor_ids = {
            row["canonical_id"] for row in self._read_alias_rows()
        }
        self.assertEqual(
            contractor_ids,
            {
                canonical_id for canonical_id, _ in EXPECTED_OVERRIDES.values()
            },
        )
        self.assertTrue(
            all(
                canonical_id.startswith("contractor:")
                and canonical_id == canonical_id.lower()
                for canonical_id in contractor_ids
            )
        )
        self.assertEqual(
            len(registry["contractor"]),
            len(EXPECTED_OVERRIDES),
        )

    def test_registry_version_payload_and_sha256_are_deterministic(self):
        first = normalization.registry_payload(REPO_ROOT)
        second = normalization.registry_payload(REPO_ROOT)
        self.assertIsInstance(first, str)
        self.assertEqual(first, second)
        self.assertEqual(
            normalization.registry_version(REPO_ROOT),
            "normalization-registry-1",
        )
        self.assertEqual(
            json.loads(first)["algorithm_version"],
            "lookup-v1",
        )
        self.assertEqual(
            hashlib.sha256(first.encode("utf-8")).hexdigest(),
            hashlib.sha256(second.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(len(hashlib.sha256(first.encode("utf-8")).hexdigest()), 64)
        self.assertNotIn(str(REPO_ROOT), first)
        self.assertNotIn("timestamp", first.lower())

    def test_missing_and_unresolved_values_preserve_raw_input_without_candidates(self):
        for raw_value in (None, "", " \t"):
            with self.subTest(raw_value=raw_value):
                result = normalization.normalize_value("contractor", raw_value)
                self.assertEqual(result["raw_value"], raw_value)
                self.assertEqual(result["alias_key"], "")
                self.assertEqual(result["canonical_id"], None)
                self.assertEqual(result["display_label"], None)
                self.assertEqual(result["status"], "missing")

        unresolved = normalization.normalize_value(
            "contractor", "A contractor not in the reviewed registry"
        )
        self.assertEqual(unresolved["raw_value"], "A contractor not in the reviewed registry")
        self.assertEqual(
            unresolved["alias_key"],
            "A CONTRACTOR NOT IN THE REVIEWED REGISTRY",
        )
        self.assertEqual(unresolved["status"], "unresolved")
        self.assertIsNone(unresolved["canonical_id"])
        self.assertIsNone(unresolved["display_label"])
        self.assertEqual(
            set(unresolved),
            {
                "raw_value",
                "alias_key",
                "canonical_id",
                "display_label",
                "status",
                "registry_version",
            },
        )
        self.assertFalse(any("score" in key or "candidate" in key for key in unresolved))

    def test_alias_key_strips_accents_normalizes_punctuation_and_whitespace(self):
        cases = (
            (
                "  construcciones\t vivi   agréGADO!! ",
                "CONSTRUCCIONES VIVI AGREGADO",
            ),
            (
                "T\t& P—CONSULTING",
                "T P CONSULTING",
            ),
            (
                "  J.F.\u00a0BUILDING / LEASE & MAINTENANCE  ",
                "J F BUILDING LEASE MAINTENANCE",
            ),
        )
        for raw_value, expected_key in cases:
            with self.subTest(raw_value=raw_value):
                result = normalization.normalize_value("contractor", raw_value)
                self.assertEqual(result["alias_key"], expected_key)
                self.assertEqual(result["status"], "resolved")

    def test_high_risk_overrides_are_retained_review_decisions_not_new_rows(self):
        path = REPO_ROOT / "data" / "normalization" / "review-decisions.csv"
        with path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        by_raw = {row["raw_value"]: row for row in rows}
        self.assertEqual(set(by_raw), {"T P CONSULTING", "INTEGRA"})
        self.assertEqual(by_raw["T P CONSULTING"]["decision"], "retained")
        self.assertEqual(by_raw["INTEGRA"]["decision"], "retained")
        self.assertEqual(
            by_raw["T P CONSULTING"]["canonical_id"],
            EXPECTED_OVERRIDES["T P CONSULTING"][0],
        )
        self.assertEqual(
            by_raw["INTEGRA"]["canonical_id"],
            EXPECTED_OVERRIDES["INTEGRA"][0],
        )
        self.assertNotIn("candidate", " ".join(path.read_text(encoding="utf-8").splitlines()[0].lower()))

    def test_other_domain_alias_files_are_header_only(self):
        expected_header = "alias,canonical_id,display_label,review_status\n"
        for domain in ("entity", "service_category", "service_type"):
            with self.subTest(domain=domain):
                path = REPO_ROOT / "data" / "normalization" / f"{domain.replace('_', '-')}-aliases.csv"
                self.assertEqual(path.read_text(encoding="utf-8"), expected_header)

    def test_contradictory_aliases_fail_closed_as_collision(self):
        with tempfile.TemporaryDirectory() as temporary_root:
            alias_dir = self._copy_registry(temporary_root)
            path = alias_dir / "contractor-aliases.csv"
            with path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            rows.append(
                {
                    "alias": "T P CONSULTING",
                    "canonical_id": "contractor:conflicting-example",
                    "display_label": "CONFLICTING EXAMPLE",
                    "review_status": "reviewed",
                }
            )
            rows.sort(
                key=lambda row: (
                    normalization.normalize_alias_key(row["alias"]),
                    row["canonical_id"],
                    row["display_label"],
                    row["review_status"],
                )
            )
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["alias", "canonical_id", "display_label", "review_status"],
                    lineterminator="\n",
                )
                writer.writeheader()
                writer.writerows(rows)

            result = normalization.normalize_value(
                "contractor", "T & P consulting", repo_root=temporary_root
            )
            self.assertEqual(result["alias_key"], "T P CONSULTING")
            self.assertEqual(result["status"], "collision")
            self.assertIsNone(result["canonical_id"])
            self.assertIsNone(result["display_label"])

    def test_identical_duplicate_alias_rows_are_harmless(self):
        baseline_payload = normalization.registry_payload(REPO_ROOT)
        with tempfile.TemporaryDirectory() as temporary_root:
            alias_dir = self._copy_registry(temporary_root)
            path = alias_dir / "contractor-aliases.csv"
            with path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            rows.append(dict(rows[0]))
            rows.sort(
                key=lambda row: (
                    normalization.normalize_alias_key(row["alias"]),
                    row["canonical_id"],
                    row["display_label"],
                    row["review_status"],
                )
            )
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["alias", "canonical_id", "display_label", "review_status"],
                    lineterminator="\n",
                )
                writer.writeheader()
                writer.writerows(rows)

            result = normalization.normalize_value(
                "contractor", rows[0]["alias"], repo_root=temporary_root
            )
            self.assertEqual(result["status"], "resolved")
            self.assertEqual(
                normalization.registry_payload(temporary_root),
                baseline_payload,
            )

    def test_loader_validates_required_files_headers_domains_order_and_ids(self):
        with tempfile.TemporaryDirectory() as temporary_root:
            alias_dir = self._copy_registry(temporary_root)
            (alias_dir / "entity-aliases.csv").unlink()
            with self.assertRaises(normalization.RegistryError):
                normalization.load_registry(temporary_root)

        with tempfile.TemporaryDirectory() as temporary_root:
            alias_dir = self._copy_registry(temporary_root)
            schema_path = alias_dir / "schema-profiles.json"
            profile = json.loads(schema_path.read_text(encoding="utf-8"))
            profile["domains"]["unknown"] = profile["domains"].pop("entity")
            schema_path.write_text(
                json.dumps(profile, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(normalization.RegistryError):
                normalization.load_registry(temporary_root)

        with tempfile.TemporaryDirectory() as temporary_root:
            alias_dir = self._copy_registry(temporary_root)
            path = alias_dir / "contractor-aliases.csv"
            with path.open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            rows[0]["canonical_id"] = "not-domain-prefixed"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=["alias", "canonical_id", "display_label", "review_status"],
                    lineterminator="\n",
                )
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaises(normalization.RegistryError):
                normalization.load_registry(temporary_root)


if __name__ == "__main__":
    unittest.main()
