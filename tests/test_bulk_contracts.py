import csv
import hashlib
import importlib
import sys
import tempfile
import unittest
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parents[1] / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "bulk"

ENCODING = "latin-1"

# Exact raw header lines (byte-exact, no space after the comma delimiter),
# matching data/raw/contratos_<fiscal-year>.csv bytes for each known profile.
V1_HEADER_LINE = (
    "Número de Entidad,Entidad,Número de Contrato,Enmienda,Otorgado En,"
    "Vigencia Desde,Vigencia Hasta,Tipo de Servicio,Categoría de Servicio,"
    "Cancelado,Cuantía,Contratista"
)
V2_HEADER_LINE = (
    "Número de Entidad,Entidad,Núm. Contrato,Enmienda,Otorgado en,"
    "Vigencia Desde,Vigencia Hasta,Tipo de Servicio,Categoría de Servicio,"
    "Cancelado,Cuantía,Contratista"
)
V3_HEADER_LINE = (
    "Número de Entidad,Entidad,Núm. Contrato,Enmienda,Otorgado en,"
    "Vigencia Desde,Vigencia Hasta,Tipo de Servicio,Categoría de Servicio,"
    "Cancelado,Cuantía,Cuantía a Recibir,Contratista"
)

V1_FIELD_COUNT = 12
V2_FIELD_COUNT = 12
V3_FIELD_COUNT = 13


def _require_pipeline_module(name):
    """Import a not-yet-implemented pipeline module, failing (not erroring
    at collection time) with a clear reason when it is missing."""
    try:
        return importlib.import_module(name)
    except ImportError as exc:
        raise AssertionError(
            f"pipeline/{name}.py is not implemented yet: {exc}"
        ) from exc


def _read_fixture_bytes(filename):
    return (FIXTURES_DIR / filename).read_bytes()


def _read_fixture_lines(filename):
    data = _read_fixture_bytes(filename)
    text = data.decode(ENCODING)
    # Fixtures use CRLF line endings; drop the trailing blank line from the
    # final terminator.
    lines = text.split("\r\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def _csv_fields(line):
    return next(csv.reader([line]))


class FixtureEncodingTests(unittest.TestCase):
    """Mechanical validation: every fixture decodes as Latin-1 and is not
    valid UTF-8 (mirroring the actual data/raw/*.csv encoding)."""

    # The five fixtures the fixture manifest documents as simulating known
    # preserved-corpus shapes, plus three synthetic file-level fail-closed
    # fixtures (duplicate/missing/unknown header) used only by the
    # production-contract RED tests below.
    FIXTURE_FILES = [
        "ocpr-bulk-v1.csv",
        "ocpr-bulk-v2.csv",
        "ocpr-bulk-v3.csv",
        "cancellation-values.csv",
        "malformed-shifted-row.csv",
        "duplicate-header.csv",
        "missing-required-header.csv",
        "unknown-header.csv",
    ]

    def test_all_fixtures_exist(self):
        for filename in self.FIXTURE_FILES:
            with self.subTest(filename=filename):
                self.assertTrue(
                    (FIXTURES_DIR / filename).is_file(),
                    f"missing fixture {filename}",
                )

    def test_all_fixtures_decode_as_latin_1(self):
        for filename in self.FIXTURE_FILES:
            with self.subTest(filename=filename):
                data = _read_fixture_bytes(filename)
                # Must not raise.
                data.decode(ENCODING)

    def test_header_row_is_not_valid_utf8(self):
        # Confirms Latin-1 is required, not merely sufficient: the accented
        # header bytes (e.g. 0xFA "ú", 0xED "í") are not valid UTF-8.
        for filename in self.FIXTURE_FILES:
            with self.subTest(filename=filename):
                header_bytes = _read_fixture_bytes(filename).split(b"\r\n")[0]
                with self.assertRaises(UnicodeDecodeError):
                    header_bytes.decode("utf-8")


class FixtureHeaderProfileTests(unittest.TestCase):
    """Mechanical validation of exact header bytes and field counts per
    known profile, independent of any production parser."""

    def test_v1_header_matches_exact_raw_bytes(self):
        lines = _read_fixture_lines("ocpr-bulk-v1.csv")
        self.assertEqual(lines[0], V1_HEADER_LINE)
        self.assertEqual(len(_csv_fields(lines[0])), V1_FIELD_COUNT)

    def test_v2_header_matches_exact_raw_bytes(self):
        lines = _read_fixture_lines("ocpr-bulk-v2.csv")
        self.assertEqual(lines[0], V2_HEADER_LINE)
        self.assertEqual(len(_csv_fields(lines[0])), V2_FIELD_COUNT)

    def test_v3_header_matches_exact_raw_bytes(self):
        lines = _read_fixture_lines("ocpr-bulk-v3.csv")
        self.assertEqual(lines[0], V3_HEADER_LINE)
        self.assertEqual(len(_csv_fields(lines[0])), V3_FIELD_COUNT)

    def test_cancellation_values_fixture_uses_v1_shaped_header(self):
        lines = _read_fixture_lines("cancellation-values.csv")
        self.assertEqual(lines[0], V1_HEADER_LINE)

    def test_malformed_shifted_row_fixture_uses_v3_shaped_header(self):
        lines = _read_fixture_lines("malformed-shifted-row.csv")
        self.assertEqual(lines[0], V3_HEADER_LINE)

    def test_v1_and_v2_headers_omit_cuantia_a_recibir(self):
        self.assertNotIn("Cuantía a Recibir", V1_HEADER_LINE)
        self.assertNotIn("Cuantía a Recibir", V2_HEADER_LINE)

    def test_v3_header_includes_cuantia_a_recibir(self):
        self.assertIn("Cuantía a Recibir", V3_HEADER_LINE)

    def test_v1_uses_capital_otorgado_en_v2_v3_use_lowercase(self):
        self.assertIn("Otorgado En,", V1_HEADER_LINE)
        self.assertIn("Otorgado en,", V2_HEADER_LINE)
        self.assertIn("Otorgado en,", V3_HEADER_LINE)

    def test_no_bulk_header_line_contains_compatibility_field_names(self):
        # procurement_method/fund_type/pco_number/document_url must never
        # appear as bulk CSV headers in any known profile.
        compatibility_markers = (
            "procurement_method",
            "fund_type",
            "pco_number",
            "document_url",
        )
        for header_line in (V1_HEADER_LINE, V2_HEADER_LINE, V3_HEADER_LINE):
            for marker in compatibility_markers:
                with self.subTest(header=header_line, marker=marker):
                    self.assertNotIn(marker, header_line)


class FixtureCancelacionRawValueTests(unittest.TestCase):
    """Mechanical validation of the Cancelado raw-value fixture."""

    def setUp(self):
        self.lines = _read_fixture_lines("cancellation-values.csv")
        self.header_fields = _csv_fields(self.lines[0])
        self.cancelado_index = self.header_fields.index("Cancelado")
        self.enmienda_index = self.header_fields.index("Enmienda")

    def test_first_data_row_has_nul_enmienda_and_nul_cancelado(self):
        fields = _csv_fields(self.lines[1])
        self.assertEqual(fields[self.enmienda_index], "\x00")
        self.assertEqual(fields[self.cancelado_index], "\x00")

    def test_second_data_row_has_amendment_a_and_dated_cancelado(self):
        fields = _csv_fields(self.lines[2])
        self.assertEqual(fields[self.enmienda_index], "A")
        self.assertEqual(fields[self.cancelado_index], "09-30-2011")

    def test_both_rows_share_contract_number(self):
        contract_index = self.header_fields.index("Número de Contrato")
        row1 = _csv_fields(self.lines[1])
        row2 = _csv_fields(self.lines[2])
        self.assertEqual(row1[contract_index], "1995-000444")
        self.assertEqual(row2[contract_index], "1995-000444")


class FixtureAmbiguousDateTests(unittest.TestCase):
    """Mechanical validation of the v3 date-classification fixture rows.

    Corpus-wide evidence (all 13 preserved fiscal-year files, 1,232,110 rows)
    establishes MM-DD-YYYY with a four-digit year as the certified per-profile
    convention: the corpus contains many day components > 12, which is only
    possible if the first component is the month. A four-digit-year value is
    therefore resolved by profile even when both components are <= 12 (e.g.
    `05-06-2014` is unambiguously May 6, 2014) — it is never quarantined on
    that basis alone. Only a value the profile cannot resolve at all — a
    two-digit year, where the century is undeterminable — is genuinely
    ambiguous.
    """

    DATE_FIELDS = ("Otorgado en", "Vigencia Desde", "Vigencia Hasta")

    def setUp(self):
        self.lines = _read_fixture_lines("ocpr-bulk-v3.csv")
        self.header_fields = _csv_fields(self.lines[0])
        self.date_indexes = {
            name: self.header_fields.index(name) for name in self.DATE_FIELDS
        }

    @staticmethod
    def _is_profile_certified_date(value):
        # MM-DD-YYYY with a four-digit year is certified by profile,
        # regardless of whether both numeric components are <= 12.
        parts = value.split("-")
        if len(parts) != 3:
            return False
        month, day, year = parts
        if len(year) != 4:
            return False
        return 1 <= int(month) <= 12 and 1 <= int(day) <= 31

    @staticmethod
    def _is_unsupported_ambiguous_date(value):
        # Genuinely ambiguous only when the profile cannot resolve it at
        # all, e.g. a two-digit year (century unresolvable by profile).
        parts = value.split("-")
        if len(parts) != 3:
            return True
        _month, _day, year = parts
        return len(year) != 4

    def test_corpus_evidence_four_digit_both_components_le_12_is_certified_not_ambiguous(
        self,
    ):
        # Direct check of the corpus-evidence example from
        # docs/project/bulk-certification.md's "Date field parsing" section.
        value = "05-06-2014"
        self.assertTrue(self._is_profile_certified_date(value))
        self.assertFalse(self._is_unsupported_ambiguous_date(value))

    def test_original_row_has_all_date_fields_profile_certified(self):
        fields = _csv_fields(self.lines[1])
        self.assertEqual(fields[self.date_indexes["Otorgado en"]], "02-15-2014")
        self.assertEqual(fields[self.date_indexes["Vigencia Desde"]], "02-15-2014")
        self.assertEqual(fields[self.date_indexes["Vigencia Hasta"]], "01-31-2015")
        for name in self.DATE_FIELDS:
            with self.subTest(field=name):
                value = fields[self.date_indexes[name]]
                self.assertTrue(self._is_profile_certified_date(value))
                self.assertFalse(self._is_unsupported_ambiguous_date(value))

    def test_amendment_a_row_has_exactly_one_unsupported_ambiguous_date_field(self):
        fields = _csv_fields(self.lines[2])
        self.assertEqual(fields[self.date_indexes["Otorgado en"]], "05-06-14")
        self.assertEqual(fields[self.date_indexes["Vigencia Desde"]], "05-20-2014")
        self.assertEqual(fields[self.date_indexes["Vigencia Hasta"]], "01-31-2015")
        ambiguous_fields = [
            name
            for name in self.DATE_FIELDS
            if self._is_unsupported_ambiguous_date(fields[self.date_indexes[name]])
        ]
        self.assertEqual(ambiguous_fields, ["Otorgado en"])
        certified_fields = [
            name
            for name in self.DATE_FIELDS
            if self._is_profile_certified_date(fields[self.date_indexes[name]])
        ]
        self.assertEqual(certified_fields, ["Vigencia Desde", "Vigencia Hasta"])


class FixtureShiftedRowTests(unittest.TestCase):
    """Mechanical validation of the shifted-row (long/short) fixture."""

    def setUp(self):
        self.lines = _read_fixture_lines("malformed-shifted-row.csv")
        self.header_fields = _csv_fields(self.lines[0])

    def test_header_has_thirteen_fields(self):
        self.assertEqual(len(self.header_fields), V3_FIELD_COUNT)

    def test_first_row_is_well_formed(self):
        fields = _csv_fields(self.lines[1])
        self.assertEqual(len(fields), V3_FIELD_COUNT)

    def test_second_row_is_shifted_long_from_unquoted_embedded_comma(self):
        fields = _csv_fields(self.lines[2])
        self.assertEqual(len(fields), 14)
        self.assertNotEqual(len(fields), len(self.header_fields))

    def test_third_row_is_shifted_short_missing_trailing_field(self):
        fields = _csv_fields(self.lines[3])
        self.assertEqual(len(fields), 12)
        self.assertNotEqual(len(fields), len(self.header_fields))

    def test_all_three_rows_share_contract_number(self):
        contract_index = self.header_fields.index("Núm. Contrato")
        for line_index in (1, 2, 3):
            with self.subTest(line_index=line_index):
                fields = _csv_fields(self.lines[line_index])
                self.assertEqual(fields[contract_index], "1996-000555")


class FixtureFailClosedHeaderShapeTests(unittest.TestCase):
    """Mechanical validation of the synthetic file-level fail-closed
    fixtures (duplicate/missing/unknown header), independent of any
    production parser."""

    def test_duplicate_header_fixture_has_a_repeated_header_string(self):
        lines = _read_fixture_lines("duplicate-header.csv")
        fields = _csv_fields(lines[0])
        self.assertEqual(len(fields), len(set(fields)) + 1)
        self.assertEqual(fields.count("Entidad"), 2)

    def test_missing_required_header_fixture_omits_contratista(self):
        lines = _read_fixture_lines("missing-required-header.csv")
        fields = _csv_fields(lines[0])
        self.assertNotIn("Contratista", fields)
        # Every remaining header belongs to the v1 header set.
        self.assertTrue(set(fields).issubset(set(_csv_fields(V1_HEADER_LINE))))

    def test_unknown_header_fixture_has_a_foreign_header_string(self):
        lines = _read_fixture_lines("unknown-header.csv")
        fields = _csv_fields(lines[0])
        known_union = (
            set(_csv_fields(V1_HEADER_LINE))
            | set(_csv_fields(V2_HEADER_LINE))
            | set(_csv_fields(V3_HEADER_LINE))
        )
        self.assertFalse(set(fields).issubset(known_union))
        self.assertIn("Nota Adicional", fields)


# ---------------------------------------------------------------------------
# Production API contract (pipeline/bulk_manifest.py, pipeline/certify_bulk.py).
# These tests exercise the implementation frozen by
# docs/project/bulk-certification.md.
# ---------------------------------------------------------------------------


class BulkManifestHeaderProfileContractTests(unittest.TestCase):
    def test_source_and_capture_status_literals_are_closed_sets(self):
        bulk_manifest = _require_pipeline_module("bulk_manifest")
        self.assertEqual(
            set(bulk_manifest.SOURCE_CHANNELS),
            {"official_bulk", "archive_bulk"},
        )
        self.assertEqual(
            set(bulk_manifest.CAPTURE_TIME_STATUSES),
            {"observed", "git_first_seen", "unknown"},
        )

    def test_detect_header_profile_recognizes_v1_exactly(self):
        bulk_manifest = _require_pipeline_module("bulk_manifest")
        headers = _csv_fields(V1_HEADER_LINE)
        self.assertEqual(bulk_manifest.detect_header_profile(headers), "v1")

    def test_detect_header_profile_recognizes_v2_exactly(self):
        bulk_manifest = _require_pipeline_module("bulk_manifest")
        headers = _csv_fields(V2_HEADER_LINE)
        self.assertEqual(bulk_manifest.detect_header_profile(headers), "v2")

    def test_detect_header_profile_recognizes_v3_exactly(self):
        bulk_manifest = _require_pipeline_module("bulk_manifest")
        headers = _csv_fields(V3_HEADER_LINE)
        self.assertEqual(bulk_manifest.detect_header_profile(headers), "v3")

    def test_exact_profile_inventory_years_and_order_independent_detection(self):
        bulk_manifest = _require_pipeline_module("bulk_manifest")
        self.assertEqual(set(bulk_manifest.HEADER_PROFILES), {"v1", "v2", "v3"})
        self.assertEqual(
            bulk_manifest.HEADER_PROFILE_FISCAL_YEARS,
            {
                "v1": ("2010-2011", "2011-2012"),
                "v2": ("2012-2013",),
                "v3": None,
            },
        )
        for profile, headers in bulk_manifest.HEADER_PROFILES.items():
            with self.subTest(profile=profile):
                self.assertEqual(
                    bulk_manifest.detect_header_profile(tuple(reversed(headers))),
                    profile,
                )

    def test_header_fingerprint_is_order_sensitive_and_byte_exact(self):
        bulk_manifest = _require_pipeline_module("bulk_manifest")
        headers = _csv_fields(V1_HEADER_LINE)
        reordered = list(reversed(headers))
        self.assertNotEqual(
            bulk_manifest.header_fingerprint(headers),
            bulk_manifest.header_fingerprint(reordered),
        )
        self.assertEqual(
            bulk_manifest.header_fingerprint(headers),
            hashlib.sha256(V1_HEADER_LINE.encode(ENCODING)).hexdigest(),
        )

    def test_compatibility_nullable_fields_are_not_required_bulk_headers(self):
        bulk_manifest = _require_pipeline_module("bulk_manifest")
        self.assertEqual(
            set(bulk_manifest.COMPATIBILITY_NULLABLE_FIELDS),
            {"procurement_method", "fund_type", "pco_number", "document_url"},
        )
        for profile_headers in bulk_manifest.HEADER_PROFILES.values():
            for field in bulk_manifest.COMPATIBILITY_NULLABLE_FIELDS:
                self.assertNotIn(field, profile_headers)


class CertifyBulkFileLevelFailClosedTests(unittest.TestCase):
    def test_duplicate_header_fails_closed_before_profile_matching(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        with self.assertRaises(certify_bulk.DuplicateHeaderError):
            certify_bulk.certify_bulk_file(
                FIXTURES_DIR / "duplicate-header.csv",
                source_channel="official_bulk",
            )

    def test_missing_required_header_fails_closed_as_strict_subset(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        with self.assertRaises(certify_bulk.MissingRequiredHeaderError):
            certify_bulk.certify_bulk_file(
                FIXTURES_DIR / "missing-required-header.csv",
                source_channel="official_bulk",
            )

    def test_unknown_header_profile_fails_closed(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        with self.assertRaises(certify_bulk.UnknownHeaderProfileError):
            certify_bulk.certify_bulk_file(
                FIXTURES_DIR / "unknown-header.csv",
                source_channel="official_bulk",
            )

    def test_v1_fixture_certifies_with_v1_profile_and_no_cuantia_a_recibir(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="official_bulk",
            fiscal_year="2010-2011",
        )
        self.assertEqual(report.header_profile, "v1")
        self.assertEqual(report.verdict, "certified")


class CertifyBulkRowLevelQuarantineTests(unittest.TestCase):
    def test_shifted_long_row_is_quarantined_with_raw_fields_preserved(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "malformed-shifted-row.csv",
            source_channel="official_bulk",
        )
        self.assertEqual(report.verdict, "certified_with_quarantine")
        shifted = [
            outcome
            for outcome in report.row_outcomes
            if outcome.status == "quarantined" and outcome.reason == "shifted_row"
        ]
        self.assertEqual(len(shifted), 2)
        self.assertEqual(len(shifted[0].raw_fields), 14)
        self.assertEqual(shifted[0].row_number, 3)
        self.assertEqual(
            shifted[0].raw_fields[12:],
            ("CONTRATISTA EJEMPLO SEIS", " INC"),
        )

    def test_shifted_short_row_is_quarantined_with_raw_fields_preserved(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "malformed-shifted-row.csv",
            source_channel="official_bulk",
        )
        shifted = [
            outcome
            for outcome in report.row_outcomes
            if outcome.status == "quarantined" and outcome.reason == "shifted_row"
        ]
        self.assertEqual(len(shifted[1].raw_fields), 12)
        self.assertEqual(shifted[1].row_number, 4)
        self.assertEqual(shifted[1].raw_fields[-1], "0.00")

    def test_two_digit_year_otorgado_en_is_quarantined_as_ambiguous_raw_preserved(self):
        # Only the two-digit-year value (05-06-14) is genuinely ambiguous; a
        # four-digit-year value with both components <= 12 is certified by
        # profile (see test_four_digit_year_date_with_both_components_le_12_is_certified
        # below), so it must not also land in this quarantine bucket.
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v3.csv",
            source_channel="official_bulk",
        )
        self.assertEqual(report.verdict, "certified_with_quarantine")
        self.assertEqual(report.rows_certified, 1)
        self.assertEqual(report.rows_quarantined, 1)
        ambiguous = [
            outcome
            for outcome in report.row_outcomes
            if outcome.status == "quarantined" and outcome.reason == "ambiguous_date"
        ]
        self.assertEqual(len(ambiguous), 1)
        header_fields = _csv_fields(V3_HEADER_LINE)
        otorgado_en_index = header_fields.index("Otorgado en")
        self.assertEqual(ambiguous[0].raw_fields[otorgado_en_index], "05-06-14")

    def test_four_digit_year_date_with_both_components_le_12_is_certified(self):
        # Corpus evidence: MM-DD-YYYY with a four-digit year is certified per
        # profile even when both month and day are <= 12 (e.g. 05-06-2014 is
        # unambiguously May 6, 2014). Build a one-row synthetic v3 fixture
        # from the checked-in v3 fixture's own header/row shape, in a
        # TemporaryDirectory so the on-disk fixture set is untouched.
        certify_bulk = _require_pipeline_module("certify_bulk")
        base_lines = _read_fixture_lines("ocpr-bulk-v3.csv")
        header_fields = _csv_fields(base_lines[0])
        otorgado_en_index = header_fields.index("Otorgado en")

        original_row = base_lines[1]
        original_fields = _csv_fields(original_row)
        self.assertEqual(original_fields[otorgado_en_index], "02-15-2014")

        certified_row = original_row.replace(",02-15-2014,", ",05-06-2014,", 1)
        self.assertNotEqual(certified_row, original_row)

        content = "\r\n".join([base_lines[0], certified_row]) + "\r\n"

        with tempfile.TemporaryDirectory() as tmp_dir:
            fixture_path = Path(tmp_dir) / "synthetic-certified-date.csv"
            fixture_path.write_bytes(content.encode(ENCODING))

            report = certify_bulk.certify_bulk_file(
                fixture_path,
                source_channel="official_bulk",
            )

        self.assertEqual(report.verdict, "certified")
        self.assertEqual(report.rows_certified, 1)
        self.assertEqual(report.rows_quarantined, 0)
        self.assertFalse(
            [
                outcome
                for outcome in report.row_outcomes
                if outcome.status == "quarantined"
            ]
        )

    def test_cancelado_nul_and_dated_values_preserved_raw_never_boolean(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "cancellation-values.csv",
            source_channel="official_bulk",
        )
        raw_cancelado_values = {
            outcome.raw_fields[9] for outcome in report.row_outcomes
        }
        self.assertIn("\x00", raw_cancelado_values)
        self.assertIn("09-30-2011", raw_cancelado_values)
        for value in raw_cancelado_values:
            self.assertNotIsInstance(value, bool)

    def test_malformed_scalar_is_quarantined_not_silently_nulled(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        # A malformed amount is neither the NUL blank marker nor a parseable
        # number; it must be quarantined with the raw value preserved, never
        # coerced to NULL/0 without a flag. None of the checked-in fixtures
        # contain a malformed amount, so build a deterministic synthetic one
        # from the known v1 profile fixture, in a TemporaryDirectory so the
        # on-disk fixture set is untouched.
        base_lines = _read_fixture_lines("ocpr-bulk-v1.csv")
        header_fields = _csv_fields(base_lines[0])
        cuantia_index = header_fields.index("Cuantía")
        contract_index = header_fields.index("Número de Contrato")
        contratista_index = header_fields.index("Contratista")

        original_row = base_lines[1]
        original_fields = _csv_fields(original_row)
        self.assertEqual(original_fields[cuantia_index], "1500.00")

        malformed_amount = "ILEGIBLE"
        malformed_row = original_row.replace(
            ",1500.00,", f",{malformed_amount},", 1
        )
        self.assertNotEqual(malformed_row, original_row)

        content = "\r\n".join([base_lines[0], malformed_row] + base_lines[2:]) + "\r\n"

        with tempfile.TemporaryDirectory() as tmp_dir:
            fixture_path = Path(tmp_dir) / "synthetic-malformed-amount.csv"
            fixture_path.write_bytes(content.encode(ENCODING))

            report = certify_bulk.certify_bulk_file(
                fixture_path,
                source_channel="official_bulk",
            )

        self.assertEqual(report.verdict, "certified_with_quarantine")
        malformed = [
            outcome
            for outcome in report.row_outcomes
            if outcome.status == "quarantined" and outcome.reason == "malformed_amount"
        ]
        self.assertEqual(len(malformed), 1)
        self.assertIsNotNone(malformed[0].raw_fields)
        self.assertEqual(malformed[0].raw_fields[cuantia_index], malformed_amount)
        self.assertEqual(
            malformed[0].raw_fields[contract_index],
            original_fields[contract_index],
        )
        self.assertEqual(
            malformed[0].raw_fields[contratista_index],
            original_fields[contratista_index],
        )


class CertifyBulkDeterminismTests(unittest.TestCase):
    def test_report_preserves_caller_supplied_evidence_metadata(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="archive_bulk",
            fiscal_year="2010-2011",
            source_url="https://example.test/source",
            archive_url="https://example.test/archive",
            capture_time="2026-04-04T20:51:08-04:00",
            capture_time_status="git_first_seen",
            http_status=200,
            content_type="text/csv",
        )
        self.assertEqual(report.fiscal_year, "2010-2011")
        self.assertEqual(report.source_channel, "archive_bulk")
        self.assertEqual(report.source_url, "https://example.test/source")
        self.assertEqual(report.archive_url, "https://example.test/archive")
        self.assertEqual(report.capture_time, "2026-04-04T20:51:08-04:00")
        self.assertEqual(report.capture_time_status, "git_first_seen")
        self.assertEqual(report.http_status, 200)
        self.assertEqual(report.content_type, "text/csv")
        self.assertEqual(report.encoding, "latin-1")
        self.assertTrue(report.parser_version)
        self.assertTrue(report.normalizer_version)
        self.assertEqual(report.byte_length, len(_read_fixture_bytes("ocpr-bulk-v1.csv")))

    def test_absent_capture_time_remains_none_and_unknown(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="official_bulk",
            fiscal_year="2010-2011",
        )
        self.assertIsNone(report.capture_time)
        self.assertEqual(report.capture_time_status, "unknown")

    def test_report_hash_excludes_certified_at(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report_a = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="official_bulk",
            fiscal_year="2010-2011",
        )
        report_b = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="official_bulk",
            fiscal_year="2010-2011",
        )
        report_b.certified_at = "1900-01-01T00:00:00+00:00"
        self.assertNotEqual(report_a.certified_at, report_b.certified_at)
        self.assertEqual(
            certify_bulk.report_hash(report_a),
            certify_bulk.report_hash(report_b),
        )

    def test_same_bytes_different_source_channel_yields_distinct_report_hash(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        official = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="official_bulk",
            fiscal_year="2010-2011",
        )
        archived = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="archive_bulk",
            fiscal_year="2010-2011",
        )
        self.assertNotEqual(
            certify_bulk.report_hash(official),
            certify_bulk.report_hash(archived),
        )

    def test_capture_time_value_and_status_affect_logical_report_hash(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        unknown = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="official_bulk",
            fiscal_year="2010-2011",
        )
        first_seen = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="official_bulk",
            fiscal_year="2010-2011",
            capture_time="2026-04-04T20:51:08-04:00",
            capture_time_status="git_first_seen",
        )
        self.assertNotEqual(
            certify_bulk.report_hash(unknown),
            certify_bulk.report_hash(first_seen),
        )

    def test_sha256_matches_exact_preserved_bytes(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v1.csv",
            source_channel="official_bulk",
            fiscal_year="2010-2011",
        )
        expected = hashlib.sha256(
            _read_fixture_bytes("ocpr-bulk-v1.csv")
        ).hexdigest()
        self.assertEqual(report.sha256, expected)


class CertifyBulkRemediationContractTests(unittest.TestCase):
    def _write_bytes(self, directory, filename, data):
        path = Path(directory) / filename
        path.write_bytes(data)
        return path

    def _first_v1_record(self):
        lines = _read_fixture_bytes("ocpr-bulk-v1.csv").split(b"\r\n")
        return lines[0], lines[1]

    def test_v3_doubled_outer_wrappers_parse_cleanly_and_preserve_raw_record(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "ocpr-bulk-v3.csv",
            source_channel="official_bulk",
            fiscal_year="2013-2014",
        )
        outcome = next(item for item in report.row_outcomes if item.row_number == 2)
        self.assertEqual(outcome.status, "certified")
        self.assertEqual(outcome.raw_fields[1], "Municipio de Ejemplo Tres")
        self.assertEqual(outcome.raw_fields[7], "VIVIENDAS")
        self.assertEqual(
            outcome.raw_fields[8],
            "COMPRA VENTA ALQUILER Y/O DESARROLLO DE INMUEBLES",
        )
        self.assertEqual(outcome.raw_fields[12], "CONTRATISTA EJEMPLO TRES")
        self.assertIn('""Municipio de Ejemplo Tres""', outcome.raw_record)
        self.assertEqual(
            outcome.raw_record_sha256,
            hashlib.sha256(outcome.raw_record.encode("latin-1")).hexdigest(),
        )

    def test_exact_duplicate_rows_are_counted_without_becoming_canonical_claims(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        header, record = self._first_v1_record()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_bytes(
                tmpdir,
                "duplicate-row.csv",
                b"\r\n".join((header, record, record, b"")),
            )
            report = certify_bulk.certify_bulk_file(
                path,
                source_channel="official_bulk",
                fiscal_year="2010-2011",
            )
        self.assertEqual(report.rows_total, 2)
        self.assertEqual(report.rows_certified, 2)
        self.assertEqual(report.duplicate_count, 1)
        self.assertEqual(report.source_unique_contribution_count, 1)
        self.assertFalse(hasattr(report, "canonical_contribution_count"))

    def test_row_numbers_include_header_as_source_record_one(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        report = certify_bulk.certify_bulk_file(
            FIXTURES_DIR / "malformed-shifted-row.csv",
            source_channel="official_bulk",
        )
        shifted_numbers = [
            item.row_number
            for item in report.row_outcomes
            if item.reason == "shifted_row"
        ]
        self.assertEqual(shifted_numbers, [3, 4])

    def test_empty_file_fails_closed_with_named_error(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_bytes(tmpdir, "empty.csv", b"")
            with self.assertRaises(certify_bulk.EmptyBulkFileError):
                certify_bulk.certify_bulk_file(
                    path,
                    source_channel="official_bulk",
                )

    def test_lf_only_file_is_parsed_as_records_not_one_giant_header(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        data = _read_fixture_bytes("ocpr-bulk-v1.csv").replace(b"\r\n", b"\n")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = self._write_bytes(tmpdir, "lf-only.csv", data)
            report = certify_bulk.certify_bulk_file(
                path,
                source_channel="archive_bulk",
                fiscal_year="2010-2011",
            )
        self.assertEqual(report.header_profile, "v1")
        self.assertEqual(report.rows_total, 2)

    def test_quoted_multiline_record_fails_closed_instead_of_fragmenting(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        header, record = self._first_v1_record()
        for embedded in (
            b'"Autoridad de Ejemplo\r\nde Puerto Rico"',
            b'"Autoridad de Ejemplo\r\nintermedia\r\nde Puerto Rico"',
        ):
            with self.subTest(embedded_newlines=embedded.count(b"\r\n")), tempfile.TemporaryDirectory() as tmpdir:
                multiline_record = record.replace(
                    b'"Autoridad de Ejemplo de Puerto Rico"',
                    embedded,
                )
                path = self._write_bytes(
                    tmpdir,
                    "multiline.csv",
                    b"\r\n".join((header, multiline_record, b"")),
                )
                with self.assertRaises(certify_bulk.UnsupportedMultilineRecordError):
                    certify_bulk.certify_bulk_file(
                        path,
                        source_channel="official_bulk",
                    )

    def test_malformed_date_shapes_are_quarantined_with_raw_value(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        header, record = self._first_v1_record()
        for value in (b"01-2011", b"AA-15-2011", b"02-30-2011"):
            with self.subTest(value=value.decode("ascii")), tempfile.TemporaryDirectory() as tmpdir:
                changed = record.replace(b"01-15-2011", value, 1)
                path = self._write_bytes(
                    tmpdir,
                    "malformed-date.csv",
                    b"\r\n".join((header, changed, b"")),
                )
                report = certify_bulk.certify_bulk_file(
                    path,
                    source_channel="official_bulk",
                )
                quarantined = [
                    item for item in report.row_outcomes
                    if item.reason == "malformed_date"
                ]
                self.assertEqual(len(quarantined), 1)
                self.assertIn(value.decode("ascii"), quarantined[0].raw_fields)

    def test_capture_time_and_status_must_be_coherent(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        fixture = FIXTURES_DIR / "ocpr-bulk-v1.csv"
        with self.assertRaises(ValueError):
            certify_bulk.certify_bulk_file(
                fixture,
                source_channel="official_bulk",
                capture_time="2026-04-04T20:51:08-04:00",
                capture_time_status="unknown",
            )
        for status in ("observed", "git_first_seen"):
            with self.subTest(status=status), self.assertRaises(ValueError):
                certify_bulk.certify_bulk_file(
                    fixture,
                    source_channel="official_bulk",
                    capture_time_status=status,
                )

    def test_all_stable_report_metadata_affects_logical_hash(self):
        certify_bulk = _require_pipeline_module("certify_bulk")
        fixture = FIXTURES_DIR / "ocpr-bulk-v1.csv"
        base = certify_bulk.certify_bulk_file(
            fixture,
            source_channel="official_bulk",
            fiscal_year="2010-2011",
            source_url="https://example.test/source-a",
            http_status=200,
            content_type="text/csv",
        )
        variants = [
            certify_bulk.certify_bulk_file(
                fixture,
                source_channel="official_bulk",
                fiscal_year="2011-2012",
                source_url="https://example.test/source-a",
                http_status=200,
                content_type="text/csv",
            ),
            certify_bulk.certify_bulk_file(
                fixture,
                source_channel="official_bulk",
                fiscal_year="2010-2011",
                source_url="https://example.test/source-b",
                http_status=200,
                content_type="text/csv",
            ),
            certify_bulk.certify_bulk_file(
                fixture,
                source_channel="official_bulk",
                fiscal_year="2010-2011",
                source_url="https://example.test/source-a",
                http_status=206,
                content_type="application/octet-stream",
            ),
        ]
        base_hash = certify_bulk.report_hash(base)
        self.assertEqual(len({base_hash, *(certify_bulk.report_hash(x) for x in variants)}), 4)


if __name__ == "__main__":
    unittest.main()
