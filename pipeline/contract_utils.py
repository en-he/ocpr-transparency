"""
Shared helpers for contract normalization, schema management, and inserts.
"""
from __future__ import annotations

import hashlib
import math
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo


RAW_SOURCE_TYPE = "csv"
LIVE_MONITOR_SOURCE_TYPE = "live_monitor"
LIVE_RECOVERY_SOURCE_TYPE = "live_recovery"

CANCELLATION_STATUSES = frozenset(
    {"cancelled", "not_cancelled", "unknown", "malformed"}
)
CANCELLATION_COLUMNS = [
    "cancellation_raw",
    "cancellation_date",
    "cancellation_status",
]

CANONICAL_LINEAGE_COLUMNS = [
    "representative_observation_id",
    "canonicalization_status",
    "normalizer_version",
]

CONTRACT_COLUMNS = [
    "contract_number",
    "entity",
    "entity_number",
    "contractor",
    "amendment",
    "service_category",
    "service_type",
    "amount",
    "amount_receivable",
    "award_date",
    "valid_from",
    "valid_to",
    "procurement_method",
    "fund_type",
    "pco_number",
    "cancelled",
    *CANCELLATION_COLUMNS,
    "document_url",
    "fiscal_year",
]

PROVENANCE_COLUMNS = [
    "source_type",
    "source_url",
    "source_contract_id",
]

CANONICAL_RECORD_COLUMNS = CONTRACT_COLUMNS + PROVENANCE_COLUMNS
INSERT_COLUMNS = (
    ["row_hash"]
    + CONTRACT_COLUMNS
    + PROVENANCE_COLUMNS
    + ["inserted_at"]
    + CANONICAL_LINEAGE_COLUMNS
)

CONTRACT_INSERT_SQL = f"""
    INSERT OR IGNORE INTO contracts (
        {", ".join(INSERT_COLUMNS)}
    ) VALUES (
        {", ".join(f":{column}" for column in INSERT_COLUMNS)}
    )
"""

PR_TIMEZONE = ZoneInfo("America/Puerto_Rico")

CONTRACTOR_ALIAS_PATTERNS = [
    re.compile(r"\bA\s+DIVISION\s+OF\b.*$", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bDIVISION\s+OF\b.*$", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bD\s*B\s*A\b.*$", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bA\s*K\s*A\b.*$", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bH\s*N\s*C\b.*$", re.IGNORECASE | re.UNICODE),
]

CONTRACTOR_STOPWORDS = {
    "INC",
    "INCORPORATED",
    "LLC",
    "LLLP",
    "LLP",
    "LP",
    "LTD",
    "LIMITED",
    "CORP",
    "CORPORATION",
    "CO",
    "COMPANY",
    "PSC",
    "CSP",
    "PC",
    "SE",
    "SC",
    "US",
    "USA",
    "THE",
    "OF",
    "FOR",
    "DE",
    "DEL",
    "LA",
    "LAS",
    "LOS",
    "EL",
    "PARA",
    "Y",
    "AND",
    "ING",
    "INGENIERO",
}

COMPACT_CONTRACTOR_SUFFIXES = (
    "INCORPORATED",
    "CORPORATION",
    "COMPANY",
    "LIMITED",
    "LLLP",
    "LLC",
    "LLP",
    "CORP",
    "LTD",
    "PSC",
    "CSP",
    "INC",
)

SPACED_CONTRACTOR_SUFFIX_PATTERNS = [
    (re.compile(r"\bL\s+L\s+L\s+P\b", re.IGNORECASE | re.UNICODE), "LLLP"),
    (re.compile(r"\bL\s+L\s+C\b", re.IGNORECASE | re.UNICODE), "LLC"),
    (re.compile(r"\bL\s+L\s+P\b", re.IGNORECASE | re.UNICODE), "LLP"),
    (re.compile(r"\bP\s+S\s+C\b", re.IGNORECASE | re.UNICODE), "PSC"),
    (re.compile(r"\bC\s+S\s+P\b", re.IGNORECASE | re.UNICODE), "CSP"),
    (re.compile(r"\bP\s+C\b", re.IGNORECASE | re.UNICODE), "PC"),
    (re.compile(r"\bS\s+C\b", re.IGNORECASE | re.UNICODE), "SC"),
    (re.compile(r"\bS\s+E\b", re.IGNORECASE | re.UNICODE), "SE"),
]

LEADING_CONTRACTOR_TITLE_PATTERN = re.compile(
    r"^(?:ING|INGENIERO)\b\s*",
    re.IGNORECASE | re.UNICODE,
)

CONTRACTOR_FAMILY_OVERRIDES = {
    "AUTORIDADF FINANCIAMIENTO INFRAESTRU": "AUTORIDAD FINANCIAMIENTO INFRAESTRUCTURA PUERTO RICO",
    "MAGLEZ ENGINEERINGS CONTRACTORS": "MAGLEZ ENGINEERING CONTRACTORS",
    "CONSTRUCCIONES VIVI AGREDADO": "CONSTRUCCIONES VIVI AGREGADOS",
    "CONSTRUCCIONES VIVI AGREGADO": "CONSTRUCCIONES VIVI AGREGADOS",
    "CONSTRUCCIONES VIVI AGRAGADOS": "CONSTRUCCIONES VIVI AGREGADOS",
    "BERMUDEZLONGODIAZ MASSO": "BERMUDEZ LONGO DIAZ MASSO",
    "DESING BUILD": "DESIGN BUILD",
    "JOSEPH HARRISON FLORESDBAHARISON CONSULTING": "JOSEPH HARRISON FLORES",
    "MUNICIPIO VIEQUES CCD": "MUNICIPIO VIEQUES",
    "MUNICIPIO SAN LOENZO": "MUNICIPIO SAN LORENZO",
    "AUTORIDAD FINANCIAMIENTO INFRAESTRUC": "AUTORIDAD FINANCIAMIENTO INFRAESTRUCTURA PUERTO RICO",
    "J F BUILDING LEASE MAINTENANCE": "JF BUILDING LEASE MAINTENANCE",
    "ISIDRO M MARTINEZ GILORMINI": "MARTINEZ GILORMINI ISIDRO M",
    "ADMINISTRACION COMPENSACIONES POR ACCIDENTES": "ADMINISTRACION COMPENSACIONES POR ACCIDENTES AUTOMOVILES",
    "CANCIO NADAL RIVERA": "CANCIONADAL RIVERA",
    "AQUINO CORDOVA ALFARO": "AQUINO CORDOVAALFARO",
    "RICHARD SANTOS GARCIA MA": "RICHARD SANTOS GARCIAMA",
    "UNIVERSITY PUERTO RICO PARKING SYSTEM": "UNIVERSIDA PUERTO RICO PARKING SYSTEM",
    "NAIOSCALY CRUZ PONCE": "CRUZ PONCE NAIOSCALY",
    "GIOVANY RIVERA CARRERO": "RIVERA CARRERO GIOVANY",
    "A1 GENERATOR SERVICES": "AI GENERATOR SERVICES",
    "T P CONSULTING": "QUANTUM HEALTH CONSULTING",
    "INTEGRA": "INTEGRA DESIGN GROUP",
}

SPANISH_MONTHS = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}


def clean_str(value) -> str | None:
    if value is None:
        return None
    cleaned = str(value).replace("\x00", "").strip().strip('"').strip()
    return cleaned or None


def strip_entity_code(value) -> str | None:
    cleaned = clean_str(value)
    if not cleaned:
        return None
    if "|" in cleaned:
        _, maybe_name = cleaned.split("|", 1)
        maybe_name = clean_str(maybe_name)
        if maybe_name:
            return maybe_name
    return cleaned


def normalize_lookup_value(value) -> str:
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFD", str(value))
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    normalized = normalized.replace("\x00", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip().upper()


# Bulk observations use an explicit typed-result contract.  The legacy
# ``parse_amount``/``parse_date`` helpers below intentionally remain permissive
# for recovery and compatibility ingestion; bulk parsing calls this stricter
# profile-aware API instead.
BULK_FIELD_STATUSES = frozenset(
    {"valid", "missing", "malformed", "ambiguous", "out_of_domain"}
)
BULK_ALLOWED_CANONICAL_STATUSES = frozenset({"valid", "missing"})
_BULK_AMOUNT_PATTERN = re.compile(r"-?(?:\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?")
_BULK_DATE_FIELDS = {"award_date", "valid_from", "valid_to"}
_BULK_AMOUNT_FIELDS = {"amount", "amount_receivable"}


@dataclass(frozen=True)
class TypedFieldResult:
    """A raw bulk field paired with its typed value and closed status."""

    value: Any
    raw_value: str | None
    status: str
    warning: str | None = None

    def __post_init__(self):
        if self.status not in BULK_FIELD_STATUSES:
            raise ValueError(f"unknown bulk field status: {self.status!r}")


@dataclass(frozen=True)
class CancellationResult:
    """Validated cancellation semantics plus the untouched source value."""

    raw_value: str | None
    date: str | None
    status: str
    legacy_cancelled: int

    def __post_init__(self):
        if self.status not in CANCELLATION_STATUSES:
            raise ValueError(f"unknown cancellation status: {self.status!r}")
        if self.legacy_cancelled not in (0, 1):
            raise ValueError("legacy cancellation projection must be 0 or 1")
        if self.legacy_cancelled != int(self.status == "cancelled"):
            raise ValueError("legacy cancellation projection is not derived from status")


_CANCELLATION_TRUE_TOKENS = frozenset(
    {"SÍ", "SI", "YES", "Y", "S", "1", "TRUE", "T", "CANCELADO", "CANCELLED", "CANCELED"}
)
_CANCELLATION_FALSE_TOKENS = frozenset(
    {"NO", "N", "0", "FALSE", "F", "NOT CANCELLED", "NO CANCELADO", "NO CANCELED"}
)
_CANCELLATION_UNKNOWN_TOKENS = frozenset(
    {"UNKNOWN", "UNKNOW", "DESCONOCIDO", "N/A", "NA", "NONE", "NULL", "PENDING"}
)


def _parse_strict_cancellation_date(raw: Any) -> tuple[str | None, str]:
    """Parse only the certified bulk ``MM-DD-YYYY`` cancellation shape."""
    text = str(raw).strip()
    parts = text.split("-")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        return None, "malformed"
    month, day, year = parts
    if len(month) == 4:
        return None, "malformed"
    if len(year) == 2:
        return None, "ambiguous"
    if len(year) != 4:
        return None, "malformed"
    try:
        return datetime.strptime(text, "%m-%d-%Y").date().isoformat(), "valid"
    except ValueError:
        return None, "malformed"


def parse_cancellation(raw: Any) -> CancellationResult:
    """Interpret a cancellation value without collapsing source evidence.

    Bulk dates are certified as ``MM-DD-YYYY``.  A two-digit year is kept as
    an unresolved/unknown value, while other nonblank values that do not match
    a known live token are malformed.  In every case the original scalar is
    retained as ``raw_value``.
    """
    raw_value = None if raw is None else str(raw)
    if raw is None:
        return CancellationResult(None, None, "unknown", 0)

    text = str(raw).strip()
    if text in {"", "\x00"}:
        return CancellationResult(raw_value, None, "unknown", 0)

    normalized = normalize_lookup_value(text)
    if normalized in _CANCELLATION_TRUE_TOKENS:
        return CancellationResult(raw_value, None, "cancelled", 1)
    if normalized in _CANCELLATION_FALSE_TOKENS:
        return CancellationResult(raw_value, None, "not_cancelled", 0)
    if normalized in _CANCELLATION_UNKNOWN_TOKENS:
        return CancellationResult(raw_value, None, "unknown", 0)

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            parsed_date = datetime.strptime(text, "%Y-%m-%d").date().isoformat()
            return CancellationResult(raw_value, parsed_date, "cancelled", 1)
        except ValueError:
            return CancellationResult(raw_value, None, "malformed", 0)

    cancellation_date, date_status = _parse_strict_cancellation_date(text)
    if date_status == "valid":
        return CancellationResult(raw_value, cancellation_date, "cancelled", 1)
    if date_status == "ambiguous":
        return CancellationResult(raw_value, None, "unknown", 0)

    # Live OCPR recovery surfaces also use MM/DD/YYYY and Microsoft AJAX
    # date scalars. These shapes are accepted here but remain out of the
    # certified bulk profile through ``_bulk_cancellation_field_status``.
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", text):
        try:
            parsed_date = datetime.strptime(text, "%m/%d/%Y").date().isoformat()
            return CancellationResult(raw_value, parsed_date, "cancelled", 1)
        except ValueError:
            pass
    if re.search(r"/Date\((-?\d+)", text):
        parsed_date = parse_ms_ajax_date(text)
        if parsed_date:
            return CancellationResult(raw_value, parsed_date, "cancelled", 1)
    return CancellationResult(raw_value, None, "malformed", 0)


def _bulk_cancellation_field_status(raw: Any, parsed: CancellationResult) -> str:
    if _bulk_blank(raw):
        return "missing"
    normalized = normalize_lookup_value(raw)
    if normalized in (
        _CANCELLATION_TRUE_TOKENS
        | _CANCELLATION_FALSE_TOKENS
        | _CANCELLATION_UNKNOWN_TOKENS
    ):
        return "valid"
    _, date_status = _parse_strict_cancellation_date(raw)
    if date_status == "ambiguous":
        return "ambiguous"
    if date_status != "valid" or parsed.status == "malformed":
        return "malformed"
    return "valid"


def _parse_bulk_cancellation_date(raw: Any) -> TypedFieldResult:
    parsed = parse_cancellation(raw)
    field_status = _bulk_cancellation_field_status(raw, parsed)
    return TypedFieldResult(parsed.date, parsed.raw_value, field_status)


def _parse_bulk_cancellation_status(raw: Any) -> TypedFieldResult:
    parsed = parse_cancellation(raw)
    field_status = _bulk_cancellation_field_status(raw, parsed)
    return TypedFieldResult(parsed.status, parsed.raw_value, field_status)


def _parse_bulk_cancellation_raw(raw: Any) -> TypedFieldResult:
    parsed = parse_cancellation(raw)
    field_status = _bulk_cancellation_field_status(raw, parsed)
    return TypedFieldResult(parsed.raw_value, parsed.raw_value, field_status)


def _bulk_blank(raw: Any) -> bool:
    return raw is None or str(raw).strip() in {"", "\x00"}


def _parse_bulk_date(raw: Any) -> TypedFieldResult:
    if _bulk_blank(raw):
        return TypedFieldResult(None, None if raw is None else str(raw), "missing")

    text = str(raw).replace("\u00a0", " ").strip()
    parts = text.split("-")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        return TypedFieldResult(None, str(raw), "malformed")

    month, day, year = parts
    if len(year) == 2:
        return TypedFieldResult(None, str(raw), "ambiguous")
    if len(year) != 4:
        return TypedFieldResult(None, str(raw), "malformed")
    try:
        parsed = datetime.strptime(text, "%m-%d-%Y").date().isoformat()
    except ValueError:
        # The shape is date-like, but the profile's fixed calendar domain
        # rejects it.  Certification separately reports this as malformed_date.
        return TypedFieldResult(None, str(raw), "out_of_domain")
    return TypedFieldResult(parsed, str(raw), "valid")


def _parse_bulk_amount(raw: Any) -> TypedFieldResult:
    if _bulk_blank(raw):
        return TypedFieldResult(None, None if raw is None else str(raw), "missing")

    text = str(raw).strip()
    if not _BULK_AMOUNT_PATTERN.fullmatch(text):
        return TypedFieldResult(None, str(raw), "malformed")
    try:
        parsed = float(text.replace(",", ""))
    except ValueError:
        return TypedFieldResult(None, str(raw), "malformed")
    if not math.isfinite(parsed):
        return TypedFieldResult(None, str(raw), "out_of_domain")
    return TypedFieldResult(parsed, str(raw), "valid")


def _parse_bulk_cancelled(raw: Any) -> TypedFieldResult:
    parsed = parse_cancellation(raw)
    field_status = _bulk_cancellation_field_status(raw, parsed)
    return TypedFieldResult(
        parsed.legacy_cancelled,
        parsed.raw_value,
        field_status,
    )


def parse_bulk_field(
    canonical: str,
    raw: Any,
    *,
    profile: str | None = None,
) -> TypedFieldResult:
    """Parse one known bulk field without trying alternate date orders.

    ``profile`` is accepted as an explicit contract marker even though all
    currently certified profiles use MM-DD-YYYY.  Keeping it in the API makes
    a future profile-specific rule change appendable and auditable rather than
    silently changing the interpretation of old observations.
    """
    del profile  # The certified corpus rule is fixed for v1/v2/v3 today.
    raw_value = None if raw is None else str(raw)
    if canonical in _BULK_DATE_FIELDS:
        return _parse_bulk_date(raw)
    if canonical in _BULK_AMOUNT_FIELDS:
        return _parse_bulk_amount(raw)
    if canonical == "cancelled":
        return _parse_bulk_cancelled(raw)
    if canonical == "cancellation_raw":
        return _parse_bulk_cancellation_raw(raw)
    if canonical == "cancellation_date":
        return _parse_bulk_cancellation_date(raw)
    if canonical == "cancellation_status":
        return _parse_bulk_cancellation_status(raw)
    if _bulk_blank(raw):
        return TypedFieldResult(None, raw_value, "missing")
    return TypedFieldResult(clean_str(raw), raw_value, "valid")


def normalize_entity_name(value) -> str:
    return normalize_lookup_value(strip_entity_code(value))


def parse_amount(raw) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    cleaned = (
        str(raw)
        .replace("$", "")
        .replace(",", "")
        .replace("\u00a0", "")
        .replace(" ", "")
        .strip()
    )
    if cleaned in {"", "-", "N/A", "NA"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_spanish_date(raw: str) -> str | None:
    normalized = (
        raw.replace("\u00a0", " ")
        .replace(" a. m.", "")
        .replace(" p. m.", "")
        .replace(" a.m.", "")
        .replace(" p.m.", "")
        .replace(" am", "")
        .replace(" pm", "")
        .strip()
        .lower()
    )
    match = re.match(r"^(\d{1,2})\s+([a-záéíóú\.]+)\s+(\d{4})", normalized)
    if not match:
        return None
    day = int(match.group(1))
    month_key = match.group(2).strip(".")
    month = SPANISH_MONTHS.get(month_key)
    if not month:
        return None
    year = int(match.group(3))
    return f"{year:04d}-{month:02d}-{day:02d}"


def parse_date(raw) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date().isoformat()

    normalized = str(raw).replace("\u00a0", " ").strip()
    if normalized in {"", "-", "N/A", "NA", "0", "0.0", "0.00"}:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        return normalized

    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(normalized[:19], fmt).date().isoformat()
        except ValueError:
            continue

    return _parse_spanish_date(normalized)


def parse_cancelled(raw) -> int:
    """Compatibility projection derived from validated cancellation status."""
    return parse_cancellation(raw).legacy_cancelled


def parse_ms_ajax_date(raw) -> str | None:
    if not raw:
        return None
    match = re.search(r"(-?\d+)", str(raw))
    if not match:
        return parse_date(raw)
    dt = datetime.fromtimestamp(int(match.group(1)) / 1000, tz=timezone.utc).astimezone(PR_TIMEZONE)
    return dt.date().isoformat()


def fiscal_year_from_date(date_str) -> str | None:
    normalized = parse_date(date_str)
    if not normalized:
        return None
    dt = datetime.strptime(normalized, "%Y-%m-%d")
    if dt.month >= 7:
        return f"{dt.year}-{dt.year + 1}"
    return f"{dt.year - 1}-{dt.year}"


def normalize_amendment_value(value) -> str:
    if value is None:
        return ""
    return str(value).replace("\x00", "").strip()


def is_original_amendment(value) -> bool:
    normalized = normalize_lookup_value(normalize_amendment_value(value))
    return normalized in {"", "ORIGINAL"}


def normalize_contractor_family(value) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFD", str(value))
    normalized = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    normalized = re.sub(r"[\u0000\.,;:()/\-]", " ", normalized)
    normalized = normalized.replace("&", " ")
    normalized = normalized.upper()

    for pattern, replacement in SPACED_CONTRACTOR_SUFFIX_PATTERNS:
        normalized = pattern.sub(replacement, normalized)

    for suffix in COMPACT_CONTRACTOR_SUFFIXES:
        normalized = re.sub(rf"(?<=[A-Z0-9]){suffix}\b", f" {suffix}", normalized)

    normalized = re.sub(r"\bP\s*R\b", "PUERTO RICO", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    for pattern in CONTRACTOR_ALIAS_PATTERNS:
        normalized = pattern.sub("", normalized).strip()

    normalized = LEADING_CONTRACTOR_TITLE_PATTERN.sub("", normalized).strip()
    tokens = [token for token in normalized.split(" ") if token and token not in CONTRACTOR_STOPWORDS]
    family = " ".join(tokens).strip()
    return CONTRACTOR_FAMILY_OVERRIDES.get(family, family)


def register_sqlite_functions(conn: sqlite3.Connection):
    conn.create_function("normalize_contractor_family", 1, normalize_contractor_family)


def row_hash(row: dict) -> str:
    key = "|".join([
        row.get("contract_number") or "",
        row.get("entity") or "",
        row.get("contractor") or "",
        normalize_amendment_value(row.get("amendment")),
        row.get("award_date") or "",
        str(row.get("amount") or ""),
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def normalize_contract_record(
    record: dict,
    *,
    default_source_type: str = RAW_SOURCE_TYPE,
    inserted_at: str | None = None,
    preserve_missing_inserted_at: bool = False,
) -> dict:
    if preserve_missing_inserted_at:
        now = inserted_at or record.get("inserted_at")
    else:
        now = inserted_at or record.get("inserted_at") or datetime.now(timezone.utc).isoformat()
    source_type = clean_str(record.get("source_type")) or default_source_type
    canonicalization_status = clean_str(record.get("canonicalization_status"))
    if not canonicalization_status:
        canonicalization_status = (
            "recovery_unlinked"
            if source_type in {LIVE_MONITOR_SOURCE_TYPE, LIVE_RECOVERY_SOURCE_TYPE}
            else "legacy_unlinked"
        )
    cancellation_source = (
        record.get("cancellation_raw")
        if "cancellation_raw" in record
        else record.get("cancelled")
    )
    cancellation = parse_cancellation(cancellation_source)
    normalized = {
        "contract_number": clean_str(record.get("contract_number")),
        "entity": strip_entity_code(record.get("entity")),
        "entity_number": clean_str(record.get("entity_number")),
        "contractor": clean_str(record.get("contractor")),
        "amendment": normalize_amendment_value(record.get("amendment")),
        "service_category": clean_str(record.get("service_category")),
        "service_type": clean_str(record.get("service_type")),
        "amount": parse_amount(record.get("amount")),
        "amount_receivable": parse_amount(record.get("amount_receivable")),
        "award_date": parse_date(record.get("award_date")),
        "valid_from": parse_date(record.get("valid_from")),
        "valid_to": parse_date(record.get("valid_to")),
        "procurement_method": clean_str(record.get("procurement_method")),
        "fund_type": clean_str(record.get("fund_type")),
        "pco_number": clean_str(record.get("pco_number")),
        "cancelled": cancellation.legacy_cancelled,
        "cancellation_raw": cancellation.raw_value,
        "cancellation_date": cancellation.date,
        "cancellation_status": cancellation.status,
        "document_url": clean_str(record.get("document_url")),
        "fiscal_year": clean_str(record.get("fiscal_year")) or fiscal_year_from_date(record.get("award_date")),
        "source_type": source_type,
        "source_url": clean_str(record.get("source_url")),
        "source_contract_id": clean_str(record.get("source_contract_id")),
        "inserted_at": now,
        "representative_observation_id": clean_str(
            record.get("representative_observation_id")
        ),
        "canonicalization_status": canonicalization_status,
        "normalizer_version": clean_str(record.get("normalizer_version")),
    }
    normalized["row_hash"] = row_hash(normalized)
    return normalized


def records_equivalent(left: dict, right: dict) -> bool:
    for column in CANONICAL_RECORD_COLUMNS:
        if column == "cancelled":
            if int(left.get(column) or 0) != int(right.get(column) or 0):
                return False
            continue
        if left.get(column) != right.get(column):
            return False
    return True


def create_schema(conn: sqlite3.Connection):
    conn.execute("PRAGMA foreign_keys = ON")
    if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise RuntimeError("schema creation requires PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA recursive_triggers = ON")
    if conn.execute("PRAGMA recursive_triggers").fetchone()[0] != 1:
        raise RuntimeError("schema creation requires PRAGMA recursive_triggers=ON")
    contracts_exists = bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'contracts'"
        ).fetchone()
    )
    if not contracts_exists:
        conn.executescript("""
            CREATE TABLE contracts (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                row_hash            TEXT UNIQUE,
                contract_number     TEXT,
                entity              TEXT,
                entity_number       TEXT,
                contractor          TEXT,
                amendment           TEXT,
                service_category    TEXT,
                service_type        TEXT,
                amount              REAL,
                amount_receivable   REAL,
                award_date          TEXT,
                valid_from          TEXT,
                valid_to            TEXT,
                procurement_method  TEXT,
                fund_type           TEXT,
                pco_number          TEXT,
                cancelled           INTEGER DEFAULT 0,
                cancellation_raw    TEXT,
                cancellation_date   TEXT,
                cancellation_status TEXT NOT NULL DEFAULT 'unknown'
                    CHECK (cancellation_status IN (
                        'cancelled', 'not_cancelled', 'unknown', 'malformed'
                    )),
                document_url        TEXT,
                fiscal_year         TEXT,
                source_type         TEXT NOT NULL DEFAULT 'csv',
                source_url          TEXT,
                source_contract_id  TEXT,
                inserted_at         TEXT,
                representative_observation_id TEXT,
                canonicalization_status TEXT NOT NULL DEFAULT 'legacy_unlinked'
                    CHECK (canonicalization_status IN (
                        'selected_observation', 'legacy_unlinked', 'recovery_unlinked'
                    )),
                normalizer_version  TEXT
            );
        """)

    migrate_contracts_schema(conn)
    conn.executescript("""
        CREATE TRIGGER IF NOT EXISTS normalize_contract_cancellation_insert
        AFTER INSERT ON contracts
        WHEN NEW.cancelled IS NULL
        BEGIN
            UPDATE contracts
            SET cancelled = CASE
                WHEN cancellation_status = 'cancelled' THEN 1 ELSE 0
            END
            WHERE id = NEW.id;
        END;

        CREATE TRIGGER IF NOT EXISTS validate_contract_cancellation_insert
        BEFORE INSERT ON contracts
        WHEN NEW.cancellation_status NOT IN (
                 'cancelled', 'not_cancelled', 'unknown', 'malformed'
             )
          OR NEW.cancelled != CASE
                 WHEN NEW.cancellation_status = 'cancelled' THEN 1 ELSE 0
             END
          OR (NEW.cancellation_date IS NOT NULL
              AND NEW.cancellation_status != 'cancelled')
        BEGIN
            SELECT RAISE(ABORT, 'invalid cancellation projection');
        END;
        CREATE TRIGGER IF NOT EXISTS validate_contract_cancellation_update
        BEFORE UPDATE OF cancelled, cancellation_raw,
                         cancellation_date, cancellation_status ON contracts
        WHEN NEW.cancellation_status NOT IN (
                 'cancelled', 'not_cancelled', 'unknown', 'malformed'
             )
          OR NEW.cancelled != CASE
                 WHEN NEW.cancellation_status = 'cancelled' THEN 1 ELSE 0
             END
          OR (NEW.cancellation_date IS NOT NULL
              AND NEW.cancellation_status != 'cancelled')
        BEGIN
            SELECT RAISE(ABORT, 'invalid cancellation projection');
        END;

        CREATE INDEX IF NOT EXISTS idx_entity       ON contracts(entity);
        CREATE INDEX IF NOT EXISTS idx_contractor   ON contracts(contractor);
        CREATE INDEX IF NOT EXISTS idx_amount       ON contracts(amount);
        CREATE INDEX IF NOT EXISTS idx_award_date   ON contracts(award_date);
        CREATE INDEX IF NOT EXISTS idx_fiscal_year  ON contracts(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_contract_no  ON contracts(contract_number);
        CREATE INDEX IF NOT EXISTS idx_service_cat  ON contracts(service_category);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_contracts_row_hash ON contracts(row_hash);

        CREATE VIRTUAL TABLE IF NOT EXISTS contracts_fts USING fts5(
            contract_number,
            entity,
            contractor,
            service_category,
            service_type,
            content='contracts',
            content_rowid='id'
        );

        CREATE TABLE IF NOT EXISTS ingestion_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_year         TEXT,
            csv_file            TEXT,
            rows_parsed         INTEGER,
            rows_new            INTEGER,
            rows_dup            INTEGER,
            ingested_at         TEXT,
            observations_total  INTEGER DEFAULT 0,
            canonical_excluded  INTEGER DEFAULT 0,
            exclusions_json     TEXT NOT NULL DEFAULT '[]'
        );
    """)
    migrate_ingestion_log_schema(conn)
    create_bulk_observation_schema(conn)


def migrate_ingestion_log_schema(conn: sqlite3.Connection):
    """Add Task 3 audit columns without changing recovery log behavior."""
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(ingestion_log)").fetchall()
    }
    additions = {
        "observations_total": "INTEGER DEFAULT 0",
        "canonical_excluded": "INTEGER DEFAULT 0",
        "exclusions_json": "TEXT NOT NULL DEFAULT '[]'",
    }
    for column, sql_type in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE ingestion_log ADD COLUMN {column} {sql_type}")
    conn.commit()


def create_bulk_observation_schema(conn: sqlite3.Connection):
    """Create the full/audit observation ledger, never the browser projection."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS evidence_objects (
            evidence_id          TEXT PRIMARY KEY
                                 CHECK (length(evidence_id) = 71 AND substr(evidence_id, 1, 7) = 'sha256:'),
            source_channel       TEXT NOT NULL
                                 CHECK (source_channel IN ('official_bulk', 'archive_bulk')),
            fiscal_year          TEXT,
            source_url           TEXT,
            archive_url          TEXT,
            captured_at          TEXT,
            capture_time_status  TEXT NOT NULL
                                 CHECK (capture_time_status IN ('observed', 'git_first_seen', 'unknown')),
            sha256               TEXT NOT NULL
                                 CHECK (length(sha256) = 64),
            byte_length          INTEGER NOT NULL CHECK (byte_length >= 0),
            encoding             TEXT NOT NULL,
            media_type           TEXT,
            header_profile       TEXT NOT NULL,
            header_fingerprint   TEXT NOT NULL,
            status               TEXT NOT NULL
                                 CHECK (status IN ('certified', 'certified_with_quarantine')),
            metadata_json        TEXT NOT NULL,
            CHECK (evidence_id = 'sha256:' || sha256),
            CHECK (
                (capture_time_status = 'unknown' AND captured_at IS NULL)
                OR
                (capture_time_status != 'unknown' AND captured_at IS NOT NULL)
            ),
            UNIQUE(source_channel, fiscal_year, sha256)
        );

        CREATE TABLE IF NOT EXISTS bulk_observations (
            observation_id             TEXT PRIMARY KEY
                                       CHECK (length(observation_id) = 71 AND substr(observation_id, 1, 7) = 'sha256:'),
            evidence_id               TEXT NOT NULL
                                      REFERENCES evidence_objects(evidence_id)
                                      ON DELETE RESTRICT,
            source_row_number         INTEGER NOT NULL CHECK (source_row_number >= 2),
            raw_row_hash              TEXT NOT NULL CHECK (length(raw_row_hash) = 64),
            raw_record                TEXT NOT NULL,
            raw_values_json           TEXT NOT NULL,
            raw_coordinates_json      TEXT NOT NULL,
            parser_profile             TEXT NOT NULL,
            parser_version            TEXT NOT NULL,
            normalizer_version        TEXT NOT NULL,
            parsed_values_json        TEXT NOT NULL,
            field_status_json         TEXT NOT NULL,
            warnings_json              TEXT NOT NULL,
            parser_outcome             TEXT NOT NULL
                                       CHECK (parser_outcome IN (
                                           'certified', 'shifted_row', 'malformed_csv',
                                           'malformed_date', 'ambiguous_date',
                                           'malformed_amount'
                                       )),
            observation_status         TEXT NOT NULL
                                      CHECK (observation_status IN ('certified', 'quarantined')),
            duplicate_status           TEXT NOT NULL
                                      CHECK (duplicate_status IN ('unique', 'exact_duplicate')),
            duplicate_of_observation_id TEXT
                                        REFERENCES bulk_observations(observation_id)
                                        ON DELETE RESTRICT,
            canonical_eligible         INTEGER NOT NULL
                                      CHECK (canonical_eligible IN (0, 1)),
            canonical_exclusion_reason TEXT,
            CHECK (
                (canonical_eligible = 1 AND canonical_exclusion_reason IS NULL)
                OR
                (canonical_eligible = 0 AND canonical_exclusion_reason IS NOT NULL)
            ),
            CHECK (
                (duplicate_status = 'unique' AND duplicate_of_observation_id IS NULL)
                OR
                (duplicate_status = 'exact_duplicate' AND duplicate_of_observation_id IS NOT NULL)
            ),
            UNIQUE(evidence_id, source_row_number, parser_version,
                   normalizer_version),
            UNIQUE(observation_id, evidence_id, source_row_number)
        );
        CREATE INDEX IF NOT EXISTS idx_bulk_observations_evidence
            ON bulk_observations(evidence_id);
        CREATE INDEX IF NOT EXISTS idx_bulk_observations_status
            ON bulk_observations(observation_status, canonical_eligible);

        CREATE TRIGGER IF NOT EXISTS validate_bulk_observation_insert
        BEFORE INSERT ON bulk_observations
        WHEN NEW.canonical_eligible = 1 AND NEW.observation_status != 'certified'
        BEGIN
            SELECT RAISE(ABORT, 'only certified observations may be canonical eligible');
        END;

        CREATE TRIGGER IF NOT EXISTS evidence_objects_no_update
        BEFORE UPDATE ON evidence_objects
        BEGIN
            SELECT RAISE(ABORT, 'evidence objects are append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS evidence_objects_no_delete
        BEFORE DELETE ON evidence_objects
        BEGIN
            SELECT RAISE(ABORT, 'evidence objects are append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS bulk_observations_no_update
        BEFORE UPDATE ON bulk_observations
        BEGIN
            SELECT RAISE(ABORT, 'bulk observations are append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS bulk_observations_no_delete
        BEFORE DELETE ON bulk_observations
        BEGIN
            SELECT RAISE(ABORT, 'bulk observations are append-only');
        END;

        CREATE TABLE IF NOT EXISTS bulk_projection_exclusions (
            exclusion_id       TEXT PRIMARY KEY
                               CHECK (length(exclusion_id) = 71 AND substr(exclusion_id, 1, 7) = 'sha256:'),
            observation_id     TEXT NOT NULL
                               REFERENCES bulk_observations(observation_id)
                               ON DELETE RESTRICT,
            evidence_id        TEXT NOT NULL
                               REFERENCES evidence_objects(evidence_id)
                               ON DELETE RESTRICT,
            source_row_number  INTEGER NOT NULL CHECK (source_row_number >= 2),
            reason             TEXT NOT NULL,
            details_json       TEXT NOT NULL,
            UNIQUE(observation_id),
            FOREIGN KEY (observation_id, evidence_id, source_row_number)
                REFERENCES bulk_observations(
                    observation_id, evidence_id, source_row_number
                ) ON DELETE RESTRICT
        );
        CREATE INDEX IF NOT EXISTS idx_bulk_exclusions_evidence
            ON bulk_projection_exclusions(evidence_id);

        CREATE TRIGGER IF NOT EXISTS validate_projection_exclusion_insert
        BEFORE INSERT ON bulk_projection_exclusions
        WHEN NOT EXISTS (
            SELECT 1 FROM bulk_observations AS observation
            WHERE observation.observation_id = NEW.observation_id
              AND (
                  (
                      observation.canonical_eligible = 0
                      AND NEW.reason = observation.canonical_exclusion_reason
                  )
                  OR
                  (
                      observation.canonical_eligible = 1
                      AND observation.observation_status = 'certified'
                      AND NEW.reason = 'canonical_row_hash_duplicate'
                  )
              )
        )
        BEGIN
            SELECT RAISE(ABORT, 'invalid projection exclusion semantics');
        END;
        CREATE TRIGGER IF NOT EXISTS bulk_projection_exclusions_no_update
        BEFORE UPDATE ON bulk_projection_exclusions
        BEGIN
            SELECT RAISE(ABORT, 'projection exclusions are append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS bulk_projection_exclusions_no_delete
        BEFORE DELETE ON bulk_projection_exclusions
        BEGIN
            SELECT RAISE(ABORT, 'projection exclusions are append-only');
        END;

        CREATE TABLE IF NOT EXISTS bulk_projection_results (
            observation_id   TEXT PRIMARY KEY
                             REFERENCES bulk_observations(observation_id)
                             ON DELETE RESTRICT,
            row_hash         TEXT,
            contract_id      INTEGER REFERENCES contracts(id) ON DELETE RESTRICT,
            projection_status TEXT NOT NULL
                              CHECK (projection_status IN ('selected', 'excluded')),
            reason           TEXT,
            CHECK (
                (projection_status = 'selected' AND contract_id IS NOT NULL
                    AND row_hash IS NOT NULL AND reason IS NULL)
                OR
                (projection_status = 'excluded' AND reason IS NOT NULL
                    AND (
                        (contract_id IS NULL AND row_hash IS NULL)
                        OR
                        (contract_id IS NOT NULL AND row_hash IS NOT NULL)
                    ))
            )
        );

        CREATE TRIGGER IF NOT EXISTS validate_contract_lineage_insert
        BEFORE INSERT ON contracts
        WHEN NEW.canonicalization_status = 'selected_observation'
        BEGIN
            SELECT CASE WHEN
                NEW.representative_observation_id IS NULL
                OR NEW.normalizer_version IS NULL
                OR NOT EXISTS (
                    SELECT 1 FROM bulk_observations AS observation
                    WHERE observation.observation_id = NEW.representative_observation_id
                      AND observation.normalizer_version = NEW.normalizer_version
                      AND observation.canonical_eligible = 1
                      AND observation.observation_status = 'certified'
                )
            THEN RAISE(ABORT, 'invalid selected-observation contract lineage') END;
        END;

        CREATE TRIGGER IF NOT EXISTS validate_contract_lineage_update
        BEFORE UPDATE OF row_hash, representative_observation_id,
                         canonicalization_status, normalizer_version ON contracts
        BEGIN
            SELECT CASE WHEN
                NEW.canonicalization_status = 'selected_observation'
                AND (
                    NEW.representative_observation_id IS NULL
                    OR NEW.normalizer_version IS NULL
                    OR NOT EXISTS (
                        SELECT 1 FROM bulk_observations AS observation
                        WHERE observation.observation_id = NEW.representative_observation_id
                          AND observation.normalizer_version = NEW.normalizer_version
                          AND observation.canonical_eligible = 1
                          AND observation.observation_status = 'certified'
                    )
                )
            THEN RAISE(ABORT, 'invalid selected-observation contract lineage') END;
            SELECT CASE WHEN EXISTS (
                SELECT 1 FROM bulk_projection_results AS result
                WHERE result.contract_id = OLD.id
                  AND (
                      (result.row_hash IS NOT NULL AND result.row_hash != NEW.row_hash)
                      OR (
                          result.projection_status = 'selected'
                          AND (
                              NEW.canonicalization_status != 'selected_observation'
                              OR result.observation_id != NEW.representative_observation_id
                          )
                      )
                  )
            ) THEN RAISE(ABORT, 'canonical contract/projection mismatch') END;
        END;

        CREATE TRIGGER IF NOT EXISTS validate_projection_result_insert
        BEFORE INSERT ON bulk_projection_results
        BEGIN
            SELECT CASE WHEN NOT EXISTS (
                SELECT 1 FROM bulk_observations AS observation
                WHERE observation.observation_id = NEW.observation_id
                  AND (
                      (
                          NEW.projection_status = 'selected'
                          AND observation.canonical_eligible = 1
                          AND observation.observation_status = 'certified'
                      )
                      OR
                      (
                          NEW.projection_status = 'excluded'
                          AND (
                              (
                                  NEW.contract_id IS NULL
                                  AND NEW.row_hash IS NULL
                                  AND observation.canonical_eligible = 0
                                  AND NEW.reason = observation.canonical_exclusion_reason
                              )
                              OR
                              (
                                  NEW.contract_id IS NOT NULL
                                  AND NEW.row_hash IS NOT NULL
                                  AND observation.canonical_eligible = 1
                                  AND observation.observation_status = 'certified'
                                  AND NEW.reason = 'canonical_row_hash_duplicate'
                              )
                          )
                      )
                  )
            ) THEN RAISE(ABORT, 'invalid projection result semantics') END;
            SELECT CASE WHEN NEW.contract_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM contracts AS contract
                WHERE contract.id = NEW.contract_id
                  AND contract.row_hash = NEW.row_hash
            ) THEN RAISE(ABORT, 'projection contract/hash mismatch') END;
            SELECT CASE WHEN NEW.projection_status = 'selected' AND NOT EXISTS (
                SELECT 1
                FROM contracts AS contract
                JOIN bulk_observations AS observation
                  ON observation.observation_id = NEW.observation_id
                WHERE contract.id = NEW.contract_id
                  AND contract.representative_observation_id = NEW.observation_id
                  AND contract.canonicalization_status = 'selected_observation'
                  AND contract.normalizer_version = observation.normalizer_version
            ) THEN RAISE(ABORT, 'selected projection lineage mismatch') END;
        END;

        CREATE TRIGGER IF NOT EXISTS validate_projection_result_update
        BEFORE UPDATE ON bulk_projection_results
        BEGIN
            SELECT CASE WHEN NEW.contract_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM contracts AS contract
                WHERE contract.id = NEW.contract_id
                  AND contract.row_hash = NEW.row_hash
            ) THEN RAISE(ABORT, 'projection contract/hash mismatch') END;
            SELECT CASE WHEN NEW.projection_status = 'selected' AND NOT EXISTS (
                SELECT 1
                FROM contracts AS contract
                JOIN bulk_observations AS observation
                  ON observation.observation_id = NEW.observation_id
                WHERE contract.id = NEW.contract_id
                  AND contract.representative_observation_id = NEW.observation_id
                  AND contract.canonicalization_status = 'selected_observation'
                  AND contract.normalizer_version = observation.normalizer_version
            ) THEN RAISE(ABORT, 'selected projection lineage mismatch') END;
        END;
        CREATE TRIGGER IF NOT EXISTS bulk_projection_results_no_update
        BEFORE UPDATE ON bulk_projection_results
        BEGIN
            SELECT RAISE(ABORT, 'projection results are append-only');
        END;
        CREATE TRIGGER IF NOT EXISTS bulk_projection_results_no_delete
        BEFORE DELETE ON bulk_projection_results
        BEGIN
            SELECT RAISE(ABORT, 'projection results are append-only');
        END;
        """
    )
    conn.commit()


def migrate_contracts_schema(conn: sqlite3.Connection):
    existing = {row[1] for row in conn.execute("PRAGMA table_info(contracts)").fetchall()}
    additions = {
        "row_hash": "TEXT",
        "contract_number": "TEXT",
        "entity": "TEXT",
        "entity_number": "TEXT",
        "contractor": "TEXT",
        "amendment": "TEXT",
        "service_category": "TEXT",
        "service_type": "TEXT",
        "amount": "REAL",
        "amount_receivable": "REAL",
        "award_date": "TEXT",
        "valid_from": "TEXT",
        "valid_to": "TEXT",
        "procurement_method": "TEXT",
        "fund_type": "TEXT",
        "pco_number": "TEXT",
        "cancelled": "INTEGER DEFAULT 0",
        "cancellation_raw": "TEXT",
        "cancellation_date": "TEXT",
        "cancellation_status": "TEXT NOT NULL DEFAULT 'unknown'",
        "document_url": "TEXT",
        "fiscal_year": "TEXT",
        "source_type": "TEXT NOT NULL DEFAULT 'csv'",
        "source_url": "TEXT",
        "source_contract_id": "TEXT",
        "inserted_at": "TEXT",
        "representative_observation_id": "TEXT",
        "canonicalization_status": "TEXT NOT NULL DEFAULT 'legacy_unlinked'",
        "normalizer_version": "TEXT",
    }
    cancellation_status_added = "cancellation_status" not in existing
    for column, sql_type in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE contracts ADD COLUMN {column} {sql_type}")

    if cancellation_status_added:
        conn.execute(
            """
            UPDATE contracts
            SET cancellation_status = CASE
                WHEN cancelled = 1 THEN 'cancelled'
                ELSE 'unknown'
            END
            """
        )
    else:
        conn.execute(
            """
            UPDATE contracts
            SET cancellation_status = CASE
                WHEN cancelled = 1 THEN 'cancelled'
                ELSE 'unknown'
            END
            WHERE cancellation_status IS NULL
               OR TRIM(cancellation_status) = ''
               OR cancellation_status NOT IN (
                   'cancelled', 'not_cancelled', 'unknown', 'malformed'
               )
            """
        )

    conn.execute(
        """
        UPDATE contracts
        SET cancelled = CASE
            WHEN cancellation_status = 'cancelled' THEN 1 ELSE 0
        END
        WHERE cancelled IS NULL
        """
    )

    conn.execute(
        "UPDATE contracts SET source_type = ? WHERE source_type IS NULL OR TRIM(source_type) = ''",
        (RAW_SOURCE_TYPE,),
    )
    conn.execute(
        """
        UPDATE contracts
        SET canonicalization_status = 'legacy_unlinked'
        WHERE canonicalization_status IS NULL OR TRIM(canonicalization_status) = ''
        """
    )
    conn.commit()


def insert_contract_rows(
    conn: sqlite3.Connection,
    rows: list[dict],
    *,
    rebuild_fts: bool = False,
) -> list[dict]:
    inserted: list[dict] = []
    for row in rows:
        normalized = normalize_contract_record(
            row,
            default_source_type=row.get("source_type") or RAW_SOURCE_TYPE,
            inserted_at=row.get("inserted_at"),
        )
        cur = conn.execute(CONTRACT_INSERT_SQL, normalized)
        if cur.rowcount > 0:
            inserted.append(normalized)

    if rebuild_fts and inserted:
        conn.execute("INSERT INTO contracts_fts(contracts_fts) VALUES('rebuild')")
    conn.commit()
    return inserted
