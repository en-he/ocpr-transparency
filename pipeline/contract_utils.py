"""
Shared helpers for contract normalization, schema management, and inserts.
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
import unicodedata
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


RAW_SOURCE_TYPE = "csv"
LIVE_MONITOR_SOURCE_TYPE = "live_monitor"
LIVE_RECOVERY_SOURCE_TYPE = "live_recovery"

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
    "document_url",
    "fiscal_year",
]

PROVENANCE_COLUMNS = [
    "source_type",
    "source_url",
    "source_contract_id",
]

CANONICAL_RECORD_COLUMNS = CONTRACT_COLUMNS + PROVENANCE_COLUMNS
INSERT_COLUMNS = ["row_hash"] + CONTRACT_COLUMNS + PROVENANCE_COLUMNS + ["inserted_at"]

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
    normalized = normalize_lookup_value(raw)
    return 1 if normalized in {"SÍ", "SI", "YES", "S", "Y", "1", "TRUE"} else 0


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
) -> dict:
    now = inserted_at or record.get("inserted_at") or datetime.now(timezone.utc).isoformat()
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
        "cancelled": parse_cancelled(record.get("cancelled")) if not isinstance(record.get("cancelled"), int) else int(record.get("cancelled")),
        "document_url": clean_str(record.get("document_url")),
        "fiscal_year": clean_str(record.get("fiscal_year")) or fiscal_year_from_date(record.get("award_date")),
        "source_type": clean_str(record.get("source_type")) or default_source_type,
        "source_url": clean_str(record.get("source_url")),
        "source_contract_id": clean_str(record.get("source_contract_id")),
        "inserted_at": now,
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
                document_url        TEXT,
                fiscal_year         TEXT,
                source_type         TEXT NOT NULL DEFAULT 'csv',
                source_url          TEXT,
                source_contract_id  TEXT,
                inserted_at         TEXT
            );
        """)

    migrate_contracts_schema(conn)
    conn.executescript("""
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
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            fiscal_year TEXT,
            csv_file    TEXT,
            rows_parsed INTEGER,
            rows_new    INTEGER,
            rows_dup    INTEGER,
            ingested_at TEXT
        );
    """)


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
        "document_url": "TEXT",
        "fiscal_year": "TEXT",
        "source_type": "TEXT NOT NULL DEFAULT 'csv'",
        "source_url": "TEXT",
        "source_contract_id": "TEXT",
        "inserted_at": "TEXT",
    }
    for column, sql_type in additions.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE contracts ADD COLUMN {column} {sql_type}")

    conn.execute(
        "UPDATE contracts SET source_type = ? WHERE source_type IS NULL OR TRIM(source_type) = ''",
        (RAW_SOURCE_TYPE,),
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
