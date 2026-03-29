"""
Ingest OCPR fiscal year CSVs into a normalized SQLite database with FTS5.

Usage:
    python pipeline/ingest.py
    python pipeline/ingest.py --csv-dir data/raw --db data/db/contratos.db
    python pipeline/ingest.py --reset

Schema:
    contracts (id, row_hash, contract_number, entity, entity_number, contractor,
               amendment, service_category, service_type, amount, amount_receivable,
               award_date, valid_from, valid_to, procurement_method, fund_type,
               pco_number, cancelled, document_url, fiscal_year, inserted_at)

    contracts_fts (FTS5 virtual table for full-text search)
    ingestion_log (tracking table for pipeline runs)
"""
import argparse
import csv
import hashlib
import io
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from config import COLUMN_MAP, DB_PATH, RAW_DIR


# ── Helpers ────────────────────────────────────────────────────────────────

def clean_str(val: str) -> str | None:
    """Strip whitespace and stray quote characters from OCPR export artifacts."""
    if not val:
        return None
    cleaned = val.strip().strip('"').strip()
    return cleaned or None


def parse_amount(raw: str) -> float | None:
    if not raw:
        return None
    cleaned = raw.replace("$", "").replace(",", "").replace(" ", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_date(raw: str) -> str | None:
    if not raw or raw.strip() in ("-", "N/A", ""):
        return None
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return raw.strip()


def parse_cancelled(raw: str) -> int:
    return 1 if raw and raw.strip().upper() in ("SÍ", "SI", "YES", "S", "Y", "1") else 0


def row_hash(row: dict) -> str:
    key = "|".join([
        row.get("contract_number") or "",
        row.get("entity") or "",
        row.get("contractor") or "",
        row.get("award_date") or "",
        str(row.get("amount") or ""),
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def detect_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            with open(path, encoding=enc) as f:
                f.read(4096)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def resolve_header(csv_headers: list[str], canonical: str) -> str | None:
    candidates = COLUMN_MAP.get(canonical, [])
    for candidate in candidates:
        for h in csv_headers:
            if h.strip().lower() == candidate.lower():
                return h.strip()
    return None


def fiscal_year_from_filename(path: Path) -> str:
    """Extract '2023-2024' from 'contratos_2023-2024.csv'."""
    for part in path.stem.split("_"):
        if "-" in part and len(part) == 9:
            return part
    return path.stem


# ── Schema ─────────────────────────────────────────────────────────────────

def create_schema(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts (
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
            inserted_at         TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_entity       ON contracts(entity);
        CREATE INDEX IF NOT EXISTS idx_contractor   ON contracts(contractor);
        CREATE INDEX IF NOT EXISTS idx_amount       ON contracts(amount);
        CREATE INDEX IF NOT EXISTS idx_award_date   ON contracts(award_date);
        CREATE INDEX IF NOT EXISTS idx_fiscal_year  ON contracts(fiscal_year);
        CREATE INDEX IF NOT EXISTS idx_contract_no  ON contracts(contract_number);
        CREATE INDEX IF NOT EXISTS idx_service_cat  ON contracts(service_category);

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
    conn.commit()


# ── Ingest ─────────────────────────────────────────────────────────────────

def ingest_csv(conn: sqlite3.Connection, csv_path: Path, fiscal_year: str):
    print(f"\n  [ingest] {csv_path.name} (fiscal year: {fiscal_year})")

    encoding = detect_encoding(csv_path)
    with open(csv_path, encoding=encoding, newline="") as f:
        content = f.read()

    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames or []
    print(f"    columns: {headers}")

    col = {canonical: resolve_header(headers, canonical) for canonical in COLUMN_MAP}
    missing = [c for c, v in col.items() if v is None]
    if missing:
        print(f"    [warn] not found: {missing}")

    now = datetime.now(timezone.utc).isoformat()
    rows_parsed = rows_new = rows_dup = 0

    insert_sql = """
        INSERT OR IGNORE INTO contracts (
            row_hash, contract_number, entity, entity_number, contractor,
            amendment, service_category, service_type, amount, amount_receivable,
            award_date, valid_from, valid_to, procurement_method, fund_type,
            pco_number, cancelled, document_url, fiscal_year, inserted_at
        ) VALUES (
            :row_hash, :contract_number, :entity, :entity_number, :contractor,
            :amendment, :service_category, :service_type, :amount, :amount_receivable,
            :award_date, :valid_from, :valid_to, :procurement_method, :fund_type,
            :pco_number, :cancelled, :document_url, :fiscal_year, :inserted_at
        )
    """

    batch = []
    for raw_row in reader:
        rows_parsed += 1

        def get(canonical):
            key = col.get(canonical)
            return (raw_row.get(key) or "").strip() if key else ""

        record = {
            "contract_number":    clean_str(get("contract_number")),
            "entity":             clean_str(get("entity")),
            "entity_number":      clean_str(get("entity_number")),
            "contractor":         clean_str(get("contractor")),
            "amendment":          clean_str(get("amendment")),
            "service_category":   clean_str(get("service_category")),
            "service_type":       clean_str(get("service_type")),
            "amount":             parse_amount(get("amount")),
            "amount_receivable":  parse_amount(get("amount_receivable")),
            "award_date":         parse_date(get("award_date")),
            "valid_from":         parse_date(get("valid_from")),
            "valid_to":           parse_date(get("valid_to")),
            "procurement_method": clean_str(get("procurement_method")),
            "fund_type":          clean_str(get("fund_type")),
            "pco_number":         clean_str(get("pco_number")),
            "cancelled":          parse_cancelled(get("cancelled")),
            "document_url":       clean_str(get("document_url")),
            "fiscal_year":        fiscal_year,
            "inserted_at":        now,
        }
        record["row_hash"] = row_hash(record)
        batch.append(record)

        if len(batch) >= 500:
            result = conn.executemany(insert_sql, batch)
            rows_new += result.rowcount
            rows_dup += len(batch) - result.rowcount
            batch.clear()

    if batch:
        result = conn.executemany(insert_sql, batch)
        rows_new += result.rowcount
        rows_dup += len(batch) - result.rowcount

    conn.commit()

    # Rebuild FTS index
    conn.execute("INSERT INTO contracts_fts(contracts_fts) VALUES('rebuild')")
    conn.commit()

    conn.execute(
        """INSERT INTO ingestion_log
           (fiscal_year, csv_file, rows_parsed, rows_new, rows_dup, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (fiscal_year, str(csv_path), rows_parsed, rows_new, rows_dup, now),
    )
    conn.commit()

    print(f"    parsed={rows_parsed}  new={rows_new}  duplicates={rows_dup}")
    return rows_parsed, rows_new, rows_dup


def print_summary(conn: sqlite3.Connection):
    total = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
    total_amount = conn.execute(
        "SELECT SUM(amount) FROM contracts WHERE amount IS NOT NULL"
    ).fetchone()[0] or 0

    print(f"\n{'='*60}")
    print(f"  Total contracts : {total:,}")
    print(f"  Total value     : ${total_amount:,.2f}")

    print("\n  Top 10 entities by contract count:")
    for row in conn.execute(
        "SELECT entity, COUNT(*) as n FROM contracts GROUP BY entity ORDER BY n DESC LIMIT 10"
    ).fetchall():
        print(f"    {(row[0] or '(unknown)'):<50} {row[1]:>6}")

    print("\n  By fiscal year:")
    for row in conn.execute(
        "SELECT fiscal_year, COUNT(*), COALESCE(SUM(amount),0) "
        "FROM contracts GROUP BY fiscal_year ORDER BY fiscal_year DESC"
    ).fetchall():
        print(f"    {row[0]}   contracts={row[1]:>7}   value=${row[2]:>18,.0f}")

    print(f"{'='*60}\n")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest OCPR CSVs into SQLite")
    parser.add_argument("--csv-dir", default=str(RAW_DIR))
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--reset", action="store_true", help="Drop and recreate the database")
    args = parser.parse_args()

    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if args.reset and db_path.exists():
        db_path.unlink()
        print(f"[reset] Deleted {db_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")
    create_schema(conn)

    csv_files = sorted(Path(args.csv_dir).glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {args.csv_dir}. Run download.py first.")
        return

    print(f"\nIngesting {len(csv_files)} CSV file(s) into {db_path}\n")
    total_new = 0

    for csv_path in csv_files:
        fiscal_year = fiscal_year_from_filename(csv_path)
        _, rows_new, _ = ingest_csv(conn, csv_path, fiscal_year)
        total_new += rows_new

    print_summary(conn)
    conn.close()
    print(f"Database saved to: {db_path}")


if __name__ == "__main__":
    main()
