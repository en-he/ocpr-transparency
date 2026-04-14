"""
Ingest OCPR CSVs into the normalized SQLite database.

Usage:
    python pipeline/ingest.py
    python pipeline/ingest.py --csv-dir data/raw --db data/db/contratos.db
    python pipeline/ingest.py --reset
"""
from __future__ import annotations

import argparse
import csv
import io
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from config import COLUMN_MAP, DB_PATH, RAW_DIR, REPO_ROOT
from contract_utils import (
    CONTRACT_INSERT_SQL,
    RAW_SOURCE_TYPE,
    clean_str,
    create_schema,
    normalize_contract_record,
    parse_amount,
    parse_cancelled,
    parse_date,
)


RECOVERY_CSV_PATH = REPO_ROOT / "data" / "recovery" / "live_recovered_contracts.csv"


def detect_encoding(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            with open(path, encoding=encoding) as fh:
                fh.read(4096)
            return encoding
        except UnicodeDecodeError:
            continue
    return "latin-1"


def resolve_header(csv_headers: list[str], canonical: str) -> str | None:
    candidates = COLUMN_MAP.get(canonical, [])
    for candidate in candidates:
        for header in csv_headers:
            if header.strip().lower() == candidate.lower():
                return header.strip()
    return None


def fiscal_year_from_filename(path: Path) -> str:
    for part in path.stem.split("_"):
        if "-" in part and len(part) == 9:
            return part
    return path.stem


def ingest_raw_csv(conn: sqlite3.Connection, csv_path: Path, fiscal_year: str):
    print(f"\n  [ingest] {csv_path.name} (fiscal year: {fiscal_year})")

    encoding = detect_encoding(csv_path)
    with open(csv_path, encoding=encoding, newline="") as fh:
        content = fh.read()

    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames or []
    print(f"    columns: {headers}")

    column_lookup = {canonical: resolve_header(headers, canonical) for canonical in COLUMN_MAP}
    missing = [canonical for canonical, value in column_lookup.items() if value is None]
    if missing:
        print(f"    [warn] not found: {missing}")

    inserted_at = datetime.now(timezone.utc).isoformat()
    rows_parsed = rows_new = rows_dup = 0
    batch: list[dict] = []

    for raw_row in reader:
        rows_parsed += 1

        def get(canonical: str) -> str:
            key = column_lookup.get(canonical)
            return (raw_row.get(key) or "").strip() if key else ""

        batch.append(
            normalize_contract_record(
                {
                    "contract_number": get("contract_number"),
                    "entity": get("entity"),
                    "entity_number": get("entity_number"),
                    "contractor": get("contractor"),
                    "amendment": get("amendment"),
                    "service_category": get("service_category"),
                    "service_type": get("service_type"),
                    "amount": parse_amount(get("amount")),
                    "amount_receivable": parse_amount(get("amount_receivable")),
                    "award_date": parse_date(get("award_date")),
                    "valid_from": parse_date(get("valid_from")),
                    "valid_to": parse_date(get("valid_to")),
                    "procurement_method": clean_str(get("procurement_method")),
                    "fund_type": clean_str(get("fund_type")),
                    "pco_number": clean_str(get("pco_number")),
                    "cancelled": parse_cancelled(get("cancelled")),
                    "document_url": clean_str(get("document_url")),
                    "fiscal_year": fiscal_year,
                    "source_type": RAW_SOURCE_TYPE,
                    "source_url": None,
                    "source_contract_id": None,
                    "inserted_at": inserted_at,
                },
                default_source_type=RAW_SOURCE_TYPE,
                inserted_at=inserted_at,
            )
        )

        if len(batch) >= 500:
            result = conn.executemany(CONTRACT_INSERT_SQL, batch)
            rows_new += result.rowcount
            rows_dup += len(batch) - result.rowcount
            batch.clear()

    if batch:
        result = conn.executemany(CONTRACT_INSERT_SQL, batch)
        rows_new += result.rowcount
        rows_dup += len(batch) - result.rowcount

    conn.commit()
    conn.execute("INSERT INTO contracts_fts(contracts_fts) VALUES('rebuild')")
    conn.commit()

    conn.execute(
        """
        INSERT INTO ingestion_log (
            fiscal_year, csv_file, rows_parsed, rows_new, rows_dup, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (fiscal_year, str(csv_path), rows_parsed, rows_new, rows_dup, inserted_at),
    )
    conn.commit()

    print(f"    parsed={rows_parsed}  new={rows_new}  duplicates={rows_dup}")
    return rows_parsed, rows_new, rows_dup


def ingest_recovery_csv(conn: sqlite3.Connection, csv_path: Path):
    print(f"\n  [ingest] {csv_path.name} (supplemental live recovery)")
    if not csv_path.exists():
        print("    [skip] recovery file not found")
        return 0, 0, 0

    with open(csv_path, encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows_parsed = rows_new = rows_dup = 0
        batch: list[dict] = []

        for raw_row in reader:
            rows_parsed += 1
            batch.append(
                normalize_contract_record(
                    raw_row,
                    default_source_type=raw_row.get("source_type") or "live_recovery",
                    inserted_at=raw_row.get("inserted_at") or datetime.now(timezone.utc).isoformat(),
                )
            )

            if len(batch) >= 500:
                result = conn.executemany(CONTRACT_INSERT_SQL, batch)
                rows_new += result.rowcount
                rows_dup += len(batch) - result.rowcount
                batch.clear()

        if batch:
            result = conn.executemany(CONTRACT_INSERT_SQL, batch)
            rows_new += result.rowcount
            rows_dup += len(batch) - result.rowcount

    conn.commit()
    conn.execute("INSERT INTO contracts_fts(contracts_fts) VALUES('rebuild')")
    conn.commit()

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO ingestion_log (
            fiscal_year, csv_file, rows_parsed, rows_new, rows_dup, ingested_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("recovery", str(csv_path), rows_parsed, rows_new, rows_dup, now),
    )
    conn.commit()

    print(f"    parsed={rows_parsed}  new={rows_new}  duplicates={rows_dup}")
    return rows_parsed, rows_new, rows_dup


def print_summary(conn: sqlite3.Connection):
    total = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
    total_amount = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM contracts WHERE amount IS NOT NULL"
    ).fetchone()[0]

    print(f"\n{'=' * 60}")
    print(f"  Total contracts : {total:,}")
    print(f"  Total value     : ${total_amount:,.2f}")

    print("\n  Top 10 entities by contract count:")
    for row in conn.execute(
        "SELECT entity, COUNT(*) AS n FROM contracts GROUP BY entity ORDER BY n DESC LIMIT 10"
    ).fetchall():
        print(f"    {(row[0] or '(unknown)'):<50} {row[1]:>6}")

    print("\n  By fiscal year:")
    for row in conn.execute(
        "SELECT fiscal_year, COUNT(*), COALESCE(SUM(amount), 0) "
        "FROM contracts GROUP BY fiscal_year ORDER BY fiscal_year DESC"
    ).fetchall():
        print(f"    {row[0]}   contracts={row[1]:>7}   value=${row[2]:>18,.0f}")

    print(f"{'=' * 60}\n")


def main():
    parser = argparse.ArgumentParser(description="Ingest OCPR CSVs into SQLite")
    parser.add_argument("--csv-dir", default=str(RAW_DIR))
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--reset", action="store_true", help="Drop and recreate the database")
    parser.add_argument(
        "--recovery-csv",
        default=str(RECOVERY_CSV_PATH),
        help="Tracked supplemental recovery CSV to ingest after raw fiscal-year CSVs",
    )
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

    for csv_path in csv_files:
        ingest_raw_csv(conn, csv_path, fiscal_year_from_filename(csv_path))

    ingest_recovery_csv(conn, Path(args.recovery_csv))

    print_summary(conn)
    conn.close()
    print(f"Database saved to: {db_path}")


if __name__ == "__main__":
    main()
