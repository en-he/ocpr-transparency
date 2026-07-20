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
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from bulk_observations import (
    generate_bulk_observations,
    insert_bulk_observations,
    project_bulk_observations,
)
from config import (
    ARCHIVED_ONLY_FISCAL_YEARS,
    COLUMN_MAP,
    DB_PATH,
    RAW_DIR,
    REPO_ROOT,
)
from contract_utils import (
    CONTRACT_INSERT_SQL,
    create_schema,
    normalize_contract_record,
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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _certification_metadata(csv_path: Path, fiscal_year: str) -> dict:
    """Load tracked metadata only when it identifies these exact source bytes."""
    report_path = (
        REPO_ROOT / "data" / "certification" / "reports" / f"{fiscal_year}.json"
    )
    if not report_path.is_file():
        return {}
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    if payload.get("sha256") != _sha256_file(csv_path):
        return {}
    return payload


def _portable_source_path(csv_path: Path) -> str:
    try:
        return csv_path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return csv_path.name


def ingest_raw_csv(
    conn: sqlite3.Connection,
    csv_path: Path,
    fiscal_year: str,
    *,
    index_fts: bool = True,
    manage_transaction: bool = True,
):
    print(f"\n  [ingest] {csv_path.name} (fiscal year: {fiscal_year})")
    metadata = _certification_metadata(csv_path, fiscal_year)
    source_channel = metadata.get("source_channel") or (
        "archive_bulk"
        if fiscal_year in ARCHIVED_ONLY_FISCAL_YEARS
        else "official_bulk"
    )
    batch = generate_bulk_observations(
        csv_path,
        fiscal_year=fiscal_year,
        source_channel=source_channel,
        source_url=metadata.get("source_url"),
        archive_url=metadata.get("archive_url"),
        capture_time=metadata.get("capture_time"),
        capture_time_status=metadata.get("capture_time_status", "unknown"),
        http_status=metadata.get("http_status"),
        content_type=metadata.get("content_type"),
        requested_url=metadata.get("source_url"),
        final_url=metadata.get("source_url"),
    )
    inserted_at = batch.evidence.captured_at

    def apply():
        insert_bulk_observations(conn, batch, manage_transaction=False)
        projection = project_bulk_observations(
            conn,
            batch,
            inserted_at=inserted_at,
            index_fts=index_fts,
        )

        exclusion_counts = dict(projection.exclusion_reason_counts)
        for reason, count in batch.report.quarantine_reason_counts.items():
            exclusion_counts[f"parser_{reason}"] = count
        conn.execute(
            """
            INSERT INTO ingestion_log (
                fiscal_year, csv_file, rows_parsed, rows_new, rows_dup,
                ingested_at, observations_total, canonical_excluded,
                exclusions_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fiscal_year,
                _portable_source_path(csv_path),
                len(batch),
                projection.rows_new,
                projection.rows_duplicate,
                inserted_at,
                len(batch),
                projection.rows_duplicate + projection.rows_ineligible,
                json.dumps(
                    exclusion_counts,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
        )
        return projection

    if manage_transaction:
        with conn:
            projection = apply()
    else:
        projection = apply()

    print(
        "    "
        f"observations={len(batch)}  new={projection.rows_new}  "
        f"duplicates={projection.rows_duplicate}  "
        f"quarantined={projection.rows_ineligible}  "
        f"existing={projection.rows_existing}"
    )
    return len(batch), projection.rows_new, projection.rows_duplicate


def ingest_bulk_csvs(conn: sqlite3.Connection, csv_files: list[Path]):
    """Ingest a complete bulk set atomically and rebuild external FTS once."""
    results = []
    rows_new = 0
    with conn:
        for csv_path in csv_files:
            result = ingest_raw_csv(
                conn,
                csv_path,
                fiscal_year_from_filename(csv_path),
                index_fts=False,
                manage_transaction=False,
            )
            results.append(result)
            rows_new += result[1]
        if rows_new:
            conn.execute("INSERT INTO contracts_fts(contracts_fts) VALUES('rebuild')")
    return results


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
        ("recovery", _portable_source_path(csv_path), rows_parsed, rows_new, rows_dup, now),
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

    ingest_bulk_csvs(conn, csv_files)

    ingest_recovery_csv(conn, Path(args.recovery_csv))

    print_summary(conn)
    conn.close()
    print(f"Database saved to: {db_path}")


if __name__ == "__main__":
    main()
