"""
Build static site artifacts from the full OCPR SQLite database.

Creates:
    - data/db/contratos-full.db.gz (full downloadable open-data DB)
    - site/contratos.db.gz         (smaller browser-serving DB)
    - site/data-manifest.json      (artifact metadata for the frontend)
"""
import argparse
import hashlib
import json
import shutil
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from config import DB_PATH, REPO_ROOT
from ingest import parse_date


DEFAULT_REPO_RAW_BASE = "https://github.com/en-he/ocpr-transparency/raw/main"
DEFAULT_FULL_DOWNLOAD_URL = (
    "https://github.com/en-he/ocpr-transparency/releases/download/data-latest/contratos-full.db.gz"
)

BROWSER_COLUMNS = [
    "id",
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


def contractor_family_expr(alias: str = "c") -> str:
    col = f"{alias}.contractor"
    return (
        "TRIM("
        "REPLACE("
        "REPLACE("
        "REPLACE("
        "REPLACE("
        f"REPLACE(UPPER(COALESCE({col}, '')), '.', ''), ',', ''),"
        "';', ''),"
        "':', ''),"
        "CHAR(0), '')"
        ")"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def gzip_file(src: Path, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "wb") as output:
        subprocess.run(
            ["gzip", "-c", "-6", str(src)],
            check=True,
            stdout=output,
        )


def normalize_date(raw: str | None) -> str | None:
    return parse_date(raw) if raw else None


def normalize_text(raw: str | None) -> str | None:
    if raw is None:
        return None
    cleaned = str(raw).replace("\x00", "").strip()
    return cleaned or None


def normalize_source_db(source_db: Path):
    conn = sqlite3.connect(source_db)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        "SELECT id, award_date, valid_from, valid_to, amendment FROM contracts ORDER BY id"
    )

    while True:
        rows = cur.fetchmany(5000)
        if not rows:
            break

        batch = []
        for row in rows:
            award_date = normalize_date(row["award_date"])
            valid_from = normalize_date(row["valid_from"])
            valid_to = normalize_date(row["valid_to"])
            amendment = normalize_text(row["amendment"])
            if (
                award_date != row["award_date"]
                or valid_from != row["valid_from"]
                or valid_to != row["valid_to"]
                or amendment != row["amendment"]
            ):
                batch.append((award_date, valid_from, valid_to, amendment, row["id"]))

        if batch:
            conn.executemany(
                "UPDATE contracts SET award_date = ?, valid_from = ?, valid_to = ?, amendment = ? WHERE id = ?",
                batch,
            )
            conn.commit()

    conn.close()


def create_browser_schema(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE contracts (
            id                  INTEGER PRIMARY KEY,
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
            fiscal_year         TEXT
        );

        CREATE INDEX idx_browser_entity       ON contracts(entity);
        CREATE INDEX idx_browser_contractor   ON contracts(contractor);
        CREATE INDEX idx_browser_amount       ON contracts(amount);
        CREATE INDEX idx_browser_award_date   ON contracts(award_date);
        CREATE INDEX idx_browser_fiscal_year  ON contracts(fiscal_year);
        CREATE INDEX idx_browser_contract_no  ON contracts(contract_number);
        CREATE INDEX idx_browser_service_cat  ON contracts(service_category);

        CREATE VIRTUAL TABLE contracts_fts USING fts5(
            contract_number,
            entity,
            contractor,
            service_category,
            service_type,
            content='contracts',
            content_rowid='id'
        );
    """)
    conn.commit()


def assert_integrity(conn: sqlite3.Connection, label: str):
    status = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if status != "ok":
        raise RuntimeError(f"{label} integrity check failed: {status}")


def assert_db_path(path: Path, label: str):
    conn = sqlite3.connect(path)
    try:
        assert_integrity(conn, label)
    finally:
        conn.close()


def checkpoint_db(path: Path):
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()


def build_browser_db(source_db: Path, browser_db: Path):
    if browser_db.exists():
        browser_db.unlink()

    src = sqlite3.connect(f"file:{source_db}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row

    dst = sqlite3.connect(browser_db)
    dst.execute("PRAGMA journal_mode=DELETE")
    dst.execute("PRAGMA synchronous=NORMAL")
    dst.execute("PRAGMA temp_store=MEMORY")
    create_browser_schema(dst)

    select_sql = f"SELECT {', '.join(BROWSER_COLUMNS)} FROM contracts ORDER BY id"
    insert_sql = """
        INSERT INTO contracts (
            id, contract_number, entity, entity_number, contractor, amendment,
            service_category, service_type, amount, amount_receivable,
            award_date, valid_from, valid_to, procurement_method, fund_type,
            pco_number, cancelled, document_url, fiscal_year
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    cur = src.execute(select_sql)
    while True:
        rows = cur.fetchmany(5000)
        if not rows:
            break

        batch = []
        for row in rows:
            batch.append((
                row["id"],
                normalize_text(row["contract_number"]),
                normalize_text(row["entity"]),
                normalize_text(row["entity_number"]),
                normalize_text(row["contractor"]),
                normalize_text(row["amendment"]),
                normalize_text(row["service_category"]),
                normalize_text(row["service_type"]),
                row["amount"],
                row["amount_receivable"],
                normalize_date(row["award_date"]),
                normalize_date(row["valid_from"]),
                normalize_date(row["valid_to"]),
                normalize_text(row["procurement_method"]),
                normalize_text(row["fund_type"]),
                normalize_text(row["pco_number"]),
                row["cancelled"],
                normalize_text(row["document_url"]),
                normalize_text(row["fiscal_year"]),
            ))

        dst.executemany(insert_sql, batch)
        dst.commit()

    dst.execute("INSERT INTO contracts_fts(contracts_fts) VALUES('rebuild')")
    dst.commit()
    dst.execute("VACUUM")
    assert_integrity(dst, "browser DB")
    dst.close()
    src.close()
    assert_db_path(browser_db, "browser DB file")


def collect_stats(browser_db: Path) -> dict:
    conn = sqlite3.connect(f"file:{browser_db}?mode=ro", uri=True)
    row_count = conn.execute("SELECT COUNT(*) FROM contracts").fetchone()[0]
    total_amount = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) FROM contracts WHERE amount IS NOT NULL"
    ).fetchone()[0]
    fiscal_years = [
        row[0] for row in conn.execute(
            "SELECT DISTINCT fiscal_year FROM contracts WHERE fiscal_year IS NOT NULL ORDER BY fiscal_year DESC"
        ).fetchall()
    ]
    conn.close()
    return {
        "row_count": row_count,
        "total_amount": total_amount,
        "fiscal_years": fiscal_years,
    }


def collect_dashboard(browser_db: Path) -> dict:
    conn = sqlite3.connect(f"file:{browser_db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    family_expr = contractor_family_expr("c")
    family_cte = f"""
        WITH ranked AS (
            SELECT
                c.*,
                {family_expr} AS contractor_family,
                ROW_NUMBER() OVER (
                    PARTITION BY c.contract_number, c.entity, {family_expr}
                    ORDER BY
                        CASE
                            WHEN NULLIF(TRIM(COALESCE(c.amendment, '')), '') IS NULL THEN 0
                            ELSE 1
                        END ASC,
                        c.award_date ASC,
                        c.id ASC
                ) AS representative_row
            FROM contracts c
        ),
        families AS (
            SELECT
                contract_number,
                entity,
                contractor_family,
                MAX(CASE WHEN representative_row = 1 THEN contractor END) AS contractor,
                MAX(CASE WHEN representative_row = 1 THEN fiscal_year END) AS fiscal_year,
                SUM(COALESCE(amount, 0)) AS family_total_amount
            FROM ranked
            GROUP BY contract_number, entity, contractor_family
        )
    """

    top_contractors = [
        {
            "name": row["name"],
            "family_count": int(row["family_count"] or 0),
            "total_amount": float(row["total_amount"] or 0),
        }
        for row in conn.execute(
            f"""{family_cte}
                SELECT
                    MAX(contractor) AS name,
                    COUNT(*) AS family_count,
                    COALESCE(SUM(family_total_amount), 0) AS total_amount
                FROM families
                WHERE contractor_family IS NOT NULL
                  AND contractor_family != ''
                GROUP BY contractor_family
                ORDER BY total_amount DESC, family_count DESC, name ASC
                LIMIT 5
            """
        ).fetchall()
    ]

    top_entities = [
        {
            "name": row["name"],
            "family_count": int(row["family_count"] or 0),
            "total_amount": float(row["total_amount"] or 0),
        }
        for row in conn.execute(
            f"""{family_cte}
                SELECT
                    entity AS name,
                    COUNT(*) AS family_count,
                    COALESCE(SUM(family_total_amount), 0) AS total_amount
                FROM families
                WHERE entity IS NOT NULL
                  AND TRIM(entity) != ''
                GROUP BY entity
                ORDER BY total_amount DESC, family_count DESC, name ASC
                LIMIT 5
            """
        ).fetchall()
    ]

    yearly_spending = [
        {
            "fiscal_year": row["fiscal_year"],
            "family_count": int(row["family_count"] or 0),
            "total_amount": float(row["total_amount"] or 0),
        }
        for row in conn.execute(
            f"""{family_cte}
                SELECT
                    fiscal_year,
                    COUNT(*) AS family_count,
                    COALESCE(SUM(family_total_amount), 0) AS total_amount
                FROM families
                WHERE fiscal_year IS NOT NULL
                  AND TRIM(fiscal_year) != ''
                GROUP BY fiscal_year
                ORDER BY fiscal_year ASC
            """
        ).fetchall()
    ]

    conn.close()
    return {
        "top_contractors": top_contractors,
        "top_entities": top_entities,
        "yearly_spending": yearly_spending,
    }


def write_manifest(
    manifest_path: Path,
    browser_gz: Path,
    full_gz: Path,
    repo_raw_base: str,
    full_download_url: str,
    stats: dict,
    dashboard: dict,
):
    now = datetime.now(timezone.utc).isoformat()
    manifest = {
        "generated_at": now,
        "row_count": stats["row_count"],
        "total_amount": stats["total_amount"],
        "fiscal_years": stats["fiscal_years"],
        "dashboard": dashboard,
        "raw_csv_base_url": f"{repo_raw_base}/data/raw/",
        "browser_db": {
            "url": browser_gz.name,
            "sha256": sha256_file(browser_gz),
            "size_bytes": browser_gz.stat().st_size,
            "format": "sqlite+gzip",
        },
        "full_download_db": {
            "url": full_download_url,
            "sha256": sha256_file(full_gz),
            "size_bytes": full_gz.stat().st_size,
            "format": "sqlite+gzip",
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Build browser DB and manifest for the static site")
    parser.add_argument("--source-db", default=str(DB_PATH))
    parser.add_argument("--browser-db", default=str(REPO_ROOT / "site" / "contratos.db"))
    parser.add_argument("--browser-gz", default=str(REPO_ROOT / "site" / "contratos.db.gz"))
    parser.add_argument("--full-gz", default=str(REPO_ROOT / "data" / "db" / "contratos-full.db.gz"))
    parser.add_argument("--manifest", default=str(REPO_ROOT / "site" / "data-manifest.json"))
    parser.add_argument("--repo-raw-base", default=DEFAULT_REPO_RAW_BASE)
    parser.add_argument("--full-download-url", default=DEFAULT_FULL_DOWNLOAD_URL)
    parser.add_argument("--normalize-source-db", action="store_true")
    args = parser.parse_args()

    source_db = Path(args.source_db)
    browser_db = Path(args.browser_db)
    browser_gz = Path(args.browser_gz)
    full_gz = Path(args.full_gz)
    manifest = Path(args.manifest)

    if args.normalize_source_db:
        print(f"Normalizing dates in source DB -> {source_db}")
        normalize_source_db(source_db)

    print(f"Building browser DB from {source_db}")
    build_browser_db(source_db, browser_db)

    checkpoint_db(source_db)

    print(f"Gzipping full DB -> {full_gz}")
    gzip_file(source_db, full_gz)

    print(f"Gzipping browser DB -> {browser_gz}")
    gzip_file(browser_db, browser_gz)

    stats = collect_stats(browser_db)
    dashboard = collect_dashboard(browser_db)
    print(f"Writing manifest -> {manifest}")
    write_manifest(
        manifest,
        browser_gz,
        full_gz,
        args.repo_raw_base.rstrip("/"),
        args.full_download_url,
        stats,
        dashboard,
    )

    if browser_db.exists():
        browser_db.unlink()

    print(
        f"Done. browser_rows={stats['row_count']:,} "
        f"browser_gz={browser_gz.stat().st_size / (1024 * 1024):.1f}MB "
        f"full_gz={full_gz.stat().st_size / (1024 * 1024):.1f}MB"
    )


if __name__ == "__main__":
    main()
