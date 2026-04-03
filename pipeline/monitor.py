"""
Tier 2 -- Monitor OCPR for new contracts and sync into the SQLite DB.

Polls the search endpoint for contracts published since the last run.
Designed to run as a nightly cron job or GitHub Actions scheduled workflow.

Usage:
    python pipeline/monitor.py
    python pipeline/monitor.py --since 2025-01-01
    python pipeline/monitor.py --dry-run
    python pipeline/monitor.py --notify email@example.com
"""
import argparse
import csv
import hashlib
import io
import json
import os
import smtplib
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path

import requests

from config import DB_PATH, HEADERS, SEARCH_URL, STATE_FILE
from ingest import create_schema


# The API ignores pageSize and returns its own fixed page size (~112 rows).
# We use this only for termination: fewer rows than this means last page.
API_PAGE_SIZE = 112
# Stop paginating after this many consecutive pages with zero new contracts.
MAX_STALE_PAGES = 5

STATE_DEFAULTS = {
    "last_success_at": None,
    "last_since_used": None,
    "last_new_count": 0,
}


# ── State ──────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        loaded = json.loads(STATE_FILE.read_text())
        if loaded.get("last_run") and not loaded.get("last_success_at"):
            loaded["last_success_at"] = loaded["last_run"]
        return {**STATE_DEFAULTS, **loaded}
    return dict(STATE_DEFAULTS)


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── Fetch ──────────────────────────────────────────────────────────────────

def build_search_payload(since_date: str, page: int = 1) -> dict:
    return {
        "entidadGubernamental": "",
        "numContrato": "",
        "contratista": "",
        "categoriaServicio": "",
        "tipoServicio": "",
        "cuantiaMin": "",
        "cuantiaMax": "",
        "fechaOtorgamientoDesde": since_date,
        "fechaOtorgamientoHasta": "",
        "fechaVigenciaDesde": "",
        "fechaVigenciaHasta": "",
        "numeroPco": "",
        "formaContratacion": "",
        "fondo": "",
        "page": page,
        "pageSize": API_PAGE_SIZE,
        "sortBy": "fechaOtorgamiento",
        "sortOrder": "desc",
    }


def fetch_page(since_date: str, page: int, dry_run: bool = False) -> tuple[list, int, bool]:
    if dry_run:
        print(f"    [dry-run] would fetch page {page} since {since_date}")
        return [], 0, True

    payload = build_search_payload(since_date, page)
    try:
        resp = requests.post(
            SEARCH_URL,
            json=payload,
            headers=HEADERS,
            timeout=60,
        )
        resp.raise_for_status()

        content = resp.content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        return rows, len(rows), True

    except requests.RequestException as e:
        print(f"    [err] fetch failed: {e}")
        return [], 0, False
    except Exception as e:
        print(f"    [err] parse failed: {e}")
        return [], 0, False


# ── Normalize ──────────────────────────────────────────────────────────────

def clean_str(val: str) -> str | None:
    if not val:
        return None
    cleaned = val.strip().strip('"').strip()
    return cleaned or None


def _to_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(str(val).replace("$", "").replace(",", "").strip())
    except ValueError:
        return None


def _to_date(val) -> str | None:
    if not val:
        return None
    raw = str(val).strip()
    if raw in ("", "-", "N/A", "0", "0.0", "0.00"):
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%m-%d-%Y",
        "%d-%m-%Y",
        "%m/%d/%Y",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(raw[:19], fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _fiscal_year_from_date(date_str) -> str | None:
    """PR fiscal year runs July 1 - June 30. 2024-10-15 -> '2024-2025'."""
    d = _to_date(date_str)
    if not d:
        return None
    try:
        dt = datetime.strptime(d, "%Y-%m-%d")
        if dt.month >= 7:
            return f"{dt.year}-{dt.year + 1}"
        else:
            return f"{dt.year - 1}-{dt.year}"
    except ValueError:
        return None


def normalize_api_row(raw: dict, fetched_at: str) -> dict:
    def get(key):
        return clean_str((raw.get(key) or "").strip())

    return {
        "contract_number":    get("Núm. Contrato"),
        "entity":             get("Entidad"),
        "entity_number":      None,
        "contractor":         get("Contratista"),
        "amendment":          get("Enmienda"),
        "service_category":   get("Categoría de Servicio"),
        "service_type":       get("Tipo de Servicio"),
        "amount":             _to_float(get("Cuantía")),
        "amount_receivable":  None,
        "award_date":         _to_date(get("Otorgado en")),
        "valid_from":         _to_date(get("Vigencia Desde")),
        "valid_to":           _to_date(get("Vigencia Hasta")),
        "procurement_method": get("Forma de Contratación"),
        "fund_type":          get("Fondo"),
        "pco_number":         get("Número PCo"),
        "cancelled":          1 if (get("Cancelado") or "").upper() in ("SÍ", "SI", "S") else 0,
        "document_url":       get("Documento"),
        "fiscal_year":        _fiscal_year_from_date(get("Otorgado en")),
        "inserted_at":        fetched_at,
    }


def row_hash(row: dict) -> str:
    key = "|".join([
        row.get("contract_number") or "",
        row.get("entity") or "",
        row.get("contractor") or "",
        row.get("award_date") or "",
        str(row.get("amount") or ""),
    ])
    return hashlib.sha256(key.encode()).hexdigest()[:16]


# ── DB ─────────────────────────────────────────────────────────────────────

def save_new_contracts(conn: sqlite3.Connection, rows: list[dict]) -> list[dict]:
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
    newly_inserted = []
    for row in rows:
        row["row_hash"] = row_hash(row)
        cur = conn.execute(insert_sql, row)
        if cur.rowcount > 0:
            newly_inserted.append(row)

    if newly_inserted:
        conn.execute("INSERT INTO contracts_fts(contracts_fts) VALUES('rebuild')")
    conn.commit()
    return newly_inserted


# ── Email ──────────────────────────────────────────────────────────────────

def send_email_alert(new_rows: list[dict], to_addr: str):
    host = os.getenv("SMTP_HOST", "localhost")
    user = os.getenv("SMTP_USER", "")
    password = os.getenv("SMTP_PASS", "")
    from_addr = os.getenv("SMTP_FROM", user or "contratos@ocpr-transparency.org")

    lines = [
        f"OCPR Transparency - {len(new_rows)} new contract(s) found\n",
        "=" * 60,
    ]
    for r in new_rows[:50]:
        amount_str = f"${r['amount']:,.2f}" if r.get("amount") else "N/A"
        lines.append(
            f"\nEntity:     {r.get('entity')}"
            f"\nContractor: {r.get('contractor')}"
            f"\nAmount:     {amount_str}"
            f"\nDate:       {r.get('award_date')}"
            f"\nContract#:  {r.get('contract_number')}"
        )
        lines.append("-" * 40)
    if len(new_rows) > 50:
        lines.append(f"\n... and {len(new_rows) - 50} more.")

    msg = MIMEText("\n".join(lines))
    msg["Subject"] = f"[OCPR Transparency] {len(new_rows)} new contract(s)"
    msg["From"] = from_addr
    msg["To"] = to_addr

    try:
        with smtplib.SMTP(host, 587) as s:
            if user:
                s.starttls()
                s.login(user, password)
            s.send_message(msg)
        print(f"  [email] alert sent to {to_addr}")
    except Exception as e:
        print(f"  [email] failed: {e}")


# ── Run ────────────────────────────────────────────────────────────────────

def run(since_date: str, dry_run: bool, notify: str | None) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    print(f"\nMonitor run: {now}")
    print(f"Fetching contracts since: {since_date}\n")

    conn = None
    if not dry_run:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        create_schema(conn)
        # Migrate: add any columns present in the INSERT but missing from the table
        existing = {r[1] for r in conn.execute("PRAGMA table_info(contracts)").fetchall()}
        expected = [
            "row_hash", "contract_number", "entity", "entity_number",
            "contractor", "amendment", "service_category", "service_type",
            "amount", "amount_receivable", "award_date", "valid_from",
            "valid_to", "procurement_method", "fund_type", "pco_number",
            "cancelled", "document_url", "fiscal_year", "inserted_at",
        ]
        for col in expected:
            if col not in existing:
                conn.execute(f"ALTER TABLE contracts ADD COLUMN {col} TEXT")

    all_new = []
    page = 1
    stale_streak = 0
    success = True

    while True:
        rows, count, ok = fetch_page(since_date, page, dry_run=dry_run)
        if not ok:
            success = False
            break
        if not rows:
            break

        normalized = [normalize_api_row(r, now) for r in rows]
        print(f"  Page {page}: {len(normalized)} rows")

        if conn:
            new = save_new_contracts(conn, normalized)
            all_new.extend(new)
            print(f"    -> {len(new)} new / {len(normalized) - len(new)} already known")

            # Stop early: if several consecutive pages have 0 new contracts,
            # we've passed the delta boundary — no need to scan the full DB.
            if len(new) == 0:
                stale_streak += 1
                if stale_streak >= MAX_STALE_PAGES:
                    print(f"\n  {MAX_STALE_PAGES} consecutive pages with 0 new contracts — stopping.")
                    break
            else:
                stale_streak = 0

        if count < API_PAGE_SIZE:
            break
        page += 1
        time.sleep(1.0)

    if conn:
        conn.close()

    print(f"\nTotal new contracts: {len(all_new)}")

    if all_new and notify:
        send_email_alert(all_new, notify)

    return {
        "new_count": len(all_new),
        "success": success,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "since_date": since_date,
    }


def main():
    global DB_PATH
    parser = argparse.ArgumentParser(description="Monitor OCPR for new contracts")
    parser.add_argument("--since", help="Date to look back from (YYYY-MM-DD)")
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--notify", metavar="EMAIL", help="Email address for alerts")
    parser.add_argument("--days-back", type=int, default=7, help="Days to look back if no state")
    args = parser.parse_args()

    DB_PATH = Path(args.db)

    state = load_state()

    if args.since:
        since = args.since
        print(f"Using explicit --since value: {since}")
    elif state.get("last_success_at"):
        since = state["last_success_at"][:10]
        print(f"Loaded previous state. Continuing from {since}.")
    else:
        since = (datetime.now() - timedelta(days=args.days_back)).date().isoformat()
        print(f"No previous state. Looking back {args.days_back} days to {since}.")

    result = run(since, args.dry_run, args.notify)

    if not args.dry_run and result["success"]:
        state["last_success_at"] = result["finished_at"]
        state["last_since_used"] = result["since_date"]
        state["last_new_count"] = result["new_count"]
        save_state(state)
        print(f"State saved -> {STATE_FILE}")
    elif not args.dry_run:
        print("State not updated because the run did not complete successfully.")


if __name__ == "__main__":
    main()
