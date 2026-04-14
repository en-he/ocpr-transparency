"""
Tier 2 -- Monitor OCPR for new contracts and sync into SQLite.

The live registry now serves the public search results through an anti-forgery
protected JSON endpoint, so this monitor boots a browser-style session, pages
through recent search results, and enriches newly seen rows from the detail page
when possible before inserting them.
"""
from __future__ import annotations

import argparse
import json
import os
import smtplib
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path

from config import DB_PATH, STATE_FILE
from contract_utils import LIVE_MONITOR_SOURCE_TYPE, create_schema, insert_contract_rows
from live_recovery import (
    OCPRContractRegistryClient,
    build_detail_url,
    enrich_from_search_row,
    normalize_search_result_row,
    parse_contract_detail_html,
)


API_PAGE_SIZE = 100
MAX_STALE_PAGES = 5

STATE_DEFAULTS = {
    "last_success_at": None,
    "last_since_used": None,
    "last_new_count": 0,
}


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


def record_exists(conn: sqlite3.Connection, row_hash_value: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM contracts WHERE row_hash = ? LIMIT 1",
            (row_hash_value,),
        ).fetchone()
    )


def enrich_monitor_record(
    client: OCPRContractRegistryClient,
    search_row: dict,
    search_record: dict,
    inserted_at: str,
) -> dict:
    source_url = search_record.get("source_url")
    if not source_url:
        return search_record
    try:
        detail_html = client.fetch_detail_html(source_url)
        parsed = parse_contract_detail_html(
            detail_html,
            source_url=source_url,
            captured_at=inserted_at,
        )
        detail_record = enrich_from_search_row(
            parsed.record,
            search_row,
            source_type=LIVE_MONITOR_SOURCE_TYPE,
        )
        detail_record["inserted_at"] = inserted_at
        return detail_record
    except Exception as exc:
        print(f"    [warn] detail enrichment failed for {source_url}: {exc}")
        return search_record


def send_email_alert(new_rows: list[dict], to_addr: str):
    host = os.getenv("SMTP_HOST", "localhost")
    user = os.getenv("SMTP_USER", "")
    password = os.getenv("SMTP_PASS", "")
    from_addr = os.getenv("SMTP_FROM", user or "contratos@ocpr-transparency.org")

    lines = [
        f"OCPR Transparency - {len(new_rows)} new contract(s) found\n",
        "=" * 60,
    ]
    for row in new_rows[:50]:
        amount_str = f"${row['amount']:,.2f}" if row.get("amount") else "N/A"
        lines.append(
            f"\nEntity:     {row.get('entity')}"
            f"\nContractor: {row.get('contractor')}"
            f"\nAmount:     {amount_str}"
            f"\nDate:       {row.get('award_date')}"
            f"\nContract#:  {row.get('contract_number')}"
            f"\nSource:     {row.get('source_url') or build_detail_url(row.get('source_contract_id') or '')}"
        )
        lines.append("-" * 40)
    if len(new_rows) > 50:
        lines.append(f"\n... and {len(new_rows) - 50} more.")

    msg = MIMEText("\n".join(lines))
    msg["Subject"] = f"[OCPR Transparency] {len(new_rows)} new contract(s)"
    msg["From"] = from_addr
    msg["To"] = to_addr

    try:
        with smtplib.SMTP(host, 587) as smtp:
            if user:
                smtp.starttls()
                smtp.login(user, password)
            smtp.send_message(msg)
        print(f"  [email] alert sent to {to_addr}")
    except Exception as exc:
        print(f"  [email] failed: {exc}")


def run(since_date: str, dry_run: bool, notify: str | None) -> dict:
    inserted_at = datetime.now(timezone.utc).isoformat()
    print(f"\nMonitor run: {inserted_at}")
    print(f"Fetching contracts since: {since_date}\n")

    client = OCPRContractRegistryClient()
    conn = None
    if not dry_run:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        create_schema(conn)

    all_new: list[dict] = []
    stale_streak = 0
    success = True

    try:
        for page_number, rows in enumerate(client.iter_recent_contract_rows(award_date_from=since_date, page_size=API_PAGE_SIZE), start=1):
            print(f"  Page {page_number}: {len(rows)} rows")
            page_new: list[dict] = []

            for search_row in rows:
                search_record = normalize_search_result_row(
                    search_row,
                    inserted_at=inserted_at,
                    source_type=LIVE_MONITOR_SOURCE_TYPE,
                )

                if conn and record_exists(conn, search_record["row_hash"]):
                    continue

                enriched = enrich_monitor_record(client, search_row, search_record, inserted_at)

                if dry_run:
                    page_new.append(enriched)
                elif conn:
                    inserted_rows = insert_contract_rows(conn, [enriched], rebuild_fts=False)
                    if inserted_rows:
                        page_new.extend(inserted_rows)

            if page_new and conn:
                conn.execute("INSERT INTO contracts_fts(contracts_fts) VALUES('rebuild')")
                conn.commit()

            all_new.extend(page_new)
            print(f"    -> {len(page_new)} new / {len(rows) - len(page_new)} already known")

            if len(page_new) == 0:
                stale_streak += 1
                if stale_streak >= MAX_STALE_PAGES:
                    print(f"\n  {MAX_STALE_PAGES} consecutive pages with 0 new contracts — stopping.")
                    break
            else:
                stale_streak = 0

            if len(rows) < API_PAGE_SIZE:
                break

            time.sleep(1.0)
    except Exception as exc:
        success = False
        print(f"\n  [err] monitor run failed: {exc}")
    finally:
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
