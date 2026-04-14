"""
Recover missing original contract rows from the live OCPR registry.

Usage:
    python pipeline/recover_live_parents.py
    python pipeline/recover_live_parents.py --dry-run
    python pipeline/recover_live_parents.py --contract-number 2022-000019
"""
from __future__ import annotations

import argparse
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from config import DB_PATH, REPO_ROOT
from contract_utils import LIVE_RECOVERY_SOURCE_TYPE, create_schema
from live_recovery import (
    OCPRContractRegistryClient,
    RecoveryTarget,
    build_detail_url,
    detect_recovery_conflict,
    enrich_from_search_row,
    extract_contract_id,
    filter_search_candidates,
    format_note,
    load_recovered_rows,
    load_recovery_targets,
    parse_contract_detail_html,
    records_equivalent,
    sort_recovery_targets,
    upsert_recovered_row,
    validate_detail_match,
    write_recovered_rows,
    write_recovery_targets,
)


DEFAULT_TARGETS_CSV = REPO_ROOT / "data" / "recovery" / "live_recovery_targets.csv"
DEFAULT_RECOVERED_CSV = REPO_ROOT / "data" / "recovery" / "live_recovered_contracts.csv"
ALLOWED_LOOKUP_MODES = {"manual_url", "auto_discover"}
ALLOWED_STATUSES = {"pending", "recovered", "unrecoverable", "ambiguous"}
PENDING_GUARD_LIMIT = 100


def should_process_target(
    target: RecoveryTarget,
    contract_number: str | None,
    selected_batches: set[str] | None = None,
    selected_statuses: set[str] | None = None,
) -> bool:
    if contract_number and target.contract_number != contract_number:
        return False
    allowed_statuses = selected_statuses or {"pending"}
    if target.status not in allowed_statuses:
        return False
    if selected_batches and (target.recovery_batch or "") not in selected_batches:
        return False
    if target.lookup_mode not in ALLOWED_LOOKUP_MODES:
        return False
    if target.lookup_mode == "manual_url" and not target.source_url:
        return False
    return True


def select_targets_for_processing(
    targets: list[RecoveryTarget],
    *,
    contract_number: str | None,
    batches: list[str] | None,
    all_pending: bool,
    retry_statuses: list[str] | None = None,
    pending_guard_limit: int = PENDING_GUARD_LIMIT,
) -> list[RecoveryTarget]:
    selected_batches = {batch.strip() for batch in (batches or []) if batch and batch.strip()}
    selected_statuses = {"pending"}
    for status in retry_statuses or []:
        normalized = (status or "").strip()
        if not normalized:
            continue
        if normalized not in ALLOWED_STATUSES:
            raise ValueError(f"Unsupported retry status '{normalized}'")
        selected_statuses.add(normalized)

    if contract_number:
        return [
            target
            for target in sort_recovery_targets(targets)
            if should_process_target(target, contract_number, selected_batches or None, selected_statuses)
        ]

    matched_targets = [
        target
        for target in targets
        if should_process_target(target, contract_number=None, selected_statuses=selected_statuses)
    ]

    if selected_batches:
        return [
            target
            for target in sort_recovery_targets(matched_targets)
            if should_process_target(
                target,
                contract_number=None,
                selected_batches=selected_batches,
                selected_statuses=selected_statuses,
            )
        ]

    if all_pending or len(matched_targets) <= pending_guard_limit:
        return sort_recovery_targets(matched_targets)

    raise ValueError(
        f"Refusing to process {len(matched_targets)} matching targets without --batch or --all-pending. "
        "Use --batch 2022-2023 --batch 2021-2022 for the first active pass."
    )


def best_effort_manual_enrichment(
    client: OCPRContractRegistryClient,
    target: RecoveryTarget,
    record: dict,
) -> dict:
    source_contract_id = record.get("source_contract_id")
    if not source_contract_id:
        return record
    try:
        entity_id = client.resolve_entity_id(target.entity)
        if not entity_id:
            return record
        payload = client.search_contract_rows(
            contract_number=target.contract_number,
            entity_id=entity_id,
            length=100,
        )
        candidates = filter_search_candidates(payload.get("data") or [], target)
        for candidate in candidates:
            if str(candidate.get("ContractId")) == source_contract_id:
                return enrich_from_search_row(record, candidate, source_type=LIVE_RECOVERY_SOURCE_TYPE)
    except Exception:
        return record
    return record


def discover_candidates(client: OCPRContractRegistryClient, target: RecoveryTarget) -> tuple[list[dict], str]:
    entity_id = client.resolve_entity_id(target.entity)
    if not entity_id:
        return [], "entity lookup returned no exact match"

    payload = client.search_contract_rows(
        contract_number=target.contract_number,
        entity_id=entity_id,
        length=200,
    )
    candidates = filter_search_candidates(payload.get("data") or [], target)
    return candidates, f"search returned {len(candidates)} exact candidate(s)"


def process_target(
    target: RecoveryTarget,
    *,
    client: OCPRContractRegistryClient,
    conn: sqlite3.Connection | None,
    recovered_rows: list[dict],
    dry_run: bool,
) -> str:
    checked_at = datetime.now(timezone.utc).isoformat()
    target.notes = None

    try:
        if target.lookup_mode == "manual_url":
            source_url = target.source_url
            if not source_url:
                target.last_checked_at = checked_at
                target.notes = "manual_url target is missing source_url"
                return target.status
            candidates = [{
                "source_url": source_url,
                "source_contract_id": extract_contract_id(source_url),
                "search_row": None,
            }]
            discovery_note = "validated manual detail URL"
        else:
            search_candidates, discovery_note = discover_candidates(client, target)
            if not search_candidates:
                target.status = "unrecoverable"
                target.notes = discovery_note
                target.last_checked_at = checked_at
                return target.status
            candidates = [
                {
                    "source_url": build_detail_url(candidate["ContractId"]),
                    "source_contract_id": str(candidate["ContractId"]),
                    "search_row": candidate,
                }
                for candidate in search_candidates
            ]

        valid_records: list[dict] = []
        invalid_reasons: list[str] = []

        for candidate in candidates:
            detail_html = client.fetch_detail_html(candidate["source_url"])
            parsed = parse_contract_detail_html(
                detail_html,
                source_url=candidate["source_url"],
                captured_at=checked_at,
            )
            record = dict(parsed.record)
            record["source_type"] = LIVE_RECOVERY_SOURCE_TYPE
            record["source_url"] = candidate["source_url"]
            record["source_contract_id"] = candidate["source_contract_id"]

            if candidate["search_row"] is not None:
                record = enrich_from_search_row(record, candidate["search_row"], source_type=LIVE_RECOVERY_SOURCE_TYPE)
            else:
                record = best_effort_manual_enrichment(client, target, record)

            is_valid, reason = validate_detail_match(target, parsed)
            if is_valid:
                if not any(records_equivalent(existing, record) for existing in valid_records):
                    valid_records.append(record)
            else:
                invalid_reasons.append(reason)

        if len(valid_records) == 0:
            target.status = "unrecoverable"
            target.notes = format_note(discovery_note, *sorted(set(invalid_reasons)))
        elif len(valid_records) > 1:
            target.status = "ambiguous"
            target.notes = format_note(discovery_note, f"{len(valid_records)} validated candidates remain")
        else:
            record = valid_records[0]
            conflict_kind, conflict_note = detect_recovery_conflict(conn, recovered_rows, record)
            if conflict_kind == "ok":
                if not dry_run:
                    upsert_recovered_row(recovered_rows, record)
                target.status = "recovered"
                target.notes = format_note(discovery_note, "recovered original row")
            elif conflict_kind == "identical":
                target.status = "recovered"
                target.notes = format_note(discovery_note, conflict_note)
            elif conflict_kind == "needs_enrichment":
                target.status = "unrecoverable"
                target.notes = format_note(discovery_note, conflict_note)
            else:
                target.status = "ambiguous"
                target.notes = format_note(discovery_note, conflict_note)
    except Exception as exc:
        target.notes = f"error: {exc}"
    finally:
        target.last_checked_at = checked_at

    return target.status


def validate_target_file(targets: list[RecoveryTarget]):
    for target in targets:
        if target.lookup_mode not in ALLOWED_LOOKUP_MODES:
            raise ValueError(
                f"Unsupported lookup_mode '{target.lookup_mode}' for {target.contract_number}"
            )
        if target.status not in ALLOWED_STATUSES:
            raise ValueError(
                f"Unsupported status '{target.status}' for {target.contract_number}"
            )
        if not (target.recovery_batch or "").strip():
            raise ValueError(
                f"Missing recovery_batch for {target.contract_number} / {target.entity}"
            )


def main():
    parser = argparse.ArgumentParser(description="Recover missing original contract rows from the live OCPR site")
    parser.add_argument("--targets-csv", default=str(DEFAULT_TARGETS_CSV))
    parser.add_argument("--recovered-csv", default=str(DEFAULT_RECOVERED_CSV))
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--contract-number", help="Process only a single contract number")
    parser.add_argument("--batch", action="append", dest="batches", help="Process only pending targets in this recovery_batch (repeatable)")
    parser.add_argument("--all-pending", action="store_true", help="Process every pending target across all recovery batches")
    parser.add_argument("--retry-status", action="append", dest="retry_statuses", help="Also process targets currently in this status (repeatable)")
    args = parser.parse_args()

    targets_path = Path(args.targets_csv)
    recovered_path = Path(args.recovered_csv)
    targets = load_recovery_targets(targets_path)
    validate_target_file(targets)
    recovered_rows = load_recovered_rows(recovered_path)

    conn = None
    db_path = Path(args.db)
    if db_path.exists():
        conn = sqlite3.connect(db_path)
        create_schema(conn)

    client = OCPRContractRegistryClient()
    outcomes: Counter[str] = Counter()

    try:
        selected_targets = select_targets_for_processing(
            targets,
            contract_number=args.contract_number,
            batches=args.batches,
            all_pending=args.all_pending,
            retry_statuses=args.retry_statuses,
        )
        for target in selected_targets:
            print(f"[recover] {target.contract_number} | {target.entity} | {target.lookup_mode}")
            outcome = process_target(
                target,
                client=client,
                conn=conn,
                recovered_rows=recovered_rows,
                dry_run=args.dry_run,
            )
            outcomes[outcome] += 1
            print(f"  -> {outcome}: {target.notes or '(no note)'}")
            if not args.dry_run:
                write_recovered_rows(recovered_path, recovered_rows)
                write_recovery_targets(targets_path, targets)
    except ValueError as exc:
        raise SystemExit(str(exc))
    finally:
        if conn:
            conn.close()

    print("\nSummary:")
    if outcomes:
        for status, count in sorted(outcomes.items()):
            print(f"  {status}: {count}")
    else:
        print("  No pending targets matched the requested filters.")


if __name__ == "__main__":
    main()
