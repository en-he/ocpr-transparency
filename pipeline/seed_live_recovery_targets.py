"""
Seed tracked live recovery targets for families missing an original contract row.

Usage:
    python pipeline/seed_live_recovery_targets.py
    python pipeline/seed_live_recovery_targets.py --batch 2022-2023 --batch 2021-2022
    python pipeline/seed_live_recovery_targets.py --dry-run
"""
from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from config import DB_PATH, REPO_ROOT
from contract_utils import fiscal_year_from_date, parse_date, is_original_amendment
from live_recovery import (
    RecoveryTarget,
    load_recovery_targets,
    recovery_target_batch_sort_key,
    recovery_target_identity,
    sort_recovery_targets,
    write_recovery_targets,
)


DEFAULT_TARGETS_CSV = REPO_ROOT / "data" / "recovery" / "live_recovery_targets.csv"

FAMILY_SCAN_SQL = """
    SELECT
        id,
        contract_number,
        entity,
        contractor,
        amendment,
        fiscal_year,
        award_date,
        valid_from,
        valid_to
    FROM contracts
    ORDER BY id ASC
"""


@dataclass
class FamilySummary:
    contract_number: str
    entity: str
    contractor: str
    recovery_batch: str | None
    rows_count: int
    has_original: bool
    representative_sort_key: tuple


TARGET_STATUS_PRIORITY = {
    "pending": 0,
    "ambiguous": 1,
    "unrecoverable": 2,
    "recovered": 3,
}


def row_recovery_batch(row: sqlite3.Row) -> str | None:
    return (
        (row["fiscal_year"] or "").strip()
        or fiscal_year_from_date(row["award_date"])
        or fiscal_year_from_date(row["valid_from"])
        or fiscal_year_from_date(row["valid_to"])
    )


def row_date_proxy(row: sqlite3.Row) -> str | None:
    return (
        parse_date(row["award_date"])
        or parse_date(row["valid_from"])
        or parse_date(row["valid_to"])
    )


def row_representative_sort_key(row: sqlite3.Row) -> tuple:
    date_proxy = row_date_proxy(row)
    if date_proxy:
        return (0, date_proxy, int(row["id"]))

    recovery_batch = row_recovery_batch(row)
    match_key = recovery_target_batch_sort_key(recovery_batch)
    if recovery_batch and match_key[0] != 10**9:
        start_year = -match_key[0]
        return (1, start_year, int(row["id"]))

    return (2, int(row["id"]))


def scan_contract_family_summaries(conn: sqlite3.Connection) -> dict[tuple[str, str, str], FamilySummary]:
    conn.row_factory = sqlite3.Row
    summaries: dict[tuple[str, str, str], FamilySummary] = {}

    for row in conn.execute(FAMILY_SCAN_SQL):
        contract_number = (row["contract_number"] or "").strip()
        entity = (row["entity"] or "").strip()
        contractor = (row["contractor"] or "").strip()
        if not contract_number or not entity:
            continue

        identity = recovery_target_identity(contract_number, entity, contractor)
        recovery_batch = row_recovery_batch(row)
        representative_sort_key = row_representative_sort_key(row)

        if identity not in summaries:
            summaries[identity] = FamilySummary(
                contract_number=contract_number,
                entity=entity,
                contractor=contractor,
                recovery_batch=recovery_batch,
                rows_count=1,
                has_original=is_original_amendment(row["amendment"]),
                representative_sort_key=representative_sort_key,
            )
            continue

        summary = summaries[identity]
        summary.rows_count += 1
        if is_original_amendment(row["amendment"]):
            summary.has_original = True

        if representative_sort_key < summary.representative_sort_key:
            summary.contract_number = contract_number
            summary.entity = entity
            summary.contractor = contractor
            summary.recovery_batch = recovery_batch
            summary.representative_sort_key = representative_sort_key
        elif not summary.recovery_batch and recovery_batch:
            summary.recovery_batch = recovery_batch

    return summaries


def build_seed_target(summary: FamilySummary) -> RecoveryTarget:
    return RecoveryTarget(
        contract_number=summary.contract_number,
        entity=summary.entity,
        contractor=summary.contractor,
        recovery_batch=summary.recovery_batch,
        lookup_mode="auto_discover",
        source_url=None,
        status="pending",
        notes=None,
        last_checked_at=None,
    )


def choose_preferred_target(left: RecoveryTarget, right: RecoveryTarget) -> RecoveryTarget:
    def preference(target: RecoveryTarget) -> tuple:
        return (
            TARGET_STATUS_PRIORITY.get(target.status, -1),
            1 if target.lookup_mode == "manual_url" else 0,
            1 if target.source_url else 0,
            1 if target.notes else 0,
            target.last_checked_at or "",
        )

    preferred, fallback = (left, right) if preference(left) >= preference(right) else (right, left)
    last_checked_at = max(
        [value for value in [preferred.last_checked_at, fallback.last_checked_at] if value],
        default=None,
    )

    return RecoveryTarget(
        contract_number=preferred.contract_number or fallback.contract_number,
        entity=preferred.entity or fallback.entity,
        contractor=preferred.contractor or fallback.contractor,
        recovery_batch=preferred.recovery_batch or fallback.recovery_batch,
        lookup_mode=preferred.lookup_mode or fallback.lookup_mode,
        source_url=preferred.source_url or fallback.source_url,
        status=preferred.status or fallback.status,
        notes=preferred.notes or fallback.notes,
        last_checked_at=last_checked_at,
    )


def collapse_duplicate_existing_targets(existing_targets: list[RecoveryTarget]) -> tuple[list[RecoveryTarget], int]:
    collapsed_targets: dict[tuple[str, str, str], RecoveryTarget] = {}
    collapsed_count = 0

    for target in existing_targets:
        identity = recovery_target_identity(target.contract_number, target.entity, target.contractor)
        existing = collapsed_targets.get(identity)
        if existing is None:
            collapsed_targets[identity] = target
            continue

        collapsed_targets[identity] = choose_preferred_target(existing, target)
        collapsed_count += 1

    return list(collapsed_targets.values()), collapsed_count


def merge_seeded_targets(
    existing_targets: list[RecoveryTarget],
    family_summaries: dict[tuple[str, str, str], FamilySummary],
    *,
    selected_batches: set[str] | None = None,
) -> tuple[list[RecoveryTarget], int, int, int, int]:
    deduped_existing_targets, collapsed_count = collapse_duplicate_existing_targets(existing_targets)
    merged_targets: list[RecoveryTarget] = []
    existing_identities: dict[tuple[str, str, str], RecoveryTarget] = {}
    removed_count = 0

    for target in deduped_existing_targets:
        identity = recovery_target_identity(target.contract_number, target.entity, target.contractor)
        summary = family_summaries.get(identity)
        if summary and summary.has_original and target.status != "recovered":
            removed_count += 1
            continue

        merged_target = target
        if summary and not (target.recovery_batch or "").strip():
            merged_target = RecoveryTarget(
                contract_number=target.contract_number,
                entity=target.entity,
                contractor=target.contractor,
                recovery_batch=summary.recovery_batch,
                lookup_mode=target.lookup_mode,
                source_url=target.source_url,
                status=target.status,
                notes=target.notes,
                last_checked_at=target.last_checked_at,
            )

        existing_identities[identity] = merged_target
        merged_targets.append(merged_target)

    candidate_summaries = [
        summary
        for identity, summary in family_summaries.items()
        if summary.rows_count > 1
        and not summary.has_original
        and identity not in existing_identities
        and (
            not selected_batches
            or (summary.recovery_batch or "") in selected_batches
        )
    ]

    candidate_summaries.sort(
        key=lambda summary: (
            recovery_target_batch_sort_key(summary.recovery_batch),
            summary.contract_number,
            summary.entity,
            summary.contractor,
        )
    )

    for summary in candidate_summaries:
        merged_targets.append(build_seed_target(summary))

    total_candidates = sum(
        1
        for summary in family_summaries.values()
        if summary.rows_count > 1
        and not summary.has_original
        and (
            not selected_batches
            or (summary.recovery_batch or "") in selected_batches
        )
    )

    return sort_recovery_targets(merged_targets), len(candidate_summaries), total_candidates, removed_count, collapsed_count


def count_pending_by_batch(targets: list[RecoveryTarget]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for target in targets:
        if target.status != "pending":
            continue
        batch = target.recovery_batch or "(missing)"
        counts[batch] = counts.get(batch, 0) + 1
    return counts


def main():
    parser = argparse.ArgumentParser(description="Seed tracked live recovery targets from the canonical SQLite DB")
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--targets-csv", default=str(DEFAULT_TARGETS_CSV))
    parser.add_argument("--batch", action="append", dest="batches", help="Only add newly discovered targets for this recovery_batch (repeatable)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_path = Path(args.db)
    targets_path = Path(args.targets_csv)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        family_summaries = scan_contract_family_summaries(conn)
    finally:
        conn.close()

    existing_targets = load_recovery_targets(targets_path)
    selected_batches = {batch.strip() for batch in (args.batches or []) if batch and batch.strip()}
    merged_targets, added_count, total_candidates, removed_count, collapsed_count = merge_seeded_targets(
        existing_targets,
        family_summaries,
        selected_batches=selected_batches or None,
    )

    if not args.dry_run:
        write_recovery_targets(targets_path, merged_targets)

    pending_by_batch = count_pending_by_batch(merged_targets)
    sorted_pending_batches = sorted(
        pending_by_batch.items(),
        key=lambda item: (recovery_target_batch_sort_key(item[0]), item[0]),
    )

    print(f"Existing tracked targets: {len(existing_targets)}")
    print(f"Eligible missing-original families in scope: {total_candidates}")
    print(f"New targets added: {added_count}")
    print(f"Duplicate target rows collapsed: {collapsed_count}")
    print(f"Stale non-recovered targets removed: {removed_count}")
    print(f"Total tracked targets after merge: {len(merged_targets)}")
    print(f"{'Would write' if args.dry_run else 'Wrote'}: {targets_path}")
    print("Pending targets by recovery_batch:")
    for recovery_batch, count in sorted_pending_batches[:12]:
        print(f"  {recovery_batch}: {count}")


if __name__ == "__main__":
    main()
