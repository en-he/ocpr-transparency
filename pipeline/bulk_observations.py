"""Append-only observations derived from the certified OCPR bulk parser.

This module does not parse CSV independently. It converts the exact row outcomes
from :mod:`certify_bulk` into versioned, deterministic observation records and
persists them in the full/audit database. Source bytes remain authoritative.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bulk_manifest import HEADER_PROFILES
from certify_bulk import (
    BulkCertificationReport,
    DuplicateHeaderError,
    MissingRequiredHeaderError,
    UnknownHeaderProfileError,
    _parse_record,
    certify_bulk_file,
)
from config import COLUMN_MAP
from contract_utils import (
    BULK_ALLOWED_CANONICAL_STATUSES,
    CONTRACT_COLUMNS,
    CONTRACT_INSERT_SQL,
    RAW_SOURCE_TYPE,
    normalize_contract_record,
    parse_bulk_field,
)

_JSON_KWARGS = {
    "ensure_ascii": False,
    "sort_keys": True,
    "separators": (",", ":"),
}


def _json(value: Any) -> str:
    return json.dumps(value, **_JSON_KWARGS)


def _sha256_id(parts: list[Any]) -> str:
    payload = _json(parts).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def observation_id(
    *,
    evidence_id: str,
    source_row_number: int,
    raw_row_hash: str,
    parser_version: str,
    normalizer_version: str,
) -> str:
    """Return a stable, version-sensitive observation identity."""
    return _sha256_id(
        [
            "bulk-observation-v1",
            evidence_id,
            source_row_number,
            raw_row_hash,
            parser_version,
            normalizer_version,
        ]
    )


@dataclass(frozen=True)
class EvidenceObject:
    evidence_id: str
    source_channel: str
    fiscal_year: str
    source_url: str | None
    archive_url: str | None
    captured_at: str | None
    capture_time_status: str
    file_sha256: str
    byte_length: int
    encoding: str
    media_type: str | None
    header_profile: str
    header_fingerprint: str
    parser_version: str
    normalizer_version: str
    status: str
    metadata_json: str

    @property
    def sha256(self) -> str:
        return self.file_sha256

    def sqlite_values(self) -> tuple[Any, ...]:
        return (
            self.evidence_id,
            self.source_channel,
            self.fiscal_year,
            self.source_url,
            self.archive_url,
            self.captured_at,
            self.capture_time_status,
            self.file_sha256,
            self.byte_length,
            self.encoding,
            self.media_type,
            self.header_profile,
            self.header_fingerprint,
            self.status,
            self.metadata_json,
        )


@dataclass(frozen=True)
class ObservationInsertResult:
    evidence_inserted: int
    observations_inserted: int
    observations_existing: int
    exclusions_inserted: int
    inserted_observation_ids: tuple[str, ...]


@dataclass(frozen=True)
class ProjectionResult:
    rows_new: int
    rows_duplicate: int
    rows_ineligible: int
    rows_existing: int
    exclusion_reason_counts: dict[str, int]


@dataclass(frozen=True)
class BulkObservationBatch(Sequence[dict[str, Any]]):
    evidence: EvidenceObject
    report: BulkCertificationReport
    headers: tuple[str, ...]
    canonical_header_indexes: dict[str, int]

    def __len__(self) -> int:
        return len(self.report.row_outcomes)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        first_by_raw_record: dict[str, str] = {}
        for outcome in self.report.row_outcomes:
            row = _build_observation(self, outcome, first_by_raw_record)
            if outcome.status == "certified":
                first_by_raw_record.setdefault(outcome.raw_record, row["observation_id"])
            yield row

    def __getitem__(self, index):
        if isinstance(index, slice):
            return list(self)[index]
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(index)
        for current, row in enumerate(self):
            if current == index:
                return row
        raise IndexError(index)


def _header_indexes(headers: tuple[str, ...]) -> dict[str, int]:
    indexes: dict[str, int] = {}
    for canonical, aliases in COLUMN_MAP.items():
        matches = [
            index
            for index, header in enumerate(headers)
            if any(header.strip().casefold() == alias.casefold() for alias in aliases)
        ]
        if len(matches) > 1:
            raise ValueError(f"ambiguous canonical header mapping for {canonical!r}")
        if matches:
            indexes[canonical] = matches[0]
    return indexes


def _evidence_from_report(
    report: BulkCertificationReport,
    *,
    requested_url: str | None,
    final_url: str | None,
    redirect_chain: Sequence[str],
) -> EvidenceObject:
    evidence_id = f"sha256:{report.sha256}"
    metadata = {
        "archive_url": report.archive_url,
        "capture_time_status": report.capture_time_status,
        "content_type": report.content_type,
        "encoding": report.encoding,
        "final_url": final_url,
        "http_status": report.http_status,
        "requested_url": requested_url,
        "redirect_chain": list(redirect_chain),
        "source_url": report.source_url,
    }
    return EvidenceObject(
        evidence_id=evidence_id,
        source_channel=report.source_channel,
        fiscal_year=report.fiscal_year or "",
        source_url=report.source_url,
        archive_url=report.archive_url,
        captured_at=report.capture_time,
        capture_time_status=report.capture_time_status,
        file_sha256=report.sha256,
        byte_length=report.byte_length,
        encoding=report.encoding,
        media_type=report.content_type,
        header_profile=report.header_profile,
        header_fingerprint=report.header_fingerprint,
        parser_version=report.parser_version,
        normalizer_version=report.normalizer_version,
        status=report.verdict,
        metadata_json=_json(metadata),
    )


def generate_bulk_observations(
    path: Path | str,
    *,
    fiscal_year: str,
    source_channel: str,
    source_url: str | None = None,
    archive_url: str | None = None,
    capture_time: str | None = None,
    capture_time_status: str = "unknown",
    http_status: int | None = None,
    content_type: str | None = None,
    requested_url: str | None = None,
    final_url: str | None = None,
    redirect_chain: Sequence[str] = (),
) -> BulkObservationBatch:
    """Generate a lazy observation batch from the authoritative certifier."""
    report = certify_bulk_file(
        path,
        source_channel=source_channel,
        fiscal_year=fiscal_year,
        source_url=source_url,
        archive_url=archive_url,
        capture_time=capture_time,
        capture_time_status=capture_time_status,
        http_status=http_status,
        content_type=content_type,
    )
    raw_header = Path(path).read_bytes().splitlines()[0].decode(report.encoding)
    headers = tuple(field.strip() for field in _parse_record(raw_header))
    if set(headers) != set(HEADER_PROFILES[report.header_profile]):
        raise RuntimeError("certified header/profile mismatch")
    return BulkObservationBatch(
        evidence=_evidence_from_report(
            report,
            requested_url=requested_url,
            final_url=final_url,
            redirect_chain=redirect_chain,
        ),
        report=report,
        headers=headers,
        canonical_header_indexes=_header_indexes(headers),
    )


def _build_observation(
    batch: BulkObservationBatch,
    outcome,
    first_by_raw_record: dict[str, str],
) -> dict[str, Any]:
    raw_fields = list(outcome.raw_fields)
    typed_values: dict[str, Any] = {}
    field_statuses: dict[str, str] = {}
    warnings: list[str] = []

    for canonical in CONTRACT_COLUMNS:
        if canonical == "fiscal_year":
            typed_values[canonical] = batch.evidence.fiscal_year
            field_statuses[canonical] = "valid"
            continue
        index = batch.canonical_header_indexes.get(canonical)
        raw_value = raw_fields[index] if index is not None and index < len(raw_fields) else None
        parsed = parse_bulk_field(canonical, raw_value, profile=batch.evidence.header_profile)
        typed_values[canonical] = parsed.value
        field_statuses[canonical] = parsed.status
        if parsed.warning:
            warnings.append(parsed.warning)
        if parsed.status not in BULK_ALLOWED_CANONICAL_STATUSES:
            warnings.append(f"{canonical}_{parsed.status}")

    parser_outcome = outcome.reason or outcome.status
    if outcome.reason and outcome.reason not in warnings:
        warnings.insert(0, outcome.reason)

    bad_fields = [
        (name, status)
        for name, status in field_statuses.items()
        if status not in BULK_ALLOWED_CANONICAL_STATUSES
    ]
    canonical_eligible = outcome.status == "certified" and not bad_fields
    if not canonical_eligible:
        if outcome.reason:
            exclusion_reason = f"parser_{outcome.reason}"
        elif bad_fields:
            name, status = bad_fields[0]
            exclusion_reason = f"field_{name}_{status}"
        else:
            exclusion_reason = "parser_not_certified"
    else:
        exclusion_reason = None

    current_id = observation_id(
        evidence_id=batch.evidence.evidence_id,
        source_row_number=outcome.row_number,
        raw_row_hash=outcome.raw_record_sha256,
        parser_version=batch.evidence.parser_version,
        normalizer_version=batch.evidence.normalizer_version,
    )
    duplicate_of = (
        first_by_raw_record.get(outcome.raw_record)
        if outcome.status == "certified"
        else None
    )
    # Certified rows already carry ordered raw values plus their exact known
    # profile. Expanded coordinates are retained only where a malformed row's
    # width no longer aligns with the profile; repeating header names for all
    # 1.23M rows would add gigabytes without adding evidence.
    coordinates = []
    if outcome.status == "quarantined":
        coordinates = [
            {
                "column_index": index,
                "header": batch.headers[index] if index < len(batch.headers) else None,
                "raw_value": value,
            }
            for index, value in enumerate(raw_fields)
        ]
    return {
        "observation_id": current_id,
        "evidence_id": batch.evidence.evidence_id,
        "source_row_number": outcome.row_number,
        "raw_row_hash": outcome.raw_record_sha256,
        "raw_record": outcome.raw_record,
        "raw_values_json": _json(raw_fields),
        "raw_coordinates_json": _json(coordinates),
        "parser_profile": batch.evidence.header_profile,
        "parser_version": batch.evidence.parser_version,
        "normalizer_version": batch.evidence.normalizer_version,
        "parsed_values_json": _json(typed_values),
        "field_status_json": _json(field_statuses),
        "warnings_json": _json(list(dict.fromkeys(warnings))),
        "parser_outcome": parser_outcome,
        "observation_status": outcome.status,
        "duplicate_status": "exact_duplicate" if duplicate_of else "unique",
        "duplicate_of_observation_id": duplicate_of,
        "canonical_eligible": int(canonical_eligible),
        "canonical_exclusion_reason": exclusion_reason,
    }


_EVIDENCE_COLUMNS = (
    "evidence_id", "source_channel", "fiscal_year", "source_url", "archive_url",
    "captured_at", "capture_time_status", "sha256", "byte_length", "encoding",
    "media_type", "header_profile", "header_fingerprint", "status",
    "metadata_json",
)
_OBSERVATION_COLUMNS = (
    "observation_id", "evidence_id", "source_row_number", "raw_row_hash",
    "raw_record", "raw_values_json", "raw_coordinates_json", "parser_profile",
    "parser_version", "normalizer_version", "parsed_values_json",
    "field_status_json", "warnings_json", "parser_outcome", "observation_status",
    "duplicate_status", "duplicate_of_observation_id", "canonical_eligible",
    "canonical_exclusion_reason",
)


def _assert_existing_row(
    conn: sqlite3.Connection,
    table: str,
    key_column: str,
    key: str,
    columns: Sequence[str],
    expected: Sequence[Any],
) -> None:
    row = conn.execute(
        f"SELECT {', '.join(columns)} FROM {table} WHERE {key_column} = ?",
        (key,),
    ).fetchone()
    if row is None or tuple(row) != tuple(expected):
        raise ValueError(f"append-only conflict for {table}.{key_column}={key!r}")


def insert_bulk_observations(
    conn: sqlite3.Connection,
    batch: BulkObservationBatch,
    *,
    manage_transaction: bool = True,
) -> ObservationInsertResult:
    """Persist one evidence object and every parser outcome idempotently."""
    if conn.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise RuntimeError("bulk observation writes require PRAGMA foreign_keys=ON")

    def apply() -> ObservationInsertResult:
        evidence_values = batch.evidence.sqlite_values()
        try:
            conn.execute(
                f"INSERT INTO evidence_objects ({', '.join(_EVIDENCE_COLUMNS)}) "
                f"VALUES ({', '.join('?' for _ in _EVIDENCE_COLUMNS)})",
                evidence_values,
            )
            evidence_inserted = 1
        except sqlite3.IntegrityError:
            _assert_existing_row(
                conn,
                "evidence_objects",
                "evidence_id",
                batch.evidence.evidence_id,
                _EVIDENCE_COLUMNS,
                evidence_values,
            )
            evidence_inserted = 0

        inserted_ids: list[str] = []
        existing = 0
        exclusions_inserted = 0
        insert_sql = (
            f"INSERT INTO bulk_observations ({', '.join(_OBSERVATION_COLUMNS)}) "
            f"VALUES ({', '.join('?' for _ in _OBSERVATION_COLUMNS)})"
        )
        for observation in batch:
            values = tuple(observation[column] for column in _OBSERVATION_COLUMNS)
            try:
                conn.execute(insert_sql, values)
                inserted_ids.append(observation["observation_id"])
            except sqlite3.IntegrityError:
                existing_row = conn.execute(
                    "SELECT 1 FROM bulk_observations WHERE observation_id = ?",
                    (observation["observation_id"],),
                ).fetchone()
                if existing_row is None:
                    raise
                existing += 1
                _assert_existing_row(
                    conn,
                    "bulk_observations",
                    "observation_id",
                    observation["observation_id"],
                    _OBSERVATION_COLUMNS,
                    values,
                )

            reason = observation["canonical_exclusion_reason"]
            if reason:
                exclusion_id = _sha256_id(
                    ["bulk-projection-exclusion-v1", observation["observation_id"], reason]
                )
                exclusion_cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO bulk_projection_exclusions (
                        exclusion_id, observation_id, evidence_id,
                        source_row_number, reason, details_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        exclusion_id,
                        observation["observation_id"],
                        observation["evidence_id"],
                        observation["source_row_number"],
                        reason,
                        _json({"parser_outcome": observation["parser_outcome"]}),
                    ),
                )
                exclusions_inserted += max(exclusion_cur.rowcount, 0)

        return ObservationInsertResult(
            evidence_inserted=evidence_inserted,
            observations_inserted=len(inserted_ids),
            observations_existing=existing,
            exclusions_inserted=exclusions_inserted,
            inserted_observation_ids=tuple(inserted_ids),
        )

    if manage_transaction:
        with conn:
            return apply()
    return apply()


def project_bulk_observations(
    conn: sqlite3.Connection,
    batch: BulkObservationBatch,
    *,
    inserted_at: str | None,
    index_fts: bool = True,
) -> ProjectionResult:
    """Project eligible observations once and retain every exclusion reason."""
    rows_new = rows_duplicate = rows_ineligible = rows_existing = 0
    reason_counts: Counter[str] = Counter()

    for observation in batch:
        already = conn.execute(
            "SELECT 1 FROM bulk_projection_results WHERE observation_id = ?",
            (observation["observation_id"],),
        ).fetchone()
        if already:
            rows_existing += 1
            continue

        if not observation["canonical_eligible"]:
            reason = observation["canonical_exclusion_reason"]
            conn.execute(
                """
                INSERT INTO bulk_projection_results (
                    observation_id, row_hash, contract_id, projection_status, reason
                ) VALUES (?, NULL, NULL, 'excluded', ?)
                """,
                (observation["observation_id"], reason),
            )
            rows_ineligible += 1
            reason_counts[reason] += 1
            continue

        parsed = json.loads(observation["parsed_values_json"])
        parsed.update(
            {
                "source_type": RAW_SOURCE_TYPE,
                "source_url": batch.evidence.source_url,
                "source_contract_id": None,
                "representative_observation_id": observation["observation_id"],
                "canonicalization_status": "selected_observation",
                "normalizer_version": observation["normalizer_version"],
            }
        )
        normalized = normalize_contract_record(
            parsed,
            default_source_type=RAW_SOURCE_TYPE,
            inserted_at=inserted_at,
            preserve_missing_inserted_at=True,
        )
        existing_contract = conn.execute(
            "SELECT id FROM contracts WHERE row_hash = ?",
            (normalized["row_hash"],),
        ).fetchone()
        if existing_contract is None:
            cur = conn.execute(CONTRACT_INSERT_SQL, normalized)
            if cur.rowcount != 1:
                raise RuntimeError("canonical insert did not select exactly one observation")
            contract_id = cur.lastrowid
            if index_fts:
                conn.execute(
                    """
                    INSERT INTO contracts_fts (
                        rowid, contract_number, entity, contractor,
                        service_category, service_type
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        contract_id,
                        normalized["contract_number"],
                        normalized["entity"],
                        normalized["contractor"],
                        normalized["service_category"],
                        normalized["service_type"],
                    ),
                )
            conn.execute(
                """
                INSERT INTO bulk_projection_results (
                    observation_id, row_hash, contract_id, projection_status, reason
                ) VALUES (?, ?, ?, 'selected', NULL)
                """,
                (observation["observation_id"], normalized["row_hash"], contract_id),
            )
            rows_new += 1
            continue

        reason = "canonical_row_hash_duplicate"
        conn.execute(
            """
            INSERT INTO bulk_projection_results (
                observation_id, row_hash, contract_id, projection_status, reason
            ) VALUES (?, ?, ?, 'excluded', ?)
            """,
            (
                observation["observation_id"],
                normalized["row_hash"],
                existing_contract[0],
                reason,
            ),
        )
        exclusion_id = _sha256_id(
            ["bulk-projection-exclusion-v1", observation["observation_id"], reason]
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO bulk_projection_exclusions (
                exclusion_id, observation_id, evidence_id,
                source_row_number, reason, details_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                exclusion_id,
                observation["observation_id"],
                observation["evidence_id"],
                observation["source_row_number"],
                reason,
                _json({"row_hash": normalized["row_hash"]}),
            ),
        )
        rows_duplicate += 1
        reason_counts[reason] += 1

    return ProjectionResult(
        rows_new=rows_new,
        rows_duplicate=rows_duplicate,
        rows_ineligible=rows_ineligible,
        rows_existing=rows_existing,
        exclusion_reason_counts=dict(sorted(reason_counts.items())),
    )
