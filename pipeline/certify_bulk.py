"""
Independent, additive certification pass over preserved OCPR bulk CSVs.

Certifies one exact preserved CSV snapshot at a time without mutating ingestion,
databases, or public artifacts. File-level schema problems fail closed with named
exceptions; row-level defects are retained as quarantined source observations.
See docs/project/bulk-certification.md for the public contract.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from bulk_manifest import (
    CAPTURE_TIME_STATUSES,
    HEADER_PROFILES,
    SOURCE_CHANNELS,
    detect_header_profile,
)

ENCODING = "latin-1"
PARSER_VERSION = "bulk-certify-parser-1.1.0"
NORMALIZER_VERSION = "bulk-certify-normalizer-1.0.0"

DATE_FIELD_NAMES = {
    "Otorgado en",
    "Otorgado En",
    "Vigencia Desde",
    "Vigencia Hasta",
    "Cancelado",
}
AMOUNT_FIELD_NAMES = {"Cuantía", "Cuantía a Recibir"}
BLANK_MARKERS = ("", "\x00")
_AMOUNT_PATTERN = re.compile(r"-?(?:\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?")
# OCPR v3 uses doubled outer wrappers at field boundaries: ,""value"",
_DOUBLED_OUTER_FIELD = re.compile(r'(^|,)""(.*?)""(?=,|$)')


class BulkCertificationError(Exception):
    """Base class for fail-closed bulk certification errors."""


class EmptyBulkFileError(BulkCertificationError):
    """The source has no header record."""


class UnsupportedMultilineRecordError(BulkCertificationError):
    """A quoted CSV field spans physical lines, an unsupported source shape."""


class MissingRequiredHeaderError(BulkCertificationError):
    """Header set is a strict subset of a known profile."""


class DuplicateHeaderError(BulkCertificationError):
    """A header string appears more than once."""


class UnknownHeaderProfileError(BulkCertificationError):
    """Header set does not resolve unambiguously to a known profile."""


@dataclass
class RowOutcome:
    # Source CSV record number; header is record 1, first data record is 2.
    row_number: int
    status: str
    reason: str | None
    # Parsed source field values before canonical normalization.
    raw_fields: tuple[str, ...]
    # Exact decoded physical source record without its line terminator.
    raw_record: str
    raw_record_sha256: str


@dataclass
class BulkCertificationReport:
    fiscal_year: str | None
    source_channel: str
    source_url: str | None
    archive_url: str | None
    capture_time: str | None
    capture_time_status: str
    sha256: str
    byte_length: int
    encoding: str
    http_status: int | None
    content_type: str | None
    header_profile: str
    header_fingerprint: str
    parser_version: str
    normalizer_version: str
    rows_total: int
    rows_certified: int
    rows_quarantined: int
    duplicate_count: int
    # Exact certified source records unique within this snapshot. This is not
    # a canonical projection contribution; cross-snapshot/canonical accounting
    # is performed by the manifest/report layer.
    source_unique_contribution_count: int
    quarantine_reason_counts: dict[str, int]
    verdict: str
    certified_at: str
    row_outcomes: list[RowOutcome]


def certify_bulk_file(
    path,
    *,
    source_channel: str,
    fiscal_year: str | None = None,
    source_url: str | None = None,
    archive_url: str | None = None,
    capture_time: str | None = None,
    capture_time_status: str = "unknown",
    http_status: int | None = None,
    content_type: str | None = None,
) -> BulkCertificationReport:
    _validate_source_metadata(
        source_channel=source_channel,
        capture_time=capture_time,
        capture_time_status=capture_time_status,
    )

    raw_bytes = Path(path).read_bytes()
    file_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    records = _decode_physical_records(raw_bytes)
    if not records or records[0] == "":
        raise EmptyBulkFileError("bulk CSV has no header record")
    _reject_supported_width_multiline_records(records)

    header_record = records[0]
    header_fields = [field.strip() for field in _parse_record(header_record)]
    duplicates = _find_duplicate_headers(header_fields)
    if duplicates:
        raise DuplicateHeaderError(
            "duplicate header field(s): " + ", ".join(sorted(duplicates))
        )

    profile = detect_header_profile(header_fields)
    if profile is None:
        _raise_unresolved_header_error(header_fields)

    row_outcomes: list[RowOutcome] = []
    seen_certified_records: set[str] = set()
    duplicate_count = 0
    quarantine_reasons: Counter[str] = Counter()

    for index, raw_record in enumerate(records[1:], start=1):
        row_number = index + 1
        next_record = records[index + 1] if index + 1 < len(records) else None
        raw_fields, parse_reason = _parse_data_record(
            raw_record,
            next_record=next_record,
            expected_fields=len(header_fields),
        )
        record_hash = hashlib.sha256(raw_record.encode(ENCODING)).hexdigest()
        if len(raw_fields) != len(header_fields):
            reason = "shifted_row"
        elif parse_reason is not None:
            reason = parse_reason
        else:
            reason = _first_field_defect(header_fields, raw_fields)

        if reason is not None:
            quarantine_reasons[reason] += 1
            row_outcomes.append(
                RowOutcome(
                    row_number=row_number,
                    status="quarantined",
                    reason=reason,
                    raw_fields=tuple(raw_fields),
                    raw_record=raw_record,
                    raw_record_sha256=record_hash,
                )
            )
            continue

        if raw_record in seen_certified_records:
            duplicate_count += 1
        else:
            seen_certified_records.add(raw_record)
        row_outcomes.append(
            RowOutcome(
                row_number=row_number,
                status="certified",
                reason=None,
                raw_fields=tuple(raw_fields),
                raw_record=raw_record,
                raw_record_sha256=record_hash,
            )
        )

    rows_total = len(row_outcomes)
    rows_quarantined = sum(quarantine_reasons.values())
    rows_certified = rows_total - rows_quarantined
    verdict = "certified" if rows_quarantined == 0 else "certified_with_quarantine"

    return BulkCertificationReport(
        fiscal_year=fiscal_year,
        source_channel=source_channel,
        source_url=source_url,
        archive_url=archive_url,
        capture_time=capture_time,
        capture_time_status=capture_time_status,
        sha256=file_sha256,
        byte_length=len(raw_bytes),
        encoding=ENCODING,
        http_status=http_status,
        content_type=content_type,
        header_profile=profile,
        header_fingerprint=hashlib.sha256(header_record.encode(ENCODING)).hexdigest(),
        parser_version=PARSER_VERSION,
        normalizer_version=NORMALIZER_VERSION,
        rows_total=rows_total,
        rows_certified=rows_certified,
        rows_quarantined=rows_quarantined,
        duplicate_count=duplicate_count,
        source_unique_contribution_count=rows_certified - duplicate_count,
        quarantine_reason_counts=dict(sorted(quarantine_reasons.items())),
        verdict=verdict,
        certified_at=datetime.now(timezone.utc).isoformat(),
        row_outcomes=row_outcomes,
    )


def report_hash(report: BulkCertificationReport) -> str:
    """Hash all stable logical report metadata.

    Excludes wall-clock ``certified_at``, local paths, this hash itself, and
    bulky row outcomes. Exact source bytes and summarized row outcomes remain
    represented by the source SHA-256 and deterministic counts/reason counts.
    """
    payload = {
        "fiscal_year": report.fiscal_year,
        "source_channel": report.source_channel,
        "source_url": report.source_url,
        "archive_url": report.archive_url,
        "capture_time": report.capture_time,
        "capture_time_status": report.capture_time_status,
        "sha256": report.sha256,
        "byte_length": report.byte_length,
        "encoding": report.encoding,
        "http_status": report.http_status,
        "content_type": report.content_type,
        "header_profile": report.header_profile,
        "header_fingerprint": report.header_fingerprint,
        "parser_version": report.parser_version,
        "normalizer_version": report.normalizer_version,
        "rows_total": report.rows_total,
        "rows_certified": report.rows_certified,
        "rows_quarantined": report.rows_quarantined,
        "duplicate_count": report.duplicate_count,
        "source_unique_contribution_count": report.source_unique_contribution_count,
        "quarantine_reason_counts": report.quarantine_reason_counts,
        "verdict": report.verdict,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_source_metadata(
    *, source_channel: str, capture_time: str | None, capture_time_status: str
) -> None:
    if source_channel not in SOURCE_CHANNELS:
        raise ValueError(f"unknown source_channel: {source_channel!r}")
    if capture_time_status not in CAPTURE_TIME_STATUSES:
        raise ValueError(f"unknown capture_time_status: {capture_time_status!r}")
    if capture_time_status == "unknown":
        if capture_time is not None:
            raise ValueError("capture_time must be None when status is 'unknown'")
        return
    if not capture_time:
        raise ValueError(f"capture_time is required for status {capture_time_status!r}")
    try:
        parsed = datetime.fromisoformat(capture_time.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("capture_time must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("capture_time must include an explicit UTC offset")


def _decode_physical_records(raw_bytes: bytes) -> list[str]:
    text = raw_bytes.decode(ENCODING)
    # ``splitlines`` accepts CRLF and LF while retaining every non-terminator
    # character, including NUL. Empty records inside the file remain records.
    return text.splitlines()


def _reject_supported_width_multiline_records(records: list[str]) -> None:
    """Fail closed when physical lines form a valid multiline CSV record.

    A single permissive CSV pass tracks how many physical lines each logical
    record consumed. This is linear in source size. Legacy broken-quote rows
    may consume adjacent lines, but remain row-level defects unless the joined
    logical record has exactly the certified schema width. A genuine quoted
    multiline record of any depth therefore fails closed without repeatedly
    reparsing growing prefixes.
    """
    if len(records) < 3:
        return
    try:
        expected_fields = len(_parse_record(records[0]))
    except BulkCertificationError:
        return

    normalized_lines = (
        _normalize_doubled_outer_wrappers(record) + "\n"
        for record in records[1:]
    )
    reader = csv.reader(normalized_lines, strict=False)
    previous_line_number = 0
    try:
        for fields in reader:
            physical_lines = reader.line_num - previous_line_number
            previous_line_number = reader.line_num
            if physical_lines > 1 and len(fields) == expected_fields:
                raise UnsupportedMultilineRecordError(
                    "quoted CSV field spans physical records"
                )
    except csv.Error:
        # Row-level parsing below retains and quarantines malformed physical
        # records. Only a completed expected-width multiline record is the
        # file-level unsupported shape handled here.
        return


def _normalize_doubled_outer_wrappers(raw_record: str) -> str:
    """Convert OCPR's ``""value""`` boundary wrappers to valid CSV quoting.

    Only complete fields beginning at record/field boundaries and ending at a
    field/record boundary are changed. Standard CSV-quoted fields are left as
    supplied. The untouched record is retained separately on ``RowOutcome``.
    """
    def replace(match: re.Match[str]) -> str:
        prefix, value = match.group(1), match.group(2)
        return prefix + '"' + value.replace('"', '""') + '"'

    return _DOUBLED_OUTER_FIELD.sub(replace, raw_record)


def _parse_record(raw_record: str) -> list[str]:
    """Strict parser for the header and other file-level records."""
    normalized = _normalize_doubled_outer_wrappers(raw_record)
    try:
        return next(csv.reader([normalized], strict=True))
    except csv.Error as exc:
        raise BulkCertificationError(f"malformed CSV record: {exc}") from exc


def _parse_data_record(
    raw_record: str,
    *,
    next_record: str | None,
    expected_fields: int,
) -> tuple[list[str], str | None]:
    """Parse one physical data record while retaining row-level defects."""
    normalized = _normalize_doubled_outer_wrappers(raw_record)
    try:
        return next(csv.reader([normalized], strict=True)), None
    except csv.Error as exc:
        if "unexpected end of data" in str(exc).lower() and next_record is not None:
            # Distinguish a genuinely quoted multiline record from the
            # corpus's known broken-quote/shifted rows. Only a join that forms
            # exactly one valid row of the certified width is a true
            # unsupported multiline shape.
            combined = normalized + "\n" + _normalize_doubled_outer_wrappers(next_record)
            try:
                joined_rows = list(csv.reader(io.StringIO(combined), strict=True))
            except csv.Error:
                joined_rows = []
            if len(joined_rows) == 1 and len(joined_rows[0]) == expected_fields:
                raise UnsupportedMultilineRecordError(
                    "quoted CSV field spans physical records"
                ) from exc

        # Preserve known malformed data rows for deterministic quarantine.
        # Permissive parsing is never sufficient for certification: callers
        # use the returned issue and/or field-count mismatch as the reason.
        fields = next(csv.reader([normalized], strict=False))
        return fields, "malformed_csv"


def _find_duplicate_headers(header_fields: list[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for name in header_fields:
        if name in seen:
            duplicates.add(name)
        else:
            seen.add(name)
    return duplicates


def _raise_unresolved_header_error(header_fields: list[str]) -> None:
    header_set = set(header_fields)
    known_union = set().union(*(set(value) for value in HEADER_PROFILES.values()))
    foreign = header_set - known_union
    if foreign:
        raise UnknownHeaderProfileError(
            "unrecognized header field(s): " + ", ".join(sorted(foreign))
        )

    candidates = []
    for name, profile_headers in HEADER_PROFILES.items():
        profile_set = set(profile_headers)
        if header_set < profile_set:
            candidates.append((len(profile_set - header_set), name, profile_set - header_set))
    if not candidates:
        raise UnknownHeaderProfileError(
            "header set does not resolve unambiguously to a known profile"
        )
    _distance, nearest_profile, missing = min(candidates)
    raise MissingRequiredHeaderError(
        f"missing required header field(s) for profile {nearest_profile}: "
        + ", ".join(sorted(missing))
    )


def _first_field_defect(header_fields: list[str], raw_fields: list[str]) -> str | None:
    for name, value in zip(header_fields, raw_fields):
        if name in DATE_FIELD_NAMES:
            reason = _classify_date_value(value)
        elif name in AMOUNT_FIELD_NAMES:
            reason = _classify_amount_value(value)
        else:
            reason = None
        if reason is not None:
            return reason
    return None


def _classify_date_value(value: str) -> str | None:
    if value in BLANK_MARKERS:
        return None
    parts = value.split("-")
    if len(parts) != 3:
        return "malformed_date"
    month_s, day_s, year_s = parts
    if not (month_s.isdigit() and day_s.isdigit() and year_s.isdigit()):
        return "malformed_date"
    if len(year_s) == 2:
        return "ambiguous_date"
    if len(year_s) != 4:
        return "malformed_date"
    try:
        date(int(year_s), int(month_s), int(day_s))
    except ValueError:
        return "malformed_date"
    return None


def _classify_amount_value(value: str) -> str | None:
    if value in BLANK_MARKERS:
        return None
    candidate = value.strip()
    if not _AMOUNT_PATTERN.fullmatch(candidate):
        return "malformed_amount"
    try:
        float(candidate.replace(",", ""))
    except ValueError:
        return "malformed_amount"
    return None
