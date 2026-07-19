"""Build deterministic Milestone A bulk-certification artifacts.

This module is deliberately additive: it reads the preserved raw CSVs, calls the
independent file certifier, and reproduces the existing ingest normalization for
canonical row-hash accounting.  It never mutates the database or the source
files.  Generated JSON contains only stable evidence metadata, quarantine row
identities, and canonical duplicate exclusions; the certifier's wall-clock
``certified_at`` value is intentionally not serialized.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

# The pipeline modules are currently flat modules and are also imported that
# way by the repository tests.  Make direct package-style imports work without
# making this generator depend on an installed package.
_PIPELINE_DIR = Path(__file__).resolve().parent
if str(_PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(_PIPELINE_DIR))

from certify_bulk import (  # noqa: E402
    BulkCertificationError,
    BulkCertificationReport,
    certify_bulk_file,
    report_hash,
)
from config import (  # noqa: E402
    ARCHIVED_ONLY_FISCAL_YEARS,
    BASE_URL,
    DOWNLOAD_PATH,
    KNOWN_LIVE_404_YEARS,
    COLUMN_MAP,
)
from contract_utils import (  # noqa: E402
    RAW_SOURCE_TYPE,
    clean_str,
    normalize_contract_record,
    parse_amount,
    parse_cancelled,
    parse_date,
)
from ingest import detect_encoding, resolve_header  # noqa: E402


MANIFEST_PATH = "data/certification/bulk-manifest.json"
REPORTS_DIRECTORY = "data/certification/reports"
REPORT_PATH_TEMPLATE = REPORTS_DIRECTORY + "/{fiscal_year}.json"
SOURCE_GLOB = "data/raw/contratos_*.csv"
REPORT_SCHEMA_VERSION = "bulk-certification-report-1"
MANIFEST_SCHEMA_VERSION = "bulk-certification-manifest-1"
GENERATOR_VERSION = "bulk-certification-generator-1.0.0"
ARCHIVE_CAPTURE_URLS = {
    "2010-2011": (
        "https://web.archive.org/web/20210421010733id_/"
        "https://consultacontratos.ocpr.gov.pr/contract/"
        "downloadfrequentsearchfiscalyeardocument?q=2010-2011"
    ),
    "2011-2012": (
        "https://web.archive.org/web/20210421010725id_/"
        "https://consultacontratos.ocpr.gov.pr/contract/"
        "downloadfrequentsearchfiscalyeardocument?q=2011-2012"
    ),
}
# This value is used only to make the normalization pass deterministic.  It is
# not evidence and is never emitted.  row_hash itself does not include it.
_NORMALIZATION_INSERTED_AT = "1970-01-01T00:00:00+00:00"
_YEAR_RE = re.compile(r"^contratos_(?P<year>\d{4}-\d{4})\.csv$")

# Stable report fields copied from BulkCertificationReport.  In particular,
# certified_at is absent because it is the run clock, not source evidence.
_REPORT_FIELDS = (
    "fiscal_year",
    "source_channel",
    "source_url",
    "archive_url",
    "capture_time",
    "capture_time_status",
    "sha256",
    "byte_length",
    "encoding",
    "http_status",
    "content_type",
    "header_profile",
    "header_fingerprint",
    "parser_version",
    "normalizer_version",
    "rows_total",
    "rows_certified",
    "rows_quarantined",
    "duplicate_count",
    "source_unique_contribution_count",
    "quarantine_reason_counts",
    "verdict",
)


def _json_bytes(value: Any) -> bytes:
    """Serialize JSON using one stable, repository-wide representation."""
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")
        + b"\n"
    )


def _repo_path(repo_root: Path | str) -> Path:
    return Path(repo_root).expanduser().resolve()


def discover_source_files(repo_root: Path | str) -> list[Path]:
    """Return exactly the direct raw CSV glob, sorted by filename."""
    root = _repo_path(repo_root)
    raw_dir = root / "data" / "raw"
    files = sorted(
        (path for path in raw_dir.glob("contratos_*.csv") if path.is_file()),
        key=lambda path: path.name,
    )
    if not files:
        raise FileNotFoundError(f"no source files matched {SOURCE_GLOB}")
    return files


def fiscal_year_from_filename(path: Path) -> str:
    """Extract the fiscal year from the required preserved-file name."""
    match = _YEAR_RE.fullmatch(path.name)
    if match is None:
        raise ValueError(
            f"source file does not match contratos_<fiscal-year>.csv: {path.name}"
        )
    return match.group("year")


def source_channel_for_year(fiscal_year: str) -> str:
    """Derive the source lane from the repository's source inventory config."""
    if fiscal_year in ARCHIVED_ONLY_FISCAL_YEARS:
        return "archive_bulk"
    return "official_bulk"


def source_url_for_year(fiscal_year: str) -> str:
    """Return the standard OCPR bulk endpoint without making a network call."""
    return f"{BASE_URL}{DOWNLOAD_PATH}?q={fiscal_year}"


def archive_url_for_year(fiscal_year: str) -> str | None:
    """Return the exact Wayback capture whose bytes match the preserved file."""
    return ARCHIVE_CAPTURE_URLS.get(fiscal_year)


def _git_run(repo_root: Path, args: Iterable[str]) -> str:
    """Run a read-only Git query with locale-independent output."""
    env = os.environ.copy()
    env["LC_ALL"] = "C"
    env["LANG"] = "C"
    env["GIT_OPTIONAL_LOCKS"] = "0"
    result = subprocess.run(
        ["git", *args],
        cwd=str(repo_root),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        env=env,
    )
    return result.stdout


def git_first_seen_timestamp(repo_root: Path | str, source_path: Path) -> str | None:
    """Find the first commit containing the current file's exact Git blob.

    A path's adding commit is not necessarily the first appearance of its
    current bytes: a later sync or recovery commit can replace the blob.  Walk
    the deterministic path history, compare each commit's tree blob with the
    current exact blob, and return Git's stored committer timestamp with its
    original UTC offset.  An untracked/locally modified blob has no defensible
    Git-first-seen timestamp and returns ``None``.
    """
    root = _repo_path(repo_root)
    relative = source_path.resolve().relative_to(root).as_posix()
    try:
        current_blob = _git_run(
            root,
            ["hash-object", "--no-filters", "--", relative],
        ).strip()
        if not current_blob:
            return None
        commits = _git_run(
            root,
            [
                "log",
                "--all",
                "--full-history",
                "--reverse",
                "--format=%H",
                "--",
                relative,
            ]
        ).splitlines()
        for commit in commits:
            tree_output = _git_run(
                root,
                ["ls-tree", "-z", commit, "--", relative],
            )
            for entry in tree_output.split("\0"):
                if not entry:
                    continue
                header, _path = entry.split("\t", 1)
                pieces = header.split()
                if len(pieces) >= 3 and pieces[1] == "blob" and pieces[2] == current_blob:
                    timestamp = _git_run(
                        root,
                        ["show", "-s", "--format=%cI", commit],
                    ).strip()
                    if timestamp:
                        return timestamp
                    return None
    except (OSError, subprocess.CalledProcessError, ValueError):
        # A source can be supplied from a non-Git checkout.  Do not substitute
        # mtime or another volatile clock for missing provenance.
        return None
    return None


def _observed_capture_metadata(
    repo_root: Path,
    source_path: Path,
) -> tuple[str | None, str] | None:
    """Use exact-hash capture metadata so pre/post-commit output stays stable."""
    match = _YEAR_RE.fullmatch(source_path.name)
    if match is None:
        return None
    digest = hashlib.sha256(source_path.read_bytes()).hexdigest()
    sidecar = (
        repo_root
        / "data"
        / "evidence"
        / "bulk"
        / match.group("year")
        / f"{digest}.csv.json"
    )
    try:
        metadata = json.loads(sidecar.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    captured_at = metadata.get("captured_at")
    if metadata.get("sha256") != digest or not isinstance(captured_at, str):
        return None
    try:
        parsed = datetime.fromisoformat(captured_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return captured_at, "observed"


def _capture_metadata(
    repo_root: Path,
    source_path: Path,
) -> tuple[str | None, str]:
    observed = _observed_capture_metadata(repo_root, source_path)
    if observed is not None:
        return observed
    timestamp = git_first_seen_timestamp(repo_root, source_path)
    if timestamp is None:
        return None, "unknown"
    # Keep the check local so a future Git/environment change cannot pass an
    # incoherent pair into certify_bulk_file.
    if "T" not in timestamp or ("+" not in timestamp and "-" not in timestamp[10:]):
        return None, "unknown"
    return timestamp, "git_first_seen"


def _quarantine_identities(report: BulkCertificationReport) -> list[dict[str, Any]]:
    """Keep only stable identities, not the certifier's full row outcomes."""
    return [
        {
            "row_number": outcome.row_number,
            "reason": outcome.reason,
            "raw_record_sha256": outcome.raw_record_sha256,
            "raw_fields": list(outcome.raw_fields),
        }
        for outcome in report.row_outcomes
        if outcome.status == "quarantined"
    ]


def _stable_report_payload(
    report: BulkCertificationReport,
    *,
    source_file: str,
) -> dict[str, Any]:
    payload = {field: getattr(report, field) for field in _REPORT_FIELDS}
    payload["report_hash"] = report_hash(report)
    payload["schema_version"] = REPORT_SCHEMA_VERSION
    payload["source_file"] = source_file
    payload["quarantined_rows"] = _quarantine_identities(report)
    # Counter/dict order is stable in certify_bulk, but make the artifact
    # guarantee explicit at this serialization boundary too.
    payload["quarantine_reason_counts"] = dict(
        sorted(payload["quarantine_reason_counts"].items())
    )
    return payload


def _snapshot_summary(report_payload: Mapping[str, Any], report_path: str) -> dict[str, Any]:
    """Select report metadata for the inventory manifest without row details."""
    return {
        "fiscal_year": report_payload["fiscal_year"],
        "source_file": report_payload["source_file"],
        "source_channel": report_payload["source_channel"],
        "source_url": report_payload["source_url"],
        "archive_url": report_payload["archive_url"],
        "capture_time": report_payload["capture_time"],
        "capture_time_status": report_payload["capture_time_status"],
        "sha256": report_payload["sha256"],
        "byte_length": report_payload["byte_length"],
        "encoding": report_payload["encoding"],
        "header_profile": report_payload["header_profile"],
        "header_fingerprint": report_payload["header_fingerprint"],
        "parser_version": report_payload["parser_version"],
        "normalizer_version": report_payload["normalizer_version"],
        "rows_total": report_payload["rows_total"],
        "rows_certified": report_payload["rows_certified"],
        "rows_quarantined": report_payload["rows_quarantined"],
        "duplicate_count": report_payload["duplicate_count"],
        "source_unique_contribution_count": report_payload[
            "source_unique_contribution_count"
        ],
        "quarantine_reason_counts": report_payload["quarantine_reason_counts"],
        "verdict": report_payload["verdict"],
        "report_hash": report_payload["report_hash"],
        "report_path": report_path,
        "status": "certified",
    }


def _coordinate(source_file: str, fiscal_year: str, record_number: int) -> dict[str, Any]:
    return {
        "source_file": source_file,
        "fiscal_year": fiscal_year,
        "record_number": record_number,
    }


def _iter_normalized_hashes(
    source_path: Path,
    *,
    fiscal_year: str,
    source_file: str,
) -> Iterable[tuple[str, dict[str, Any]]]:
    """Yield the exact row hashes produced by ``ingest.ingest_raw_csv``.

    This intentionally mirrors its read/strip/column-map/normalization calls,
    including ``csv.DictReader``'s handling of shifted rows.  It yields one
    small normalized result at a time and never retains source rows.
    """
    encoding = detect_encoding(source_path)
    with open(source_path, encoding=encoding, newline="") as handle:
        content = handle.read()

    reader = csv.DictReader(io.StringIO(content))
    headers = reader.fieldnames or []
    column_lookup = {
        canonical: resolve_header(headers, canonical) for canonical in COLUMN_MAP
    }

    for record_number, raw_row in enumerate(reader, start=2):
        def get(canonical: str) -> str:
            key = column_lookup.get(canonical)
            return (raw_row.get(key) or "").strip() if key else ""

        normalized = normalize_contract_record(
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
            },
            default_source_type=RAW_SOURCE_TYPE,
            inserted_at=_NORMALIZATION_INSERTED_AT,
        )
        yield normalized["row_hash"], _coordinate(
            source_file, fiscal_year, record_number
        )


def _record_exclusions(
    source_path: Path,
    *,
    fiscal_year: str,
    source_file: str,
    first_seen: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Record duplicate row-hash exclusions in source order."""
    exclusions: list[dict[str, Any]] = []
    for row_hash_value, current in _iter_normalized_hashes(
        source_path,
        fiscal_year=fiscal_year,
        source_file=source_file,
    ):
        previous = first_seen.get(row_hash_value)
        if previous is None:
            first_seen[row_hash_value] = current
            continue
        exclusions.append(
            {
                "source_file": current["source_file"],
                "fiscal_year": current["fiscal_year"],
                "record_number": current["record_number"],
                "row_hash": row_hash_value,
                "reason": "canonical_row_hash_duplicate",
                "first_seen": dict(previous),
            }
        )
    return exclusions


def _unavailable_states(
    available_years: set[str],
) -> list[dict[str, Any]]:
    """Describe known unavailable years without inventing byte metadata."""
    states: list[dict[str, Any]] = []
    for fiscal_year in sorted(set(KNOWN_LIVE_404_YEARS) - available_years):
        states.append(
            {
                "fiscal_year": fiscal_year,
                "status": "unavailable",
                "source_channel": "official_bulk",
                "source_url": source_url_for_year(fiscal_year),
                "archive_url": None,
                "reason": "official bulk export is unavailable; no preserved bytes",
            }
        )
    return states


def build_artifacts(repo_root: Path | str) -> dict[str, bytes]:
    """Build all deterministic certification artifacts in memory.

    The returned keys are repository-relative POSIX paths.  Each source file is
    certified independently.  Full certifier outcomes live only for the
    current file; only quarantine identities and canonical exclusions survive
    into the returned bytes.
    """
    root = _repo_path(repo_root)
    source_files = discover_source_files(root)
    artifacts: dict[str, bytes] = {}
    snapshots: list[dict[str, Any]] = []
    first_seen: dict[str, dict[str, Any]] = {}
    all_exclusions: list[dict[str, Any]] = []
    aggregate_reason_counts: Counter[str] = Counter()
    rows_total = rows_certified = rows_quarantined = 0
    successful_years: set[str] = set()

    for source_path in source_files:
        fiscal_year = fiscal_year_from_filename(source_path)
        source_file = source_path.resolve().relative_to(root).as_posix()
        source_channel = source_channel_for_year(fiscal_year)
        report_path = REPORT_PATH_TEMPLATE.format(fiscal_year=fiscal_year)
        capture_time, capture_time_status = _capture_metadata(root, source_path)

        try:
            report = certify_bulk_file(
                source_path,
                source_channel=source_channel,
                fiscal_year=fiscal_year,
                source_url=source_url_for_year(fiscal_year),
                archive_url=archive_url_for_year(fiscal_year),
                capture_time=capture_time,
                capture_time_status=capture_time_status,
            )
        except BulkCertificationError as exc:
            # A file-level failure is isolated to its own report.  No other
            # source's certification or canonical accounting depends on it.
            failed_payload = {
                "schema_version": REPORT_SCHEMA_VERSION,
                "fiscal_year": fiscal_year,
                "source_file": source_file,
                "source_channel": source_channel,
                "source_url": source_url_for_year(fiscal_year),
                "archive_url": archive_url_for_year(fiscal_year),
                "capture_time": capture_time,
                "capture_time_status": capture_time_status,
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "quarantined_rows": [],
            }
            artifacts[report_path] = _json_bytes(failed_payload)
            snapshots.append(
                {
                    "fiscal_year": fiscal_year,
                    "source_file": source_file,
                    "source_channel": source_channel,
                    "source_url": source_url_for_year(fiscal_year),
                    "archive_url": archive_url_for_year(fiscal_year),
                    "capture_time": capture_time,
                    "capture_time_status": capture_time_status,
                    "report_path": report_path,
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
            continue

        payload = _stable_report_payload(report, source_file=source_file)
        artifacts[report_path] = _json_bytes(payload)
        snapshots.append(_snapshot_summary(payload, report_path))
        successful_years.add(fiscal_year)
        rows_total += report.rows_total
        rows_certified += report.rows_certified
        rows_quarantined += report.rows_quarantined
        aggregate_reason_counts.update(report.quarantine_reason_counts)

        # This is a separate pass matching ingest, not certification's exact
        # record-byte duplicate counter.  It runs in the same sorted file order
        # as the source discovery loop.
        all_exclusions.extend(
            _record_exclusions(
                source_path,
                fiscal_year=fiscal_year,
                source_file=source_file,
                first_seen=first_seen,
            )
        )
        del report

    unavailable = _unavailable_states(successful_years)
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "generator_version": GENERATOR_VERSION,
        "source_glob": SOURCE_GLOB,
        "source_sort": "filename_ascending",
        "encoding": "latin-1",
        "snapshots": snapshots,
        "available_fiscal_years": [
            item["fiscal_year"] for item in snapshots if item["status"] == "certified"
        ],
        "unavailable": unavailable,
        "aggregate": {
            "source_file_count": len(snapshots),
            "rows_total": rows_total,
            "rows_certified": rows_certified,
            "rows_quarantined": rows_quarantined,
            "quarantine_reason_counts": dict(sorted(aggregate_reason_counts.items())),
            "canonical_rows": len(first_seen),
            "canonical_exclusions": len(all_exclusions),
        },
        "canonical_exclusions": all_exclusions,
    }
    artifacts[MANIFEST_PATH] = _json_bytes(manifest)

    # Dict insertion order is not used for JSON determinism, but keeping the
    # public mapping ordered makes CLI output and callers' iteration predictable.
    return {
        path: artifacts[path]
        for path in [
            MANIFEST_PATH,
            *[
                REPORT_PATH_TEMPLATE.format(
                    fiscal_year=fiscal_year_from_filename(source_path)
                )
                for source_path in source_files
            ],
        ]
        if path in artifacts
    }


def write_artifacts(
    repo_root: Path | str,
    artifacts: Mapping[str, bytes] | None = None,
) -> dict[str, bytes]:
    """Write generated bytes below ``repo_root`` and return the bytes written."""
    root = _repo_path(repo_root)
    if artifacts is None:
        artifacts = build_artifacts(root)
    for relative_path, content in artifacts.items():
        destination = root / Path(relative_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    return dict(artifacts)


def check_artifacts(
    repo_root: Path | str,
    artifacts: Mapping[str, bytes] | None = None,
) -> bool:
    """Return whether on-disk artifacts exactly match a deterministic rebuild."""
    root = _repo_path(repo_root)
    expected = dict(artifacts) if artifacts is not None else build_artifacts(root)
    expected_paths = {Path(relative) for relative in expected}

    for relative_path, content in expected.items():
        destination = root / Path(relative_path)
        if not destination.is_file() or destination.read_bytes() != content:
            return False

    certification_root = root / "data" / "certification"
    if certification_root.exists():
        actual_paths = {
            path.relative_to(root)
            for path in certification_root.rglob("*.json")
            if path.is_file()
        }
        if actual_paths != expected_paths:
            return False
    return True


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate deterministic bulk certification artifacts"
    )
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="repository root (default: this checkout)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="rebuild in memory and verify existing artifacts without writing",
    )
    args = parser.parse_args(argv)
    root = _repo_path(args.repo_root)

    try:
        if args.check:
            ok = check_artifacts(root)
            print("bulk certification artifacts: " + ("OK" if ok else "MISMATCH"))
            return 0 if ok else 1
        written = write_artifacts(root)
        for relative_path in written:
            print(f"wrote {relative_path}")
        return 0
    except (OSError, ValueError, BulkCertificationError) as exc:
        print(f"bulk certification generator: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
