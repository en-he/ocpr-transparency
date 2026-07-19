"""
Download fiscal year bulk CSVs from the OCPR contract registry.

Years preserved locally from archive.org are treated as archive-only copies and
are kept in place during refreshes even when the live portal no longer serves
them.

Usage:
    python pipeline/download.py
    python pipeline/download.py --years 2022-2023 2023-2024
    python pipeline/download.py --refresh-live
    python pipeline/download.py --force
"""
import argparse
import hashlib
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

from capture_bulk_snapshot import (
    capture_bulk_snapshot,
    promote_bulk_snapshot,
    retain_existing_bulk_snapshot,
)
from discover_bulk_sources import SourceObservation, discover_bulk_sources

from config import (
    ARCHIVED_ONLY_FISCAL_YEARS,
    ALLOWED_SOURCE_HOSTS,
    BASE_URL,
    BULK_CSV_START_YEAR,
    DOWNLOAD_PATH,
    EVIDENCE_DIR,
    HEADERS,
    MAX_BULK_BYTES,
    QUARANTINE_DIR,
    RAW_DIR,
    REGISTRY_URL,
    bulk_csv_years_through_current,
    current_fiscal_year,
    format_fiscal_year,
    parse_fiscal_year,
)


FISCAL_YEAR_FILENAME_PATTERN = re.compile(r"^contratos_(\d{4}-\d{4})\.csv$")


def discover_local_raw_fiscal_years(out_dir: Path) -> list[str]:
    fiscal_years: list[str] = []
    for csv_path in sorted(out_dir.glob("contratos_*.csv")):
        match = FISCAL_YEAR_FILENAME_PATTERN.match(csv_path.name)
        if match:
            fiscal_years.append(match.group(1))
    return sorted(set(fiscal_years), key=parse_fiscal_year, reverse=True)


def discover_live_refresh_years(out_dir: Path, *, today: date | None = None) -> list[str]:
    local_fiscal_years = discover_local_raw_fiscal_years(out_dir)
    current_start_year, _ = parse_fiscal_year(current_fiscal_year(today))

    refreshable_local_years = [
        fiscal_year
        for fiscal_year in local_fiscal_years
        if fiscal_year not in ARCHIVED_ONLY_FISCAL_YEARS
    ]

    if refreshable_local_years:
        start_year, _ = parse_fiscal_year(refreshable_local_years[0])
    else:
        start_year = BULK_CSV_START_YEAR

    if start_year > current_start_year:
        return []

    return [
        format_fiscal_year(start_year)
        for start_year in range(start_year, current_start_year + 1)
    ]


def _requests_get(url: str, *, method: str = "GET", **kwargs):
    if method.upper() != "GET":
        raise ValueError("bulk source transport is GET-only")
    return requests.get(url, **kwargs)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _has_symlink_component(path: Path) -> bool:
    absolute = path if path.is_absolute() else Path.cwd() / path
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        if current.is_symlink():
            return True
    return False


def download_year(
    year: str,
    out_dir: Path,
    force: bool = False,
    *,
    observation: SourceObservation | None = None,
    http_get=_requests_get,
    quarantine_dir: Path = QUARANTINE_DIR,
    evidence_dir: Path = EVIDENCE_DIR,
    captured_at: str | None = None,
    max_bytes: int = MAX_BULK_BYTES,
) -> bool:
    """Capture and explicitly promote one independently discovered CSV."""
    del force  # Hash identity, not a force flag, controls replacement.
    out_path = out_dir / f"contratos_{year}.csv"
    if _has_symlink_component(out_path):
        print(f"  [hold] {year} active path contains a symlink")
        return False

    if year in ARCHIVED_ONLY_FISCAL_YEARS:
        if out_path.exists():
            print(f"  [keep] {year} preserved archived copy retained")
            return True
        print(f"  [warn] {year} is archive-only and missing locally")
        return False

    if observation is None or observation.fiscal_year != year:
        print(f"  [hold] {year} has no bounded discovery observation")
        return False
    if observation.review_required or not observation.eligible:
        if observation.status in {"listed_but_404", "unavailable"}:
            print(f"  [none] {year} {observation.status}; active evidence unchanged")
            return True
        print(f"  [hold] {year} {observation.status}: {observation.reason}")
        return False
    if not observation.sha256:
        print(f"  [hold] {year} eligible observation has no source hash")
        return False

    if out_path.is_file() and _sha256_file(out_path) == observation.sha256:
        print(f"  [same] {year} exact source hash already active")
        return True

    print(f"  [capture] {year} <- {observation.requested_url}")
    response = None
    try:
        response = http_get(
            observation.requested_url,
            method="GET",
            headers=dict(HEADERS),
            timeout=120,
            allow_redirects=False,
            stream=True,
        )
        result = capture_bulk_snapshot(
            response=response,
            fiscal_year=year,
            source_url=observation.requested_url,
            quarantine_dir=quarantine_dir,
            evidence_dir=evidence_dir,
            active_view=out_path,
            allowed_hosts=ALLOWED_SOURCE_HOSTS,
            captured_at=captured_at
            or datetime.now(timezone.utc).isoformat(timespec="seconds"),
            max_bytes=max_bytes,
        )
    except (requests.RequestException, OSError, ValueError) as exc:
        print(f"  [err]  {year} capture failed: {exc}")
        return False
    finally:
        close = getattr(response, "close", None)
        if callable(close):
            close()

    if result.status not in {"captured", "unchanged"} or result.evidence_path is None:
        print(f"  [hold] {year} {result.status}: {result.reason}")
        return False
    if result.sha256 != observation.sha256:
        print(f"  [hold] {year} changed between discovery and capture")
        return False

    try:
        if out_path.is_file():
            retain_existing_bulk_snapshot(
                source_path=out_path,
                evidence_dir=evidence_dir,
                fiscal_year=year,
                max_bytes=max_bytes,
            )
        promote_bulk_snapshot(evidence_path=result.evidence_path, active_view=out_path)
    except (OSError, ValueError) as exc:
        print(f"  [err]  {year} promotion failed: {exc}")
        return False
    if _sha256_file(out_path) != observation.sha256:
        print(f"  [err]  {year} promoted bytes failed hash verification")
        return False

    print(f"  [ok]   {year} promoted certified evidence {observation.sha256}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Download OCPR fiscal year CSVs")
    parser.add_argument(
        "--years", nargs="*",
        help="Fiscal years to download (default: every bulk CSV year from 2010-2011 through the current fiscal year)",
    )
    parser.add_argument(
        "--refresh-live",
        action="store_true",
        help="Only probe fiscal years newer than the newest locally preserved raw CSV",
    )
    parser.add_argument("--out-dir", default=str(RAW_DIR))
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-evaluate discovered sources; identical hashes remain no-ops",
    )
    parser.add_argument("--delay", type=float, default=1.5, help="Seconds between requests")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.years and args.refresh_live:
        parser.error("--years and --refresh-live cannot be used together")

    if args.years:
        years = args.years
    elif args.refresh_live:
        years = discover_live_refresh_years(out_dir)
    else:
        years = bulk_csv_years_through_current()

    if not years:
        print(f"\nNo new live fiscal years to probe in {out_dir}.\n")
        return

    print(f"\nDiscovering bounded official sources for {len(years)} fiscal year(s)\n")
    local_live_years = [
        year
        for year in discover_local_raw_fiscal_years(out_dir)
        if year not in ARCHIVED_ONLY_FISCAL_YEARS
    ]
    newest_certified = local_live_years[0] if local_live_years else None
    report = discover_bulk_sources(
        registry_url=REGISTRY_URL,
        newest_certified_year=newest_certified,
        current_fiscal_year=current_fiscal_year(),
        http_get=_requests_get,
        allowed_hosts=ALLOWED_SOURCE_HOSTS,
        bulk_url_template=f"{BASE_URL}{DOWNLOAD_PATH}?q={{fiscal_year}}",
        adjacent_newer_years=1,
        max_bytes=MAX_BULK_BYTES,
    )
    observations = {item.fiscal_year: item for item in report.observations}

    promoted = err = unchanged = 0
    for i, year in enumerate(years):
        out_path = out_dir / f"contratos_{year}.csv"
        safe_active = out_path.is_file() and not _has_symlink_component(out_path)
        before_hash = _sha256_file(out_path) if safe_active else None
        observation = observations.get(year)
        if observation is None and safe_active:
            print(f"  [keep] {year} outside bounded refresh window; certified active retained")
            success = True
        else:
            success = download_year(
                year,
                out_dir,
                force=args.force,
                observation=observation,
            )
        after_safe = out_path.is_file() and not _has_symlink_component(out_path)
        after_hash = _sha256_file(out_path) if after_safe else None

        if not success:
            err += 1
        elif before_hash == after_hash:
            unchanged += 1
        else:
            promoted += 1

        if i < len(years) - 1:
            time.sleep(args.delay)

    print(
        f"\nDone. {promoted} promoted, {unchanged} unchanged/unavailable, "
        f"{err} held or failed."
    )
    if err:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
