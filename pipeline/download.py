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
import re
import time
from datetime import date
from pathlib import Path

import requests

from config import (
    ARCHIVED_ONLY_FISCAL_YEARS,
    BASE_URL,
    BULK_CSV_START_YEAR,
    DOWNLOAD_PATH,
    HEADERS,
    KNOWN_LIVE_404_YEARS,
    RAW_DIR,
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


def download_year(year: str, out_dir: Path, force: bool = False) -> bool:
    """Download a single fiscal year CSV. Returns True on success."""
    out_path = out_dir / f"contratos_{year}.csv"
    tmp_path = out_dir / f"contratos_{year}.csv.part"

    if out_path.exists() and not force:
        print(f"  [skip] {year} already exists ({out_path.stat().st_size / 1024:.1f} KB)")
        return True

    if year in ARCHIVED_ONLY_FISCAL_YEARS:
        if out_path.exists():
            print(f"  [keep] {year} preserved archived copy retained ({out_path.stat().st_size / 1024:.1f} KB)")
            return True
        print(f"  [warn] {year} is archive-only and missing locally; live portal copy is unavailable")
        return False

    url = f"{BASE_URL}{DOWNLOAD_PATH}?q={year}"
    if year in KNOWN_LIVE_404_YEARS:
        print(f"  [probe] {year} is still listed in the portal UI but currently unresolved in the official record")
    print(f"  [fetch] {year} <- {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=120, stream=True)
        resp.raise_for_status()

        out_path.parent.mkdir(parents=True, exist_ok=True)
        total = 0
        if tmp_path.exists():
            tmp_path.unlink()
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                total += len(chunk)
        tmp_path.replace(out_path)

        print(f"  [ok]   {year} -> {out_path} ({total / 1024:.1f} KB)")
        return True

    except (requests.RequestException, OSError) as e:
        print(f"  [err]  {year} failed: {e}")
        if tmp_path.exists():
            tmp_path.unlink()
        if out_path.exists():
            print(f"  [keep] {year} left existing local copy in place")
        elif year in KNOWN_LIVE_404_YEARS:
            print(f"  [note] {year} remains absent locally until an official bulk CSV is recovered")
        return False


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
    parser.add_argument("--force", action="store_true", help="Re-download existing files")
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

    print(f"\nDownloading {len(years)} fiscal year(s) -> {out_dir}\n")

    ok = err = skip = 0
    for i, year in enumerate(years):
        out_path = out_dir / f"contratos_{year}.csv"
        existed = out_path.exists() and not args.force

        success = download_year(year, out_dir, force=args.force)

        if existed and success:
            skip += 1
        elif success:
            ok += 1
        else:
            err += 1

        if i < len(years) - 1 and not existed:
            time.sleep(args.delay)

    print(f"\nDone. {ok} downloaded, {skip} skipped, {err} errors.")


if __name__ == "__main__":
    main()
