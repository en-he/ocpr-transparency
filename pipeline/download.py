"""
Download fiscal year bulk CSVs from the OCPR contract registry.

Years preserved locally from archive.org are treated as archive-only copies and
are kept in place during refreshes even when the live portal no longer serves
them.

Usage:
    python pipeline/download.py
    python pipeline/download.py --years 2022-2023 2023-2024
    python pipeline/download.py --force
"""
import argparse
import time
from pathlib import Path

import requests

from config import (
    ALL_FISCAL_YEARS,
    ARCHIVED_ONLY_FISCAL_YEARS,
    BASE_URL,
    DOWNLOAD_PATH,
    HEADERS,
    KNOWN_LIVE_404_YEARS,
    RAW_DIR,
)


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
        "--years", nargs="*", default=ALL_FISCAL_YEARS,
        help="Fiscal years to download (default: all)",
    )
    parser.add_argument("--out-dir", default=str(RAW_DIR))
    parser.add_argument("--force", action="store_true", help="Re-download existing files")
    parser.add_argument("--delay", type=float, default=1.5, help="Seconds between requests")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nDownloading {len(args.years)} fiscal year(s) -> {out_dir}\n")

    ok = err = skip = 0
    for i, year in enumerate(args.years):
        out_path = out_dir / f"contratos_{year}.csv"
        existed = out_path.exists() and not args.force

        success = download_year(year, out_dir, force=args.force)

        if existed and success:
            skip += 1
        elif success:
            ok += 1
        else:
            err += 1

        if i < len(args.years) - 1 and not existed:
            time.sleep(args.delay)

    print(f"\nDone. {ok} downloaded, {skip} skipped, {err} errors.")


if __name__ == "__main__":
    main()
