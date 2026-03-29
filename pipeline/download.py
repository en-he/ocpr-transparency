"""
Download all fiscal year bulk CSVs from the OCPR contract registry.

Usage:
    python pipeline/download.py
    python pipeline/download.py --years 2023-2024 2022-2023
    python pipeline/download.py --force
"""
import argparse
import time
from pathlib import Path

import requests

from config import ALL_FISCAL_YEARS, BASE_URL, DOWNLOAD_PATH, HEADERS, RAW_DIR


def download_year(year: str, out_dir: Path, force: bool = False) -> bool:
    """Download a single fiscal year CSV. Returns True on success."""
    out_path = out_dir / f"contratos_{year}.csv"

    if out_path.exists() and not force:
        print(f"  [skip] {year} already exists ({out_path.stat().st_size / 1024:.1f} KB)")
        return True

    url = f"{BASE_URL}{DOWNLOAD_PATH}?q={year}"
    print(f"  [fetch] {year} <- {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=120, stream=True)
        resp.raise_for_status()

        out_path.parent.mkdir(parents=True, exist_ok=True)
        total = 0
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
                total += len(chunk)

        print(f"  [ok]   {year} -> {out_path} ({total / 1024:.1f} KB)")
        return True

    except requests.RequestException as e:
        print(f"  [err]  {year} failed: {e}")
        if out_path.exists():
            out_path.unlink()
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
