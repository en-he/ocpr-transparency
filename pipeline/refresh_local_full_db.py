"""
Refresh the local full database from the latest published GitHub release asset.

Downloads contratos-full.db.gz from the data-latest release, verifies the SHA-256
checksum recorded in site/data-manifest.json, and decompresses it into
data/db/contratos.db.

Usage:
    python3 pipeline/refresh_local_full_db.py
    python3 pipeline/refresh_local_full_db.py --no-backup
    python3 pipeline/refresh_local_full_db.py --manifest path/to/data-manifest.json
"""
import argparse
import gzip
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from config import DB_PATH, HEADERS, REPO_ROOT

DEFAULT_MANIFEST = REPO_ROOT / "site" / "data-manifest.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_gz(url: str, dest: Path) -> None:
    tmp = dest.with_suffix(".part")
    tmp.unlink(missing_ok=True)

    print(f"  [fetch] {url}")

    curl_bin = shutil.which("curl")
    if curl_bin:
        head_cmd = [
            curl_bin,
            "-sSIL",
            "-H", f"User-Agent: {HEADERS['User-Agent']}",
            "-H", f"Accept: {HEADERS['Accept']}",
            "-H", f"Referer: {HEADERS['Referer']}",
            url,
        ]
        try:
            head = subprocess.run(head_cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as err:
            tmp.unlink(missing_ok=True)
            sys.exit(f"  [error] curl HEAD request failed (exit {err.returncode}): {url}")

        if " 404 " in head.stdout:
            sys.exit(
                f"  [error] Release asset not found (404): {url}\n"
                "         Has the data-latest release been published on GitHub?"
            )

        download_cmd = [
            curl_bin,
            "-fL",
            "--progress-bar",
            "-H", f"User-Agent: {HEADERS['User-Agent']}",
            "-H", f"Accept: {HEADERS['Accept']}",
            "-H", f"Referer: {HEADERS['Referer']}",
            "-o", str(tmp),
            url,
        ]
        try:
            subprocess.run(download_cmd, check=True)
        except subprocess.CalledProcessError as err:
            tmp.unlink(missing_ok=True)
            sys.exit(f"  [error] curl download failed (exit {err.returncode}): {url}")
        tmp.rename(dest)
        return

    request = Request(url, headers=HEADERS)

    try:
        total = 0
        with urlopen(request, timeout=300) as resp, open(tmp, "wb") as fh:
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                fh.write(chunk)
                total += len(chunk)
                print(f"\r  [fetch] {total / (1024 * 1024):.1f} MB downloaded", end="", flush=True)
    except HTTPError as err:
        tmp.unlink(missing_ok=True)
        if err.code == 404:
            sys.exit(
                f"  [error] Release asset not found (404): {url}\n"
                "         Has the data-latest release been published on GitHub?"
            )
        sys.exit(f"  [error] HTTP {err.code} while downloading: {url}")
    except (URLError, OSError) as err:
        tmp.unlink(missing_ok=True)
        sys.exit(f"  [error] Download failed: {err}")
    print()

    tmp.rename(dest)


def decompress_gz(gz_path: Path, out_path: Path) -> None:
    print(f"  [decompress] {gz_path.name} -> {out_path.name}")
    with gzip.open(gz_path, "rb") as src, open(out_path, "wb") as dst:
        shutil.copyfileobj(src, dst)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh local contratos.db from the latest published release asset"
    )
    parser.add_argument(
        "--manifest",
        default=str(DEFAULT_MANIFEST),
        help="Path to data-manifest.json (default: site/data-manifest.json)",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip backing up the existing local DB before replacement",
    )
    parser.add_argument(
        "--skip-checksum",
        action="store_true",
        help="Skip SHA-256 verification (use when manifest and release are known to be out of sync)",
    )
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        sys.exit(f"[error] Manifest not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text())
    full_db_meta = manifest.get("full_download_db")
    if not full_db_meta:
        sys.exit("[error] Manifest does not contain full_download_db metadata.")

    url = full_db_meta["url"]
    expected_sha256 = full_db_meta.get("sha256")

    gz_path = DB_PATH.parent / "contratos-full.db.gz"
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Download
    download_gz(url, gz_path)

    # Verify checksum
    if args.skip_checksum:
        print("  [verify] Skipping SHA-256 check (--skip-checksum).")
    elif expected_sha256:
        print(f"  [verify] Checking SHA-256...", end=" ", flush=True)
        actual = sha256_file(gz_path)
        if actual != expected_sha256:
            gz_path.unlink(missing_ok=True)
            sys.exit(
                f"\n[error] Checksum mismatch!\n"
                f"  expected: {expected_sha256}\n"
                f"  actual:   {actual}\n"
                "  The downloaded file has been removed.\n"
                "  If the release was recently rebuilt, run with --skip-checksum\n"
                "  or trigger a full-rebuild workflow to resync the manifest."
            )
        print("OK")
    else:
        print("  [warn] No SHA-256 in manifest; skipping checksum verification.")

    # Backup existing DB
    if DB_PATH.exists() and not args.no_backup:
        backup = DB_PATH.with_suffix(".db.bak")
        print(f"  [backup] {DB_PATH.name} -> {backup.name}")
        shutil.copy2(DB_PATH, backup)

    # Decompress
    decompress_gz(gz_path, DB_PATH)
    gz_path.unlink()

    size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    print(f"  [done] {DB_PATH} ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
