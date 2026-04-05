# Backlog

## Tier 2 Data Recovery

### Recover missing original contracts from the live OCPR site

Problem:
Some contract families in the fiscal-year CSV exports only contain amendment rows, while the official live site still shows an original parent contract. Confirmed examples:

- `2022-000019` / `Autoridad de Transporte Marítimo de Puerto Rico y las Islas Municipios` / `IEMES PSC`
- `2008-000669` / `Municipio de Humacao` / `IEMS & M H, INC.`

Evidence:

- The archived CSVs in `data/raw/` only contain amendment rows for those families.
- The rebuilt SQLite DB in `data/db/contratos.db` matches the CSVs.
- The live OCPR site still exposes a parent/original contract row for at least `2022-000019`.

Goal:
Backfill missing original parent contracts from the live OCPR website and preserve clear provenance for those recovered rows.

Proposed implementation:

1. Detect amendment-only families where every row has a non-empty `amendment`.
2. Query the live OCPR search/detail flow for the exact contract family.
3. Extract the original contract row and normalize it with the same parsers used by ingest and monitor.
4. Store recovered rows with explicit provenance fields such as `source_type = live_recovery` and `source_url`.
5. Prevent duplicate recovery by hashing and family-level dedup rules.
6. Prefer recovered original rows as the parent contract in the UI when available.

Acceptance criteria:

- `2022-000019` renders with a real recovered original row instead of a synthetic family header.
- `2008-000669` is either recovered or explicitly flagged as unrecoverable from the live site.
- Recovered rows are distinguishable from CSV-origin rows.
- The monitor/recovery process is idempotent across reruns.

## Developer Ergonomics

### Refresh local full DB from the latest published release asset

Problem:
The canonical full database will live on GitHub as the `data-latest` release asset, while the uncompressed working DB remains local-only. After automated syncs run on GitHub, a local developer currently has to refresh the local full DB manually.

Goal:
Provide a one-command way to pull the latest published full DB and refresh the local uncompressed copy.

Proposed implementation:

1. Add a helper script such as `pipeline/refresh_local_full_db.py`.
2. Download `contratos-full.db.gz` from the `data-latest` release asset.
3. Verify checksum when `site/data-manifest.json` provides one.
4. Decompress into `data/db/contratos.db`.
5. Optionally keep a backup of the previous local DB before replacement.

Acceptance criteria:

- Running the helper updates the local `data/db/contratos.db` to the latest published dataset.
- The helper fails clearly if the release asset is unavailable or the checksum does not match.
- The workflow remains one-way by default: GitHub updates do not overwrite local data unless the helper is run explicitly.
