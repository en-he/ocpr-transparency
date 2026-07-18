# OCPR Transparency

Open-source tool for searching and analyzing Puerto Rico government contracts from the [Oficina del Contralor](https://consultacontratos.ocpr.gov.pr/).

**1.23M+ contract records in the current snapshot | 13 preserved fiscal years (2010-2023)**

Documentation: [project authority map](docs/project/README.md)

## Quick Start

### Search UI (no setup needed)

Visit the hosted site or run locally:

```bash
python3 -m http.server 8080 -d site
# Open http://localhost:8080
```

The site loads a SQLite database in your browser via WebAssembly — no backend required.

### Build the database from scratch

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r pipeline/requirements.txt

# Refresh currently available OCPR CSVs
# Older missing years preserved in data/raw stay in place
.venv/bin/python pipeline/download.py

# Delete/recreate the SQLite DB, then ingest with full-text search
.venv/bin/python pipeline/ingest.py --reset

# Build the browser DB, full downloadable DB, and manifest
.venv/bin/python pipeline/build_site_artifacts.py
```

## Development verification

From the repository root, run the local baseline used by the PR/main CI workflow and by sync before side effects:

```bash
.venv/bin/python pipeline/check_repository_portability.py
.venv/bin/python -m py_compile pipeline/*.py tests/*.py
.venv/bin/python -m unittest discover -s tests
```

## Features

- **Cross-entity search** — find a contractor across all government agencies
- **Amount range filter** — surface the largest contracts
- **Full-text search** — keyword search across all fields via FTS5
- **Date filtering** — search by award date range
- **Category & fiscal year filters** — drill into specific areas
- **CSV export** — download filtered results
- **Shareable searches** — URL hash state for sharing specific queries
- **Offline capable** — database cached in IndexedDB after first load

## Project Structure

```
pipeline/          Python data pipeline
  config.py        Constants, column mappings, OCPR URLs
  download.py      Bulk CSV downloader with archive-safe refreshes
  ingest.py        CSV → SQLite with FTS5 full-text search
  monitor.py       Deferred live-monitor prototype (scheduled monitoring disabled)

site/              Static search UI (sql.js / WebAssembly)
  index.html       SPA shell (Spanish)
  js/db.js         sql.js wrapper, query builder, IndexedDB cache
  js/ui.js         DOM rendering, filters, pagination, export
  js/app.js        Init, event wiring, search orchestration

data/
  raw/             Archived fiscal year CSVs (committed in normal Git)
  db/monitor_state.json  Tracked delta-sync cursor metadata

site/
  contratos.db.gz(.part-*)  Browser-serving SQLite DB artifact(s) tracked in normal Git
```

The full downloadable SQLite DB is published as a GitHub Release asset rather than stored in the repo. When the browser DB grows too large for GitHub's single-file limit, the site build automatically splits it into `contratos.db.gz.part-*` chunks and the manifest tells the frontend how to reassemble them. This keeps GitHub Pages and clones working without Git LFS.

## Data Source
Structured contract data comes from the OCPR contract registry at `consultacontratos.ocpr.gov.pr`. Fiscal-year CSVs are preserved when the official bulk endpoint serves them; the two oldest preserved files, `2010-2011` and `2011-2012`, were recovered from Archive.org. The integrity of the source data remains the responsibility of the entities that granted the contracts, as stated by OCPR.

The current preserved bulk corpus contains 13 fiscal years, `2010-2011` through `2022-2023`. Post-2023 official bulk exports are not currently preserved. A year shown in the live portal is not treated as an available bulk snapshot until its source bytes are recovered and recorded.

## Known Data Gaps

Some contract families appear in the bulk CSV exports only as amendments, even when the live OCPR website still shows an original parent contract. Examples already confirmed in this repo include `2022-000019` (`IEMES PSC`) and `2008-000669` (`IEMS & M H, INC.`).

Post-2023 official bulk exports are not currently preserved; live-registry records and bulk-publication coverage are tracked as separate evidence states.

The current site handles those families with a synthetic parent/family view so users are not shown a misleading amendment as the top-level contract. The tracked Phase 2A recovery ledger contains `11,983` targets (`7,177` recovered and `4,806` unrecoverable) for its defined multi-row missing-original scope. A broader audit found `31,264` families without stored originals, including `26,209` single-row amendment-only families; these are coverage states, not a completeness claim. The maintained interpretation is in [`docs/project/data-provenance.md`](docs/project/data-provenance.md).

### Available fields

| Field | Description |
|-------|-------------|
| contract_number | Contract identifier |
| entity | Government agency |
| contractor | Contractor name |
| amount | Reported registered contract amount (not verified payments) |
| award_date | Date contract was granted |
| valid_from / valid_to | Contract validity period |
| service_category | Category of service |
| service_type | Specific service type |
| procurement_method | How the contract was procured |
| fund_type | Funding source |
| fiscal_year | PR fiscal year (July-June) |

## Automated Sync

GitHub Actions currently supports a weekly official bulk refresh, a monthly audit rebuild, and a manual full rebuild:

- **Weekly** (Sunday) — refresh the newest already-preserved live bulk year and probe newer fiscal years
- **Monthly** (day 2) — reset/rebuild from tracked sources and republish the full database artifact as an audit pass
- **Manual full rebuild** — reprocess all available bulk sources on demand

Scheduled live monitoring is disabled. The `monitor.py` code and monitor-state path are retained as deferred capability, not as an active nightly service.

The workflow commits the browser DB artifact chunks and tracked metadata to the repo, and publishes the full SQLite DB as a GitHub Release asset for open-data downloads.

Archive-only preserved years such as `2010-2011` and `2011-2012` remain committed in `data/raw/` and are intentionally not replaced from the live portal during refreshes.

## Legal Context

Puerto Rico's [Act 122 of 2019](https://bvirtualogp.pr.gov/ogp/Bvirtual/leyesreferencia/PDF/2-ingles/122-2019.pdf) (Open Government Data Act) mandates that government agencies publish contracts and procurement data openly.

## License

MIT
