# OCPR Transparency

Open-source tool for searching and analyzing Puerto Rico government contracts from the [Oficina del Contralor](https://consultacontratos.ocpr.gov.pr/).

**994,000+ contracts | $170B+ in government spending | 11 fiscal years (2012-2023)**

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
pip install requests

# Download all fiscal year CSVs from OCPR
python3 pipeline/download.py

# Ingest into SQLite with full-text search
python3 pipeline/ingest.py

# Build the browser DB, full downloadable DB, and manifest
python3 pipeline/build_site_artifacts.py
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
  download.py      Bulk CSV downloader (11 fiscal years)
  ingest.py        CSV → SQLite with FTS5 full-text search
  monitor.py       Tier 2: nightly delta sync from OCPR search

site/              Static search UI (sql.js / WebAssembly)
  index.html       SPA shell (Spanish)
  js/db.js         sql.js wrapper, query builder, IndexedDB cache
  js/ui.js         DOM rendering, filters, pagination, export
  js/app.js        Init, event wiring, search orchestration

data/
  raw/             Archived fiscal year CSVs (committed in normal Git)
  db/monitor_state.json  Tracked delta-sync cursor metadata

site/
  contratos.db.gz  Browser-serving SQLite DB (committed in normal Git)
```

The full downloadable SQLite DB is published as a GitHub Release asset rather than stored in the repo. This keeps GitHub Pages and clones working without Git LFS.

## Data Source

All data comes from the OCPR contract registry at `consultacontratos.ocpr.gov.pr`. Fiscal year CSV exports are downloaded via their bulk download endpoint. The integrity of the data is the responsibility of the entities that granted the contracts, as stated by OCPR.

## Known Data Gaps

Some contract families appear in the bulk CSV exports only as amendments, even when the live OCPR website still shows an original parent contract. Examples already confirmed in this repo include `2022-000019` (`IEMES PSC`) and `2008-000669` (`IEMS & M H, INC.`).

The current site handles those families with a synthetic parent header so users are not shown a misleading amendment as the top-level contract. A future Tier 2 recovery task will query the live site to backfill missing original contracts with explicit provenance. The concrete design is tracked in [docs/backlog.md](docs/backlog.md).

### Available fields

| Field | Description |
|-------|-------------|
| contract_number | Contract identifier |
| entity | Government agency |
| contractor | Contractor name |
| amount | Contract value (USD) |
| award_date | Date contract was granted |
| valid_from / valid_to | Contract validity period |
| service_category | Category of service |
| service_type | Specific service type |
| procurement_method | How the contract was procured |
| fund_type | Funding source |
| fiscal_year | PR fiscal year (July-June) |

## Automated Sync

GitHub Actions runs three sync schedules:

- **Nightly** — delta sync via `monitor.py` (new contracts since last run)
- **Weekly** (Sunday) — re-download current fiscal year CSVs
- **Monthly** (manual) — full rebuild from all CSVs

The workflow commits the browser DB and tracked metadata to the repo, and publishes the full SQLite DB as a GitHub Release asset for open-data downloads.

## Legal Context

Puerto Rico's [Act 122 of 2019](https://bvirtualogp.pr.gov/ogp/Bvirtual/leyesreferencia/PDF/2-ingles/122-2019.pdf) (Open Government Data Act) mandates that government agencies publish contracts and procurement data openly.

## License

MIT
