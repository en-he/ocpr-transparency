# Operating model

**Status:** Maintained description of current operation and maturity gates.

The operating model favors reproducible, evidence-preserving checkpoints over the appearance of continuous freshness. A script existing in the repository is not the same as a scheduled or production lane.

## Current automated cadence

The current GitHub Actions workflow is `.github/workflows/sync.yml`:

- **Weekly official bulk refresh — Sunday 07:00 UTC (03:00 AST):** runs `pipeline/download.py --refresh-live --force`. It refreshes the newest already-preserved live bulk year and probes newer fiscal years. Archive-only copies are kept in place. If `data/raw/` changes, the job resets/re-ingests tracked sources with `pipeline/ingest.py --reset`.
- **Monthly audit rebuild — day 2 at 08:00 UTC (04:00 AST):** resets/re-ingests from tracked sources even when raw files did not change, then rebuilds the browser/full artifacts and the release asset as an audit pass.
- **Manual dispatch:** supports `weekly`, `monthly-audit`, and `full-rebuild`. The full rebuild is an explicit operation that downloads all requested available years and resets the database.
- **Artifact publication:** when a rebuild is needed, `pipeline/build_site_artifacts.py` creates the browser SQLite gzip/chunks, manifest, and full downloadable SQLite asset. The workflow commits selected data/artifact paths and uploads the full DB release asset.

The current site remains a static GitHub Pages/sql.js application. A hosted query backend is not part of this operation. Automated data commits do not have a reliable, independently verified Pages freshness/deployment gate: Pages is triggered by pushes affecting `site/**` or its workflow, while the data workflow uses an automated commit path. Treat the deployed site as requiring an explicit post-run check until that trigger/deployment contract is tested and hardened.

There is no scheduled nightly live monitor in the current operating model. `pipeline/monitor.py` and monitor-state support remain dormant/prototype capability. The current database has no `live_monitor` rows.

The current repository now defines a read-only PR/main CI baseline in `.github/workflows/ci.yml`. It runs the portability check, Python compilation check, and unittest discovery after installing the declared requirements. The scheduled/manual sync job runs the same checks immediately after dependency installation and before hydration, acquisition, database mutation, artifact or release publication, or auto-commit. This is a pre-publication validation gate; it does not resolve the separate Pages freshness/deployment gap described above.

## Current manual Phase 2A recovery

Phase 2A is manual-script-first and source-preserving within its defined scope:

1. Seed or refresh tracked targets with `pipeline/seed_live_recovery_targets.py`.
2. Process bounded fiscal-year/target batches with `pipeline/recover_live_parents.py`, using explicit batch, retry, or all-pending options.
3. Validate search candidates against the primary contract-family identity and parse detail HTML when available.
4. Record terminal target outcomes and normalized recovered rows in the tracked recovery CSVs.
5. Rebuild with `pipeline/ingest.py --reset`; the reset path re-reads `data/recovery/live_recovered_contracts.csv` so current recovery rows are not silently lost.
6. Review counts, unresolved families, source links, and artifact provenance before publication.

The present ledger has 11,983 targets, all terminal (`7,177` recovered and `4,806` unrecoverable), but it covers the tracked multi-row missing-original backlog. It does not close the broader 31,264-family audit or implement Phase 2B enrichment. Do not report Phase 2A as universal parent recovery.

## Green-checkpoint workflow

A checkpoint is green only when the applicable evidence is present and the next destructive or public action is bounded:

1. **Scope gate:** confirm changed paths and ensure only permitted documentation files are being changed for this migration. Do not mix cleanup, generated artifacts, or source-code changes into the docs authority task.
2. **Evidence gate:** inventory source layers and unavailable years; preserve raw bytes/links and record checksums/timestamps where the current lane supports them. Never infer coverage from the canonical DB alone.
3. **Transformation gate:** run the relevant parser/normalization/recovery tests; check warnings, row counts, source-type counts, and recovery scope.
4. **Projection gate:** rebuild or inspect the canonical/browser projections when data changes; run SQLite integrity checks, compare manifest counts/hashes, and verify that archive-facing years come from preserved raw CSVs.
5. **Publication gate:** inspect the diff, run Markdown/mechanical checks, and explicitly test Pages/workflow freshness when a deployment claim is made. A generated artifact commit is not itself proof that the public site deployed it.
6. **Review gate:** record open ambiguity, especially amendment amount semantics, cancellation behavior, unresolved family coverage, and any source whose raw bytes were not retained.

These gates are evidence checkpoints, not automatic Phase completion claims.

## Local maintenance boundary

Recovery-checkout cleanup, local caches, worktrees, stashes, execution state, and machine-specific paths are maintained in ignored `docs/local/` records on Denali. They are not dependencies of the public product. Destructive cleanup remains maturity-gated and requires an explicit, itemized approval; public documentation changes never authorize pruning Git history or local recovery evidence.

## Operational target, not current behavior

The target cadence is a durable minimum weekly live-ingestion/reconciliation run after the live evidence lane is implemented, with alerts based on reviewed changes rather than raw row churn. It must preserve raw API/detail responses, use append-only observations, survive reset-based rebuilds, and surface failed/unavailable fetches. This target must not be enabled by simply restoring the old nightly schedule.
