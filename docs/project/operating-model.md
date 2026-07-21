# Operating model

**Status:** Maintained description of current operation and maturity gates.

The operating model favors reproducible, evidence-preserving checkpoints over the appearance of continuous freshness. A script existing in the repository is not the same as a scheduled or production lane.

## Current automated cadence

The current GitHub Actions workflow is `.github/workflows/sync.yml`:

- **Weekly official bulk refresh — Sunday 07:00 UTC (03:00 AST):** runs `pipeline/download.py --refresh-live --force`. The command first performs bounded official-page/endpoint discovery using streamed GET requests with redirects disabled. Eligible bytes are streamed into ignored quarantine, validated against size/media/disposition/encoding/schema rules, retained under a content-addressed `data/evidence/bulk/` object, and only then explicitly promoted to the `data/raw/` compatibility view. Existing same-year bytes are retained before replacement; unchanged hashes are a clean no-op. Unavailable candidates are recorded without changing the active view, while held/invalid candidates fail closed and leave the last certified active bytes intact. Archive-only copies are never refreshed from the live endpoint. Any invalid payload, unexpected movement, changed-between-discovery-and-capture hash, or filesystem safety failure stops before ingest/publication and uploads bounded diagnostics.
- **Monthly audit rebuild — day 2 at 08:00 UTC (04:00 AST):** resets/re-ingests from tracked sources even when raw files did not change, then rebuilds the browser/full artifacts and the release asset as an audit pass.
- **Manual dispatch:** supports `weekly`, `monthly-audit`, and `full-rebuild` on `main`. A dispatch targeting another ref is intentionally skipped. The full rebuild is an explicit operation that downloads all requested available years and resets the database.
- **Artifact publication:** when a rebuild is needed, `pipeline/build_site_artifacts.py` creates the browser SQLite gzip/chunks, manifest, and full downloadable SQLite asset. The workflow safely stages only present or tracked publication paths, commits and pushes the browser artifacts first, uploads the matching full DB release asset only after that commit succeeds, and then explicitly dispatches Pages.

The current site remains a static GitHub Pages/sql.js application. A hosted query backend is not part of this operation. Phase 0 found that GitHub-token-authored sync commits did not trigger the path-filtered Pages push workflow, leaving the public deployment stale. The sync workflow now grants `contents: write`, `actions: write`, and `issues: write`, runs only for `refs/heads/main`, serializes runs in one non-cancelling concurrency group, and explicitly checks out current `main` before performing side effects. After the allowlisted publication commit step reports `changes_detected == 'true'`, it dispatches the existing `pages.yml` workflow on `main`, retrying a transient dispatch failure up to three times. A persistent dispatch failure leaves the sync run visibly failed and requires a manual `pages.yml` dispatch on `main`. Any sync failure uploads bounded capture diagnostics first, then creates or comments on the open GitHub issue titled `Contract Sync review required`; keeping one such issue open is the operator policy. This supplements Actions email rather than relying on it alone; the issue remains open until an operator classifies and resolves the endpoint, payload, schema, test, publication, or deployment exception. Milestone B exercised both branches: an unmatched optional staging path failed the first monthly publication attempt and opened the review issue, while the corrected run safely committed the browser artifacts, uploaded the matching full database, dispatched Pages at the bot commit, and produced a live manifest byte-identical to repository `main`. Future publication claims still require observing the resulting workflow/deployment and served manifest rather than inferring freshness from a commit.

There is no scheduled nightly live monitor in the current operating model. `pipeline/monitor.py` and monitor-state support remain dormant/prototype capability. The current database has no `live_monitor` rows.

The current repository defines a read-only PR/main CI baseline in `.github/workflows/ci.yml`. It runs the portability check, Python compilation, unittest discovery, and deterministic bulk-certification artifact check after installing the declared requirements. The scheduled/manual sync job runs the same baseline before acquisition. Weekly and full-rebuild acquisition then regenerate and recheck the certification manifest before ingest; a post-sync publication gate rechecks certification, reruns the suite, and requires `PRAGMA integrity_check = ok` before browser artifact construction, release upload, auto-commit, or Pages dispatch. `pages.yml` independently rechecks the certified archive revision before uploading the site. These are fail-closed gates: diagnostic artifact retention is allowed on failure, but tracked source replacement cannot advance to publication.

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
