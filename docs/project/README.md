# OCPR Transparency project documentation authority

**Status:** Maintained project documentation for the archival-first civic-data system.

This directory is the maintained authority for the project's current state, target architecture, evidence/provenance rules, operating model, and roadmap. It deliberately separates what is implemented from what is intended. A statement in these documents is not a substitute for checking the repository, the data artifacts, or the workflows named as evidence.

## Authority order

Use the following order when claims conflict:

1. **Current implementation evidence** — tracked pipeline/site code, tracked data and recovery files, generated manifests, tests, and GitHub Actions workflows. These establish what the checkout actually does.
2. **Maintained project documents** — the documents in this directory explain the current implementation and the approved target direction. They must label `Current`, `Target`, and `Not yet implemented` explicitly.
3. **README** — the repository README is a concise orientation and quick-start page. It links here but is not the architecture or provenance authority.
4. **Denali-local records, when present** — ignored `docs/local/` files preserve recovery evidence, historical handoffs, current-cycle state, and maintenance ledgers. They are operational context, not public architecture authority, and are intentionally absent from ordinary clones.

When a local historical claim and current code disagree, retain the local source record and correct the maintained document rather than silently rewriting history.

## Maintained documents

- [`architecture.md`](architecture.md) — current architecture, target evidence flow, boundaries, and explicit non-implementation statements.
- [`data-provenance.md`](data-provenance.md) — evidence layers, coverage axes, source/status semantics, amount semantics, and known gaps.
- [`operating-model.md`](operating-model.md) — actual weekly/monthly operation, manual recovery, green checkpoints, and deferred cleanup policy.
- [`roadmap.md`](roadmap.md) — Phase 0 onward, dependencies, gates, and the downstream/optional position of RAG.

The ignored local companion records are indexed from `docs/local/README.local.md` when they exist. Public contributors must not need those files to understand or build the project.

## Current baseline used by these documents

The current repository evidence supports these bounded statements:

- `data/raw/` preserves 13 fiscal-year CSV snapshots, from `2010-2011` through `2022-2023`. The two oldest preserved files, `2010-2011` and `2011-2012`, were recovered from Archive.org. Post-2023 official bulk exports are not currently preserved.
- The canonical database contains `1,238,597` deduplicated rows: `1,231,508` with `source_type=csv` and `7,089` with `source_type=live_recovery`. It contains no `live_monitor` rows in this baseline.
- The recovery ledger contains `11,983` targets (`7,177` recovered and `4,806` unrecoverable), but that ledger covers the defined multi-row missing-original backlog. A broader audit found `31,264` families without stored originals, including `26,209` single-row amendment-only families.
- The public product is a static browser SQLite/sql.js application with downloadable artifacts. A hosted query API is a target, not the current backend.
- Phase 2B enrichment and the document/OCR lane are not implemented. Scheduled live monitoring is disabled.

These are coverage and implementation snapshots, not claims that the registry, its history, or its contract families are complete.

## Reading and change rules

- Preserve source bytes and provenance before normalizing or projecting data.
- Treat bulk publication coverage, live-registry coverage, document coverage, and canonical-row coverage as different axes.
- Treat a reported/registered contract amount as distinct from a derived current contract value and from actual payments. Do not call the current amount aggregate “government spending.”
- A target feature is not current merely because code, fixtures, a historical plan, or a dormant script exists.
- A missing source snapshot is an explicit coverage state. It is not evidence that a year had no records, and it does not invalidate certification of available snapshots.
- Cleanup of stale repository residue is a separate maturity-gated decision. Documentation migration does not authorize deletion.
