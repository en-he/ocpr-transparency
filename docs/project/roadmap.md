# Roadmap

**Status:** Maintained roadmap. Phase labels are delivery order, not claims of completion.

The project is archival-first. We stabilize evidence and reconciliation before optimizing queries or adding intelligence. A missing source snapshot is recorded as a coverage state; it is not a failure of a phase that certifies the snapshots that are available.

## Phase 0 — Repository and operating baseline

**Status: Complete.** The repository now has a maintained public authority map, an explicit public/local documentation boundary, cross-platform portability controls, a read-only CI baseline, pre-side-effect sync validation, independently reproducible database/browser projections, representative browser certification, and a remotely observed sync-to-Pages freshness path. Imported recovery state was preserved under archival refs and a read-back-verified short-term backup before the isolated recovery checkout was retired under the approved maintenance gate.

Exit criteria:

- maintained documents label Current / Target / Not yet implemented without blurring them;
- local historical Markdown body equality and SVG byte equality were mechanically verified;
- the recovery ledger's narrow scope is carried forward without turning it into a completeness claim;
- tests, clean isolated rebuilds, browser behavior, CI, sync, release publication, and exact-revision Pages deployment were observed green;
- recovery retirement occurred only after archival refs, an independent Git bundle, exact file manifests, restore/read-back checks, and explicit itemized approval.

## Phase 1 — Certify the available bulk archive

**Status: Next certification gate; implementation exists but Phase 1 certification is not yet claimed.** Establish a reproducible inventory of preserved bulk files, their source channel, checksums, capture metadata, parser version, row counts, and rebuild inputs. The current available corpus is 13 preserved fiscal years, `2010-2011` through `2022-2023`; the two oldest are Archive.org recoveries.

A missing or unavailable official CSV year is an explicit coverage state. It does **not** fail Phase 1 certification of the years whose source bytes are available, and it must not be filled by assuming that live/recovered rows were once an official bulk export. Post-2023 bulk exports remain unavailable in the current preserved corpus.

Phase 1 also hardens the archive-facing contract: public year lists derive from preserved raw CSVs, normalized/canonical projections retain source links, and generated artifacts carry reproducible build metadata and checksums.

## Phase 2 — Live reconciliation and enrichment

**Status: Target; Phase 2B is not implemented.** Phase 2 owns live acquisition, reconciliation, missing-original handling, and structured enrichment on top of the bulk archive.

### 2A — Bounded missing-original recovery

The tracked Phase 2A ledger is terminal for its defined multi-row backlog (`7,177` recovered, `4,806` unrecoverable), but the broader audit still found `31,264` families without stored originals, including `26,209` single-row amendment-only families. Treat that work as classified evidence, not universal recovery. New bulk years may require a new bounded recovery pass.

### 2B — Live reconciliation/enrichment

Start with fiscal years **2020-2023** and the highest-value/most ambiguous families. Build the durable evidence and observation model before broadening volume:

- preserve raw live search/API responses and detail HTML with checksums/timestamps;
- retain normalized observations and parser versions instead of writing directly to a deduplicated canonical row;
- expose supplemental live fields without relabeling a CSV-base row as live-origin;
- implement human-reviewed, confidence-scored entity/family resolution with explicit conflicts and review status;
- validate amendment amount sign/encoding and cancellation semantics;
- distinguish reported amount, derived current contract value, and actual payments;
- only after those controls are stable, operate a minimum weekly live-ingestion/reconciliation cadence. The current scheduled live monitor remains disabled.

## Phase 3 — Hosted query and document/OCR lanes

These are distinct workstreams with a strict order:

### 3A — Hosted query backend

Build the hosted query API and a versioned public projection before advanced analytics. Keep full downloadable artifacts for reproducibility during migration, but move routine public queries away from downloading the full browser database. Add authentication/rate limits/observability and deploy/freshness checks appropriate to the public service.

### 3B — Document acquisition, OCR, and search

Implement a separate document evidence/index lane: download and checksum original documents, preserve metadata, extract text, OCR where needed, index/search documents, and link reviewed document identities to contract observations. Documents must not be collapsed into the `contracts` table or used to redefine bulk archive coverage. This lane is **not yet implemented**.

## Phase 4 — Optional downstream analytics and RAG

Only after Phases 1-3 foundations are stable may the project add richer dashboards, entity-network analysis, alerts over reviewed changes, or optional RAG over retained/OCR'd documents. RAG is downstream and optional: it cannot repair missing evidence, make an entity match authoritative, calculate payments, or replace citations to the underlying evidence and observations.

## Cross-phase gates

Every phase must retain:

- source bytes or an explicit unavailable/error record;
- URL/request, capture/build timestamp, checksum, parser/build version, and coverage scope;
- separate status for observed, normalized, reconciled, reviewed, unavailable, and unresolved;
- a reproducible projection/build path and a tested deployment/freshness story;
- an honest statement of what remains outside scope.

Cleanup of old worktrees, settings, refs, stashes, caches, snapshots, and other residue is not a roadmap deliverable. It remains a separate, user-approved maturity-gated operation.
