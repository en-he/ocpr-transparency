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

**Status: Complete.** The repository contains a reproducible inventory and one independent report for each of 13 preserved fiscal years, `2010-2011` through `2022-2023`. The evidence records source channel, source/capture metadata, exact SHA-256 and byte size, parser/rule versions, row outcomes, duplicate accounting, canonical contribution, and a source-row reconciliation for every exclusion in the historical Phase 1 compatibility projection. The two oldest snapshots link to exact-hash Archive.org captures.

A missing or unavailable official CSV year is an explicit coverage state. It does **not** fail certification of the years whose source bytes are available, and it is not filled by assuming that live/recovered rows were once an official bulk export. The bounded discovery run used for Phase 1 closure confirmed the preserved `2022-2023` bytes were unchanged, `2023-2024` was listed but unavailable, and the later bounded candidates were unavailable at that observation time. Runtime discovery re-evaluates the bounded candidate window without treating that result as permanent.

The archive-facing contract fails closed on unknown source profiles and unsupported records, quarantines invalid or suspicious future responses before validation, retains accepted changed bytes content-addressed, requires explicit promotion, and gates ingest/publication/Pages on deterministic certification. Independent immutable-revision review, GitHub CI, exact-revision Pages deployment, CDN-manifest parity, and representative public browser behavior all passed before this completion claim was recorded.

## Phase 1→2 Milestone B — Data-contract migration

**Status: Complete.** This separately reviewed migration does not rewrite the Phase 1 archive certification. It adds an append-only bulk evidence/observation ledger, conservative cancellation evidence, one versioned reviewed normalization registry, full canonical and family identities, deterministic representatives, complete contributor lineage, and public language that treats `Cuantía` as an OCPR-reported source-row amount rather than spending, payment, disbursement, or a derived current contract value.

The strict bulk projection retains all 1,232,110 physical observations, including 507 parser quarantines and 521 duplicate contributors, and produces 1,231,082 canonical bulk records with 1,231,603 contributor links. Versioned canonical identity preserves ten entity-number-distinct records that the previous runtime hash conflated. The browser projection carries stable identities and validated cancellation fields but excludes the evidence and lineage audit tables.

Future official CSVs still pass through bounded discovery, immutable quarantine capture, exact schema certification, and explicit promotion before complete-set deterministic reprojection. An unknown schema, changed endpoint, invalid payload, or failed publication gate stops release and creates or comments on the open GitHub issue titled `Contract Sync review required`; no fuzzy or novel semantic match is auto-published.

The generated-artifact commit, exact-revision CI, monthly sync/release chain, Pages deployment, CDN-manifest parity, full/browser artifact integrity, and representative Spanish/English live-browser checks all passed before this completion claim. The deployed browser snapshot contains 1,238,171 rows: 1,231,082 strict canonical bulk records plus the 7,089 supplemental live-recovery compatibility rows. Milestone B does not include the Audit Workspace, candidate scoring, broad live acquisition, documents/OCR, hosted query, analytics, RAG, or scheduled live monitoring.

## Milestone C — Intermediate Audit Workspace

**Status: Deferred; not implemented.** Milestone C redesigns the static search surface and adds only evidence-supported filters, query-state behavior, exports, responsive/mobile behavior, bilingual copy, accessibility, and audit-workflow QA. It remains a separate release after Milestone B and before broad Phase 2 acquisition. Procurement, fund, PCo, document, and other sparse fields stay capability-gated until later evidence exists.

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
- validate amendment amount sign/encoding and cancellation effects on family/current-value lifecycle calculations; source-row cancellation raw/date/status semantics are already implemented;
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
