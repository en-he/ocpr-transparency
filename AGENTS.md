# OCPR Transparency Agent Instructions

OCPR Transparency is an archival-first civic-technology project that preserves and improves access to Puerto Rico public contract-registry evidence. The project is not merely a scraper or dashboard: acquisition, provenance, reconciliation, searchable documents, and honest public interpretation are all product boundaries.

## Start here

Before editing:

1. For maintainer/recovery work on Denali, confirm the repository root is the canonical checkout named `ocpr-transparency/` with upstream remote `https://github.com/en-he/ocpr-transparency.git`; do not edit the sibling recovery checkout. Contributors may use forks or differently named clones and should verify their intended base/upstream instead.
2. Read [`docs/project/README.md`](docs/project/README.md), then only the maintained document relevant to the task.
3. If present locally, read `docs/local/current-cycle.local.md` for the exact active checkpoint and next action. It is intentionally ignored and may be absent from public clones.
4. Run `git status --short --branch`, inspect recent commits, and explain any unexpected change before proceeding.
5. Do not use any isolated recovery checkout for normal development. Recovery checkouts contain machine-local preservation residue and exist only for evidence retention and deferred cleanup.

## Documentation authority and privacy boundary

- Current code, tests, workflows, tracked evidence, and generated manifests establish implemented behavior.
- `docs/project/` is the maintained public authority for architecture, provenance, operations, and roadmap.
- `docs/local/` is Denali-local operational context: recovery evidence, historical handoffs, execution ledgers, maintenance state, and temporary plans. It is ignored and must not be required to build or understand the public project.
- `.hermes/`, `.claude/settings.local.json`, `.claude/launch.json`, tool-specific worktrees/state/cache, credentials, tokens, absolute personal paths, and machine-specific reports remain local.
- Never make a local-only file public merely to simplify a handoff. Promote only durable, reviewed, privacy-safe conclusions into `docs/project/`.
- Historical source material may be stale. Preserve it locally; do not treat it as current authority or silently rewrite it.

## Non-negotiable evidence rules

- Preserve source evidence before normalization. A normalized SQLite row is a projection, not the archive itself.
- Keep acquisition lanes distinct: official bulk CSV, Archive.org-recovered bulk, live search/API, detail HTML, and contract/amendment documents.
- Future immutable evidence objects require original bytes, source URL/request context, UTC capture time, SHA-256, media/HTTP metadata, and acquisition status.
- Future normalized observations must identify their evidence object and parser/normalizer version. Parser corrections append observations; they do not overwrite source bytes.
- Keep documents and OCR text separate from `contracts`. Link them through reviewed evidence/observation identities.
- Treat missing, unavailable, unrecoverable, ambiguous, conflicting, and unreviewed as different states. Never convert absence into a synthetic fact.
- Never claim complete historical or live-registry coverage from the fiscal-year range in the canonical database.

## Amount and identity semantics

- Keep **OCPR-reported amount**, **derived current contract value**, and **actual payments** separate in schemas, APIs, UI labels, exports, and analytics.
- Amendment `Cuantía` is a working increase/decrease-delta interpretation pending validation of sign, encoding, family, cancellation, and lifecycle rules against source evidence.
- Actual payments require an independent payment evidence source; this repository does not currently contain a payment ledger.
- Contractor/family normalization must be auditable. Prefer conservative deterministic matches; richer entity resolution requires confidence, aliases, provenance, and human review.
- Do not modify legitimate source records merely because a person or company name resembles a developer identity.

## Phase boundaries

- **Phase 0 — repository and operating baseline:** portability, documentation authority, CI gates, reproducible build/browser certification, deployment-freshness verification, and maturity-gated recovery cleanup.
- **Phase 1 — bulk archive certification:** inventory and certify every available preserved CSV independently. Missing years are explicit coverage states, not Phase 1 failures.
- **Phase 2 — live reconciliation and enrichment:** begin with 2020–2023; preserve raw live evidence, create observations, validate family/amendment semantics, and add human-reviewed entity resolution before scaling weekly ingestion.
- **Phase 3 — hosted query and documents/OCR:** move routine search away from downloading the full browser DB; independently preserve, OCR, index, and search documents.
- **Phase 4 — analytics and optional RAG:** only after evidence, reconciliation, document search, citations, access controls, and hosted delivery are stable. RAG never becomes a source of truth.

Read [`docs/project/roadmap.md`](docs/project/roadmap.md) for public phase definitions and status. When `docs/local/current-cycle.local.md` exists, use it for Denali's exact active checkpoint. A phase description is not evidence that the phase is complete.

## Development workflow

- Use one writer at a time. Give implementation workers a bounded contract with immutable base, writable files, read-only context, prohibited scope, focused tests, and a stop condition.
- Preferred task loop: focused RED evidence when behavior changes → bounded implementation → controller diff/test verification → specification review → independent quality/security/data-integrity review → full relevant regression gate → green commit.
- Worker self-reports are leads, not proof. Verify files, outputs, hashes, remote resources, and browser behavior independently.
- Use capable coding workers for bounded execution when available, and reserve the strongest independent review route for genuinely adversarial architecture, evidence-loss, security, privacy, migration, or release gates. Provider/model choices are optional operational details; model prestige never substitutes for tests or evidence.
- Ask the project owner rather than guessing when a change affects public semantics, privacy, legal/takedown policy, source-retention policy, destructive cleanup, hosted-service cost, or publication.
- Keep public changelogs focused on user-facing behavior. Keep detailed worker sessions, allowance snapshots, internal QA ledgers, and maintenance state in `docs/local/`.

## Verification

Create a local environment when needed:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r pipeline/requirements.txt
```

Minimum repository gate:

```sh
.venv/bin/python pipeline/check_repository_portability.py
.venv/bin/python -m unittest discover -s tests
.venv/bin/python -m py_compile pipeline/*.py tests/*.py
git diff --check
```

For data or artifact changes, also require:

- SQLite `PRAGMA integrity_check` and expected source-type/row counts.
- Reproducible rebuild inputs and output hashes/counts.
- Manifest/artifact consistency, including split gzip chunks and downloadable DB assets.
- Browser verification of representative search, family/amendment routing, filters, downloads, and hostile-data rendering.
- Explicit Pages/deployment freshness verification; a committed artifact is not proof the public site deployed it.
- No unbounded live acquisition or document harvesting before a bounded pilot validates rate behavior, resumability, failure capture, storage, and QA.

## Git and publication safety

- Maintainer-controlled commits on Denali use the repository-local identity configured by the maintainer. Contributors must preserve and verify their own Git identity; never copy or impersonate another contributor's identity. Do not set global Git identity as part of project setup.
- `.mailmap` canonicalizes a historical maintainer identity non-destructively for display. It is not an instruction for contributor authorship. Do not rewrite published history, automation identities, tags, or archival refs without explicit approval and a coordinated migration plan.
- Do not push, force-push, publish releases, enable live schedules, deploy hosted services, or delete recovery evidence without the applicable approval gate.
- Generated database/artifact changes must not be mixed casually with source, schema, or documentation changes.
- The stale worktree, branch, stash, LFS cache, snapshots, and other recovery residue are isolated outside the canonical checkout. Their cleanup is governed by the local maintenance ledger and explicit itemized approval.
