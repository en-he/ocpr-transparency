# Data provenance and coverage

**Status:** Maintained provenance contract for the archival-first data product.

The product must answer two different questions without blending them:

1. **What evidence did OCPR publish or expose, and when did we capture it?**
2. **What normalized and reconciled projection do we currently make available?**

A canonical row is never a substitute for the evidence that produced it. The certified bulk path now retains append-only evidence, observations, projection outcomes, representative links, and contributor lineage. Live API/HTML capture and document/OCR preservation remain future evidence lanes.

## Evidence layers

| Layer | Meaning | Current implementation | Target preservation contract |
|---|---|---|---|
| Official bulk CSV | Bytes downloaded from an OCPR bulk-CSV endpoint while that endpoint served the file. | Thirteen active snapshots are preserved under `data/raw/`; exact per-file reports and the aggregate manifest are under `data/certification/`. Future accepted versions are first retained content-addressed under `data/evidence/bulk/`. | Keep original bytes, URL/parameters, retrieval time, HTTP metadata, SHA-256, file size, and source availability result. |
| Archive.org recovered bulk | A bulk CSV recovered from an Archive.org capture because the live portal no longer served the older export. | `2010-2011` and `2011-2012` are preserved in `data/raw/`, labeled `archive_bulk`, and linked to exact Wayback `id_` captures whose fetched bytes match their report hashes. | Store capture URL/timestamp and retrieval checksum alongside the original bytes; never relabel it as a live OCPR download. |
| Live search/API | Search-result JSON or equivalent response from the live OCPR registry. | Used by the manual recovery client and dormant monitor; raw response retention is **not yet implemented**. | Preserve the exact response bytes, endpoint/query/page, response status/content type, UTC capture time, and SHA-256. |
| Detail HTML | A contract/family detail-page response used to extract fields or establish a source link. | Fetched transiently by recovery/monitor parsing; `source_url` may survive in a normalized row, but raw HTML retention is **not yet implemented**. | Preserve exact HTML bytes and request metadata, with parser version and extraction warnings in later observations. |
| Documents | Downloaded contracts, amendments, attachments, or other linked files. | Document acquisition, retention, OCR, and document search are **not yet implemented**. | Keep original bytes, media type, source URL, capture metadata, checksum, and a stable document ID separate from contract records. |
| Normalized observations | A parser's typed interpretation of one evidence object at one parser/normalizer version. | Certified bulk ingestion writes immutable `evidence_objects`, append-only `bulk_observations`, explicit projection results/exclusions, raw values, field statuses, warnings, source coordinates, parser/normalizer versions, and evidence hashes. Supplemental live recovery remains a compatibility row path rather than a byte-retained evidence lane. | Extend the same evidence/observation contract to live API, HTML, and document acquisitions; never overwrite an earlier observation. |
| Canonical records/projections | Reconciled records optimized for queries, exports, and UI. | Bulk `contracts` rows carry separate versioned `canonical_id` and `family_id` values, a direct representative-observation link, and complete append-only representative/duplicate contributor lineage. The browser SQLite projects stable identities but intentionally omits the evidence and lineage ledgers. | Keep projection build revision/time, links to source observations, reconciliation decisions, status/value derivations, and reproducible build inputs. |

## Current inventory and bounded counts

- The certified bulk corpus contains 13 fiscal years, `2010-2011` through `2022-2023`: 1,232,110 physical source records, 1,231,603 structurally certified records, 507 retained `shifted_row` quarantines, and 521 true canonical duplicates. The Milestone B strict bulk projection contains 1,231,082 canonical records and 1,231,603 contributor links: one representative per canonical record plus all 521 duplicate contributors. Its 1,028 explicit exclusions are the 507 parser quarantines plus those 521 duplicates. Versioned canonical identity includes entity number, which preserves 10 distinct records that the prior runtime hash conflated.
- The immutable Phase 1 certification manifest remains a historical source-certification artifact: it records 1,231,508 permissive rows and 602 exclusions under its original truncated six-field hash contract. Milestone B does not rewrite those certified bytes or reuse that compatibility hash as canonical identity. These discrepancy dimensions describe different projection contracts and must not be added together. Post-2023 bulk exports are currently unavailable; a missing year is an unavailable-source state, not evidence of zero contracts.
- The pre-Milestone-B deployed compatibility DB contains `1,238,597` rows: `1,231,508` `csv` rows plus `7,089` `live_recovery` rows. There are no `live_monitor` rows in that baseline. Milestone B bulk-gate counts above do not silently claim that supplemental live-recovery evidence has been recertified under the bulk ledger.
- Bulk cancellation evidence accounts for every physical observation: 1,205,905 blank/NUL values remain `unknown`, 26,203 valid date-like values are retained as effective cancellation dates with `cancelled` status, and 2 malformed values remain `malformed`. Notification date and effective cancellation date are separate official concepts.
- The active normalization registry is `normalization-registry-1` using exact `lookup-v1` resolution. It contains 23 reviewed contractor aliases, rejects registry collisions, and has a separate deterministic manifest under `data/normalization/`; no fuzzy candidate is auto-published.
- The tracked Phase 2A recovery ledger has `11,983` targets: `7,177` recovered and `4,806` unrecoverable. The recovered-target count is not identical to the number of stored recovered rows because some outcomes resolve idempotently against an existing/recovered row.
- The ledger's scope is the multi-row missing-original backlog it tracked. A broader audit found `31,264` families without stored originals, including `26,209` single-row amendment-only families. Those are unresolved coverage/review states, not proof that all families should or can be recovered.
- The reviewed exact-match normalization registry is implemented. Broader Phase 2B confidence-scored entity resolution/structured enrichment and the document/OCR lane have not been implemented.

These figures describe this preserved/rebuilt baseline. They do not establish complete historical or live-registry coverage.

## Evidence envelope

Every future acquisition should retain, at minimum:

- `evidence_id` — stable content/object identity;
- source class (`official_bulk`, `archive_bulk`, `live_search_api`, `detail_html`, or `document`);
- source URL and request parameters, where applicable;
- capture timestamp in UTC and, when available, the upstream publication/update timestamp;
- HTTP status, content type, byte length, and the original response/file bytes;
- SHA-256 checksum of the preserved bytes;
- retrieval attempt/result and an explicit unavailable/error state when bytes cannot be obtained;
- parser and normalizer version for each derived observation;
- repository/build revision that produced a canonical projection.

A checksum identifies bytes; it does not prove that the source is official, complete, current, or semantically correct. Timestamps identify capture/build events; they do not turn a live snapshot into a historical archive without retention of the bytes.

The browser/full database build manifest records generated time, row counts, artifact sizes, and SHA-256 values. Separately, immutable `data/certification/bulk-manifest.json` and its 13 per-snapshot reports provide deterministic source-level hashes, sizes, source/capture metadata, parser/rule versions, row outcomes, and the Phase 1 exclusion reconciliation. `data/normalization/registry-manifest.json` independently versions and hashes the reviewed normalization registry without modifying those Phase 1 artifacts. Future accepted network captures add exact response metadata sidecars beside content-addressed evidence objects before the active compatibility view can change.

## Coverage axes

Coverage must be reported on separate axes rather than compressed into one percentage or one fiscal-year range:

| Axis | Question answered | Current gap/state |
|---|---|---|
| Bulk publication | Which fiscal-year CSVs were actually preserved from OCPR or an identified archive capture? | 13 preserved years; post-2023 official bulk exports unavailable. |
| Live freshness | What portion of the live registry has been queried, and how recently? | Manual recovery only for the defined backlog; scheduled monitor disabled; no live-monitor rows. |
| Family/record | Which rows and contract families have originals, amendments, or only amendment evidence? | Broad audit shows 31,264 families without stored originals; Phase 2A ledger is narrower. |
| Field | Which source fields are present, missing, or supplemented? | CSV fields are normalized; broad live detail enrichment (including procurement/fund fields) is not implemented. |
| Document | Which linked documents are retained, parsed, and searchable? | No durable document/OCR/search lane yet. |
| Reconciliation | Which links are deterministic, reviewed, uncertain, or conflicting? | Current recovery validation is scripted; human-reviewed confidence-scored entity resolution is future work. |
| Freshness/operations | Which artifacts were rebuilt and deployed from which inputs? | Phase 0 CI, reproducible-build checks, exact-revision Pages deployment, and live-manifest freshness verification passed; provenance remains partial because artifacts are not yet linked to complete per-source evidence metadata. |

## Source/status semantics

Current row-level `source_type` values are:

- `csv` — normalized from a preserved bulk CSV. This says where the row was observed, not that the row is a complete contract family.
- `live_recovery` — recovered from the live registry as part of the tracked Phase 2A missing-original process.
- `live_monitor` — supported by the schema/code for a monitor observation, but absent from the current database and not scheduled.

For certified bulk rows, canonical projection and contributor relations link back to immutable observation/evidence IDs; `source_url` and `source_contract_id` remain compatibility/display fields rather than the provenance authority. Supplemental live-recovery rows still rely on normalized source links and do not identify an immutable API/HTML response object.

Recovery ledger statuses (`pending`, `recovered`, `unrecoverable`, and `ambiguous`) describe a recovery target's workflow outcome. A terminal `unrecoverable` result means that the defined recovery attempt did not establish a parent; it does not mean the whole registry lacks that contract, and it does not close families outside the target seed scope. “Unreviewed,” “conflict,” and “not searched” should remain distinct in future reconciliation queues.

## Amount, amendment, and status semantics

Public surfaces must keep the following concepts separate:

1. **Source-row reported amount** — the source's normalized `Cuantía`/`amount` value for one physical source row. It is an OCPR-reported contract amount, not a payment, actual spending, or current contract value.
2. **Representative original reported amount** — the source-row reported amount from the deterministic original/representative row selected for a displayed family. It is not a reconstructed current contract value and does not absorb amendment rows.
3. **Family-row sum** — an unvalidated arithmetic sum of the reported amounts on the source rows included in a displayed family or filtered result. If shown, it must carry an explicit warning that it is not current contract value or actual payments. It must never be labeled spending, disbursement, or family value.
4. **Derived current contract value** — a future, explicitly labeled calculation over a reviewed contract family. It may combine a base amount with validated amendment deltas and cancellation/status rules. It must not be presented until the sign/encoding and family rules are source-validated.
5. **Actual payments** — money actually disbursed. This requires a separate payment evidence source and ledger; no payment data is implemented in this repository.

The current amount-range filter applies to `contracts.amount` before family grouping: it therefore selects **source-row reported amounts**. Family summaries are then built only from matching rows. Public filter help and exports must disclose that scope.

The project's current working interpretation treats amendment `Cuantía` as a possible increase/decrease delta, but its sign/encoding remains unvalidated against complete OCPR source evidence. Until that validation and reconciliation exist, dashboard and result sums are unvalidated aggregates of reported source-row amounts, not claims about government spending, actual payments, or current contract value.

Cancellation is represented separately as exact source evidence (`cancellation_raw`), a validated effective cancellation date when present (`cancellation_date`), and a closed status (`cancelled`, `not_cancelled`, `unknown`, or `malformed`). Blank or NUL bulk evidence is `unknown`, not false; the legacy integer `cancelled` field is only a compatibility projection derived from validated status. Notification date and effective cancellation date are distinct concepts and must not be conflated.

## Known gaps and preservation rules

- Raw live API responses and detail HTML are not yet durably retained; current recovery output preserves normalized results and links, not every response byte.
- Downloaded documents, OCR text, document search, and document-to-contract review are not implemented.
- The full database contains both the deduplicated canonical projection and separate append-only bulk evidence/observation/contributor ledgers. The browser database intentionally omits those audit ledgers and is not source authority.
- Versioned canonical and family IDs are deterministic projection identities, not proof that two real-world legal entities are the same. Future fuzzy/entity resolution must expose confidence and human review decisions.
- The current archive-year list is intentionally derived from actual files in `data/raw/`, not from all fiscal years appearing in the canonical DB.
- Any source gap, failed fetch, parser warning, or unresolved family must be retained as an explicit state; do not silently drop it or fill it with a synthetic fact.
