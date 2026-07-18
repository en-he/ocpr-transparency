# Data provenance and coverage

**Status:** Maintained provenance contract for the archival-first data product.

The product must answer two different questions without blending them:

1. **What evidence did OCPR publish or expose, and when did we capture it?**
2. **What normalized and reconciled projection do we currently make available?**

A canonical row is never a substitute for the evidence that produced it. Current implementation is marked explicitly below; the target evidence model is not yet fully implemented.

## Evidence layers

| Layer | Meaning | Current implementation | Target preservation contract |
|---|---|---|---|
| Official bulk CSV | Bytes downloaded from an OCPR bulk-CSV endpoint while that endpoint served the file. | Preserved under `data/raw/`; these files define public bulk-archive coverage. | Keep original bytes, URL/parameters, retrieval time, HTTP metadata, SHA-256, file size, and source availability result. |
| Archive.org recovered bulk | A bulk CSV recovered from an Archive.org capture because the live portal no longer served the older export. | `2010-2011` and `2011-2012` are preserved in `data/raw/` and are labeled as recovered archive copies. | Store capture URL/timestamp and retrieval checksum alongside the original bytes; never relabel it as a live OCPR download. |
| Live search/API | Search-result JSON or equivalent response from the live OCPR registry. | Used by the manual recovery client and dormant monitor; raw response retention is **not yet implemented**. | Preserve the exact response bytes, endpoint/query/page, response status/content type, UTC capture time, and SHA-256. |
| Detail HTML | A contract/family detail-page response used to extract fields or establish a source link. | Fetched transiently by recovery/monitor parsing; `source_url` may survive in a normalized row, but raw HTML retention is **not yet implemented**. | Preserve exact HTML bytes and request metadata, with parser version and extraction warnings in later observations. |
| Documents | Downloaded contracts, amendments, attachments, or other linked files. | Document acquisition, retention, OCR, and document search are **not yet implemented**. | Keep original bytes, media type, source URL, capture metadata, checksum, and a stable document ID separate from contract records. |
| Normalized observations | A parser's typed interpretation of one evidence object at one parser/normalizer version. | Current normalization writes deduplicated rows to SQLite/recovery CSV and records row-level source fields plus `inserted_at`; there is no append-only observation table. | Store observation ID, evidence ID, parser/normalizer versions, parsed fields, field status, warnings, and creation time. Never overwrite an earlier observation. |
| Canonical records/projections | Reconciled records optimized for queries, exports, and UI. | `data/db/contratos.db` has a deduplicated `contracts` table and FTS5; browser SQLite and release artifacts are derived projections. | Keep projection build revision/time, links to source observations, reconciliation decisions, status/value derivations, and reproducible build inputs. |

## Current inventory and bounded counts

- Preserved bulk files cover 13 fiscal years, `2010-2011` through `2022-2023`. Post-2023 bulk exports are currently unavailable in the preserved corpus. A missing year is an unavailable-source state, not evidence of zero contracts.
- The canonical DB contains `1,238,597` rows: `1,231,508` `csv` rows plus `7,089` `live_recovery` rows. There are no `live_monitor` rows in the current baseline.
- The tracked Phase 2A recovery ledger has `11,983` targets: `7,177` recovered and `4,806` unrecoverable. The recovered-target count is not identical to the number of stored recovered rows because some outcomes resolve idempotently against an existing/recovered row.
- The ledger's scope is the multi-row missing-original backlog it tracked. A broader audit found `31,264` families without stored originals, including `26,209` single-row amendment-only families. Those are unresolved coverage/review states, not proof that all families should or can be recovered.
- Tier 2B structured enrichment and the document/OCR lane have not been implemented.

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

The current build manifest already records generated time, row counts, artifact sizes, and SHA-256 values for the browser/full DB artifacts. That is useful artifact provenance, but it is not yet a substitute for per-source evidence metadata.

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
| Freshness/operations | Which artifacts were rebuilt and deployed from which inputs? | Local/workflow manifests provide partial artifact provenance; CI tests and deployment freshness checks are incomplete. |

## Source/status semantics

Current row-level `source_type` values are:

- `csv` — normalized from a preserved bulk CSV. This says where the row was observed, not that the row is a complete contract family.
- `live_recovery` — recovered from the live registry as part of the tracked Phase 2A missing-original process.
- `live_monitor` — supported by the schema/code for a monitor observation, but absent from the current database and not scheduled.

`source_url` and `source_contract_id` are useful row-level links, but they do not currently identify an immutable response object. The future model should link canonical fields to observation/evidence IDs instead of relying on one source label.

Recovery ledger statuses (`pending`, `recovered`, `unrecoverable`, and `ambiguous`) describe a recovery target's workflow outcome. A terminal `unrecoverable` result means that the defined recovery attempt did not establish a parent; it does not mean the whole registry lacks that contract, and it does not close families outside the target seed scope. “Unreviewed,” “conflict,” and “not searched” should remain distinct in future reconciliation queues.

## Amount, amendment, and status semantics

Public surfaces must keep three concepts separate:

1. **Reported amount** — the source's `Cuantía`/amount value for a row. This is a registered contract amount as reported by the source. It is not verified actual spending or payment.
2. **Derived current contract value** — a future, explicitly labeled calculation over a reviewed contract family. It may combine a base amount with validated amendment deltas and cancellation/status rules. It must not be presented until the sign/encoding and family rules are source-validated.
3. **Actual payments** — money actually disbursed. This requires a separate payment evidence source and ledger; no payment data is implemented in this repository.

The project's current working interpretation treats amendment `Cuantía` as a possible increase/decrease delta, but its sign/encoding and cancellation semantics still require validation against OCPR source evidence. Until that validation and reconciliation exist, sums in the current SQLite/dashboard surfaces are aggregates of reported row amounts, not a claim about government spending or actual payments. The source `cancelled` flag should remain source status until lifecycle semantics are validated.

## Known gaps and preservation rules

- Raw live API responses and detail HTML are not yet durably retained; current recovery output preserves normalized results and links, not every response byte.
- Downloaded documents, OCR text, document search, and document-to-contract review are not implemented.
- The canonical DB is deduplicated and therefore cannot serve as the immutable observation ledger.
- Current row hashes/deduplication are implementation aids, not proof of entity identity. Future entity resolution must expose confidence and human review decisions.
- The current archive-year list is intentionally derived from actual files in `data/raw/`, not from all fiscal years appearing in the canonical DB.
- Any source gap, failed fetch, parser warning, or unresolved family must be retained as an explicit state; do not silently drop it or fill it with a synthetic fact.
