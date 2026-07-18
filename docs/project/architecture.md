# Maintained architecture

**Status:** Maintained current/target architecture. This document is the authority for the intended boundaries after the Phase 0 documentation migration.

The project is archival-first civic tech: preserve what was obtained, record how it was obtained, and expose projections without making a stronger claim than the evidence supports. The labels below are normative. `Current` describes this checkout; `Target` describes planned work; `Not yet implemented` is an explicit status, not a promise that a dormant script is production-ready.

## Current implementation

### Acquisition

- **Bulk lane — Current:** `pipeline/download.py` preserves fiscal-year CSVs in `data/raw/`. The current preserved set has 13 years, `2010-2011` through `2022-2023`. The two oldest files were recovered from Archive.org; newer preserved files are the available OCPR bulk corpus. A portal-listed post-2023 year is not treated as an available CSV unless the bytes are actually recovered and preserved.
- **Live search/API and detail lane — Current but manual:** `pipeline/live_recovery.py` and `pipeline/recover_live_parents.py` can query the live registry, parse search results and detail HTML, and write tracked Phase 2A recovery CSV output. Detail HTML is currently fetched for parsing; a durable raw-response archive is **not yet implemented**.
- **Scheduled live monitor — Not current operation:** `pipeline/monitor.py` exists and has state/provenance support, but the scheduled job is disabled. The current database has no `live_monitor` rows.
- **Document lane — Not yet implemented:** downloaded-document preservation, document metadata, OCR, and document search are future work. Documents remain a separate evidence lane and are not contract rows.

### Storage and transformation

- **Normalized records — Current, but not an observation ledger:** `pipeline/contract_utils.py` normalizes values and creates row hashes. `pipeline/ingest.py` reads the preserved CSVs and the tracked recovery CSV into SQLite, with row-level `source_type`, `source_url`, `source_contract_id`, and insertion timestamps.
- **Canonical records — Current:** `data/db/contratos.db` contains a deduplicated `contracts` table plus FTS5 data. It is a canonical projection, not an immutable ledger of every fetched response or observation. Rebuilding it from sources is supported.
- **Public projection — Current:** `pipeline/build_site_artifacts.py` creates a browser-oriented SQLite projection, gzip/chunks, a manifest, and a full downloadable SQLite artifact. The site runs `sql.js` in the browser; it does not query a hosted API.
- **Hosted query backend — Not yet implemented:** a server-side query API is the long-term replacement for requiring the browser to download the full searchable database. It must be built after evidence and reconciliation foundations are stable and before advanced analytics.

## Target flow

The target architecture keeps acquisition channels parallel and lets them converge only through explicit evidence and reconciliation stages:

```text
                         +----------------------+
                         | Official bulk CSVs  |
                         +----------+-----------+
                                    |
+----------------------+            |
| Live search/API      |------------+
| Detail HTML          |            |
+----------+-----------+            v
           |              +--------------------------+
+----------v-----------+  | Immutable evidence      |
| Documents / metadata |->| raw bytes + URL/request  |
| (separate lane)      |  | time + SHA-256 + status |
+----------------------+  +------------+-------------+
                                       |
                                       v
                         +--------------------------+
                         | Normalized observations  |
                         | parser/normalizer version|
                         +------------+-------------+
                                      |
                                      v
                         +--------------------------+
                         | Reconciliation, entity  |
                         | resolution, human review|
                         +------------+-------------+
                                      |
                                      v
                         +--------------------------+
                         | Canonical projections    |
                         | contracts + status/value |
                         +-----+----------+---------+
                               |          |
                    +----------v--+   +---v----------------+
                    | Hosted query |   | UI / exports /     |
                    | API (target) |   | alerts             |
                    +--------------+   +--------------------+
```

The diagram is a target flow. In the current checkout, the bulk and manual recovery lanes reach a deduplicated SQLite projection directly; the immutable-evidence and append-only-observation stages are incomplete, and the hosted API/document lane do not exist.

## Target boundaries and contracts

### Immutable evidence

**Target:** Every acquisition stores the original bytes (or a content-addressed immutable object) with source channel, request/response metadata, UTC capture time, SHA-256, and retention identity. A later parser correction adds a new observation; it does not overwrite the source bytes.

**Current gap:** raw CSV files and tracked recovery CSVs are preserved, and generated DB artifacts carry checksums in the manifest, but there is no unified evidence store for raw live JSON, detail HTML, and documents with one consistent metadata envelope.

### Observations

**Target:** A normalized observation references exactly one evidence object and records parser/normalizer versions, parse time, field-level extraction/status, and any validation warnings. Observations are append-only and can be reprojected.

**Current gap:** the SQLite row schema has provenance fields and `inserted_at`, but the canonical table is deduplicated. It does not preserve every source observation or every parser result as a first-class immutable record.

### Reconciliation and human review

**Target:** Link observations into contract families using deterministic keys where safe, then use confidence-scored entity resolution with a human review queue for uncertain matches. Keep “not found,” “unrecoverable,” “unreviewed,” and “conflict” distinct from “recovered.”

**Current:** Phase 2A recovery uses scripted search/detail parsing and family validation, with a tracked target ledger. The `7,177 recovered` and `4,806 unrecoverable` counts apply only to that defined ledger scope; they do not resolve the broader 31,264-family audit. Human-reviewed confidence scoring is **not yet implemented**.

### Canonical projections and public surfaces

**Current:** the `contracts` projection includes row-level source fields and can be rebuilt from CSV plus recovery inputs. The static browser DB and downloadable DB are projections, not source authority.

**Target:** publish separate projections for reported source fields, derived family/current-value fields, lifecycle/status fields, and evidence links. A hosted query API should serve the public UI and bounded exports; full artifact downloads remain useful for reproducibility until the API is mature.

### Documents and RAG

Documents are not contracts. **Target:** preserve document bytes and metadata, extract/OCR text in a document-index lane, and link documents to contract observations without replacing the contract evidence model. **Not yet implemented:** document downloads, OCR, document search, and document-to-contract review.

RAG is an optional downstream consumer of reviewed document text and canonical/evidence links. It is not a source of truth, is not required for Phase 0/1, and must wait until evidence retention, provenance, document search, reconciliation, and access controls are stable.

## Amount and status boundary

The current `amount`/`Cuantía` field is a reported or registered contract amount from a source row. It is not actual payment data. Amendment amounts may represent increase/decrease deltas, but sign/encoding and cancellation semantics still require source validation. A future derived current contract value must be labeled as derived and calculated only after those rules are validated. Actual payments require a separate evidence source and ledger; no such payment ledger is implemented here.

## Architectural non-goals for this migration

- Do not turn the canonical deduplicated SQLite database into a claim of complete registry coverage.
- Do not infer official bulk publication from a fiscal year that appears only in live/recovered records.
- Do not re-enable scheduled live monitoring before its raw evidence and durable-source behavior survive reset-based rebuilds.
- Do not merge documents, OCR text, or RAG indexes into the `contracts` table.
- Do not perform cleanup of historical repository residue as part of documentation authority migration.
