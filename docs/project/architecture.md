# Maintained architecture

**Status:** Maintained current/target architecture. This document is the authority for the intended boundaries after the Phase 0 documentation migration.

The project is archival-first civic tech: preserve what was obtained, record how it was obtained, and expose projections without making a stronger claim than the evidence supports. The labels below are normative. `Current` describes this checkout; `Target` describes planned work; `Not yet implemented` is an explicit status, not a promise that a dormant script is production-ready.

## Current implementation

### Acquisition

- **Bulk lane — Current:** `pipeline/discover_bulk_sources.py` performs bounded official-source discovery; `pipeline/capture_bulk_snapshot.py` streams responses into quarantine, validates and fully certifies them, retains accepted bytes content-addressed, and exposes explicit promotion; `pipeline/download.py` integrates that sequence for sync. `data/raw/` remains the active compatibility view. The current certified set has 13 years, `2010-2011` through `2022-2023`, with deterministic reports under `data/certification/`. The two oldest files are exact-hash Archive.org recoveries. Unavailable post-2023 years remain explicit states rather than synthetic snapshots.
- **Live search/API and detail lane — Current but manual:** `pipeline/live_recovery.py` and `pipeline/recover_live_parents.py` can query the live registry, parse search results and detail HTML, and write tracked Phase 2A recovery CSV output. Detail HTML is currently fetched for parsing; a durable raw-response archive is **not yet implemented**.
- **Scheduled live monitor — Not current operation:** `pipeline/monitor.py` exists and has state/provenance support, but the scheduled job is disabled. The current database has no `live_monitor` rows.
- **Document lane — Not yet implemented:** downloaded-document preservation, document metadata, OCR, and document search are future work. Documents remain a separate evidence lane and are not contract rows.

### Storage and transformation

- **Bulk evidence and observations — Current:** `pipeline/ingest.py` certifies preserved CSVs one source at a time and writes immutable `evidence_objects`, append-only `bulk_observations`, typed parser results, explicit exclusions/projection results, and complete canonical contributor lineage. Raw values, source coordinates, evidence hashes, parser outcomes, and quarantined rows survive canonical projection. Supplemental live-recovery rows still use the compatibility path and do not yet have byte-retained evidence objects.
- **Reviewed normalization — Current:** `pipeline/normalization.py` loads one deterministic, versioned exact-match registry for reviewed schema and contractor aliases. Registry collisions fail closed. Fuzzy or learned candidate scoring is not implemented and cannot affect publication.
- **Canonical records — Current:** `data/db/contratos.db` contains a reproducible `contracts` projection plus FTS5 data. Bulk rows carry separate versioned `canonical_id` and `family_id` values, a deterministic representative observation, and append-only representative/duplicate contributor relations. Supplemental recovery rows remain `recovery_unlinked` until their acquisition lane gains immutable evidence and observations. Rebuilding from the certified source set is order-independent; introducing a new source through the single-source projection path fails closed so caller arrival order cannot become canonical state.
- **Public projection — Current:** `pipeline/build_site_artifacts.py` creates a browser-oriented SQLite projection, gzip/chunks, a manifest, and a full downloadable SQLite artifact. It includes stable canonical/family identities and validated cancellation fields while intentionally excluding evidence, observation, exclusion, projection, and contributor audit tables. The site runs `sql.js` in the browser; it does not query a hosted API.
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

The diagram is both a current bulk flow and a target cross-channel flow. In the current checkout, certified bulk CSVs pass through immutable evidence, append-only observations, reviewed normalization, deterministic reconciliation, and canonical projection. Manual live recovery still joins through a compatibility row path rather than byte-retained evidence and observations. The hosted API and document/OCR lanes do not exist.

## Target boundaries and contracts

### Immutable evidence

**Current for bulk; target across channels:** Every certified bulk file has exact preserved bytes, source metadata, SHA-256 identity, deterministic certification, and a database evidence object. A later parser correction adds observations rather than replacing source evidence. Bounded future capture retains accepted changed bytes content-addressed before explicit promotion.

**Current gap:** there is no unified evidence store spanning raw live JSON, detail HTML, and documents. Those channels must adopt the same immutable-object contract before they become broad automated acquisition lanes.

### Observations

**Current for bulk; target across channels:** Each bulk observation references exactly one evidence object and retains parser/normalizer versions, source coordinates, raw values, field-level extraction/status, warnings, and projection eligibility. Observations and their projection/contributor relations are append-only and can be reprojected without deleting duplicates or quarantines.

**Current gap:** supplemental live recovery still writes compatibility rows and does not preserve every API/HTML response or parser result as a first-class immutable observation.

### Reconciliation and human review

**Target:** Link observations into contract families using deterministic keys where safe, then use confidence-scored entity resolution with a human review queue for uncertain matches. Keep “not found,” “unrecoverable,” “unreviewed,” and “conflict” distinct from “recovered.”

**Current:** Phase 2A recovery uses scripted search/detail parsing and family validation, with a tracked target ledger. The `7,177 recovered` and `4,806 unrecoverable` counts apply only to that defined ledger scope; they do not resolve the broader 31,264-family audit. Human-reviewed confidence scoring is **not yet implemented**.

### Canonical projections and public surfaces

**Current:** the `contracts` projection includes row-level source fields, versioned canonical/family identities for bulk rows, conservative cancellation status, and deterministic representative links, and can be rebuilt from CSV plus recovery inputs. The static browser DB and downloadable DB are projections, not source authority; detailed evidence and lineage remain in the full database.

**Target:** publish separate projections for reported source fields, derived family/current-value fields, lifecycle/status fields, and evidence links. A hosted query API should serve the public UI and bounded exports; full artifact downloads remain useful for reproducibility until the API is mature.

### Documents and RAG

Documents are not contracts. **Target:** preserve document bytes and metadata, extract/OCR text in a document-index lane, and link documents to contract observations without replacing the contract evidence model. **Not yet implemented:** document downloads, OCR, document search, and document-to-contract review.

RAG is an optional downstream consumer of reviewed document text and canonical/evidence links. It is not a source of truth, is not required for Phase 0/1, and must wait until evidence retention, provenance, document search, reconciliation, and access controls are stable.

## Amount and status boundary

The current `amount`/`Cuantía` field is an OCPR-reported contract amount from a source row. It is not actual payment data, spending, a disbursement, or current contract value. Amendment amounts may represent increase/decrease deltas, but sign/encoding still requires source validation. Cancellation is preserved separately as its raw scalar, a validated effective date when present, a closed status, and a compatibility boolean derived only from validated status; blank/NUL evidence remains unknown. A future derived current contract value must be labeled as derived and calculated only after family and amendment rules are validated. Actual payments require a separate evidence source and ledger; no such payment ledger is implemented here.

## Architectural non-goals for this migration

- Do not turn the canonical deduplicated SQLite database into a claim of complete registry coverage.
- Do not infer official bulk publication from a fiscal year that appears only in live/recovered records.
- Do not re-enable scheduled live monitoring before its raw evidence and durable-source behavior survive reset-based rebuilds.
- Do not merge documents, OCR text, or RAG indexes into the `contracts` table.
- Do not perform cleanup of historical repository residue as part of documentation authority migration.
