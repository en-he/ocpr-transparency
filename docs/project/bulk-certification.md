# Bulk archive certification contract

**Status:** Current Milestone A release-candidate contract. `pipeline/bulk_manifest.py`, `pipeline/certify_bulk.py`, the fixtures under `tests/fixtures/bulk/`, and `tests/test_bulk_contracts.py` implement and verify the independent parser layer. `pipeline/generate_bulk_certification.py` produces the tracked 13-snapshot manifest and reports, while the bounded discovery/capture and workflow gates prevent uncertified source bytes from entering publication. See [`roadmap.md`](roadmap.md) Phase 1 and [`data-provenance.md`](data-provenance.md) for the surrounding evidence model.

Nothing in this document changes current ingestion behavior (`pipeline/ingest.py`, `pipeline/contract_utils.py`). It defines a separate, additive certification pass over the preserved bulk CSVs.

## Scope and independence

Phase 1 certifies each exact preserved CSV snapshot (`data/raw/contratos_<fiscal-year>.csv`, plus any recovered archive copy) **independently**. Certifying one fiscal year's bytes never depends on another year's bytes being present, correct, or certified. A missing fiscal year is an explicit, named coverage gap (absent from the manifest), never inferred, interpolated, or silently skipped.

## Source channels

Two source channels are tracked and must never be blended into one label:

- `official_bulk` — bytes downloaded directly from the live OCPR bulk-CSV endpoint while it served the file.
- `archive_bulk` — bytes recovered from an Archive.org (or equivalent) capture because the live portal no longer serves that year.

The same underlying bytes certified once as `official_bulk` and once as `archive_bulk` (e.g., a re-verification against an archive capture of a file originally captured live) produce two distinct certification identities: distinct `source_channel`, distinct `report_hash`. Byte identity does not collapse channel identity.

## Evidence fields captured per certified file

Every successful file-level certification produces a report with, at minimum:

| Field | Meaning |
|---|---|
| `fiscal_year` | The fiscal year the file claims to cover (e.g. `2013-2014`), supplied by the caller — never guessed from row contents. |
| `source_channel` | `official_bulk` or `archive_bulk`. |
| `source_url` | Live OCPR endpoint/query, when known. |
| `archive_url` | Archive.org (or equivalent) capture URL, when known. |
| `capture_time` | The UTC capture/retrieval timestamp, or `None`. |
| `capture_time_status` | One of `observed` (measured at the moment of retrieval), `git_first_seen` (derived from repository history — the commit that first introduced the file, **not** the retrieval time), or `unknown` (no reliable timestamp exists). |
| `sha256` | SHA-256 of the exact preserved bytes. |
| `byte_length` | Exact byte length of the preserved file. |
| `encoding` | The encoding used to decode the header/rows for certification (e.g. `latin-1`). |
| `http_status`, `content_type` | HTTP/media metadata, when known; `None` when not captured. |
| `header_profile` | `v1`, `v2`, or `v3` (see below). File-level failures raise before a successful report exists and are recorded separately by the manifest/report wrapper. |
| `header_fingerprint` | A deterministic fingerprint (hash) of the exact raw header row, order-sensitive. |
| `parser_version`, `normalizer_version` | Versions of the code that produced this report, so a later parser fix is traceable. |
| `rows_total`, `rows_certified`, `rows_quarantined` | Row-level outcome counts. |
| `duplicate_count` | Rows that are exact duplicates of an earlier row in the same file. |
| `source_unique_contribution_count` | Certified source records unique by exact record bytes within this snapshot. This is not a canonical-projection contribution. |
| `quarantine_reason_counts` | Deterministic counts by row-level quarantine reason. |
| `verdict` | `certified` (all rows certified) or `certified_with_quarantine` (file-level checks passed, but at least one row was quarantined). File-level `failed` is an outer manifest/attempt state. |
| `report_hash` | A deterministic hash over the logical fields above (see "Determinism"). |
| `certified_at` | Wall-clock time this certification run executed. Excluded from `report_hash`. |

`capture_time` must never be invented. If the only available signal is when the file first appeared in Git history, that is recorded as `git_first_seen` — it is evidence about repository history, not about when OCPR served the bytes. When no timestamp signal exists at all, `capture_time` stays `None` and `capture_time_status` stays `unknown`. The parser rejects incoherent pairs: `unknown` requires a null time, while `observed` and `git_first_seen` require an offset-aware ISO-8601 value.

## Header profiles

Three header profiles are known from the preserved corpus:

| Profile | Fiscal years | Contract-number header | Has `Cuantía a Recibir`? |
|---|---|---|---|
| `v1` | `2010-2011`, `2011-2012` | `Número de Contrato` | No |
| `v2` | `2012-2013` | `Núm. Contrato` | No |
| `v3` | `2013-2014` onward | `Núm. Contrato` | Yes |

Exact raw header line (byte-exact, order-sensitive, byte-exact including capitalization — the source has **no space after the comma delimiter**; do not insert one when transcribing or fingerprinting):

- **v1:** `Número de Entidad,Entidad,Número de Contrato,Enmienda,Otorgado En,Vigencia Desde,Vigencia Hasta,Tipo de Servicio,Categoría de Servicio,Cancelado,Cuantía,Contratista`
- **v2:** `Número de Entidad,Entidad,Núm. Contrato,Enmienda,Otorgado en,Vigencia Desde,Vigencia Hasta,Tipo de Servicio,Categoría de Servicio,Cancelado,Cuantía,Contratista`
- **v3:** `Número de Entidad,Entidad,Núm. Contrato,Enmienda,Otorgado en,Vigencia Desde,Vigencia Hasta,Tipo de Servicio,Categoría de Servicio,Cancelado,Cuantía,Cuantía a Recibir,Contratista`

Note `v1` uses `Otorgado En` (capital "En") while `v2`/`v3` use `Otorgado en` (lowercase "en") — an observed byte-level difference, not a typo to normalize away at the header-fingerprint layer.

Parsed header tuple (each field trimmed and split on the delimiter — shown here comma-space-joined purely for human readability; this rendering convention is not part of the source bytes and must never be used to build `header_fingerprint`):

- **v1:** `("Número de Entidad", "Entidad", "Número de Contrato", "Enmienda", "Otorgado En", "Vigencia Desde", "Vigencia Hasta", "Tipo de Servicio", "Categoría de Servicio", "Cancelado", "Cuantía", "Contratista")`
- **v2:** `("Número de Entidad", "Entidad", "Núm. Contrato", "Enmienda", "Otorgado en", "Vigencia Desde", "Vigencia Hasta", "Tipo de Servicio", "Categoría de Servicio", "Cancelado", "Cuantía", "Contratista")`
- **v3:** `("Número de Entidad", "Entidad", "Núm. Contrato", "Enmienda", "Otorgado en", "Vigencia Desde", "Vigencia Hasta", "Tipo de Servicio", "Categoría de Servicio", "Cancelado", "Cuantía", "Cuantía a Recibir", "Contratista")`

### Header resolution algorithm

1. Read the raw header row as an ordered list of trimmed strings, in the file's declared encoding.
2. If any header string (case-sensitive, post-trim) appears more than once, fail closed with a duplicate-header error naming the duplicated string(s). This check runs before profile matching.
3. Compare the header set (order-independent) against each of `v1`, `v2`, `v3`:
   - **Exact set match** to one profile → that profile is selected; certification proceeds.
   - **Strict subset** of a known profile's header set (all present headers belong to that profile, but at least one of that profile's headers is missing, and no foreign/unrecognized header is present) → fail closed with a missing-required-header error naming the missing header(s) and the nearest candidate profile.
   - **Anything else** (contains a header string that is not part of the union of all three known profiles, or otherwise does not resolve unambiguously to one candidate profile) → fail closed with an unknown-header-profile error. An unrecognized schema is an explicit failure state, never a best-effort guess.

## Required vs. compatibility fields

The CSV header row only ever carries the columns listed under the three profiles above. In particular, `procurement_method`, `fund_type`, `pco_number`, and `document_url` (canonical columns in `pipeline/contract_utils.py`) are **never** present as bulk CSV headers in any known profile. They are nullable compatibility fields populated (if ever) by a later, non-bulk source — their absence from a bulk file is normal and must never be reported as a missing-required-header failure.

## Cancelado raw value preservation

The `Cancelado` column's raw byte value must be preserved verbatim in certification output. Observed raw shapes in the preserved corpus:

- A single NUL byte (`\x00`) — the source's marker for "not cancelled." This is the same blank marker the source uses for other empty fields (e.g. an original, unamended `Enmienda`); it is not an empty string.
- A `MM-DD-YYYY` date string — the date the contract was cancelled.

Certification must retain the exact raw string for `Cancelado` (and surface it in row-level output) rather than collapsing it into a derived boolean. Interpreting cancellation lifecycle semantics is out of scope for Phase 1 certification (see `data-provenance.md`'s amount/amendment/status semantics section).

## Date field parsing (per profile)

Corpus-wide evidence across all 13 preserved fiscal-year files (1,232,110 rows) establishes the date parsing convention for every known header profile (`v1`, `v2`, `v3`) — this is a per-profile, corpus-established fact, not a row-by-row inference:

- Every known profile uses `MM-DD-YYYY` with a **four-digit year**, consistently. This is proven, not assumed: the corpus contains a large number of date values whose second numeric component (the day) is greater than 12, which is only possible if the first component is the month. That rules out `DD-MM-YYYY` for these profiles as a whole.
- Because the convention is resolved per profile from corpus-wide evidence, a four-digit-year value whose month and day components are **both ≤ 12** (e.g. `05-06-2014`) is **not ambiguous**. It resolves unambiguously to May 6, 2014 under the certified profile convention — the same way every other date in that profile is resolved. Treating such values as ambiguous and quarantining them would wrongly reject a large share of otherwise valid records.
- Certification must never re-derive day/month order per row or per file, and must never fall back to a different order "just for this value." It applies the profile's known, corpus-established convention uniformly to every well-formed four-digit-year date in that profile.
- This certainty does not extend past what the corpus actually establishes: it covers four-digit-year `MM-DD-YYYY` values in a known profile. A value that does not fit that shape (see `ambiguous_date` below) is not covered by this evidence and is not silently forced into it.

## Row-level fail-closed behavior

File-level empty/header, duplicate, missing-required, unknown-profile, malformed-CSV, or unsupported quoted-multiline problems raise a named fail-closed exception before a successful report exists. The manifest/report wrapper records a separate `failed` attempt with the error class/details and no active profile. Row-level problems do not fail the whole file; instead each affected row is quarantined with an explicit reason, and the file's successful-report verdict becomes `certified_with_quarantine`:

- **`shifted_row`** — the row's raw field count does not match the header's column count (e.g. an unquoted embedded delimiter shifted subsequent fields, or a row is missing trailing fields). The row is quarantined with its raw fields preserved; it is never silently re-aligned or truncated to fit.
- **`ambiguous_date`** — a date field (`Otorgado en`/`Otorgado En`, `Vigencia Desde`, `Vigencia Hasta`, or a dated `Cancelado`) does not resolve to a single interpretation under the certified per-profile `MM-DD-YYYY` four-digit-year convention described above, and no profile-level fact resolves it. The known genuine case is a **two-digit year** (e.g. `05-06-14`): the century is not determinable from the value or from the profile, so the value admits multiple plausible interpretations. The row is quarantined rather than silently choosing a century or a different day/month order. A four-digit-year value with both month and day components ≤ 12 is explicitly **not** this case — see "Date field parsing" above.
- **`malformed_amount`** (and similarly named reasons for other malformed scalar fields) — a numeric/date field contains content that is neither a recognized blank marker (NUL) nor a parseable value. The row is quarantined with the raw value preserved; it is never silently coerced to `NULL`/`0` without a flag.

A quarantined row is retained (raw fields preserved, reason recorded) — never dropped and never allowed to silently become a "trustworthy" null in the certified output.

`row_number` is the source CSV record number including the header as record 1, so the first data record is 2. `raw_fields` are the CSV-decoded source fields before canonical normalization. `raw_record` and `raw_record_sha256` preserve the exact decoded physical source record (without its line terminator), including OCPR's nonstandard doubled outer wrappers. The exact file bytes and file SHA-256 remain the evidence authority.

The parser accepts CRLF and LF record separators. The current corpus contains no valid quoted field spanning physical records; such a future shape fails closed with `UnsupportedMultilineRecordError` rather than being silently fragmented. OCPR's v3 `""value""` outer-wrapper convention is decoded only at complete field boundaries for field validation while the untouched record remains retained.

## Determinism

`report_hash` covers every stable logical report field: fiscal year, source channel/URLs, capture value/status, HTTP/media metadata, encoding, exact file identity, header profile/fingerprint, parser/normalizer versions, row/outcome/duplicate/source-unique counts, quarantine-reason counts, and verdict. It explicitly **excludes** `certified_at`, local filesystem paths, the hash itself, and bulky row-outcome details. Re-running certification against unchanged bytes and unchanged caller-supplied metadata produces an identical hash regardless of execution time.

## Fixture manifest

| File | Simulates | Exercises |
|---|---|---|
| `tests/fixtures/bulk/ocpr-bulk-v1.csv` | `v1` profile (2010-2011/2011-2012 shape) | Profile recognition; no `Cuantía a Recibir`; NUL blank marker. |
| `tests/fixtures/bulk/ocpr-bulk-v2.csv` | `v2` profile (2012-2013 shape) | Profile recognition; no `Cuantía a Recibir`. |
| `tests/fixtures/bulk/ocpr-bulk-v3.csv` | `v3` profile (2013-2014-onward shape, incl. the source's doubled-quote (`""text""`) text-field wrapping) | Profile recognition; presence of `Cuantía a Recibir`; row `1994-000333`'s original row has `Otorgado en`/`Vigencia Desde`/`Vigencia Hasta` all four-digit-year and certified-valid under the profile's `MM-DD-YYYY` convention (not merely because day > 12 — see "Date field parsing"); its amendment `A` row carries exactly one genuinely unsupported/ambiguous date field, `Otorgado en` (`05-06-14` — a two-digit year, century unresolvable by profile), while its `Vigencia Desde` (`05-20-2014`) and `Vigencia Hasta` (`01-31-2015`) remain four-digit-year certified-valid — for ambiguous-date classification. |
| `tests/fixtures/bulk/cancellation-values.csv` | `v1`-shaped header, two rows sharing contract `1995-000444` | Cancelado raw preservation: row amendment blank (NUL) vs. amendment `A` with a dated cancellation (`09-30-2011`). |
| `tests/fixtures/bulk/malformed-shifted-row.csv` | `v3`-shaped header, three rows sharing contract `1996-000555` | Row 1 (amendment blank) is well-formed; row 2 (amendment `B`) is shifted long (14 raw fields, from an unquoted embedded comma in the contractor name); row 3 (amendment `C`) is shifted short (12 raw fields, missing the trailing contractor column). Both malformed rows must be quarantined, not silently realigned. |
| `tests/fixtures/bulk/duplicate-header.csv` | `v1`-shaped header with `Entidad` repeated | File-level `DuplicateHeaderError`, checked before profile matching. |
| `tests/fixtures/bulk/missing-required-header.csv` | `v1`-shaped header with `Contratista` omitted | File-level `MissingRequiredHeaderError` (strict subset of `v1`). |
| `tests/fixtures/bulk/unknown-header.csv` | `v1`-shaped header plus a foreign `Nota Adicional` column | File-level `UnknownHeaderProfileError`. |

All fixtures use synthetic entity/contractor names and contract numbers; none reproduce real preserved-corpus rows.

## Implemented API surface (Milestone A)

This surface is exercised by `tests/test_bulk_contracts.py`.

- `pipeline/bulk_manifest.py`
  - `SOURCE_CHANNELS`, `CAPTURE_TIME_STATUSES` — the allowed literal values above.
  - `HEADER_PROFILES` — mapping of `v1`/`v2`/`v3` to fiscal years and exact header tuples.
  - `COMPATIBILITY_NULLABLE_FIELDS` — `procurement_method`, `fund_type`, `pco_number`, `document_url`.
  - `detect_header_profile(headers) -> str | None`
  - `header_fingerprint(headers) -> str`
- `pipeline/certify_bulk.py`
  - `EmptyBulkFileError`, `UnsupportedMultilineRecordError`, `MissingRequiredHeaderError`, `DuplicateHeaderError`, `UnknownHeaderProfileError` — file-level fail-closed exceptions.
  - `RowOutcome` — per-row `row_number`, `status` (`certified`/`quarantined`), `reason`, parsed `raw_fields`, exact `raw_record`, and `raw_record_sha256`.
  - `BulkCertificationReport` — the evidence fields table above, plus `row_outcomes`.
  - `certify_bulk_file(path, *, source_channel, fiscal_year=None, source_url=None, archive_url=None, capture_time=None, capture_time_status="unknown", http_status=None, content_type=None) -> BulkCertificationReport`
  - `report_hash(report) -> str`

## Current certified corpus results

The tracked manifest at `data/certification/bulk-manifest.json` and 13 per-year reports under `data/certification/reports/` are generated by `pipeline/generate_bulk_certification.py`. They certify the exact preserved bytes independently and currently reconcile:

- 13 preserved snapshots and 1,232,110 source rows;
- 1,231,603 structurally certified rows;
- 507 retained `shifted_row` quarantines, each with source record number, parsed raw fields, and exact raw-record SHA-256;
- 521 exact duplicate source records within individual snapshots;
- 602 separately classified current canonical `row_hash` exclusions, each linked to the excluded source record and first-seen source record;
- 1,231,508 current canonical bulk rows;
- `2010-2011` and `2011-2012` labeled `archive_bulk`, never relabeled as live official downloads; their recorded Wayback `id_` captures were independently fetched and are byte-identical to the preserved files;
- `2023-2024` recorded as unavailable with no invented byte metadata.

The three counts—507 parser quarantines, 521 exact source duplicates, and 602 current canonical exclusions—measure different things and must not be combined. The canonical exclusions reproduce the existing projection's deliberately limited 16-hex `row_hash`; they document current behavior rather than endorsing that hash as a future observation or identity key.

Regenerate or verify without network access:

```sh
.venv/bin/python pipeline/generate_bulk_certification.py
.venv/bin/python pipeline/generate_bulk_certification.py --check
```

The tracked representation excludes the run-time `certified_at` value and all local filesystem paths, so unchanged source bytes, Git evidence, and parser code produce byte-identical artifacts.

## Outside Milestone A

- Reconciliation into a future append-only observation/lineage schema remains a later data-contract migration; Milestone A certifies and documents the current canonical projection without migrating it.
- The intermediate Audit Workspace and review-trained normalization workflow remain separate post-Phase-1 releases.
