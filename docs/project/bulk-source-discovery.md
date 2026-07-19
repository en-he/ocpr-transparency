# Bulk source discovery and immutable capture contract

**Status:** Target contract for Milestone A Task 8. The implementation modules named below are intentionally not present in this checkpoint.

This document freezes the boundary for finding official bulk CSV releases and preserving them without replacing evidence. It is deliberately narrower than a crawler: it probes the registry page, the known bulk-download pattern, and a bounded fiscal-year window. It must not turn search, documents, email, or spreadsheet export into an acquisition lane.

## Scope and non-goals

The only acquisition object in this contract is an official fiscal-year CSV. A fiscal year is the closed string `YYYY-YYYY`, where the second year is the first year plus one. Discovery may inspect:

1. the injected registry page and its frequent-search/bulk-year links;
2. one known bulk URL per candidate year, preferring an advertised URL;
3. an explicitly supplied official HTTPS host allowlist and redirect metadata.

Discovery does **not** crawl arbitrary links, probe contract documents, call live search/query endpoints, automate the 30,000-row Excel/email export lane, or use Archive/search results as proof of current official availability. Secondary recovery leads belong to a later, separate workflow.

The implementation must be offline-testable: all network access is through the injected `http_get` callable. Tests never contact the OCPR site.

## `pipeline/discover_bulk_sources.py`

The module exposes these public values and functions.

### Closed status set

```python
DISCOVERY_STATUSES = (
    "listed_available",
    "listed_but_404",
    "unlisted_available",
    "relocated_redirect",
    "unavailable",
    "transient_error",
    "invalid_payload",
)
```

`listed` means that the fiscal year was extracted from the registry page. A stable HTTP 404 is `listed_but_404` for a listed year and `unavailable` for an unlisted candidate. Retryable transport failures or retryable HTTP statuses that exhaust the retry budget are `transient_error`. A 200 response that fails payload checks is `invalid_payload`.

### Registry parsing

```python
@dataclass(frozen=True)
class FiscalYearLink:
    fiscal_year: str
    url: str


def extract_fiscal_year_links(
    html: str,
    *,
    page_url: str,
) -> tuple[FiscalYearLink, ...]: ...
```

The parser resolves relative links against `page_url`, preserves first-seen order, and de-duplicates an exact `(fiscal_year, url)` pair. It extracts only links that identify a fiscal-year CSV through the known bulk-download shape or a `.csv` filename. It ignores search, detail/document, arbitrary export, `mailto:`, and other non-bulk links. URL safety is checked again during probing; parsing alone is not availability proof.

The current fixture uses the existing official bulk endpoint. The moved fixture uses a different path on the official host and is evidence that path changes must be surfaced rather than silently normalized away.

### Bounded candidate window

```python
def candidate_fiscal_years(
    *,
    newest_certified_year: str | None,
    advertised_years: Iterable[str],
    current_fiscal_year: str,
    adjacent_newer_years: int = 1,
) -> tuple[str, ...]: ...
```

The result is a deterministic, de-duplicated tuple sorted by fiscal-year start year. It contains every year in the bounded window from `newest_certified_year` through `current_fiscal_year`, any explicitly advertised years, and exactly the requested number of years immediately newer than the greatest anchor. The certified-to-current interval is capped at 16 years to prevent an accidental broad probe. It never expands to the full historical range. `adjacent_newer_years` is non-negative; a negative value is a programming error.

For example, with newest certified `2021-2022`, current `2024-2025`, no older advertised years, and `adjacent_newer_years=1`, the candidate set is `2021-2022` through `2025-2026` only (plus any explicitly advertised years).

### Probe report and injection contract

```python
@dataclass(frozen=True)
class SourceObservation:
    fiscal_year: str
    status: str
    advertised: bool
    requested_url: str
    final_url: str | None
    redirect_chain: tuple[str, ...]
    http_status: int | None
    content_type: str | None
    content_disposition: str | None
    byte_length: int | None
    sha256: str | None
    eligible: bool
    review_required: bool
    reason: str | None

@dataclass(frozen=True)
class DiscoveryReport:
    registry_url: str
    advertised_links: tuple[FiscalYearLink, ...]
    candidate_years: tuple[str, ...]
    observations: tuple[SourceObservation, ...]


def discover_bulk_sources(
    *,
    registry_url: str,
    newest_certified_year: str | None,
    current_fiscal_year: str,
    http_get: Callable[..., object],
    allowed_hosts: Collection[str],
    bulk_url_template: str,
    adjacent_newer_years: int = 1,
    max_retries: int = 2,
    max_redirects: int = 3,
    backoff_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
    max_bytes: int = 50_000_000,
    timeout_seconds: float = 120.0,
) -> DiscoveryReport: ...
```

`http_get(url, **kwargs)` is the sole transport seam and represents an HTTP **GET**. It receives project-identifying headers, a timeout, `stream=True`, and `allow_redirects=False`. A HEAD request is never used as a substitute for fetching bytes. Redirects are followed manually up to `max_redirects`; every resolved `Location` must pass the HTTPS/exact-host allowlist before another request is made. `max_retries` counts retries after the initial request, so `max_retries=2` permits at most three attempts per URL. Retry backoff calls the injected `sleep` with `backoff_seconds`, then `2 * backoff_seconds`, etc.; stable 404s, redirects, and validation failures are not retried.

The registry URL, every requested bulk URL, and every final redirect URL must use HTTPS and have a hostname in `allowed_hosts`. The allowlist is exact: subdomains and HTTP versions are not inferred. The report retains the requested URL, final URL, and complete redirect chain. Any redirect/path movement is `relocated_redirect`, sets `review_required=True` and `eligible=False`, and stops automatic publication. A redirect ending on a host outside the HTTPS allowlist is also `relocated_redirect` with reason `redirect_host_not_allowlisted`; it is retained for review, never promoted automatically.

The bulk URL selected for a year is its advertised link when present, otherwise `bulk_url_template.format(fiscal_year=year)`. No other link from the page is fetched. The report records one logical observation per candidate year; retries do not create additional observations.

A 200 payload is acceptable to discovery only when the capture validation rules below pass. Discovery may report its byte length and SHA-256, but it does not write an active view or publish anything.

## `pipeline/capture_bulk_snapshot.py`

Capture is a transport-independent persistence boundary. It receives the final response from discovery and always writes its original bytes into the quarantine directory before validation. Invalid input remains available for diagnosis in quarantine; it is never promoted or used to overwrite an active compatibility view.

### Payload validation

```python
@dataclass(frozen=True)
class PayloadValidation:
    valid: bool
    reason: str | None
    sha256: str
    byte_length: int
    encoding: str | None
    truncated: bool = False


def validate_bulk_response(
    response: object,
    *,
    fiscal_year: str,
    max_bytes: int = 50_000_000,
) -> PayloadValidation: ...
```

The response supplies `status_code`, case-insensitive `headers`, `url`, optional `history`, and either `iter_content()` or a bounded in-memory `content` fallback for offline adapters. Production transport is streamed. Validation reads at most `max_bytes + 1`, never requires unbounded response materialization, and is fail-closed and ordered as follows:

1. HTTP status must be 200 (`http_status_not_200` otherwise).
2. The body must be non-empty (`empty_body`) and no larger than `max_bytes` (`byte_limit_exceeded`).
3. `Content-Type` must be a plausible CSV media type (`text/csv`, `application/csv`, or `application/octet-stream`); otherwise `invalid_media_type`.
4. `Content-Disposition` must be an attachment naming `contratos_<fiscal_year>.csv`; otherwise `invalid_content_disposition`.
5. An HTML/sign-in/error body is rejected as `html_body`, even if its headers claim CSV.
6. The known source encoding is Latin-1. UTF-16/UTF-32 BOMs or embedded NUL byte patterns that identify another encoding are `unrecognized_encoding`.
7. The header must match a known/explicitly reviewed bulk schema and contain a nonzero record; an unknown schema is `unknown_schema`.
8. The exact bytes receive a SHA-256. No decoded or normalized representation is substituted for the evidence hash.

All validation failures are represented as `valid=False` with a stable reason. A valid result has `reason=None`, `encoding="latin-1"`, and the exact body length/hash.

### Capture and promotion API

```python
def capture_bulk_snapshot(
    *,
    response: object,
    fiscal_year: str,
    source_url: str,
    quarantine_dir: Path,
    evidence_dir: Path,
    active_view: Path,
    allowed_hosts: Collection[str],
    captured_at: str,
    max_bytes: int = 50_000_000,
    validator: Callable[[Path, str], str | None] | None = None,
) -> CaptureResult: ...


def promote_bulk_snapshot(
    *,
    evidence_path: Path,
    active_view: Path,
) -> Path: ...
```

`captured_at` is an explicit ISO-8601 UTC timestamp supplied by the caller. Preliminary response validation is always followed by the full fail-closed bulk certifier against the already-written quarantine file before evidence acceptance. If `validator` is supplied, it is an additional review seam called afterward with the quarantine path and fiscal year; it returns `None` for acceptance or a non-empty reason for rejection. Tests and future schema reviewers can therefore add checks without bypassing transport, payload, or full certification gates.

`CaptureResult` has these fields:

```python
@dataclass(frozen=True)
class CaptureResult:
    fiscal_year: str
    status: str                 # captured | unchanged | invalid_payload | rejected
    sha256: str
    evidence_path: Path | None
    metadata_path: Path | None
    quarantine_path: Path | None
    reason: str | None
    quarantine_truncated: bool = False
```

Capture streams at most `max_bytes + 1` into quarantine before validation. For an oversized response, quarantine retains only that bounded diagnostic prefix, `quarantine_truncated=True`, and the object is never eligible for evidence promotion; its prefix hash is not represented as a hash of the complete upstream body. For accepted bytes, the evidence object is immutable and content-addressed (its path includes the exact complete-body SHA-256); a same-year byte change creates another object and leaves the prior object and metadata untouched. A same-year identical hash is a no-op: no new object, no active-view write, and no rebuild trigger. Capture itself never writes `active_view`, even when an older active file exists. Promotion is an explicit review action and must atomically make `active_view.read_bytes()` equal `evidence_path.read_bytes()`.

All capture/promote paths are operator-supplied trusted roots, but the implementation still rejects parent traversal and any existing symlink leaf or ancestor. Immutable leaves use exclusive/no-follow opens. Promotion pins and verifies the evidence file by descriptor, writes a temporary file inside a verified destination directory, and atomically replaces the active view only after the source remains stable. `captured_at` must be an offset-aware UTC timestamp with zero UTC offset.

Each accepted evidence object has a sidecar metadata JSON object containing at least `fiscal_year`, `source_url`, `requested_url`, `final_url`, `redirect_chain`, `captured_at`, `sha256`, `byte_length`, `http_status`, `content_type`, `content_disposition`, `encoding`, and the acquisition status. Metadata retains redirect/path movement and source/request context; it is not regenerated by normalizing CSV rows.

An invalid response has no evidence or active-view path. Its quarantine path remains; under-limit failures retain exact bytes, while over-limit failures retain only the explicitly marked bounded prefix. A moved host/path is review-required and cannot reach `promote_bulk_snapshot` until an explicit reviewer selects an evidence object.

## Offline test evidence

`tests/test_bulk_source_discovery.py` exercises the two modules with no live network. The three offline HTML fixtures cover the current registry shape, a moved official path, and an HTML/error payload. The existing Latin-1 `tests/fixtures/bulk/ocpr-bulk-v1.csv` fixture is reused as a valid CSV body. Security regressions cover pre-request redirect allowlisting, bounded streaming without `.content`, truncated quarantine semantics, strict UTC capture time, and symlink-ancestor rejection.
