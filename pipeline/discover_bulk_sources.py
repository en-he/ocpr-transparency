"""Bounded, offline-testable discovery of official bulk CSV sources.

Only the injected ``http_get`` callable performs transport.  The discovery
window is deliberately finite: advertised years plus the supplied certified,
current, and adjacent-year anchors.  Discovery reports what it saw; it never
writes or promotes source bytes.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Callable, Collection, Iterable
from urllib.parse import parse_qs, unquote, urljoin, urlsplit

try:  # Support both package and direct ``pipeline`` imports.
    from .capture_bulk_snapshot import validate_bulk_response
    from .config import HEADERS
except ImportError:  # pragma: no cover - exercised by direct test imports.
    from capture_bulk_snapshot import validate_bulk_response
    from config import HEADERS


DISCOVERY_STATUSES = (
    "listed_available",
    "listed_but_404",
    "unlisted_available",
    "relocated_redirect",
    "unavailable",
    "transient_error",
    "invalid_payload",
)

_FISCAL_YEAR_RE = re.compile(r"^(?P<start>[0-9]{4})-(?P<end>[0-9]{4})$")
_BULK_DOWNLOAD_PATH = "/contract/downloadfrequentsearchfiscalyeardocument"
_CSV_YEAR_RE = re.compile(r"(?<![0-9])([0-9]{4}-[0-9]{4})(?=\.csv$)", re.IGNORECASE)
_RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_MAX_INTERVENING_YEARS = 16


@dataclass(frozen=True)
class FiscalYearLink:
    fiscal_year: str
    url: str


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


class _AnchorParser(HTMLParser):
    """Collect only anchor hrefs; no arbitrary HTML link crawling."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value is not None:
                self.hrefs.append(value)
                break

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        self.handle_starttag(tag, attrs)


def _parse_fiscal_year(value: str) -> int:
    if not isinstance(value, str):
        raise ValueError("fiscal year must be a YYYY-YYYY string")
    match = _FISCAL_YEAR_RE.fullmatch(value)
    if match is None:
        raise ValueError("fiscal year must be a YYYY-YYYY string")
    start = int(match.group("start"))
    end = int(match.group("end"))
    if end != start + 1:
        raise ValueError("fiscal year must contain consecutive years")
    return start


def _format_fiscal_year(start: int) -> str:
    return f"{start:04d}-{start + 1:04d}"


def _year_from_bulk_link(url: str) -> str | None:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return None
    path = unquote(parsed.path or "")
    query = parse_qs(parsed.query, keep_blank_values=True)

    # Current registry endpoint: the fiscal year is the q parameter.  Require
    # the exact known path so search/export/document endpoints remain decoys.
    if path.rstrip("/").lower() == _BULK_DOWNLOAD_PATH:
        values = query.get("q", ())
        if len(values) != 1:
            return None
        candidate = values[0]
        try:
            _parse_fiscal_year(candidate)
        except ValueError:
            return None
        return candidate

    # Moved official paths and future reviewed paths identify the year in a
    # CSV filename.  The host is intentionally not checked here; probing
    # applies the caller's exact allowlist and retains an unapproved lead for
    # review instead of silently dropping it.
    filename = path.rsplit("/", 1)[-1]
    match = _CSV_YEAR_RE.search(filename)
    if match is None:
        return None
    candidate = match.group(1)
    try:
        _parse_fiscal_year(candidate)
    except ValueError:
        return None
    return candidate


def extract_fiscal_year_links(
    html: str,
    *,
    page_url: str,
) -> tuple[FiscalYearLink, ...]:
    """Extract first-seen, de-duplicated fiscal-year bulk links from HTML."""
    if not isinstance(html, str):
        raise TypeError("html must be a string")
    if not isinstance(page_url, str) or not page_url:
        raise ValueError("page_url must be a non-empty URL")

    parser = _AnchorParser()
    parser.feed(html)
    parser.close()

    links: list[FiscalYearLink] = []
    seen: set[tuple[str, str]] = set()
    for href in parser.hrefs:
        href = href.strip()
        if not href:
            continue
        try:
            resolved = urljoin(page_url, href)
        except ValueError:
            continue
        fiscal_year = _year_from_bulk_link(resolved)
        if fiscal_year is None:
            continue
        pair = (fiscal_year, resolved)
        if pair in seen:
            continue
        seen.add(pair)
        links.append(FiscalYearLink(fiscal_year=fiscal_year, url=resolved))
    return tuple(links)


def candidate_fiscal_years(
    *,
    newest_certified_year: str | None,
    advertised_years: Iterable[str],
    current_fiscal_year: str,
    adjacent_newer_years: int = 1,
) -> tuple[str, ...]:
    """Build a sorted bounded union of explicit anchors and newer neighbours."""
    if (
        not isinstance(adjacent_newer_years, int)
        or isinstance(adjacent_newer_years, bool)
        or adjacent_newer_years < 0
    ):
        raise ValueError("adjacent_newer_years must be non-negative")

    anchors: set[str] = set()
    if newest_certified_year is not None:
        newest_start = _parse_fiscal_year(newest_certified_year)
        anchors.add(newest_certified_year)
    else:
        newest_start = None
    current_start = _parse_fiscal_year(current_fiscal_year)
    anchors.add(current_fiscal_year)

    if newest_start is not None:
        distance = current_start - newest_start
        if distance < 0 or distance > _MAX_INTERVENING_YEARS:
            raise ValueError("certified-to-current fiscal-year window is invalid or too large")
        for start_year in range(newest_start, current_start + 1):
            anchors.add(_format_fiscal_year(start_year))

    for advertised_year in advertised_years:
        _parse_fiscal_year(advertised_year)
        anchors.add(advertised_year)

    greatest_anchor = max(_parse_fiscal_year(year) for year in anchors)
    for offset in range(1, adjacent_newer_years + 1):
        anchors.add(_format_fiscal_year(greatest_anchor + offset))

    return tuple(sorted(anchors, key=_parse_fiscal_year))


def _allowed_hosts(allowed_hosts: Collection[str]) -> frozenset[str]:
    try:
        hosts = frozenset(host.lower() for host in allowed_hosts)
    except (AttributeError, TypeError) as exc:
        raise ValueError("allowed_hosts must contain hostnames") from exc
    if not hosts or any(not host or "/" in host or ":" in host for host in hosts):
        raise ValueError("allowed_hosts must contain exact hostnames")
    return hosts


def _url_allowlist_error(url: object, hosts: frozenset[str]) -> str | None:
    if not isinstance(url, str) or not url:
        return "url_missing"
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
    except ValueError:
        return "url_invalid"
    if parsed.scheme.lower() != "https":
        return "url_not_https"
    if not hostname or hostname.lower() not in hosts:
        return "host_not_allowlisted"
    if parsed.username is not None or parsed.password is not None:
        return "url_invalid"
    return None


def _header_value(response: object, name: str) -> str | None:
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    try:
        items = headers.items()
    except AttributeError:
        return None
    wanted = name.lower()
    for key, value in items:
        if str(key).lower() == wanted:
            return None if value is None else str(value)
    return None


def _content(response: object) -> bytes | None:
    value = getattr(response, "content", None)
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return None


def _redirect_chain(response: object, requested_url: str) -> tuple[str, ...]:
    chain: list[str] = [requested_url]
    history = getattr(response, "history", ()) or ()
    try:
        history_items = tuple(history)
    except TypeError:
        history_items = ()

    for hop in history_items:
        hop_url = getattr(hop, "url", None)
        if isinstance(hop_url, str) and hop_url and hop_url != chain[-1]:
            chain.append(hop_url)
        location = _header_value(hop, "Location")
        if location:
            target = urljoin(chain[-1], location)
            if target != chain[-1]:
                chain.append(target)

    final_url = getattr(response, "url", None)
    if not isinstance(final_url, str) or not final_url:
        final_url = requested_url
    if final_url != chain[-1]:
        chain.append(final_url)
    return tuple(chain)


def _response_final_url(response: object, requested_url: str) -> str:
    final_url = getattr(response, "url", None)
    return final_url if isinstance(final_url, str) and final_url else requested_url


def _status_code(response: object) -> int | None:
    value = getattr(response, "status_code", None)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _retryable(status_code: int | None) -> bool:
    return status_code in _RETRYABLE_STATUSES or (
        status_code is not None and 500 <= status_code <= 599
    )


def _request_with_retries(
    *,
    url: str,
    http_get: Callable[..., object],
    max_retries: int,
    backoff_seconds: float,
    sleep: Callable[[float], None],
    timeout_seconds: float,
) -> tuple[object | None, str | None]:
    """Return the last response or a stable transport/retry failure reason."""
    last_response: object | None = None
    for attempt in range(max_retries + 1):
        try:
            response = http_get(
                url,
                method="GET",
                headers=dict(HEADERS),
                timeout=timeout_seconds,
                allow_redirects=False,
                stream=True,
            )
        except Exception:
            if attempt < max_retries:
                sleep(backoff_seconds * (2**attempt))
                continue
            return None, "transport_error"

        last_response = response
        if _retryable(_status_code(response)):
            if attempt < max_retries:
                sleep(backoff_seconds * (2**attempt))
                continue
            return last_response, "retryable_http_status"
        return response, None
    return last_response, "transport_error"


def _request_with_safe_redirects(
    *,
    url: str,
    http_get: Callable[..., object],
    hosts: frozenset[str],
    max_retries: int,
    max_redirects: int,
    backoff_seconds: float,
    sleep: Callable[[float], None],
    timeout_seconds: float,
) -> tuple[object | None, str | None, tuple[str, ...], str | None]:
    """Issue bounded GETs, validating each redirect before contacting it."""
    current_url = url
    chain: list[str] = [url]
    for redirect_count in range(max_redirects + 1):
        response, failure = _request_with_retries(
            url=current_url,
            http_get=http_get,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
            sleep=sleep,
            timeout_seconds=timeout_seconds,
        )
        if response is None or failure is not None:
            return response, failure, tuple(chain), None

        status = _status_code(response)
        if status is not None and 300 <= status <= 399:
            location = _header_value(response, "Location")
            if not location:
                return response, None, tuple(chain), "redirect_location_missing"
            target = urljoin(current_url, location)
            if target != chain[-1]:
                chain.append(target)
            target_error = _url_allowlist_error(target, hosts)
            if target_error is not None:
                return response, None, tuple(chain), "redirect_host_not_allowlisted"
            if redirect_count >= max_redirects:
                return response, None, tuple(chain), "redirect_limit_exceeded"
            current_url = target
            continue

        # Compatibility defense: if a custom transport ignored
        # allow_redirects=False, retain and validate its observed chain.
        observed = _redirect_chain(response, current_url)
        for observed_url in observed[1:]:
            if observed_url != chain[-1]:
                chain.append(observed_url)
        if any(_url_allowlist_error(item, hosts) is not None for item in chain):
            return response, None, tuple(chain), "redirect_host_not_allowlisted"
        return response, None, tuple(chain), None

    return None, "redirect_limit_exceeded", tuple(chain), "redirect_limit_exceeded"


def _observation(
    *,
    fiscal_year: str,
    advertised: bool,
    requested_url: str,
    final_url: str | None = None,
    redirect_chain: tuple[str, ...] = (),
    status: str,
    response: object | None = None,
    byte_length: int | None = None,
    sha256: str | None = None,
    eligible: bool = False,
    review_required: bool = False,
    reason: str | None = None,
) -> SourceObservation:
    http_status = _status_code(response) if response is not None else None
    content_type = _header_value(response, "Content-Type") if response is not None else None
    content_disposition = (
        _header_value(response, "Content-Disposition") if response is not None else None
    )
    return SourceObservation(
        fiscal_year=fiscal_year,
        status=status,
        advertised=advertised,
        requested_url=requested_url,
        final_url=final_url,
        redirect_chain=redirect_chain,
        http_status=http_status,
        content_type=content_type,
        content_disposition=content_disposition,
        byte_length=byte_length,
        sha256=sha256,
        eligible=eligible,
        review_required=review_required,
        reason=reason,
    )


def _registry_html(response: object | None, *, max_bytes: int) -> str:
    if response is None:
        return ""
    limit = max_bytes + 1
    body = bytearray()
    try:
        iterator_factory = getattr(response, "iter_content", None)
        if callable(iterator_factory):
            chunks = iterator_factory(chunk_size=min(65536, max(1, limit)))
        else:
            chunks = (getattr(response, "content", b""),)
        for chunk in chunks:
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                return ""
            remaining = limit - len(body)
            if remaining <= 0:
                break
            body.extend(bytes(chunk[:remaining]))
            if len(chunk) > remaining or len(body) >= limit:
                return ""
    except Exception:
        return ""
    return bytes(body).decode("utf-8", errors="replace")


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
    sleep: Callable[[float], None] = __import__("time").sleep,
    max_bytes: int = 50_000_000,
    timeout_seconds: float = 120.0,
) -> DiscoveryReport:
    """Discover and classify one bounded set of official bulk URLs."""
    hosts = _allowed_hosts(allowed_hosts)
    if _url_allowlist_error(registry_url, hosts) is not None:
        raise ValueError("registry_url must be HTTPS on an allowed host")
    if not callable(http_get):
        raise TypeError("http_get must be callable")
    if (
        not isinstance(max_retries, int)
        or isinstance(max_retries, bool)
        or max_retries < 0
    ):
        raise ValueError("max_retries must be non-negative")
    if (
        not isinstance(max_redirects, int)
        or isinstance(max_redirects, bool)
        or max_redirects < 0
    ):
        raise ValueError("max_redirects must be non-negative")
    if isinstance(backoff_seconds, bool) or backoff_seconds < 0:
        raise ValueError("backoff_seconds must be non-negative")
    if isinstance(timeout_seconds, bool) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 0:
        raise ValueError("max_bytes must be a non-negative integer")

    registry_response, registry_failure, registry_chain, registry_redirect_issue = (
        _request_with_safe_redirects(
            url=registry_url,
            http_get=http_get,
            hosts=hosts,
            max_retries=max_retries,
            max_redirects=max_redirects,
            backoff_seconds=backoff_seconds,
            sleep=sleep,
            timeout_seconds=timeout_seconds,
        )
    )
    advertised_links: tuple[FiscalYearLink, ...] = ()
    if (
        registry_failure is None
        and registry_redirect_issue is None
        and registry_response is not None
        and _status_code(registry_response) == 200
    ):
        advertised_links = extract_fiscal_year_links(
            _registry_html(registry_response, max_bytes=max_bytes),
            page_url=registry_chain[-1],
        )

    advertised_by_year: dict[str, str] = {}
    for link in advertised_links:
        advertised_by_year.setdefault(link.fiscal_year, link.url)

    candidate_years = candidate_fiscal_years(
        newest_certified_year=newest_certified_year,
        advertised_years=(link.fiscal_year for link in advertised_links),
        current_fiscal_year=current_fiscal_year,
        adjacent_newer_years=adjacent_newer_years,
    )

    observations: list[SourceObservation] = []
    for fiscal_year in candidate_years:
        advertised = fiscal_year in advertised_by_year
        requested_url = advertised_by_year.get(fiscal_year)
        if requested_url is None:
            try:
                requested_url = bulk_url_template.format(fiscal_year=fiscal_year)
            except (KeyError, IndexError, ValueError) as exc:
                raise ValueError("bulk_url_template must accept fiscal_year") from exc
        assert requested_url is not None

        requested_error = _url_allowlist_error(requested_url, hosts)
        if requested_error is not None:
            observations.append(
                _observation(
                    fiscal_year=fiscal_year,
                    advertised=advertised,
                    requested_url=requested_url,
                    final_url=None,
                    redirect_chain=(requested_url,),
                    status="relocated_redirect",
                    eligible=False,
                    review_required=True,
                    reason=(
                        "requested_host_not_allowlisted"
                        if requested_error == "host_not_allowlisted"
                        else f"requested_{requested_error}"
                    ),
                )
            )
            continue

        response, request_failure, chain, redirect_issue = _request_with_safe_redirects(
            url=requested_url,
            http_get=http_get,
            hosts=hosts,
            max_retries=max_retries,
            max_redirects=max_redirects,
            backoff_seconds=backoff_seconds,
            sleep=sleep,
            timeout_seconds=timeout_seconds,
        )
        if response is None:
            observations.append(
                _observation(
                    fiscal_year=fiscal_year,
                    advertised=advertised,
                    requested_url=requested_url,
                    final_url=chain[-1] if len(chain) > 1 else None,
                    redirect_chain=chain,
                    status="transient_error",
                    reason=request_failure or "transport_error",
                )
            )
            continue

        final_url = chain[-1]
        moved = len(chain) > 1
        if redirect_issue is not None or moved:
            reason = redirect_issue or "path_moved_pending_review"
            validation = None
            if _status_code(response) == 200:
                try:
                    validation = validate_bulk_response(
                        response, fiscal_year=fiscal_year, max_bytes=max_bytes
                    )
                except Exception:
                    validation = None
            observations.append(
                _observation(
                    fiscal_year=fiscal_year,
                    advertised=advertised,
                    requested_url=requested_url,
                    final_url=final_url,
                    redirect_chain=chain,
                    status="relocated_redirect",
                    response=response,
                    byte_length=(validation.byte_length if validation is not None else None),
                    sha256=(validation.sha256 if validation is not None else None),
                    eligible=False,
                    review_required=True,
                    reason=reason,
                )
            )
            continue

        status_code = _status_code(response)
        if request_failure is not None:
            observations.append(
                _observation(
                    fiscal_year=fiscal_year,
                    advertised=advertised,
                    requested_url=requested_url,
                    final_url=final_url,
                    redirect_chain=chain,
                    status="transient_error",
                    response=response,
                    reason=request_failure,
                )
            )
            continue

        if status_code == 404:
            observations.append(
                _observation(
                    fiscal_year=fiscal_year,
                    advertised=advertised,
                    requested_url=requested_url,
                    final_url=final_url,
                    redirect_chain=chain,
                    status="listed_but_404" if advertised else "unavailable",
                    response=response,
                    reason="http_404",
                )
            )
            continue

        if status_code != 200:
            observations.append(
                _observation(
                    fiscal_year=fiscal_year,
                    advertised=advertised,
                    requested_url=requested_url,
                    final_url=final_url,
                    redirect_chain=chain,
                    status="unavailable",
                    response=response,
                    reason=(
                        f"http_status_{status_code}"
                        if status_code is not None
                        else "invalid_http_status"
                    ),
                )
            )
            continue

        try:
            validation = validate_bulk_response(
                response, fiscal_year=fiscal_year, max_bytes=max_bytes
            )
        except Exception:
            observations.append(
                _observation(
                    fiscal_year=fiscal_year,
                    advertised=advertised,
                    requested_url=requested_url,
                    final_url=final_url,
                    redirect_chain=chain,
                    status="invalid_payload",
                    response=response,
                    reason="validator_error",
                )
            )
            continue

        if not validation.valid:
            observations.append(
                _observation(
                    fiscal_year=fiscal_year,
                    advertised=advertised,
                    requested_url=requested_url,
                    final_url=final_url,
                    redirect_chain=chain,
                    status="invalid_payload",
                    response=response,
                    byte_length=validation.byte_length,
                    sha256=validation.sha256,
                    reason=validation.reason,
                )
            )
        else:
            observations.append(
                _observation(
                    fiscal_year=fiscal_year,
                    advertised=advertised,
                    requested_url=requested_url,
                    final_url=final_url,
                    redirect_chain=chain,
                    status=("listed_available" if advertised else "unlisted_available"),
                    response=response,
                    byte_length=validation.byte_length,
                    sha256=validation.sha256,
                    eligible=True,
                    reason=None,
                )
            )

    return DiscoveryReport(
        registry_url=registry_url,
        advertised_links=advertised_links,
        candidate_years=candidate_years,
        observations=tuple(observations),
    )
