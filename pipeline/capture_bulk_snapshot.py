"""Validation and immutable capture for official OCPR bulk CSV responses.

The module deliberately has no transport dependency.  Callers provide a
requests-like response object and, when needed, a small path-based validator.
The response bytes are copied to quarantine before payload validation so that a
failed or hostile response remains available for diagnosis without ever
changing the active compatibility view.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Collection, Mapping
from urllib.parse import urlsplit

try:  # Support both ``import pipeline.foo`` and the test suite's path import.
    from .bulk_manifest import detect_header_profile
    from .certify_bulk import BulkCertificationError, certify_bulk_file
except ImportError:  # pragma: no cover - direct path import in tests
    from bulk_manifest import detect_header_profile
    from certify_bulk import BulkCertificationError, certify_bulk_file


ENCODING = "latin-1"
_ALLOWED_MEDIA_TYPES = {"text/csv", "application/csv", "application/octet-stream"}
_FISCAL_YEAR_RE = re.compile(r"^(?P<start>[0-9]{4})-(?P<end>[0-9]{4})$")
_EXPECTED_FILENAME_RE = re.compile(r"^contratos_[0-9]{4}-[0-9]{4}\.csv$")


@dataclass(frozen=True)
class PayloadValidation:
    valid: bool
    reason: str | None
    sha256: str
    byte_length: int
    encoding: str | None
    truncated: bool = False


@dataclass(frozen=True)
class CaptureResult:
    fiscal_year: str
    status: str
    sha256: str
    evidence_path: Path | None
    metadata_path: Path | None
    quarantine_path: Path | None
    reason: str | None
    quarantine_truncated: bool = False


@dataclass(frozen=True)
class _BoundedBody:
    body: bytes
    sha256: str
    byte_length: int
    truncated: bool
    error: str | None = None


@dataclass(frozen=True)
class _SpooledBody:
    sha256: str
    byte_length: int
    truncated: bool
    temporary_path: Path
    error: str | None = None


class _ResponseBodyView:
    """Expose bounded on-disk bytes without touching a streaming response."""

    def __init__(self, response: object, body: bytes) -> None:
        self._response = response
        self.content = body

    def __getattr__(self, name: str) -> object:
        return getattr(self._response, name)

    def iter_content(self, chunk_size: int = 65536):
        size = max(1, int(chunk_size or 65536))
        for offset in range(0, len(self.content), size):
            yield self.content[offset : offset + size]


def _validate_fiscal_year(fiscal_year: str) -> tuple[int, int]:
    if not isinstance(fiscal_year, str):
        raise ValueError("fiscal_year must be a YYYY-YYYY string")
    match = _FISCAL_YEAR_RE.fullmatch(fiscal_year)
    if match is None:
        raise ValueError("fiscal_year must be a YYYY-YYYY string")
    start = int(match.group("start"))
    end = int(match.group("end"))
    if end != start + 1:
        raise ValueError("fiscal_year must contain consecutive years")
    return start, end


def _host_allowlist(allowed_hosts: Collection[str]) -> frozenset[str]:
    try:
        hosts = frozenset(host.lower() for host in allowed_hosts)
    except (AttributeError, TypeError) as exc:
        raise ValueError("allowed_hosts must contain hostnames") from exc
    if not hosts or any(not host or "/" in host or ":" in host for host in hosts):
        raise ValueError("allowed_hosts must contain exact hostnames")
    return hosts


def _url_error(url: object, hosts: frozenset[str], *, label: str) -> str | None:
    if not isinstance(url, str) or not url:
        return f"{label}_url_missing"
    try:
        parsed = urlsplit(url)
        hostname = parsed.hostname
    except ValueError:
        return f"{label}_url_invalid"
    if parsed.scheme.lower() != "https":
        return f"{label}_url_not_https"
    if not hostname or hostname.lower() not in hosts:
        return f"{label}_host_not_allowlisted"
    # Userinfo can make a displayed URL deceptively resemble an allowlisted
    # host.  It is not part of the official source URL contract.
    if parsed.username is not None or parsed.password is not None:
        return f"{label}_url_invalid"
    return None


def _bounded_chunk(chunk: object, limit: int) -> tuple[bytes, bool]:
    if isinstance(chunk, bytes):
        length = len(chunk)
        return chunk[:limit], length > limit
    if isinstance(chunk, bytearray):
        length = len(chunk)
        return bytes(chunk[:limit]), length > limit
    if isinstance(chunk, memoryview):
        length = len(chunk)
        return chunk[:limit].tobytes(), length > limit
    raise TypeError("response body chunks must be bytes-like")


def _read_response_bounded(response: object, max_bytes: int) -> _BoundedBody:
    """Read at most ``max_bytes + 1`` response bytes without materializing more."""
    limit = max_bytes + 1
    body = bytearray()
    error: str | None = None
    try:
        iterator_factory = getattr(response, "iter_content", None)
        if callable(iterator_factory):
            iterator = iterator_factory(chunk_size=min(65536, max(1, limit)))
            for chunk in iterator:
                remaining = limit - len(body)
                if remaining <= 0:
                    break
                piece, chunk_exceeded = _bounded_chunk(chunk, remaining)
                body.extend(piece)
                if chunk_exceeded or len(body) >= limit:
                    break
        else:
            raw = getattr(response, "content")
            piece, _chunk_exceeded = _bounded_chunk(raw, limit)
            body.extend(piece)
    except Exception:
        error = "body_read_error"
    bounded = bytes(body)
    return _BoundedBody(
        body=bounded,
        sha256=hashlib.sha256(bounded).hexdigest(),
        byte_length=len(bounded),
        truncated=len(bounded) > max_bytes,
        error=error,
    )


def _header_items(headers: object) -> list[tuple[str, object]]:
    if headers is None:
        return []
    if isinstance(headers, Mapping):
        return [(str(key), value) for key, value in headers.items()]
    items = getattr(headers, "items", None)
    if callable(items):
        return [(str(key), value) for key, value in items()]
    return []


def _header_value(headers: object, wanted: str) -> str | None:
    wanted_lower = wanted.lower()
    for name, value in _header_items(headers):
        if name.lower() == wanted_lower:
            if value is None:
                return None
            return str(value)
    return None


def _response_url_chain(response: object, requested_url: str) -> tuple[str, ...]:
    """Return the requested URL, history URLs, and final URL without dupes."""
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
        # A few lightweight HTTP fakes expose only Location on history
        # objects.  Retain that hop when it can be resolved safely.
        location = _header_value(getattr(hop, "headers", None), "Location")
        if location:
            base = chain[-1]
            try:
                from urllib.parse import urljoin

                target = urljoin(base, location)
            except TypeError:
                target = None
            if target and target != chain[-1]:
                chain.append(target)

    final_url = getattr(response, "url", None)
    if not isinstance(final_url, str) or not final_url:
        final_url = requested_url
    if final_url != chain[-1]:
        chain.append(final_url)
    return tuple(chain)


def _redirect_reason(
    chain: tuple[str, ...],
    *,
    requested_url: str,
    allowed_hosts: frozenset[str],
) -> str | None:
    for url in chain:
        error = _url_error(url, allowed_hosts, label="redirect")
        if error is not None:
            # The public contract uses this single reason for an unapproved
            # redirect endpoint, regardless of which hop was external.
            return "redirect_host_not_allowlisted"
    if len(chain) > 1 or chain[-1] != requested_url:
        return "path_moved_pending_review"
    return None


def _looks_like_html_or_error(content: bytes) -> bool:
    sample = content[:8192].decode(ENCODING, errors="ignore")
    stripped = sample.lstrip(" \t\r\n\ufeff").lower()
    if stripped.startswith("<!doctype html") or stripped.startswith("<html"):
        return True
    if re.match(r"^<(?:head|body|title|form|script|meta)\b", stripped):
        return True
    # Some gateways return a plain-text sign-in/error page.  Keep this
    # deliberately narrow so a CSV value merely containing the word "error"
    # is not rejected as HTML.
    if re.match(
        r"^(?:access\s+denied|please\s+sign\s+in|sign\s+in\s+to\s+continue|"
        r"unauthorized|forbidden|internal\s+server\s+error)\b",
        stripped,
    ):
        return True
    return False


def _looks_like_non_latin_encoding(content: bytes) -> bool:
    if content.startswith((b"\xff\xfe", b"\xfe\xff", b"\xff\xfe\x00\x00", b"\x00\x00\xfe\xff")):
        return True

    # A Latin-1 source may contain isolated NUL marker fields.  Reject only
    # repeated UTF-16/UTF-32-shaped NUL patterns, not a single source marker.
    sample = content[:16384]
    if re.search(rb"(?:[\x09-\x7e]\x00){2,}", sample):
        return True
    if re.search(rb"(?:\x00[\x09-\x7e]){2,}", sample):
        return True
    if re.search(rb"(?:[\x09-\x7e]\x00\x00\x00){2,}", sample):
        return True
    if re.search(rb"(?:\x00\x00\x00[\x09-\x7e]){2,}", sample):
        return True
    return False


def _schema_and_record_ok(content: bytes) -> tuple[bool, str]:
    try:
        text = content.decode(ENCODING)
        header_text, separator, data_text = text.partition("\n")
        if not separator:
            return False, "empty_record"
        header = next(csv.reader([header_text.rstrip("\r")], strict=True))
    except (UnicodeError, csv.Error, StopIteration):
        return False, "unknown_schema"

    if detect_header_profile(header) is None:
        return False, "unknown_schema"

    # This is a preliminary response-shape check. OCPR's recent exports use a
    # nonstandard doubled outer quote wrapper, so the first record is read
    # permissively here; the full fail-closed certifier runs against the
    # quarantined file before an evidence object can be accepted.
    try:
        first_record = next(csv.reader(io.StringIO(data_text, newline=""), strict=False))
    except StopIteration:
        return False, "empty_record"
    except csv.Error:
        return False, "unknown_schema"

    # A record containing a source NUL marker is still a real source record;
    # only a completely blank CSV row is treated as zero-record content.
    if not first_record or not any(field != "" for field in first_record):
        return False, "empty_record"
    return True, ""


def validate_bulk_response(
    response: object,
    *,
    fiscal_year: str,
    max_bytes: int = 50_000_000,
) -> PayloadValidation:
    """Validate a complete bulk response in the frozen, fail-closed order."""
    _validate_fiscal_year(fiscal_year)
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 0:
        raise ValueError("max_bytes must be a non-negative integer")

    bounded = _read_response_bounded(response, max_bytes)
    content = bounded.body
    digest = bounded.sha256
    length = bounded.byte_length

    status_code = getattr(response, "status_code", None)
    if status_code != 200:
        return PayloadValidation(
            False, "http_status_not_200", digest, length, None, bounded.truncated
        )
    if length == 0:
        return PayloadValidation(False, "empty_body", digest, length, None, bounded.truncated)
    if bounded.error is not None:
        return PayloadValidation(
            False, bounded.error, digest, length, None, bounded.truncated
        )
    if bounded.truncated:
        return PayloadValidation(
            False, "byte_limit_exceeded", digest, length, None, True
        )

    headers = getattr(response, "headers", None)
    content_type = _header_value(headers, "Content-Type")
    media_type = content_type.split(";", 1)[0].strip().lower() if content_type else ""
    if media_type not in _ALLOWED_MEDIA_TYPES:
        return PayloadValidation(
            False, "invalid_media_type", digest, length, None, bounded.truncated
        )

    disposition = _header_value(headers, "Content-Disposition")
    if not _valid_content_disposition(disposition, fiscal_year):
        return PayloadValidation(
            False,
            "invalid_content_disposition",
            digest,
            length,
            None,
            bounded.truncated,
        )

    if _looks_like_html_or_error(content):
        return PayloadValidation(False, "html_body", digest, length, None, bounded.truncated)
    if _looks_like_non_latin_encoding(content):
        return PayloadValidation(
            False, "unrecognized_encoding", digest, length, None, bounded.truncated
        )

    schema_ok, schema_reason = _schema_and_record_ok(content)
    if not schema_ok:
        return PayloadValidation(
            False, schema_reason, digest, length, None, bounded.truncated
        )

    return PayloadValidation(True, None, digest, length, ENCODING, False)


def _valid_content_disposition(value: str | None, fiscal_year: str) -> bool:
    if not value:
        return False
    if re.match(r"^\s*attachment\s*(?:;|$)", value, flags=re.IGNORECASE) is None:
        return False

    expected = f"contratos_{fiscal_year}.csv"
    # The portal uses filename=, but accept the usual quoted and unquoted
    # forms while requiring the complete exact safe filename.
    match = re.search(
        r"(?:^|;)\s*filename\s*=\s*(?:\"([^\"]*)\"|([^;\s]*))",
        value,
        flags=re.IGNORECASE,
    )
    if match is None:
        # RFC 5987 filename* is harmless only when it resolves to the exact
        # expected basename; reject all other forms rather than guessing.
        match_star = re.search(
            r"(?:^|;)\s*filename\*\s*=\s*(?:UTF-8''|)([^;\s]+)",
            value,
            flags=re.IGNORECASE,
        )
        if match_star is None:
            return False
        from urllib.parse import unquote

        filename = unquote(match_star.group(1))
    else:
        filename = match.group(1) if match.group(1) is not None else match.group(2)
    return filename == expected and _EXPECTED_FILENAME_RE.fullmatch(filename) is not None


def _reject_parent_components(path: Path) -> None:
    if ".." in path.parts:
        raise ValueError(f"path traversal is not allowed: {path}")


def _reject_symlink_ancestors(path: Path) -> None:
    """Reject symlink components before any mkdir/open/replace operation."""
    path = Path(path)
    _reject_parent_components(path)
    absolute = path if path.is_absolute() else Path.cwd() / path
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        if current.is_symlink():
            raise ValueError(f"refusing symlink path ancestor: {current}")


def _ensure_directory(path: Path) -> None:
    path = Path(path)
    _reject_symlink_ancestors(path)
    if path.exists() and (path.is_symlink() or not path.is_dir()):
        raise ValueError(f"directory path is not a regular directory: {path}")
    path.mkdir(parents=True, exist_ok=True)
    _reject_symlink_ancestors(path)
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"directory path is not a regular directory: {path}")


_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)


def _write_all(handle: object, content: bytes) -> None:
    fd = handle if isinstance(handle, int) else handle.fileno()
    offset = 0
    view = memoryview(content)
    while offset < len(view):
        written = os.write(fd, view[offset:])
        if written <= 0:
            raise OSError("short file write")
        offset += written


def _open_regular(path: Path, *, writable: bool = False) -> tuple[int, os.stat_result]:
    flags = (os.O_RDWR if writable else os.O_RDONLY) | _O_NOFOLLOW
    try:
        fd = os.open(os.fspath(path), flags)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"path is not an existing regular file: {path}"
        ) from exc
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"path is not a regular file: {path}")
        return fd, metadata
    except Exception:
        os.close(fd)
        raise


def _read_fd(fd: int, *, max_bytes: int | None = None) -> bytes:
    pieces: list[bytes] = []
    total = 0
    while True:
        chunk_size = 65536
        if max_bytes is not None:
            remaining = max_bytes - total
            if remaining <= 0:
                break
            chunk_size = min(chunk_size, remaining)
        chunk = os.read(fd, chunk_size)
        if not chunk:
            break
        pieces.append(chunk)
        total += len(chunk)
    return b"".join(pieces)


def _read_regular_file(path: Path, *, max_bytes: int | None = None) -> bytes:
    _reject_symlink_ancestors(path)
    fd, metadata = _open_regular(path)
    try:
        if max_bytes is not None and metadata.st_size > max_bytes:
            raise ValueError(f"file exceeds bounded read: {path}")
        return _read_fd(fd, max_bytes=max_bytes)
    finally:
        os.close(fd)


def _file_matches(path: Path, expected: bytes) -> bool:
    try:
        actual = _read_regular_file(path, max_bytes=len(expected) + 1)
    except (OSError, ValueError):
        return False
    return actual == expected


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(os.fspath(path), os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _write_once(path: Path, content: bytes) -> None:
    """Create a regular file with no-follow/exclusive leaf semantics."""
    path = Path(path)
    _reject_symlink_ancestors(path)
    parent = path.parent
    _ensure_directory(parent)
    try:
        fd = os.open(
            os.fspath(path),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW,
            0o600,
        )
    except FileExistsError as exc:
        if path.is_symlink() or not _file_matches(path, content):
            raise ValueError(f"immutable file content conflict: {path}") from exc
        return
    try:
        _write_all(fd, content)
        os.fsync(fd)
    finally:
        os.close(fd)
    _fsync_directory(parent)


def _spool_response(
    response: object,
    *,
    quarantine_dir: Path,
    max_bytes: int,
) -> _SpooledBody:
    """Spool at most max_bytes + 1 bytes, hashing and writing incrementally."""
    _ensure_directory(quarantine_dir)
    temporary_path: Path | None = None
    digest = hashlib.sha256()
    byte_length = 0
    truncated = False
    error: str | None = None
    limit = max_bytes + 1
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=quarantine_dir,
            prefix=".quarantine-",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            iterator_factory = getattr(response, "iter_content", None)
            if callable(iterator_factory):
                iterator = iterator_factory(chunk_size=min(65536, max(1, limit)))
                chunks = iterator
            else:
                raw = getattr(response, "content")
                chunks = (raw,)
            for chunk in chunks:
                remaining = limit - byte_length
                if remaining <= 0:
                    break
                piece, chunk_exceeded = _bounded_chunk(chunk, remaining)
                if piece:
                    handle.write(piece)
                    digest.update(piece)
                    byte_length += len(piece)
                if chunk_exceeded or byte_length >= limit:
                    truncated = True
                    break
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        error = "body_read_error"
        if temporary_path is None:
            raise
    assert temporary_path is not None
    return _SpooledBody(
        sha256=digest.hexdigest(),
        byte_length=byte_length,
        truncated=truncated,
        temporary_path=temporary_path,
        error=error,
    )


def _finalize_quarantine(
    spool: _SpooledBody,
    *,
    quarantine_dir: Path,
    fiscal_year: str,
) -> Path:
    quarantine_path = Path(quarantine_dir) / f"contratos_{fiscal_year}_{spool.sha256}.csv"
    _reject_symlink_ancestors(quarantine_path)
    if quarantine_path.is_symlink():
        raise ValueError(f"refusing symlink quarantine path: {quarantine_path}")
    if quarantine_path.exists():
        if not _file_matches(
            quarantine_path,
            _read_regular_file(spool.temporary_path, max_bytes=spool.byte_length + 1),
        ):
            raise ValueError(f"immutable file content conflict: {quarantine_path}")
        spool.temporary_path.unlink()
    else:
        os.replace(spool.temporary_path, quarantine_path)
        _fsync_directory(quarantine_path.parent)
    return quarantine_path
def _response_metadata(
    response: object,
    *,
    fiscal_year: str,
    source_url: str,
    captured_at: str,
    validation: PayloadValidation,
    redirect_chain: tuple[str, ...],
) -> dict[str, object]:
    final_url = getattr(response, "url", None)
    if not isinstance(final_url, str) or not final_url:
        final_url = source_url
    headers = getattr(response, "headers", None)
    return {
        "acquisition_status": "captured",
        "status": "captured",
        "fiscal_year": fiscal_year,
        "source_url": source_url,
        "requested_url": source_url,
        "final_url": final_url,
        "redirect_chain": list(redirect_chain),
        "captured_at": captured_at,
        "sha256": validation.sha256,
        "byte_length": validation.byte_length,
        "http_status": getattr(response, "status_code", None),
        "content_type": _header_value(headers, "Content-Type"),
        "content_disposition": _header_value(headers, "Content-Disposition"),
        "encoding": validation.encoding or ENCODING,
    }


def _stable_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2)
        + "\n"
    ).encode("utf-8")


def _capture_result(
    *,
    fiscal_year: str,
    status: str,
    digest: str,
    quarantine_path: Path,
    reason: str | None = None,
    evidence_path: Path | None = None,
    metadata_path: Path | None = None,
    quarantine_truncated: bool = False,
) -> CaptureResult:
    return CaptureResult(
        fiscal_year=fiscal_year,
        status=status,
        sha256=digest,
        evidence_path=evidence_path,
        metadata_path=metadata_path,
        quarantine_path=quarantine_path,
        reason=reason,
        quarantine_truncated=quarantine_truncated,
    )


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
) -> CaptureResult:
    """Quarantine, validate, and immutably retain one response body.

    This function never writes ``active_view``.  Promotion is intentionally a
    separate explicit operation.  The quarantine reader stops at
    ``max_bytes + 1``; an over-limit quarantine is therefore a diagnostic
    prefix and is explicitly marked truncated in the result.
    """
    _validate_fiscal_year(fiscal_year)
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 0:
        raise ValueError("max_bytes must be a non-negative integer")

    quarantine_dir = Path(quarantine_dir)
    evidence_dir = Path(evidence_dir)
    active_view = Path(active_view)
    spool = _spool_response(
        response,
        quarantine_dir=quarantine_dir,
        max_bytes=max_bytes,
    )
    quarantine_path = _finalize_quarantine(
        spool,
        quarantine_dir=quarantine_dir,
        fiscal_year=fiscal_year,
    )
    digest = spool.sha256

    try:
        hosts = _host_allowlist(allowed_hosts)
    except ValueError as exc:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="rejected",
            digest=digest,
            quarantine_path=quarantine_path,
            reason=str(exc),
            quarantine_truncated=spool.truncated,
        )

    source_error = _url_error(source_url, hosts, label="source")
    if source_error is not None:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="rejected",
            digest=digest,
            quarantine_path=quarantine_path,
            reason=source_error,
            quarantine_truncated=spool.truncated,
        )

    chain = _response_url_chain(response, source_url)
    redirect_reason = _redirect_reason(
        chain, requested_url=source_url, allowed_hosts=hosts
    )
    if redirect_reason is not None:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="rejected",
            digest=digest,
            quarantine_path=quarantine_path,
            reason=redirect_reason,
            quarantine_truncated=spool.truncated,
        )

    try:
        parsed_capture_time = datetime.fromisoformat(
            captured_at.replace("Z", "+00:00")
            if isinstance(captured_at, str)
            else captured_at
        )
        if (
            parsed_capture_time.tzinfo is None
            or parsed_capture_time.utcoffset() is None
        ):
            raise ValueError("offset")
        if parsed_capture_time.utcoffset() != timedelta(0):
            return _capture_result(
                fiscal_year=fiscal_year,
                status="rejected",
                digest=digest,
                quarantine_path=quarantine_path,
                reason="captured_at_must_be_utc",
                quarantine_truncated=spool.truncated,
            )
    except (AttributeError, TypeError, ValueError):
        return _capture_result(
            fiscal_year=fiscal_year,
            status="rejected",
            digest=digest,
            quarantine_path=quarantine_path,
            reason="captured_at_must_be_offset_aware",
            quarantine_truncated=spool.truncated,
        )

    if spool.error is not None:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="rejected",
            digest=digest,
            quarantine_path=quarantine_path,
            reason=spool.error,
            quarantine_truncated=spool.truncated,
        )
    if spool.truncated:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="invalid_payload",
            digest=digest,
            quarantine_path=quarantine_path,
            reason="byte_limit_exceeded",
            quarantine_truncated=True,
        )

    try:
        quarantined_body = _read_regular_file(
            quarantine_path,
            max_bytes=max_bytes + 1,
        )
    except (OSError, ValueError):
        return _capture_result(
            fiscal_year=fiscal_year,
            status="rejected",
            digest=digest,
            quarantine_path=quarantine_path,
            reason="quarantine_read_error",
        )

    try:
        validation = validate_bulk_response(
            _ResponseBodyView(response, quarantined_body),
            fiscal_year=fiscal_year,
            max_bytes=max_bytes,
        )
    except Exception:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="rejected",
            digest=digest,
            quarantine_path=quarantine_path,
            reason="validator_error",
        )
    if not validation.valid:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="invalid_payload",
            digest=digest,
            quarantine_path=quarantine_path,
            reason=validation.reason,
            quarantine_truncated=validation.truncated,
        )

    try:
        certify_bulk_file(
            quarantine_path,
            source_channel="official_bulk",
            fiscal_year=fiscal_year,
            source_url=source_url,
            capture_time=captured_at,
            capture_time_status="observed",
        )
    except BulkCertificationError as exc:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="invalid_payload",
            digest=digest,
            quarantine_path=quarantine_path,
            reason=f"certification_failed:{type(exc).__name__}",
        )
    except Exception:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="rejected",
            digest=digest,
            quarantine_path=quarantine_path,
            reason="certifier_error",
        )

    if validator is not None:
        try:
            injected_reason = validator(quarantine_path, fiscal_year)
        except Exception:
            return _capture_result(
                fiscal_year=fiscal_year,
                status="rejected",
                digest=digest,
                quarantine_path=quarantine_path,
                reason="validator_error",
            )
        if injected_reason is not None:
            reason = str(injected_reason) or "validator_rejected"
            return _capture_result(
                fiscal_year=fiscal_year,
                status="rejected",
                digest=digest,
                quarantine_path=quarantine_path,
                reason=reason,
            )

    evidence_path = evidence_dir / fiscal_year / f"{digest}.csv"
    metadata_path = evidence_path.with_suffix(evidence_path.suffix + ".json")
    metadata = _response_metadata(
        response,
        fiscal_year=fiscal_year,
        source_url=source_url,
        captured_at=captured_at,
        validation=validation,
        redirect_chain=chain,
    )
    metadata_bytes = _stable_json_bytes(metadata)

    try:
        _ensure_directory(evidence_path.parent)
        existed = evidence_path.exists()
        if evidence_path.is_symlink() or (existed and not evidence_path.is_file()):
            raise ValueError(f"refusing non-file evidence path: {evidence_path}")
        if existed:
            if not _file_matches(evidence_path, quarantined_body):
                raise ValueError(f"immutable file content conflict: {evidence_path}")
        else:
            _write_once(evidence_path, quarantined_body)

        metadata_existed = metadata_path.exists()
        if metadata_path.is_symlink() or (metadata_existed and not metadata_path.is_file()):
            raise ValueError(f"refusing non-file metadata path: {metadata_path}")
        if metadata_existed:
            if not _file_matches(metadata_path, metadata_bytes):
                return _capture_result(
                    fiscal_year=fiscal_year,
                    status="unchanged",
                    digest=digest,
                    quarantine_path=quarantine_path,
                    evidence_path=evidence_path,
                    metadata_path=metadata_path,
                )
        else:
            _write_once(metadata_path, metadata_bytes)
    except (OSError, ValueError) as exc:
        return _capture_result(
            fiscal_year=fiscal_year,
            status="rejected",
            digest=digest,
            quarantine_path=quarantine_path,
            reason=str(exc),
            evidence_path=evidence_path if evidence_path.is_file() else None,
            metadata_path=metadata_path if metadata_path.is_file() else None,
        )

    return _capture_result(
        fiscal_year=fiscal_year,
        status="unchanged" if existed and metadata_existed else "captured",
        digest=digest,
        quarantine_path=quarantine_path,
        evidence_path=evidence_path,
        metadata_path=metadata_path,
    )


def retain_existing_bulk_snapshot(
    *,
    source_path: Path,
    evidence_dir: Path,
    fiscal_year: str,
    max_bytes: int = 50_000_000,
) -> Path:
    """Retain an already-certified active snapshot before replacing its view."""
    _validate_fiscal_year(fiscal_year)
    source_path = Path(source_path)
    evidence_dir = Path(evidence_dir)
    _reject_symlink_ancestors(source_path)
    body = _read_regular_file(source_path, max_bytes=max_bytes + 1)
    if len(body) > max_bytes:
        raise ValueError("existing active snapshot exceeds max_bytes")
    digest = hashlib.sha256(body).hexdigest()
    evidence_path = evidence_dir / fiscal_year / f"{digest}.csv"
    metadata_path = evidence_path.with_suffix(evidence_path.suffix + ".json")
    metadata = _stable_json_bytes(
        {
            "acquisition_status": "retained_existing_active",
            "fiscal_year": fiscal_year,
            "sha256": digest,
            "byte_length": len(body),
            "source_url": None,
            "capture_time": None,
            "capture_time_status": "unknown",
        }
    )
    _ensure_directory(evidence_path.parent)
    _write_once(evidence_path, body)
    _write_once(metadata_path, metadata)
    return evidence_path


def promote_bulk_snapshot(*, evidence_path: Path, active_view: Path) -> Path:
    """Atomically copy a pinned regular evidence file to ``active_view``."""
    evidence_path = Path(evidence_path)
    active_view = Path(active_view)
    _reject_symlink_ancestors(evidence_path)
    _reject_symlink_ancestors(active_view)

    if evidence_path.is_symlink():
        raise ValueError(f"refusing symlink evidence path: {evidence_path}")
    if active_view.is_symlink():
        raise ValueError(f"refusing symlink active view: {active_view}")
    if active_view.exists() and not active_view.is_file():
        raise ValueError(f"active view is not a regular file: {active_view}")
    _ensure_directory(active_view.parent)
    _reject_symlink_ancestors(active_view)

    evidence_fd, before = _open_regular(evidence_path)
    temporary_path: Path | None = None
    source_digest = hashlib.sha256()
    source_length = 0
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=active_view.parent,
            prefix=f".{active_view.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            while True:
                chunk = os.read(evidence_fd, 65536)
                if not chunk:
                    break
                _write_all(handle, chunk)
                source_digest.update(chunk)
                source_length += len(chunk)
            after = os.fstat(evidence_fd)
            signature_before = (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
                before.st_ctime_ns,
            )
            signature_after = (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
            if signature_before != signature_after or source_length != before.st_size:
                raise OSError("evidence changed during promotion")
            handle.flush()
            os.fsync(handle.fileno())
        _reject_symlink_ancestors(active_view)
        os.replace(temporary_path, active_view)
        temporary_path = None
        _fsync_directory(active_view.parent)
    finally:
        os.close(evidence_fd)
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass

    active_fd, active_metadata = _open_regular(active_view)
    try:
        active_digest = hashlib.sha256()
        active_length = 0
        while True:
            chunk = os.read(active_fd, 65536)
            if not chunk:
                break
            active_digest.update(chunk)
            active_length += len(chunk)
        if (
            active_metadata.st_size != source_length
            or active_length != source_length
            or active_digest.hexdigest() != source_digest.hexdigest()
        ):
            raise OSError("atomic promotion verification failed")
    finally:
        os.close(active_fd)
    return active_view
