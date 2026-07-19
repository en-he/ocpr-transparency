"""
Header-profile constants and recognition for OCPR bulk-CSV certification.

Defines the closed set of known preserved-corpus header shapes
(`v1`/`v2`/`v3`) and the recognition/fingerprint helpers used by
`pipeline/certify_bulk.py`. See docs/project/bulk-certification.md for the
frozen contract this module implements.
"""
from __future__ import annotations

import hashlib

SOURCE_CHANNELS = ("official_bulk", "archive_bulk")

CAPTURE_TIME_STATUSES = ("observed", "git_first_seen", "unknown")

COMPATIBILITY_NULLABLE_FIELDS = (
    "procurement_method",
    "fund_type",
    "pco_number",
    "document_url",
)

# Exact, order-sensitive parsed header tuples for each known preserved-corpus
# profile (see docs/project/bulk-certification.md "Header profiles").
HEADER_PROFILES = {
    "v1": (
        "Número de Entidad",
        "Entidad",
        "Número de Contrato",
        "Enmienda",
        "Otorgado En",
        "Vigencia Desde",
        "Vigencia Hasta",
        "Tipo de Servicio",
        "Categoría de Servicio",
        "Cancelado",
        "Cuantía",
        "Contratista",
    ),
    "v2": (
        "Número de Entidad",
        "Entidad",
        "Núm. Contrato",
        "Enmienda",
        "Otorgado en",
        "Vigencia Desde",
        "Vigencia Hasta",
        "Tipo de Servicio",
        "Categoría de Servicio",
        "Cancelado",
        "Cuantía",
        "Contratista",
    ),
    "v3": (
        "Número de Entidad",
        "Entidad",
        "Núm. Contrato",
        "Enmienda",
        "Otorgado en",
        "Vigencia Desde",
        "Vigencia Hasta",
        "Tipo de Servicio",
        "Categoría de Servicio",
        "Cancelado",
        "Cuantía",
        "Cuantía a Recibir",
        "Contratista",
    ),
}

# Fiscal years each profile is known to cover in the preserved corpus.
HEADER_PROFILE_FISCAL_YEARS = {
    "v1": ("2010-2011", "2011-2012"),
    "v2": ("2012-2013",),
    "v3": None,  # 2013-2014 onward; open-ended, never enumerated.
}


def detect_header_profile(headers) -> str | None:
    """Recognize an exact known header profile from a raw header row.

    `headers` is the ordered list/tuple of already CSV-split header fields.
    Each field is trimmed before comparison. Matching is by header *set*
    (order-independent), per the contract's header resolution algorithm.
    Returns the profile name only on an unambiguous exact-set match; returns
    None for duplicates, strict subsets, foreign headers, or any other shape
    that does not resolve unambiguously to a single known profile. This
    function never guesses — callers needing the specific failure reason
    (duplicate vs. missing vs. unknown) build on top of this primitive.
    """
    trimmed = [header.strip() for header in headers]
    header_set = set(trimmed)
    if len(header_set) != len(trimmed):
        return None
    for profile, profile_headers in HEADER_PROFILES.items():
        if header_set == set(profile_headers):
            return profile
    return None


def header_fingerprint(headers) -> str:
    """Deterministic, order-sensitive fingerprint of a raw header row.

    Reproduces the exact raw header line (no space after the delimiter, per
    the source's convention) by comma-joining the trimmed fields, then
    hashes the Latin-1 byte representation. Order-sensitive because the join
    order matters and the corpus header bytes are order-sensitive evidence.
    """
    raw_header_line = ",".join(header.strip() for header in headers)
    return hashlib.sha256(raw_header_line.encode("latin-1")).hexdigest()
