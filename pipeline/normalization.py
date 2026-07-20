"""Deterministic, reviewed normalization-registry foundation.

The registry is deliberately standalone: it uses only the Python standard
library and does not import the legacy normalization helpers.  A later task can
wire this module into ingestion without changing the registry's lookup contract.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
import unicodedata
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REGISTRY_VERSION = "normalization-registry-1"
ALGORITHM_VERSION = "lookup-v1"
SUPPORTED_DOMAINS = (
    "contractor",
    "entity",
    "service_category",
    "service_type",
)
_ALIAS_HEADERS = ("alias", "canonical_id", "display_label", "review_status")
_REVIEW_HEADERS = (
    "domain",
    "raw_value",
    "canonical_id",
    "display_label",
    "decision",
    "notes",
)
_REVIEW_DECISIONS = frozenset({"retained", "ambiguous", "rejected"})
_ID_SLUG = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_FORBIDDEN_HEADER_TERMS = frozenset({"candidate", "confidence", "rank", "score", "similarity"})


class RegistryError(ValueError):
    """Raised when registry files violate the deterministic data contract."""


class NormalizationResult(dict):
    """Mapping result with both dictionary and attribute access.

    The mapping contains exactly the six fields in the public result contract.
    Keeping this as a small dict subclass makes the result convenient for both
    pipeline callers and tests while avoiding a second, scoring-oriented result
    shape.
    """

    _FIELDS = (
        "raw_value",
        "alias_key",
        "canonical_id",
        "display_label",
        "status",
        "registry_version",
    )

    def __init__(
        self,
        *,
        raw_value: Any,
        alias_key: str,
        canonical_id: str | None,
        display_label: str | None,
        status: str,
        registry_version: str,
    ) -> None:
        super().__init__(
            raw_value=raw_value,
            alias_key=alias_key,
            canonical_id=canonical_id,
            display_label=display_label,
            status=status,
            registry_version=registry_version,
        )

    def __getattr__(self, name: str) -> Any:
        if name in self._FIELDS:
            return self[name]
        raise AttributeError(name)


@dataclass(frozen=True)
class AliasEntry:
    alias: str
    alias_key: str
    canonical_id: str
    display_label: str
    review_status: str

    def payload(self) -> dict[str, str]:
        return {
            "alias": self.alias,
            "alias_key": self.alias_key,
            "canonical_id": self.canonical_id,
            "display_label": self.display_label,
            "review_status": self.review_status,
        }


@dataclass(frozen=True)
class ReviewDecision:
    domain: str
    raw_value: str
    canonical_id: str
    display_label: str
    decision: str
    notes: str

    def payload(self) -> dict[str, str]:
        return {
            "canonical_id": self.canonical_id,
            "decision": self.decision,
            "display_label": self.display_label,
            "domain": self.domain,
            "notes": self.notes,
            "raw_value": self.raw_value,
        }


class Registry(Mapping[str, tuple[AliasEntry, ...]]):
    """Loaded registry plus its deterministic payload and lookup indexes."""

    def __init__(
        self,
        *,
        version: str,
        algorithm_version: str,
        aliases: dict[str, tuple[AliasEntry, ...]],
        alias_index: dict[str, dict[str, tuple[AliasEntry, ...]]],
        review_decisions: tuple[ReviewDecision, ...],
        payload: str,
    ) -> None:
        self.registry_version = version
        self.algorithm_version = algorithm_version
        self.aliases = aliases
        self.review_decisions = review_decisions
        self._alias_index = alias_index
        self.payload = payload
        self.payload_sha256 = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def __getitem__(self, domain: str) -> tuple[AliasEntry, ...]:
        return self.aliases[domain]

    def __iter__(self) -> Iterator[str]:
        return iter(self.aliases)

    def __len__(self) -> int:
        return len(self.aliases)

    def normalize_value(self, domain: str, raw_value: Any) -> NormalizationResult:
        _validate_domain(domain)
        alias_key = normalize_alias_key(raw_value)
        if alias_key == "":
            status = "missing"
            canonical_id = None
            display_label = None
        else:
            matches = self._alias_index[domain].get(alias_key, ())
            if len(matches) == 1:
                status = "resolved"
                canonical_id = matches[0].canonical_id
                display_label = matches[0].display_label
            elif len(matches) > 1:
                status = "collision"
                canonical_id = None
                display_label = None
            else:
                status = "unresolved"
                canonical_id = None
                display_label = None
        return NormalizationResult(
            raw_value=raw_value,
            alias_key=alias_key,
            canonical_id=canonical_id,
            display_label=display_label,
            status=status,
            registry_version=self.registry_version,
        )


def normalize_alias_key(value: Any) -> str:
    """Return the deterministic lookup key used by all registry domains.

    This is intentionally a conservative exact-key operation.  Unicode is
    NFD-normalized and combining marks are removed; punctuation, symbols,
    controls, and whitespace become separators; repeated separators collapse;
    and the result is uppercased.  No token reordering, suffix removal,
    edit-distance matching, or candidate scoring occurs here.
    """
    if value is None:
        return ""

    normalized = unicodedata.normalize("NFD", str(value))
    output: list[str] = []
    for character in normalized:
        category = unicodedata.category(character)
        if category.startswith("M"):
            continue
        if (
            character.isspace()
            or character == "\x00"
            or category[0] in {"C", "P", "S"}
        ):
            output.append(" ")
        else:
            output.append(character)
    return " ".join("".join(output).upper().split())


# Short alias for callers that want to name the operation after its result.
alias_key = normalize_alias_key


def load_registry(repo_root: str | Path | None = None) -> Registry:
    """Load, validate, and index the complete registry from ``repo_root``."""
    root = _resolve_repo_root(repo_root)
    normalization_dir = root / "data" / "normalization"
    profile = _load_schema_profile(normalization_dir)

    aliases_by_domain: dict[str, tuple[AliasEntry, ...]] = {}
    index_by_domain: dict[str, dict[str, tuple[AliasEntry, ...]]] = {}
    for domain in SUPPORTED_DOMAINS:
        config = profile["domains"][domain]
        path = _safe_registry_file(normalization_dir, config["file"], domain)
        entries = _load_alias_file(path, domain, tuple(config["headers"]))
        aliases_by_domain[domain] = entries
        index_by_domain[domain] = _build_alias_index(entries)

    review_path = _safe_registry_file(
        normalization_dir,
        profile["review_decisions"]["file"],
        "review_decisions",
    )
    review_decisions = _load_review_file(
        review_path,
        tuple(profile["review_decisions"]["headers"]),
    )

    payload = _build_payload(
        version=profile["registry_version"],
        algorithm_version=profile["algorithm_version"],
        aliases=aliases_by_domain,
        review_decisions=review_decisions,
    )
    return Registry(
        version=profile["registry_version"],
        algorithm_version=profile["algorithm_version"],
        aliases=aliases_by_domain,
        alias_index=index_by_domain,
        review_decisions=review_decisions,
        payload=payload,
    )


def normalize_value(
    domain: str,
    raw_value: Any,
    repo_root: str | Path | None = None,
) -> NormalizationResult:
    """Normalize one value using only an exact reviewed registry alias."""
    return load_registry(repo_root).normalize_value(domain, raw_value)


def registry_version(repo_root: str | Path | None = None) -> str:
    """Return the validated registry version."""
    return load_registry(repo_root).registry_version


def registry_payload(repo_root: str | Path | None = None) -> str:
    """Return the canonical JSON payload used for deterministic hashing."""
    return load_registry(repo_root).payload


def _resolve_repo_root(repo_root: str | Path | None) -> Path:
    if repo_root is None:
        return Path(__file__).resolve().parents[1]
    return Path(repo_root).expanduser().resolve()


def _validate_domain(domain: str) -> None:
    if domain not in SUPPORTED_DOMAINS:
        raise RegistryError(f"unsupported normalization domain: {domain!r}")


def _load_schema_profile(normalization_dir: Path) -> dict[str, Any]:
    path = normalization_dir / "schema-profiles.json"
    if not path.is_file():
        raise RegistryError("missing required schema-profiles.json")
    try:
        profile = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise RegistryError("invalid schema-profiles.json") from exc
    if not isinstance(profile, dict):
        raise RegistryError("schema-profiles.json must contain an object")
    if profile.get("registry_version") != REGISTRY_VERSION:
        raise RegistryError("schema profile has the wrong registry_version")
    if profile.get("algorithm_version") != ALGORITHM_VERSION:
        raise RegistryError("schema profile has the wrong algorithm_version")
    if profile.get("schema_version") != "normalization-schema-1":
        raise RegistryError("schema profile has the wrong schema_version")

    domains = profile.get("domains")
    if not isinstance(domains, dict) or set(domains) != set(SUPPORTED_DOMAINS):
        raise RegistryError("schema profile must declare exactly the supported domains")
    if tuple(domains) != tuple(sorted(SUPPORTED_DOMAINS)):
        raise RegistryError("schema profile domains must use stable sorted ordering")
    for domain in SUPPORTED_DOMAINS:
        config = domains[domain]
        if not isinstance(config, dict):
            raise RegistryError(f"schema profile for {domain} must be an object")
        expected_file = f"{domain.replace('_', '-')}-aliases.csv"
        if config.get("file") != expected_file:
            raise RegistryError(f"schema profile has the wrong file for {domain}")
        if tuple(config.get("headers", ())) != _ALIAS_HEADERS:
            raise RegistryError(f"schema profile has the wrong headers for {domain}")
        _validate_header_names(config["headers"], f"{domain} headers")

    review = profile.get("review_decisions")
    if not isinstance(review, dict):
        raise RegistryError("schema profile must declare review_decisions")
    if review.get("file") != "review-decisions.csv":
        raise RegistryError("schema profile has the wrong review decision file")
    if tuple(review.get("headers", ())) != _REVIEW_HEADERS:
        raise RegistryError("schema profile has the wrong review decision headers")
    _validate_header_names(review["headers"], "review decision headers")
    return profile


def _validate_header_names(headers: list[str] | tuple[str, ...], description: str) -> None:
    if len(set(headers)) != len(headers):
        raise RegistryError(f"{description} contain duplicate names")
    for header in headers:
        lowered = header.lower()
        if any(term in lowered for term in _FORBIDDEN_HEADER_TERMS):
            raise RegistryError(f"{description} contain candidate-scoring data")


def _safe_registry_file(normalization_dir: Path, filename: str, label: str) -> Path:
    if not isinstance(filename, str):
        raise RegistryError(f"missing filename for {label}")
    relative = Path(filename)
    if relative.is_absolute() or relative.name != filename or ".." in relative.parts:
        raise RegistryError(f"unsafe filename for {label}")
    path = normalization_dir / relative
    if not path.is_file():
        raise RegistryError(f"missing required registry file for {label}")
    return path


def _read_csv(path: Path, expected_headers: tuple[str, ...]) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != expected_headers:
                raise RegistryError(f"{path.name} has invalid headers")
            rows: list[dict[str, str]] = []
            for row_number, row in enumerate(reader, start=2):
                if None in row or any(value is None for value in row.values()):
                    raise RegistryError(f"{path.name} has an invalid row {row_number}")
                if any("\n" in value or "\r" in value for value in row.values()):
                    raise RegistryError(f"{path.name} has embedded line breaks")
                rows.append({header: row[header] for header in expected_headers})
    except (OSError, UnicodeError, csv.Error) as exc:
        if isinstance(exc, RegistryError):
            raise
        raise RegistryError(f"could not read {path.name}") from exc
    return rows


def _load_alias_file(
    path: Path,
    domain: str,
    headers: tuple[str, ...],
) -> tuple[AliasEntry, ...]:
    if headers != _ALIAS_HEADERS:
        raise RegistryError(f"invalid alias schema for {domain}")
    rows = _read_csv(path, headers)
    entries: list[AliasEntry] = []
    seen_rows: set[tuple[str, str, str, str]] = set()
    canonical_labels: dict[str, str] = {}
    for row in rows:
        alias = row["alias"]
        canonical_id = row["canonical_id"]
        display_label = row["display_label"]
        review_status = row["review_status"]
        if not alias or alias != alias.strip():
            raise RegistryError(f"{path.name} contains a blank or untrimmed alias")
        if not display_label or display_label != display_label.strip():
            raise RegistryError(f"{path.name} contains a blank or untrimmed display label")
        if review_status != "reviewed":
            raise RegistryError(f"{path.name} contains an unreviewed alias")
        _validate_canonical_id(canonical_id, domain)
        prior_label = canonical_labels.setdefault(canonical_id, display_label)
        if prior_label != display_label:
            raise RegistryError(f"{path.name} assigns conflicting labels to {canonical_id}")
        alias_key_value = normalize_alias_key(alias)
        if not alias_key_value:
            raise RegistryError(f"{path.name} contains an empty alias key")
        row_key = (alias, canonical_id, display_label, review_status)
        if row_key in seen_rows:
            continue
        seen_rows.add(row_key)
        entries.append(
            AliasEntry(
                alias=alias,
                alias_key=alias_key_value,
                canonical_id=canonical_id,
                display_label=display_label,
                review_status=review_status,
            )
        )

    _validate_stable_order(
        entries,
        key=lambda entry: (
            entry.alias_key,
            entry.canonical_id,
            entry.display_label,
            entry.review_status,
            entry.alias,
        ),
        description=path.name,
    )
    return tuple(entries)


def _load_review_file(path: Path, headers: tuple[str, ...]) -> tuple[ReviewDecision, ...]:
    if headers != _REVIEW_HEADERS:
        raise RegistryError("invalid review decision schema")
    rows = _read_csv(path, headers)
    decisions: list[ReviewDecision] = []
    seen_rows: set[tuple[str, str, str, str, str, str]] = set()
    for row in rows:
        domain = row["domain"]
        raw_value = row["raw_value"]
        canonical_id = row["canonical_id"]
        display_label = row["display_label"]
        decision = row["decision"]
        notes = row["notes"]
        _validate_domain(domain)
        if not raw_value or raw_value != raw_value.strip():
            raise RegistryError("review decisions contain a blank or untrimmed raw value")
        if decision not in _REVIEW_DECISIONS:
            raise RegistryError(f"unknown review decision: {decision!r}")
        if decision == "retained":
            if not canonical_id or not display_label:
                raise RegistryError("retained decisions require a canonical identity")
            _validate_canonical_id(canonical_id, domain)
        elif canonical_id or display_label:
            raise RegistryError("ambiguous/rejected decisions cannot assert an identity")
        row_key = (domain, raw_value, canonical_id, display_label, decision, notes)
        if row_key in seen_rows:
            continue
        seen_rows.add(row_key)
        decisions.append(
            ReviewDecision(
                domain=domain,
                raw_value=raw_value,
                canonical_id=canonical_id,
                display_label=display_label,
                decision=decision,
                notes=notes,
            )
        )

    _validate_stable_order(
        decisions,
        key=lambda item: (
            item.domain,
            normalize_alias_key(item.raw_value),
            item.decision,
            item.canonical_id,
            item.display_label,
            item.notes,
            item.raw_value,
        ),
        description=path.name,
    )
    return tuple(decisions)


def _validate_stable_order(
    values: list[Any],
    *,
    key: Any,
    description: str,
) -> None:
    keys = [key(value) for value in values]
    if keys != sorted(keys):
        raise RegistryError(f"{description} rows are not in stable sorted order")


def _validate_canonical_id(canonical_id: str, domain: str) -> None:
    prefix = f"{domain}:"
    if not canonical_id.startswith(prefix):
        raise RegistryError(f"canonical ID {canonical_id!r} is not domain-prefixed")
    slug = canonical_id[len(prefix) :]
    if not _ID_SLUG.fullmatch(slug):
        raise RegistryError(f"canonical ID {canonical_id!r} is not a stable ID")


def _build_alias_index(
    entries: tuple[AliasEntry, ...],
) -> dict[str, tuple[AliasEntry, ...]]:
    grouped: dict[str, list[AliasEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.alias_key, [])
        if entry not in grouped[entry.alias_key]:
            grouped[entry.alias_key].append(entry)
    return {key: tuple(value) for key, value in grouped.items()}


def _build_payload(
    *,
    version: str,
    algorithm_version: str,
    aliases: dict[str, tuple[AliasEntry, ...]],
    review_decisions: tuple[ReviewDecision, ...],
) -> str:
    payload = {
        "algorithm_version": algorithm_version,
        "domains": {
            domain: [entry.payload() for entry in aliases[domain]]
            for domain in SUPPORTED_DOMAINS
        },
        "registry_version": version,
        "review_decisions": [item.payload() for item in review_decisions],
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


__all__ = [
    "ALGORITHM_VERSION",
    "REGISTRY_VERSION",
    "NormalizationResult",
    "Registry",
    "RegistryError",
    "alias_key",
    "load_registry",
    "normalize_alias_key",
    "normalize_value",
    "registry_payload",
    "registry_version",
]
