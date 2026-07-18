"""Check tracked repository paths for common cross-platform portability hazards."""
from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
import ntpath
import os
from pathlib import Path
import posixpath
import re
import subprocess
import sys
from typing import Iterable, Sequence


_KIND_ORDER = {
    "macos-metadata": 0,
    "absolute-path": 1,
    "backslash": 2,
    "case-fold-collision": 3,
    "trailing-dot-space": 4,
    "windows-reserved-name": 5,
}

_MACOS_METADATA_NAMES = {".AppleDouble", ".DS_Store", "__MACOSX"}
_WINDOWS_RESERVED_NAMES = {
    "con",
    "prn",
    "aux",
    "nul",
    *(f"com{number}" for number in range(1, 10)),
    *(f"lpt{number}" for number in range(1, 10)),
    "com¹",
    "com²",
    "com³",
    "lpt¹",
    "lpt²",
    "lpt³",
}
_COMPONENT_SEPARATOR_RE = re.compile(r"[/\\]")


@dataclass(frozen=True)
class Violation:
    """One portability violation found in a tracked path."""

    kind: str
    path: str
    detail: str = ""

    def __str__(self) -> str:
        suffix = f" ({self.detail})" if self.detail else ""
        return f"{self.kind}: {self.path!r}{suffix}"


class RepositoryNotFoundError(RuntimeError):
    """Raised when a repository root cannot be found from a starting path."""


def _candidate_directories(start: Path) -> Iterable[Path]:
    candidate = start.expanduser().resolve()
    if candidate.is_file():
        candidate = candidate.parent
    yield candidate
    yield from candidate.parents


def find_repo_root(start: Path | str | None = None) -> Path:
    """Find the Git root from *start*, using Git before a marker fallback.

    The default starts at the current working directory, so the checker works
    both from the repository root and from ``pipeline/``.  The script's own
    directory is also tried as a fallback for direct invocation elsewhere.
    """

    requested = Path.cwd() if start is None else Path(start)
    starts = [requested]
    script_directory = Path(__file__).resolve().parent
    if start is None and script_directory not in starts:
        starts.append(script_directory)

    for initial in starts:
        initial = initial.expanduser().resolve()
        if initial.is_file():
            initial = initial.parent
        try:
            result = subprocess.run(
                ["git", "-C", str(initial), "rev-parse", "--show-toplevel"],
                check=True,
                capture_output=True,
            )
        except (OSError, subprocess.CalledProcessError):
            result = None
        if result is not None:
            raw_root = result.stdout.rstrip(b"\r\n")
            if raw_root:
                return Path(os.fsdecode(raw_root)).resolve()

        for candidate in _candidate_directories(initial):
            if (candidate / ".git").exists():
                return candidate

    raise RepositoryNotFoundError(
        f"could not find a Git repository from {str(requested.resolve())!r}"
    )


def parse_git_ls_files_output(output: bytes) -> list[str]:
    """Decode NUL-delimited ``git ls-files -z`` output without losing bytes."""

    return [os.fsdecode(raw_path) for raw_path in output.split(b"\0") if raw_path]


def tracked_paths(repo_root: Path | str) -> list[str]:
    """Return index-tracked paths using Git's NUL-delimited output mode."""

    result = subprocess.run(
        ["git", "-C", str(repo_root), "ls-files", "--cached", "-z"],
        check=True,
        capture_output=True,
    )
    return parse_git_ls_files_output(result.stdout)


def _components(path: str) -> tuple[str, ...]:
    return tuple(part for part in _COMPONENT_SEPARATOR_RE.split(path) if part)


def _is_macos_metadata_component(component: str) -> bool:
    return (
        component in _MACOS_METADATA_NAMES
        or component.startswith("._")
    )


def _is_windows_reserved_component(component: str) -> bool:
    # Windows reserves the device name before the first extension, and also
    # treats trailing dots/spaces as if they were absent.
    normalized = component.rstrip(" .").casefold()
    if not normalized:
        return False
    stem = normalized.split(".", 1)[0]
    return stem in _WINDOWS_RESERVED_NAMES


def _is_absolute_path(path: str) -> bool:
    # Check both grammars: this checker runs on POSIX but validates paths that
    # may be checked out on Windows too.
    return posixpath.isabs(path) or ntpath.isabs(path)


def check_paths(paths: Iterable[str]) -> list[Violation]:
    """Return deterministic portability violations for the supplied paths."""

    unique_paths = sorted({str(path) for path in paths})
    violations: list[Violation] = []

    for path in unique_paths:
        components = _components(path)
        if any(_is_macos_metadata_component(component) for component in components):
            violations.append(Violation("macos-metadata", path))
        if _is_absolute_path(path):
            violations.append(Violation("absolute-path", path))
        if "\\" in path:
            violations.append(Violation("backslash", path))
        if any(component.endswith((".", " ")) for component in components):
            violations.append(Violation("trailing-dot-space", path))
        if any(_is_windows_reserved_component(component) for component in components):
            violations.append(Violation("windows-reserved-name", path))

    casefolded: dict[str, list[str]] = defaultdict(list)
    for path in unique_paths:
        casefolded[path.casefold()].append(path)
    for members in casefolded.values():
        if len(members) > 1:
            detail = "collides with: " + ", ".join(repr(member) for member in members)
            for path in members:
                violations.append(Violation("case-fold-collision", path, detail))

    return sorted(
        violations,
        key=lambda violation: (
            _KIND_ORDER[violation.kind],
            violation.path,
            violation.detail,
        ),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check Git-tracked paths for cross-platform portability hazards."
    )
    parser.add_argument(
        "--root",
        type=Path,
        help="repository or subdirectory to inspect (defaults to the current directory)",
    )
    args = parser.parse_args(argv)

    try:
        repo_root = find_repo_root(args.root)
        paths = tracked_paths(repo_root)
    except (OSError, RepositoryNotFoundError, subprocess.CalledProcessError) as error:
        print(f"repository portability check could not run: {error}", file=sys.stderr)
        return 2

    violations = check_paths(paths)
    for violation in violations:
        print(violation)
    if violations:
        print(
            f"repository portability check failed: {len(violations)} violation(s)",
            file=sys.stderr,
        )
        return 1

    print(
        f"repository portability check passed: {len(paths)} tracked path(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
