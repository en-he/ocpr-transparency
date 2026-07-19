import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = REPO_ROOT / "pipeline"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from check_repository_portability import (  # noqa: E402
    Violation,
    check_paths,
    find_repo_root,
    main,
    parse_git_ls_files_output,
)


class RepositoryPortabilityTests(unittest.TestCase):
    def test_case_fold_collision_reports_each_colliding_path(self):
        violations = check_paths(
            [
                "docs/Read Me.md",
                "docs/read me.MD",
                "docs/Other.md",
            ]
        )

        collision_paths = {
            violation.path
            for violation in violations
            if violation.kind == "case-fold-collision"
        }
        self.assertEqual(
            collision_paths,
            {"docs/Read Me.md", "docs/read me.MD"},
        )

    def test_macos_metadata_names_are_rejected(self):
        violations = check_paths(
            [
                "vendor/.DS_Store",
                "vendor/._resource",
                "vendor/.AppleDouble/file",
                "__MACOSX/archive/file.txt",
            ]
        )

        self.assertEqual(
            {violation.kind for violation in violations},
            {"macos-metadata"},
        )

    def test_absolute_and_backslash_paths_are_rejected(self):
        violations = check_paths(
            [
                "/absolute/file.txt",
                "C:/absolute/file.txt",
                "folder\\file.txt",
            ]
        )

        self.assertIn(
            ("absolute-path", "/absolute/file.txt"),
            {(violation.kind, violation.path) for violation in violations},
        )
        self.assertIn(
            ("absolute-path", "C:/absolute/file.txt"),
            {(violation.kind, violation.path) for violation in violations},
        )
        self.assertEqual(
            {
                violation.path
                for violation in violations
                if violation.kind == "backslash"
            },
            {"folder\\file.txt"},
        )

    def test_trailing_dot_and_space_components_are_rejected(self):
        violations = check_paths(
            [
                "reports/name./review.md",
                "reports/name /review.md",
                "reports/normal/review.md",
            ]
        )

        self.assertEqual(
            {
                violation.path
                for violation in violations
                if violation.kind == "trailing-dot-space"
            },
            {"reports/name./review.md", "reports/name /review.md"},
        )

    def test_windows_reserved_components_are_rejected_even_with_extensions(self):
        violations = check_paths(
            [
                "exports/CON.txt",
                "exports/nul",
                "exports/Com1.csv",
                "exports/LPT9.data",
                "exports/ordinary.txt",
            ]
        )

        self.assertEqual(
            {
                violation.path
                for violation in violations
                if violation.kind == "windows-reserved-name"
            },
            {
                "exports/CON.txt",
                "exports/nul",
                "exports/Com1.csv",
                "exports/LPT9.data",
            },
        )

    def test_unicode_and_space_containing_path_is_valid(self):
        self.assertEqual(
            check_paths(["docs/Évidence Review 2026/über.svg"]),
            [],
        )

    def test_git_ls_files_parser_preserves_newlines_and_unicode(self):
        self.assertEqual(
            parse_git_ls_files_output(
                b"docs/line\nname.md\0data/raw/\xc3\xa9vidence.csv\0"
            ),
            ["docs/line\nname.md", "data/raw/évidence.csv"],
        )

    def test_repo_root_is_found_from_root_and_pipeline_directory(self):
        self.assertEqual(find_repo_root(REPO_ROOT), REPO_ROOT)
        self.assertEqual(find_repo_root(PIPELINE_DIR), REPO_ROOT)

    def test_explicit_invalid_root_does_not_fall_back_to_script_repository(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            invalid_root = Path(temporary_directory) / "not-a-repository"
            with self.assertRaisesRegex(RuntimeError, "could not find a Git repository"):
                find_repo_root(invalid_root)

    def test_cli_returns_two_for_explicit_invalid_root(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            invalid_root = Path(temporary_directory) / "not-a-repository"
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                exit_code = main(["--root", str(invalid_root)])

        self.assertEqual(exit_code, 2)
        self.assertIn("repository portability check could not run", stderr.getvalue())

    def test_cli_escapes_control_characters_in_invalid_root(self):
        hostile_root = Path(tempfile.gettempdir()) / "not-a-repository\n\x1b[31m"
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            exit_code = main(["--root", str(hostile_root)])

        rendered = stderr.getvalue()
        self.assertEqual(exit_code, 2)
        self.assertNotIn("\x1b", rendered)
        self.assertEqual(rendered.count("\n"), 1)
        self.assertIn(r"\n\x1b[31m", rendered)

    def test_violation_output_escapes_control_characters(self):
        rendered = str(
            Violation(
                "case-fold-collision",
                "docs/line\nname\x1b[31m.md",
                "collides with: 'docs/LINE\\nNAME.md'",
            )
        )

        self.assertNotIn("\n", rendered)
        self.assertNotIn("\x1b", rendered)
        self.assertIn(r"\n", rendered)
        self.assertIn(r"\x1b", rendered)

    def test_git_attributes_protect_evidence_and_normalize_reviewable_text(self):
        paths = [
            "README.md",
            ".github/workflows/sync.yml",
            "docs/diagram.svg",
            "data/raw/example.csv",
            "data/recovery/live_recovered_contracts.csv",
            "tests/fixtures/bulk/ocpr-bulk-v1.csv",
            "data/db/example.db-wal",
            "data/db/archive.sqlite3.bak",
            "site/example.db.gz",
            "site/example.db.gz.part-001",
            "documents/example.pdf",
            "images/example.png",
        ]
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "check-attr", "-z", "text", "eol", "diff", "--", *paths],
            check=True,
            capture_output=True,
        )
        fields = [os.fsdecode(field) for field in result.stdout.split(b"\0") if field]
        attributes = {
            (fields[index], fields[index + 1]): fields[index + 2]
            for index in range(0, len(fields), 3)
        }

        for path in paths[:3]:
            self.assertIn(attributes[(path, "text")], {"set", "auto"})
            self.assertEqual(attributes[(path, "eol")], "lf")
        for path in paths[3:]:
            self.assertEqual(attributes[(path, "text")], "unset")
            self.assertEqual(attributes[(path, "eol")], "unset")
            self.assertEqual(attributes[(path, "diff")], "unset")

    def test_gitignore_rules_are_scoped_to_local_residue(self):
        ignored_paths = [
            ".DS_Store",
            "nested/._resource",
            ".AppleDouble/resource",
            "__MACOSX/archive/file.txt",
            ".claude/settings.local.json",
            ".claude/worktrees/example/file.txt",
            ".idea/workspace.xml",
        ]
        visible_paths = [
            "docs/project/architecture.md",
            "docs/project/notes~",
            "nested/.idea/project.xml",
            ".claude/architecture.md",
        ]

        # Use a temporary repository so copied machine-local excludes in the real
        # checkout cannot mask whether the project .gitignore itself is scoped.
        with tempfile.TemporaryDirectory() as temporary_directory:
            test_repository = Path(temporary_directory)
            subprocess.run(
                ["git", "init", "-q", str(test_repository)],
                check=True,
            )
            (test_repository / ".gitignore").write_bytes(
                (REPO_ROOT / ".gitignore").read_bytes()
            )

            for path in ignored_paths:
                result = subprocess.run(
                    ["git", "-C", str(test_repository), "check-ignore", "--no-index", "-q", "--", path]
                )
                self.assertEqual(result.returncode, 0, path)
            for path in visible_paths:
                result = subprocess.run(
                    ["git", "-C", str(test_repository), "check-ignore", "--no-index", "-q", "--", path]
                )
                self.assertEqual(result.returncode, 1, path)


if __name__ == "__main__":
    unittest.main()
