import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
BASELINE_COMMANDS = (
    "python pipeline/check_repository_portability.py",
    "python -m py_compile pipeline/*.py tests/*.py",
    "python -m unittest discover -s tests",
    "python pipeline/generate_bulk_certification.py --check",
)
SYNC_SIDE_EFFECT_STEPS = (
    "Hydrate full DB from release asset when needed",
    "Bootstrap full DB from archived CSVs",
    "Weekly — bounded discovery, capture, and promotion",
    "Weekly — reset and re-ingest tracked sources",
    "Monthly audit — reset and ingest tracked sources",
    "Full rebuild — bounded discovery, capture, and promotion",
    "Full rebuild — reset and ingest",
    "Build browser DB + manifest",
    "Publish full DB release asset",
    "Commit updated data",
)


def workflow_text(filename):
    return (WORKFLOWS_DIR / filename).read_text(encoding="utf-8")


def baseline_positions(workflow):
    positions = [workflow.index(command) for command in BASELINE_COMMANDS]
    if positions != sorted(positions):
        raise AssertionError("baseline commands are not in the required order")
    return positions


def named_step_block(workflow, step_name):
    match = re.search(
        rf"(?m)^(?P<indent>\s*)- name:\s*{re.escape(step_name)}\s*$",
        workflow,
    )
    if match is None:
        raise AssertionError(f"step not found: {step_name}")

    indent = match.group("indent")
    next_step = re.search(
        rf"(?m)^{re.escape(indent)}- (?:name:|uses:|run:)",
        workflow[match.end() :],
    )
    end = match.end() + next_step.start() if next_step else len(workflow)
    return workflow[match.start() : end]


class CIWorkflowTests(unittest.TestCase):
    def test_ci_workflow_has_pull_request_and_main_push_triggers(self):
        ci = WORKFLOWS_DIR / "ci.yml"
        self.assertTrue(ci.is_file())
        workflow = ci.read_text(encoding="utf-8")
        self.assertRegex(workflow, r"(?m)^[ \t]+pull_request:[ \t]*$")
        self.assertRegex(workflow, r"(?m)^[ \t]+push:[ \t]*$")
        self.assertRegex(
            workflow,
            r"(?ms)^[ \t]+push:[ \t]*\n.*?^[ \t]+branches:[ \t]*$.*?^[ \t]+-[ \t]*['\"]?main['\"]?[ \t]*$",
        )

    def test_ci_is_read_only_and_uses_portable_checkout_and_cached_python(self):
        workflow = workflow_text("ci.yml")
        permissions = re.search(
            r"(?ms)^permissions:\s*\n(?P<body>(?:^[ \t]+[^\n]+\n?)+)",
            workflow,
        )
        self.assertIsNotNone(permissions)
        permission_lines = [
            line.strip()
            for line in permissions.group("body").splitlines()
            if line.strip()
        ]
        self.assertEqual(permission_lines, ["contents: read"])
        self.assertRegex(
            workflow,
            r"(?ms)actions/checkout@v4.*?with:\s*\n"
            r"\s+fetch-depth:\s*0.*?lfs:\s*false.*?persist-credentials:\s*false",
        )
        self.assertRegex(workflow, r"(?ms)actions/setup-python@v5.*?cache:\s*pip")
        self.assertRegex(
            workflow,
            r"(?ms)actions/setup-python@v5.*?cache-dependency-path:\s*pipeline/requirements\.txt",
        )

    def test_both_workflows_install_requirements_and_share_ordered_baseline(self):
        ci = workflow_text("ci.yml")
        sync = workflow_text("sync.yml")
        for workflow in (ci, sync):
            self.assertRegex(workflow, r"python-version:\s*['\"]?3\.12['\"]?")
            self.assertIn("pip install -r pipeline/requirements.txt", workflow)
            positions = baseline_positions(workflow)
            self.assertLess(
                workflow.index("pip install -r pipeline/requirements.txt"),
                min(positions),
            )
            for command in BASELINE_COMMANDS:
                self.assertEqual(named_step_block(workflow, "Validate repository baseline").count(command), 1)

        self.assertEqual(
            [command for command in BASELINE_COMMANDS if command in ci],
            [command for command in BASELINE_COMMANDS if command in sync],
        )

    def test_every_certification_workflow_checks_out_complete_history(self):
        for filename in ("ci.yml", "sync.yml", "pages.yml"):
            workflow = workflow_text(filename)
            checkout = re.search(
                r"(?ms)uses: actions/checkout@v[45]\s*\n"
                r"\s+with:\s*\n(?P<body>(?:\s+[^\n]+\n?)+?)"
                r"(?=\s+- (?:uses:|name:|run:))",
                workflow,
            )
            self.assertIsNotNone(checkout, filename)
            self.assertRegex(
                checkout.group("body"),
                r"(?m)^\s+fetch-depth:\s*0\s*$",
                filename,
            )

    def test_sync_baseline_precedes_all_current_side_effect_steps(self):
        workflow = workflow_text("sync.yml")
        positions = baseline_positions(workflow)
        install_position = workflow.index("pip install -r pipeline/requirements.txt")

        self.assertGreater(min(positions), install_position)
        for step_name in SYNC_SIDE_EFFECT_STEPS:
            self.assertLess(
                max(positions),
                workflow.index(f"- name: {step_name}"),
                step_name,
            )

        validate_step = workflow.index("- name: Validate repository baseline")
        first_side_effect = min(
            workflow.index(f"- name: {step_name}")
            for step_name in SYNC_SIDE_EFFECT_STEPS
        )
        self.assertLess(validate_step, first_side_effect)

    def test_baseline_commands_are_not_continue_on_error(self):
        for filename in ("ci.yml", "sync.yml"):
            workflow = workflow_text(filename)
            validate_step = named_step_block(workflow, "Validate repository baseline")
            self.assertNotRegex(
                validate_step,
                r"(?m)^\s*continue-on-error:\s*",
            )
            for command in BASELINE_COMMANDS:
                self.assertIn(command, validate_step)

    def test_step_block_includes_controls_before_run(self):
        workflow = """jobs:
  baseline:
    steps:
      - name: Validate repository baseline
        continue-on-error: true
        run: |
          python pipeline/check_repository_portability.py
      - run: echo next
"""
        block = named_step_block(workflow, "Validate repository baseline")
        self.assertIn("continue-on-error: true", block)
        self.assertNotIn("echo next", block)

    def test_sync_permissions_are_exactly_contents_actions_and_issues_write(self):
        workflow = workflow_text("sync.yml")
        permissions = re.search(
            r"(?ms)^    permissions:\s*\n(?P<body>(?:^      [^\n]+\n?)+)",
            workflow,
        )
        self.assertIsNotNone(permissions)
        permission_lines = [
            line.strip()
            for line in permissions.group("body").splitlines()
            if line.strip()
        ]
        self.assertEqual(
            permission_lines,
            ["contents: write", "actions: write", "issues: write"],
        )

    def test_sync_is_main_only_serialized_and_checks_out_latest_main(self):
        workflow = workflow_text("sync.yml")
        self.assertRegex(
            workflow,
            r"(?ms)^concurrency:\s*\n"
            r"  group:\s*contract-sync\s*\n"
            r"  cancel-in-progress:\s*false\s*$",
        )

        sync_header = workflow[
            workflow.index("  sync:") : workflow.index("    steps:")
        ]
        self.assertRegex(
            sync_header,
            r"(?m)^    if:\s*github\.ref == 'refs/heads/main'\s*$",
        )

        checkout = re.search(
            r"(?ms)^\s*- uses: actions/checkout@v4\s*\n"
            r"\s+with:\s*\n(?P<body>(?:\s+[^\n]+\n?)+?)"
            r"(?=\s+- (?:uses:|name:|run:))",
            workflow,
        )
        self.assertIsNotNone(checkout)
        self.assertRegex(checkout.group("body"), r"(?m)^\s+ref:\s*main\s*$")
        self.assertRegex(checkout.group("body"), r"(?m)^\s+fetch-depth:\s*0\s*$")
        self.assertRegex(checkout.group("body"), r"(?m)^\s+lfs:\s*false\s*$")

    def test_sync_auto_commit_handles_optional_paths_and_has_stable_id(self):
        workflow = workflow_text("sync.yml")
        auto_commit = named_step_block(workflow, "Commit updated data")
        self.assertIn("id: auto_commit", auto_commit)
        self.assertNotIn("git-auto-commit-action", auto_commit)
        self.assertIn(
            'COMMIT_MESSAGE: "data: ${{ steps.mode.outputs.mode }} sync ${{ steps.stamp.outputs.date }}"',
            auto_commit,
        )
        self.assertIn('add_if_present_or_tracked "data/evidence/bulk"', auto_commit)
        self.assertIn('add_if_present_or_tracked "data/db/monitor_state.json"', auto_commit)
        self.assertIn("git ls-files 'site/contratos.db.gz*'", auto_commit)
        self.assertIn("compgen -G 'site/contratos.db.gz*' || true", auto_commit)
        self.assertIn('git add -A -- "${paths[@]}"', auto_commit)
        self.assertIn('echo "changes_detected=false" >> "$GITHUB_OUTPUT"', auto_commit)
        self.assertIn('echo "changes_detected=true" >> "$GITHUB_OUTPUT"', auto_commit)
        self.assertIn('git push origin HEAD:main', auto_commit)

    def test_sync_commits_before_release_publication_and_pages_dispatch(self):
        workflow = workflow_text("sync.yml")
        commit = workflow.index("- name: Commit updated data")
        publish = workflow.index("- name: Publish full DB release asset")
        dispatch = workflow.index("- name: Dispatch Pages deployment")
        self.assertLess(commit, publish)
        self.assertLess(publish, dispatch)

    def test_sync_dispatches_pages_only_after_a_detected_auto_commit(self):
        workflow = workflow_text("sync.yml")
        auto_commit_start = workflow.index("- name: Commit updated data")
        dispatch_start = workflow.index("- name: Dispatch Pages deployment")
        self.assertGreater(dispatch_start, auto_commit_start)

        dispatch = named_step_block(workflow, "Dispatch Pages deployment")
        self.assertRegex(
            dispatch,
            r"(?m)^\s*if: steps\.auto_commit\.outputs\.changes_detected == 'true'\s*$",
        )
        self.assertRegex(
            dispatch,
            r"(?ms)^\s*env:\s*\n\s+GH_TOKEN:\s*\$\{\{ github\.token \}\}\s*$",
        )
        command = "gh workflow run pages.yml --ref main"
        self.assertEqual(workflow.count(command), 1)
        self.assertIn("for attempt in 1 2 3; do", dispatch)
        self.assertIn(f"if {command}; then", dispatch)
        self.assertIn('sleep "$((attempt * 10))"', dispatch)
        self.assertIn("exit 1", dispatch)

    def test_sync_certifies_each_source_mutation_before_ingest_and_publication(self):
        workflow = workflow_text("sync.yml")
        weekly_download = workflow.index("- name: Weekly — bounded discovery, capture, and promotion")
        weekly_certify = workflow.index("- name: Weekly — certify promoted snapshots")
        weekly_ingest = workflow.index("- name: Weekly — reset and re-ingest tracked sources")
        full_download = workflow.index("- name: Full rebuild — bounded discovery, capture, and promotion")
        full_certify = workflow.index("- name: Full rebuild — certify promoted snapshots")
        full_ingest = workflow.index("- name: Full rebuild — reset and ingest")
        post_gate = workflow.index("- name: Validate post-sync publication gate")
        build = workflow.index("- name: Build browser DB + manifest")
        publish = workflow.index("- name: Publish full DB release asset")
        commit = workflow.index("- name: Commit updated data")

        self.assertLess(weekly_download, weekly_certify)
        self.assertLess(weekly_certify, weekly_ingest)
        self.assertLess(full_download, full_certify)
        self.assertLess(full_certify, full_ingest)
        self.assertLess(max(weekly_ingest, full_ingest), post_gate)
        self.assertLess(post_gate, min(build, publish, commit))
        self.assertLess(build, commit)
        self.assertLess(commit, publish)
        for step_name in (
            "Weekly — certify promoted snapshots",
            "Full rebuild — certify promoted snapshots",
            "Validate post-sync publication gate",
        ):
            block = named_step_block(workflow, step_name)
            self.assertIn("python pipeline/generate_bulk_certification.py --check", block)
            self.assertNotIn("continue-on-error", block)

    def test_sync_retains_failed_capture_diagnostics_without_publishing(self):
        workflow = workflow_text("sync.yml")
        upload = named_step_block(workflow, "Retain failed capture diagnostics")
        self.assertRegex(upload, r"(?m)^\s*if:\s*failure\(\)\s*$")
        self.assertIn("actions/upload-artifact@v4", upload)
        self.assertIn("data/quarantine/bulk", upload)
        self.assertIn("data/evidence/bulk", upload)
        self.assertGreater(
            workflow.index("- name: Retain failed capture diagnostics"),
            workflow.index("- name: Dispatch Pages deployment"),
        )

    def test_sync_opens_or_updates_one_failure_review_issue_after_diagnostics(self):
        workflow = workflow_text("sync.yml")
        diagnostics_start = workflow.index("- name: Retain failed capture diagnostics")
        notification_start = workflow.index("- name: Notify failed sync review")
        self.assertGreater(notification_start, diagnostics_start)

        notification = named_step_block(workflow, "Notify failed sync review")
        self.assertRegex(notification, r"(?m)^\s*if:\s*failure\(\)\s*$")
        self.assertRegex(
            notification,
            r"(?ms)^\s*env:\s*\n\s+GH_TOKEN:\s*\$\{\{ github\.token \}\}",
        )
        self.assertIn("GH_REPO: ${{ github.repository }}", notification)
        self.assertIn('TITLE: "Contract Sync review required"', notification)
        self.assertIn("REQUESTED_MODE: ${{ inputs.mode }}", notification)
        self.assertIn("SCHEDULE: ${{ github.event.schedule }}", notification)
        self.assertNotIn("steps.mode.outputs.mode", notification)
        self.assertIn("gh issue list --state open", notification)
        self.assertIn(
            "--search 'in:title \"Contract Sync review required\"'", notification
        )
        self.assertIn(
            'select(.title == "Contract Sync review required")', notification
        )
        self.assertIn('if [ -n "$REQUESTED_MODE" ]; then', notification)
        self.assertIn('elif [ "$SCHEDULE" = "0 7 * * 0" ]; then', notification)
        self.assertIn('elif [ "$SCHEDULE" = "0 8 2 * *" ]; then', notification)
        self.assertIn('gh issue comment "$issue_number" --body "$body"', notification)
        self.assertIn('gh issue create --title "$TITLE" --body "$body"', notification)
        self.assertIn("${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}", notification)
        self.assertIn("The workflow did not complete the publication chain.", notification)
        self.assertNotIn("The workflow blocked publication.", notification)
        self.assertNotIn("continue-on-error", notification)

    def test_pages_recertifies_repository_before_upload_and_deploy(self):
        pages = workflow_text("pages.yml")
        check = pages.index("python pipeline/generate_bulk_certification.py --check")
        upload = pages.index("uses: actions/upload-pages-artifact@v4")
        deploy = pages.index("uses: actions/deploy-pages@v4")
        self.assertLess(check, upload)
        self.assertLess(upload, deploy)

    def test_pages_workflow_retains_manual_dispatch_trigger(self):
        pages = workflow_text("pages.yml")
        self.assertRegex(pages, r"(?m)^  workflow_dispatch:\s*$")


if __name__ == "__main__":
    unittest.main()
