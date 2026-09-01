"""Static contracts for immutable, fail-closed repository automation."""

import re
from pathlib import Path


ROOT = Path(__file__).parent.parent
AUTOMATION_REVISION = "2f34a4da5c552bc23c75edd3d8d81be0a4b3271c"
PYTHON_LIBRARIES_REVISION = "28fa329702bc76896cc54ab8d05ec5b1bd3d929e"


def test_reusable_workflows_are_immutably_pinned() -> None:
    expected = {
        "ci.yml": "reusable-ci.yml",
        "release.yml": "reusable-release.yml",
    }
    for name, reusable_name in expected.items():
        workflow = (ROOT / ".github" / "workflows" / name).read_text()
        refs = re.findall(
            rf"uses: groovemap-music/automation/\.github/workflows/{reusable_name}@([^\s]+)",
            workflow,
        )
        assert refs == [AUTOMATION_REVISION]
        assert "groovemap-music/.github/" not in workflow
        assert "secrets: inherit" not in workflow


def test_dependabot_pull_requests_run_the_ordinary_required_ci_graph() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text()

    assert "pull_request:" in workflow
    assert "schedule:" in workflow
    assert "workflow_dispatch:" in workflow
    jobs = workflow.split("jobs:\n", 1)[1]
    assert len(re.findall(r"^  [a-zA-Z0-9_-]+:\s*$", jobs, re.MULTILINE)) == 1
    assert "jobs:\n  required:" in workflow
    assert "github.actor" not in workflow.lower()
    assert "dependabot" not in workflow.lower()
    assert "fallback-command" not in workflow
    assert "if:" not in workflow.lower()

    for fragment in (
        "language: python",
        "setup-command: just setup",
        "check-command: just check",
        "coverage-command: just test",
        "audit-command: just audit",
        "license-command: just license-check",
        "secret-scan-command: just secret-scan",
        "package-command: just build",
        "install-command: just install-check",
        "image-command: just image",
        "coverage-files: coverage.xml",
        "upload-codecov: true",
        "CODECOV_TOKEN: ${{ secrets.CODECOV_TOKEN }}",
    ):
        assert fragment in workflow

    for marker in (
        "requires-private-library",
        "private-library-client-id",
        "private-library-revision",
        "private_library_private_key",
        "groovemap_ci_app_client_id",
        "groovemap_ci_app_private_key",
    ):
        assert marker not in workflow.lower()

    pyproject = (ROOT / "pyproject.toml").read_text()
    assert "https://github.com/groovemap-music/python-libraries.git" in pyproject
    assert PYTHON_LIBRARIES_REVISION in pyproject


def test_release_is_tag_only_attested_and_repository_named() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text()

    assert re.search(r'on:\s*\n  push:\s*\n    tags: \["v\*"\]', workflow)
    assert "workflow_dispatch:" not in workflow
    assert "schedule:" not in workflow
    assert "branches:" not in workflow
    assert "attestations: write" in workflow
    assert "id-token: write" in workflow
    assert "packages: write" in workflow
    assert "repository-name: discogs-sql-loader" in workflow
    assert "release-command: just release-dry-run" in workflow
    assert "publish-image: true" in workflow
    assert "prepare-image-command: just prepare-runtime-wheel" in workflow
    assert "latest" not in workflow.lower()
    for marker in (
        "requires-private-library",
        "private-library-client-id",
        "private-library-revision",
        "private_library_private_key",
        "groovemap_ci_app_client_id",
        "groovemap_ci_app_private_key",
    ):
        assert marker not in workflow.lower()


def test_required_regression_suites_remain_in_the_full_gate() -> None:
    expected_tests = {
        "tests/test_shutdown_delivery_churn.py": (
            "test_shutdown_guard_leaves_repeated_deliveries_unsettled",
            "test_shutdown_cancels_every_consumer_before_connection_close",
        ),
        "tests/test_file_completion.py": (
            "test_file_complete_flushes_marks_and_acknowledges",
            "test_failed_drain_requeues_marker_without_marking_complete",
        ),
        "tests/test_batch_performance.py": (
            "test_batch_size_500_processes_1000_records_with_two_writes",
            "test_mocked_throughput_remains_above_500_records_per_second",
        ),
        "tests/test_batch_processor.py": (
            "test_drain_blocks_on_in_flight_batch",
            "test_transient_failures_increment_on_operational_error",
        ),
    }
    for relative_path, test_names in expected_tests.items():
        source = (ROOT / relative_path).read_text()
        for test_name in test_names:
            assert f"def {test_name}(" in source


def test_no_renovate_or_legacy_claude_workflow_exists() -> None:
    repository_paths = [path.relative_to(ROOT).as_posix().lower() for path in ROOT.rglob("*") if path.is_file()]
    assert not any("renovate" in path for path in repository_paths)
    assert not any(path.startswith(".github/workflows/claude") for path in repository_paths)
