from __future__ import annotations

from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from scripts import versioning  # noqa: E402


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def build_repo_fixture(repo_root: Path, *, version: str, release_notes_version: str | None = None) -> None:
    release_version = release_notes_version or version
    write_text(repo_root / "VERSION", f"{version}\n")
    write_text(
        repo_root / "README.md",
        "\n".join(
            [
                f"docker build -f bdw/Dockerfile -t bit-data-workbench:{version} .",
                f"  -e IMAGE_VERSION={version} ^",
                f"  bit-data-workbench:{version}",
                f"docker-hub.nexus.bit.admin.ch/svabra/bit-data-workbench:{version}",
                "",
            ]
        ),
    )
    write_text(
        repo_root / "k8s" / "bdw-deployment.yaml",
        (
            "spec:\n"
            "  template:\n"
            "    spec:\n"
            "      containers:\n"
            "        - name: bit-data-workbench\n"
            f"          image: docker-hub.nexus.bit.admin.ch/svabra/bit-data-workbench:{version}\n"
        ),
    )
    write_text(repo_root / "Dockerfile", 'ARG IMAGE_VERSION=""\n')
    write_text(repo_root / "bdw" / "Dockerfile", 'ARG IMAGE_VERSION=""\n')
    write_text(
        repo_root / "compose.yaml",
        (
            "services:\n"
            "  bit-data-workbench:\n"
            "    environment:\n"
            '      PORT: "8000"\n'
        ),
    )
    write_text(
        repo_root / "bdw" / "bit_data_workbench" / "release_notes.py",
        (
            f"# Derived from git history through version {release_version}. Keep entries concise and\n"
            "RELEASE_NOTES = [\n"
            f'    {{"version": "{release_version}", "releasedAt": "2026-01-01T00:00:00+00:00", "features": []}},\n'
            "]\n"
        ),
    )
    write_text(repo_root / "tests" / "test_placeholder.py", "def test_placeholder():\n    assert True\n")


def git(repo_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


class VersioningScriptTests(TestCase):
    def test_check_repository_detects_release_surface_drift(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            build_repo_fixture(repo_root, version="1.2.3")
            write_text(
                repo_root / "README.md",
                "\n".join(
                    [
                        "docker build -f bdw/Dockerfile -t bit-data-workbench:1.2.2 .",
                        "  -e IMAGE_VERSION=1.2.3 ^",
                        "  bit-data-workbench:1.2.3",
                        "docker-hub.nexus.bit.admin.ch/svabra/bit-data-workbench:1.2.3",
                        "",
                    ]
                ),
            )

            errors = versioning.check_repository(repo_root)

            self.assertTrue(
                any(
                    "README.md has README docker build example at 1.2.2, expected 1.2.3."
                    in error
                    for error in errors
                )
            )

    def test_sync_release_surfaces_updates_pinned_release_targets(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            build_repo_fixture(repo_root, version="1.2.2", release_notes_version="1.2.4")

            changed_paths = versioning.sync_release_surfaces("1.2.4", repo_root=repo_root)

            self.assertIn(repo_root / "VERSION", changed_paths)
            self.assertIn(repo_root / "README.md", changed_paths)
            self.assertIn(repo_root / "k8s" / "bdw-deployment.yaml", changed_paths)
            self.assertEqual(versioning.check_repository(repo_root), [])

    def test_version_changed_in_range_only_tracks_version_file_changes(self) -> None:
        with TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            build_repo_fixture(repo_root, version="1.2.3")

            git(repo_root, "init")
            git(repo_root, "config", "user.name", "Version Test")
            git(repo_root, "config", "user.email", "version-test@example.com")
            git(repo_root, "add", ".")
            git(repo_root, "commit", "-m", "initial")
            initial_commit = git(repo_root, "rev-parse", "HEAD")

            write_text(
                repo_root / "README.md",
                "\n".join(
                    [
                        "docker build -f bdw/Dockerfile -t bit-data-workbench:1.2.3 .",
                        "  -e IMAGE_VERSION=1.2.3 ^",
                        "  bit-data-workbench:1.2.3",
                        "docker-hub.nexus.bit.admin.ch/svabra/bit-data-workbench:1.2.3",
                        "",
                        "Additional README context.",
                    ]
                ),
            )
            git(repo_root, "add", "README.md")
            git(repo_root, "commit", "-m", "docs")
            docs_commit = git(repo_root, "rev-parse", "HEAD")

            self.assertFalse(versioning.version_changed_in_range(initial_commit, docs_commit, repo_root=repo_root))

            write_text(repo_root / "VERSION", "1.2.4\n")
            git(repo_root, "add", "VERSION")
            git(repo_root, "commit", "-m", "release")
            release_commit = git(repo_root, "rev-parse", "HEAD")

            self.assertTrue(versioning.version_changed_in_range(docs_commit, release_commit, repo_root=repo_root))
