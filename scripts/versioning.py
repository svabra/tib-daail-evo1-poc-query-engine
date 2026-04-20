from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = REPO_ROOT / "VERSION"
RELEASE_NOTES_PATH = Path("bdw/bit_data_workbench/release_notes.py")
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
FIRST_RELEASE_NOTE_VERSION = re.compile(r'"version": "(?P<version>\d+\.\d+\.\d+)"')
RELEASE_NOTES_HEADER_VERSION = re.compile(r"through version (?P<version>\d+\.\d+\.\d+)")
RELEASE_NOTES_HEADER_RULE = re.compile(
    r"(?m)^(?P<prefix># Derived from git history through version )"
    r"(?P<version>\d+\.\d+\.\d+)"
    r"(?P<suffix>\. Keep entries concise and)$"
)
FIRST_RELEASE_NOTE_VERSION_RULE = re.compile(
    r'(?P<prefix>"version": ")(?P<version>\d+\.\d+\.\d+)(?P<suffix>")'
)
DOCKERFILE_IMAGE_ARG = re.compile(r'(?m)^ARG IMAGE_VERSION=(?P<value>.*)$')
COMPOSE_IMAGE_VERSION = re.compile(r'(?m)^\s+IMAGE_VERSION:\s+"?(?P<value>[^"\r\n]+)"?\s*$')


@dataclass(frozen=True)
class VersionRule:
    relative_path: Path
    description: str
    pattern: re.Pattern[str]


RELEASE_PIN_RULES = (
    VersionRule(
        relative_path=Path("README.md"),
        description="README docker build example",
        pattern=re.compile(
            r"(?m)^(?P<prefix>docker build -f bdw/Dockerfile -t bit-data-workbench:)"
            r"(?P<version>\d+\.\d+\.\d+)(?P<suffix> \.)$"
        ),
    ),
    VersionRule(
        relative_path=Path("README.md"),
        description="README docker run IMAGE_VERSION example",
        pattern=re.compile(
            r"(?m)^(?P<prefix>\s+-e IMAGE_VERSION=)(?P<version>\d+\.\d+\.\d+)(?P<suffix> \^)$"
        ),
    ),
    VersionRule(
        relative_path=Path("README.md"),
        description="README docker run image tag",
        pattern=re.compile(
            r"(?m)^(?P<prefix>\s+bit-data-workbench:)(?P<version>\d+\.\d+\.\d+)(?P<suffix>)$"
        ),
    ),
    VersionRule(
        relative_path=Path("README.md"),
        description="README current RHOS image tag",
        pattern=re.compile(
            r"(?m)^(?P<prefix>docker-hub\.nexus\.bit\.admin\.ch/svabra/bit-data-workbench:)"
            r"(?P<version>\d+\.\d+\.\d+)(?P<suffix>)$"
        ),
    ),
    VersionRule(
        relative_path=Path("k8s/bdw-deployment.yaml"),
        description="BDW deployment image tag",
        pattern=re.compile(
            r"(?m)^(?P<prefix>\s+image:\s+docker-hub\.nexus\.bit\.admin\.ch/svabra/bit-data-workbench:)"
            r"(?P<version>\d+\.\d+\.\d+)(?P<suffix>)$"
        ),
    ),
)

RELEASE_NOTE_RULES = (
    VersionRule(
        relative_path=RELEASE_NOTES_PATH,
        description="release notes header",
        pattern=RELEASE_NOTES_HEADER_RULE,
    ),
    VersionRule(
        relative_path=RELEASE_NOTES_PATH,
        description="release notes top version entry",
        pattern=FIRST_RELEASE_NOTE_VERSION_RULE,
    ),
)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def canonical_version(repo_root: Path = REPO_ROOT) -> str:
    return read_text(repo_root / "VERSION").strip()


def validate_semver(version: str) -> None:
    if not SEMVER_PATTERN.fullmatch(version):
        raise ValueError(f"Invalid semantic version {version!r}. Expected X.Y.Z.")


def resolve_rule_path(rule: VersionRule, *, repo_root: Path) -> Path:
    return repo_root / rule.relative_path


def update_rule_version(
    content: str,
    *,
    rule: VersionRule,
    version: str,
    repo_root: Path,
) -> tuple[str, bool]:
    match = rule.pattern.search(content)
    if match is None:
        raise ValueError(
            f"Could not find {rule.description} in {resolve_rule_path(rule, repo_root=repo_root).relative_to(repo_root)}."
        )
    replacement = f"{match.group('prefix')}{version}{match.group('suffix')}"
    updated = rule.pattern.sub(replacement, content, count=1)
    return updated, updated != content


def sync_release_surfaces(version: str, *, repo_root: Path = REPO_ROOT) -> list[Path]:
    validate_semver(version)
    changed_paths: list[Path] = []
    version_file = repo_root / "VERSION"
    if read_text(version_file).strip() != version:
        write_text(version_file, f"{version}\n")
        changed_paths.append(version_file)

    for rule in (*RELEASE_PIN_RULES, *RELEASE_NOTE_RULES):
        rule_path = resolve_rule_path(rule, repo_root=repo_root)
        content = read_text(rule_path)
        updated, changed = update_rule_version(
            content,
            rule=rule,
            version=version,
            repo_root=repo_root,
        )
        if not changed:
            continue
        write_text(rule_path, updated)
        if rule_path not in changed_paths:
            changed_paths.append(rule_path)
    return changed_paths


def version_changed_in_range(
    base_rev: str,
    head_rev: str,
    *,
    repo_root: Path = REPO_ROOT,
) -> bool:
    result = subprocess.run(
        ["git", "diff", "--quiet", base_rev, head_rev, "--", "VERSION"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return False
    if result.returncode == 1:
        return True
    raise RuntimeError(
        f"git diff failed while comparing VERSION between {base_rev} and {head_rev}: "
        f"{(result.stderr or result.stdout).strip()}"
    )


def _first_release_note_version_errors(version: str, *, repo_root: Path) -> list[str]:
    release_notes_path = repo_root / RELEASE_NOTES_PATH
    content = read_text(release_notes_path)
    first_entry = FIRST_RELEASE_NOTE_VERSION.search(content)
    if first_entry is None:
        return [f"{release_notes_path.relative_to(repo_root)} is missing a top release-note version entry."]

    errors: list[str] = []
    first_version = first_entry.group("version")
    if first_version != version:
        errors.append(
            f"{release_notes_path.relative_to(repo_root)} top release-note version is {first_version}, "
            f"expected {version}."
        )

    header_version = RELEASE_NOTES_HEADER_VERSION.search(content)
    if header_version is None:
        errors.append(
            f"{release_notes_path.relative_to(repo_root)} is missing the 'through version X.Y.Z' header."
        )
    elif header_version.group("version") != version:
        errors.append(
            f"{release_notes_path.relative_to(repo_root)} header version is {header_version.group('version')}, "
            f"expected {version}."
        )
    return errors


def _dockerfile_default_errors(version: str, *, repo_root: Path) -> list[str]:
    errors: list[str] = []
    for relative_path in (Path("Dockerfile"), Path("bdw/Dockerfile")):
        path = repo_root / relative_path
        match = DOCKERFILE_IMAGE_ARG.search(read_text(path))
        if match is None:
            errors.append(f"{relative_path.as_posix()} is missing ARG IMAGE_VERSION.")
            continue
        raw_value = match.group("value").strip().strip('"').strip("'")
        if SEMVER_PATTERN.fullmatch(raw_value):
            errors.append(
                f"{relative_path.as_posix()} still hardcodes release version {raw_value}; "
                "Dockerfiles must not be an independent version source."
            )
        if raw_value == version:
            errors.append(
                f"{relative_path.as_posix()} still embeds the canonical release version {version}."
            )
    return errors


def _compose_version_errors(*, repo_root: Path) -> list[str]:
    compose_path = repo_root / "compose.yaml"
    match = COMPOSE_IMAGE_VERSION.search(read_text(compose_path))
    if match is None:
        return []
    return [
        f"compose.yaml still sets IMAGE_VERSION={match.group('value')}; "
        "dev compose must not pin the release version independently."
    ]


def _test_fixture_errors(version: str, *, repo_root: Path) -> list[str]:
    errors: list[str] = []
    for path in sorted((repo_root / "tests").rglob("*.py")):
        content = read_text(path)
        if version in content:
            errors.append(
                f"{path.relative_to(repo_root)} still hardcodes the canonical release version {version}."
            )
    return errors


def check_repository(repo_root: Path = REPO_ROOT) -> list[str]:
    errors: list[str] = []
    version = canonical_version(repo_root)
    try:
        validate_semver(version)
    except ValueError as exc:
        errors.append(str(exc))
        return errors

    for rule in RELEASE_PIN_RULES:
        rule_path = resolve_rule_path(rule, repo_root=repo_root)
        content = read_text(rule_path)
        matches = list(rule.pattern.finditer(content))
        if not matches:
            errors.append(
                f"{rule_path.relative_to(repo_root)} is missing the tracked version surface: {rule.description}."
            )
            continue
        if len(matches) > 1:
            errors.append(
                f"{rule_path.relative_to(repo_root)} matches {rule.description} more than once; "
                "version sync rules are ambiguous."
            )
            continue
        found_version = matches[0].group("version")
        if found_version != version:
            errors.append(
                f"{rule_path.relative_to(repo_root)} has {rule.description} at {found_version}, expected {version}."
            )

    errors.extend(_first_release_note_version_errors(version, repo_root=repo_root))
    errors.extend(_dockerfile_default_errors(version, repo_root=repo_root))
    errors.extend(_compose_version_errors(repo_root=repo_root))
    errors.extend(_test_fixture_errors(version, repo_root=repo_root))
    return errors


def run_check() -> int:
    errors = check_repository(REPO_ROOT)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    print(f"Version consistency check passed for {canonical_version(REPO_ROOT)}.")
    return 0


def run_bump(version: str) -> int:
    sync_release_surfaces(version, repo_root=REPO_ROOT)
    return run_check()


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Maintain and validate repo version surfaces.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bump_parser = subparsers.add_parser("bump", help="Rewrite tracked release surfaces to a semver.")
    bump_parser.add_argument("version", help="Semantic version in X.Y.Z format.")

    subparsers.add_parser("check", help="Validate that tracked version surfaces match VERSION.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if args.command == "bump":
        return run_bump(args.version)
    if args.command == "check":
        return run_check()
    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
