from __future__ import annotations

import os
import re
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from urllib.request import urlopen


BASE_URL = "https://esm.sh"
OUTPUT_ROOT = (
    Path(__file__).resolve().parent.parent
    / "bdw"
    / "bit_data_workbench"
    / "static"
    / "vendor"
    / "esmsh"
)
ENTRYPOINTS = [
    "https://esm.sh/codemirror@6.0.1/es2022/codemirror.mjs",
    "https://esm.sh/@codemirror/lang-sql@6.8.0/es2022/lang-sql.mjs",
]
SPEC_RE = re.compile(
    r'(?P<prefix>(?:import|export)\s*(?:[^"\']*?\s*from\s*)?)(?P<quote>["\'])(?P<spec>[^"\']+)(?P=quote)',
    re.DOTALL,
)


def normalize_url(specifier: str) -> str | None:
    if specifier.startswith("https://esm.sh/"):
        target = specifier
    elif specifier.startswith("/"):
        target = BASE_URL + specifier
    else:
        return None

    parsed = urlsplit(target)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def local_rel_path(url: str) -> Path:
    parsed = urlsplit(url)
    raw_path = parsed.path.lstrip("/")
    if not raw_path:
        raw_path = "index"
    path = Path(raw_path)
    query_suffix = ""
    if parsed.query:
        safe_query = re.sub(r"[^A-Za-z0-9._-]+", "_", parsed.query)
        query_suffix = f"__{safe_query}"

    if path.suffix in {".mjs", ".js", ".json"}:
        filename = f"{path.stem}{query_suffix}{path.suffix}"
        return Path(*path.parts[:-1]) / filename

    return Path(f"{raw_path}{query_suffix}.mjs")


def relative_import(from_file: Path, to_file: Path) -> str:
    rel = Path(os.path.relpath(to_file, start=from_file.parent)).as_posix()
    if not rel.startswith("."):
        rel = f"./{rel}"
    return rel


def fetch_text(url: str) -> str:
    with urlopen(url) as response:
        return response.read().decode("utf-8")


def rewrite_module(source_url: str, content: str, queue: list[str]) -> str:
    current_local = OUTPUT_ROOT / local_rel_path(source_url)

    def replace(match: re.Match[str]) -> str:
        spec = match.group("spec")
        normalized = normalize_url(spec)
        if not normalized:
            return match.group(0)
        queue.append(normalized)
        target_local = OUTPUT_ROOT / local_rel_path(normalized)
        rewritten = relative_import(current_local, target_local)
        return f'{match.group("prefix")}{match.group("quote")}{rewritten}{match.group("quote")}'

    return SPEC_RE.sub(replace, content)


def main() -> None:
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    queue = list(ENTRYPOINTS)
    seen: set[str] = set()

    while queue:
        url = queue.pop(0)
        if url in seen:
            continue
        seen.add(url)

        local_path = OUTPUT_ROOT / local_rel_path(url)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        raw = fetch_text(url)
        rewritten = rewrite_module(url, raw, queue)
        local_path.write_text(rewritten, encoding="utf-8")
        print(f"Fetched {url} -> {local_path}")


if __name__ == "__main__":
    main()
