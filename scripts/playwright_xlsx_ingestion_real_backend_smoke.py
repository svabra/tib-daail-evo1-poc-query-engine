from __future__ import annotations

import asyncio

from playwright_file_ingestion_smoke_lib import FileSmokeSpec, parse_args, run_smoke


SPEC = FileSmokeSpec(
    ingestor_id="xlsx",
    label="Excel",
    extension=".xlsx",
    mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    s3_prefix_root="playwright/xlsx-imports",
)


def main() -> int:
    args = parse_args(
        "Exercise real browser Excel ingestion to Shared Workspace S3 and PostgreSQL.",
        SPEC.s3_prefix_root,
    )
    return asyncio.run(run_smoke(args, SPEC))


if __name__ == "__main__":
    raise SystemExit(main())
