from __future__ import annotations

import asyncio

from playwright_file_ingestion_smoke_lib import FileSmokeSpec, parse_args, run_smoke


SPEC = FileSmokeSpec(
    ingestor_id="xml",
    label="XML",
    extension=".xml",
    mime_type="application/xml",
    s3_prefix_root="playwright/xml-imports",
)


def main() -> int:
    args = parse_args(
        "Exercise real browser XML ingestion to Shared Workspace S3 and PostgreSQL.",
        SPEC.s3_prefix_root,
    )
    return asyncio.run(run_smoke(args, SPEC))


if __name__ == "__main__":
    raise SystemExit(main())
