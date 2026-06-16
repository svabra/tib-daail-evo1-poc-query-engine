from __future__ import annotations

import asyncio

from playwright_file_ingestion_smoke_lib import FileSmokeSpec, parse_args, run_smoke


SPEC = FileSmokeSpec(
    ingestor_id="parquet",
    label="Parquet",
    extension=".parquet",
    mime_type="application/vnd.apache.parquet",
    s3_prefix_root="playwright/parquet-imports",
)


def main() -> int:
    args = parse_args(
        "Exercise real browser Parquet ingestion to S3 Object Storage and PostgreSQL.",
        SPEC.s3_prefix_root,
    )
    return asyncio.run(run_smoke(args, SPEC))


if __name__ == "__main__":
    raise SystemExit(main())
