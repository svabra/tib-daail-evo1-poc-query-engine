from __future__ import annotations

import argparse
import asyncio
import json
from urllib.parse import urljoin

from playwright.async_api import async_playwright


COMMENTED_UNION_SQL = "\n".join(
    [
        'SELECT * FROM s3.kbpoimports."KBPO_2018undvorher.parquet"',
        "UNION ALL",
        'SELECT * FROM s3.kbpoimports."KBPO_2019.parquet"',
        "UNION ALL",
        '--SELECT * FROM s3.kbpoimports."kbpo2020.parquet"',
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify pipeline DuckDB SQL preview wraps commented stage SQL in a "
            "safe multiline COPY TO parquet statement."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    request_payload = {
        "sql": COMMENTED_UNION_SQL,
        "displaySql": COMMENTED_UNION_SQL,
        "notebookId": "playwright-pipeline-copy-comment",
        "notebookTitle": "Playwright Pipeline COPY Comment Regression",
        "cellId": "cell-stage-commented-union",
        "dataSources": ["s3"],
        "localRelations": {},
        "queryOptions": {"validation": {"sourceExistence": "off"}},
        "stage": {
            "enabled": True,
            "stageId": "stage-merge-all",
            "alias": "merge all",
            "materialize": True,
            "outputFileName": "merge_all.parquet",
        },
    }

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context()
        page = await context.new_page()
        try:
            await page.goto(
                urljoin(args.base_url, "query-workbench"),
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            response = await page.evaluate(
                """
                async (payload) => {
                  const response = await fetch("/api/query-sql/prepare", {
                    method: "POST",
                    headers: {
                      Accept: "application/json",
                      "Content-Type": "application/json",
                    },
                    body: JSON.stringify(payload),
                  });
                  return {
                    ok: response.ok,
                    status: response.status,
                    text: await response.text(),
                  };
                }
                """,
                request_payload,
            )
            if not response["ok"]:
                raise RuntimeError(
                    f"Prepare request failed: {response['status']} {response['text']}"
                )
            payload = json.loads(response["text"])
            execution_sql = str(payload.get("executionSql") or "")
            expected_start = (
                "COPY (\n"
                "SELECT * FROM read_parquet('s3://kbpoimports/KBPO_2018undvorher.parquet')"
            )
            expected_comment = '--SELECT * FROM s3.kbpoimports."kbpo2020.parquet"\n)\nTO '
            if not execution_sql.startswith(expected_start):
                raise RuntimeError(f"Prepared SQL did not start with safe COPY wrapper:\n{execution_sql}")
            if expected_comment not in execution_sql:
                raise RuntimeError(
                    "Prepared SQL did not put the closing parenthesis and TO after the line comment:\n"
                    f"{execution_sql}"
                )
            if 'kbpo2020.parquet") TO ' in execution_sql:
                raise RuntimeError(f"Prepared SQL still comments out the COPY TO suffix:\n{execution_sql}")
            if payload.get("duckdbExecutionPath") != "isolated-write":
                raise RuntimeError(f"Unexpected DuckDB execution path: {payload}")
            if payload.get("stageOutputFileName") != "merge_all.parquet":
                raise RuntimeError(f"Unexpected stage output filename: {payload}")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
