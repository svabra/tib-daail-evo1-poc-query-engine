from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime, timedelta
import uuid

from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise Query Workbench recent-first landing and blank notebook reuse."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


def shared_notebook_payload(notebook_id: str, title: str, summary: str) -> dict[str, object]:
    created_at = datetime.now(UTC).isoformat()
    cell = {
        "cellId": f"cell-{notebook_id}",
        "sql": "select 1 as recent_activity_smoke",
        "language": "sql",
        "dataSources": [],
    }
    return {
        "notebookId": notebook_id,
        "title": title,
        "summary": summary,
        "tags": ["playwright", "recent-activity"],
        "treePath": ["Shared Notebooks"],
        "linkedGeneratorId": "",
        "createdAt": created_at,
        "cells": [cell],
        "versions": [
            {
                "versionId": f"initial-{notebook_id}",
                "createdAt": created_at,
                "title": title,
                "summary": summary,
                "tags": ["playwright", "recent-activity"],
                "cells": [cell],
            }
        ],
    }


async def open_query_workbench_entry(page, base_url: str, timeout_ms: int) -> None:
    await page.evaluate("window.localStorage.removeItem('bdw.lastNotebook.v1')")
    await page.goto(
        f"{base_url.rstrip('/')}/query-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-query-workbench-entry-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def create_shared_notebook(page, notebook_id: str, title: str, summary: str, client_id: str) -> None:
    result = await page.evaluate(
        """
        async ({ payload, clientId }) => {
          const response = await fetch("/api/notebooks/shared", {
            method: "POST",
            headers: {
              "Accept": "application/json",
              "Content-Type": "application/json",
              "X-Workbench-Client-Id": clientId,
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
        {
            "payload": shared_notebook_payload(notebook_id, title, summary),
            "clientId": client_id,
        },
    )
    if not result["ok"]:
        raise RuntimeError(
            f"Failed to create shared notebook {notebook_id}: {result['status']} {result['text']}"
        )


async def touch_shared_notebook(page, notebook_id: str, action: str, client_id: str) -> None:
    result = await page.evaluate(
        """
        async ({ notebookId, action, clientId }) => {
          const response = await fetch("/api/notebook-activity/touch", {
            method: "POST",
            headers: {
              "Accept": "application/json",
              "Content-Type": "application/json",
              "X-Workbench-Client-Id": clientId,
            },
            body: JSON.stringify({ notebookId, action }),
          });
          return {
            ok: response.ok,
            status: response.status,
            payload: await response.json().catch(() => ({})),
          };
        }
        """,
        {"notebookId": notebook_id, "action": action, "clientId": client_id},
    )
    if not result["ok"] or result["payload"].get("available") is False:
        raise RuntimeError(
            f"Failed to record shared notebook activity: {result['status']} {result['payload']}"
        )


async def local_notebook_count(page) -> int:
    return await page.evaluate(
        """
        () => {
          const raw = window.localStorage.getItem("bdw.notebookMeta.v1");
          if (!raw) {
            return 0;
          }
          const parsed = JSON.parse(raw);
          return Object.entries(parsed)
            .filter(([key, value]) => key.startsWith("local-notebook-") && !value?.deleted)
            .length;
        }
        """
    )


async def visible_notebook_id(page) -> str:
    return await page.locator("[data-workspace-notebook]").first.get_attribute("data-notebook-id") or ""


async def main() -> None:
    args = parse_args()
    unique_id = uuid.uuid4().hex[:10]
    client_a = f"playwright-recent-a-{unique_id}"
    client_b = f"playwright-recent-b-{unique_id}"
    shared_ids = [f"shared-notebook-recent-smoke-{unique_id}-{index}" for index in range(6)]
    shared_title = f"Shared Activity Smoke {unique_id}"
    private_title = f"Private Local Activity Smoke {unique_id}"
    console_messages: list[str] = []
    page_errors: list[str] = []

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context_a = await browser.new_context()
        context_b = await browser.new_context()
        await context_a.add_init_script(
            f"window.localStorage.setItem('bdw.clientId.v1', '{client_a}');"
        )
        await context_b.add_init_script(
            f"window.localStorage.setItem('bdw.clientId.v1', '{client_b}');"
        )
        page_a = await context_a.new_page()
        page_b = await context_b.new_page()
        page_b.on("console", lambda message: console_messages.append(f"{message.type}: {message.text}"))
        page_b.on("pageerror", lambda error: page_errors.append(str(error)))
        try:
            await page_a.goto(args.base_url.rstrip("/"), wait_until="domcontentloaded", timeout=args.timeout_ms)
            for index, notebook_id in enumerate(shared_ids):
                await create_shared_notebook(
                    page_a,
                    notebook_id,
                    f"{shared_title} {index + 1}",
                    f"Shared activity smoke summary {index + 1}",
                    client_a,
                )
            await touch_shared_notebook(page_a, shared_ids[-1], "open", client_a)

            await open_query_workbench_entry(page_b, args.base_url, args.timeout_ms)
            base_time = datetime.now(UTC) - timedelta(minutes=len(shared_ids))
            activity = {
                notebook_id: {
                    "notebookId": notebook_id,
                    "title": f"My Recent Smoke Notebook {index + 1}",
                    "summary": f"My latest notebook summary {index + 1}",
                    "touchedAt": (base_time + timedelta(minutes=index)).isoformat(),
                    "reason": "run" if index % 2 else "open",
                }
                for index, notebook_id in enumerate(shared_ids)
            }
            activity[f"local-notebook-private-{unique_id}"] = {
                "notebookId": f"local-notebook-private-{unique_id}",
                "title": private_title,
                "summary": "Private activity must stay local.",
                "touchedAt": datetime.now(UTC).isoformat(),
                "reason": "open",
            }
            expected_my_titles = [
                activity[notebook_id]["title"] for notebook_id in reversed(shared_ids[-5:])
            ]
            await page_b.evaluate(
                """
                (activity) => {
                  window.localStorage.setItem("bdw.notebookActivity.v1", JSON.stringify(activity));
                  window.localStorage.removeItem("bdw.lastNotebook.v1");
                }
                """,
                activity,
            )
            await page_b.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)
            await page_b.locator("[data-query-workbench-entry-page]").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page_b.wait_for_function(
                """
                (expectedTitles) => {
                  const items = Array.from(document.querySelectorAll(
                    '[data-query-entry-my-notebooks] [data-open-recent-notebook]'
                  ));
                  const titles = items.map((item) =>
                    item.querySelector('.query-entry-activity-title')?.textContent?.trim()
                  );
                  return items.length === expectedTitles.length
                    && titles.every((title, index) => title === expectedTitles[index]);
                }
                """,
                arg=expected_my_titles,
                timeout=args.timeout_ms,
            )
            my_latest_text = await page_b.locator("[data-query-entry-my-notebooks]").inner_text()
            if activity[shared_ids[0]]["title"] in my_latest_text:
                raise RuntimeError("My Latest Notebooks rendered more than five entries.")

            await page_b.locator("[data-query-entry-shared-activity]").get_by_text(
                f"{shared_title} 6"
            ).wait_for(state="visible", timeout=args.timeout_ms)
            shared_activity_text = await page_b.locator("[data-query-entry-shared-activity]").inner_text()
            if "Opened by another browser" not in shared_activity_text:
                raise RuntimeError("Shared Activity did not show the anonymous actor label.")
            if private_title in shared_activity_text:
                raise RuntimeError("Private/local notebook activity leaked into Shared Activity.")

            before_second_create_count = await local_notebook_count(page_b)
            await page_b.locator("[data-query-workbench-entry-page] [data-create-notebook]").click()
            await page_b.locator("[data-workspace-notebook]").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            first_blank_notebook_id = await visible_notebook_id(page_b)
            await page_b.evaluate("window.localStorage.removeItem('bdw.lastNotebook.v1')")
            await open_query_workbench_entry(page_b, args.base_url, args.timeout_ms)
            before_reuse_count = await local_notebook_count(page_b)
            await page_b.locator("[data-query-workbench-entry-page] [data-create-notebook]").click()
            await page_b.locator("[data-workspace-notebook]").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            second_blank_notebook_id = await visible_notebook_id(page_b)
            after_reuse_count = await local_notebook_count(page_b)
            if first_blank_notebook_id != second_blank_notebook_id:
                raise RuntimeError(
                    "Create New Workbench did not reuse the existing untouched blank notebook."
                )
            if after_reuse_count != before_reuse_count:
                raise RuntimeError(
                    "Reusing an untouched blank notebook changed the local notebook count."
                )
            if before_reuse_count < before_second_create_count:
                raise RuntimeError("Local notebook count moved backwards during blank reuse smoke.")
        except Exception:
            if page_errors:
                print("Page errors:")
                for error in page_errors:
                    print(f"  {error}")
            if console_messages:
                print("Recent console messages:")
                for message in console_messages[-25:]:
                    print(f"  {message}")
            raise
        finally:
            await context_a.close()
            await context_b.close()
            await browser.close()

    print("Playwright Query Workbench recent activity smoke passed.")


if __name__ == "__main__":
    asyncio.run(main())
