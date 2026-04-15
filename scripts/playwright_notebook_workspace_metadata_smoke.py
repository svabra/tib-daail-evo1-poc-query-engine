from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from urllib.parse import urljoin
from uuid import uuid4

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


NOTEBOOK_METADATA_STORAGE_KEY = "bdw.notebookMeta.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise notebook workspace summary, tag, and version interactions "
            "in the browser using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


def unique_value(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:8]}"


async def open_query_workbench(page, args: argparse.Namespace) -> None:
    await page.goto(
        urljoin(args.base_url, "query-workbench"),
        wait_until="domcontentloaded",
        timeout=args.timeout_ms,
    )
    await page.locator("[data-sidebar]").wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )


async def create_notebook(page, timeout_ms: int) -> tuple[str, float]:
    previous_notebook_id = await page.evaluate(
        """
        () => document.querySelector("[data-notebook-meta]")?.dataset.notebookId || ""
        """
    )

    notebook_section_summary = page.locator("[data-notebook-section] > summary").first
    await notebook_section_summary.wait_for(state="visible", timeout=timeout_ms)
    await notebook_section_summary.hover()

    create_button = page.locator(
        "[data-notebook-section] > summary [data-create-notebook]"
    ).first
    await create_button.wait_for(state="visible", timeout=timeout_ms)

    started = time.perf_counter()
    await create_button.click(force=True)
    await page.wait_for_function(
        """
        (previousNotebookId) => {
          const meta = document.querySelector("[data-notebook-meta]");
          return Boolean(
            meta &&
            meta.dataset.notebookId &&
            meta.dataset.notebookId !== previousNotebookId &&
            meta.dataset.notebookId.startsWith("local-notebook-")
          );
        }
        """,
        arg=previous_notebook_id,
        timeout=timeout_ms,
    )
    notebook_id = (
        await page.locator("[data-notebook-meta]").first.get_attribute("data-notebook-id")
    ) or ""
    return notebook_id, (time.perf_counter() - started) * 1000


async def notebook_metadata_state(page, notebook_id: str) -> dict[str, object]:
    payload = await page.evaluate(
        """
        ([storageKey, notebookId]) => {
          const raw = window.localStorage.getItem(storageKey);
          if (!raw) {
            return null;
          }

          const parsed = JSON.parse(raw);
          if (!parsed || typeof parsed !== "object") {
            return null;
          }

          const entry = parsed[notebookId];
          return entry && typeof entry === "object" ? entry : null;
        }
        """,
        [NOTEBOOK_METADATA_STORAGE_KEY, notebook_id],
    )
    if not isinstance(payload, dict):
        raise RuntimeError(
            f"The notebook metadata store did not contain {notebook_id}."
        )
    return payload


async def update_summary(
    page,
    notebook_id: str,
    summary: str,
    timeout_ms: int,
) -> float:
    summary_display = page.locator("[data-summary-display]").first
    summary_input = page.locator("[data-summary-input]").first

    started = time.perf_counter()
    await summary_display.click()
    await summary_input.wait_for(state="visible", timeout=timeout_ms)
    await summary_input.fill(summary)
    await summary_input.evaluate("(node) => node.blur()")
    await page.wait_for_function(
        """
        ({ notebookId, summary }) => {
          const meta = document.querySelector("[data-notebook-meta]");
          const display = document.querySelector("[data-summary-display]");
          return Boolean(
            meta &&
            meta.dataset.notebookId === notebookId &&
            display &&
            display.textContent.trim() === summary
          );
        }
        """,
        arg={"notebookId": notebook_id, "summary": summary},
        timeout=timeout_ms,
    )

    stored_state = await notebook_metadata_state(page, notebook_id)
    if str(stored_state.get("summary") or "").strip() != summary:
        raise RuntimeError(
            "Notebook summary was not persisted to local storage."
        )

    return (time.perf_counter() - started) * 1000


async def add_tag(
    page,
    notebook_id: str,
    tag: str,
    timeout_ms: int,
) -> float:
    started = time.perf_counter()
    await page.locator("[data-tag-toggle]").first.click()
    tag_input = page.locator("[data-tag-input]").first
    await tag_input.wait_for(state="visible", timeout=timeout_ms)
    await tag_input.fill(tag)
    await tag_input.press("Enter")

    tag_chip = page.locator(f'[data-tag-remove="{tag}"]').first
    await tag_chip.wait_for(state="visible", timeout=timeout_ms)

    stored_state = await notebook_metadata_state(page, notebook_id)
    stored_tags = stored_state.get("tags") or []
    if tag not in stored_tags:
        raise RuntimeError("Notebook tag was not persisted to local storage.")

    return (time.perf_counter() - started) * 1000


async def save_version(page, timeout_ms: int) -> tuple[str, int, float]:
    version_toggle = page.locator("[data-version-toggle]").first
    await version_toggle.wait_for(state="visible", timeout=timeout_ms)
    await version_toggle.click()

    version_list = page.locator("[data-version-list]").first
    version_items = page.locator("[data-version-load]")
    before_count = await version_items.count()

    started = time.perf_counter()
    await page.locator("[data-save-version]").first.click()
    await page.wait_for_function(
        """
        (beforeCount) => document.querySelectorAll("[data-version-load]").length > beforeCount
        """,
        arg=before_count,
        timeout=timeout_ms,
    )

    saved_version_id = (
        await page.locator("[data-version-load]").first.get_attribute("data-version-id")
    ) or ""
    if not saved_version_id:
        raise RuntimeError("The saved notebook version did not expose a version id.")

    if not await version_list.is_visible():
        raise RuntimeError("The notebook version panel closed unexpectedly after saving.")

    return saved_version_id, before_count + 1, (time.perf_counter() - started) * 1000


async def remove_tag(page, tag: str, timeout_ms: int) -> float:
    tag_chip = page.locator(f'[data-tag-remove="{tag}"]').first
    await tag_chip.wait_for(state="visible", timeout=timeout_ms)

    started = time.perf_counter()
    await tag_chip.click()
    await tag_chip.wait_for(state="detached", timeout=timeout_ms)
    return (time.perf_counter() - started) * 1000


async def load_version_and_assert_restore(
    page,
    notebook_id: str,
    saved_version_id: str,
    expected_summary: str,
    expected_tag: str,
    timeout_ms: int,
) -> float:
    version_button = page.locator(
        f'[data-version-load][data-version-id="{saved_version_id}"]'
    ).first
    await version_button.wait_for(state="visible", timeout=timeout_ms)

    started = time.perf_counter()
    await version_button.click()
    confirm_button = page.locator("[data-confirm-submit]").first
    await confirm_button.wait_for(state="visible", timeout=timeout_ms)
    await confirm_button.click()

    await page.wait_for_function(
        """
        ({ notebookId, summary, tag }) => {
          const meta = document.querySelector("[data-notebook-meta]");
          const display = document.querySelector("[data-summary-display]");
          const tagChip = document.querySelector(`[data-tag-remove="${tag}"]`);
          return Boolean(
            meta &&
            meta.dataset.notebookId === notebookId &&
            display &&
            display.textContent.trim() === summary &&
            tagChip
          );
        }
        """,
        arg={
            "notebookId": notebook_id,
            "summary": expected_summary,
            "tag": expected_tag,
        },
        timeout=timeout_ms,
    )

    stored_state = await notebook_metadata_state(page, notebook_id)
    if str(stored_state.get("summary") or "").strip() != expected_summary:
        raise RuntimeError("Loading the saved version did not restore the notebook summary.")
    if expected_tag not in (stored_state.get("tags") or []):
        raise RuntimeError("Loading the saved version did not restore the notebook tags.")

    return (time.perf_counter() - started) * 1000


async def assert_reload_persists_state(
    page,
    args: argparse.Namespace,
    notebook_id: str,
    expected_summary: str,
    expected_tag: str,
) -> None:
    await page.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)
    await page.wait_for_function(
        """
        (notebookId) => {
          const meta = document.querySelector("[data-notebook-meta]");
          return Boolean(meta && meta.dataset.notebookId === notebookId);
        }
        """,
        arg=notebook_id,
        timeout=args.timeout_ms,
    )

    summary_display = page.locator("[data-summary-display]").first
    await summary_display.wait_for(state="visible", timeout=args.timeout_ms)
    displayed_summary = (await summary_display.inner_text()).strip()
    if displayed_summary != expected_summary:
        raise RuntimeError(
            "Reload did not restore the expected notebook summary."
        )

    await page.locator(f'[data-tag-remove="{expected_tag}"]').first.wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )


async def run_smoke(args: argparse.Namespace) -> int:
    original_summary = unique_value("pw-summary")
    modified_summary = unique_value("pw-summary-updated")
    notebook_tag = unique_value("pw-tag")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1440, "height": 1200})
        console_messages: list[str] = []
        page.on(
            "console",
            lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"),
        )
        page.on(
            "pageerror",
            lambda exc: console_messages.append(f"pageerror:{exc}"),
        )

        try:
            await open_query_workbench(page, args)
            notebook_id, create_ms = await create_notebook(page, args.timeout_ms)
            summary_ms = await update_summary(
                page,
                notebook_id,
                original_summary,
                args.timeout_ms,
            )
            tag_ms = await add_tag(
                page,
                notebook_id,
                notebook_tag,
                args.timeout_ms,
            )
            saved_version_id, expected_version_count, version_save_ms = await save_version(
                page,
                args.timeout_ms,
            )
            mutate_summary_ms = await update_summary(
                page,
                notebook_id,
                modified_summary,
                args.timeout_ms,
            )
            remove_tag_ms = await remove_tag(
                page,
                notebook_tag,
                args.timeout_ms,
            )
            version_load_ms = await load_version_and_assert_restore(
                page,
                notebook_id,
                saved_version_id,
                original_summary,
                notebook_tag,
                args.timeout_ms,
            )
            await assert_reload_persists_state(
                page,
                args,
                notebook_id,
                original_summary,
                notebook_tag,
            )

            version_count = await page.locator("[data-version-load]").count()
            if version_count != expected_version_count:
                raise RuntimeError(
                    "Notebook version count changed unexpectedly after loading a saved version."
                )
        except (PlaywrightTimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(f"Notebook create: {create_ms:.0f} ms")
    print(f"Notebook summary edit: {summary_ms:.0f} ms")
    print(f"Notebook tag add: {tag_ms:.0f} ms")
    print(f"Notebook version save: {version_save_ms:.0f} ms")
    print(f"Notebook summary mutate: {mutate_summary_ms:.0f} ms")
    print(f"Notebook tag remove: {remove_tag_ms:.0f} ms")
    print(f"Notebook version load: {version_load_ms:.0f} ms")
    print("Playwright notebook workspace metadata smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
