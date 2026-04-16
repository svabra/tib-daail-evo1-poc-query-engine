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


NOTEBOOK_TREE_STORAGE_KEY = "bdw.notebookTree.v2"
SIDEBAR_COLLAPSED_KEY = "bdw.sidebarCollapsed.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise notebook folder create/rename/delete flows "
            "in the sidebar "
            "using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


def unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:8]}"


async def ensure_details_open(page, selector: str) -> None:
    locator = page.locator(selector)
    await locator.wait_for(state="attached")
    is_open = await locator.evaluate("node => node.hasAttribute('open')")
    if not is_open:
        await locator.locator(":scope > summary").click()


async def ensure_sidebar_expanded(page) -> None:
    toggle = page.locator("[data-sidebar-toggle]").first
    await toggle.wait_for(state="visible")
    expanded = await toggle.get_attribute("aria-expanded")
    if expanded == "false":
        await toggle.click()


def notebook_folder(page, folder_name: str):
    return page.locator(
        "xpath=//details[@data-tree-folder]"
        "[./summary//span["
        "contains(@class, 'tree-folder-label') and "
        f"normalize-space()=\"{folder_name}\"]]"
    ).first


async def wait_for_notebook_folder(page, folder_name: str, timeout_ms: int):
    await ensure_sidebar_expanded(page)
    await ensure_details_open(page, "[data-notebook-section]")
    folder = notebook_folder(page, folder_name)
    await folder.wait_for(state="attached", timeout=timeout_ms)
    return folder


async def ensure_folder_open(folder) -> None:
    if not await folder.evaluate("node => node.hasAttribute('open')"):
        await folder.locator(":scope > summary").click()


async def folder_is_open(folder) -> bool:
    return bool(await folder.evaluate("node => node.hasAttribute('open')"))


async def notebook_tree_state(page) -> list[dict]:
    raw_value = await page.evaluate(
        """([key]) => window.localStorage.getItem(key)""",
        [NOTEBOOK_TREE_STORAGE_KEY],
    )
    if not raw_value:
        return []
    parsed = json.loads(raw_value)
    return parsed if isinstance(parsed, list) else []


def tree_contains_folder(nodes: list[dict], folder_name: str) -> bool:
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("type") == "folder":
            if str(node.get("name") or "").strip() == folder_name:
                return True
            if tree_contains_folder(node.get("children") or [], folder_name):
                return True
    return False


def tree_contains_notebook(nodes: list[dict], notebook_id: str) -> bool:
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if (
            node.get("type") == "notebook"
            and node.get("notebookId") == notebook_id
        ):
            return True
        if node.get("type") == "folder" and tree_contains_notebook(
            node.get("children") or [], notebook_id
        ):
            return True
    return False


async def create_root_folder(page, folder_name: str, timeout_ms: int) -> float:
    await ensure_sidebar_expanded(page)
    await ensure_details_open(page, "[data-notebook-section]")
    section = page.locator("[data-notebook-section]")
    summary = section.locator(":scope > summary")
    await summary.hover()
    await summary.locator("[data-add-tree-item]").click(force=True)
    await page.locator("[data-folder-name-input]").fill(folder_name)
    started = time.perf_counter()
    await page.locator("[data-folder-name-submit]").click()
    await wait_for_notebook_folder(page, folder_name, timeout_ms)
    return (time.perf_counter() - started) * 1000


async def rename_folder(
    page,
    current_name: str,
    new_name: str,
    timeout_ms: int,
) -> float:
    folder = await wait_for_notebook_folder(page, current_name, timeout_ms)
    summary = folder.locator(":scope > summary")
    await summary.hover()
    await summary.locator("[data-rename-tree-folder]").click()
    await page.locator("[data-folder-name-input]").fill(new_name)
    started = time.perf_counter()
    await page.locator("[data-folder-name-submit]").click()
    await wait_for_notebook_folder(page, new_name, timeout_ms)
    await notebook_folder(page, current_name).wait_for(
        state="detached",
        timeout=timeout_ms,
    )
    return (time.perf_counter() - started) * 1000


async def create_notebook_in_folder(
    page,
    folder_name: str,
    timeout_ms: int,
) -> tuple[str, float]:
    folder = await wait_for_notebook_folder(page, folder_name, timeout_ms)
    await ensure_folder_open(folder)
    summary = folder.locator(":scope > summary")
    await summary.hover()
    previous_notebook_id = await page.evaluate(
        """
        () => {
            const meta = document.querySelector('[data-notebook-meta]');
            return meta?.dataset.notebookId || '';
        }
        """
    )

    started = time.perf_counter()
    await summary.locator("[data-create-notebook]").click()
    await page.wait_for_function(
        """
        (previousNotebookId) => {
          const meta = document.querySelector('[data-notebook-meta]');
          return Boolean(
            meta &&
            meta.dataset.notebookId &&
            meta.dataset.notebookId !== previousNotebookId &&
            meta.dataset.notebookId.startsWith('local-notebook-')
          );
        }
        """,
        arg=previous_notebook_id,
        timeout=timeout_ms,
    )

    notebook_id = await page.evaluate(
        """
        () => {
            const meta = document.querySelector('[data-notebook-meta]');
            return meta?.dataset.notebookId || '';
        }
        """
    )
    if not notebook_id.startswith("local-notebook-"):
        raise RuntimeError(
            "Folder notebook creation did not select a local notebook."
        )

    notebook_link = folder.locator(
        f'[data-draggable-notebook][data-notebook-id="{notebook_id}"]'
    )
    await notebook_link.wait_for(state="attached", timeout=timeout_ms)
    return notebook_id, (time.perf_counter() - started) * 1000


async def delete_folder_recursive(
    page,
    folder_name: str,
    timeout_ms: int,
) -> float:
    folder = await wait_for_notebook_folder(page, folder_name, timeout_ms)
    summary = folder.locator(":scope > summary")
    await summary.hover()
    await summary.locator("[data-delete-tree-folder]").click()
    option = page.locator("[data-confirm-option-input]")
    await option.wait_for(state="visible", timeout=timeout_ms)
    await option.check()

    started = time.perf_counter()
    await page.locator("[data-confirm-submit]").click()
    await notebook_folder(page, folder_name).wait_for(
        state="detached",
        timeout=timeout_ms,
    )
    return (time.perf_counter() - started) * 1000


async def run_smoke(args: argparse.Namespace) -> int:
    root_folder = unique_name("pw-nb-folder")
    renamed_folder = f"{root_folder}-renamed"
    sibling_folder = unique_name("pw-nb-sibling")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1440, "height": 1200})
        console_messages: list[str] = []
        page.on(
            "console",
            lambda msg: console_messages.append(
                f"console:{msg.type}:{msg.text}"
            ),
        )
        page.on(
            "pageerror",
            lambda exc: console_messages.append(f"pageerror:{exc}"),
        )

        try:
            await page.add_init_script(
                script=(
                    "window.localStorage.setItem("
                    f'"{SIDEBAR_COLLAPSED_KEY}", '
                    '"false");'
                )
            )
            await page.goto(
                urljoin(args.base_url, "query-workbench"),
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )

            create_ms = await create_root_folder(
                page,
                root_folder,
                args.timeout_ms,
            )
            state = await notebook_tree_state(page)
            if not tree_contains_folder(state, root_folder):
                raise RuntimeError(
                    "Notebook folder was not persisted after creation: "
                    f"{root_folder}"
                )

            rename_ms = await rename_folder(
                page,
                root_folder,
                renamed_folder,
                args.timeout_ms,
            )
            state = await notebook_tree_state(page)
            if (
                tree_contains_folder(state, root_folder)
                or not tree_contains_folder(state, renamed_folder)
            ):
                raise RuntimeError(
                    "Notebook folder rename was not persisted: "
                    f"{renamed_folder}"
                )

            sibling_create_ms = await create_root_folder(
                page,
                sibling_folder,
                args.timeout_ms,
            )
            state = await notebook_tree_state(page)
            if not tree_contains_folder(state, sibling_folder):
                raise RuntimeError(
                    "Sibling notebook folder was not persisted after creation: "
                    f"{sibling_folder}"
                )

            notebook_id, create_notebook_ms = await create_notebook_in_folder(
                page,
                renamed_folder,
                args.timeout_ms,
            )
            state = await notebook_tree_state(page)
            if not tree_contains_notebook(state, notebook_id):
                raise RuntimeError(
                    "Created notebook was not persisted in the notebook tree: "
                    f"{notebook_id}"
                )

            sibling = await wait_for_notebook_folder(
                page,
                sibling_folder,
                args.timeout_ms,
            )
            if await folder_is_open(sibling):
                raise RuntimeError(
                    "Opening a notebook left an unrelated notebook-tree branch open."
                )

            await page.reload(
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            folder = await wait_for_notebook_folder(
                page,
                renamed_folder,
                args.timeout_ms,
            )
            if not await folder_is_open(folder):
                raise RuntimeError(
                    "The active notebook branch was not reopened after reload."
                )
            await folder.locator(
                f'[data-draggable-notebook][data-notebook-id="{notebook_id}"]'
            ).wait_for(
                state="attached",
                timeout=args.timeout_ms,
            )
            sibling = await wait_for_notebook_folder(
                page,
                sibling_folder,
                args.timeout_ms,
            )
            if await folder_is_open(sibling):
                raise RuntimeError(
                    "Reloading the active notebook reopened an unrelated notebook-tree branch."
                )

            delete_ms = await delete_folder_recursive(
                page,
                renamed_folder,
                args.timeout_ms,
            )
            state = await notebook_tree_state(page)
            if tree_contains_folder(state, renamed_folder):
                raise RuntimeError(
                    "Deleted notebook folder is still present in tree state: "
                    f"{renamed_folder}"
                )
            if tree_contains_notebook(state, notebook_id):
                raise RuntimeError(
                    "Deleted folder notebook is still present in tree state: "
                    f"{notebook_id}"
                )

            await page.reload(
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            await ensure_sidebar_expanded(page)
            await ensure_details_open(page, "[data-notebook-section]")
            await notebook_folder(page, renamed_folder).wait_for(
                state="detached",
                timeout=args.timeout_ms,
            )
            await page.locator(
                f'[data-draggable-notebook][data-notebook-id="{notebook_id}"]'
            ).wait_for(
                state="detached",
                timeout=args.timeout_ms,
            )
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(f"Notebook folder create: {create_ms:.0f} ms")
    print(f"Notebook folder rename: {rename_ms:.0f} ms")
    print(f"Sibling notebook folder create: {sibling_create_ms:.0f} ms")
    print(f"Notebook create in folder: {create_notebook_ms:.0f} ms")
    print(f"Notebook folder recursive delete: {delete_ms:.0f} ms")
    print("Playwright notebook folder smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
