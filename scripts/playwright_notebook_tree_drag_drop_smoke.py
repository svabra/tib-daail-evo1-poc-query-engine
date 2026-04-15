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
            "Exercise notebook drag-and-drop moves in the sidebar "
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
    is_open = await folder.evaluate("node => node.hasAttribute('open')")
    if not is_open:
        await folder.evaluate(
            """
            (node) => {
                node.open = true;
                node.setAttribute('open', '');
            }
            """
        )


async def notebook_tree_state(page) -> list[dict]:
    raw_value = await page.evaluate(
        """([key]) => window.localStorage.getItem(key)""",
        [NOTEBOOK_TREE_STORAGE_KEY],
    )
    if not raw_value:
        return []

    parsed = json.loads(raw_value)
    return parsed if isinstance(parsed, list) else []


def folder_contains_notebook(
    nodes: list[dict],
    folder_name: str,
    notebook_id: str,
) -> bool:
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("type") != "folder":
            continue

        if str(node.get("name") or "").strip() == folder_name:
            return any(
                isinstance(child, dict)
                and child.get("type") == "notebook"
                and child.get("notebookId") == notebook_id
                for child in node.get("children") or []
            )

        if folder_contains_notebook(
            node.get("children") or [],
            folder_name,
            notebook_id,
        ):
            return True

    return False


async def create_root_folder(page, folder_name: str, timeout_ms: int) -> float:
    await ensure_sidebar_expanded(page)
    await ensure_details_open(page, "[data-notebook-section]")
    section = page.locator("[data-notebook-section]")
    summary = section.locator(":scope > summary")
    await summary.hover()
    await summary.locator("[data-add-tree-item]").click()
    await page.locator("[data-folder-name-input]").fill(folder_name)
    started = time.perf_counter()
    await page.locator("[data-folder-name-submit]").click()
    await wait_for_notebook_folder(page, folder_name, timeout_ms)
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


async def drag_notebook_to_folder(
    page,
    notebook_id: str,
    source_folder_name: str,
    destination_folder_name: str,
    timeout_ms: int,
) -> float:
    source_folder = await wait_for_notebook_folder(
        page,
        source_folder_name,
        timeout_ms,
    )
    destination_folder = await wait_for_notebook_folder(
        page,
        destination_folder_name,
        timeout_ms,
    )
    await ensure_folder_open(source_folder)
    await ensure_folder_open(destination_folder)

    source_notebook = source_folder.locator(
        f'[data-draggable-notebook][data-notebook-id="{notebook_id}"]'
    )
    destination_children = destination_folder.locator(
        ":scope > [data-tree-children]"
    )
    await source_notebook.wait_for(state="visible", timeout=timeout_ms)
    await destination_children.wait_for(state="attached", timeout=timeout_ms)

    started = time.perf_counter()
    await page.evaluate(
        """
        ({ notebookId, destinationFolderName }) => {
            const source = document.querySelector(
                `[data-draggable-notebook][data-notebook-id="${notebookId}"]`
            );
            const destination = Array.from(
                document.querySelectorAll('[data-tree-folder]')
            ).find((folder) => {
                const label = folder.querySelector(
                    ':scope > summary .tree-folder-label'
                );
                return label?.textContent?.trim() === destinationFolderName;
            });
            const destinationChildren = destination?.querySelector(
                ':scope > [data-tree-children]'
            );
            if (!(source instanceof HTMLElement)) {
                throw new Error('Source notebook element could not be found.');
            }
            if (!(destination instanceof HTMLDetailsElement)) {
                throw new Error(
                    'Destination folder element could not be found.'
                );
            }
            if (!(destinationChildren instanceof HTMLElement)) {
                throw new Error(
                    'Destination folder container could not be found.'
                );
            }

            destination.open = true;
            destination.setAttribute('open', '');

            const dataTransfer = new DataTransfer();
            source.dispatchEvent(
                new DragEvent('dragstart', {
                    bubbles: true,
                    cancelable: true,
                    dataTransfer,
                })
            );
            destinationChildren.dispatchEvent(
                new DragEvent('dragover', {
                    bubbles: true,
                    cancelable: true,
                    dataTransfer,
                })
            );
            destinationChildren.dispatchEvent(
                new DragEvent('drop', {
                    bubbles: true,
                    cancelable: true,
                    dataTransfer,
                })
            );
            source.dispatchEvent(
                new DragEvent('dragend', {
                    bubbles: true,
                    cancelable: true,
                    dataTransfer,
                })
            );
        }
        """,
        {
            "notebookId": notebook_id,
            "destinationFolderName": destination_folder_name,
        },
    )
    await source_folder.locator(
        f'[data-draggable-notebook][data-notebook-id="{notebook_id}"]'
    ).wait_for(state="detached", timeout=timeout_ms)
    await destination_folder.locator(
        f'[data-draggable-notebook][data-notebook-id="{notebook_id}"]'
    ).wait_for(state="attached", timeout=timeout_ms)
    return (time.perf_counter() - started) * 1000


async def run_smoke(args: argparse.Namespace) -> int:
    source_folder_name = unique_name("pw-nb-move-src")
    destination_folder_name = unique_name("pw-nb-move-dst")

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

            create_source_ms = await create_root_folder(
                page,
                source_folder_name,
                args.timeout_ms,
            )
            create_destination_ms = await create_root_folder(
                page,
                destination_folder_name,
                args.timeout_ms,
            )
            notebook_id, create_notebook_ms = await create_notebook_in_folder(
                page,
                source_folder_name,
                args.timeout_ms,
            )

            move_ms = await drag_notebook_to_folder(
                page,
                notebook_id,
                source_folder_name,
                destination_folder_name,
                args.timeout_ms,
            )

            state = await notebook_tree_state(page)
            if folder_contains_notebook(
                state,
                source_folder_name,
                notebook_id,
            ):
                raise RuntimeError(
                    "Dragged notebook is still recorded under the source "
                    "folder."
                )
            if not folder_contains_notebook(
                state,
                destination_folder_name,
                notebook_id,
            ):
                raise RuntimeError(
                    "Dragged notebook was not recorded under the "
                    "destination folder."
                )

            await page.reload(
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            source_folder = await wait_for_notebook_folder(
                page,
                source_folder_name,
                args.timeout_ms,
            )
            destination_folder = await wait_for_notebook_folder(
                page,
                destination_folder_name,
                args.timeout_ms,
            )
            await ensure_folder_open(source_folder)
            await ensure_folder_open(destination_folder)
            await source_folder.locator(
                f'[data-draggable-notebook][data-notebook-id="{notebook_id}"]'
            ).wait_for(state="detached", timeout=args.timeout_ms)
            await destination_folder.locator(
                f'[data-draggable-notebook][data-notebook-id="{notebook_id}"]'
            ).wait_for(state="attached", timeout=args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(f"Notebook move create source folder: {create_source_ms:.0f} ms")
    print(
        "Notebook move create destination folder: "
        f"{create_destination_ms:.0f} ms"
    )
    print(f"Notebook move create notebook: {create_notebook_ms:.0f} ms")
    print(f"Notebook move drag and drop: {move_ms:.0f} ms")
    print("Playwright notebook drag-and-drop smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
