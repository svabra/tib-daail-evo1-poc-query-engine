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


LOCAL_WORKSPACE_FOLDERS_KEY = "bdw.localWorkspaceFolders.v1"
SIDEBAR_COLLAPSED_KEY = "bdw.sidebarCollapsed.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise Local Workspace folder create/delete in the sidebar "
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
    if not await locator.evaluate("node => node.hasAttribute('open')"):
        await locator.locator(":scope > summary").click()


async def ensure_sidebar_expanded(page) -> None:
    toggle = page.locator("[data-sidebar-toggle]").first
    await toggle.wait_for(state="visible")
    expanded = await toggle.get_attribute("aria-expanded")
    if expanded == "false":
        await toggle.click()


def local_workspace_catalog(page):
    return page.locator('[data-source-catalog-source-id="workspace.local"]')


async def wait_for_local_workspace_folder(page, folder_path: str, timeout_ms: int):
    await ensure_sidebar_expanded(page)
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(page, '[data-source-catalog-source-id="workspace.local"]')
    folder = page.locator(
        f'[data-local-workspace-folder-node][data-local-workspace-folder-path="{folder_path}"]'
    )
    await folder.wait_for(state="visible", timeout=timeout_ms)
    return folder


async def local_workspace_folders_in_storage(page) -> list[str]:
    raw_value = await page.evaluate(
        """([key]) => window.localStorage.getItem(key)""",
        [LOCAL_WORKSPACE_FOLDERS_KEY],
    )
    if not raw_value:
        return []
    parsed = json.loads(raw_value)
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


async def create_root_folder(page, folder_name: str, timeout_ms: int) -> float:
    await ensure_sidebar_expanded(page)
    await ensure_details_open(page, "[data-data-sources-section]")
    catalog = local_workspace_catalog(page)
    await ensure_details_open(page, '[data-source-catalog-source-id="workspace.local"]')
    root_summary = page.locator(
        '[data-source-schema][data-source-schema-key="workspace_local::saved-results"] > summary'
    )
    if await root_summary.count():
        if await root_summary.first.is_visible():
            raise RuntimeError("The synthetic Saved Results root is still visible in Local Workspace.")
    summary = catalog.locator(":scope > summary")
    await summary.hover()
    await summary.locator("[data-create-local-workspace-root-folder]").click()
    await page.locator("[data-folder-name-input]").fill(folder_name)
    await page.locator("[data-folder-name-submit]").click()
    await page.locator("[data-confirm-submit]").click()

    started = time.perf_counter()
    await (
        page.locator("[data-source-operation-status-title]")
        .filter(has_text="Folder created")
        .wait_for(timeout=timeout_ms)
    )
    await wait_for_local_workspace_folder(page, folder_name, timeout_ms)
    return (time.perf_counter() - started) * 1000


async def create_subfolder(page, parent_path: str, child_name: str, timeout_ms: int) -> float:
    parent_folder = await wait_for_local_workspace_folder(page, parent_path, timeout_ms)
    if not await parent_folder.evaluate("node => node.hasAttribute('open')"):
        await parent_folder.locator(":scope > summary").click()
    summary = parent_folder.locator(":scope > summary")
    await summary.hover()
    await summary.locator("[data-source-action-menu-toggle]").click()
    await summary.locator("[data-create-local-workspace-folder-path]").click()
    await page.locator("[data-folder-name-input]").fill(child_name)
    await page.locator("[data-folder-name-submit]").click()
    await page.locator("[data-confirm-submit]").click()

    started = time.perf_counter()
    await (
        page.locator("[data-source-operation-status-title]")
        .filter(has_text="Folder created")
        .wait_for(timeout=timeout_ms)
    )
    child_path = f"{parent_path}/{child_name}"
    await wait_for_local_workspace_folder(page, child_path, timeout_ms)
    return (time.perf_counter() - started) * 1000


async def delete_folder(page, folder_path: str, timeout_ms: int) -> float:
    folder = await wait_for_local_workspace_folder(page, folder_path, timeout_ms)
    summary = folder.locator(":scope > summary")
    await summary.hover()
    await summary.locator("[data-source-action-menu-toggle]").click()
    await summary.locator("[data-delete-local-workspace-folder-path]").click()
    await page.locator("[data-confirm-submit]").click()

    started = time.perf_counter()
    await (
        page.locator("[data-source-operation-status-title]")
        .filter(has_text="Folder deleted")
        .wait_for(timeout=timeout_ms)
    )
    await page.locator(
        f'[data-local-workspace-folder-node][data-local-workspace-folder-path="{folder_path}"]'
    ).wait_for(state="detached", timeout=timeout_ms)
    return (time.perf_counter() - started) * 1000


async def run_smoke(args: argparse.Namespace) -> int:
    root_folder = unique_name("pw-local-folder")
    child_folder = unique_name("nested")

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
            await page.add_init_script(
                script=(
                    f'window.localStorage.setItem("{SIDEBAR_COLLAPSED_KEY}", "false");'
                )
            )
            await page.goto(
                urljoin(args.base_url, "query-workbench"),
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            await ensure_sidebar_expanded(page)

            create_root_ms = await create_root_folder(page, root_folder, args.timeout_ms)
            stored_paths = await local_workspace_folders_in_storage(page)
            if root_folder not in stored_paths:
                raise RuntimeError(f"Root folder not persisted in local storage: {root_folder}")

            await page.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)
            await ensure_sidebar_expanded(page)
            await wait_for_local_workspace_folder(page, root_folder, args.timeout_ms)

            create_child_ms = await create_subfolder(
                page,
                root_folder,
                child_folder,
                args.timeout_ms,
            )
            child_path = f"{root_folder}/{child_folder}"
            stored_paths = await local_workspace_folders_in_storage(page)
            if child_path not in stored_paths:
                raise RuntimeError(f"Nested folder not persisted in local storage: {child_path}")

            await page.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)
            await ensure_sidebar_expanded(page)
            await wait_for_local_workspace_folder(page, child_path, args.timeout_ms)

            delete_ms = await delete_folder(page, root_folder, args.timeout_ms)
            stored_paths = await local_workspace_folders_in_storage(page)
            if any(path == root_folder or path.startswith(f"{root_folder}/") for path in stored_paths):
                raise RuntimeError(f"Deleted folder branch still exists in local storage: {root_folder}")

            await page.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)
            await ensure_sidebar_expanded(page)
            await ensure_details_open(page, "[data-data-sources-section]")
            await ensure_details_open(page, '[data-source-catalog-source-id="workspace.local"]')
            await page.locator(
                f'[data-local-workspace-folder-node][data-local-workspace-folder-path="{root_folder}"]'
            ).wait_for(state="detached", timeout=args.timeout_ms)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(f"Local Workspace create root folder: {create_root_ms:.0f} ms")
    print(f"Local Workspace create nested folder: {create_child_ms:.0f} ms")
    print(f"Local Workspace delete folder branch: {delete_ms:.0f} ms")
    print("Playwright Local Workspace folder smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
