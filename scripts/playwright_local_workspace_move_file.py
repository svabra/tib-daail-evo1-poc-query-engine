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


LOCAL_WORKSPACE_DB = "bdw.localWorkspace.v1"
LOCAL_WORKSPACE_STORE = "exports"
LOCAL_WORKSPACE_FOLDERS_KEY = "bdw.localWorkspaceFolders.v1"
SIDEBAR_COLLAPSED_KEY = "bdw.sidebarCollapsed.v1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise Local Workspace file move and rename in the sidebar "
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


async def ensure_local_workspace_open(page) -> None:
    await ensure_sidebar_expanded(page)
    await ensure_details_open(page, "[data-data-sources-section]")
    await ensure_details_open(page, '[data-source-catalog-source-id="workspace.local"]')


async def seed_local_workspace_entry(
    page,
    *,
    entry_id: str,
    source_folder: str,
    destination_folder: str,
    file_name: str,
) -> None:
    payload = {
        "entryId": entry_id,
        "sourceFolder": source_folder,
        "destinationFolder": destination_folder,
        "fileName": file_name,
    }
    await page.evaluate(
        """
        async ({ entryId, sourceFolder, destinationFolder, fileName, dbName, storeName, folderKey }) => {
          window.localStorage.setItem(folderKey, JSON.stringify([sourceFolder, destinationFolder]));

          const timestamp = new Date().toISOString();
          await new Promise((resolve, reject) => {
            const request = window.indexedDB.open(dbName, 1);
            request.onupgradeneeded = () => {
              const db = request.result;
              if (!db.objectStoreNames.contains(storeName)) {
                db.createObjectStore(storeName, { keyPath: "id" });
              }
            };
            request.onerror = () => reject(request.error || new Error("IndexedDB open failed."));
            request.onsuccess = () => {
              const db = request.result;
              const transaction = db.transaction(storeName, "readwrite");
              const store = transaction.objectStore(storeName);
              store.put({
                id: entryId,
                fileName,
                folderPath: sourceFolder,
                exportFormat: "json",
                mimeType: "application/json",
                sizeBytes: 18,
                createdAt: timestamp,
                updatedAt: timestamp,
                notebookTitle: "Playwright move seed",
                cellId: "pw-move-cell",
                columnCount: 1,
                rowCount: 1,
                blob: new Blob(['{"hello":"world"}'], { type: "application/json" }),
              });
              transaction.oncomplete = () => resolve(true);
              transaction.onerror = () =>
                reject(transaction.error || new Error("IndexedDB write failed."));
            };
          });
        }
        """,
        {
            **payload,
            "dbName": LOCAL_WORKSPACE_DB,
            "storeName": LOCAL_WORKSPACE_STORE,
            "folderKey": LOCAL_WORKSPACE_FOLDERS_KEY,
        },
    )


async def read_local_workspace_entry(page, entry_id: str):
    return await page.evaluate(
        """
        async ({ entryId, dbName, storeName }) => {
          return await new Promise((resolve, reject) => {
            const request = window.indexedDB.open(dbName, 1);
            request.onupgradeneeded = () => {
              const db = request.result;
              if (!db.objectStoreNames.contains(storeName)) {
                db.createObjectStore(storeName, { keyPath: "id" });
              }
            };
            request.onerror = () => reject(request.error || new Error("IndexedDB open failed."));
            request.onsuccess = () => {
              const db = request.result;
              const transaction = db.transaction(storeName, "readonly");
              const store = transaction.objectStore(storeName);
              const getRequest = store.get(entryId);
              getRequest.onsuccess = () => {
                const entry = getRequest.result;
                if (!entry) {
                  resolve(null);
                  return;
                }
                resolve({
                  id: entry.id,
                  fileName: entry.fileName,
                  folderPath: entry.folderPath,
                  exportFormat: entry.exportFormat,
                  mimeType: entry.mimeType,
                  sizeBytes: entry.sizeBytes,
                  rowCount: entry.rowCount,
                  columnCount: entry.columnCount,
                });
              };
              getRequest.onerror = () =>
                reject(getRequest.error || new Error("IndexedDB read failed."));
            };
          });
        }
        """,
        {
            "entryId": entry_id,
            "dbName": LOCAL_WORKSPACE_DB,
            "storeName": LOCAL_WORKSPACE_STORE,
        },
    )


async def wait_for_local_workspace_file(page, entry_id: str, timeout_ms: int):
    await ensure_local_workspace_open(page)
    locator = page.locator(f'[data-local-workspace-entry-id="{entry_id}"]')
    await locator.wait_for(state="visible", timeout=timeout_ms)
    return locator


async def move_local_workspace_file(
    page,
    *,
    entry_id: str,
    destination_folder: str,
    moved_file_name: str,
    timeout_ms: int,
) -> float:
    file_node = await wait_for_local_workspace_file(page, entry_id, timeout_ms)
    await file_node.hover()
    await file_node.locator("[data-source-action-menu-toggle]").click()
    await file_node.locator("[data-move-local-workspace-object]").click()
    await page.locator("[data-local-workspace-move-folder-path]").fill(destination_folder)
    await page.locator("[data-local-workspace-move-file-name]").fill(moved_file_name)

    started = time.perf_counter()
    await page.locator("[data-local-workspace-move-submit]").click()
    await (
        page.locator("[data-message-title]")
        .filter(has_text="Local Workspace file moved")
        .wait_for(timeout=timeout_ms)
    )
    await page.locator("[data-message-submit]").click()
    await wait_for_local_workspace_file(page, entry_id, timeout_ms)
    return (time.perf_counter() - started) * 1000


async def run_smoke(args: argparse.Namespace) -> int:
    source_folder = unique_name("pw-local-move-src")
    destination_folder = unique_name("pw-local-move-dst")
    entry_id = f"pw-local-entry-{uuid4().hex[:8]}"
    original_file_name = "move-me.json"
    moved_file_name = "moved-file.json"

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(viewport={"width": 1440, "height": 1200})
        console_messages: list[str] = []
        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: console_messages.append(f"pageerror:{exc}"))

        try:
            await page.add_init_script(
                script=f'window.localStorage.setItem("{SIDEBAR_COLLAPSED_KEY}", "false");'
            )
            await page.goto(
                urljoin(args.base_url, "query-workbench"),
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            await seed_local_workspace_entry(
                page,
                entry_id=entry_id,
                source_folder=source_folder,
                destination_folder=destination_folder,
                file_name=original_file_name,
            )
            await page.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)
            file_node = await wait_for_local_workspace_file(page, entry_id, args.timeout_ms)
            if await file_node.get_attribute("data-source-object-name") != original_file_name:
                raise RuntimeError("Seeded Local Workspace file was not rendered with the expected name.")

            move_ms = await move_local_workspace_file(
                page,
                entry_id=entry_id,
                destination_folder=destination_folder,
                moved_file_name=moved_file_name,
                timeout_ms=args.timeout_ms,
            )

            file_node = await wait_for_local_workspace_file(page, entry_id, args.timeout_ms)
            rendered_folder = await file_node.get_attribute("data-local-workspace-folder-path")
            rendered_name = await file_node.get_attribute("data-source-object-name")
            if rendered_folder != destination_folder:
                raise RuntimeError(
                    f"Rendered Local Workspace folder did not update: {rendered_folder!r}"
                )
            if rendered_name != moved_file_name:
                raise RuntimeError(f"Rendered Local Workspace file name did not update: {rendered_name!r}")

            stored_entry = await read_local_workspace_entry(page, entry_id)
            if not stored_entry:
                raise RuntimeError("Moved Local Workspace file was not found in IndexedDB.")
            if stored_entry.get("folderPath") != destination_folder:
                raise RuntimeError(
                    f"IndexedDB folderPath did not update: {stored_entry.get('folderPath')!r}"
                )
            if stored_entry.get("fileName") != moved_file_name:
                raise RuntimeError(
                    f"IndexedDB fileName did not update: {stored_entry.get('fileName')!r}"
                )

            await page.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)
            file_node = await wait_for_local_workspace_file(page, entry_id, args.timeout_ms)
            persisted_folder = await file_node.get_attribute("data-local-workspace-folder-path")
            persisted_name = await file_node.get_attribute("data-source-object-name")
            if persisted_folder != destination_folder or persisted_name != moved_file_name:
                raise RuntimeError("Moved Local Workspace file did not persist after reload.")
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(f"Local Workspace move file: {move_ms:.0f} ms")
    print("Playwright Local Workspace file move smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
