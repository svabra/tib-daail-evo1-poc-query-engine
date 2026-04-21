from __future__ import annotations

import argparse
import asyncio
import sys
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
            "Exercise Local Workspace -> Shared Workspace move from the sidebar "
            "using Playwright. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
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


async def seed_local_workspace_entry(page, *, entry_id: str, folder_path: str, file_name: str) -> None:
    await page.evaluate(
        """
        async ({ entryId, folderPath, fileName, dbName, storeName, folderKey }) => {
          window.localStorage.setItem(folderKey, JSON.stringify([folderPath]));

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
                folderPath,
                exportFormat: "json",
                mimeType: "application/json",
                sizeBytes: 18,
                createdAt: timestamp,
                updatedAt: timestamp,
                notebookTitle: "Playwright S3 move seed",
                cellId: "pw-s3-move-cell",
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
            "entryId": entry_id,
            "folderPath": folder_path,
            "fileName": file_name,
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
              getRequest.onsuccess = () => resolve(getRequest.result || null);
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


async def create_bucket(page, bucket_name: str):
    return await page.evaluate(
        """
        async ({ bucketName }) => {
          const response = await fetch("/api/s3/explorer/buckets", {
            method: "POST",
            headers: {
              Accept: "application/json",
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ bucketName }),
          });
          if (!response.ok) {
            let detail = `Bucket create failed: ${response.status}`;
            try {
              const payload = await response.json();
              detail = payload?.detail || detail;
            } catch (_error) {
              // Ignore invalid JSON bodies.
            }
            throw new Error(detail);
          }
          return response.json();
        }
        """,
        {"bucketName": bucket_name},
    )


async def list_bucket_entries(page, bucket_name: str):
    return await page.evaluate(
        """
        async ({ bucketName }) => {
          const response = await fetch(`/api/s3/explorer?bucket=${encodeURIComponent(bucketName)}`, {
            headers: { Accept: "application/json" },
          });
          if (!response.ok) {
            let detail = `Bucket list failed: ${response.status}`;
            try {
              const payload = await response.json();
              detail = payload?.detail || detail;
            } catch (_error) {
              // Ignore invalid JSON bodies.
            }
            throw new Error(detail);
          }
          return response.json();
        }
        """,
        {"bucketName": bucket_name},
    )


async def delete_bucket(page, bucket_name: str) -> None:
    await page.evaluate(
        """
        async ({ bucketName }) => {
          await fetch("/api/s3/explorer/entries", {
            method: "DELETE",
            headers: {
              Accept: "application/json",
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              entryKind: "bucket",
              bucket: bucketName,
              prefix: "",
            }),
          });
        }
        """,
        {"bucketName": bucket_name},
    )


async def move_local_workspace_file_to_s3(
    page,
    *,
    entry_id: str,
    bucket_name: str,
    moved_file_name: str,
    timeout_ms: int,
) -> str:
    file_node = await wait_for_local_workspace_file(page, entry_id, timeout_ms)
    await file_node.hover()
    await file_node.locator("[data-source-action-menu-toggle]").click()
    await file_node.locator("[data-move-local-workspace-object]").click()

    await page.locator("[data-local-workspace-move-destination]").select_option("s3")
    bucket_node = page.locator(
        f'[data-local-workspace-move-dialog] [data-s3-explorer-node][data-s3-explorer-kind="bucket"][data-s3-explorer-bucket="{bucket_name}"]'
    )
    await bucket_node.wait_for(state="visible", timeout=timeout_ms)
    await bucket_node.locator(":scope > summary").click()

    await page.locator("[data-local-workspace-move-file-name]").fill(moved_file_name)
    await page.locator("[data-local-workspace-move-submit]").click()
    await (
        page.locator("[data-message-title]")
        .filter(has_text="Local Workspace file moved")
        .wait_for(timeout=timeout_ms)
    )
    move_copy = await page.locator("[data-message-copy]").text_content()
    await page.locator("[data-message-submit]").click()
    return move_copy or ""


async def run_smoke(args: argparse.Namespace) -> int:
    source_folder = unique_name("pw-local-s3-src")
    entry_id = f"pw-local-s3-entry-{uuid4().hex[:8]}"
    original_file_name = "move-me.json"
    moved_file_name = f"moved-to-s3-{uuid4().hex[:6]}.json"
    bucket_name = unique_name("pw-local-move-bucket")

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
                folder_path=source_folder,
                file_name=original_file_name,
            )
            await create_bucket(page, bucket_name)
            await page.reload(wait_until="domcontentloaded", timeout=args.timeout_ms)

            move_copy = await move_local_workspace_file_to_s3(
                page,
                entry_id=entry_id,
                bucket_name=bucket_name,
                moved_file_name=moved_file_name,
                timeout_ms=args.timeout_ms,
            )
            if bucket_name not in move_copy or moved_file_name not in move_copy:
                raise RuntimeError("Move confirmation did not mention the Shared Workspace destination.")

            if await read_local_workspace_entry(page, entry_id) is not None:
                raise RuntimeError("Local Workspace entry still exists in IndexedDB after move to S3.")

            bucket_snapshot = await list_bucket_entries(page, bucket_name)
            entries = bucket_snapshot.get("entries") or []
            moved_entry = next(
                (
                    item
                    for item in entries
                    if item.get("entryKind") == "file" and item.get("name") == moved_file_name
                ),
                None,
            )
            if not moved_entry:
                raise RuntimeError("Moved file was not found in the target Shared Workspace bucket.")
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            try:
                await delete_bucket(page, bucket_name)
            except Exception:
                pass
            await browser.close()
            return 1

        await delete_bucket(page, bucket_name)
        await browser.close()

    print("Playwright Local Workspace -> Shared Workspace move smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
