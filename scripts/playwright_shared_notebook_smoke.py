from __future__ import annotations

import argparse
import asyncio
import sys
import time
import uuid
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise the shared notebook flow in the browser using Playwright. "
            "The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    parser.add_argument("--debug", action="store_true", default=False)
    return parser.parse_args()


def clear_bdw_keys_script() -> str:
    return """
        () => {
          const keys = [];
          for (let index = 0; index < window.localStorage.length; index += 1) {
            const key = window.localStorage.key(index);
            if (key && key.startsWith("bdw.")) {
              keys.push(key);
            }
          }
          for (const key of keys) {
            window.localStorage.removeItem(key);
          }

          const sessionKeys = [];
          for (let index = 0; index < window.sessionStorage.length; index += 1) {
            const key = window.sessionStorage.key(index);
            if (key && key.startsWith("bdw.")) {
              sessionKeys.push(key);
            }
          }
          for (const key of sessionKeys) {
            window.sessionStorage.removeItem(key);
          }
        }
    """


async def clear_bdw_storage(page) -> None:
    await page.evaluate(
        """
        () => {
          const keys = [];
          for (let index = 0; index < window.localStorage.length; index += 1) {
            const key = window.localStorage.key(index);
            if (key && key.startsWith("bdw.")) {
              keys.push(key);
            }
          }
          for (const key of keys) {
            window.localStorage.removeItem(key);
          }
        }
        """
    )


async def read_workspace_notebook_id(page) -> str:
    return (
        await page.evaluate(
            """
            () => {
              const workspace = document.querySelector("[data-workspace-notebook]");
              return workspace?.dataset?.notebookId || "";
            }
            """
        )
    ) or ""


async def open_query_workbench_entry(page, base_url: str, timeout_ms: int) -> None:
    await page.goto(
        urljoin(base_url, "query-workbench"),
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )

    await page.evaluate(
        """
        async () => {
          const response = await window.fetch("/query-workbench", {
            headers: {
              Accept: "text/html",
              "HX-Request": "true",
            },
          });
          if (!response.ok) {
            throw new Error(`failed to load query-workbench partial (${response.status})`);
          }

          const html = await response.text();
          const panel = document.getElementById("workspace-panel");
          if (!panel) {
            throw new Error("workspace-panel was not found.");
          }
          panel.innerHTML = html;
        }
        """
    )
    await page.locator("[data-query-workbench-entry-page]").wait_for(
        state="visible",
        timeout=timeout_ms,
    )


async def wait_for_local_notebook_id_stable(page, timeout_ms: int) -> str:
    await page.wait_for_selector("[data-workspace-notebook]", state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        """
        () => {
          const workspace = document.querySelector("[data-workspace-notebook]");
          if (!workspace) {
            return false;
          }
          const notebookId = workspace.dataset?.notebookId || "";
          return typeof notebookId === "string" && notebookId.startsWith("local-notebook-");
        }
        """,
        timeout=timeout_ms,
    )

    stable_id = (await read_workspace_notebook_id(page)).strip()
    if not stable_id:
        raise RuntimeError("The workspace did not expose a notebook id.")

    for _ in range(6):
        await page.wait_for_timeout(125)
        next_id = (await read_workspace_notebook_id(page)).strip()
        if not next_id:
            continue
        if not next_id.startswith("local-notebook-"):
            raise RuntimeError(
                "The workspace switched to a non-local notebook during creation."
            )
        if next_id == stable_id:
            return stable_id
        stable_id = next_id

    raise RuntimeError("The notebook id did not stabilize after creating a local notebook.")


async def click_folder_tool(page, folder_name: str, selector: str, timeout_ms: int = 30000) -> None:
    deadline = time.monotonic() + (timeout_ms / 1000)
    clicked = False
    while time.monotonic() < deadline:
        clicked = await page.evaluate(
            """
            ({ folderName, selector }) => {
              const isVisible = (node) => Boolean(node && (
                node.offsetWidth || node.offsetHeight || node.getClientRects().length
              ));
              const folders = Array.from(document.querySelectorAll("[data-tree-folder]"));
              const folder = folders.find((candidate) => {
                const label = candidate.querySelector(":scope > summary .tree-folder-label");
                return label && label.textContent.trim() === folderName && isVisible(candidate);
              }) || folders.find((candidate) => {
                const label = candidate.querySelector(":scope > summary .tree-folder-label");
                return label && label.textContent.trim() === folderName;
              });
              const button = folder?.querySelector(`:scope > summary ${selector}`);
              if (!button) {
                return false;
              }
              button.click();
              return true;
            }
            """,
            {"folderName": folder_name, "selector": selector},
        )
        if clicked:
            break
        await page.wait_for_timeout(250)

    if not clicked:
        sidebar_snapshot = await page.evaluate(
            """
            () => Array.from(document.querySelectorAll("[data-tree-folder]"))
              .map((folder) => folder.querySelector(":scope > summary .tree-folder-label")?.textContent?.trim() || "")
              .filter(Boolean)
              .join(", ")
            """
        )
        raise RuntimeError(
            f"Could not click {selector} for folder {folder_name}. "
            f"Visible folders: {sidebar_snapshot}"
        )


async def folder_is_public(page, folder_name: str) -> bool:
    return bool(
        await page.evaluate(
            """
            (folderName) => {
              const folders = Array.from(document.querySelectorAll("[data-tree-folder]"));
              const folder = folders.find((candidate) => {
                const label = candidate.querySelector(":scope > summary .tree-folder-label");
                return label && label.textContent.trim() === folderName;
              });
              return folder?.dataset?.folderShared === "true";
            }
            """,
            folder_name,
        )
    )


async def refresh_notebook_sidebar(page) -> None:
    await page.evaluate(
        """
        async () => {
          const response = await window.fetch("/sidebar?mode=notebook", {
            headers: { Accept: "text/html" },
          });
          if (!response.ok) {
            throw new Error(`failed to refresh sidebar: ${response.status}`);
          }
          const html = await response.text();
          const sidebar = document.getElementById("sidebar");
          if (!sidebar) {
            throw new Error("sidebar was not found");
          }
          sidebar.outerHTML = html;
        }
        """
    )


async def create_root_folder(page, folder_name: str, timeout_ms: int) -> None:
    await page.evaluate(
        """
        async (folderName) => {
          const response = await window.fetch("/api/notebooks/shared/folders", {
            method: "POST",
            headers: {
              Accept: "application/json",
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              path: [folderName],
              displayName: folderName,
              isPublic: false,
              canEdit: true,
              canDelete: true,
            }),
          });
          if (!response.ok) {
            throw new Error(`failed to create folder ${folderName}: ${response.status}`);
          }
        }
        """,
        folder_name,
    )
    await refresh_notebook_sidebar(page)
    await page.wait_for_function(
        """
        (folderName) => {
          return Array.from(document.querySelectorAll("[data-tree-folder]")).some((folder) => {
            const label = folder.querySelector(":scope > summary .tree-folder-label");
            return label && label.textContent.trim() === folderName;
          });
        }
        """,
        arg=folder_name,
        timeout=timeout_ms,
    )


async def set_folder_public(page, folder_name: str, is_public: bool, timeout_ms: int) -> None:
    current = await folder_is_public(page, folder_name)
    if current == is_public:
        return
    await page.evaluate(
        """
        async ({ folderName, isPublic }) => {
          const response = await window.fetch("/api/notebooks/shared/folders/visibility", {
            method: "PATCH",
            headers: {
              Accept: "application/json",
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              path: [folderName],
              displayName: folderName,
              isPublic,
            }),
          });
          if (!response.ok) {
            throw new Error(`failed to update folder visibility ${folderName}: ${response.status}`);
          }
        }
        """,
        {"folderName": folder_name, "isPublic": is_public},
    )
    await refresh_notebook_sidebar(page)
    await page.wait_for_function(
        """
        ({ folderName, isPublic }) => {
          const folders = Array.from(document.querySelectorAll("[data-tree-folder]"));
          const folder = folders.find((candidate) => {
            const label = candidate.querySelector(":scope > summary .tree-folder-label");
            return label && label.textContent.trim() === folderName;
          });
          return Boolean(folder && (folder.dataset.folderShared === "true") === isPublic);
        }
        """,
        arg={"folderName": folder_name, "isPublic": is_public},
        timeout=timeout_ms,
    )


async def create_notebook_in_folder(page, folder_name: str, expect_shared: bool, timeout_ms: int) -> str:
    if expect_shared:
        async with page.expect_response(
            lambda response: (
                response.request.method == "POST"
                and response.url.endswith("/api/notebooks/shared")
                and response.status == 200
            ),
            timeout=timeout_ms,
        ) as response_info:
            await click_folder_tool(page, folder_name, "[data-create-notebook]")
        payload = await (await response_info.value).json()
        notebook_id = str(payload.get("notebook", {}).get("notebookId", "")).strip()
        if not notebook_id:
            raise RuntimeError("Shared notebook creation did not return a notebook id.")
        await page.wait_for_function(
            """
            (expected) => {
              const workspace = document.querySelector("[data-workspace-notebook]");
              return Boolean(workspace && workspace.dataset.notebookId === expected);
            }
            """,
            arg=notebook_id,
            timeout=timeout_ms,
        )
    else:
        await click_folder_tool(page, folder_name, "[data-create-notebook]")
        notebook_id = await wait_for_local_notebook_id_stable(page, timeout_ms)

    badge_copy = await page.locator(
        f"[data-draggable-notebook][data-notebook-id='{notebook_id}'] .notebook-sharing-pill"
    ).first.inner_text()
    expected_copy = "Public / Shared" if expect_shared else "Private / Local"
    if expected_copy not in badge_copy:
        raise RuntimeError(f"Expected {expected_copy} badge for {notebook_id}, got {badge_copy}.")
    return notebook_id


async def replace_first_cell_sql(page, sql_text: str, timeout_ms: int) -> None:
    editor = page.locator("[data-query-cell]").first.locator(".cm-content")
    await editor.wait_for(state="visible", timeout=timeout_ms)
    await editor.click()
    select_shortcut = "Meta+A" if sys.platform == "darwin" else "Control+A"
    await page.keyboard.press(select_shortcut)
    await page.keyboard.press("Backspace")
    await page.keyboard.type(sql_text)


async def read_workspace_cell_sqls(page) -> list[str]:
    return await page.evaluate(
        """
        () => Array.from(document.querySelectorAll("[data-query-cell]"))
          .map((cell) => cell.querySelector("[data-editor-source]")?.value ?? "")
        """
    )


async def wait_for_workspace_cell_count(page, expected_count: int, timeout_ms: int) -> None:
    await page.wait_for_function(
        """
        (expectedCount) => document.querySelectorAll("[data-query-cell]").length === expectedCount
        """,
        arg=expected_count,
        timeout=timeout_ms,
    )


def shared_notebook_sync_response(sql_marker: str | None = None):
    def _matches(response) -> bool:
        if response.request.method != "POST" or not response.url.endswith("/api/notebooks/shared"):
            return False
        if response.status != 200:
            return False
        if sql_marker is None:
            return True
        return sql_marker in (response.request.post_data or "")

    return _matches


def shared_notebook_sync_request(sql_marker: str):
    def _matches(request) -> bool:
        return (
            request.method == "POST"
            and request.url.endswith("/api/notebooks/shared")
            and sql_marker in (request.post_data or "")
        )

    return _matches


async def read_shared_notebook_cells_in_new_context(
    browser,
    base_url: str,
    shared_notebook_id: str,
    expected_count: int,
    timeout_ms: int,
) -> list[str]:
    second_context = await browser.new_context()
    await second_context.add_init_script(clear_bdw_keys_script())
    try:
        viewer_page = await second_context.new_page()
        await viewer_page.goto(
            f"{base_url.rstrip('/')}/notebooks/{shared_notebook_id}",
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        await viewer_page.wait_for_selector("[data-workspace-notebook]", timeout=timeout_ms)
        await wait_for_workspace_cell_count(viewer_page, expected_count, timeout_ms)
        return await read_workspace_cell_sqls(viewer_page)
    finally:
        await second_context.close()


async def assert_shared_add_cell_preserves_sql(
    page,
    browser,
    base_url: str,
    shared_notebook_id: str,
    timeout_ms: int,
) -> None:
    initial_sql = f"select 42 as preserved_{uuid.uuid4().hex[:6]}"

    async with page.expect_response(
        shared_notebook_sync_response(initial_sql),
        timeout=timeout_ms,
    ):
        await replace_first_cell_sql(page, initial_sql, timeout_ms)

    async with page.expect_response(
        shared_notebook_sync_response(initial_sql),
        timeout=timeout_ms,
    ):
        await page.locator("[data-add-cell]").click()

    await wait_for_workspace_cell_count(page, 2, timeout_ms)
    cell_sqls = await read_workspace_cell_sqls(page)
    if cell_sqls != [initial_sql, ""]:
        raise RuntimeError(
            "Adding a shared-notebook cell did not preserve the existing cell SQL. "
            f"Observed cells: {cell_sqls!r}"
        )

    persisted_cell_sqls = await read_shared_notebook_cells_in_new_context(
        browser,
        base_url,
        shared_notebook_id,
        2,
        timeout_ms,
    )
    if persisted_cell_sqls != [initial_sql, ""]:
        raise RuntimeError(
            "The shared notebook did not persist the add-cell state after reload. "
            f"Observed cells: {persisted_cell_sqls!r}"
        )


async def assert_newer_shared_edit_survives_inflight_sync(
    page,
    browser,
    base_url: str,
    shared_notebook_id: str,
    timeout_ms: int,
) -> None:
    stale_sql = f"select 1 as stale_{uuid.uuid4().hex[:6]}"
    latest_sql = f"select 2 as latest_{uuid.uuid4().hex[:6]}"
    delayed_posts = 0

    async def delay_first_shared_sync(route):
        nonlocal delayed_posts
        request = route.request
        if request.method == "POST" and request.url.endswith("/api/notebooks/shared"):
            delayed_posts += 1
            if delayed_posts == 1:
                await asyncio.sleep(0.35)
        await route.continue_()

    await page.route("**/api/notebooks/shared", delay_first_shared_sync)
    try:
        first_request = asyncio.create_task(
            page.wait_for_request(
                shared_notebook_sync_request(stale_sql),
                timeout=timeout_ms,
            )
        )
        await replace_first_cell_sql(page, stale_sql, timeout_ms)
        await first_request

        async with page.expect_response(
            shared_notebook_sync_response(latest_sql),
            timeout=timeout_ms,
        ):
            await replace_first_cell_sql(page, latest_sql, timeout_ms)
    finally:
        await page.unroute("**/api/notebooks/shared", delay_first_shared_sync)

    await page.wait_for_timeout(250)
    cell_sqls = await read_workspace_cell_sqls(page)
    if not cell_sqls or cell_sqls[0] != latest_sql:
        raise RuntimeError(
            "A stale shared-notebook sync response discarded a newer local edit. "
            f"Observed cells: {cell_sqls!r}"
        )

    persisted_cell_sqls = await read_shared_notebook_cells_in_new_context(
        browser,
        base_url,
        shared_notebook_id,
        2,
        timeout_ms,
    )
    if not persisted_cell_sqls or persisted_cell_sqls[0] != latest_sql:
        raise RuntimeError(
            "The newer shared-notebook edit was not persisted after an inflight sync. "
            f"Observed cells: {persisted_cell_sqls!r}"
        )


async def delete_shared_notebook_by_api(page, notebook_id: str) -> None:
    if not notebook_id:
        return
    await page.evaluate(
        """
        async (notebookId) => {
          await window.fetch(`/api/notebooks/shared/${encodeURIComponent(notebookId)}`, {
            method: "DELETE",
            headers: { Accept: "application/json" },
          });
        }
        """,
        notebook_id,
    )


async def create_notebook(
    page,
    base_url: str,
    timeout_ms: int,
    max_attempts: int = 3,
    debug: bool = False,
) -> str:
    create_button = page.locator(
        "[data-query-workbench-entry-page] [data-create-notebook]"
    )
    await create_button.wait_for(state="visible", timeout=timeout_ms)

    previous_notebook_id = await read_workspace_notebook_id(page)

    for attempt in range(1, max_attempts + 1):
        await page.wait_for_function(
            """
            () => {
              const entryRoot = document.querySelector("[data-query-workbench-entry-page]");
              const createButton = entryRoot?.querySelector("[data-create-notebook]");
              const workspaceRoot = document.querySelector("[data-workspace-notebook]");
              return Boolean(entryRoot && createButton && !workspaceRoot);
            }
            """,
            timeout=timeout_ms,
        )
        await page.wait_for_timeout(600)
        await create_button.click(force=True)
        try:
            notebook_id = await wait_for_local_notebook_id_stable(page, timeout_ms)
        except RuntimeError as exc:
            if attempt >= max_attempts:
                raise
            await open_query_workbench_entry(page, base_url, timeout_ms)
            await clear_bdw_storage(page)
            create_button = page.locator("[data-query-workbench-entry-page] [data-create-notebook]")
            await page.wait_for_timeout(250)
            if debug:
                print(f"DEBUG create attempt {attempt} failed, retrying: {exc}")
            continue

        if previous_notebook_id and notebook_id == previous_notebook_id:
            if attempt >= max_attempts:
                raise RuntimeError(
                    "The selected notebook id did not change after creating a notebook."
                )
            await open_query_workbench_entry(page, base_url, timeout_ms)
            await clear_bdw_storage(page)
            create_button = page.locator("[data-query-workbench-entry-page] [data-create-notebook]")
            continue

        return notebook_id

    raise RuntimeError("Failed to create a new local notebook after multiple retries.")


async def share_notebook(page, timeout_ms: int) -> str:
    async with page.expect_response(
        lambda response: (
            response.request.method == "POST"
            and response.url.endswith("/api/notebooks/shared")
            and response.status == 200
        ),
        timeout=timeout_ms,
    ) as response_info:
        await page.locator('[data-notebook-shared-toggle]').click()

    response = await response_info.value
    response_payload = await response.json()
    shared_notebook_id = str(
        (response_payload or {}).get("notebook", {}).get("notebookId", "")
    ).strip()
    if not shared_notebook_id:
        raise RuntimeError("Share API response did not return a notebook id.")
    await page.wait_for_function(
        """
        (expected) => {
          const workspace = document.querySelector("[data-workspace-notebook]");
          return Boolean(workspace && workspace.dataset.notebookId === expected);
        }
        """,
        arg=shared_notebook_id,
        timeout=timeout_ms,
    )

    sharing_copy = await page.locator(
        '[data-notebook-meta] [data-notebook-shared-toggle] .workspace-sharing-toggle-copy'
    ).first.inner_text()
    if "Public / Shared" not in sharing_copy:
        raise RuntimeError(
            f"Expected share toggle copy to be Public / Shared, got: {sharing_copy}"
        )

    return shared_notebook_id


async def ensure_shared_list_contains(
    page,
    base_url: str,
    shared_notebook_id: str,
    timeout_ms: int,
) -> str:
    await open_query_workbench_entry(page, base_url, timeout_ms)
    shared_entry = page.locator(
        "[data-query-workbench-entry-page] .query-entry-shared-list "
        f"[data-notebook-id='{shared_notebook_id}']"
    )
    await shared_entry.wait_for(state="attached", timeout=timeout_ms)
    shared_title = await shared_entry.locator(
        ".query-entry-shared-title-row .query-entry-shared-title"
    ).first.inner_text()
    pill_copy = await shared_entry.locator("small.notebook-sharing-pill").first.inner_text()
    if "Public / Shared" not in pill_copy:
        raise RuntimeError(
            "Shared notebook list entry did not advertise Public / Shared."
        )
    return shared_title


async def open_shared_notebook_in_new_context(
    browser,
    base_url: str,
    shared_notebook_id: str,
    timeout_ms: int,
) -> str:
    second_context = await browser.new_context()
    try:
        viewer_page = await second_context.new_page()
        await viewer_page.goto(
            f"{base_url.rstrip('/')}/notebooks/{shared_notebook_id}",
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
        await viewer_page.wait_for_selector("[data-workspace-notebook]", timeout=timeout_ms)
        await viewer_page.wait_for_selector(
            '[data-notebook-meta][data-shared="true"] [data-notebook-shared-toggle]',
            timeout=timeout_ms,
        )

        sharing_copy = await viewer_page.locator(
            '[data-notebook-meta] [data-notebook-shared-toggle] .workspace-sharing-toggle-copy'
        ).first.inner_text()
        if "Public / Shared" not in sharing_copy:
            raise RuntimeError(
                "A notebook opened via /notebooks/{id} did not stay marked as Public / Shared."
            )

        return sharing_copy
    finally:
        await second_context.close()


async def unshare_notebook(page, shared_notebook_id: str, timeout_ms: int) -> None:
    async with page.expect_response(
        lambda response: (
            response.request.method == "DELETE"
            and f"/api/notebooks/shared/{shared_notebook_id}" in response.url
            and response.status == 200
        ),
        timeout=timeout_ms,
    ) as response_info:
        await page.locator('[data-notebook-shared-toggle]').click()
    await response_info.value

    await page.wait_for_function(
        """
        () => {
          const copy = document.querySelector(
            "[data-workspace-notebook] [data-notebook-shared-toggle] .workspace-sharing-toggle-copy"
          );
          return copy && copy.textContent.includes("Private / Local");
        }
        """,
        timeout=timeout_ms,
    )


async def run_smoke(args: argparse.Namespace) -> int:
    network_responses: list[tuple[str, str, int]] = []
    console_messages: list[str] = []
    page_errors: list[str] = []

    def _append_network_event(response) -> None:
        try:
            network_responses.append(
                (response.request.method, response.url, response.status)
            )
        except Exception:
            network_responses.append(("UNKNOWN", "unable-to-read-response", -1))

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(viewport={"width": 1440, "height": 1200})
        await context.add_init_script(clear_bdw_keys_script())
        page = await context.new_page()

        page.on("console", lambda msg: console_messages.append(f"console:{msg.type}:{msg.text}"))
        page.on("pageerror", lambda exc: page_errors.append(f"pageerror:{exc}"))
        page.on("response", lambda resp: _append_network_event(resp))
        stage = "initializing"

        try:
            stage = "open query-workbench"
            if args.debug:
                print(f"DEBUG opening query-workbench at {args.base_url}")
            await open_query_workbench_entry(page, args.base_url, args.timeout_ms)
            if args.debug:
                print("DEBUG opening complete")

            stage = "clear local storage"
            await clear_bdw_storage(page)
            if args.debug:
                print("DEBUG storage cleared")

            suffix = uuid.uuid4().hex[:8]
            public_folder = f"PW Public {suffix}"
            private_folder = f"PW Private {suffix}"

            stage = "create public and private folders"
            await create_root_folder(page, public_folder, args.timeout_ms)
            await create_root_folder(page, private_folder, args.timeout_ms)
            await set_folder_public(page, public_folder, True, args.timeout_ms)
            if await folder_is_public(page, private_folder):
                raise RuntimeError("New notebook folders should default to Private.")

            stage = "create notebook in public folder"
            public_shared_notebook_id = await create_notebook_in_folder(
                page,
                public_folder,
                True,
                args.timeout_ms,
            )

            stage = "preserve shared notebook SQL when adding a cell"
            await assert_shared_add_cell_preserves_sql(
                page,
                browser,
                args.base_url,
                public_shared_notebook_id,
                args.timeout_ms,
            )

            stage = "preserve newer shared notebook edit during inflight sync"
            await assert_newer_shared_edit_survives_inflight_sync(
                page,
                browser,
                args.base_url,
                public_shared_notebook_id,
                args.timeout_ms,
            )

            stage = "create notebook in private folder"
            private_local_notebook_id = await create_notebook_in_folder(
                page,
                private_folder,
                False,
                args.timeout_ms,
            )

            stage = "toggle private folder and create future shared notebook"
            await set_folder_public(page, private_folder, True, args.timeout_ms)
            future_shared_notebook_id = await create_notebook_in_folder(
                page,
                private_folder,
                True,
                args.timeout_ms,
            )
            private_local_shared = await page.evaluate(
                """
                (notebookId) => {
                  const raw = window.localStorage.getItem("bdw.notebookMeta.v1") || "{}";
                  const state = JSON.parse(raw);
                  return state?.[notebookId]?.shared === true;
                }
                """,
                private_local_notebook_id,
            )
            if private_local_shared:
                raise RuntimeError("Toggling a folder changed an existing local notebook to shared.")

            stage = "locate shared notebooks on entry page"
            public_shared_title = await ensure_shared_list_contains(
                page,
                args.base_url,
                public_shared_notebook_id,
                args.timeout_ms,
            )
            await ensure_shared_list_contains(
                page,
                args.base_url,
                future_shared_notebook_id,
                args.timeout_ms,
            )

            stage = "open shared folder and notebook in second context"
            second_context = await browser.new_context()
            await second_context.add_init_script(clear_bdw_keys_script())
            try:
                viewer_page = await second_context.new_page()
                await open_query_workbench_entry(viewer_page, args.base_url, args.timeout_ms)
                await viewer_page.wait_for_function(
                    """
                    ({ folderName, notebookId }) => {
                      const folderVisible = Array.from(document.querySelectorAll("[data-tree-folder]")).some((folder) => {
                        const label = folder.querySelector(":scope > summary .tree-folder-label");
                        return label && label.textContent.trim() === folderName && folder.dataset.folderShared === "true";
                      });
                      const notebookVisible = Boolean(document.querySelector(
                        `[data-draggable-notebook][data-notebook-id="${notebookId}"][data-shared="true"]`
                      ));
                      return folderVisible && notebookVisible;
                    }
                    """,
                    arg={"folderName": public_folder, "notebookId": public_shared_notebook_id},
                    timeout=args.timeout_ms,
                )
                if args.debug:
                    await viewer_page.screenshot(path=f"shared-notebook-viewer-{public_shared_notebook_id}.png")
            finally:
                await second_context.close()

            stage = "cleanup shared notebooks"
            await delete_shared_notebook_by_api(page, public_shared_notebook_id)
            await delete_shared_notebook_by_api(page, future_shared_notebook_id)
            final_copy = "Private / Local"

            if args.debug:
                await page.screenshot(path=f"shared-notebook-owner-{public_shared_notebook_id}.png")
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            print(f"FAILED during step: {stage}", file=sys.stderr)
            print(str(exc), file=sys.stderr)
            for method, url, status in network_responses:
                if "/api/notebooks/shared" in url:
                    print(f"HTTP {method} {status} {url}", file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            for message in page_errors:
                print(message, file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print(
        "Playwright shared notebook smoke passed: "
        f"private={private_local_notebook_id}, "
        f"public_shared={public_shared_notebook_id}, "
        f"future_shared={future_shared_notebook_id}, "
        f"title='{public_shared_title}', "
        f"final='{final_copy}'"
    )
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
