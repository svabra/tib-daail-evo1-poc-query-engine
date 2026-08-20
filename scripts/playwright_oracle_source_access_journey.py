from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


SOURCE_ID = "ora_bazg_zoll"
JOIN_SQL = """SELECT a.MRN, w.WARENBESCHREIBUNG, w.WERT_CHF
FROM ora_bazg_zoll.ZOLL.ANMELDUNGEN a
JOIN ora_bazg_zoll.ZOLL.WARENPOSITIONEN w USING (ANMELDUNG_ID)
ORDER BY w.WERT_CHF DESC"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the DAAIF to DaCa governed Oracle source-access journey."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8010")
    parser.add_argument("--daca-url", default="http://127.0.0.1:8080")
    parser.add_argument("--timeout-ms", type=int, default=120000)
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--artifact-dir", default="")
    return parser.parse_args()


async def response_json(response) -> object:
    text = await response.text()
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status} for {response.url}: {text}")
    return json.loads(text) if text else {}


async def assert_no_horizontal_overflow(page) -> None:
    overflow = await page.evaluate(
        "() => Math.max(document.documentElement.scrollWidth, document.body.scrollWidth) "
        "- document.documentElement.clientWidth"
    )
    if overflow > 1:
        raise RuntimeError(f"Page has {overflow}px of horizontal overflow: {page.url}")


async def wait_for_query_job(page, job_id: str, timeout_ms: int) -> dict[str, object]:
    deadline = time.monotonic() + timeout_ms / 1000
    last_job: dict[str, object] | None = None
    while time.monotonic() < deadline:
        response = await page.context.request.get(f"{page.url.split('/data-sources', 1)[0]}/api/query-jobs")
        payload = await response_json(response)
        jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
        for candidate in jobs:
            if not isinstance(candidate, dict) or candidate.get("jobId") != job_id:
                continue
            last_job = candidate
            status = str(candidate.get("status") or "").lower()
            if status == "completed":
                return candidate
            if status in {"failed", "cancelled", "canceled"}:
                raise RuntimeError(f"Oracle join job ended as {status}: {candidate.get('error')}")
        await page.wait_for_timeout(250)
    raise RuntimeError(f"Oracle join job timed out. Last state: {last_job}")


async def create_request_in_daaif(page, base_url: str, timeout_ms: int) -> dict[str, object]:
    await page.goto(f"{base_url}/ingestion-workbench", wait_until="domcontentloaded", timeout=timeout_ms)
    splitter = page.locator("[data-ingestion-splitter-page]")
    await splitter.wait_for(state="visible", timeout=timeout_ms)
    if await splitter.locator('.ingestion-splitter-card[href="/ingestion-workbench/manual"]').count() != 1:
        raise RuntimeError("The existing manual-ingestion destination is missing from the splitter.")
    await splitter.locator('.ingestion-splitter-card[href="/ingestion-workbench/sourcing"]').click()

    wizard = page.locator("[data-source-sourcing-wizard]")
    await wizard.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        "document.querySelector('[data-source-sourcing-wizard]')?.dataset.initialized === 'true'",
        timeout=timeout_ms,
    )
    source_type = wizard.locator("[data-sourcing-source-type]")
    disabled_values = await source_type.locator("option:disabled").evaluate_all(
        "options => options.map(option => option.value).filter(Boolean)"
    )
    if disabled_values != ["postgres", "dwh", "s3", "http"]:
        raise RuntimeError(f"Unexpected disabled source types: {disabled_values}")
    await source_type.select_option("oracle")
    await wizard.locator('[data-sourcing-step="1"] [data-sourcing-next]').click()
    await wizard.locator("[data-sourcing-catalog-summary]").get_by_text(
        "30 of 38 Oracle databases are discoverable to you.", exact=False
    ).wait_for(state="visible", timeout=timeout_ms)

    await wizard.locator("[data-sourcing-query]").fill("BZGZOLL1")
    target = wizard.locator(f'input[name="sourceId"][value="{SOURCE_ID}"]')
    await target.wait_for(state="attached", timeout=timeout_ms)
    await target.locator("xpath=ancestor::label").click()
    if not await target.is_checked():
        raise RuntimeError("Clicking the Oracle source card did not select it.")
    await wizard.locator('[data-sourcing-step="2"] [data-sourcing-next]').click()

    group = wizard.locator('input[name="subject"][value="group:estv-business-intelligence"]')
    await group.wait_for(state="attached", timeout=timeout_ms)
    if not await group.is_checked():
        raise RuntimeError("The recommended ESTV Business Intelligence group was not preselected.")
    request_title = f"Oracle customs analytics E2E {int(time.time())}"
    await wizard.locator('input[name="requestTitle"]').fill(request_title)
    await wizard.locator('input[name="conditionsAccepted"]').check()
    await wizard.locator('[data-sourcing-step="3"] [data-sourcing-next]').click()

    review = wizard.locator("[data-sourcing-review]")
    await review.get_by_text("ESTV Business Intelligence", exact=True).wait_for(
        state="visible", timeout=timeout_ms
    )
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith("/api/ingestion/sourcing/requests"),
        timeout=timeout_ms,
    ) as response_info:
        await wizard.locator("[data-sourcing-submit]").click()
    receipt = await response_json(await response_info.value)
    if not isinstance(receipt, dict) or receipt.get("status") != "submitted":
        raise RuntimeError(f"Unexpected source request receipt: {receipt}")
    await wizard.locator("[data-sourcing-receipt]").wait_for(state="visible", timeout=timeout_ms)
    return receipt


async def approve_in_daca(
    page,
    daca_url: str,
    request_id: str,
    timeout_ms: int,
    artifact_dir: Path | None,
) -> None:
    await page.goto(
        f"{daca_url}/tasks?demoUser=sandro.wenger",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    user_switcher = page.get_by_label("Demo-Benutzer wechseln")
    await user_switcher.wait_for(state="visible", timeout=timeout_ms)
    if await user_switcher.input_value() != "sandro.wenger":
        await user_switcher.select_option("sandro.wenger")
    card = page.locator(f'[data-source-request-id="{request_id}"]')
    await card.wait_for(state="visible", timeout=timeout_ms)
    if "Revision" not in await card.inner_text() or "Unbefristet" not in await card.inner_text():
        raise RuntimeError("The DaCa owner card does not show the immutable group or validity evidence.")
    if artifact_dir:
        await page.screenshot(
            path=artifact_dir / "02-daca-owner-request-desktop.png",
            full_page=True,
        )
    await page.set_viewport_size({"width": 390, "height": 844})
    await assert_no_horizontal_overflow(page)
    if artifact_dir:
        await page.screenshot(
            path=artifact_dir / "03-daca-owner-request-mobile.png",
            full_page=True,
        )
    await page.set_viewport_size({"width": 1600, "height": 1100})
    card = page.locator(f'[data-source-request-id="{request_id}"]')
    await card.wait_for(state="visible", timeout=timeout_ms)
    async with page.expect_response(
        lambda response: response.request.method == "POST"
        and response.url.endswith(f"/api/v1/source-access-requests/{request_id}/decision"),
        timeout=timeout_ms,
    ) as response_info:
        await card.get_by_role("button", name="Zugriff genehmigen").click()
    approved = await response_json(await response_info.value)
    if not isinstance(approved, dict) or approved.get("status") != "approved":
        raise RuntimeError(f"Unexpected DaCa decision response: {approved}")


async def verify_grant_and_query(page, base_url: str, timeout_ms: int) -> dict[str, object]:
    source_link = page.locator("[data-sourcing-open-source]")
    await source_link.wait_for(state="visible", timeout=timeout_ms)
    if f"source_id={SOURCE_ID}" not in (await source_link.get_attribute("href") or ""):
        raise RuntimeError("The active-grant receipt does not link to the selected Oracle source.")
    await source_link.click()
    source_page = page.locator(f'[data-data-source-management-page][data-selected-source-id="{SOURCE_ID}"]')
    await source_page.wait_for(state="visible", timeout=timeout_ms)
    await source_page.get_by_text("BAZG Zentrale Zollabwicklung", exact=True).first.wait_for(
        state="visible", timeout=timeout_ms
    )
    await assert_no_horizontal_overflow(page)

    await source_page.locator(f'[data-open-data-source-explorer="{SOURCE_ID}"]').first.click()
    explorer = page.locator(f'[data-data-source-explorer-page][data-selected-source-id="{SOURCE_ID}"]')
    await explorer.wait_for(state="visible", timeout=timeout_ms)
    objects = explorer.locator('[data-source-object][data-source-option-id="ora_bazg_zoll"]')
    await objects.first.wait_for(state="attached", timeout=timeout_ms)
    object_names = await objects.evaluate_all("nodes => nodes.map(node => node.dataset.sourceObjectName)")
    if object_names != ["ANMELDUNGEN", "WARENPOSITIONEN", "ABGABEN_UEBERSICHT_V"]:
        raise RuntimeError(f"Unexpected Oracle explorer objects: {object_names}")

    response = await page.context.request.post(
        f"{base_url}/api/query-jobs",
        form={
            "sql": JOIN_SQL,
            "displaySql": JOIN_SQL,
            "notebook_id": "oracle-source-access-playwright",
            "notebook_title": "Oracle Source Access E2E",
            "cell_id": "oracle-join",
            "data_sources": SOURCE_ID,
            "localRelations": "{}",
            "queryOptions": json.dumps({"validation": {"sourceExistence": "on"}}),
        },
    )
    started = await response_json(response)
    job_id = str(started.get("jobId") or "") if isinstance(started, dict) else ""
    if not job_id:
        raise RuntimeError(f"Oracle query did not return a job id: {started}")
    job = await wait_for_query_job(page, job_id, timeout_ms)
    if int(job.get("rowCount") or 0) != 5:
        raise RuntimeError(f"Oracle join returned an unexpected row count: {job}")
    if job.get("rows", [])[0][:2] != ["26CH000002B7", "Pharmaceutical Products"]:
        raise RuntimeError(f"Oracle join returned unexpected data: {job.get('rows')}")
    return job


async def verify_mobile_layout(page, base_url: str, timeout_ms: int, artifact_dir: Path | None) -> None:
    await page.set_viewport_size({"width": 390, "height": 844})
    await page.goto(
        f"{base_url}/ingestion-workbench",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    await page.locator("[data-ingestion-splitter-page]").wait_for(
        state="visible", timeout=timeout_ms
    )
    await assert_no_horizontal_overflow(page)
    if artifact_dir:
        await page.screenshot(
            path=artifact_dir / "06-daaif-mobile-ingestion-splitter.png",
            full_page=True,
        )

    await page.goto(
        f"{base_url}/data-sources?source_id={SOURCE_ID}",
        wait_until="domcontentloaded",
        timeout=timeout_ms,
    )
    source_page = page.locator(
        f'[data-data-source-management-page][data-selected-source-id="{SOURCE_ID}"]'
    )
    await source_page.wait_for(state="visible", timeout=timeout_ms)
    await page.wait_for_function(
        "root => root.dataset.sourceControlsInitialized === 'true'",
        arg=await source_page.element_handle(),
        timeout=timeout_ms,
    )
    await source_page.locator("[data-source-filter-query]").fill("BAZG")
    await source_page.locator("[data-source-filter-technology]").select_option(
        "BIT Oracle RDBMS"
    )
    visible_source_ids = await source_page.locator(
        "[data-source-filter-card]"
    ).evaluate_all(
        "nodes => nodes.filter(node => !node.hidden).map(node => node.dataset.openQueryDataSource)"
    )
    if visible_source_ids != [SOURCE_ID]:
        raise RuntimeError(
            f"Data Source Workbench filters returned unexpected sources: {visible_source_ids}"
        )
    await source_page.locator('[data-source-view-mode="list"]').click()
    if await source_page.get_attribute("data-source-view-mode") != "list":
        raise RuntimeError("Data Source Workbench did not switch to the scalable list view.")
    await assert_no_horizontal_overflow(page)
    if artifact_dir:
        await page.screenshot(
            path=artifact_dir / "07-daaif-mobile-oracle-source.png",
            full_page=True,
        )


async def verify_noemie_is_denied(page, base_url: str) -> None:
    identity = await page.context.request.post(
        f"{base_url}/api/ingestion/sourcing/identity",
        data={"userId": "noemie.rochat"},
        headers={"Content-Type": "application/json"},
    )
    await response_json(identity)
    explorer = await page.context.request.get(
        f"{base_url}/api/data-sources/explorer/{SOURCE_ID}"
    )
    if explorer.status != 404:
        raise RuntimeError(f"Noémie could inspect the Oracle source without a grant: HTTP {explorer.status}")
    query = await page.context.request.post(
        f"{base_url}/api/query-jobs",
        form={
            "sql": "SELECT * FROM ora_bazg_zoll.ZOLL.ANMELDUNGEN",
            "displaySql": "SELECT * FROM ora_bazg_zoll.ZOLL.ANMELDUNGEN",
            "notebook_id": "oracle-source-access-negative",
            "notebook_title": "Oracle Source Access Negative E2E",
            "cell_id": "oracle-denied",
            "data_sources": SOURCE_ID,
            "localRelations": "{}",
            "queryOptions": "{}",
        },
    )
    denied = await response_json(query)
    if (
        query.status != 200
        or not isinstance(denied, dict)
        or denied.get("status") != "failed"
        or "active DaCa source grant" not in str(denied.get("error") or "")
    ):
        raise RuntimeError(
            "Noémie's direct Oracle SQL bypass was not rejected during query preparation: "
            f"HTTP {query.status}, payload={denied}."
        )


async def run_smoke(args: argparse.Namespace) -> int:
    artifact_dir = Path(args.artifact_dir).resolve() if args.artifact_dir else None
    if artifact_dir:
        artifact_dir.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        context = await browser.new_context(viewport={"width": 1600, "height": 1100})
        await context.add_init_script("localStorage.setItem('daaif-demo-user', 'joel.ruod')")
        daaif_page = await context.new_page()
        daca_page = await context.new_page()
        diagnostics: list[str] = []
        for page in (daaif_page, daca_page):
            page.on("console", lambda message: diagnostics.append(f"console:{message.type}:{message.text}"))
            page.on("pageerror", lambda error: diagnostics.append(f"pageerror:{error}"))
        try:
            receipt = await create_request_in_daaif(
                daaif_page, args.base_url.rstrip("/"), args.timeout_ms
            )
            if artifact_dir:
                await daaif_page.screenshot(path=artifact_dir / "01-daaif-request-receipt.png", full_page=True)
            await approve_in_daca(
                daca_page,
                args.daca_url.rstrip("/"),
                str(receipt["id"]),
                args.timeout_ms,
                artifact_dir,
            )
            if artifact_dir:
                await daca_page.screenshot(path=artifact_dir / "04-daca-owner-decision.png", full_page=True)
            job = await verify_grant_and_query(
                daaif_page, args.base_url.rstrip("/"), args.timeout_ms
            )
            if artifact_dir:
                await daaif_page.screenshot(path=artifact_dir / "05-daaif-oracle-explorer.png", full_page=True)
            await verify_mobile_layout(
                daaif_page,
                args.base_url.rstrip("/"),
                args.timeout_ms,
                artifact_dir,
            )
            await verify_noemie_is_denied(daaif_page, args.base_url.rstrip("/"))
        except (PlaywrightTimeoutError, RuntimeError, KeyError, ValueError) as exc:
            print(str(exc), flush=True)
            for diagnostic in diagnostics[-40:]:
                print(diagnostic, flush=True)
            await browser.close()
            return 1
        await browser.close()
    print(
        "Oracle source-access journey passed: "
        f"request={receipt['requestNumber']} source={SOURCE_ID} query={job['jobId']} rows={job['rowCount']}",
        flush=True,
    )
    return 0


def main() -> int:
    return asyncio.run(run_smoke(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
