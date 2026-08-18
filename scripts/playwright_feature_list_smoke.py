from __future__ import annotations

import argparse
import asyncio
import sys
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify the DaCa-aligned DAAIF feature list from the fixed version overlay."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


def assert_inside_viewport(name: str, rect: dict, viewport: dict) -> None:
    if (
        rect["left"] < -1
        or rect["top"] < -1
        or rect["right"] > viewport["width"] + 1
        or rect["bottom"] > viewport["height"] + 1
    ):
        raise AssertionError(f"{name} escaped the mobile viewport: {rect}")


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 1440, "height": 1000},
            base_url=args.base_url.rstrip("/"),
        )
        page_errors: list[str] = []
        page.on("pageerror", lambda exc: page_errors.append(str(exc)))

        try:
            await page.goto(
                urljoin(args.base_url, "/"),
                wait_until="domcontentloaded",
                timeout=args.timeout_ms,
            )
            await page.locator("[data-home-page]").wait_for(
                state="visible",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                "() => document.documentElement.dataset.workbenchInteractive === 'true'",
                timeout=args.timeout_ms,
            )

            response = await page.request.get(
                urljoin(args.base_url, "/api/workbench/release-notes"),
                timeout=args.timeout_ms,
            )
            if not response.ok:
                raise AssertionError(
                    f"Feature-list metadata returned HTTP {response.status}."
                )
            releases = await response.json()
            expected_releases = releases[0]["featureList"]["releases"]
            expected_feature_count = sum(
                len(release["features"]) for release in expected_releases
            )

            async def delay_release_notes(route) -> None:
                await asyncio.sleep(1)
                await route.continue_()

            await page.route("**/api/workbench/release-notes", delay_release_notes)

            trigger = page.locator(".app-version-feature-trigger")
            await trigger.wait_for(state="visible", timeout=args.timeout_ms)
            if await trigger.get_attribute("aria-haspopup") != "dialog":
                raise AssertionError("Version overlay trigger is not a dialog control.")
            if await trigger.get_attribute("aria-controls") != "daaif-feature-list-dialog":
                raise AssertionError("Version overlay trigger targets the wrong dialog.")
            version_label = (
                await page.locator(".app-version-overlay-row")
                .filter(has_text="DAAIF Factory")
                .locator(".app-version-overlay-value")
                .inner_text()
            ).strip()
            before_url = page.url

            await trigger.click()
            dialog = page.locator("[data-feature-list-dialog]")
            await dialog.wait_for(state="visible", timeout=500)
            loading = dialog.locator("[data-feature-list-loading]")
            await loading.wait_for(state="visible", timeout=500)
            await page.wait_for_function(
                """() => (
                    document.activeElement?.matches('[data-feature-list-close]')
                    && !document.querySelector('[data-feature-list-loading]')
                )""",
                timeout=args.timeout_ms,
            )
            desktop = await page.evaluate(
                """
                () => {
                  const dialog = document.querySelector('[data-feature-list-dialog]');
                  const labelledBy = dialog.getAttribute('aria-labelledby') || '';
                  const describedBy = dialog.getAttribute('aria-describedby') || '';
                  const trigger = document.querySelector('.app-version-feature-trigger');
                  const triggerStyle = getComputedStyle(trigger);
                  const dialogRect = dialog.getBoundingClientRect();
                  const noteRect = dialog
                    .querySelector('[data-feature-list-note]')
                    .getBoundingClientRect();
                  const submitRect = dialog
                    .querySelector('[data-feature-list-submit]')
                    .getBoundingClientRect();
                  return {
                    expanded: trigger.getAttribute('aria-expanded'),
                    release: dialog.querySelector('[data-feature-list-release]').textContent.trim(),
                    title: dialog.querySelector('[data-feature-list-title]').textContent.trim(),
                    itemCount: dialog.querySelectorAll('.feature-list-item').length,
                    versionCount: dialog.querySelectorAll('[data-feature-list-version]').length,
                    versions: Array.from(dialog.querySelectorAll('[data-feature-list-version]'))
                      .map((node) => node.dataset.featureListVersion),
                    currentBadges: dialog.querySelectorAll('.feature-list-current-badge').length,
                    note: dialog.querySelector('[data-feature-list-note]').textContent.trim(),
                    labelled: Boolean(labelledBy && document.getElementById(labelledBy)),
                    described: Boolean(describedBy && document.getElementById(describedBy)),
                    closeFocused: document.activeElement.matches('[data-feature-list-close]'),
                    overlayPointerEvents: getComputedStyle(
                      document.querySelector('.app-version-overlay')
                    ).pointerEvents,
                    triggerPointerEvents: triggerStyle.pointerEvents,
                    triggerHeight: trigger.getBoundingClientRect().height,
                    fixedContentVisible:
                      noteRect.bottom <= dialogRect.bottom + 1
                      && submitRect.bottom <= dialogRect.bottom + 1,
                  };
                }
                """
            )
            if page.url != before_url:
                raise AssertionError("Opening the feature list changed the URL.")
            if desktop["expanded"] != "true" or not desktop["closeFocused"]:
                raise AssertionError("Dialog state or initial focus is incorrect.")
            if desktop["release"] != f"Featureliste · {version_label}":
                raise AssertionError("Dialog and version overlay show different versions.")
            if desktop["title"] != "Was kann DAAIF Factory?":
                raise AssertionError("Feature dialog title is incorrect.")
            if desktop["itemCount"] != expected_feature_count:
                raise AssertionError("Feature cards do not match the curated history.")
            if desktop["versionCount"] != len(expected_releases):
                raise AssertionError("Feature-list version groups are incomplete.")
            if desktop["versions"] != [
                release["version"] for release in expected_releases
            ]:
                raise AssertionError("Feature-list version order differs from metadata.")
            if desktop["currentBadges"] != 1:
                raise AssertionError("The current release is not marked exactly once.")
            if not desktop["note"].startswith(
                "Hinweis: Diese Liste beschreibt den aktuellen PoC-Stand."
            ):
                raise AssertionError("Feature dialog is missing the PoC note.")
            if not desktop["labelled"] or not desktop["described"]:
                raise AssertionError("Dialog accessible name or description is unresolved.")
            if not desktop["fixedContentVisible"]:
                raise AssertionError("Feature dialog note or close action is clipped.")
            if (
                desktop["overlayPointerEvents"] != "none"
                or desktop["triggerPointerEvents"] != "auto"
                or desktop["triggerHeight"] < 24
            ):
                raise AssertionError("Version overlay pointer or target-size contract regressed.")

            await page.keyboard.press("Escape")
            await page.wait_for_function(
                "() => !document.querySelector('[data-feature-list-dialog]').open",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                """() => (
                    document.activeElement?.matches('.app-version-feature-trigger')
                    && document.querySelector('.app-version-feature-trigger')
                      ?.getAttribute('aria-expanded') === 'false'
                )""",
                timeout=args.timeout_ms,
            )
            if await trigger.get_attribute("aria-expanded") != "false":
                raise AssertionError("Feature trigger stayed expanded after Escape.")

            await page.set_viewport_size({"width": 360, "height": 640})
            await page.wait_for_timeout(100)
            await trigger.click()
            await dialog.wait_for(state="visible", timeout=args.timeout_ms)
            mobile = await page.evaluate(
                """
                () => {
                  const rect = (selector) => {
                    const { left, top, right, bottom, width, height } =
                      document.querySelector(selector).getBoundingClientRect();
                    return { left, top, right, bottom, width, height };
                  };
                  const body = document.querySelector('.feature-list-dialog-body');
                  return {
                    overlay: rect('.app-version-overlay'),
                    dialog: rect('[data-feature-list-dialog]'),
                    close: rect('[data-feature-list-submit]'),
                    bodyOverflow: getComputedStyle(body).overflowY,
                    bodyScrolls: body.scrollHeight > body.clientHeight,
                    viewport: { width: innerWidth, height: innerHeight },
                  };
                }
                """
            )
            for name in ("overlay", "dialog", "close"):
                assert_inside_viewport(name, mobile[name], mobile["viewport"])
            if mobile["bodyOverflow"] != "auto" or not mobile["bodyScrolls"]:
                raise AssertionError("Feature cards do not scroll independently on mobile.")

            await page.mouse.click(1, 1)
            await page.wait_for_function(
                "() => !document.querySelector('[data-feature-list-dialog]').open",
                timeout=args.timeout_ms,
            )
            await page.wait_for_function(
                """() => (
                    document.activeElement?.matches('.app-version-feature-trigger')
                    && document.querySelector('.app-version-feature-trigger')
                      ?.getAttribute('aria-expanded') === 'false'
                )""",
                timeout=args.timeout_ms,
            )
            if page_errors:
                raise AssertionError(f"Browser page errors: {page_errors}")
        except (PlaywrightTimeoutError, AssertionError) as exc:
            print(str(exc), file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright feature-list smoke passed.")
    return 0


def main() -> int:
    return asyncio.run(run_smoke(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
