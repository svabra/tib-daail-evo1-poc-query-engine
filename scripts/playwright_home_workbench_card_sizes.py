from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from urllib.parse import urljoin

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_PATH = REPO_ROOT / "logs" / "playwright" / "home-workbench-card-sizes.png"
FEATURED_TITLES = {"Query Workbench", "Data Products Workbench"}
COMPACT_TITLES = {
    "Data Source Workbench",
    "Published Catalog",
    "Ingestion Workbench",
    "Loader Workbench",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify featured and compact home workbench cards render with "
            "equal heights and different widths. The target app must already be running."
        )
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=20000)
    return parser.parse_args()


async def open_home(page, args: argparse.Namespace) -> None:
    await page.goto(
        urljoin(args.base_url, "/"),
        wait_until="domcontentloaded",
        timeout=args.timeout_ms,
    )
    await page.locator("[data-home-page]").wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    await page.locator(".home-workbench-grid .home-workbench-card").first.wait_for(
        state="visible",
        timeout=args.timeout_ms,
    )
    await page.wait_for_timeout(800)


async def collect_card_metrics(page) -> dict[str, dict[str, object]]:
    return await page.eval_on_selector_all(
        ".home-workbench-grid .home-workbench-card",
        """
        (nodes) => Object.fromEntries(
          nodes.map((node) => {
            const title = node.querySelector(".home-workbench-title")?.textContent?.trim() || "";
            const rect = node.getBoundingClientRect();
            return [
              title,
              {
                height: Math.round(rect.height),
                width: Math.round(rect.width),
                className: node.className,
              },
            ];
          })
        )
        """,
    )


def verify_card_metrics(cards: dict[str, dict[str, object]]) -> None:
    expected_titles = FEATURED_TITLES | COMPACT_TITLES
    missing_titles = expected_titles - set(cards.keys())
    if missing_titles:
        raise RuntimeError(
            f"Home page is missing expected workbench cards: {sorted(missing_titles)}"
        )

    featured_heights: list[int] = []
    featured_widths: list[int] = []
    compact_heights: list[int] = []
    compact_widths: list[int] = []

    for title in FEATURED_TITLES:
        card = cards[title]
        if "home-workbench-card-featured" not in str(card.get("className") or ""):
            raise RuntimeError(f"{title!r} is missing the featured card class.")
        featured_heights.append(int(card["height"]))
        featured_widths.append(int(card["width"]))

    for title in COMPACT_TITLES:
        card = cards[title]
        if "home-workbench-card-compact" not in str(card.get("className") or ""):
            raise RuntimeError(f"{title!r} is missing the compact card class.")
        compact_heights.append(int(card["height"]))
        compact_widths.append(int(card["width"]))

    if max(featured_heights) - min(featured_heights) > 6:
        raise RuntimeError(
            "Featured workbench cards do not share a consistent height. "
            f"featured_heights={featured_heights}"
        )

    if max(compact_heights) - min(compact_heights) > 6:
        raise RuntimeError(
            "Compact workbench cards do not share a consistent height. "
            f"compact_heights={compact_heights}"
        )

    if abs(min(featured_heights) - min(compact_heights)) > 6:
        raise RuntimeError(
            "Featured and compact workbench cards are not rendering at the same height. "
            f"featured_heights={featured_heights}, compact_heights={compact_heights}"
        )

    if min(featured_widths) < max(compact_widths) * 1.8:
        raise RuntimeError(
            "Featured workbench cards are not visibly wider than compact cards. "
            f"featured_widths={featured_widths}, compact_widths={compact_widths}"
        )


async def run_smoke(args: argparse.Namespace) -> int:
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        page = await browser.new_page(
            viewport={"width": 2048, "height": 900},
            base_url=args.base_url.rstrip("/"),
        )
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
            await open_home(page, args)
            cards = await collect_card_metrics(page)
            verify_card_metrics(cards)
        except (PlaywrightTimeoutError, RuntimeError) as exc:
            ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
            await page.screenshot(path=str(ARTIFACT_PATH), full_page=True)
            print(str(exc), file=sys.stderr)
            for message in console_messages:
                print(message, file=sys.stderr)
            print(f"Saved screenshot: {ARTIFACT_PATH}", file=sys.stderr)
            await browser.close()
            return 1

        await browser.close()

    print("Playwright home workbench card size smoke passed.")
    return 0


def main() -> int:
    args = parse_args()
    return asyncio.run(run_smoke(args))


if __name__ == "__main__":
    raise SystemExit(main())
