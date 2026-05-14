from __future__ import annotations

import argparse
import asyncio

from playwright.async_api import async_playwright


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exercise the topbar SSE connection status indicator."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--headless", action="store_true", default=True)
    parser.add_argument("--headed", dest="headless", action="store_false")
    parser.add_argument("--timeout-ms", type=int, default=30000)
    return parser.parse_args()


CONNECTED_EVENT_SOURCE_STUB = """
(() => {
  window.__eventSourceUrls = [];
  class FakeEventSource {
    constructor(url) {
      this.url = url;
      this.readyState = FakeEventSource.CONNECTING;
      window.__eventSourceUrls.push(url);
      setTimeout(() => {
        this.readyState = FakeEventSource.OPEN;
        if (typeof this.onopen === "function") {
          this.onopen(new Event("open"));
        }
      }, 25);
    }
    addEventListener() {}
    close() {
      this.readyState = FakeEventSource.CLOSED;
    }
  }
  FakeEventSource.CONNECTING = 0;
  FakeEventSource.OPEN = 1;
  FakeEventSource.CLOSED = 2;
  window.EventSource = FakeEventSource;
})();
"""


DISCONNECTED_EVENT_SOURCE_STUB = """
(() => {
  window.__eventSourceUrls = [];
  class FakeEventSource {
    constructor(url) {
      this.url = url;
      this.readyState = FakeEventSource.CONNECTING;
      window.__eventSourceUrls.push(url);
      setTimeout(() => {
        if (typeof this.onerror === "function") {
          this.onerror(new Event("error"));
        }
      }, 25);
    }
    addEventListener() {}
    close() {
      this.readyState = FakeEventSource.CLOSED;
    }
  }
  FakeEventSource.CONNECTING = 0;
  FakeEventSource.OPEN = 1;
  FakeEventSource.CLOSED = 2;
  window.EventSource = FakeEventSource;
})();
"""


async def open_home_with_stub(browser, args: argparse.Namespace, stub: str):
    context = await browser.new_context()
    await context.add_init_script(stub)
    page = await context.new_page()
    await page.goto(args.base_url.rstrip("/"), wait_until="domcontentloaded", timeout=args.timeout_ms)
    await page.locator("[data-home-page]").wait_for(state="visible", timeout=args.timeout_ms)
    await page.wait_for_function(
        "() => window.__eventSourceUrls?.some((url) => String(url).includes('/api/events/stream'))",
        timeout=args.timeout_ms,
    )
    return context, page


async def main() -> None:
    args = parse_args()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=args.headless)
        try:
            connected_context, connected_page = await open_home_with_stub(
                browser, args, CONNECTED_EVENT_SOURCE_STUB
            )
            try:
                await connected_page.wait_for_function(
                    """
                    () => {
                      const indicator = document.querySelector('[data-sse-connection-status]');
                      return indicator
                        && indicator.hidden
                        && indicator.dataset.connectionStatus === 'connected';
                    }
                    """,
                    timeout=args.timeout_ms,
                )
            finally:
                await connected_context.close()

            disconnected_context, disconnected_page = await open_home_with_stub(
                browser, args, DISCONNECTED_EVENT_SOURCE_STUB
            )
            try:
                await disconnected_page.wait_for_function(
                    """
                    () => {
                      const indicator = document.querySelector('[data-sse-connection-status]');
                      return indicator
                        && !indicator.hidden
                        && indicator.dataset.connectionStatus === 'disconnected'
                        && indicator.textContent.includes('Offline');
                    }
                    """,
                    timeout=args.timeout_ms,
                )
                layout_ok = await disconnected_page.evaluate(
                    """
                    () => {
                      const indicator = document.querySelector('[data-sse-connection-status]');
                      return indicator?.parentElement?.firstElementChild === indicator;
                    }
                    """
                )
                if not layout_ok:
                    raise RuntimeError("Offline indicator is not rendered before the workbench icons.")

                animation_name = await disconnected_page.locator(
                    ".topbar-connection-status-dot"
                ).evaluate("(node) => getComputedStyle(node).animationName")
                if "topbar-connection-status-pulse" not in animation_name:
                    raise RuntimeError(
                        f"Offline indicator dot is not blinking; animation={animation_name!r}."
                    )
            finally:
                await disconnected_context.close()
        finally:
            await browser.close()

    print("Playwright SSE connection status smoke passed.")


if __name__ == "__main__":
    asyncio.run(main())
