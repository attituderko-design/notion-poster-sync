#!/usr/bin/env python
"""
Streamlit app full-page screenshot capturer.

Usage:
  python tools/fullpage_capture.py --url http://localhost:8501 --outdir artifacts/screenshots
"""

from __future__ import annotations

import argparse
import re
import time
from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


HARMONIA_PAGES = [
    "🏠 ホーム",
    "練習管理",
    "楽曲・楽器管理",
    "奏者・出欠・持参楽器",
    "アサイン検討",
    "レンタル管理",
    "収支・振込管理",
    "🧪 テストデータ管理",
]

MUSE_MODES = [
    "新規登録",
    "出演アーカイブ",
    "データ管理",
    "出演情報管理",
    "自動同期",
]


def _slug(text: str) -> str:
    s = re.sub(r"[^\w\-]+", "_", text, flags=re.UNICODE).strip("_")
    return s or "page"


def _click_sidebar_text(page, text: str, timeout_ms: int = 2500) -> bool:
    sidebar = page.locator("section[data-testid='stSidebar']")
    loc = sidebar.get_by_text(text, exact=True)
    if loc.count() == 0:
        return False
    try:
        loc.first.click(timeout=timeout_ms)
        return True
    except PlaywrightTimeoutError:
        return False


def _capture(page, outdir: Path, order: int, name: str) -> None:
    out = outdir / f"{order:02d}_{_slug(name)}.png"
    page.screenshot(path=str(out), full_page=True)
    print(f"[saved] {out}")


def run(url: str, outdir: Path, delay_ms: int, include_muse_modes: bool) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1600, "height": 1200})
        page = context.new_page()
        page.goto(url, wait_until="networkidle")
        page.wait_for_selector("section[data-testid='stSidebar']", timeout=15000)
        time.sleep(delay_ms / 1000)

        order = 1
        _capture(page, outdir, order, "initial")
        order += 1

        # System mode: MUSE
        if _click_sidebar_text(page, "MUSE"):
            time.sleep(delay_ms / 1000)
            _capture(page, outdir, order, "MUSE")
            order += 1

            if include_muse_modes:
                for mode in MUSE_MODES:
                    if _click_sidebar_text(page, mode):
                        time.sleep(delay_ms / 1000)
                        _capture(page, outdir, order, f"MUSE_{mode}")
                        order += 1

        # System mode: HARMONIA
        if _click_sidebar_text(page, "HARMONIA"):
            time.sleep(delay_ms / 1000)
            _capture(page, outdir, order, "HARMONIA")
            order += 1

            for page_name in HARMONIA_PAGES:
                if _click_sidebar_text(page, page_name):
                    time.sleep(delay_ms / 1000)
                    _capture(page, outdir, order, f"HARMONIA_{page_name}")
                    order += 1

        context.close()
        browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture full-page screenshots for Streamlit app pages.")
    parser.add_argument("--url", default="http://localhost:8501", help="Streamlit app URL")
    parser.add_argument("--outdir", default="artifacts/screenshots", help="Output directory")
    parser.add_argument("--delay-ms", type=int, default=1200, help="Wait after each navigation")
    parser.add_argument("--include-muse-modes", action="store_true", help="Also capture MUSE mode radio pages")
    args = parser.parse_args()

    run(
        url=args.url,
        outdir=Path(args.outdir),
        delay_ms=args.delay_ms,
        include_muse_modes=args.include_muse_modes,
    )


if __name__ == "__main__":
    main()

