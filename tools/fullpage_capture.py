#!/usr/bin/env python
"""
Streamlit app full-page screenshot capturer.

Usage:
  python tools/fullpage_capture.py --url http://localhost:8501 --outdir artifacts/screenshots
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

try:
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api import sync_playwright
except Exception:
    PlaywrightError = Exception
    PlaywrightTimeoutError = Exception
    sync_playwright = None


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


def _launch_chromium(playwright, auto_install_browser: bool):
    def _to_runtime_error(err: Exception) -> RuntimeError:
        msg = str(err)
        if "error while loading shared libraries" in msg:
            return RuntimeError(
                "PlaywrightのLinux依存ライブラリが不足しています。"
                " `python -m playwright install --with-deps chromium` を実行するか、"
                " デプロイ環境では `packages.txt` に必要ライブラリ（例: libglib2.0-0, libnss3, libatk1.0-0, libx11-6）を追加してください。"
            )
        return RuntimeError(msg)

    try:
        return playwright.chromium.launch(headless=True)
    except PlaywrightError as e:
        msg = str(e)
        missing_browser = "Executable doesn't exist" in msg or "Please run the following command to download new browsers" in msg
        if not missing_browser:
            raise
        if not auto_install_browser:
            raise RuntimeError(
                "PlaywrightのChromiumが未インストールです。"
                " `python -m playwright install chromium` を実行してから再試行してください。"
            ) from e
        print("[info] Chromiumが未インストールのため、`python -m playwright install chromium` を実行します...")
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
        print("[info] Chromiumインストール完了。キャプチャを再開します。")
        try:
            return playwright.chromium.launch(headless=True)
        except PlaywrightError as e2:
            raise _to_runtime_error(e2) from e2


def run(url: str, outdir: Path, delay_ms: int, include_muse_modes: bool, auto_install_browser: bool) -> None:
    if sync_playwright is None:
        raise RuntimeError(
            "playwright が未インストールです。"
            " `pip install playwright` と `python -m playwright install chromium` を実行してください。"
        )

    outdir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = _launch_chromium(p, auto_install_browser=auto_install_browser)
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
    parser.add_argument("--auto-install-browser", action="store_true", help="Install Playwright Chromium automatically if missing")
    args = parser.parse_args()

    run(
        url=args.url,
        outdir=Path(args.outdir),
        delay_ms=args.delay_ms,
        include_muse_modes=args.include_muse_modes,
        auto_install_browser=args.auto_install_browser,
    )


if __name__ == "__main__":
    main()
