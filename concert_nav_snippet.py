# ============================================================
# app.py への追記スニペット
# ============================================================
#
# [1] ファイル冒頭の import ブロックに追加:
#
from concert.services.notion_client import build_concert_ctx
from concert.pages import (
    concert_mgmt,
    songs,
    players,
    rental,
)
#
# [2] 既存のナビゲーション分岐の末尾（最後の elif or else の直前）に追加:
#
# ---- Concert System ナビゲーション ----
st.sidebar.divider()
st.sidebar.markdown("**Concert System**")
CONCERT_PAGES = [
    "演奏会・練習管理",
    "楽曲・楽器管理",
    "奏者・出欠・アサイン",
    "レンタル管理",
]
concert_page = st.sidebar.radio(
    "concert_nav",
    CONCERT_PAGES,
    label_visibility="collapsed",
    key="concert_page_radio",
)

if concert_page == "演奏会・練習管理":
    concert_ctx = build_concert_ctx()
    concert_mgmt.render(concert_ctx)

elif concert_page == "楽曲・楽器管理":
    concert_ctx = build_concert_ctx()
    songs.render(concert_ctx)

elif concert_page == "奏者・出欠・アサイン":
    concert_ctx = build_concert_ctx()
    players.render(concert_ctx)

elif concert_page == "レンタル管理":
    concert_ctx = build_concert_ctx()
    rental.render(concert_ctx)
