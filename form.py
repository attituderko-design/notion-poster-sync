"""
artemis-cers/form.py
artemis-form.streamlit.app のエントリポイント。
実装は concert/pages/form.py に集約。

単一URLエントリ対応：
  - クエリパラメータなし → ログイン or 招待コードで演奏会を選択
  - concert=TOKEN&cid=CID → 従来の演奏会固定URL（後方互換）
"""
import streamlit as st
from pathlib import Path

_ASSET_BASE_URL = "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets"

def _get_asset(filename: str) -> str:
    local = Path(__file__).parent / "assets" / filename
    return str(local) if local.exists() else f"{_ASSET_BASE_URL}/{filename}"

st.set_page_config(
    page_title="HARMONIA",
    page_icon=_get_asset("favicon.png"),
    layout="centered",
)

from concert.services.notion_client import build_concert_ctx
from concert.pages.form import verify_form_token, render_form

try:
    _ctx = build_concert_ctx()
except Exception as _e:
    st.error(f"フォームの初期化に失敗しました: {_e}")
    st.stop()

_qp    = st.query_params
_token = _qp.get("concert", "")
_cid   = _qp.get("cid", "")

# 従来の演奏会固定URL（後方互換）
if _token and _cid:
    if verify_form_token(_token, _cid):
        try:
            render_form(_ctx, _cid)
        except Exception as _e:
            st.error(f"フォームの読み込みに失敗しました: {_e}")
    else:
        st.error("URLが無効です。正しいURLを使用してください。")
else:
    # 単一URLエントリ：concert_idなしで呼び出し
    try:
        render_form(_ctx)
    except Exception as _e:
        st.error(f"フォームの読み込みに失敗しました: {_e}")
