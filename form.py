"""
artemis-cers/form.py
artemis-form.streamlit.app のエントリポイント。
実装は concert/pages/form.py に集約。
"""
import streamlit as st
from concert.services.notion_client import build_concert_ctx
from concert.pages.form import verify_form_token, render_form
import streamlit.components.v1 as components

target = "https://artemis-harmonia.com/"
components.html(
    f"""
    <script>
      const target = "{target}";
      try {{
        if (window.top) {{
          window.top.location.href = target;
        }} else {{
          window.location.href = target;
        }}
      }} catch (e) {{
        window.location.href = target;
      }}
    </script>
    <div style="
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      color: #f3f4f6;
      background: #111827;
      border: 1px solid #374151;
      border-radius: 10px;
      padding: 14px;
      margin: 8px;
      line-height: 1.6;
    ">
      新URLへ移動しています。移動しない場合は
      <a href="{target}" target="_top" rel="noopener noreferrer" style="color:#93c5fd;">
        こちらをクリック
      </a>
    </div>
    <noscript>
      <meta http-equiv="refresh" content="0;url={target}">
    </noscript>
    """,
    height=110,
)
st.stop()

st.set_page_config(
    page_title="HARMONIA",
    page_icon="https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/favicon.png",
    layout="centered",
    initial_sidebar_state="collapsed",
)

_qp = st.query_params
_token = _qp.get("concert", "")
_cid = _qp.get("cid", "")

if _token and _cid:
    try:
        if verify_form_token(_token, _cid):
            try:
                _ctx = build_concert_ctx()
                render_form(_ctx, _cid)
            except Exception as _e:
                st.error(f"フォームの読み込みに失敗しました: {_e}")
        else:
            st.error("URLが無効です。正しいURLを使用してください。")
    except Exception as _e:
        st.error(f"フォームの初期化に失敗しました: {_e}")
else:
    # クエリパラメータなし → 招待コード入力画面として通常起動
    try:
        _ctx = build_concert_ctx()
        render_form(_ctx)
    except Exception as _e:
        st.error(f"フォームの読み込みに失敗しました: {_e}")
