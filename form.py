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
        window.top.location.replace(target);
      }} catch (e) {{}}
      window.location.replace(target);
    </script>
    <div style="font-family:sans-serif;padding:8px 12px;font-size:14px;">
      新URLへ移動します。移動しない場合は
      <a href="{target}" target="_top" rel="noopener noreferrer">こちら</a>
    </div>
    <noscript>
      <meta http-equiv="refresh" content="0;url={target}">
    </noscript>
    """,
    height=72,
    width=0,
)
st.stop()


st.set_page_config(
    page_title="HARMONIA",
    page_icon="https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/favicon.png",
    layout="centered",
    initial_sidebar_state="collapsed",
)

_qp    = st.query_params
_token = _qp.get("concert", "")
_cid   = _qp.get("cid", "")

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
