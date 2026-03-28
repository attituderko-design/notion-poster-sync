import streamlit as st
from pathlib import Path


st.set_page_config(
    page_title="ArtéMis HARMONIA Form",
    page_icon="assets/favicon.png",
    layout="wide",
)

st.markdown(
    "<style>div.block-container{padding-top:1.0rem;}</style>",
    unsafe_allow_html=True,
)
_logo_local = Path(__file__).resolve().parent / "assets" / "logo.png"
if _logo_local.exists():
    st.image(str(_logo_local), width=320)
else:
    st.image("https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/logo.png", width=320)
st.title("ArtéMis HARMONIA")
st.caption("奏者入力フォーム")

try:
    from concert.pages.form import verify_form_token, render_form
    from concert.services.notion_client import build_concert_ctx
except Exception as e:
    st.error(f"フォームモジュールの読み込みに失敗しました: {e}")
    st.stop()

qp = st.query_params
token = qp.get("concert")
cid = qp.get("cid")

if not token or not cid:
    st.info("URLが不完全です。管理者から受け取ったフォームURLを開いてください。")
    st.stop()

if not verify_form_token(token, cid):
    st.error("URLが無効です。正しいフォームURLを使用してください。")
    st.stop()

try:
    form_ctx = build_concert_ctx()
    render_form(form_ctx, cid)
except KeyError as e:
    st.error(f"フォーム設定が不足しています。secretsを確認してください。（{e}）")
except Exception as e:
    st.error(f"フォームの読み込みに失敗しました: {e}")
