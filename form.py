"""
artemis-cers/form.py
Legacy Streamlit endpoint for artemis-form.streamlit.app.
This page now only shows the destination URL for manual navigation.
"""
import streamlit as st

TARGET_URL = "https://artemis-harmonia.com/"

st.set_page_config(
    page_title="HARMONIA",
    page_icon="https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/favicon.png",
    layout="centered",
    initial_sidebar_state="collapsed",
)

st.title("このURLは移行しました")
st.write("以下のURLからアクセスしてください。")
st.markdown(f"[{TARGET_URL}]({TARGET_URL})")
