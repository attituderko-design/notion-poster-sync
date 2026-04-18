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

st.markdown(
    """
    <style>
      .stApp {
        background:
          radial-gradient(1200px 600px at 10% -20%, #15306c 0%, rgba(21,48,108,0) 60%),
          radial-gradient(900px 500px at 95% 5%, #2a4ea7 0%, rgba(42,78,167,0) 55%),
          linear-gradient(160deg, #050a1a 0%, #0a1533 45%, #0d2047 100%);
      }
      .hero-wrap {
        max-width: 740px;
        margin: 8vh auto 0;
        padding: 34px 30px;
        border: 1px solid rgba(180, 210, 255, 0.25);
        border-radius: 16px;
        background: rgba(8, 16, 38, 0.78);
        box-shadow: 0 20px 60px rgba(0, 0, 0, 0.45);
        backdrop-filter: blur(6px);
      }
      .eyebrow {
        margin: 0;
        color: #9bb8ff;
        font-size: 0.82rem;
        letter-spacing: 0.1em;
        text-transform: uppercase;
      }
      .hero-title {
        margin: 10px 0 12px;
        color: #f4f7ff;
        font-size: 1.7rem;
        line-height: 1.35;
        letter-spacing: 0.01em;
      }
      .hero-copy {
        margin: 0 0 24px;
        color: #d5e2ff;
        font-size: 0.98rem;
        line-height: 1.75;
      }
      .cta-row {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 10px;
      }
      .cta-btn {
        display: inline-block;
        padding: 10px 16px;
        border-radius: 9px;
        background: linear-gradient(135deg, #8fc6ff 0%, #6d9eff 100%);
        color: #081a3b;
        font-weight: 700;
        text-decoration: none;
      }
      .cta-btn:hover {
        filter: brightness(1.05);
      }
      .sub-note {
        margin: 0;
        color: #aac2ef;
        font-size: 0.86rem;
      }
      @media (max-width: 768px) {
        .hero-wrap {
          margin-top: 4vh;
          padding: 28px 20px;
        }
        .hero-title {
          font-size: 1.4rem;
        }
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    f"""
    <section class="hero-wrap">
      <p class="eyebrow">Artemis Harmonia</p>
      <h1 class="hero-title">新生HARMONIAへ移行しました</h1>
      <p class="hero-copy">
        旧フォームURLはクローズし、運用を <strong>HARMONIA Web</strong> に一本化しています。<br>
        下のボタンから新しいポータルへアクセスしてください。
      </p>
      <div class="cta-row">
        <a class="cta-btn" href="{TARGET_URL}" target="_blank" rel="noopener noreferrer">
          HARMONIAを開く
        </a>
      </div>
      <p class="sub-note">遷移先: {TARGET_URL}</p>
    </section>
    """,
    unsafe_allow_html=True,
)
