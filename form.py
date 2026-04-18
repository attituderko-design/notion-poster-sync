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
        max-width: 760px;
        margin: 10vh auto 0;
        padding: 40px 34px;
        border: 1px solid rgba(180, 210, 255, 0.25);
        border-radius: 18px;
        background: rgba(8, 16, 38, 0.78);
        box-shadow: 0 25px 80px rgba(0, 0, 0, 0.5);
        backdrop-filter: blur(6px);
      }
      .eyebrow {
        margin: 0;
        color: #9bb8ff;
        font-size: 0.9rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
      }
      .hero-title {
        margin: 12px 0 10px;
        color: #f4f7ff;
        font-size: 2.2rem;
        line-height: 1.2;
        letter-spacing: 0.03em;
      }
      .hero-copy {
        margin: 0 0 24px;
        color: #d5e2ff;
        font-size: 1.02rem;
        line-height: 1.7;
      }
      .cta-row {
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        gap: 12px;
      }
      .cta-btn {
        display: inline-block;
        padding: 12px 18px;
        border-radius: 10px;
        background: linear-gradient(135deg, #9fd4ff 0%, #75a8ff 100%);
        color: #061126;
        font-weight: 700;
        text-decoration: none;
      }
      .cta-btn:hover {
        filter: brightness(1.05);
      }
      .plain-link {
        color: #9ec4ff;
        text-decoration: underline;
        word-break: break-all;
      }
      @media (max-width: 768px) {
        .hero-wrap {
          margin-top: 4vh;
          padding: 30px 22px;
        }
        .hero-title {
          font-size: 1.72rem;
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
        <a class="plain-link" href="{TARGET_URL}" target="_blank" rel="noopener noreferrer">
          {TARGET_URL}
        </a>
      </div>
    </section>
    """,
    unsafe_allow_html=True,
)
