"""
concert/pages/form.py
奏者向け入力フォーム（URLトークンでアクセス制御）
"""
import streamlit as st
import hashlib, hmac, random, string
import requests as _requests
from datetime import datetime, timedelta
from pathlib import Path

from concert.services.keys import (
    CONCERT_NAME_KEYS, CONCERT_DATE_KEYS, CONCERT_VENUE_KEYS,
    CONCERT_CONDUCTOR_KEYS, CONCERT_SOLOIST_KEYS,
    PRACTICE_NAME_KEYS, PRACTICE_DATE_KEYS, PRACTICE_VENUE_KEYS,
    PRACTICE_CONCERT_REL_KEYS, PRACTICE_CONCERT_DAY_KEYS,
    PARTICIPANT_PART_REL_KEYS, PARTICIPANT_ROLE_KEYS, PARTICIPANT_ROLE_OPS_KEYS,
    PARTICIPANT_FEE_KEYS, PARTICIPANT_PAID_KEYS, PARTICIPANT_CONCERT_REL_KEYS,
    PARTICIPANT_PLAYER_REL_KEYS, PARTICIPANT_RECORD_KEYS, PARTICIPANT_SYSTEM_ROLE_KEYS,
    PARTDEF_NAME_KEYS, PARTDEF_DISPLAY_NAME_KEYS, PARTDEF_SONG_REL_KEYS, PARTDEF_INST_REL_KEYS, PARTDEF_PART_REL_KEYS,
    PARTMASTER_NAME_KEYS, PARTMASTER_TYPE_KEYS,
    SONG_NAME_KEYS, SONG_CREATOR_KEYS, SONG_CONCERT_REL_KEYS,
    INSTRUMENT_NAME_KEYS,
    PLAYER_NAME_KEYS, PLAYER_HN_KEYS, PLAYER_EMAIL_KEYS, PLAYER_RECEIVE_KEYS,
    PLAYER_PHONE_KEYS, PLAYER_LINE_KEYS, PLAYER_PASSWORD_HASH_KEYS,
    ATTENDANCE_KEY_KEYS, ATT_RECORD_KEYS, ATT_PLAYER_REL_KEYS, ATT_PRACTICE_REL_KEYS, ATT_STATUS_KEYS, ATT_NOTE_KEYS,
    PI_PARTICIPANT_REL_KEYS, PI_PLAYER_REL_KEYS, PI_INST_REL_KEYS, PI_CONCERT_REL_KEYS, PI_OWN_COUNT_KEYS,
    PREFERENCE_KEY_KEYS, PREF_PLAYER_REL_KEYS, PREF_PART_REL_KEYS, PREF_PRIORITY_KEYS,
    CONCERT_CONFIRMED_FEE_KEYS,
    HARMONIA_CONCERT_PLAN_KEYS, HARMONIA_CONCERT_ASSIGN_KEYS, HARMONIA_CONCERT_INVITE_CODE_KEYS,
)
from concert.services.relation_utils import find_relation_prop
from concert.services.song_utils import get_song_display_name, build_song_name_map

_TOKEN_SECRET = "harmonia_form_2024"
PRIORITY_OPTS = ["未回答", "第1希望", "第2希望", "第3希望", "希望なし/降り番でも可", "NG"]
ATT_OPTS      = ["○", "△", "×"]

# フォーム用クッキーマネージャー（モジュールレベルで一度だけインスタンス化）
_FORM_COOKIE_MGR = None
try:
    from streamlit_cookies_controller import CookieController
    _FORM_COOKIE_MGR = CookieController()
except Exception:
    pass
OTHER_PART    = "一覧にない（管理者に連絡）"

def IS_PERC(part_name_or_type: str) -> bool:
    """打楽器かどうかを判定。
    PART_MASTERの種別（"打楽器"）またはパート名（旧来の"perc"等）どちらでも対応。
    """
    v = (part_name_or_type or "").strip()
    return v == "打楽器" or v.lower() in ("perc", "percussion")


# ── ロール定数 ────────────────────────────────────────────────
ROLE_PLAYER  = 0
ROLE_LEADER  = 1
ROLE_MANAGER = 2
_ROLE_MAP = {"Manager": ROLE_MANAGER, "Leader": ROLE_LEADER, "Player": ROLE_PLAYER}


def _resolve_user_role(ctx, player_id: str, concert_id: str) -> int:
    """CONCERT_CASTのシステムロールフィールドから権限レベルを返す。
    未設定またはPlayerの場合はROLE_PLAYER(0)を返す。
    """
    try:
        all_cast = st.session_state.get("form_participant_rows_concert")
        if all_cast is None:
            all_cast = [
                r for r in ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
                if concert_id in ctx["extract_relation_ids_any"](r, PARTICIPANT_CONCERT_REL_KEYS)
            ]
        for r in all_cast:
            pids = ctx["extract_relation_ids_any"](r, PARTICIPANT_PLAYER_REL_KEYS)
            if player_id in pids:
                role_str = ctx["extract_prop_text_any"](r, PARTICIPANT_SYSTEM_ROLE_KEYS) or ""
                return _ROLE_MAP.get(role_str, ROLE_PLAYER)
    except Exception:
        pass
    return ROLE_PLAYER

def _get_part_master_map(ctx) -> dict[str, dict]:
    """PART_MASTERをquery_allしてid→{name, type}のdictを返す。"""
    try:
        rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
        ext  = ctx["extract_prop_text_any"]
        return {
            r.get("id", ""): {
                "name": ext(r, PARTMASTER_NAME_KEYS) or "",
                "type": ext(r, PARTMASTER_TYPE_KEYS) or "",
            }
            for r in rows
        }
    except Exception:
        return {}

def _resolve_part_type(part_master_id: str) -> str:
    """PART_MASTERのIDから種別文字列（"打楽器" etc.）を返す。session_stateのmapを参照。"""
    pm_map: dict = st.session_state.get("form_part_master_map") or {}
    return pm_map.get(part_master_id, {}).get("type", "")


def _get_part_leader_recipients(
    ctx: dict,
    concert_id: str,
    part_master_id: str,
    exclude_player_id: str = "",
) -> list[dict]:
    """同一演奏会・同一パートのLeader宛先を返す。"""
    if not concert_id or not part_master_id:
        return []
    try:
        ext = ctx["extract_prop_text_any"]
        participants = st.session_state.get("form_participant_rows_concert")
        if participants is None:
            participants = [
                r for r in ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
                if concert_id in ctx["extract_relation_ids_any"](r, PARTICIPANT_CONCERT_REL_KEYS)
            ]
        players = st.session_state.get("form_all_players")
        if players is None:
            players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
        player_map = {p.get("id", ""): p for p in players}

        recipients: list[dict] = []
        seen: set[str] = set()
        for row in participants:
            role_name = (ext(row, PARTICIPANT_SYSTEM_ROLE_KEYS) or "").strip()
            if role_name != "Leader":
                continue
            row_part_ids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PART_REL_KEYS)
            if part_master_id not in row_part_ids:
                continue
            row_player_ids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
            for pid in row_player_ids:
                if pid == exclude_player_id:
                    continue
                pl = player_map.get(pid, {})
                email = (ext(pl, PLAYER_EMAIL_KEYS) or "").strip()
                if not email:
                    continue
                key = email.lower()
                if key in seen:
                    continue
                seen.add(key)
                recipients.append({
                    "name": (ext(pl, PLAYER_NAME_KEYS) or "パートリーダー").strip(),
                    "email": email,
                })
        return recipients
    except Exception:
        return []


def _render_next_practice_panel(ctx: dict, practices: list) -> None:
    """直近の練習カード（PDF生成含む）を表示。"""
    ext = ctx["extract_prop_text_any"]
    from datetime import date as _date_cls

    today = _date_cls.today()
    next_practice = None
    for pr in practices:
        pr_date_str = ext(pr, PRACTICE_DATE_KEYS) or ""
        if pr_date_str and len(pr_date_str) >= 10:
            try:
                pr_d = _date_cls.fromisoformat(pr_date_str[:10])
                if pr_d >= today:
                    if next_practice is None or pr_d < _date_cls.fromisoformat(ext(next_practice, PRACTICE_DATE_KEYS)[:10]):
                        next_practice = pr
            except Exception:
                pass
    if not next_practice:
        return

    pr_id_next    = next_practice.get("id", "")
    pr_name_next  = ext(next_practice, PRACTICE_NAME_KEYS) or "練習"
    pr_date_next  = ext(next_practice, PRACTICE_DATE_KEYS) or ""
    pr_venue_next = ext(next_practice, PRACTICE_VENUE_KEYS) or ""
    pr_addr_next  = ext(next_practice, ["会場住所", "住所", "Address"]) or ""
    pr_date_disp  = pr_date_next[:10] if pr_date_next else ""
    pr_time_disp  = pr_date_next[11:16] if len(pr_date_next) > 10 else ""
    try:
        _wd = _date_cls.fromisoformat(pr_date_disp)
        pr_weekday = ["月", "火", "水", "木", "金", "土", "日"][_wd.weekday()]
    except Exception:
        pr_weekday = ""

    with st.container(border=True):
        st.caption("📅 直近の練習")
        st.markdown(f"**{pr_name_next}**")
        if pr_date_disp:
            _label = f"{pr_date_disp}（{pr_weekday}）" if pr_weekday else pr_date_disp
            if pr_time_disp:
                _label += f" {pr_time_disp}"
            st.caption(f"🗓 {_label}")
        if pr_venue_next:
            st.caption(f"📍 {pr_venue_next}")
        if pr_addr_next:
            import urllib.parse
            _maps_url = "https://www.google.com/maps/search/?api=1&query=" + urllib.parse.quote(pr_addr_next)
            st.markdown(f"[🗺 Google Mapsで開く]({_maps_url})")

        _pdf_key = f"form_pdf_bytes_{pr_id_next}"
        if _pdf_key not in st.session_state:
            if st.button("📄 練習情報PDFを生成する", key=f"gen_pdf_{pr_id_next}", use_container_width=True):
                with st.spinner("PDF生成中..."):
                    try:
                        from concert.services.practice_report import generate_practice_report
                        _pdf_bytes = generate_practice_report(ctx, pr_id_next)
                        st.session_state[_pdf_key] = _pdf_bytes
                        st.rerun()
                    except Exception as _e:
                        st.error(f"PDF生成に失敗しました: {_e}")
        else:
            from concert.services.convert_utils import render_report_output
            render_report_output(
                st.session_state[_pdf_key],
                filename=f"練習情報PDF_{pr_name_next.replace('/', '-')}",
                label="練習情報PDF",
                key_prefix=f"player_{pr_id_next}",
            )
            if st.button("🔄 PDFを再生成", key=f"regen_pdf_{pr_id_next}", use_container_width=True):
                st.session_state.pop(_pdf_key, None)
                st.rerun()


def _collapsible_open(title: str, key: str, default_open: bool = False) -> bool:
    """st.expander代替の折りたたみ。文字化け耐性のためシンプルなボタンで実装。"""
    state_key = f"form_collapse_{key}"
    if state_key not in st.session_state:
        st.session_state[state_key] = bool(default_open)
    is_open = bool(st.session_state.get(state_key, False))
    icon = "▾" if is_open else "▸"
    if st.button(f"{icon} {title}", key=f"{state_key}_btn", use_container_width=True):
        st.session_state[state_key] = not is_open
        st.rerun()
    return bool(st.session_state.get(state_key, False))


def _render_brand_logo() -> None:
    """フォーム上部ワードマークを表示。"""
    st.html("""
        <div class="h-f1" style="padding: 16px 0 14px; text-align: center;">
            <div class="h-wordmark">ArtéMis HARMONIA</div>
            <div class="h-wordmark-sub">Concert Management</div>
        </div>
        """)


def _inject_form_styles() -> None:
    """form.py専用スタイル — モダンダークミニマル。"""
    st.html("""
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600&family=Noto+Sans+JP:wght@300;400;500&display=swap" rel="stylesheet">
        <style>
        /* ── base ── */
        :root {
            --h-container-max: 480px;
            --h-font-body: 15px;
            --h-font-sub: 13px;
            --h-font-heading: 20px;
            --h-font-button: 15px;
            --h-font-caption: 12px;
            --h-tap-min: 48px;
            --h-pad-x: clamp(.8rem, 4vw, 1.4rem);
        }
        .stApp, .stApp * { font-family: 'Noto Sans JP', sans-serif !important; }
        .stApp .block-container {
            max-width: var(--h-container-max) !important;
            padding-top: 1.2rem !important;
            padding-left: var(--h-pad-x) !important;
            padding-right: var(--h-pad-x) !important;
        }
        @media (max-width: 359px) { :root { --h-container-max: 100vw; --h-font-body: 14px; --h-font-sub: 12px; --h-font-heading: 18px; --h-font-button: 14px; --h-font-caption: 12px; --h-tap-min: 48px; --h-pad-x: .6rem; } }
        @media (min-width: 360px) and (max-width: 639px) { :root { --h-container-max: 480px; --h-font-body: 15px; --h-font-sub: 13px; --h-font-heading: 20px; --h-font-button: 15px; --h-font-caption: 12px; --h-tap-min: 48px; --h-pad-x: clamp(.75rem, 3.2vw, 1.1rem); } }
        @media (min-width: 640px) and (max-width: 1023px) { :root { --h-container-max: 700px; --h-font-body: 16px; --h-font-sub: 14px; --h-font-heading: 23px; --h-font-button: 16px; --h-font-caption: 13.5px; --h-tap-min: 48px; --h-pad-x: clamp(1rem, 3vw, 1.7rem); } }
        @media (min-width: 1024px) { :root { --h-container-max: 920px; --h-font-body: 16px; --h-font-sub: 14px; --h-font-heading: 26px; --h-font-button: 16px; --h-font-caption: 14px; --h-tap-min: 46px; --h-pad-x: clamp(1.1rem, 2.4vw, 2rem); } }

        /* メニューカードの裏にある実ボタンを隠し、カードクリックで発火させる */
        .st-key-menu_att, .st-key-menu_pref, .st-key-menu_pref_disabled, .st-key-menu_own {
            display: none !important;
        }

        /* ── fade-in animations ── */
        @keyframes hFadeUp {
            from { opacity: 0; transform: translateY(10px); }
            to   { opacity: 1; transform: translateY(0); }
        }
        .h-f1 { animation: hFadeUp .35s ease both; }
        .h-f2 { animation: hFadeUp .35s .08s ease both; }
        .h-f3 { animation: hFadeUp .35s .16s ease both; }
        .h-f4 { animation: hFadeUp .35s .24s ease both; }
        .h-f5 { animation: hFadeUp .35s .32s ease both; }
        .h-f6 { animation: hFadeUp .35s .40s ease both; }

        /* ── wordmark ── */
        .h-wordmark {
            font-family: 'Outfit', sans-serif !important;
            font-size: calc(var(--h-font-heading) + 1px); font-weight: 600;
            color: #e8edf7; letter-spacing: .05em;
        }
        .h-wordmark-sub {
            font-family: 'Outfit', sans-serif !important;
            font-size: var(--h-font-caption); color: rgba(160,180,220,.4);
            letter-spacing: .13em; margin-top: 3px;
        }

        /* ── concert header card ── */
        .h-concert-card {
            background: rgba(74,158,255,.07);
            border: 0.5px solid rgba(74,158,255,.18);
            border-radius: 14px;
            padding: 14px 16px;
            margin-bottom: 14px;
        }
        .h-concert-name {
            font-family: 'Outfit', sans-serif !important;
            font-size: calc(var(--h-font-heading) - 2px); font-weight: 500;
            color: #e8edf7; margin-bottom: 8px; line-height: 1.3;
        }
        .h-concert-row {
            display: flex; align-items: center;
            gap: 8px; font-size: var(--h-font-sub);
            color: rgba(160,180,220,.6); margin-bottom: 3px;
        }
        .h-concert-dot {
            width: 4px; height: 4px; border-radius: 50%;
            background: rgba(74,158,255,.55); flex-shrink: 0;
        }

        /* ── greeting ── */
        .h-greet {
            font-size: var(--h-font-body); color: rgba(160,180,220,.7);
            margin-bottom: 14px; line-height: 1.6;
        }
        .h-greet strong { color: #e8edf7; font-weight: 500; }
        .h-greet-part {
            display: block; font-size: var(--h-font-caption);
            color: rgba(160,180,220,.35); margin-top: 1px;
        }

        /* ── menu items ── */
        .h-menu-item {
            display: flex; align-items: center;
            background: rgba(255,255,255,.035);
            border: 0.5px solid rgba(255,255,255,.08);
            border-radius: 13px;
            padding: 13px 14px;
            margin-bottom: 8px;
            min-height: 56px; gap: 12px;
            cursor: pointer;
            transition: border-color .15s ease, background .15s ease;
        }
        .h-menu-item:hover {
            background: rgba(255,255,255,.055);
            border-color: rgba(74,158,255,.3);
        }
        .h-mi-icon {
            width: 36px; height: 36px; border-radius: 9px;
            display: flex; align-items: center; justify-content: center;
            font-size: 16px; flex-shrink: 0;
        }
        .h-ic-a { background: rgba(74,158,255,.13); }
        .h-ic-b { background: rgba(100,200,130,.1); }
        .h-ic-c { background: rgba(220,160,60,.1); }
        .h-ic-d { background: rgba(180,100,240,.1); }
        .h-mi-body { flex: 1; min-width: 0; }
        .h-mi-ttl {
            font-family: 'Outfit', sans-serif !important;
            font-size: var(--h-font-body); color: #c8d4ed;
            white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        }
        .h-mi-hint { font-size: var(--h-font-caption); color: rgba(160,180,220,.4); margin-top: 2px; }
        .h-mi-right { display: flex; align-items: center; gap: 6px; flex-shrink: 0; }
        .h-badge {
            font-family: 'Outfit', sans-serif !important;
            font-size: var(--h-font-caption); background: rgba(74,158,255,.15);
            color: #4a9eff; border-radius: 6px;
            padding: 3px 8px; white-space: nowrap;
        }
        .h-badge-ok { background: rgba(100,200,130,.12); color: #64c882; }
        .h-chev { font-size: 14px; color: rgba(160,180,220,.25); }

        /* ── section label ── */
        .h-section {
            font-family: 'Outfit', sans-serif !important;
            font-size: var(--h-font-caption); color: rgba(160,180,220,.4);
            letter-spacing: .1em; text-transform: uppercase;
            margin-bottom: 8px; margin-top: 4px;
        }

        /* ── step indicator ── */
        .h-steps {
            display: flex; align-items: center;
            gap: 5px; margin-bottom: 16px;
        }
        .h-s-dot {
            width: 5px; height: 5px; border-radius: 50%;
            background: rgba(255,255,255,.12);
        }
        .h-s-dot.active { background: #4a9eff; width: 16px; border-radius: 3px; }
        .h-s-dot.done   { background: rgba(100,200,130,.55); }
        .h-s-line { flex: 1; height: .5px; background: rgba(255,255,255,.06); margin-left: 2px; }
        .h-s-lbl {
            font-family: 'Outfit', sans-serif !important;
            font-size: 11px; color: rgba(160,180,220,.35);
        }

        /* ── screen title ── */
        .h-scr-title {
            font-family: 'Outfit', sans-serif !important;
            font-size: var(--h-font-heading); font-weight: 500; color: #e8edf7;
            margin-bottom: 4px;
        }
        .h-scr-sub {
            font-size: var(--h-font-sub); color: rgba(160,180,220,.55);
            margin-bottom: 16px; line-height: 1.55;
        }

        /* ── footer ── */
        .h-footer {
            font-family: 'Outfit', sans-serif !important;
            font-size: var(--h-font-caption); color: rgba(160,180,220,.18);
            text-align: center; margin-top: 20px;
            padding-top: 14px;
            border-top: .5px solid rgba(255,255,255,.05);
        }

        /* ── streamlit overrides ── */
        .stButton > button, .stDownloadButton > button {
            border-radius: 13px !important;
            border: .5px solid rgba(255,255,255,.08) !important;
            min-height: var(--h-tap-min) !important;
            font-family: 'Outfit', 'Noto Sans JP', sans-serif !important;
            font-size: var(--h-font-button) !important;
            background: rgba(255,255,255,.035) !important;
            color: #dbe5f9 !important;
            transition: border-color .15s ease, background .15s ease;
        }
        .stButton > button:hover, .stDownloadButton > button:hover {
            border-color: rgba(74,158,255,.5) !important;
            background: rgba(74,158,255,.08) !important;
        }
        div[data-testid="stFormSubmitButton"] > button {
            border-radius: 13px !important;
            border: .5px solid rgba(255,255,255,.08) !important;
            min-height: var(--h-tap-min) !important;
            font-family: 'Outfit', 'Noto Sans JP', sans-serif !important;
            font-size: var(--h-font-button) !important;
            background: rgba(255,255,255,.035) !important;
            color: #dbe5f9 !important;
        }
        .stTextInput input, .stTextArea textarea,
        .stSelectbox [data-baseweb="select"] > div,
        .stMultiSelect [data-baseweb="select"] > div {
            border-radius: 11px !important;
            border: .5px solid rgba(255,255,255,.12) !important;
            background: rgba(255,255,255,.04) !important;
            font-size: var(--h-font-body) !important;
        }
        .stSelectbox [data-baseweb="select"] *,
        .stMultiSelect [data-baseweb="select"] *,
        .stDateInput *, .stNumberInput *, .stRadio *,
        .stTabs [data-baseweb="tab-list"] *, .stProgress * {
            font-family: 'Outfit', 'Noto Sans JP', sans-serif !important;
        }
        .stTabs [data-baseweb="tab"] {
            font-family: 'Outfit', 'Noto Sans JP', sans-serif !important;
            font-size: var(--h-font-button) !important;
        }
        .stTextInput input:focus, .stTextArea textarea:focus {
            border-color: rgba(74,158,255,.45) !important;
            box-shadow: 0 0 0 3px rgba(74,158,255,.1) !important;
        }
        [data-testid="stExpander"] {
            border: .5px solid rgba(255,255,255,.09) !important;
            border-radius: 13px !important;
            overflow: hidden;
            background: rgba(255,255,255,.025) !important;
        }
        [data-testid="stMetricValue"] { font-size: var(--h-font-body) !important; }
        [data-testid="stMetricLabel"] > div {
            font-size: var(--h-font-caption) !important;
            color: rgba(160,180,220,.55) !important;
        }
        .stRadio label { font-size: var(--h-font-body) !important; }
        .stCheckbox label { font-size: var(--h-font-body) !important; min-height: 28px; }
        p, li, .stMarkdown p { font-size: var(--h-font-body) !important; line-height: 1.7 !important; }
        label[data-testid="stWidgetLabel"] { font-size: var(--h-font-sub) !important; color: rgba(160,180,220,.65) !important; }
        .stAlert p { font-size: var(--h-font-sub) !important; }
        </style>
        <script>
        (() => {
          const bindCard = (id, key) => {
            const card = document.getElementById(id);
            const btn = document.querySelector(`.st-key-${key} button`);
            if (!card || !btn || card.dataset.bound === "1") return;
            card.dataset.bound = "1";
            card.style.cursor = "pointer";
            card.addEventListener("click", () => btn.click());
          };
          const run = () => {
            bindCard("menu-att", "menu_att");
            bindCard("menu-pref", "menu_pref");
            bindCard("menu-own", "menu_own");
          };
          run();
          const mo = new MutationObserver(run);
          mo.observe(document.body, { childList: true, subtree: true });
        })();
        </script>
        """)


# ── マジックコード認証 ──────────────────────────────────────────

_CODE_EXPIRY_MINUTES = 10
_CODE_MAX_ATTEMPTS   = 5
_CODE_RESEND_SECONDS = 30
_INVITE_MAX_ATTEMPTS = 10
_INVITE_WINDOW_MINUTES = 10

def _generate_code() -> str:
    return "".join(random.choices(string.digits, k=6))

def _hash_code(code: str) -> str:
    """認証コードをSHA-256ハッシュ化して返す。平文をsession_stateに持たない。"""
    return hashlib.sha256(code.encode()).hexdigest()

def _verify_code(entered: str, stored_hash: str) -> bool:
    return hmac.compare_digest(_hash_code(entered.strip()), stored_hash)

def _send_magic_code(ctx: dict, email: str, code: str, concert_name: str) -> bool:
    try:
        from concert.services.mailer import send_text_to_all
        subject = "ArteMis HARMONIA 認証コード: " + code
        body = (
            "ArteMis HARMONIA フォームへのアクセス認証コードです。\n\n"
            "演奏会: " + concert_name + "\n\n"
            "認証コード: " + code + "\n\n"
            "このコードは" + str(_CODE_EXPIRY_MINUTES) + "分間有効です。\n"
            "心当たりがない場合はこのメールを無視してください。"
        )
        result = send_text_to_all(
            ctx,
            [{"name": "", "email": email}],
            subject, body,
        )
        return len(result.sent) > 0
    except Exception:
        return False

def _is_code_valid() -> bool:
    expires = st.session_state.get("auth_code_expires")
    if not expires:
        return False
    return datetime.now() < expires

# ── パスワード管理 ────────────────────────────────────────────

def _hash_password(password: str) -> str:
    """パスワードをSHA-256ハッシュ化して返す。"""
    return hashlib.sha256(password.strip().encode()).hexdigest()

def _verify_password(entered: str, stored_hash: str) -> bool:
    """パスワードを検証する。"""
    if not entered or not stored_hash:
        return False
    return hmac.compare_digest(_hash_password(entered), stored_hash)

def _save_password_hash(ctx: dict, player_id: str, password: str) -> bool:
    """PERFORMERのpassword_hashを更新する。"""
    t_pl = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
    if not t_pl:
        return False
    props: dict = {}
    ctx["put_prop_any"](props, t_pl, PLAYER_PASSWORD_HASH_KEYS, _hash_password(password))
    res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{player_id}",
                              json={"properties": props})
    return res is not None and res.status_code == 200

def _get_proposal_flag(ctx: dict, concert_id: str) -> bool:
    """HARMONIA_CONCERTの表示解禁フラグを取得する。
    「案提示」または「アサイン確定」のいずれかが真なら表示可。
    """
    def _is_true(v: str) -> bool:
        return (v or "").strip().lower() in ("true", "1", "yes", "y", "on")

    try:
        hc_db = ctx.get("CONCERT_DB_HARMONIA_CONCERT", "")
        if not hc_db:
            return False
        hc_rows = st.session_state.get("form_harmonia_rows")
        if hc_rows is None:
            hc_rows = ctx["query_all"](hc_db, None)
        hc_row = next(
            (r for r in hc_rows
             if concert_id in ctx["extract_relation_ids_any"](r, ["演奏会", "FK演奏会", "concert"])),
            None
        )
        if not hc_row:
            return False
        proposal_val = ctx["extract_prop_text_any"](hc_row, HARMONIA_CONCERT_PLAN_KEYS)
        assign_val   = ctx["extract_prop_text_any"](hc_row, HARMONIA_CONCERT_ASSIGN_KEYS)
        return _is_true(proposal_val) or _is_true(assign_val)
    except Exception:
        return False

def _has_published_assignments(ctx: dict, concert_id: str) -> bool:
    """CONCERT_ASSIGNMENTに公開済み（担当フラグ=True）が1件でもあるか。"""
    try:
        from concert.services.keys import (
            ASSIGNMENT_CONCERT_REL_KEYS, ASSIGNMENT_FLAG_KEYS,
        )
        ext = ctx["extract_prop_text_any"]
        ext_rel = ctx["extract_relation_ids_any"]
        all_assign = st.session_state.get("form_assignment_rows")
        if all_assign is None:
            all_assign = ctx["query_all"](ctx["CONCERT_DB_CONCERT_ASSIGNMENT"], None)
        return any(
            concert_id in ext_rel(r, ASSIGNMENT_CONCERT_REL_KEYS)
            and (ext(r, ASSIGNMENT_FLAG_KEYS) or "").strip().lower() == "true"
            for r in all_assign
        )
    except Exception:
        return False

def _load_existing_prefs(ctx: dict, concert_id: str, player_id: str, partdefs: list) -> dict[str, str]:
    """既存のPREFERENCEデータを取得してpd_id→priorityのdictを返す。"""
    try:
        pref_db = ctx.get("CONCERT_DB_PREFERENCE", "")
        if not pref_db:
            return {}
        all_pref = st.session_state.get("form_preference_rows")
        if all_pref is None:
            all_pref = ctx["query_all"](pref_db, None)
        # cast_idも考慮
        all_cast = st.session_state.get("form_participant_rows_concert")
        if all_cast is None:
            all_cast = [
                r for r in ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
                if concert_id in ctx["extract_relation_ids_any"](r, PARTICIPANT_CONCERT_REL_KEYS)
            ]
        cast_id = ""
        for r in all_cast:
            pids = ctx["extract_relation_ids_any"](r, PARTICIPANT_PLAYER_REL_KEYS)
            if player_id in pids:
                cast_id = r.get("id", "")
                break
        targets = {player_id}
        if cast_id:
            targets.add(cast_id)
        pd_ids = {pd.get("id", "") for pd in partdefs}
        result: dict[str, str] = {}
        for r in all_pref:
            pl_ids = set(ctx["extract_relation_ids_any"](r, PREF_PLAYER_REL_KEYS))
            pt_ids = ctx["extract_relation_ids_any"](r, PREF_PART_REL_KEYS)
            if not pl_ids.intersection(targets):
                continue
            if not pt_ids or pt_ids[0] not in pd_ids:
                continue
            priority = ctx["extract_prop_text_any"](r, PREF_PRIORITY_KEYS) or "未回答"
            result[pt_ids[0]] = priority
        return result
    except Exception:
        return {}

# ── トークン ──────────────────────────────────────────────────

def make_form_token(concert_id: str) -> str:
    h = hashlib.sha256(f"{_TOKEN_SECRET}:{concert_id}".encode()).hexdigest()
    return h[:12]

def verify_form_token(token: str, concert_id: str) -> bool:
    return hmac.compare_digest(token, make_form_token(concert_id))

# ── TinyURL ───────────────────────────────────────────────────

def shorten_url(long_url: str) -> str:
    try:
        res = _requests.get("https://tinyurl.com/api-create.php",
                            params={"url": long_url}, timeout=5)
        if res.status_code == 200 and res.text.startswith("http"):
            return res.text.strip()
    except Exception:
        pass
    return long_url

# ── データ一括取得（初回のみ） ────────────────────────────────

def _load_form_data(ctx, concert_id: str, progress=None):
    """フォーム用データを一括取得。progress(ratio, text)コールバックでプログレスバーを更新。"""
    if st.session_state.get("form_data_loaded") == concert_id:
        return
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    def _prog(ratio: float, text: str):
        if progress:
            progress(ratio, text)

    _prog(0.1, "🎵 演奏会情報を取得中...")
    concert_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT"], None)
    concert = next((r for r in concert_rows if r.get("id") == concert_id), None)

    _prog(0.3, "📅 練習日程を取得中...")
    all_prac = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    # 練習回のみ（本番当日除く）
    practices = sorted(
        [p for p in all_prac
         if concert_id in ext_rel(p, PRACTICE_CONCERT_REL_KEYS)
         and ext(p, PRACTICE_CONCERT_DAY_KEYS) != "True"],
        key=lambda p: ext(p, PRACTICE_DATE_KEYS) or "9999"
    )
    # 本番当日レコード
    concert_day = next(
        (p for p in all_prac
         if concert_id in ext_rel(p, PRACTICE_CONCERT_REL_KEYS)
         and ext(p, PRACTICE_CONCERT_DAY_KEYS) == "True"),
        None
    )

    _prog(0.5, "🎶 演奏曲目を取得中...")
    from concert.services.song_utils import get_songs_for_concert as _get_songs
    songs = _get_songs(ctx, concert_id)
    # フォールバック：get_songs_for_concertが空の場合は旧ロジックで取得
    if not songs:
        all_songs = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
        songs = [s for s in all_songs
                 if concert_id in ext_rel(s, SONG_CONCERT_REL_KEYS)]
    song_ids = {s.get("id","") for s in songs}

    _prog(0.65, "🧩 パート定義を取得中...")
    all_pd = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
    partdefs = [p for p in all_pd
                if any(sid in ext_rel(p, PARTDEF_SONG_REL_KEYS) for sid in song_ids)]

    _prog(0.8, "🎼 楽器情報を取得中...")
    instruments = ctx["query_all"](ctx["CONCERT_DB_INSTRUMENT"], None)
    inst_map = {i.get("id",""): ext(i, INSTRUMENT_NAME_KEYS) or "" for i in instruments}

    req_inst_ids: set[str] = set()
    for pd in partdefs:
        req_inst_ids.update(ext_rel(pd, PARTDEF_INST_REL_KEYS))

    _prog(0.95, "⚙️ 仕上げ中...")
    # PART_MASTERからパート一覧を取得
    part_master_rows = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
    ext_pm = ctx["extract_prop_text_any"]
    part_master_map: dict[str, dict] = {
        r.get("id", ""): {
            "name": ext_pm(r, PARTMASTER_NAME_KEYS) or "",
            "type": ext_pm(r, PARTMASTER_TYPE_KEYS) or "",
        }
        for r in part_master_rows
    }
    # パート名リスト（種別でグループ化、各グループ内はアルファベット順）
    part_opts = sorted(
        [v["name"] for v in part_master_map.values() if v["name"]],
        key=lambda x: x.lower()
    )

    _prog(0.98, "🚀 表示高速化データを準備中...")
    participant_rows_all = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    participant_rows_concert = [
        r for r in participant_rows_all
        if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
    ]
    all_players = st.session_state.get("form_all_players")
    if all_players is None:
        all_players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_name_map = {p.get("id", ""): ext(p, PLAYER_NAME_KEYS) or "" for p in all_players}
    attendance_rows = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"], None)
    preference_rows = ctx["query_all"](ctx["CONCERT_DB_PREFERENCE"], None)
    assignment_rows = ctx["query_all"](ctx["CONCERT_DB_CONCERT_ASSIGNMENT"], None)
    hc_rows = ctx["query_all"](ctx["CONCERT_DB_HARMONIA_CONCERT"], None) if ctx.get("CONCERT_DB_HARMONIA_CONCERT") else []

    st.session_state.update({
        "form_data_loaded":       concert_id,
        "form_concert":           concert,
        "form_practices":         practices,
        "form_concert_day":       concert_day,
        "form_songs":             songs,
        "form_partdefs":          partdefs,
        "form_inst_map":          inst_map,
        "form_req_insts":         sorted(req_inst_ids, key=lambda x: inst_map.get(x, x)),
        "form_part_opts":         part_opts + [OTHER_PART],
        "form_part_master_map":   part_master_map,
        "form_participant_rows_concert": participant_rows_concert,
        "form_all_players":       all_players,
        "form_player_name_map":   player_name_map,
        "form_attendance_rows":   attendance_rows,
        "form_preference_rows":   preference_rows,
        "form_assignment_rows":   assignment_rows,
        "form_harmonia_rows":     hc_rows,
    })


def _get_select_opts(ctx, db_id: str, field_keys: list) -> list[str]:
    try:
        t = ctx["get_prop_types"](db_id)
        field_name = ctx["find_prop_name"](t, field_keys) if t else None
        if not field_name:
            return []
        res = ctx["api_request"]("get", f"https://api.notion.com/v1/databases/{db_id}")
        if not res or res.status_code != 200:
            return []
        opts = res.json().get("properties", {}).get(field_name, {}).get("select", {}).get("options", [])
        return [o["name"] for o in opts if o.get("name")]
    except Exception:
        return []

# ── 送信処理 ──────────────────────────────────────────────────

def _get_form_cast_and_att_map(ctx, concert_id: str, player_id: str) -> tuple[str, dict[str, dict]]:
    """この演奏会におけるcast_id と、練習ID -> {status, comment} を返す。"""
    ext_rel = ctx["extract_relation_ids_any"]
    ext_txt = ctx["extract_prop_text_any"]

    cast_id = ""
    all_cast = st.session_state.get("form_participant_rows_concert")
    if all_cast is None:
        all_cast = [
            r for r in ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
            if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
        ]
    for r in all_cast:
        pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
        if player_id in pids:
            cast_id = r.get("id", "")
            break

    att_map: dict[str, dict] = {}
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    t_att = ctx["get_prop_types"](att_db)
    if not t_att:
        return cast_id, att_map

    practice_rel_key = find_relation_prop(
        t_att,
        ATT_PRACTICE_REL_KEYS,
        ["練習", "practice"],
    )
    player_rel_key = find_relation_prop(
        t_att,
        ATT_PLAYER_REL_KEYS,
        ["奏者", "出演者", "participant", "player"],
        exclude={practice_rel_key} if practice_rel_key else set(),
    )
    status_key = ctx["find_prop_name"](t_att, ATT_STATUS_KEYS)
    note_key = ctx["find_prop_name"](t_att, ATT_NOTE_KEYS)

    if not player_rel_key or not practice_rel_key:
        return cast_id, att_map

    rel_targets = {player_id}
    if cast_id:
        rel_targets.add(cast_id)

    all_att = st.session_state.get("form_attendance_rows")
    if all_att is None:
        all_att = ctx["query_all"](att_db, None)
    for row in all_att:
        rel_ids = ext_rel(row, [player_rel_key])
        pr_ids = ext_rel(row, [practice_rel_key])
        if not rel_ids or not pr_ids:
            continue
        if any(rid in rel_targets for rid in rel_ids):
            att_map[pr_ids[0]] = {
                "status": ext_txt(row, [status_key]) or "未回答",
                "comment": ext_txt(row, [note_key]) or "",
            }

    if st.query_params.get("debug") == "1":
        st.session_state["form_att_debug"] = {
            "concert_id": concert_id,
            "player_id": player_id,
            "cast_id": cast_id,
            "player_rel_key": player_rel_key,
            "practice_rel_key": practice_rel_key,
            "status_key": status_key,
            "note_key": note_key,
            "att_map_size": len(att_map),
            "att_map": att_map,
        }

    return cast_id, att_map


def _submit_all(ctx, concert_id: str, concert_name: str,
                player_id: str, player_name: str,
                att: dict, att_comment: dict, pref: dict, own: dict) -> tuple[int, list[str], dict]:
    """全データをNotionに送信。(成功件数, エラーリスト, デバッグ情報)を返す。"""
    ext_rel  = ctx["extract_relation_ids_any"]
    ok_n     = 0
    errors: list[str] = []
    debug: dict = {}  # テスト用デバッグ情報

    # ── CONCERT_CAST ──────────────────────────────────────────
    cast_db = ctx["CONCERT_DB_PARTICIPANT"]
    t_cast  = ctx["get_prop_types"](cast_db)
    cast_id = ""
    if t_cast:
        all_cast = ctx["query_all"](cast_db, None)
        for r in all_cast:
            pids = ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
            cids = ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
            if player_id in pids and concert_id in cids:
                cast_id = r.get("id", "")
                break
        if not cast_id:
            props: dict = {}
            ctx["put_prop_any"](props, t_cast, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
            ctx["put_prop_any"](props, t_cast, PARTICIPANT_PLAYER_REL_KEYS,  player_id)
            ctx["put_key_any"](props, t_cast, PARTICIPANT_RECORD_KEYS,
                               concert_id, player_id, prefix="participant")
            # パートはRelation型（PART_MASTERへ）
            part_master_id = st.session_state.get("form_player_part_id", "")
            if part_master_id:
                ctx["put_prop_any"](props, t_cast, PARTICIPANT_PART_REL_KEYS, part_master_id)
            confirmed_fee = st.session_state.get(f"confirmed_fee_{concert_id}")
            if confirmed_fee is not None:
                ctx["put_prop_any"](props, t_cast, PARTICIPANT_FEE_KEYS, confirmed_fee)
            res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                     json={"parent": {"database_id": cast_db}, "properties": props})
            if res and res.status_code == 200:
                cast_id = res.json().get("id", "")
                ok_n += 1
                debug["cast_id"] = cast_id
            else:
                errors.append(f"CONCERT_CAST登録失敗 (status={getattr(res,'status_code','?')})")
        else:
            debug["cast_id"] = cast_id + "（既存）"
            # 既存レコードのパートが空なら書き込む
            existing_row = next((r for r in all_cast if r.get("id","") == cast_id), {})
            if not ext_rel(existing_row, PARTICIPANT_PART_REL_KEYS):
                part_master_id = st.session_state.get("form_player_part_id", "")
                if part_master_id:
                    props_p: dict = {}
                    ctx["put_prop_any"](props_p, t_cast, PARTICIPANT_PART_REL_KEYS, part_master_id)
                    ctx["api_request"]("patch",
                        f"https://api.notion.com/v1/pages/{cast_id}",
                        json={"properties": props_p})

    debug["player_id"] = player_id

    # ── ATTENDANCE（練習回 + 本番当日を○で自動登録） ──────────
    att_db = ctx["CONCERT_DB_ATTENDANCE"]
    t_att  = ctx["get_prop_types"](att_db)
    if t_att:
        all_att = ctx["query_all"](att_db, None)
        practices = st.session_state.get("form_practices", [])
        concert_day = st.session_state.get("form_concert_day")
        prac_name_map = {p.get("id",""): ctx["extract_prop_text_any"](p, PRACTICE_NAME_KEYS) or ""
                         for p in practices}
        if concert_day:
            prac_name_map[concert_day.get("id","")] = \
                ctx["extract_prop_text_any"](concert_day, PRACTICE_NAME_KEYS) or "本番当日"

        att_all = dict(att)
        if concert_day:
            att_all[concert_day.get("id","")] = "○"

        # 既存players.pyと同じロジック：_find_relation_propでフィールド名を特定
        practice_rel_key = find_relation_prop(t_att, ATT_PRACTICE_REL_KEYS, ["練習", "practice"])
        player_rel_key   = find_relation_prop(t_att, ATT_PLAYER_REL_KEYS,
                                     ["奏者", "出演者", "participant", "player"],
                                     exclude={practice_rel_key} if practice_rel_key else set())
        status_key = ctx["find_prop_name"](t_att, ATT_STATUS_KEYS)
        note_key = ctx["find_prop_name"](t_att, ATT_NOTE_KEYS)
        rel_target = cast_id if cast_id else player_id

        att_ids: list[str] = []
        for pr_id, status in att_all.items():
            existing_id = ""
            for r in all_att:
                pl = (r.get("properties",{}).get(player_rel_key,{}).get("relation",[]) or [])
                pr = (r.get("properties",{}).get(practice_rel_key,{}).get("relation",[]) or [])
                pl_ids = [x.get("id","") for x in pl]
                pr_ids = [x.get("id","") for x in pr]
                if rel_target in pl_ids and pr_id in pr_ids:
                    existing_id = r.get("id","")
                    break
            pname = prac_name_map.get(pr_id, "")
            props = {}
            # attendance_key
            ctx["put_key_any"](props, t_att, ATTENDANCE_KEY_KEYS,
                               rel_target, pr_id, prefix="att")
            # relationsをput_propで直接指定
            if player_rel_key:
                ctx["put_prop"](props, t_att, player_rel_key,   rel_target)
            if practice_rel_key:
                ctx["put_prop"](props, t_att, practice_rel_key, pr_id)
            if status_key:
                ctx["put_prop"](props, t_att, status_key, status)
            if note_key:
                cmt = (att_comment.get(pr_id, "") or "").strip()
                ctx["put_prop"](props, t_att, note_key, cmt)
            if existing_id:
                res = ctx["api_request"]("patch",
                    f"https://api.notion.com/v1/pages/{existing_id}",
                    json={"properties": props})
            else:
                res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                         json={"parent": {"database_id": att_db},
                                               "properties": props})
            if res and res.status_code == 200:
                ok_n += 1
                att_ids.append(res.json().get("id","") if not existing_id else existing_id)
            else:
                errors.append(f"出欠登録失敗：{pname} (status={getattr(res,'status_code','?')})")
        debug["att_ids"] = att_ids

    # ── パート希望 ────────────────────────────────────────────
    if pref:
        pref_db = ctx["CONCERT_DB_PREFERENCE"]
        t_pref  = ctx["get_prop_types"](pref_db)
        if t_pref:
            all_pref = ctx["query_all"](pref_db, None)
            for pd_id, priority in pref.items():
                if priority == "未回答":
                    continue
                existing_id = ""
                for r in all_pref:
                    pl = ext_rel(r, PREF_PLAYER_REL_KEYS)
                    pd = ext_rel(r, PREF_PART_REL_KEYS)
                    pref_id_check = cast_id if cast_id else player_id
                    if pref_id_check in pl and pd_id in pd:
                        existing_id = r.get("id", "")
                        break
                pref_target = cast_id if cast_id else player_id
                props = {}
                ctx["put_key_any"](props, t_pref, PREFERENCE_KEY_KEYS,
                                   pref_target, pd_id, prefix="pref")
                ctx["put_prop_any"](props, t_pref, PREF_PLAYER_REL_KEYS, pref_target)
                ctx["put_prop_any"](props, t_pref, PREF_PART_REL_KEYS,   pd_id)
                ctx["put_prop_any"](props, t_pref, PREF_PRIORITY_KEYS,   priority)
                if existing_id:
                    res = ctx["api_request"]("patch",
                        f"https://api.notion.com/v1/pages/{existing_id}",
                        json={"properties": props})
                else:
                    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                             json={"parent": {"database_id": pref_db},
                                                   "properties": props})
                if res and res.status_code == 200:
                    ok_n += 1
                else:
                    errors.append(f"希望登録失敗：{pd_id[:8]} (status={getattr(res,'status_code','?')})")

    # ── 所有楽器 ──────────────────────────────────────────────
    if own:
        pi_db  = ctx["CONCERT_DB_PLAYER_INSTRUMENT"]
        t_pi   = ctx["get_prop_types"](pi_db)
        inst_map = st.session_state.get("form_inst_map", {})
        if t_pi:
            all_pi = ctx["query_all"](pi_db, None)
            for iid, cnt in own.items():
                if cnt == 0:
                    continue
                iname = inst_map.get(iid, iid)
                existing_id = ""
                for r in all_pi:
                    if (player_id in ext_rel(r, PI_PLAYER_REL_KEYS) and
                            iid in ext_rel(r, PI_INST_REL_KEYS) and
                            concert_id in ext_rel(r, PI_CONCERT_REL_KEYS)):
                        existing_id = r.get("id", "")
                        break
                props = {}
                ctx["put_key_any"](props, t_pi, ["record_key", "タイトル", "PK名称"],
                                   player_id, iid, prefix="assign")
                ctx["put_prop_any"](props, t_pi, PI_PLAYER_REL_KEYS,  player_id)
                ctx["put_prop_any"](props, t_pi, PI_INST_REL_KEYS,    iid)
                ctx["put_prop_any"](props, t_pi, PI_CONCERT_REL_KEYS, concert_id)
                ctx["put_prop_any"](props, t_pi, PI_OWN_COUNT_KEYS,   cnt)
                if existing_id:
                    res = ctx["api_request"]("patch",
                        f"https://api.notion.com/v1/pages/{existing_id}",
                        json={"properties": props})
                else:
                    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                             json={"parent": {"database_id": pi_db},
                                                   "properties": props})
                if res and res.status_code == 200:
                    ok_n += 1
                else:
                    errors.append(f"楽器登録失敗：{iname} (status={getattr(res,'status_code','?')})")

    return ok_n, errors, debug

# ── プライバシーポリシー ──────────────────────────────────────

_PRIVACY_POLICY = """
## プライバシーポリシー

本フォームは **ArtéMis HARMONIA** が提供する演奏会運営支援フォームです。

### 収集する情報
- 氏名・ハンドルネーム
- メールアドレス・電話番号・LINE ID（任意）
- 練習出欠・パート希望・所有楽器情報

### 利用目的
収集した情報は演奏会の運営・調整（出欠管理・パートアサイン・楽器手配）のみに使用します。

### 第三者提供
収集した個人情報を本演奏会の運営目的以外に使用せず、第三者に提供しません。

### 管理者
ArtéMis HARMONIA開発者　喜田悠太
attituderko@gmail.com

### 開示・削除請求
上記メールアドレスにご連絡ください。

---
*本フォームへの入力・送信をもって上記に同意したものとみなします。*
"""

def _render_attendance_overview(ctx, concert_id: str, participant_rows: list,
                                 practices: list, my_part_master_id: str,
                                 role: int):
    """出欠一覧をロールに応じて表示する。
    Leader: 自パートのみ、Manager: 全員
    """
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    pm_map  = st.session_state.get("form_part_master_map") or {}

    # 自分のパートに絞るかどうか
    filter_part = (role == ROLE_LEADER)

    # CONCERT_CASTから対象者を絞り込む
    target_cast = []
    for r in participant_rows:
        cids = ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
        if concert_id not in cids:
            continue
        if filter_part:
            pm_ids = ext_rel(r, PARTICIPANT_PART_REL_KEYS)
            if not pm_ids or pm_ids[0] != my_part_master_id:
                continue
        target_cast.append(r)

    if not target_cast:
        st.info("対象メンバーが見つかりません。")
        return

    # PERFORMER名マップ
    all_players = st.session_state.get("form_all_players")
    if all_players is None:
        all_players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_name_map = {p.get("id",""): ext(p, PLAYER_NAME_KEYS) or "" for p in all_players}

    # 出欠データ取得
    all_att = st.session_state.get("form_attendance_rows")
    if all_att is None:
        all_att = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"], None)
    # cast_id or player_id → {practice_id: status}
    att_map: dict[str, dict[str,str]] = {}
    for r in all_att:
        pl_ids = ext_rel(r, ATT_PLAYER_REL_KEYS)
        pr_ids = ext_rel(r, ATT_PRACTICE_REL_KEYS)
        if not pl_ids or not pr_ids:
            continue
        status = ext(r, ATT_STATUS_KEYS) or "—"
        att_map.setdefault(pl_ids[0], {})[pr_ids[0]] = status

    # ヘッダー行
    pr_labels = [ext(p, PRACTICE_DATE_KEYS)[:10] if ext(p, PRACTICE_DATE_KEYS) else ext(p, PRACTICE_NAME_KEYS) or "?"
                 for p in practices]
    header = ["パート", "氏名"] + pr_labels
    rows = []
    for cast_row in sorted(target_cast, key=lambda r: (
        pm_map.get((ext_rel(r, PARTICIPANT_PART_REL_KEYS) or [""])[0], {}).get("name",""),
        player_name_map.get((ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0], "")
    )):
        pids   = ext_rel(cast_row, PARTICIPANT_PLAYER_REL_KEYS)
        pm_ids = ext_rel(cast_row, PARTICIPANT_PART_REL_KEYS)
        pid    = pids[0] if pids else ""
        pname  = player_name_map.get(pid, "—")
        part   = pm_map.get(pm_ids[0], {}).get("name","") if pm_ids else ""
        # cast_idまたはplayer_idで出欠を探す
        cast_id = cast_row.get("id","")
        p_att = att_map.get(cast_id) or att_map.get(pid) or {}
        row = [part, pname] + [p_att.get(p.get("id",""), "—") for p in practices]
        rows.append(row)

    import pandas as pd
    df = pd.DataFrame(rows, columns=header)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_member_list(ctx, concert_id: str, participant_rows: list,
                        my_part_master_id: str, role: int):
    """メンバー一覧をロールに応じて表示する。
    Leader: 自パートのみ、Manager: 全員（連絡先含む）
    """
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]
    pm_map  = st.session_state.get("form_part_master_map") or {}

    filter_part = (role == ROLE_LEADER)
    target_cast = []
    for r in participant_rows:
        cids = ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
        if concert_id not in cids:
            continue
        if filter_part:
            pm_ids = ext_rel(r, PARTICIPANT_PART_REL_KEYS)
            if not pm_ids or pm_ids[0] != my_part_master_id:
                continue
        target_cast.append(r)

    if not target_cast:
        st.info("対象メンバーが見つかりません。")
        return

    all_players = st.session_state.get("form_all_players")
    if all_players is None:
        all_players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_map  = {p.get("id",""): p for p in all_players}

    rows = []
    for cast_row in sorted(target_cast, key=lambda r: (
        pm_map.get((ext_rel(r, PARTICIPANT_PART_REL_KEYS) or [""])[0], {}).get("name",""),
        (ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0]
    )):
        pids   = ext_rel(cast_row, PARTICIPANT_PLAYER_REL_KEYS)
        pm_ids = ext_rel(cast_row, PARTICIPANT_PART_REL_KEYS)
        pid    = pids[0] if pids else ""
        p      = player_map.get(pid, {})
        part   = pm_map.get(pm_ids[0], {}).get("name","") if pm_ids else ""
        pname  = ext(p, PLAYER_NAME_KEYS) or "—"
        hn     = ext(p, PLAYER_HN_KEYS)   or ""
        row = {"パート": part, "氏名": pname, "H.N.": hn}
        if role >= ROLE_MANAGER:
            row["メール"]  = ext(p, PLAYER_EMAIL_KEYS) or ""
            row["電話"]    = ext(p, PLAYER_PHONE_KEYS) or ""
            row["LINE ID"] = ext(p, PLAYER_LINE_KEYS)  or ""
        rows.append(row)

    import pandas as pd
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_assignment_view(ctx, concert_id: str, my_part_master_id: str, role: int):
    """アサイン結果をロールに応じて表示する。
    Player/Leader: 自パート、Manager: パート選択可能。
    表示は「案提示」後のみ。
    """
    from concert.services.keys import (
        ASSIGNMENT_CONCERT_REL_KEYS, ASSIGNMENT_PLAYER_REL_KEYS,
        ASSIGNMENT_PARTDEF_REL_KEYS, ASSIGNMENT_SONG_REL_KEYS,
        ASSIGNMENT_FLAG_KEYS,
    )
    ext     = ctx["extract_prop_text_any"]
    ext_rel = ctx["extract_relation_ids_any"]

    # 案提示または公開済みアサインがある場合に表示
    if not (_get_proposal_flag(ctx, concert_id) or _has_published_assignments(ctx, concert_id)):
        st.info("アサイン案がまだ提示されていません。提示後に閲覧できます。")
        return

    # アサイン結果取得
    try:
        all_assign = st.session_state.get("form_assignment_rows")
        if all_assign is None:
            all_assign = ctx["query_all"](ctx["CONCERT_DB_CONCERT_ASSIGNMENT"], None)
    except Exception:
        st.error("アサイン結果の取得に失敗しました。")
        return

    concert_assigns = [
        r for r in all_assign
        if concert_id in ext_rel(r, ASSIGNMENT_CONCERT_REL_KEYS)
        and ext(r, ASSIGNMENT_FLAG_KEYS) == "True"
    ]

    if not concert_assigns:
        st.info("アサイン結果がまだ登録されていません。")
        return

    # 曲・奏者・パート定義・PART_MASTER名マップ
    all_players = st.session_state.get("form_all_players")
    if all_players is None:
        all_players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    player_name_map = {
        p.get("id", ""): ext(p, PLAYER_NAME_KEYS) or ""
        for p in all_players
    }

    all_songs = st.session_state.get("form_songs")
    if all_songs is None:
        all_songs = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
    song_name_map = build_song_name_map(ctx, all_songs)

    all_pd = st.session_state.get("form_partdefs")
    if all_pd is None:
        all_pd = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)

    # PART_DEFINITION → PART_MASTER / 表示パート名
    pd_part_map = {
        p.get("id", ""): (ext_rel(p, PARTDEF_PART_REL_KEYS) or [""])[0]
        for p in all_pd
    }
    pd_display_part_map = {}
    for p in all_pd:
        pdid = p.get("id", "")
        disp = ext(p, PARTDEF_DISPLAY_NAME_KEYS) or ext(p, PARTDEF_NAME_KEYS) or ""
        pd_display_part_map[pdid] = disp

    # PART_MASTER ID → パート名
    pm_cache = st.session_state.get("form_part_master_map") or {}
    pm_name_map = {pid: (meta.get("name") or "") for pid, meta in pm_cache.items()}
    if not pm_name_map:
        all_pm = ctx["query_all"](ctx["CONCERT_DB_PART_MASTER"], None)
        pm_name_map = {
            p.get("id", ""): ext(p, PARTMASTER_NAME_KEYS) or ""
            for p in all_pm
        }

    # CONCERT_CAST（表示対象メンバー）
    participant_rows = st.session_state.get("form_participant_rows_concert")
    if participant_rows is None:
        participant_rows = [
            r for r in ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
            if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
        ]
    cast_targets = participant_rows

    # role別のパート選択
    part_options = []
    for r in cast_targets:
        pm_ids = ext_rel(r, PARTICIPANT_PART_REL_KEYS)
        if not pm_ids:
            continue
        _pm_id = pm_ids[0]
        if _pm_id not in pm_name_map:
            continue
        part_options.append((_pm_id, pm_name_map.get(_pm_id, "—")))
    part_options = sorted(list({p[0]: p for p in part_options}.values()), key=lambda x: x[1])

    selected_part_id = my_part_master_id
    selected_part_name = pm_name_map.get(selected_part_id, "—")
    if role >= ROLE_MANAGER:
        if not part_options:
            st.info("表示対象パートが見つかりません。")
            return
        part_labels = [name for _, name in part_options]
        part_id_by_name = {name: pid for pid, name in part_options}
        default_part_name = part_labels[0]
        selected_part_name = st.selectbox(
            "担当パートで絞り込み",
            part_labels,
            index=part_labels.index(default_part_name),
            key=f"assign_part_select_{concert_id}",
        )
        selected_part_id = part_id_by_name.get(selected_part_name, "")
    else:
        st.caption(f"表示パート: {selected_part_name}")

    # アサイン情報
    # 1) part_master x song x player の存在判定
    assign_lookup: dict[tuple[str, str], set[str]] = {}
    # 2) part_master x song x player ごとの表示パート名（PART_DEFINITION）
    assign_display_lookup: dict[tuple[str, str, str], set[str]] = {}
    # 3) part_master欠損時のフォールバック
    assign_lookup_by_song: dict[str, set[str]] = {}
    assign_display_lookup_by_song: dict[tuple[str, str], set[str]] = {}
    for r in concert_assigns:
        player_ids = ext_rel(r, ASSIGNMENT_PLAYER_REL_KEYS)
        pd_ids     = ext_rel(r, ASSIGNMENT_PARTDEF_REL_KEYS)
        song_ids   = ext_rel(r, ASSIGNMENT_SONG_REL_KEYS)

        if not player_ids:
            continue

        if not song_ids:
            continue
        song_id = song_ids[0]
        pid = player_ids[0]
        assign_lookup_by_song.setdefault(song_id, set()).add(pid)

        if not pd_ids:
            continue
        pd_id = pd_ids[0]
        pm_id = pd_part_map.get(pd_id, "")
        display_part = (pd_display_part_map.get(pd_id, "") or "").strip()
        if not pm_id:
            if display_part:
                assign_display_lookup_by_song.setdefault((song_id, pid), set()).add(display_part)
            continue
        key = (pm_id, song_id)
        assign_lookup.setdefault(key, set()).add(pid)
        if display_part:
            assign_display_lookup.setdefault((pm_id, song_id, pid), set()).add(display_part)

    # 対象パートのメンバー
    target_cast = []
    for r in cast_targets:
        pm_ids = ext_rel(r, PARTICIPANT_PART_REL_KEYS)
        if not pm_ids or pm_ids[0] != selected_part_id:
            continue
        target_cast.append(r)
    if not target_cast:
        st.info("対象パートのメンバーが見つかりません。")
        return

    # 対象曲（Leader/Managerはプルダウン）
    target_player_ids = {
        (ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0]
        for r in target_cast
        if (ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])
    }
    song_ids_for_part = sorted({
        sid for (pmid, sid), pset in assign_lookup.items()
        if pmid == selected_part_id and pset
    }, key=lambda sid: song_name_map.get(sid, ""))
    # フォールバック: アサイン行にパート定義が無い場合は、奏者所属パートから逆引き
    if not song_ids_for_part and target_player_ids:
        song_ids_for_part = sorted([
            sid for sid, pset in assign_lookup_by_song.items()
            if pset.intersection(target_player_ids)
        ], key=lambda sid: song_name_map.get(sid, ""))
    if not song_ids_for_part:
        st.info("このパートのアサイン結果がまだ登録されていません。")
        return

    selected_song_ids = song_ids_for_part
    if role >= ROLE_LEADER:
        song_label_all = "全曲"
        song_labels = [song_label_all] + [song_name_map.get(sid, "—") for sid in song_ids_for_part]
        selected_song_label = st.selectbox(
            "表示曲",
            song_labels,
            index=0,
            key=f"assign_song_select_{role}_{concert_id}_{selected_part_id}",
        )
        if selected_song_label != song_label_all:
            selected_song_ids = [
                sid for sid in song_ids_for_part
                if song_name_map.get(sid, "—") == selected_song_label
            ]

    # マトリックス構築
    matrix_rows = []
    for cast_row in sorted(target_cast, key=lambda r: player_name_map.get((ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS) or [""])[0], "")):
        pids = ext_rel(cast_row, PARTICIPANT_PLAYER_REL_KEYS)
        pid = pids[0] if pids else ""
        pname = player_name_map.get(pid, "—")
        row = {"担当パート": "", "奏者": pname}
        player_display_parts: set[str] = set()
        for sid in selected_song_ids:
            is_assigned = (
                pid in assign_lookup.get((selected_part_id, sid), set())
                or pid in assign_lookup_by_song.get(sid, set())
            )
            if is_assigned:
                player_display_parts.update(assign_display_lookup.get((selected_part_id, sid, pid), set()))
                player_display_parts.update(assign_display_lookup_by_song.get((sid, pid), set()))
            row[song_name_map.get(sid, "—")] = "○" if is_assigned else "—"
        row["担当パート"] = " / ".join(sorted(player_display_parts)) if player_display_parts else "-"
        matrix_rows.append(row)

    import pandas as pd
    df = pd.DataFrame(matrix_rows)
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_concert_selector(ctx):
    """concert_id未確定時のエントリ画面。
    ログイン済み → 参加演奏会選択 or 招待コードで別演奏会追加
    未ログイン   → ログイン or 招待コードで新規参加
    """
    ext = ctx["extract_prop_text_any"]
    pm_map_for_selector = _get_part_master_map(ctx)
    pm_name_to_id = {
        v.get("name", ""): k for k, v in pm_map_for_selector.items() if v.get("name")
    }
    pm_names = sorted(pm_name_to_id.keys(), key=lambda x: x.lower())

    def _invite_rate_limited() -> bool:
        now = datetime.now()
        window_start = now - timedelta(minutes=_INVITE_WINDOW_MINUTES)
        ts_list = st.session_state.get("sel_invite_fail_ts", [])
        ts_list = [t for t in ts_list if isinstance(t, datetime) and t >= window_start]
        st.session_state["sel_invite_fail_ts"] = ts_list
        if len(ts_list) >= _INVITE_MAX_ATTEMPTS:
            st.warning(
                f"招待コードの入力失敗が一定回数を超えました。"
                f"{_INVITE_WINDOW_MINUTES}分ほど待って再試行してください。"
            )
            return True
        return False

    def _record_invite_failure() -> None:
        now = datetime.now()
        ts_list = st.session_state.get("sel_invite_fail_ts", [])
        ts_list.append(now)
        st.session_state["sel_invite_fail_ts"] = ts_list

    selector_mode = st.session_state.get("selector_mode")  # None / "login" / "invite"

    # ── モード未選択 ─────────────────────────────────────────
    if not selector_mode:
        st.html('<div class="h-scr-title">ArtéMis HARMONIA ログイン</div>')
        col_l, col_i = st.columns(2)
        if col_l.button("🔑 ログイン", use_container_width=True, type="primary", key="sel_login"):
            st.session_state["selector_mode"] = "login"
            st.rerun()
        if col_i.button("🎟 招待コードで参加", use_container_width=True, key="sel_invite"):
            st.session_state["selector_mode"] = "invite"
            st.rerun()
        return

    # ── 招待コード入力モード ──────────────────────────────────
    if selector_mode == "invite":
        st.html('<div class="h-scr-title">招待コードを入力してください</div>')
        st.caption("管理者から受け取った招待コードを入力してください。")
        _invite_logged_in = bool(st.session_state.get("sel_pw_verified") and st.session_state.get("sel_player_id"))
        with st.form("invite_code_form"):
            code_input = st.text_input("招待コード", max_chars=20, placeholder="例: K7X2MN9A")
            selected_part_name = ""
            if _invite_logged_in:
                selected_part_name = st.selectbox(
                    "担当パート *",
                    ["（選択してください）"] + pm_names,
                    key="invite_part_select",
                )
            submitted = st.form_submit_button("次へ →", type="primary", use_container_width=True)
        if submitted:
            if _invite_rate_limited():
                return
            cid, invite_err = _resolve_concert_id_by_invite_code(ctx, code_input)
            if cid:
                st.session_state.pop("sel_invite_fail_ts", None)
                # selector内でログイン済みなら、演奏会紐付けしてログイン画面へ戻す
                if _invite_logged_in:
                    part_id = pm_name_to_id.get(selected_part_name, "")
                    if not part_id:
                        st.error("担当パートを選択してください。")
                        return
                    ok, msg = _link_player_to_concert_by_invite(ctx, st.session_state.get("sel_player_id", ""), cid, part_id)
                    if ok:
                        st.session_state["selector_mode"] = "login"
                        st.session_state["sel_preselect_cid"] = cid
                        st.success("演奏会を追加しました。")
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    # 未ログイン時は新規登録フローへ直行
                    st.session_state["form_resolved_concert_id"] = cid
                    st.session_state["form_auth_mode"] = "new"
                    st.session_state.pop("form_privacy_agreed", None)
                    st.session_state.pop("auth_email_submitted", None)
                    st.session_state.pop("selector_mode", None)
                    st.rerun()
            else:
                _record_invite_failure()
                if invite_err == "ambiguous":
                    st.error("同じ招待コードが複数の演奏会に設定されています。管理者に連絡してください。")
                elif invite_err == "invalid":
                    st.error("招待コードを入力してください。")
                else:
                    st.error("招待コードが見つかりませんでした。管理者に確認してください。")
        if st.button("← 戻る", key="invite_back"):
            st.session_state.pop("selector_mode", None)
            st.rerun()
        return

    # ── ログインモード ────────────────────────────────────────
    if selector_mode == "login":
        # メールアドレス未入力フェーズ
        if not st.session_state.get("sel_email_submitted"):
            st.html('<div class="h-scr-title">ログイン</div>')
            with st.form("sel_email_form"):
                sel_email = st.text_input("メールアドレス", placeholder="yamada@example.com")
                submitted_e = st.form_submit_button("次へ", type="primary", use_container_width=True)
            if submitted_e:
                if not sel_email.strip():
                    st.error("メールアドレスを入力してください。")
                    return
                email_lower = sel_email.strip().lower()
                players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
                matched = next(
                    (p for p in players
                     if (ext(p, PLAYER_EMAIL_KEYS) or "").strip().lower() == email_lower),
                    None
                )
                if not matched:
                    st.warning("ユーザー登録がありません。参加予定の演奏会招待コードを入力し、ユーザー登録してください。")
                    st.session_state["selector_mode"] = "invite"
                    st.rerun()
                    return
                st.session_state.update({
                    "sel_email":           email_lower,
                    "sel_player_id":       matched.get("id", ""),
                    "sel_player_name":     ext(matched, PLAYER_NAME_KEYS) or "",
                    "sel_has_password":    bool(ext(matched, PLAYER_PASSWORD_HASH_KEYS)),
                    "sel_email_submitted": True,
                })
                st.rerun()
            if st.button("← 戻る", key="sel_login_back"):
                st.session_state.pop("selector_mode", None)
                st.rerun()
            return

        # パスワード認証フェーズ
        sel_email       = st.session_state.get("sel_email", "")
        sel_pid         = st.session_state.get("sel_player_id", "")
        sel_pname       = st.session_state.get("sel_player_name", "")
        sel_has_pw      = st.session_state.get("sel_has_password", False)
        sel_pw_verified = st.session_state.get("sel_pw_verified", False)

        if not sel_pw_verified:
            if sel_has_pw:
                st.html(f'<div class="h-scr-title">おかえりなさい、{sel_pname} さん</div>')
                with st.form("sel_pw_form"):
                    pw = st.text_input("パスワード", type="password")
                    submitted_pw = st.form_submit_button("ログイン", type="primary", use_container_width=True)
                if submitted_pw:
                    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
                    matched = next((p for p in players if p.get("id") == sel_pid), None)
                    stored_hash = ext(matched, PLAYER_PASSWORD_HASH_KEYS) if matched else ""
                    if _verify_password(pw, stored_hash):
                        st.session_state["sel_pw_verified"] = True
                        st.rerun()
                    else:
                        st.error("パスワードが違います。")
            else:
                # パスワード未設定 → selector内で確認コード認証
                st.html(f'<div class="h-scr-title">おかえりなさい、{sel_pname} さん</div>')
                st.warning("パスワードが設定されていません。確認コードでログインしてパスワードを設定してください。")
                st.caption(f"登録メールアドレス: {sel_email}")
                if not st.session_state.get("sel_magic_sent"):
                    if st.button("📧 確認コードでログインする",
                                 type="primary", use_container_width=True,
                                 key="sel_goto_magic"):
                        code = _generate_code()
                        ok = _send_magic_code(ctx, sel_email, code, "HARMONIA")
                        if ok:
                            st.session_state.update({
                                "sel_magic_sent": True,
                                "sel_magic_hash": _hash_code(code),
                                "sel_magic_expires": datetime.now() + timedelta(minutes=_CODE_EXPIRY_MINUTES),
                                "sel_magic_attempts": 0,
                                "sel_magic_last_sent": datetime.now(),
                            })
                            st.success("確認コードを送信しました。")
                            st.rerun()
                        else:
                            st.error("確認コード送信に失敗しました。")
                else:
                    st.info("確認コードを登録済みメールアドレスに送信しました。確認コードを入力してください。")
                    with st.form("sel_magic_verify_form"):
                        entered_code = st.text_input("確認コード（6桁）", max_chars=6, placeholder="123456")
                        c_ok, c_resend = st.columns(2)
                        submitted_code = c_ok.form_submit_button("確認", type="primary", use_container_width=True)
                        submitted_resend = c_resend.form_submit_button("再送信", use_container_width=True)
                    if submitted_resend:
                        last_sent = st.session_state.get("sel_magic_last_sent")
                        if last_sent and (datetime.now() - last_sent).total_seconds() < _CODE_RESEND_SECONDS:
                            remaining_sec = int(_CODE_RESEND_SECONDS - (datetime.now() - last_sent).total_seconds())
                            st.warning(f"再送信は{remaining_sec}秒後に行えます。")
                            return
                        code = _generate_code()
                        ok = _send_magic_code(ctx, sel_email, code, "HARMONIA")
                        if ok:
                            st.session_state.update({
                                "sel_magic_hash": _hash_code(code),
                                "sel_magic_expires": datetime.now() + timedelta(minutes=_CODE_EXPIRY_MINUTES),
                                "sel_magic_attempts": 0,
                                "sel_magic_last_sent": datetime.now(),
                            })
                            st.success("確認コードを再送しました。")
                            st.rerun()
                        else:
                            st.error("再送信に失敗しました。")
                    if submitted_code:
                        expires = st.session_state.get("sel_magic_expires")
                        if not expires or datetime.now() >= expires:
                            st.error("確認コードの有効期限が切れています。再送信してください。")
                        elif _verify_code(entered_code, st.session_state.get("sel_magic_hash", "")):
                            st.session_state["sel_pw_verified"] = True
                            st.session_state["sel_need_set_password"] = True
                            for k in ["sel_magic_sent", "sel_magic_hash", "sel_magic_expires", "sel_magic_attempts", "sel_magic_last_sent"]:
                                st.session_state.pop(k, None)
                            st.rerun()
                        else:
                            attempts = int(st.session_state.get("sel_magic_attempts", 0)) + 1
                            st.session_state["sel_magic_attempts"] = attempts
                            remain = _CODE_MAX_ATTEMPTS - attempts
                            if remain <= 0:
                                st.error(f"確認コードを{_CODE_MAX_ATTEMPTS}回間違えました。最初からやり直してください。")
                                for k in ["sel_email_submitted","sel_email","sel_player_id","sel_player_name",
                                          "sel_has_password","sel_pw_verified","sel_need_set_password",
                                          "sel_magic_sent","sel_magic_hash","sel_magic_expires","sel_magic_attempts","sel_magic_last_sent"]:
                                    st.session_state.pop(k, None)
                                st.rerun()
                            else:
                                st.error(f"確認コードが違います。あと{remain}回入力できます。")
            if st.button("← 戻る", key="sel_pw_back"):
                for k in ["sel_email_submitted","sel_email","sel_player_id",
                          "sel_player_name","sel_has_password","sel_pw_verified","sel_need_set_password",
                          "sel_magic_sent","sel_magic_hash","sel_magic_expires","sel_magic_attempts","sel_magic_last_sent"]:
                    st.session_state.pop(k, None)
                st.rerun()
            return

        # パスワード未設定で認証成功した場合は、演奏会選択前に必ず設定させる
        if st.session_state.get("sel_need_set_password"):
            st.html(f'<div class="h-scr-title">{sel_pname} さん、パスワードを設定してください</div>')
            st.caption("次回からメールアドレスとパスワードでログインできます。")
            with st.form("sel_set_password_form"):
                pw1 = st.text_input("パスワード（6文字以上）", type="password")
                pw2 = st.text_input("パスワード（確認）", type="password")
                c_set, c_logout = st.columns(2)
                submitted_set = c_set.form_submit_button("設定する", type="primary", use_container_width=True)
                submitted_logout = c_logout.form_submit_button("ログアウト", use_container_width=True)
            if submitted_logout:
                _logout_to_entry(_FORM_COOKIE_MGR)
            if submitted_set:
                if len((pw1 or "").strip()) < 6:
                    st.error("パスワードは6文字以上で入力してください。")
                elif pw1 != pw2:
                    st.error("パスワードが一致しません。")
                else:
                    ok = _save_password_hash(ctx, sel_pid, pw1)
                    if ok:
                        st.session_state["sel_need_set_password"] = False
                        st.success("パスワードを設定しました。")
                        st.rerun()
                    else:
                        st.error("パスワード設定に失敗しました。")
            return

        # 認証済み → 参加演奏会を選択
        st.html(f'<div class="h-scr-title">こんにちは、{sel_pname} さん</div>')
        my_concerts = _get_my_concerts(ctx, sel_pid)

        def _go_selected_concert(selected_cid: str):
            st.session_state.update({
                "form_resolved_concert_id": selected_cid,
                "form_auth_verified":        True,
                "form_auth_email":           sel_email,
                "form_player_id":            sel_pid,
                "form_player_name":          sel_pname,
                "form_is_new":               False,
                "form_is_existing_auth":     True,
                "form_need_set_password":    bool(st.session_state.get("sel_need_set_password", False)),
                "form_step":                 1,
            })
            _save_form_session_to_cookie(
                _FORM_COOKIE_MGR, selected_cid, sel_pid, sel_pname)
            for k in ["selector_mode","sel_email_submitted","sel_email",
                      "sel_player_id","sel_player_name","sel_has_password",
                      "sel_pw_verified","sel_need_set_password","sel_concert_select",
                      "sel_magic_sent","sel_magic_hash","sel_magic_expires","sel_magic_attempts","sel_magic_last_sent",
                      "sel_preselect_cid","sel_cid_autorouted"]:
                st.session_state.pop(k, None)
            st.rerun()

        if my_concerts:
            concert_opts = {
                ext(c, CONCERT_NAME_KEYS) or c.get("id","")[:8]: c.get("id","")
                for c in my_concerts
            }
            # 招待コード経由で追加直後の演奏会を優先選択
            _preselect_cid = st.session_state.get("sel_preselect_cid", "")

            # URLにcidがあり、参加済み演奏会なら自動遷移
            _cid_q = (st.query_params.get("cid") or "").strip()
            if (not _preselect_cid) and _cid_q and _cid_q in concert_opts.values() and not st.session_state.get("sel_cid_autorouted"):
                st.session_state["sel_cid_autorouted"] = True
                _go_selected_concert(_cid_q)

            _labels = list(concert_opts.keys())
            _default_index = 0
            if _preselect_cid:
                for i, _label in enumerate(_labels):
                    if concert_opts.get(_label) == _preselect_cid:
                        _default_index = i
                        break
            selected_name = st.selectbox(
                "参加している演奏会を選択してください",
                _labels,
                index=_default_index,
                key="sel_concert_select",
            )
            if st.button("この演奏会に進む →", type="primary",
                         use_container_width=True, key="sel_concert_go"):
                _go_selected_concert(concert_opts[selected_name])
        else:
            st.info("演奏会出演履歴がデータベース上にありません。招待コードを入力してください。")

        st.divider()
        st.markdown("**🎟 招待コードで演奏会に参加する**")
        with st.form("sel_add_concert_form"):
            sel_invite_code = st.text_input("招待コード", max_chars=20, placeholder="例: K7X2MN9A")
            sel_part_name = st.selectbox(
                "担当パート *",
                ["（選択してください）"] + pm_names,
                key="sel_add_part_select",
            )
            submitted_invite = st.form_submit_button("この招待コードで追加する", use_container_width=True)
        if submitted_invite:
            if _invite_rate_limited():
                return
            cid, invite_err = _resolve_concert_id_by_invite_code(ctx, sel_invite_code)
            if not cid:
                _record_invite_failure()
                if invite_err == "ambiguous":
                    st.error("同じ招待コードが複数の演奏会に設定されています。管理者に連絡してください。")
                elif invite_err == "invalid":
                    st.error("招待コードを入力してください。")
                else:
                    st.error("招待コードが見つかりませんでした。管理者に確認してください。")
            else:
                st.session_state.pop("sel_invite_fail_ts", None)
                part_id = pm_name_to_id.get(sel_part_name, "")
                if not part_id:
                    st.error("担当パートを選択してください。")
                    return
                ok, msg = _link_player_to_concert_by_invite(ctx, sel_pid, cid, part_id)
                if ok:
                    st.session_state["sel_preselect_cid"] = cid
                    st.success("演奏会を追加しました。")
                    st.rerun()
                else:
                    st.error(msg)

        if st.button("🔓 ログアウト", use_container_width=True, key="sel_logout"):
            _logout_to_entry(_FORM_COOKIE_MGR)


# ── フォームメイン ────────────────────────────────────────────

def _resolve_concert_id_by_invite_code(ctx, code: str) -> tuple[str, str]:
    """招待コードからconcert_idを解決する。
    戻り値: (concert_id, error_code)
      - ("<cid>", ""): 正常
      - ("", "not_found"): 未検出
      - ("", "ambiguous"): 同一コードが複数演奏会に存在
      - ("", "invalid"): 入力不正
    """
    try:
        norm_code = (code or "").strip().upper()
        if not norm_code:
            return "", "invalid"
        hc_rows = ctx["query_all"](ctx["CONCERT_DB_HARMONIA_CONCERT"], None)
        ext = ctx["extract_prop_text_any"]
        matched_cids: list[str] = []
        for r in hc_rows:
            stored = (ext(r, HARMONIA_CONCERT_INVITE_CODE_KEYS) or "").strip().upper()
            if stored and stored == norm_code:
                rel_ids = ctx["extract_relation_ids_any"](r, ["演奏会", "FK演奏会", "concert"])
                if rel_ids:
                    matched_cids.extend([cid for cid in rel_ids if cid])
        uniq = sorted(set(matched_cids))
        if len(uniq) == 1:
            return uniq[0], ""
        if len(uniq) >= 2:
            return "", "ambiguous"
    except Exception:
        pass
    return "", "not_found"


def _get_my_concerts(ctx, player_id: str) -> list[dict]:
    """CONCERT_CASTからplayer_idに紐づく演奏会情報のリストを返す。"""
    try:
        ext_rel = ctx["extract_relation_ids_any"]
        ext     = ctx["extract_prop_text_any"]
        all_cast = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
        concert_ids = [
            ext_rel(r, ["出演", "演奏会", "FK演奏会"])[0]
            for r in all_cast
            if player_id in ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
            and ext_rel(r, ["出演", "演奏会", "FK演奏会"])
        ]
        if not concert_ids:
            return []
        all_concerts = ctx["query_all"](ctx["CONCERT_DB_CONCERT"], None)
        result = []
        for cid in concert_ids:
            c = next((r for r in all_concerts if r.get("id") == cid), None)
            if c:
                result.append(c)
        # 本番日の降順でソート
        result.sort(key=lambda r: ext(r, CONCERT_DATE_KEYS) or "", reverse=True)
        return result
    except Exception:
        return []

def _link_player_to_concert_by_invite(ctx, player_id: str, concert_id: str,
                                      part_master_id: str) -> tuple[bool, str]:
    """既存奏者を指定演奏会へ紐付け（CONCERT_CAST作成）。既存なら成功扱い。"""
    try:
        ext_rel = ctx["extract_relation_ids_any"]
        cast_db = ctx["CONCERT_DB_PARTICIPANT"]
        t_cast = ctx["get_prop_types"](cast_db)
        if not t_cast:
            return False, "CONCERT_CASTの設定取得に失敗しました。"

        all_cast = ctx["query_all"](cast_db, None)
        # 既存紐付けチェック
        for r in all_cast:
            if (
                player_id in ext_rel(r, PARTICIPANT_PLAYER_REL_KEYS)
                and concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)
            ):
                return True, "already_linked"

        if not part_master_id:
            return False, "担当パートを選択してください。"

        props: dict = {}
        ctx["put_prop_any"](props, t_cast, PARTICIPANT_CONCERT_REL_KEYS, concert_id)
        ctx["put_prop_any"](props, t_cast, PARTICIPANT_PLAYER_REL_KEYS, player_id)
        ctx["put_prop_any"](props, t_cast, PARTICIPANT_PART_REL_KEYS, part_master_id)
        ctx["put_key_any"](props, t_cast, PARTICIPANT_RECORD_KEYS, concert_id, player_id, prefix="participant")
        res = ctx["api_request"](
            "post",
            "https://api.notion.com/v1/pages",
            json={"parent": {"database_id": cast_db}, "properties": props},
        )
        if res and res.status_code == 200:
            return True, "linked"
        return False, f"紐付けに失敗しました (status={getattr(res, 'status_code', '?')})"
    except Exception as e:
        return False, f"紐付けに失敗しました: {e}"


_FORM_COOKIE_PREFIX = "harmonia_form_"
_FORM_COOKIE_MAX_AGE = 60 * 60 * 24 * 30  # 30日間


def _get_form_cookie_manager():
    return _FORM_COOKIE_MGR


def _save_form_session_to_cookie(cookie_mgr, concert_id: str, player_id: str,
                                  player_name: str) -> None:
    """ログイン状態をクッキーに保存する。session_stateにも pending として記録。"""
    if cookie_mgr is None:
        return
    try:
        cookie_mgr.set(f"{_FORM_COOKIE_PREFIX}concert_id",  concert_id,
                       max_age=_FORM_COOKIE_MAX_AGE)
        cookie_mgr.set(f"{_FORM_COOKIE_PREFIX}player_id",   player_id,
                       max_age=_FORM_COOKIE_MAX_AGE)
        cookie_mgr.set(f"{_FORM_COOKIE_PREFIX}player_name", player_name,
                       max_age=_FORM_COOKIE_MAX_AGE)
    except Exception:
        pass
    # rerun後にも再保存できるようにsession_stateにも記録
    st.session_state["_form_cookie_pending"] = {
        "concert_id": concert_id,
        "player_id": player_id,
        "player_name": player_name,
    }
    st.session_state.pop("form_skip_cookie_restore", None)


def _clear_form_cookie(cookie_mgr) -> None:
    """ログアウト時にクッキーを削除する。"""
    if cookie_mgr is None: return
    for key in ("concert_id", "player_id", "player_name"):
        try:
            cookie_mgr.delete(f"{_FORM_COOKIE_PREFIX}{key}")
        except Exception:
            pass

def _logout_to_entry(cookie_mgr) -> None:
    """ログアウトして入口画面（concert_id未確定）へ戻す。"""
    try:
        _clear_form_cookie(cookie_mgr)
    except Exception:
        pass
    for k in list(st.session_state.keys()):
        if (
            k.startswith("form_")
            or k.startswith("auth_")
            or k.startswith("sel_")
            or k == "selector_mode"
            or k == "_form_cookie_pending"
        ):
            st.session_state.pop(k, None)
    st.session_state["form_skip_cookie_restore"] = True
    try:
        st.query_params.clear()
    except Exception:
        pass
    st.rerun()


def _restore_form_session_from_cookie(cookie_mgr) -> bool:
    """クッキーからログイン状態を復元する。復元できたらTrueを返す。"""
    if st.session_state.get("form_skip_cookie_restore"):
        return False
    if st.session_state.get("form_is_existing_auth"): return False  # 既にログイン済み

    # pendingデータがあればクッキーに再保存（rerun後の保存）
    pending = st.session_state.pop("_form_cookie_pending", None)
    if pending and cookie_mgr is not None:
        try:
            cookie_mgr.set(f"{_FORM_COOKIE_PREFIX}concert_id",  pending["concert_id"],
                           max_age=_FORM_COOKIE_MAX_AGE)
            cookie_mgr.set(f"{_FORM_COOKIE_PREFIX}player_id",   pending["player_id"],
                           max_age=_FORM_COOKIE_MAX_AGE)
            cookie_mgr.set(f"{_FORM_COOKIE_PREFIX}player_name", pending["player_name"],
                           max_age=_FORM_COOKIE_MAX_AGE)
        except Exception:
            pass

    if cookie_mgr is None: return False
    try:
        concert_id  = cookie_mgr.get(f"{_FORM_COOKIE_PREFIX}concert_id")
        player_id   = cookie_mgr.get(f"{_FORM_COOKIE_PREFIX}player_id")
        player_name = cookie_mgr.get(f"{_FORM_COOKIE_PREFIX}player_name")
        if concert_id and player_id:
            st.session_state["form_resolved_concert_id"] = concert_id
            st.session_state["form_player_id"]           = player_id
            st.session_state["form_player_name"]         = player_name or ""
            st.session_state["form_is_existing_auth"]    = True
            st.session_state["form_auth_verified"]       = True
            return True
    except Exception:
        pass
    return False


def render_form(ctx, concert_id: str = ""):
    ext = ctx["extract_prop_text_any"]
    _inject_form_styles()

    # クッキーからセッション復元（サーバー再起動後のログイン維持）
    _form_cookie_mgr = _FORM_COOKIE_MGR
    _restore_form_session_from_cookie(_form_cookie_mgr)

    # form.py を直接 entrypoint にしていない場合でもロゴを表示
    _render_brand_logo()

    # ── concert_id未確定時：ログイン→演奏会選択 or 招待コード入力 ──
    if not concert_id:
        concert_id = st.session_state.get("form_resolved_concert_id", "")

    if not concert_id:
        _render_concert_selector(ctx)
        return

    # 初回のみデータ一括取得
    if st.session_state.get("form_data_loaded") != concert_id:
        _progress_bar = st.progress(0, text="🎵 フォームを読み込み中...")
        def _on_progress(ratio: float, text: str):
            _progress_bar.progress(ratio, text=text)
        _load_form_data(ctx, concert_id, progress=_on_progress)
        _progress_bar.empty()

    concert   = st.session_state.get("form_concert")
    practices = st.session_state.get("form_practices", [])
    songs     = st.session_state.get("form_songs", [])
    partdefs  = st.session_state.get("form_partdefs", [])
    inst_map  = st.session_state.get("form_inst_map", {})
    req_insts = st.session_state.get("form_req_insts", [])
    part_opts = st.session_state.get("form_part_opts", [OTHER_PART])

    if not concert:
        st.error("演奏会が見つかりません。URLを確認してください。")
        return

    # ── 演奏会情報ヘッダー ────────────────────────────────────
    c_name      = ext(concert, CONCERT_NAME_KEYS) or "演奏会"
    c_date      = (ext(concert, CONCERT_DATE_KEYS) or "")[:10]
    c_venue     = ext(concert, CONCERT_VENUE_KEYS) or ""
    c_conductor = ext(concert, CONCERT_CONDUCTOR_KEYS) or ""
    c_soloist   = ext(concert, CONCERT_SOLOIST_KEYS) or ""

    # カバー画像（あれば表示）
    if concert:
        _cover = concert.get("cover") or {}
        _cover_type = _cover.get("type", "")
        _cover_url = ""
        if _cover_type == "external":
            _cover_url = (_cover.get("external") or {}).get("url", "")
        elif _cover_type == "file":
            _cover_url = (_cover.get("file") or {}).get("url", "")
        if _cover_url:
            st.image(_cover_url, use_container_width=True)

    # 演奏会情報カード
    _meta_rows = ""
    if c_date:      _meta_rows += f'<div class="h-concert-row"><div class="h-concert-dot"></div>{c_date}</div>'
    if c_venue:     _meta_rows += f'<div class="h-concert-row"><div class="h-concert-dot"></div>{c_venue}</div>'
    if c_conductor: _meta_rows += f'<div class="h-concert-row"><div class="h-concert-dot"></div>指揮：{c_conductor}</div>'
    if c_soloist:   _meta_rows += f'<div class="h-concert-row"><div class="h-concert-dot"></div>ソリスト：{c_soloist}</div>'
    if songs:
        _song_txt = "　/　".join(
            f"{get_song_display_name(ctx, s)}" for s in songs
        )
        _meta_rows += f'<div class="h-concert-row"><div class="h-concert-dot"></div>{_song_txt}</div>'

    st.html(f"""
        <div class="h-f2 h-concert-card">
            <div class="h-concert-name">{c_name}</div>
            {_meta_rows}
        </div>
        """)

    if st.query_params.get("debug") == "1" and st.session_state.get("form_att_debug"):
        with st.expander("🔧 出欠読込デバッグ", expanded=False):
            st.json(st.session_state.get("form_att_debug"))

    step = st.session_state.get("form_step", 1)

    # STEP2以降は認証済みであることを確認（認証バイパス防止）
    if step > 1 and not st.session_state.get("form_auth_verified"):
        st.warning("セッションが切れました。最初からやり直してください。")
        st.session_state.clear()
        st.rerun()
        return

    # ── STEP 0: 新規登録 / ログイン 選択 ─────────────────────
    if step == 1 and not st.session_state.get("form_auth_verified") and not st.session_state.get("form_auth_mode"):
        st.html('<div class="h-f3 h-scr-title">はじめに</div>')
        st.html('<div class="h-scr-sub">初めての方は新規登録、2回目以降はログインをお選びください</div>')
        col_new, col_login = st.columns(2)
        if col_new.button("📝 新規登録", use_container_width=True, type="primary", key="mode_new"):
            st.session_state["form_auth_mode"] = "new"
            st.rerun()
        if col_login.button("🔑 ログイン", use_container_width=True, key="mode_login"):
            st.session_state["form_auth_mode"] = "login"
            st.rerun()
        return

    # ── STEP 0a: プライバシーポリシー同意（新規登録のみ） ─────
    auth_mode = st.session_state.get("form_auth_mode", "new")
    if step == 1 and not st.session_state.get("form_auth_verified") and auth_mode == "new" and not st.session_state.get("form_privacy_agreed"):
        st.html('<div class="h-scr-title">プライバシーポリシー</div>')
        st.markdown(_PRIVACY_POLICY)
        col_agree, col_back = st.columns(2)
        if col_agree.button("✅ 同意して進む", type="primary", use_container_width=True, key="privacy_agree"):
            st.session_state["form_privacy_agreed"] = True
            st.rerun()
        if col_back.button("← 戻る", use_container_width=True, key="privacy_back"):
            st.session_state.pop("form_auth_mode", None)
            st.rerun()
        return

    # ── STEP 0b: 認証（既登録者→パスワード / 新規→マジックコード） ──
    if step == 1 and not st.session_state.get("form_auth_verified"):

        # メールアドレス入力フェーズ
        if not st.session_state.get("auth_email_submitted"):
            if auth_mode == "login":
                st.html('<div class="h-f3 h-scr-title">ログイン</div>')
                st.html('<div class="h-scr-sub">登録済みのメールアドレスを入力してください</div>')
            else:
                st.html('<div class="h-f3 h-scr-title">メールアドレスを入力</div>')
                st.html('<div class="h-scr-sub">確認コードをお送りします</div>')
            with st.form("auth_email_form"):
                auth_email = st.text_input("メールアドレス *", placeholder="yamada@example.com")
                submitted_email = st.form_submit_button("次へ", type="primary",
                                                         use_container_width=True)
            if submitted_email:
                if not auth_email.strip():
                    st.error("メールアドレスを入力してください。")
                    return
                email_input = auth_email.strip().lower()
                with st.spinner("確認中..."):
                    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
                    matched = next(
                        (p for p in players
                         if (ext(p, PLAYER_EMAIL_KEYS) or "").strip().lower() == email_input),
                        None
                    )
                # ログインモードで未登録メールの場合
                if auth_mode == "login" and not matched:
                    st.error("このメールアドレスは登録されていません。新規登録からお進みください。")
                    return
                has_password = bool(
                    matched and ext(matched, PLAYER_PASSWORD_HASH_KEYS)
                ) if matched else False
                st.session_state.update({
                    "auth_email":           email_input,
                    "auth_player_id":       matched.get("id","") if matched else None,
                    "auth_is_existing":     matched is not None,
                    "auth_has_password":    has_password,
                    "auth_email_submitted": True,
                })
                st.rerun()
            if st.button("← 戻る", key="email_back"):
                st.session_state.pop("form_auth_mode", None)
                st.session_state.pop("form_privacy_agreed", None)
                st.rerun()
            return

        auth_email   = st.session_state.get("auth_email", "")
        is_existing  = st.session_state.get("auth_is_existing", False)
        has_password = st.session_state.get("auth_has_password", False)
        existing_pid = st.session_state.get("auth_player_id")

        # ── 既登録者 × パスワードあり → パスワードログイン ──
        if is_existing and has_password and not st.session_state.get("auth_code_sent"):
            players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
            matched_player = next((p for p in players if p.get("id") == existing_pid), None)
            pname_display = ext(matched_player, PLAYER_NAME_KEYS) if matched_player else ""
            _greet = f"おかえりなさい、{pname_display} さん" if pname_display else "パスワードでログイン"
            st.html(f'<div class="h-f3 h-scr-title">{_greet}</div>')
            st.html(f'<div class="h-scr-sub">{auth_email}</div>')
            with st.form("auth_password_form"):
                entered_pw = st.text_input("パスワード", type="password")
                col_ok, col_forgot = st.columns(2)
                submitted_pw     = col_ok.form_submit_button("ログイン", type="primary", use_container_width=True)
                submitted_forgot = col_forgot.form_submit_button("パスワードを忘れた", use_container_width=True)
            if submitted_pw:
                stored_hash = ext(matched_player, PLAYER_PASSWORD_HASH_KEYS) if matched_player else ""
                if _verify_password(entered_pw, stored_hash):
                    st.session_state.update({
                        "form_auth_verified":     True,
                        "form_auth_email":        auth_email,
                        "form_player_id":         existing_pid,
                        "form_player_name":       pname_display,
                        "form_is_new":            False,
                        "form_is_existing_auth":  True,
                    })
                    st.rerun()
                else:
                    st.error("パスワードが違います。")
            if submitted_forgot:
                # マジックコードによるリセットフローへ
                st.session_state["auth_reset_mode"] = True
                st.rerun()
            return

        # ── パスワードリセット or 新規 or パスワード未設定 → マジックコード ──
        is_reset = st.session_state.get("auth_reset_mode", False)
        if is_reset:
            st.html('<div class="h-scr-title">パスワードをリセット</div>')
            st.caption("登録済みのメールアドレスに確認コードを送ります。")
        elif is_existing and not has_password:
            st.html('<div class="h-scr-title">初回ログイン</div>')
            st.caption("確認コードを送りますので、ログイン後にパスワードを設定してください。")
        else:
            st.html('<div class="h-scr-title">メールアドレスで本人確認</div>')
            st.caption("確認コードを送ります。")

        if not st.session_state.get("auth_code_sent"):
            last_sent = st.session_state.get("auth_last_sent")
            if last_sent and (datetime.now() - last_sent).total_seconds() < 10:
                st.warning("少し時間をおいてから再試行してください。")
                return
            code = _generate_code()
            ok   = _send_magic_code(ctx, auth_email, code, c_name)
            if ok:
                st.session_state.update({
                    "auth_code_hash":    _hash_code(code),
                    "auth_code_expires": datetime.now() + timedelta(minutes=_CODE_EXPIRY_MINUTES),
                    "auth_attempts":     0,
                    "auth_code_sent":    True,
                    "auth_last_sent":    datetime.now(),
                })
                st.rerun()
            else:
                st.error("メール送信に失敗しました。管理者にお問い合わせください。")
            return

        # コード入力フォーム
        attempts = st.session_state.get("auth_attempts", 0)
        st.info("確認コードをメールで送信しました。迷惑メールフォルダもご確認ください。")
        st.caption(f"送信先: {auth_email}")

        if not _is_code_valid():
            st.warning("確認コードの有効期限が切れました。最初からやり直してください。")
            for k in ["auth_code_sent","auth_code_hash","auth_code_expires",
                      "auth_attempts","auth_last_sent","auth_reset_mode","auth_email_submitted"]:
                st.session_state.pop(k, None)
            st.rerun()
            return

        with st.form("auth_code_form"):
            entered_code = st.text_input("確認コード（6桁）", max_chars=6, placeholder="123456")
            col_ok, col_resend = st.columns(2)
            submitted_code   = col_ok.form_submit_button("確認", type="primary", use_container_width=True)
            submitted_resend = col_resend.form_submit_button("再送信", use_container_width=True)

        if submitted_resend:
            last_sent = st.session_state.get("auth_last_sent")
            if last_sent and (datetime.now() - last_sent).total_seconds() < _CODE_RESEND_SECONDS:
                remaining_sec = int(_CODE_RESEND_SECONDS - (datetime.now() - last_sent).total_seconds())
                st.warning(f"再送信は{remaining_sec}秒後に行えます。")
                return
            code = _generate_code()
            ok   = _send_magic_code(ctx, auth_email, code, c_name)
            if ok:
                st.session_state.update({
                    "auth_code_hash":    _hash_code(code),
                    "auth_code_expires": datetime.now() + timedelta(minutes=_CODE_EXPIRY_MINUTES),
                    "auth_attempts":     0,
                    "auth_last_sent":    datetime.now(),
                })
                st.success("確認コードを再送しました。")
            else:
                st.error("再送に失敗しました。")
            return

        if submitted_code:
            stored_hash = st.session_state.get("auth_code_hash", "")
            if _verify_code(entered_code, stored_hash):
                for k in ["auth_code_hash","auth_code_expires","auth_attempts",
                           "auth_code_sent","auth_last_sent"]:
                    st.session_state.pop(k, None)
                st.session_state["form_auth_verified"] = True
                st.session_state["form_auth_email"]    = auth_email
                if is_existing and existing_pid:
                    players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
                    matched_player = next((p for p in players if p.get("id") == existing_pid), None)
                    _pname_cookie = ext(matched_player, PLAYER_NAME_KEYS) if matched_player else ""
                    st.session_state["form_player_id"]   = existing_pid
                    st.session_state["form_player_name"] = _pname_cookie
                    st.session_state["form_is_new"]           = False
                    st.session_state["form_is_existing_auth"] = True
                    # パスワード未設定 or リセット → パスワード設定フラグ
                    st.session_state["form_need_set_password"] = True
                    # クッキーに保存（concert_idはresolved済みのものを使用）
                    _cid_for_cookie = st.session_state.get("form_resolved_concert_id", concert_id)
                    _save_form_session_to_cookie(
                        _form_cookie_mgr, _cid_for_cookie, existing_pid, _pname_cookie)
                else:
                    st.session_state["form_is_new"]           = True
                    st.session_state["form_is_existing_auth"] = False
                st.session_state.pop("auth_reset_mode", None)
                st.rerun()
            else:
                attempts += 1
                st.session_state["auth_attempts"] = attempts
                remaining = _CODE_MAX_ATTEMPTS - attempts
                if remaining <= 0:
                    st.error(f"確認コードを{_CODE_MAX_ATTEMPTS}回間違えました。最初からやり直してください。")
                    for k in ["auth_code_hash","auth_code_expires","auth_attempts",
                               "auth_code_sent","auth_last_sent","auth_player_id",
                               "auth_is_existing","auth_email_submitted"]:
                        st.session_state.pop(k, None)
                    st.rerun()
                else:
                    st.error(f"確認コードが違います。あと{remaining}回入力できます。")
        return

    # ── STEP 1: 氏名・連絡先・パート ──────────────────────────
    if step == 1:
        # ── パスワード設定が必要な場合（初回マジックコード認証後） ──
        if st.session_state.get("form_need_set_password") and st.session_state.get("form_is_existing_auth"):
            pname = st.session_state.get("form_player_name", "")
            st.html(f'<div class="h-scr-title">{pname} さん、パスワードを設定してください</div>')
            st.caption("次回から、メールアドレスとパスワードでログインできます。")
            with st.form("set_password_form"):
                pw1 = st.text_input("パスワード（6文字以上）", type="password")
                pw2 = st.text_input("パスワード（確認）", type="password")
                col_set, col_skip = st.columns(2)
                submitted_set  = col_set.form_submit_button("設定する", type="primary", use_container_width=True)
                submitted_skip = col_skip.form_submit_button("スキップ", use_container_width=True)
            if submitted_set:
                if len(pw1.strip()) < 6:
                    st.error("パスワードは6文字以上で入力してください。")
                elif pw1 != pw2:
                    st.error("パスワードが一致しません。")
                else:
                    pid = st.session_state.get("form_player_id", "")
                    ok = _save_password_hash(ctx, pid, pw1)
                    if ok:
                        st.session_state.pop("form_need_set_password", None)
                        st.cache_data.clear()
                        st.success("✅ パスワードを設定しました。")
                        # 新規登録の場合はSTEP2へ
                        if st.session_state.get("form_is_new"):
                            st.session_state["form_step"] = 2
                        st.rerun()
                    else:
                        st.error("パスワードの設定に失敗しました。スキップして進んでください。")
            if submitted_skip:
                st.session_state.pop("form_need_set_password", None)
                # 新規登録の場合はSTEP2へ
                if st.session_state.get("form_is_new"):
                    st.session_state["form_step"] = 2
                st.rerun()
            return

        # ── 既存奏者: メニュー形式 ────────────────────────────
        if st.session_state.get("form_is_existing_auth"):
            pname = st.session_state.get("form_player_name", "")
            pid   = st.session_state.get("form_player_id", "")

            # パート・ロールをCONCERT_CASTから取得
            participant_rows = st.session_state.get("form_participant_rows_concert")
            if participant_rows is None:
                participant_rows = [
                    r for r in ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
                    if concert_id in ctx["extract_relation_ids_any"](r, PARTICIPANT_CONCERT_REL_KEYS)
                ]
            my_part = ""
            my_part_master_id = ""
            my_cast_row = None
            for row in participant_rows:
                p_ids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PLAYER_REL_KEYS)
                c_ids = ctx["extract_relation_ids_any"](row, PARTICIPANT_CONCERT_REL_KEYS)
                if pid in p_ids and concert_id in c_ids:
                    part_ids = ctx["extract_relation_ids_any"](row, PARTICIPANT_PART_REL_KEYS)
                    if part_ids:
                        my_part_master_id = part_ids[0]
                        pm_map_local: dict = st.session_state.get("form_part_master_map") or {}
                        my_part = pm_map_local.get(my_part_master_id, {}).get("name", "")
                    my_cast_row = row
                    break
            st.session_state["form_player_part"]    = my_part
            st.session_state["form_player_part_id"] = my_part_master_id

            # ロール解決
            user_role = _resolve_user_role(ctx, pid, concert_id)
            st.session_state["form_user_role"] = user_role

            if st.query_params.get("debug") == "1":
                st.caption(f"🔧 DEBUG: user_role={user_role} (0=Player,1=Leader,2=Manager), pid={pid[:8]}, concert_id={concert_id[:8]}")

            # 案提示フラグ確認
            proposal_done = _get_proposal_flag(ctx, concert_id)

            # 出欠・希望の回答状況
            _, p_to_att_map = _get_form_cast_and_att_map(ctx, concert_id, pid)
            att_answered = sum(1 for pr in practices
                               if (p_to_att_map.get(pr.get("id",""), {}) or {}).get("status","") not in ("", "未回答"))
            att_total    = len(practices)
            existing_pref = _load_existing_prefs(ctx, concert_id, pid, partdefs)
            pref_answered = sum(1 for v in existing_pref.values() if v not in ("未回答", ""))
            pref_total    = len(partdefs)

            _role_label = {ROLE_LEADER: "パートリーダー", ROLE_MANAGER: "Manager"}.get(user_role, "")
            _part_disp  = f"{my_part}　{_role_label}" if my_part and _role_label else (my_part or _role_label)

            # ── グリーティング ──
            st.html(f"""
                <div class="h-f3 h-greet">
                    こんにちは、<strong>{pname}</strong> さん
                    {"" if not _part_disp else f'<span class="h-greet-part">{_part_disp}</span>'}
                </div>
                """)

            # ── メニューカード ──
            att_unanswered = att_total - att_answered
            att_badge_html = (f'<div class="h-badge">{att_unanswered}件</div>' if att_unanswered > 0
                              else '<div class="h-badge h-badge-ok">完了</div>')
            att_hint = f"未回答 {att_unanswered}件" if att_unanswered > 0 else f"{att_answered}/{att_total}回 回答済"

            pref_badge_html = ""
            pref_hint = ""
            if pref_total > 0:
                if pref_answered == pref_total:
                    pref_badge_html = '<div class="h-badge h-badge-ok">完了</div>'
                    pref_hint = "入力済み"
                elif proposal_done:
                    pref_badge_html = '<div class="h-badge" style="background:rgba(180,100,240,.15);color:#c070f0;">確定済</div>'
                    pref_hint = "アサイン案提示中"
                else:
                    pref_badge_html = f'<div class="h-badge">{pref_total - pref_answered}件未入力</div>'
                    pref_hint = f"{pref_answered}/{pref_total}パート 回答済"

            st.html(f"""
                <div class="h-f4">
                  <div class="h-section">メニュー</div>
                  <div class="h-menu-item" id="menu-att">
                    <div class="h-mi-icon h-ic-a">📅</div>
                    <div class="h-mi-body">
                      <div class="h-mi-ttl">出欠入力</div>
                      <div class="h-mi-hint">{att_hint}</div>
                    </div>
                    <div class="h-mi-right">{att_badge_html}<div class="h-chev">›</div></div>
                  </div>
                  {"" if pref_total == 0 else f'''
                  <div class="h-menu-item" id="menu-pref">
                    <div class="h-mi-icon h-ic-b">🎵</div>
                    <div class="h-mi-body">
                      <div class="h-mi-ttl">楽器・パート希望</div>
                      <div class="h-mi-hint">{pref_hint}</div>
                    </div>
                    <div class="h-mi-right">{pref_badge_html}<div class="h-chev">›</div></div>
                  </div>
                  '''}
                  {"" if not IS_PERC(my_part) else '''
                  <div class="h-menu-item" id="menu-own">
                    <div class="h-mi-icon h-ic-d">🥁</div>
                    <div class="h-mi-body">
                      <div class="h-mi-ttl">所有楽器</div>
                      <div class="h-mi-hint">入力・変更</div>
                    </div>
                    <div class="h-mi-right"><div class="h-chev">›</div></div>
                  </div>
                  '''}
                </div>
                """)

            # 実際のボタン（非表示HTMLカードの裏で動くStreamlitボタン）
            if st.button("📅 出欠を入力・変更する", use_container_width=True, key="menu_att"):
                _, existing_att_map = _get_form_cast_and_att_map(ctx, concert_id, pid)
                preload_att: dict[str, str] = {}
                preload_comment: dict[str, str] = {}
                for _pr_id, _v in (existing_att_map or {}).items():
                    preload_att[_pr_id] = (_v or {}).get("status", "")
                    preload_comment[_pr_id] = (_v or {}).get("comment", "")
                st.session_state.update({
                    "form_att": preload_att,
                    "form_att_comment": preload_comment,
                    "form_step": 2,
                    "form_menu_mode": True,
                })
                st.rerun()

            if partdefs:
                if proposal_done:
                    st.button("🎵 パート希望（アサイン案提示中のため変更不可）",
                              use_container_width=True, disabled=True, key="menu_pref_disabled")
                    st.caption("アサイン案が提示されています。変更が必要な場合は管理者にご連絡ください。")
                else:
                    if st.button("🎵 パート希望を入力・変更する", use_container_width=True, key="menu_pref"):
                        st.session_state.update({
                            "form_pref": existing_pref,
                            "form_step": 3,
                            "form_menu_mode": True,
                        })
                        st.rerun()

            # アサイン閲覧
            _can_show_assign_menu = (user_role >= ROLE_LEADER) or proposal_done or _has_published_assignments(ctx, concert_id)
            if _can_show_assign_menu:
                if user_role == ROLE_MANAGER:
                    _assign_label = "アサイン状況（パート/曲で絞り込み）"
                elif user_role == ROLE_LEADER:
                    _assign_label = "自パートのアサイン状況（曲で絞り込み）"
                else:
                    _assign_label = "自パートのアサイン状況"
                if _collapsible_open(f"🎯 {_assign_label}", "assign_view", default_open=False):
                    _render_assignment_view(ctx, concert_id, my_part_master_id, user_role)

            if IS_PERC(my_part):
                if st.button("🥁 所有楽器を入力・変更する", use_container_width=True, key="menu_own"):
                    st.session_state.update({
                        "form_own":  {},
                        "form_step": 4,
                        "form_menu_mode": True,
                    })
                    st.rerun()

            # ── Leader / Manager 専用メニュー ─────────────────
            if user_role >= ROLE_LEADER:
                _section_label = "パートリーダーメニュー" if user_role == ROLE_LEADER else "Managerメニュー"
                st.html(f'<div class="h-section" style="margin-top:18px;">🎖 {_section_label}</div>')
                tab_att, tab_mem, tab_doc = st.tabs(["📋 出欠", "👥 メンバー", "📄 資料"])
                with tab_att:
                    _att_label = "自パートの出欠一覧" if user_role == ROLE_LEADER else "全員の出欠一覧"
                    if _collapsible_open(f"📋 {_att_label}", "leader_att_overview", default_open=False):
                        if practices:
                            _render_attendance_overview(
                                ctx, concert_id, participant_rows, practices,
                                my_part_master_id, user_role
                            )
                        else:
                            st.info("練習日が登録されていません。")
                with tab_mem:
                    _mem_label = "自パートのメンバー一覧" if user_role == ROLE_LEADER else "全員のメンバー一覧（連絡先含む）"
                    if _collapsible_open(f"👥 {_mem_label}", "leader_member_overview", default_open=False):
                        _render_member_list(
                            ctx, concert_id, participant_rows,
                            my_part_master_id, user_role
                        )
                with tab_doc:
                    if practices:
                        if _collapsible_open("📄 練習情報PDF（全練習回）", "leader_all_practice_pdf", default_open=False):
                            _all_sched  = ctx["query_all"](ctx.get("CONCERT_DB_SCHEDULE","") or "", None) if ctx.get("CONCERT_DB_SCHEDULE") else []
                            _all_rental = ctx["query_all"](ctx.get("CONCERT_DB_RENTAL","") or "", None) if ctx.get("CONCERT_DB_RENTAL") else []
                            _all_pi     = ctx["query_all"](ctx.get("CONCERT_DB_PLAYER_INSTRUMENT","") or "", None) if ctx.get("CONCERT_DB_PLAYER_INSTRUMENT") else []

                            for _pr in practices:
                                _pr_id   = _pr.get("id","")
                                _pr_name = ext(_pr, PRACTICE_NAME_KEYS) or "練習"
                                _pr_date = (ext(_pr, PRACTICE_DATE_KEYS) or "")[:10]
                                _pdf_key = f"form_pdf_bytes_{_pr_id}"

                                _venue_ok   = bool(ext(_pr, PRACTICE_VENUE_KEYS))
                                _sched_ok   = any(
                                    _pr_id in ctx["extract_relation_ids_any"](r, ["練習","FK練習","practice"])
                                    for r in _all_sched
                                )
                                _rental_ok  = any(
                                    _pr_id in ctx["extract_relation_ids_any"](r, ["練習","FK練習","practice"])
                                    and ctx["extract_prop_text_any"](r, ["確定フラグ","確定","Confirmed"]) == "True"
                                    for r in _all_rental
                                )
                                _bring_ok   = any(
                                    _pr_id in ctx["extract_relation_ids_any"](r, ["練習","FK練習","practice"])
                                    and ctx["extract_prop_text_any"](r, ["持参担当","持参担当フラグ"]) == "True"
                                    for r in _all_pi
                                )
                                _inst_ok = _rental_ok or _bring_ok

                                def _badge(ok: bool, label: str) -> str:
                                    return f"{'✅' if ok else '⚠️'} {label}"

                                _col_a, _col_b = st.columns([3, 2])
                                _col_a.caption(f"**{_pr_date}　{_pr_name}**")
                                _col_a.caption(
                                    f"{_badge(_venue_ok,'会場')}　"
                                    f"{_badge(_sched_ok,'スケジュール')}　"
                                    f"{_badge(_inst_ok,'レンタル/持参')}"
                                )
                                if _pdf_key not in st.session_state:
                                    if _col_b.button("生成", key=f"leader_gen_{_pr_id}", use_container_width=True):
                                        with st.spinner("生成中..."):
                                            try:
                                                from concert.services.practice_report import generate_practice_report
                                                st.session_state[_pdf_key] = generate_practice_report(ctx, _pr_id)
                                                st.rerun()
                                            except Exception as _e:
                                                st.error(f"失敗: {_e}")
                                else:
                                    from concert.services.convert_utils import render_report_output
                                    render_report_output(
                                        st.session_state[_pdf_key],
                                        filename=f"練習情報_{_pr_name}",
                                        label=f"{_pr_name}",
                                        key_prefix=f"leader_{_pr_id}",
                                    )

            # ── 楽譜リンク（全ロール共通：自パートのもの） ──────
            _player_score_links: list[tuple[str,str]] = []
            for _pd in partdefs:
                _pd_pm_ids = ctx["extract_relation_ids_any"](_pd, PARTDEF_PART_REL_KEYS)
                _is_my_part = _pd_pm_ids and _pd_pm_ids[0] == my_part_master_id
                if not _pd_pm_ids:
                    _is_my_part = True
                if not _is_my_part:
                    continue
                _pd_url = ctx["extract_prop_text_any"](_pd, ["楽譜URL","score_url","ScoreURL"]) or ""
                _pd_lbl = ctx["extract_prop_text_any"](_pd, PARTDEF_NAME_KEYS) or "楽譜"
                if _pd_url:
                    _player_score_links.append((_pd_lbl, _pd_url))
                else:
                    _pd_song_ids = ctx["extract_relation_ids_any"](_pd, PARTDEF_SONG_REL_KEYS)
                    if _pd_song_ids:
                        _s = next((s for s in songs if s.get("id") == _pd_song_ids[0]), None)
                        if _s:
                            _s_url = ctx["extract_prop_text_any"](_s, ["楽譜URL","score_url","ScoreURL"]) or ""
                            _s_lbl = get_song_display_name(ctx, _s) or "楽譜"
                            if _s_url:
                                _player_score_links.append((f"{_s_lbl}（全体）", _s_url))
            _seen_urls: set[str] = set()
            _deduped_links = []
            for _lbl, _url in _player_score_links:
                if _url not in _seen_urls:
                    _seen_urls.add(_url)
                    _deduped_links.append((_lbl, _url))
            if _deduped_links:
                if _collapsible_open("🎼 楽譜リンク", "player_score_links", default_open=False):
                    for _lbl, _url in _deduped_links:
                        st.markdown(f"[📄 {_lbl}]({_url})")

            # ── フッター ──────────────────────────────────────
            st.html('<div style="height:16px;"></div>')
            if st.button("ログアウト", use_container_width=True, key="menu_logout"):
                _logout_to_entry(_form_cookie_mgr)
            st.html('<div class="h-footer">ArtéMis HARMONIA</div>')
            return
        # 新規奏者の場合: 名前・パート入力
        st.html('<div class="h-scr-title">Step 1 / 基本情報を入力してください</div>')
        _auth_email = st.session_state.get("form_auth_email", "")

        # ── 種別選択はフォーム外（変更即時反映のため） ──────────
        pm_map_step1: dict = st.session_state.get("form_part_master_map") or {}
        type_opts = sorted({v["type"] for v in pm_map_step1.values() if v["type"]}, key=lambda x: x.lower())
        part_type_sel = st.selectbox(
            "パート種別 *", ["（選択してください）"] + type_opts,
            key="step1_part_type",
        )
        # 種別に応じたパート名リストを事前計算（フォーム内で参照）
        if part_type_sel and part_type_sel != "（選択してください）":
            _filtered_part_opts = sorted(
                [v["name"] for v in pm_map_step1.values()
                 if v["type"] == part_type_sel and v["name"]],
                key=lambda x: x.lower()
            ) + [OTHER_PART]
        else:
            _filtered_part_opts = [OTHER_PART]

        with st.form("step1"):
            col_last, col_first = st.columns(2)
            last_name  = col_last.text_input("姓 *",  placeholder="例：山田")
            first_name = col_first.text_input("名 *", placeholder="例：太郎")
            hn         = st.text_input("H.N.（任意）", placeholder="例：酒席ティンパニ奏者")
            email    = st.text_input("メールアドレス *", value=_auth_email,
                                      disabled=True,
                                      help="認証済みのメールアドレスが自動入力されています。")

            # 練習情報PDF受信設定
            _sample_url = ctx.get("FORM_PRACTICE_SAMPLE_PDF_URL", "")
            _caption_parts = ["練習情報PDFをメールでお送りします。"]
            if _sample_url:
                _caption_parts.append(f"[サンプルPDFを見る]({_sample_url})")
            st.caption(" / ".join(_caption_parts))
            receive_pdf = st.checkbox(
                "練習情報PDFをメールで受け取る",
                value=True,
                help="チェックを入れると、練習・本番の前日に資料PDFがメールで届きます。メールアドレスの登録が必要です。",
            )
            if receive_pdf and not email.strip():
                st.caption("⚠️ メールアドレスを入力するとPDFを受け取れます。")

            phone    = st.text_input("電話番号（任意）", placeholder="例：09012345678")
            line_id  = st.text_input("LINE ID（任意）", placeholder="例：yamada_taro")
            st.divider()
            # パート名選択（種別選択の結果を反映）
            part_sel = st.selectbox("担当パート *", _filtered_part_opts)
            part_other = ""
            if part_sel == OTHER_PART:
                part_other = st.text_input("パートを入力してください")
            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            if not last_name.strip() or not first_name.strip():
                st.error("姓・名はどちらも必須です。")
                return
            name = last_name.strip() + first_name.strip()  # スペースなしで結合
            actual_part = part_other.strip() if part_sel == OTHER_PART else part_sel
            if not actual_part or actual_part == OTHER_PART:
                st.error("パートを選択または入力してください。")
                return

            with st.spinner("確認中..."):
                players = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
                existing = next(
                    (p for p in players
                     if (ext(p, PLAYER_NAME_KEYS) or "").strip() == name),
                    None
                )
                if existing:
                    player_id = existing.get("id", "")
                    # 連絡先の更新（入力があった場合のみ）
                    t_pl = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
                    if t_pl and any([hn.strip(), email.strip(), phone.strip(), line_id.strip(), True]):
                        upd: dict = {}
                        if hn.strip():      ctx["put_prop_any"](upd, t_pl, PLAYER_HN_KEYS,    hn.strip())
                        if email.strip():   ctx["put_prop_any"](upd, t_pl, PLAYER_EMAIL_KEYS, email.strip())
                        if phone.strip():   ctx["put_prop_any"](upd, t_pl, PLAYER_PHONE_KEYS, phone.strip())
                        if line_id.strip(): ctx["put_prop_any"](upd, t_pl, PLAYER_LINE_KEYS,  line_id.strip())
                        ctx["put_prop_any"](upd, t_pl, PLAYER_RECEIVE_KEYS, receive_pdf)
                        ctx["api_request"]("patch",
                            f"https://api.notion.com/v1/pages/{player_id}",
                            json={"properties": upd})
                    st.session_state["form_is_new"] = False
                else:
                    t_pl = ctx["get_prop_types"](ctx["CONCERT_DB_PLAYER"])
                    props = {}
                    ctx["put_prop_any"](props, t_pl, PLAYER_NAME_KEYS, name)
                    if hn.strip():      ctx["put_prop_any"](props, t_pl, PLAYER_HN_KEYS,    hn.strip())
                    if email.strip():   ctx["put_prop_any"](props, t_pl, PLAYER_EMAIL_KEYS, email.strip())
                    if phone.strip():   ctx["put_prop_any"](props, t_pl, PLAYER_PHONE_KEYS, phone.strip())
                    if line_id.strip(): ctx["put_prop_any"](props, t_pl, PLAYER_LINE_KEYS,  line_id.strip())
                    ctx["put_prop_any"](props, t_pl, PLAYER_RECEIVE_KEYS, receive_pdf)
                    res = ctx["api_request"]("post", "https://api.notion.com/v1/pages",
                                             json={"parent": {"database_id": ctx["CONCERT_DB_PLAYER"]},
                                                   "properties": props})
                    player_id = res.json().get("id", "") if res and res.status_code == 200 else ""
                    st.session_state["form_is_new"] = True

            if not player_id:
                st.error("登録に失敗しました。もう一度お試しください。")
                return

            # パート名→PART_MASTERのIDを逆引き
            pm_map: dict = st.session_state.get("form_part_master_map") or {}
            part_master_id = next(
                (pid for pid, v in pm_map.items() if v["name"] == actual_part),
                ""
            )

            st.session_state.update({
                "form_player_id":        player_id,
                "form_player_name":      name,
                "form_player_part":      actual_part,
                "form_player_part_id":   part_master_id,
                "form_att":  {},
                "form_att_comment": {},
                "form_pref": {},
                "form_own":  {},
                "form_step": 2,
            })
            # 新規登録かつメールアドレス入力済みの場合はパスワード設定を促す
            if st.session_state.get("form_is_new") and email.strip():
                st.session_state["form_need_set_password"] = True
                st.session_state["form_is_existing_auth"]  = True
                st.session_state["form_step"] = 1  # パスワード設定画面を先に表示
            st.rerun()

    # ── STEP 2: 出欠 ──────────────────────────────────────────
    elif step == 2:
        pname  = st.session_state.get("form_player_name","")
        part   = st.session_state.get("form_player_part","")
        is_new = st.session_state.get("form_is_new", False)

        st.html('<div class="h-scr-title">Step 2 / 練習出欠を入力してください</div>')
        st.caption(f"👤 {pname}　　パート：{part}")
        if is_new:
            st.success("✅ 新規奏者として登録しました。")
        st.caption("※ 本番当日の出席は自動で登録されます。")

        if not practices:
            st.info("練習日が登録されていません。")
            st.session_state["form_step"] = 3 if partdefs else 5
            if st.button("次へ →", type="primary", use_container_width=True):
                st.rerun()
            return

        existing_att: dict[str, dict] = {}
        current_pid = st.session_state.get("form_player_id", "")
        if current_pid:
            _, existing_att = _get_form_cast_and_att_map(ctx, concert_id, current_pid)
        # 入力途中で戻ってきた場合はsession_stateを最優先し、無ければDB既存値を使う
        session_att = st.session_state.get("form_att") or {}
        session_att_comment = st.session_state.get("form_att_comment") or {}

        with st.form("step2"):
            att: dict[str, str] = {}
            att_comment: dict[str, str] = {}
            for p in practices:
                pr_id    = p.get("id","")
                pr_name  = ext(p, PRACTICE_NAME_KEYS) or pr_id
                pr_date  = ext(p, PRACTICE_DATE_KEYS) or ""
                pr_venue = ext(p, PRACTICE_VENUE_KEYS) or ""
                date_disp = pr_date[:10] if pr_date else "日時未設定"
                time_disp = pr_date[11:16] if len(pr_date) > 10 else ""
                # 曜日を追加
                weekday_jp = ""
                if pr_date and len(pr_date) >= 10:
                    try:
                        from datetime import date as _date
                        _d = _date.fromisoformat(pr_date[:10])
                        weekday_jp = ["月","火","水","木","金","土","日"][_d.weekday()]
                    except Exception:
                        pass
                label = f"**{pr_name}**　{date_disp}{'（' + weekday_jp + '）' if weekday_jp else ''}"
                if time_disp: label += f" {time_disp}"
                if pr_venue:  label += f"　📍 {pr_venue}"
                st.markdown(label)
                cur = existing_att.get(pr_id, {})
                cur_status = session_att.get(pr_id) or cur.get("status", "△")
                idx = ATT_OPTS.index(cur_status) if cur_status in ATT_OPTS else 1
                val = st.radio("　", ATT_OPTS, index=idx, horizontal=True,
                               key=f"att_{pr_id}", label_visibility="collapsed")
                att[pr_id] = val
                att_comment[pr_id] = st.text_input(
                    "コメント（任意）",
                    value=session_att_comment.get(pr_id, cur.get("comment", "")),
                    key=f"att_note_{pr_id}",
                )
                st.divider()
            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            st.session_state["form_att"]  = att
            st.session_state["form_att_comment"] = att_comment
            if st.session_state.get("form_menu_mode"):
                # メニューから来た場合は送信→完了→メニューに戻る
                st.session_state["form_step"] = 6
            else:
                st.session_state["form_step"] = 3 if partdefs else 5
            st.rerun()

    # ── STEP 3: パート希望（パート定義が存在する場合） ──────────
    elif step == 3:
        # partとpart_master_idをここで取得（このスコープでは未定義のため必須）
        part           = st.session_state.get("form_player_part", "")
        part_master_id = st.session_state.get("form_player_part_id", "")

        # 奏者のパートの種別をPART_MASTERから取得
        pm_map: dict = st.session_state.get("form_part_master_map") or {}
        my_part_type = pm_map.get(part_master_id, {}).get("type", "") if part_master_id else ""
        my_is_perc   = IS_PERC(my_part_type or part)  # マスタ種別優先、なければパート名でフォールバック

        # PART_DEFINITIONの「パート区分」RelationからPART_MASTERIDを引き、
        # 奏者のpart_master_idと一致するものだけ表示する。
        # パート区分が未設定の定義はスキップ（安全側）。
        def _partdef_matches(pd) -> bool:
            pd_part_ids = ctx["extract_relation_ids_any"](pd, PARTDEF_PART_REL_KEYS)
            if not pd_part_ids:
                return False
            return part_master_id in pd_part_ids

        visible_partdefs = [pd for pd in partdefs if _partdef_matches(pd)]

        st.html('<div class="h-scr-title">Step 3 / パート希望を入力してください</div>')
        st.caption("希望・NGのあるパートだけ入力してください。入力しないパートは「希望なし/降り番でも可」として扱われます。")

        if not visible_partdefs:
            st.info("パート定義がまだ登録されていません。スキップします。")
            st.session_state["form_pref"] = {}
            st.session_state["form_step"] = 4 if my_is_perc else 5
            st.rerun()
            return

        # 曲ごとにグループ化
        song_name_map = build_song_name_map(ctx, songs)
        from collections import defaultdict
        pd_by_song: dict[str, list] = defaultdict(list)
        for pd in visible_partdefs:
            sids = ctx["extract_relation_ids_any"](pd, PARTDEF_SONG_REL_KEYS)
            sid = sids[0] if sids else "__none__"
            pd_by_song[sid].append(pd)

        # セッションから既存の希望を読み込む
        pref: dict[str, str] = dict(st.session_state.get("form_pref") or {})

        for sid, pds in pd_by_song.items():
            sname = song_name_map.get(sid, "曲目未設定")
            st.markdown(f"**🎵 {sname}**")
            for pd in pds:
                pd_id   = pd.get("id","")
                pd_name = ext(pd, PARTDEF_NAME_KEYS) or pd_id
                cur_val = pref.get(pd_id, "希望なし/降り番でも可")
                col_name, col_sel = st.columns([3, 3])
                col_name.html(f"<div style='padding-top:8px'>{pd_name}</div>")
                new_val = col_sel.selectbox(
                    pd_name, PRIORITY_OPTS,
                    index=PRIORITY_OPTS.index(cur_val) if cur_val in PRIORITY_OPTS else 3,
                    key=f"pref_{pd_id}",
                    label_visibility="collapsed",
                )
                pref[pd_id] = new_val
            st.divider()

        # 入力内容をリアルタイムでセッションに反映
        st.session_state["form_pref"] = pref

        # 入力済みパートのサマリ
        active = {pd_id: v for pd_id, v in pref.items() if v not in ("希望なし/降り番でも可", "")}
        if active:
            pd_name_map = {pd.get("id",""): ext(pd, PARTDEF_NAME_KEYS) or "" for pd in visible_partdefs}
            with st.expander(f"入力済み: {len(active)}パート", expanded=False):
                for pd_id, v in active.items():
                    st.caption(f"{pd_name_map.get(pd_id, pd_id)}：{v}")

        if st.button("次へ →", type="primary", use_container_width=True, key="step3_next"):
            if st.session_state.get("form_menu_mode"):
                st.session_state["form_step"] = 6
            else:
                st.session_state["form_step"] = 4 if my_is_perc else 5
            st.rerun()

    # ── STEP 4: 所有楽器（Percのみ） ─────────────────────────
    elif step == 4:
        st.html('<div class="h-scr-title">Step 4 / 所有楽器の台数を入力してください</div>')
        st.caption("所有していない楽器は 0 のままで構いません。")

        if not req_insts:
            st.info("必要楽器の情報がありません。スキップします。")
            st.session_state["form_own"]  = {}
            st.session_state["form_step"] = 5
            st.rerun()
            return

        with st.form("step4"):
            own: dict[str, int] = {}
            for iid in req_insts:
                iname = inst_map.get(iid, iid)
                val = st.number_input(iname, min_value=0, max_value=10,
                                      step=1, value=0, key=f"own_{iid}")
                own[iid] = int(val)
            submitted = st.form_submit_button("次へ →", type="primary",
                                              use_container_width=True)
        if submitted:
            st.session_state["form_own"]  = own
            if st.session_state.get("form_menu_mode"):
                st.session_state["form_step"] = 6
            else:
                st.session_state["form_step"] = 5
            st.rerun()

    # ── STEP 5: 確認・送信 ───────────────────────────────────
    elif step == 5:
        player_id    = st.session_state.get("form_player_id","")
        player_name  = st.session_state.get("form_player_name","")
        part         = st.session_state.get("form_player_part","")
        att          = st.session_state.get("form_att",  {})
        att_comment  = st.session_state.get("form_att_comment", {})
        pref         = st.session_state.get("form_pref", {})
        own          = st.session_state.get("form_own",  {})
        concert_name = ext(concert, CONCERT_NAME_KEYS) or ""

        st.html('<div class="h-scr-title">Step 5 / 内容を確認して送信してください</div>')
        st.markdown(f"**氏名：** {player_name}　　**パート：** {part}")

        if att:
            with st.expander("出欠", expanded=False):
                prac_map = {p.get("id",""): ext(p, PRACTICE_NAME_KEYS) or "" for p in practices}
                for pr_id, status in att.items():
                    st.write(f"{prac_map.get(pr_id, pr_id)}：**{status}**")
                    cmt = (att_comment.get(pr_id, "") or "").strip()
                    if cmt:
                        st.caption(f"　コメント: {cmt}")
                st.caption("※ 本番当日は自動で○が登録されます。")

        if pref:
            with st.expander("パート希望", expanded=False):
                pd_map = {p.get("id",""): ext(p, PARTDEF_NAME_KEYS) or "" for p in partdefs}
                for pd_id, priority in pref.items():
                    st.write(f"{pd_map.get(pd_id, pd_id)}：**{priority}**")

        owned = {iid: cnt for iid, cnt in own.items() if cnt > 0}
        if owned:
            with st.expander("所有楽器", expanded=False):
                for iid, cnt in owned.items():
                    st.write(f"{inst_map.get(iid, iid)}：{cnt}台")

        col1, col2 = st.columns(2)
        if col1.button("← 修正する", use_container_width=True):
            st.session_state["form_step"] = 2
            st.rerun()

        if col2.button("✅ 送信する", type="primary", use_container_width=True):
            with st.spinner("送信中..."):
                ok_n, errors, debug = _submit_all(
                    ctx, concert_id, concert_name,
                    player_id, player_name, att, att_comment, pref, own
                )
            st.session_state["form_submit_count"] = ok_n
            st.session_state["form_submit_errors"] = errors
            st.session_state["form_submit_debug"]  = debug
            st.session_state["form_step"] = 6
            st.rerun()

    # ── STEP 6: 完了 ─────────────────────────────────────────
    elif step == 6:
        player_name = st.session_state.get("form_player_name","")
        ok_n        = st.session_state.get("form_submit_count", 0)
        errors      = st.session_state.get("form_submit_errors", [])
        debug       = st.session_state.get("form_submit_debug", {})

        if errors:
            st.warning(f"⚠️ 一部の登録に失敗しました。")
            for e in errors:
                st.error(e)
        else:
            if not st.session_state.get("form_balloons_done"):
                st.balloons()
                st.session_state["form_balloons_done"] = True
            st.success(f"✅ 送信完了！ {ok_n}件のデータが登録されました。")
        st.markdown(f"**{player_name}** さん、ありがとうございました。")

        # メニューモードの場合はメニューに戻るボタンを表示
        if st.session_state.get("form_menu_mode"):
            if st.button("← メニューに戻る", use_container_width=True, key="back_to_menu"):
                st.session_state.update({
                    "form_step": 1,
                    "form_notified": False,
                    "form_balloons_done": False,
                    "form_menu_mode": False,
                })
                st.rerun()
        else:
            st.info("このページを閉じて構いません。")

        # 通知用に入力内容を退避（この後 session_state をクリアするため）
        att_dict_snapshot = dict(st.session_state.get("form_att", {}) or {})
        att_comment_snapshot = dict(st.session_state.get("form_att_comment", {}) or {})
        pref_dict_snapshot = dict(st.session_state.get("form_pref", {}) or {})

        # 認証情報・入力データをクリア（完了後は不要）
        for _k in ["form_auth_email", "form_auth_verified", "auth_player_id",
                   "auth_is_existing", "form_att", "form_att_comment", "form_pref", "form_own"]:
            st.session_state.pop(_k, None)

        # 管理者への変更通知（1回だけ送信）
        if not st.session_state.get("form_notified"):
            try:
                from concert.services.mailer import send_text_to_all
                admin_email = st.secrets.get("SMTP_USER", "")
                is_new   = st.session_state.get("form_is_new", False)
                action   = "新規登録" if is_new else "内容更新"
                att_dict = att_dict_snapshot
                pref_dict= pref_dict_snapshot
                lines    = [
                    f"[ArteMis HARMONIA] フォーム{action}通知",
                    "",
                    f"奏者: {player_name}",
                    f"演奏会: {c_name}",
                    f"操作: {action}（{ok_n}件登録）",
                ]
                if att_dict:
                    lines.append("")
                    lines.append("【出欠】")
                    # 練習名をIDから逆引き
                    pr_name_map = {
                        pr.get("id",""): (
                            ext(pr, PRACTICE_DATE_KEYS) or ext(pr, PRACTICE_NAME_KEYS) or "練習"
                        )[:10]
                        for pr in (st.session_state.get("form_practices") or [])
                    }
                    for pr_id, status in att_dict.items():
                        pr_label = pr_name_map.get(pr_id, "練習")
                        lines.append(f"  {pr_label} : {status}")
                        cmt = (att_comment_snapshot.get(pr_id, "") or "").strip()
                        if cmt:
                            lines.append(f"    コメント: {cmt}")
                if pref_dict:
                    lines.append("")
                    lines.append("【パート希望】")
                    pd_name_map = {
                        pd.get("id",""): ext(pd, PARTDEF_NAME_KEYS) or ""
                        for pd in (st.session_state.get("form_partdefs") or [])
                    }
                    for pd_id, priority in pref_dict.items():
                        if priority not in ("未回答", ""):
                            lines.append(f"  {pd_name_map.get(pd_id, pd_id[:8])}: {priority}")

                if admin_email:
                    send_text_to_all(
                        ctx,
                        [{"name": "管理者", "email": admin_email}],
                        subject=f"[HARMONIA] {player_name} さんが{action}しました",
                        body="\n".join(lines),
                    )

                # 同パートLeaderにも通知（出欠/希望の登録がある場合のみ）
                has_member_update = bool(att_dict) or any(v not in ("未回答", "") for v in pref_dict.values())
                if has_member_update:
                    part_master_id = st.session_state.get("form_player_part_id", "")
                    leader_targets = _get_part_leader_recipients(
                        ctx,
                        concert_id=concert_id,
                        part_master_id=part_master_id,
                        exclude_player_id=st.session_state.get("form_player_id", ""),
                    )
                    if leader_targets:
                        part_name = st.session_state.get("form_player_part", "") or "（未設定）"
                        leader_body = "\n".join([
                            "[ArteMis HARMONIA] パートメンバー更新通知",
                            "",
                            f"奏者: {player_name}",
                            f"演奏会: {c_name}",
                            f"対象パート: {part_name}",
                            "",
                            "下記の入力が更新されました。",
                            "",
                            *lines[5:],  # 出欠/希望ブロックを再利用
                        ])
                        send_text_to_all(
                            ctx,
                            leader_targets,
                            subject=f"[HARMONIA] {part_name}メンバーの出欠/希望が更新されました",
                            body=leader_body,
                        )
                st.session_state["form_notified"] = True
            except Exception:
                pass  # 通知失敗はサイレントに（ユーザー体験を壊さない）

        # テスト用デバッグ情報（URLにdebug=1がある場合のみ表示）
        if st.query_params.get("debug") == "1" and debug:
            with st.expander("🔧 デバッグ情報", expanded=False):
                for k, v in debug.items():
                    st.code(f"{k}: {v}")


# ── 管理者：URL生成UI ─────────────────────────────────────────

def _delete_form_test_data(ctx, concert_id: str) -> dict:
    """フォームから送信されたテストデータを一括削除。
    対象：指定演奏会に紐づくATTENDANCE・CONCERT_CAST・PREFERENCE・PLAYER_INSTRUMENT。
    PERFORMERは[TEST]プレフィックスのもののみ削除。
    """
    ext_rel = ctx["extract_relation_ids_any"]
    ext     = ctx["extract_prop_text_any"]
    summary: dict[str, int] = {}

    def archive(page_id: str) -> bool:
        res = ctx["api_request"]("patch", f"https://api.notion.com/v1/pages/{page_id}",
                                 json={"archived": True})
        return res is not None and res.status_code == 200

    # 1. この演奏会のCONCERT_CASTを取得（後続の削除に使用）
    all_cast = ctx["query_all"](ctx["CONCERT_DB_PARTICIPANT"], None)
    cast_rows = [r for r in all_cast
                 if concert_id in ext_rel(r, PARTICIPANT_CONCERT_REL_KEYS)]
    cast_ids = {r.get("id","") for r in cast_rows}

    # 2. ATTENDANCE（cast_idまたはplayer_idでリレーション）
    all_att = ctx["query_all"](ctx["CONCERT_DB_ATTENDANCE"], None)
    all_pr = ctx["query_all"](ctx["CONCERT_DB_PRACTICE"], None)
    pr_concert_map = {p.get("id",""): ext_rel(p, PRACTICE_CONCERT_REL_KEYS) for p in all_pr}
    att_count = 0
    for r in all_att:
        pl_ids = ext_rel(r, ATT_PLAYER_REL_KEYS)
        pr_ids = ext_rel(r, ATT_PRACTICE_REL_KEYS)
        # 練習がこの演奏会に紐づいているか確認
        if any(concert_id in pr_concert_map.get(pr_id, []) for pr_id in pr_ids):
            if archive(r.get("id","")):
                att_count += 1
    if att_count: summary["ATTENDANCE"] = att_count

    # 3. PREFERENCE（player_idで紐づく、この演奏会のパート定義に関連）
    all_songs = ctx["query_all"](ctx["CONCERT_DB_SONG"], None)
    song_ids = {s.get("id","") for s in all_songs
                if concert_id in ext_rel(s, SONG_CONCERT_REL_KEYS)}
    all_pd = ctx["query_all"](ctx["CONCERT_DB_PART_DEFINITION"], None)
    pd_ids = {p.get("id","") for p in all_pd
              if any(sid in ext_rel(p, PARTDEF_SONG_REL_KEYS) for sid in song_ids)}
    all_pref = ctx["query_all"](ctx["CONCERT_DB_PREFERENCE"], None)
    pref_count = 0
    for r in all_pref:
        if any(pd_id in ext_rel(r, PREF_PART_REL_KEYS) for pd_id in pd_ids):
            if archive(r.get("id","")): pref_count += 1
    if pref_count: summary["PREFERENCE"] = pref_count

    # 4. PLAYER_INSTRUMENT（この演奏会に紐づく）
    all_pi = ctx["query_all"](ctx["CONCERT_DB_PLAYER_INSTRUMENT"], None)
    pi_count = 0
    for r in all_pi:
        if concert_id in ext_rel(r, PI_CONCERT_REL_KEYS):
            if archive(r.get("id","")): pi_count += 1
    if pi_count: summary["PLAYER_INSTRUMENT"] = pi_count

    # 5. CONCERT_CAST
    cast_count = sum(1 for r in cast_rows if archive(r.get("id","")))
    if cast_count: summary["CONCERT_CAST"] = cast_count

    # 6. PERFORMER（[TEST]プレフィックスのみ）
    from concert.services.keys import PLAYER_NAME_KEYS as PNK
    all_pl = ctx["query_all"](ctx["CONCERT_DB_PLAYER"], None)
    pl_count = 0
    for r in all_pl:
        if (ext(r, PNK) or "").startswith("[TEST]"):
            if archive(r.get("id","")): pl_count += 1
    if pl_count: summary["PERFORMER[TEST]"] = pl_count

    return summary


def render_url_generator(ctx: dict, concert_id: str, concert_name: str):
    if not concert_id:
        st.caption("演奏会を選択するとURLが生成されます。")
        return
    token    = make_form_token(concert_id)
    base_url = (st.secrets.get("FORM_BASE_URL", "https://artemis-form.streamlit.app") or "").strip().rstrip("/")
    long_url = f"{base_url}/?concert={token}&cid={concert_id}"

    st.code(long_url, language=None)
    if st.button("🔗 TinyURLで短縮", key="shorten_url_btn", use_container_width=True):
        with st.spinner("短縮中..."):
            short = shorten_url(long_url)
        st.session_state["form_short_url"] = short
        st.rerun()

    short = st.session_state.get("form_short_url", "")
    if short:
        if short != long_url:
            st.success("短縮URL：")
            st.code(short, language=None)
            st.caption("テスト用（デバッグ情報表示）：")
            st.code(f"{long_url}&debug=1", language=None)
        else:
            st.warning("短縮に失敗しました。上のURLをそのまま使用してください。")

    st.divider()
    st.caption("🗑️ フォームテストデータ削除")
    st.caption("この演奏会に紐づくATTENDANCE・CONCERT_CAST・PREFERENCE・PLAYER_INSTRUMENTを削除します。PERFORMERは[TEST]プレフィックスのもののみ対象。")
    confirm = st.checkbox("削除対象を確認しました", key="form_test_delete_confirm")
    if st.button("🗑️ フォームテストデータを削除", type="secondary",
                 use_container_width=True, key="form_test_delete_btn",
                 disabled=not confirm):
        with st.spinner("削除中..."):
            summary = _delete_form_test_data(ctx, concert_id)
        if summary:
            st.success("✅ 削除完了")
            for k, v in summary.items():
                st.caption(f"  {k}: {v}件")
        else:
            st.info("削除対象が見つかりませんでした。")
