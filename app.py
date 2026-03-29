import re
import json
import tomllib
from collections.abc import Mapping
import requests
import time
import random
import streamlit as st
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote, unquote, urlparse
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
import io
import uuid
from components.form_inputs import clearable_text_input as _clearable_text_input_component
from services.notion_read import query_notion_database_all as _query_notion_database_all_service
from services.relation_utils import clean_relation_ids as _clean_relation_ids_service
from services.relation_utils import prune_selected_relations as _prune_selected_relations_service
from services.reconcile import analyze_performance_relation_integrity_service as _analyze_reconcile_service
from services.reconcile import run_performance_relation_repair_service as _run_reconcile_repair_service
from services.sync_logs import build_update_log as _build_update_log_service
from services.performance_ops import create_performance_participant_rows_service as _create_participants_service
from services.performance_ops import create_setlist_rows_for_performance_service as _create_setlist_service
from services.performance_ops import create_song_assignment_rows_service as _create_song_assign_service
from services.performance_ops import get_cast_row_map_for_performance_service as _get_cast_row_map_service
from services.performance_ops import upsert_score_master_links_service as _upsert_score_master_links_service
try:
    from concert.services.notion_client import build_concert_ctx
    from concert.pages import (
        concert_mgmt,
        finance,
        test_data,
        songs,
        players,
        rental,
        assign,
    )
    CONCERT_SYSTEM_AVAILABLE = True
    CONCERT_IMPORT_ERROR = ""
except Exception as _concert_import_error:
    CONCERT_SYSTEM_AVAILABLE = False
    CONCERT_IMPORT_ERROR = str(_concert_import_error)

# ============================================================
# 設定（secrets.toml から読み込み）
# ============================================================
NOTION_API_KEY  = st.secrets["NOTION_API_KEY"]
NOTION_DB_ID    = st.secrets["NOTION_DB_ID"]
NOTION_SCORE_DB_ID = st.secrets.get("NOTION_SCORE_DB_ID", st.secrets.get("NOTION_SETLIST_DB_ID", ""))
NOTION_PERFORMER_DB_ID = st.secrets.get("NOTION_PERFORMER_DB_ID", "")
NOTION_PERFORMANCE_CAST_DB_ID = st.secrets.get("NOTION_PERFORMANCE_CAST_DB_ID", st.secrets.get("NOTION_ASSIGNMENT_DB_ID", ""))
NOTION_SONG_ASSIGN_DB_ID = st.secrets.get("NOTION_SONG_ASSIGN_DB_ID", "")
NOTION_PERFORMER_MASTER_DB_ID = st.secrets.get("NOTION_PERFORMER_MASTER_DB_ID", "")
NOTION_COUNTRY_MASTER_DB_ID = st.secrets.get("NOTION_COUNTRY_MASTER_DB_ID", "")
NOTION_WORK_DB_ID = st.secrets.get("NOTION_WORK_DB_ID", "3284532d7d56805885ecdc62403489cf")
NOTION_COMPOSER_DB_ID = st.secrets.get("NOTION_COMPOSER_DB_ID", "3284532d7d5680ab87b4d93899a68033")
NOTION_MOVEMENT_DB_ID = st.secrets.get("NOTION_MOVEMENT_DB_ID", "3284532d7d5680e9bc10fc96fe7bfb99")
NOTION_GAME_JP_DICT_DB_ID = st.secrets.get("NOTION_GAME_JP_DICT_DB_ID", "3234532d7d5680639809cb0d2a5da940")
DEFAULT_PERFORMER_NAME = st.secrets.get("DEFAULT_PERFORMER_NAME", "")
TMDB_API_KEY         = st.secrets["TMDB_API_KEY"]
RAKUTEN_APP_ID = st.secrets.get("RAKUTEN_APP_ID", "")
DRIVE_FOLDER_ID = st.secrets["DRIVE_FOLDER_ID"]
IGDB_CLIENT_ID     = st.secrets.get("IGDB_CLIENT_ID", "")
IGDB_CLIENT_SECRET = st.secrets.get("IGDB_CLIENT_SECRET", "")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

DEFAULT_TIMEOUT = 20
REFRESH_BATCH_SIZE = 20
APP_VERSION = "11.60"
GAME_JP_LEARNED_MAP_PATH = Path("data/game_jp_learned.json")
API_AUDIT_LOG_PATH = Path("logs/api_events.jsonl")
OPERATION_AUDIT_LOG_PATH = Path("logs/operation_events.jsonl")
WIKIMEDIA_HEADERS = {
    "User-Agent": "ArteMisCERS/9.x (metadata resolver; contact: app operator)",
    "Accept": "application/json",
}


def wikimedia_get(url: str, params: dict | None = None, timeout: int = DEFAULT_TIMEOUT):
    return requests.get(url, params=params, timeout=timeout, headers=WIKIMEDIA_HEADERS)


def _truncate_text(value: str, max_len: int = 240) -> str:
    text = str(value or "").replace("\n", " ").strip()
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def append_api_audit_log(event_type: str, payload: dict | None = None):
    """API失敗の切り分け用に、軽量JSONLログを残す。"""
    try:
        API_AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        rec = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "event": event_type,
            "payload": payload or {},
        }
        with API_AUDIT_LOG_PATH.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        # ログ出力失敗で本処理は止めない
        return


def push_runtime_api_error(title: str, detail: str, *, where: str = "", status_code: int | None = None):
    msg = f"{title}: {detail}".strip(": ")
    append_api_audit_log(
        "api_error",
        {
            "where": where,
            "status_code": status_code,
            "message": msg,
        },
    )
    recent = st.session_state.get("runtime_api_errors", [])
    recent.append(
        {
            "time": datetime.now().strftime("%H:%M:%S"),
            "where": where or "unknown",
            "status_code": status_code,
            "message": msg,
        }
    )
    st.session_state["runtime_api_errors"] = recent[-20:]


def append_operation_audit_log(operation: str, stats: dict):
    try:
        OPERATION_AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        rec = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "operation": operation,
            "stats": stats or {},
        }
        with OPERATION_AUDIT_LOG_PATH.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        return
    recent = st.session_state.get("operation_reports", [])
    recent.append(
        {
            "time": datetime.now().strftime("%H:%M:%S"),
            "operation": operation,
            "summary": _truncate_text(json.dumps(stats or {}, ensure_ascii=False), 280),
        }
    )
    st.session_state["operation_reports"] = recent[-10:]

# ============================================================
# 媒体マッピング
# ============================================================
MEDIA_ICON_MAP = {
    "映画":          ("🎬 映画",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/camera-reels.svg"),
    "ドラマ":        ("📺 ドラマ",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/display.svg"),
    "演奏会（鑑賞）": ("🎼 演奏会（鑑賞）", "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/music-note-beamed.svg"),
    "出演":          ("🎻 出演",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/music-note-list.svg"),
    "展示会":        ("🖼️ 展示会",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/exhibition.svg"),
    "ライブ/ショー": ("🎤 ライブ/ショー", "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/mic.svg"),
    "イベント":      ("🎆 イベント",      "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/event.svg"),
    "書籍":          ("📖 書籍",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/book.svg"),
    "漫画":          ("📚 漫画",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/book-manga.svg"),
    "音楽アルバム":  ("🎵 音楽アルバム",  "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/disc.svg"),
    "ゲーム":        ("🎮 ゲーム",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/controller.svg"),
    "演奏曲":        ("🎼 演奏曲",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/music-score.svg"),
    "アニメ":        ("🌟 アニメ",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/anime.svg"),
}

MEDIA_LABEL_ALIASES = {
    "演奏会（出演）": "出演",
}

_custom_icon_secret = st.secrets.get("MEDIA_ICON_CUSTOM_EMOJI_IDS", st.secrets.get("NOTION_MEDIA_ICON_CUSTOM_EMOJI_IDS", {}))
if isinstance(_custom_icon_secret, str):
    try:
        _custom_icon_secret = json.loads(_custom_icon_secret)
    except Exception:
        try:
            # Streamlit Cloudのsecrets UIで inline table 文字列を入れたケースに対応
            _parsed = tomllib.loads(f"_v = {_custom_icon_secret}")
            _custom_icon_secret = _parsed.get("_v", {})
        except Exception:
            _custom_icon_secret = {}
if isinstance(_custom_icon_secret, Mapping):
    _custom_icon_secret = dict(_custom_icon_secret)
if not isinstance(_custom_icon_secret, dict):
    _custom_icon_secret = {}
MEDIA_ICON_CUSTOM_EMOJI_IDS = {
    MEDIA_LABEL_ALIASES.get(str(k).strip(), str(k).strip()): str(v).strip()
    for k, v in _custom_icon_secret.items()
    if str(k).strip() and str(v).strip()
}

RATING_OPTIONS = ["", "★", "★★", "★★★", "★★★★", "★★★★★"]
EXPERIENCE_DATE_PROP_CANDIDATES = ("体験日", "鑑賞日")
EXPERIENCE_SORT_NEW = "体験日（新しい順）"
EXPERIENCE_SORT_OLD = "体験日（古い順）"
LEGACY_SORT_LABEL_MAP = {
    "鑑賞日（新しい順）": EXPERIENCE_SORT_NEW,
    "鑑賞日（古い順）": EXPERIENCE_SORT_OLD,
}


def queue_new_search_from_enter() -> None:
    # 新規登録>検索タブで Enter されたら検索実行フラグを立てる
    st.session_state["_pending_new_search_enter"] = True


def queue_action(flag_key: str) -> None:
    st.session_state[flag_key] = True

def drive_image_url(file_id: str) -> str:
    """Notion/ブラウザで扱いやすいDrive画像URLを返す。"""
    return f"https://drive.google.com/thumbnail?id={file_id}&sz=w2000"

def with_cache_bust(url: str) -> str:
    """同一Drive URL更新時のブラウザキャッシュ残りを回避する。"""
    if not url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}v={int(time.time()*1000)}"

def format_premiere_source_message(source: str) -> str:
    src = (source or "").strip()
    if src == "musicbrainz-work":
        return "MusicBrainz Workの初期日付を利用"
    if src == "musicbrainz-relation":
        return "MusicBrainz premiere relationを利用"
    if src == "musicbrainz-recording":
        return "MusicBrainz recording初出日を利用"
    if src == "wikidata-qid":
        return "Wikidata（作品QID）を利用"
    if src == "wikidata-search":
        return "Wikidata（検索解決）を利用"
    if src == "wikidata-candidate":
        return "Wikidata候補から手動選択"
    if src == "wikidata-candidate-partial":
        return "Wikidata候補は年月日不足（リリース日は手入力）"
    if src == "work-id-empty":
        return "作品IDが空のため未取得"
    if src.startswith("mb-work-"):
        return f"MusicBrainz作品取得エラー（{src.replace('mb-work-', '')}）"
    if src == "exception":
        return "初演情報取得処理で例外が発生"
    return "参照先に初演情報が見つかりませんでした"

def get_media_icon_url(media_label: str) -> str:
    normalized = MEDIA_LABEL_ALIASES.get(media_label, media_label)
    return MEDIA_ICON_MAP.get(normalized, ("", ""))[1]

def get_media_icon_emoji(media_label: str) -> str:
    normalized = MEDIA_LABEL_ALIASES.get(media_label, media_label)
    label = MEDIA_ICON_MAP.get(normalized, ("📁", ""))[0]
    return (label.split(" ", 1)[0] or "📁").strip()

@st.cache_data(ttl=3600)
def fetch_notion_custom_emoji_rows() -> list[dict]:
    """
    workspaceのカスタム絵文字一覧（name/id/url）
    Notion APIに /v1/emojis エンドポイントは無いため、
    /v1/search でページに使われている custom_emoji を収集する。
    """
    rows, _ = fetch_notion_custom_emoji_rows_debug()
    return rows

@st.cache_data(ttl=3600)
def fetch_notion_custom_emoji_name_map() -> dict:
    """workspaceのカスタム絵文字 name -> id を取得"""
    out = {}
    for r in fetch_notion_custom_emoji_rows():
        name = str((r or {}).get("name") or "").strip()
        eid = str((r or {}).get("id") or "").strip()
        if name and eid:
            out[name.lower()] = eid
    return out

def fetch_notion_custom_emoji_rows_debug() -> tuple[list[dict], str]:
    """ページ上で使用されているカスタム絵文字を収集（エラー可視化用）。"""
    try:
        rows_map = {}
        has_more = True
        next_cursor = None
        rounds = 0
        while has_more and rounds < 20:  # 最大2000ページ相当
            rounds += 1
            payload = {
                "page_size": 100,
                "filter": {"property": "object", "value": "page"},
            }
            if next_cursor:
                payload["start_cursor"] = next_cursor
            res = api_request("post", "https://api.notion.com/v1/search", headers=NOTION_HEADERS, json=payload)
            if res is None:
                return [], "API応答なし（ネットワーク/認証/RateLimit）"
            if res.status_code != 200:
                msg = ""
                try:
                    body = res.json() or {}
                    msg = body.get("message", "") or body.get("code", "")
                except Exception:
                    msg = (res.text or "")[:200]
                return [], f"HTTP {res.status_code}: {msg}"
            data = res.json() or {}
            for p in (data.get("results") or []):
                icon = p.get("icon") or {}
                if icon.get("type") != "custom_emoji":
                    continue
                c = icon.get("custom_emoji") or {}
                name = str(c.get("name") or "").strip()
                eid = str(c.get("id") or "").strip()
                url = str(c.get("url") or "").strip()
                if not (name and eid):
                    continue
                rows_map[eid] = {"name": name, "id": eid, "url": url}
            has_more = bool(data.get("has_more"))
            next_cursor = data.get("next_cursor")

        rows = list(rows_map.values())
        rows.sort(key=lambda x: (x.get("name") or "").lower())
        if not rows:
            return [], (
                "custom_emoji が1件も見つかりませんでした。"
                "（/v1/search で取得できるのは、ページアイコンとして実際に使用中のカスタム絵文字のみです）"
            )
        return rows, ""
    except Exception as ex:
        return [], f"例外: {ex}"

def guess_media_icon_custom_ids_from_names(rows: list[dict]) -> dict:
    """絵文字名から媒体への推定マッピングを作る（最初の一致を採用）"""
    hints = {
        "映画": ["camera-reels", "movie", "film"],
        "ドラマ": ["display", "tv", "drama"],
        "演奏会（鑑賞）": ["music-note-beamed", "concert-listen"],
        "出演": ["music-note-list", "performance", "cast"],
        "展示会": ["exhibition", "gallery", "museum"],
        "ライブ/ショー": ["mic", "live", "show"],
        "イベント": ["event", "fireworks"],
        "書籍": ["book"],
        "漫画": ["book-manga", "manga", "comic"],
        "音楽アルバム": ["disc", "album", "music-album"],
        "ゲーム": ["controller", "game"],
        "演奏曲": ["music-score", "score", "sheet"],
        "アニメ": ["anime"],
    }
    out = {}
    lower_rows = [{"name": (r.get("name") or "").lower(), "id": r.get("id", "")} for r in (rows or [])]
    for media, keys in hints.items():
        for r in lower_rows:
            n = r["name"]
            if any(k in n for k in keys):
                out[media] = r["id"]
                break
    return out

def resolve_media_icon_payload(
    media_label: str,
    allow_external_fallback: bool = True,
    allow_emoji_fallback: bool = True,
) -> tuple[dict | None, dict]:
    """媒体アイコンpayloadを解決し、解決メタ情報も返す。"""
    media_label_s = str(media_label or "").strip()
    normalized = MEDIA_LABEL_ALIASES.get(media_label_s, media_label_s)
    if not normalized:
        if allow_emoji_fallback:
            return {"type": "emoji", "emoji": "📁"}, {"normalized": normalized, "source": "fallback-emoji-default"}
        return None, {"normalized": normalized, "error": "empty-media-label"}
    explicit_id = MEDIA_ICON_CUSTOM_EMOJI_IDS.get(normalized, "")
    if explicit_id:
        return {"type": "custom_emoji", "custom_emoji": {"id": explicit_id}}, {
            "normalized": normalized,
            "source": "custom-emoji-secret",
            "custom_emoji_id": explicit_id,
        }

    # secrets未設定でも、name一致なら自動解決
    emoji_map = fetch_notion_custom_emoji_name_map()
    if emoji_map:
        icon_url = get_media_icon_url(normalized)
        basename = ""
        if icon_url:
            basename = Path(urlparse(icon_url).path).stem.lower()
        candidates = [
            normalized.lower(),
            normalized.replace("/", "").replace("（", "").replace("）", "").lower(),
            basename,
        ]
        for c in candidates:
            if c and c in emoji_map:
                return {"type": "custom_emoji", "custom_emoji": {"id": emoji_map[c]}}, {
                    "normalized": normalized,
                    "source": f"custom-emoji-name:{c}",
                    "custom_emoji_id": emoji_map[c],
                }

    if allow_external_fallback:
        icon_url = get_media_icon_url(normalized)
        if icon_url:
            return {"type": "external", "external": {"url": icon_url}}, {
                "normalized": normalized,
                "source": "fallback-external",
                "external_url": icon_url,
                "error": "custom-emoji-unresolved",
            }
    if allow_emoji_fallback:
        return {"type": "emoji", "emoji": get_media_icon_emoji(normalized)}, {
            "normalized": normalized,
            "source": "fallback-emoji",
            "error": "custom-emoji-unresolved",
        }
    return None, {"normalized": normalized, "error": "custom-emoji-unresolved"}

def get_media_icon_payload(media_label: str) -> dict:
    """媒体アイコンのpayload。カスタム絵文字IDがあれば優先、なければ外部URL。"""
    payload, _ = resolve_media_icon_payload(media_label)
    if payload is None:
        return {"type": "emoji", "emoji": "📁"}
    return payload

def icon_semantically_matches(current_icon: dict | None, target_icon: dict | None) -> bool:
    """Notionのicon比較を意味ベースで行う（余分な name/url 差分を無視）。"""
    c = current_icon or {}
    t = target_icon or {}
    ct = c.get("type")
    tt = t.get("type")
    if ct != tt:
        return False
    if tt == "custom_emoji":
        cid = str(((c.get("custom_emoji") or {}).get("id") or "")).strip()
        tid = str(((t.get("custom_emoji") or {}).get("id") or "")).strip()
        return bool(cid and tid and cid == tid)
    if tt == "emoji":
        return str(c.get("emoji") or "") == str(t.get("emoji") or "")
    if tt == "external":
        curl = str(((c.get("external") or {}).get("url") or "")).strip()
        turl = str(((t.get("external") or {}).get("url") or "")).strip()
        return bool(curl and turl and curl == turl)
    return c == t

def diagnose_media_icon_payloads() -> list[dict]:
    rows = []
    for media in MEDIA_ICON_MAP.keys():
        payload = get_media_icon_payload(media)
        rows.append({
            "媒体": media,
            "payload_type": payload.get("type", ""),
            "custom_emoji_id": ((payload.get("custom_emoji") or {}).get("id") if payload.get("type") == "custom_emoji" else ""),
            "external_url": ((payload.get("external") or {}).get("url") if payload.get("type") == "external" else ""),
            "emoji": (payload.get("emoji") if payload.get("type") == "emoji" else ""),
        })
    return rows

def detect_media_icon_custom_emoji_ids_from_parent_db() -> dict:
    """親DBの既存ページから 媒体 -> custom emoji id を抽出する。"""
    out = {}
    pages = query_notion_database_all(NOTION_DB_ID) or []
    for p in pages:
        media = get_page_media(p)
        if not media:
            continue
        icon = p.get("icon") or {}
        if icon.get("type") != "custom_emoji":
            continue
        eid = str((icon.get("custom_emoji") or {}).get("id") or "").strip()
        if not eid:
            continue
        out[media] = eid
    return out

def is_media_icon_url(url: str | None) -> bool:
    if not url:
        return False
    icon_urls = {v[1] for v in MEDIA_ICON_MAP.values() if len(v) > 1 and v[1]}
    return url in icon_urls

def country_code_to_flag(code: str) -> str:
    c = (code or "").strip().upper()
    # Notionで扱える現行ISO2へ正規化
    c = normalize_country_code_for_flag(c)
    if len(c) != 2 or not c.isalpha():
        return ""
    base = ord("🇦") - ord("A")
    return chr(ord(c[0]) + base) + chr(ord(c[1]) + base)

def normalize_country_code_for_flag(code: str) -> str:
    c = (code or "").strip().upper()
    if not c:
        return ""
    # 互換コード
    if c == "UK":
        return "GB"
    # 廃止/非推奨コードは誤国旗の原因になるため、ここでは空扱いにして再解決へ回す
    deprecated = {"SU", "DD", "YU", "CS", "TP", "AN", "ZR"}
    if c in deprecated:
        return ""
    return c if re.fullmatch(r"[A-Z]{2}", c) else ""

ASSET_BASE_URL = "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets"

def get_asset_path_or_url(filename: str) -> str:
    local_path = Path(__file__).parent / "assets" / filename
    if local_path.exists():
        return str(local_path)
    return f"{ASSET_BASE_URL}/{filename}"

def load_user_guide_markdown() -> str:
    guide_path = Path(__file__).parent / "docs" / "USER_GUIDE.md"
    if not guide_path.exists():
        return ""
    try:
        return guide_path.read_text(encoding="utf-8")
    except Exception:
        return ""

def format_cover_url(url: str, max_len: int = 90) -> str:
    if not url:
        return ""
    base = url.split("?", 1)[0]
    if len(base) <= max_len:
        return base
    return base[:60] + "…" + base[-20:]

def emit_scroll_top_script():
    st.components.v1.html(
        """
        <script>
        function _scrollTopSafe(w) {
          try { w.scrollTo({ top: 0, left: 0, behavior: "instant" }); }
          catch (e1) {
            try { w.scrollTo(0, 0); } catch (e2) {}
          }
          try { if (w.document && w.document.documentElement) w.document.documentElement.scrollTop = 0; } catch (e3) {}
          try { if (w.document && w.document.body) w.document.body.scrollTop = 0; } catch (e4) {}
        }
        function _doScrollTop() {
          _scrollTopSafe(window);
          try {
            _scrollTopSafe(window.parent);
            const d = window.parent.document;
            const sels = [
              '[data-testid="stAppViewContainer"]',
              '[data-testid="stMain"]',
              '.stMain',
              'section.main',
              '.main'
            ];
            sels.forEach((s) => {
              d.querySelectorAll(s).forEach((el) => {
                try { el.scrollTop = 0; } catch (e1) {}
              });
            });
          } catch (e) {}
        }
        setTimeout(_doScrollTop, 0);
        setTimeout(_doScrollTop, 120);
        setTimeout(_doScrollTop, 400);
        </script>
        """,
        height=0,
    )

# ============================================================
# 登録完了後UI（共通）
# ============================================================
def show_post_register_ui():
    """登録完了後UIの共通コンポーネント"""
    st.success("✅ 登録完了！")
    if st.button("🔄 新しく登録する", type="primary", key="post_reg_reset"):
        st.session_state.clear()
        st.rerun()

def reset_new_register_state():
    """新規登録フォームの入力値をクリアする（媒体を跨いで残る値を防止）"""
    keys = [
        "reg_media",
        "inp_jp_main", "inp_en_main", "inp_creator_main", "inp_cast_main",
        "inp_jp_manga", "inp_creator_manga",
        "inp_jp_album", "inp_creator_album",
        "inp_jp_game",
        "inp_jp_anime",
        "final_jp", "final_en", "final_isbn",
        "confirm_date", "confirm_rating", "confirm_wl",
        "ev_title", "ev_creator", "ev_cast", "ev_genre",
        "ev_start", "ev_end", "ev_watch", "ev_watch2", "ev_rating", "ev_wl",
        "new_search_results", "new_search_done", "new_search_excluded",
        "new_search_raw_count",
        "last_game_query_jp",
        "bulk_checked", "confirm_reg", "reg_cart",
        "album_tracks_cache", "album_tracks_id",
        # location_search_ui
        "confirm_loc_query", "confirm_loc_results", "confirm_loc_selected",
        "event_loc_query", "event_loc_results", "event_loc_selected",
        # 演奏会（出演）- 演奏曲関連
        "ev_score_query", "ev_score_selected",
        "ev_participants", "ev_part_name", "ev_part_instruments", "ev_part_memo",
        # 演奏曲 - 演奏会（出演）関連
        "score_perf_query", "score_perf_selected",
        "score_perf_selected_ids",
        "game_work_selected",
        "game_series_suggestions",
        "game_series_pick_fallback",
    ]
    for k in keys:
        st.session_state.pop(k, None)
        st.session_state.pop(f"_cti_{k}", None)

def reset_score_search_state(clear_cache: bool = False):
    """演奏曲検索まわりの状態を明示的に初期化する。"""
    keys = [
        "mb_composer_query",
        "mb_work_title_filter",
        "mb_composers",
        "mb_works",
        "mb_checked",
        "mb_selected_comp",
        "mb_title_filter",
        "mb_portrait_url",
        "mb_portrait_comp",
        "mb_composer_submit",
        "mb_work_submit",
        "mb_comp_radio",
    ]
    for k in keys:
        st.session_state.pop(k, None)
        st.session_state.pop(f"_cti_{k}", None)
    if clear_cache:
        try:
            search_mb_works.clear()
        except Exception:
            pass

def upsert_page_in_state(page: dict):
    if not page or "id" not in page:
        return
    pid = page["id"]
    for key in ["pages", "all_pages"]:
        if key not in st.session_state:
            continue
        pages = st.session_state.get(key, [])
        found = False
        for i, p in enumerate(pages):
            if p.get("id") == pid:
                pages[i] = page
                found = True
                break
        if not found:
            pages.append(page)
        st.session_state[key] = pages

def sync_notion_after_update(page_id: str | None = None, updated_page: dict | None = None):
    """更新後のデータ同期（手動/自動/半自動）"""
    mode = st.session_state.get("auto_reload_mode", "manual")
    if mode == "manual":
        st.session_state.created_pages = []
        return
    if mode == "full":
        with st.spinner("Notionデータ再取得中..."):
            all_pages = load_notion_data()
            if st.session_state.get("last_notion_load_ok", True):
                st.session_state.all_pages      = all_pages
                st.session_state.pages          = filter_target_pages(all_pages)
                st.session_state.search_results = {}
                st.session_state.manual_page    = 0
            else:
                st.warning("Notion再取得に失敗しました。手動で再試行してください。")
        st.session_state.created_pages = []
        return
    # partial
    if updated_page:
        upsert_page_in_state(updated_page)
        return
    if page_id:
        res = api_request("get", f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS)
        if res is not None and res.status_code == 200:
            upsert_page_in_state(res.json())
        else:
            st.warning("該当ページの再取得に失敗しました。")

# ============================================================
# Google Drive API クライアント
# ============================================================
@st.cache_resource
def get_drive_service():
    creds = Credentials(
        token=None,
        refresh_token=st.secrets["GOOGLE_REFRESH_TOKEN"],
        client_id=st.secrets["GOOGLE_CLIENT_ID"],
        client_secret=st.secrets["GOOGLE_CLIENT_SECRET"],
        token_uri="https://oauth2.googleapis.com/token",
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds, cache_discovery=False)

def get_drive_service_safe():
    """Drive接続の安全ラッパー（短時間の連続失敗はクールダウン）"""
    now = time.time()
    blocked_until = st.session_state.get("drive_blocked_until", 0)
    if blocked_until and now < blocked_until:
        return None
    try:
        return get_drive_service()
    except Exception as e:
        get_drive_service.clear()
        try:
            return get_drive_service()
        except Exception as e2:
            st.session_state["drive_blocked_until"] = now + 45
            last_err = st.session_state.get("drive_last_error", "")
            msg = f"Google Drive 接続エラー: {e2 or e}"
            if msg != last_err:
                st.warning(msg)
                st.session_state["drive_last_error"] = msg
            return None


def is_drive_skip_mode() -> bool:
    return bool(st.session_state.get("drive_skip_mode", False))

# ============================================================
# ユーティリティ
# ============================================================

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", name)

def make_noid_filename(title: str, page_id: str) -> str:
    return f"{sanitize_filename(title)}_noid_{page_id}.jpg"

def has_any_id(props) -> bool:
    if props.get("TMDB_ID", {}).get("number"):
        return True
    if props.get("AniList_ID", {}).get("number"):
        return True
    if props.get("IGDB_ID", {}).get("number"):
        return True
    if props.get("iTunes_ID", {}).get("number"):
        return True
    isbn_val = plain_text_join((props.get("ISBN") or {}).get("rich_text", []))
    if isbn_val.strip():
        return True
    return False

def fetch_image_bytes(cover_url: str) -> tuple[bytes | None, str | None]:
    if not cover_url:
        return None, None
    img_url = cover_url
    if "image.tmdb.org/t/p/" in cover_url and "w600_and_h900_bestv2" in cover_url:
        img_url = cover_url.replace("w600_and_h900_bestv2", "original")
    img_res = api_request("get", img_url)
    if img_res is None or img_res.status_code != 200:
        return None, None
    mimetype = img_res.headers.get("Content-Type", "image/jpeg").split(";")[0]
    if not mimetype.startswith("image/"):
        mimetype = "image/jpeg"
    return img_res.content, mimetype

def _rank_portrait_candidate_url(url: str) -> int:
    u = (url or "").lower()
    score = 0
    plus_keywords = ["portrait", "head", "bust", "composer", "photo", "photograph"]
    minus_keywords = ["grave", "memorial", "plaque", "window", "church", "house", "statue", "tomb", "cemetery"]
    for k in plus_keywords:
        if k in u:
            score += 8
    for k in minus_keywords:
        if k in u:
            score -= 10
    if "commons.wikimedia.org/wiki/special:filepath/" in u:
        score += 2
    if "upload.wikimedia.org" in u:
        score += 1
    return score

def save_bytes_to_drive(filename: str, image_bytes: bytes, mimetype: str, make_public: bool = False) -> str | None:
    service = get_drive_service_safe()
    if service is None:
        return None
    files = get_drive_files()
    media = MediaIoBaseUpload(io.BytesIO(image_bytes), mimetype=mimetype, resumable=False)
    file_id = None
    cached_id = files.get(filename)
    escaped_name = filename.replace("'", "\\'")
    try:
        # 同名重複がある場合は最新を優先し、古い重複は削除して今後の取り違えを防ぐ
        listed = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and name='{escaped_name}' and trashed=false",
            fields="files(id,name,modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=20,
        ).execute().get("files", [])
        if listed:
            cached_id = listed[0].get("id") or cached_id
            files[filename] = cached_id
            for dup in listed[1:]:
                dup_id = dup.get("id")
                if dup_id:
                    try:
                        service.files().delete(fileId=dup_id).execute()
                    except Exception:
                        pass
    except Exception:
        pass

    if cached_id:
        try:
            service.files().update(fileId=cached_id, media_body=media).execute()
            file_id = cached_id
        except HttpError as e:
            status = getattr(getattr(e, "resp", None), "status", None)
            if status == 404:
                cache = st.session_state.get("drive_files_cache")
                if isinstance(cache, dict):
                    cache.pop(filename, None)
                file_id = None
            else:
                # 一時通信エラー時に新規作成へフォールバックすると重複が増えるため中断
                return None
        except Exception:
            return None
    if file_id is None:
        try:
            result = service.files().create(
                body={"name": filename, "parents": [DRIVE_FOLDER_ID]},
                media_body=media,
                fields="id",
            ).execute()
            file_id = result["id"]
            cache = st.session_state.get("drive_files_cache")
            if not isinstance(cache, dict):
                cache = {}
                st.session_state["drive_files_cache"] = cache
            cache[filename] = file_id
        except Exception:
            return None
    if make_public:
        try:
            service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
        except Exception:
            pass
    return file_id

def save_cover_to_drive_noid(cover_url: str, title: str, page_id: str) -> str | None:
    if not cover_url or cover_url.startswith("https://drive.google.com"):
        return None
    image_bytes, mimetype = fetch_image_bytes(cover_url)
    if not image_bytes or not mimetype:
        return None
    filename = make_noid_filename(title, page_id)
    return save_bytes_to_drive(filename, image_bytes, mimetype, make_public=True)

def clearable_text_input(
    label: str,
    key: str,
    placeholder: str = "",
    value: str = "",
    container=None,
    refresh_on_value_change: bool = False,
    **kwargs
) -> str:
    return _clearable_text_input_component(
        label=label,
        key=key,
        placeholder=placeholder,
        value=value,
        container=container,
        refresh_on_value_change=refresh_on_value_change,
        **kwargs,
    )

def clean_author(name: str) -> str:
    """著者名クリーニング: 接尾語除去 + スペース正規化（半角スペース1個に統一）"""
    name = re.sub(r'[（(][^）)]*[）)]', '', name)           # 括弧内除去
    name = re.sub(r'\s*(著|訳|編|著者|監修|イラスト)$', '', name)  # 接尾語除去
    name = re.sub(r'[\s\u3000]+', ' ', name).strip()        # 全角スペース含む連続スペースを半角1個に
    return name

def clean_author_list(authors: list) -> str:
    """著者リストをクリーニングして ' / ' 結合"""
    return " / ".join(clean_author(a) for a in authors if a.strip())

def plain_text_join(items) -> str:
    """Notion rich_text/title 配列を安全に文字列化する。"""
    vals = []
    for t in (items or []):
        if not isinstance(t, dict):
            continue
        pt = t.get("plain_text")
        if pt is None:
            pt = ((t.get("text") or {}).get("content") or "")
        vals.append(pt)
    return "".join(vals)

def get_experience_date_property_name(type_map: dict | None = None, database_id: str = NOTION_DB_ID) -> str:
    """体験日/鑑賞日のどちらを使うかをDB定義から解決する。"""
    if type_map is None:
        type_map = get_notion_db_property_types(database_id)
    for name in EXPERIENCE_DATE_PROP_CANDIDATES:
        if name in (type_map or {}):
            return name
    return EXPERIENCE_DATE_PROP_CANDIDATES[0]

def get_experience_date_from_props(props: dict | None) -> str:
    p = props or {}
    for name in EXPERIENCE_DATE_PROP_CANDIDATES:
        val = ((p.get(name) or {}).get("date") or {}).get("start", "") or ""
        if val:
            return val
    return ""

def make_filename(title: str, tmdb_id) -> str:
    return f"{sanitize_filename(title)}_{tmdb_id}.jpg"

def get_title(props):
    def _texts(items):
        vals = []
        for t in (items or []):
            if not isinstance(t, dict):
                continue
            if t.get("plain_text") is not None:
                vals.append(t.get("plain_text", ""))
                continue
            vals.append(((t.get("text") or {}).get("content") or ""))
        return "".join(vals)
    jp = _texts((props.get("タイトル") or {}).get("title", []))
    en = _texts((props.get("International Title") or {}).get("rich_text", []))
    return (jp if jp else en), jp, en

def get_season_number(props) -> int | None:
    en = "".join(
        (t.get("plain_text") if isinstance(t, dict) and t.get("plain_text") is not None else ((t.get("text") or {}).get("content") if isinstance(t, dict) else ""))
        or ""
        for t in (props.get("International Title") or {}).get("rich_text", [])
    )
    m = re.search(r'[Ss]eason\s*(\d+)', en)
    return int(m.group(1)) if m else None

def get_current_notion_url(item) -> str | None:
    def _safe_url(v) -> str | None:
        if not isinstance(v, str):
            return None
        u = v.strip()
        if not u:
            return None
        if not (u.startswith("http://") or u.startswith("https://")):
            return None
        return u

    cover = item.get("cover")
    if cover and cover.get("type") == "external":
        return _safe_url((cover.get("external") or {}).get("url"))
    if cover and cover.get("type") == "file":
        return _safe_url((cover.get("file") or {}).get("url"))
    return None

def is_unreleased(page) -> bool:
    date_prop = page["properties"].get("リリース日", {}).get("date")
    if not date_prop:
        return True
    release_str = date_prop.get("start", "")
    if not release_str:
        return True
    try:
        return date.fromisoformat(release_str[:10]) > date.today()
    except ValueError:
        return False

def is_incomplete(page) -> bool:
    """媒体別に欠損チェック（自動補填対象かどうか）"""
    props  = page["properties"]
    media  = get_page_media(page)
    if is_unreleased(page): return False
    if not page.get("cover"): return True
    if media in ("映画", "ドラマ"):
        if not props.get("TMDB_ID", {}).get("number"):        return True
        if not props.get("ジャンル", {}).get("multi_select"): return True
        if not props.get("キャスト・関係者", {}).get("rich_text"): return True
        if not props.get("クリエイター", {}).get("rich_text"):    return True
        if props.get("TMDB_score", {}).get("number") is None: return True
    elif media == "アニメ":
        if not props.get("AniList_ID", {}).get("number"):     return True
    elif media in ("書籍", "漫画"):
        if not props.get("ISBN", {}).get("rich_text"):        return True
        if not props.get("クリエイター", {}).get("rich_text"):    return True
    elif media == "音楽アルバム":
        if not props.get("クリエイター", {}).get("rich_text"):    return True
    elif media == "ゲーム":
        if not props.get("IGDB_ID", {}).get("number"):        return True
    return False

# ============================================================
# Drive ファイル一覧（session_stateで管理）
# ============================================================

def get_drive_files() -> dict:
    if is_drive_skip_mode():
        if "drive_files_cache" not in st.session_state or not isinstance(st.session_state.get("drive_files_cache"), dict):
            st.session_state["drive_files_cache"] = {}
        return st.session_state.get("drive_files_cache", {})
    if "drive_files_cache" not in st.session_state or not isinstance(st.session_state.get("drive_files_cache"), dict):
        refresh_drive_files()
    return st.session_state.get("drive_files_cache", {})

def refresh_drive_files():
    if is_drive_skip_mode():
        return
    service = get_drive_service_safe()
    if service is None:
        if "drive_files_cache" not in st.session_state or not isinstance(st.session_state.get("drive_files_cache"), dict):
            st.session_state["drive_files_cache"] = {}
        return
    try:
        results = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
            fields="files(id, name, modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=1000,
        ).execute()
        files = {}
        for f in (results.get("files", []) or []):
            nm = (f.get("name") or "").strip()
            fid = (f.get("id") or "").strip()
            if not nm or not fid or nm in files:
                continue
            # modifiedTime descで取得しているため、同名重複時は最初の1件(最新)を採用
            files[nm] = fid
        st.session_state["drive_files_cache"] = files
        st.session_state["drive_blocked_until"] = 0
    except Exception as e:
        st.warning(f"Drive一覧取得失敗: {e}")
        st.session_state["drive_blocked_until"] = time.time() + 60
        if "drive_files_cache" not in st.session_state or not isinstance(st.session_state.get("drive_files_cache"), dict):
            st.session_state["drive_files_cache"] = {}

def _get_title_prop_name(database_id: str) -> str:
    type_map = get_notion_db_property_types(database_id) or {}
    for k, t in type_map.items():
        if t == "title":
            return k
    return "タイトル"

def run_production_api_selftest(enable_write: bool = False) -> dict:
    """
    本番環境向けの軽量セルフテスト。
    - 読み取り: Notion DB GET / query, Drive list
    - 書き込み(任意): Notionにテストページを1件作成して即アーカイブ
    """
    report = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "notion_db_get": "",
        "notion_query": "",
        "drive_list": "",
        "write_create": "SKIP",
        "write_archive": "SKIP",
        "write_page_id": "",
        "error": "",
    }
    try:
        db_res = api_request("get", f"https://api.notion.com/v1/databases/{NOTION_DB_ID}", headers=NOTION_HEADERS)
        report["notion_db_get"] = f"HTTP {db_res.status_code}" if db_res is not None else "No response"
        if db_res is None or db_res.status_code != 200:
            return report
        q_res = api_request(
            "post",
            f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query",
            headers=NOTION_HEADERS,
            json={"page_size": 1},
        )
        report["notion_query"] = f"HTTP {q_res.status_code}" if q_res is not None else "No response"
        if is_drive_skip_mode():
            report["drive_list"] = "SKIP (drive_skip_mode=ON)"
        else:
            svc = get_drive_service_safe()
            if svc is None:
                report["drive_list"] = "NG (service unavailable)"
            else:
                ls = svc.files().list(
                    q=f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
                    fields="files(id,name)",
                    pageSize=1,
                ).execute()
                report["drive_list"] = f"OK ({len(ls.get('files', []))} sample)"
        if not enable_write:
            return report
        tprop = _get_title_prop_name(NOTION_DB_ID)
        tmap = get_notion_db_property_types(NOTION_DB_ID) or {}
        props = {}
        _put_notion_prop(
            props,
            tmap,
            tprop,
            f"SELFTEST {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        )
        create_res = api_request(
            "post",
            "https://api.notion.com/v1/pages",
            headers=NOTION_HEADERS,
            json={"parent": {"database_id": NOTION_DB_ID}, "properties": props},
        )
        report["write_create"] = f"HTTP {create_res.status_code}" if create_res is not None else "No response"
        if create_res is None or create_res.status_code != 200:
            return report
        page_id = (create_res.json() or {}).get("id", "")
        report["write_page_id"] = page_id
        arc_res = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=NOTION_HEADERS,
            json={"archived": True},
        )
        report["write_archive"] = f"HTTP {arc_res.status_code}" if arc_res is not None else "No response"
        return report
    except Exception as e:
        report["error"] = str(e)
        return report

def drive_exists(title: str, tmdb_id) -> bool:
    if is_drive_skip_mode():
        return True
    return make_filename(title, tmdb_id) in get_drive_files()

def drive_exists_fuzzy(title: str) -> bool:
    if is_drive_skip_mode():
        return True
    prefix = sanitize_filename(title) + "_"
    return any(name.startswith(prefix) and name.endswith(".jpg") for name in get_drive_files())

def save_to_drive(cover_url: str, title: str, tmdb_id, image_bytes: bytes | None = None, mimetype: str = "image/jpeg") -> str | None:
    """Drive保存成功時はfile_idを返す、失敗時はNone"""
    if is_drive_skip_mode():
        return "SKIPPED"
    try:
        if image_bytes is None:
            if not cover_url:
                return None
            image_bytes, fetched_mime = fetch_image_bytes(cover_url)
            if image_bytes is None:
                return None
            mimetype = fetched_mime or "image/jpeg"
        fname = make_filename(title, tmdb_id)
        return save_bytes_to_drive(fname, image_bytes, mimetype, make_public=True)
    except Exception as e:
        st.warning(f"Drive保存失敗 ({title}): {e}")
        return None

def get_drive_public_url(title: str, tmdb_id) -> str | None:
    """Drive上のファイルIDから公開URLを返す"""
    if is_drive_skip_mode():
        return None
    try:
        fname = make_filename(title, tmdb_id)
        files = get_drive_files()
        if fname not in files:
            return None
        file_id = files[fname]
        service = get_drive_service_safe()
        if service is None:
            return None
        service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
        return drive_image_url(file_id)
    except Exception:
        return None

def delete_from_drive(title: str, tmdb_id) -> bool:
    if is_drive_skip_mode():
        return True
    try:
        fname = make_filename(title, tmdb_id)
        files = get_drive_files()
        if fname not in files:
            return True
        service = get_drive_service_safe()
        if service is None:
            return False
        service.files().delete(fileId=files[fname]).execute()
        cache = st.session_state.get("drive_files_cache")
        if isinstance(cache, dict) and fname in cache:
            del cache[fname]
        return True
    except Exception as e:
        st.warning(f"Drive削除失敗 ({title} / {tmdb_id}): {e}")
        return False

# ============================================================
# 差分判定
# ============================================================

def get_diff_status(item) -> tuple:
    log_title, _, _ = get_title(item["properties"])
    notion_ok = bool(item.get("cover"))
    if is_drive_skip_mode():
        drive_ok = True
    else:
        tmdb_id = st.session_state.get("tmdb_id_cache", {}).get(item["id"])
        drive_ok = drive_exists(log_title, tmdb_id) if tmdb_id else drive_exists_fuzzy(log_title)
    return notion_ok, drive_ok

def apply_diff_filter(pages: list, diff_filter: str) -> list:
    if diff_filter == "フィルタなし":
        return pages
    result = []
    for item in pages:
        notion_ok, drive_ok = get_diff_status(item)
        if diff_filter == "Notionのみ更新（Driveあり・Notionカバーなし）":
            if drive_ok and not notion_ok: result.append(item)
        elif diff_filter == "Driveのみ更新（Notionカバーあり・Driveなし）":
            if notion_ok and not drive_ok: result.append(item)
        elif diff_filter == "どちらも更新（両方なし）":
            if not notion_ok and not drive_ok: result.append(item)
    return result

def diff_badge(item) -> str:
    notion_ok, drive_ok = get_diff_status(item)
    if is_drive_skip_mode():
        badge = ("🟢" if notion_ok else "🔴") + " Notion ⏭ Drive"
    else:
        badge = ("🟢" if notion_ok else "🔴") + " Notion " + ("🟢" if drive_ok else "🔴") + " Drive"
    if is_unreleased(item):
        badge += " 🔜未公開"
    return badge

# ============================================================
# APIリトライラッパー
# ============================================================

def api_request(method: str, url: str, max_retries: int = 3, **kwargs):
    method_l = method.lower()
    fn = {"get": requests.get, "post": requests.post, "patch": requests.patch, "delete": requests.delete}.get(method_l)
    if fn is None:
        raise ValueError(f"Unsupported method: {method}")
    if "timeout" not in kwargs:
        kwargs["timeout"] = DEFAULT_TIMEOUT
    last_exc = None
    connection_like_error = False
    for attempt in range(max_retries):
        # 短いジッターで瞬間的な同時アクセスを分散
        time.sleep(random.uniform(0.05, 0.25))
        try:
            res = fn(url, **kwargs)
            if res.status_code == 429:
                push_runtime_api_error(
                    "APIレート制限",
                    f"{method_l.upper()} {url}",
                    where="api_request",
                    status_code=429,
                )
                retry_after = res.headers.get("Retry-After", 5)
                try:
                    retry_after = int(retry_after)
                except Exception:
                    retry_after = 5
                time.sleep(retry_after + random.uniform(0.1, 0.6))
                continue
            if res.status_code >= 500:
                push_runtime_api_error(
                    "APIサーバーエラー",
                    f"{method_l.upper()} {url}",
                    where="api_request",
                    status_code=res.status_code,
                )
                time.sleep((2 ** attempt) + random.uniform(0.1, 0.6))
                continue
            return res
        except requests.exceptions.RequestException as e:
            last_exc = e
            msg = str(e).lower()
            if (
                "broken pipe" in msg
                or "errno 32" in msg
                or "connection reset" in msg
                or "connection aborted" in msg
                or "connection refused" in msg
                or "10060" in msg
                or "timed out" in msg
            ):
                connection_like_error = True
            time.sleep((2 ** attempt) + random.uniform(0.1, 0.6))
    if connection_like_error:
        st.session_state["api_connection_error_hint"] = (
            "⚠️ ネットワーク接続が不安定なため通信が切断されました（Broken pipe 等）。"
            " ページを再読み込みして再実行してください。"
        )
        push_runtime_api_error(
            "ネットワーク通信エラー",
            f"{method_l.upper()} {url}: {last_exc}",
            where="api_request",
        )
    elif last_exc is not None:
        st.session_state["api_connection_error_hint"] = f"⚠️ API通信エラー: {last_exc}"
        push_runtime_api_error(
            "API通信エラー",
            f"{method_l.upper()} {url}: {last_exc}",
            where="api_request",
        )
    return None

# ============================================================
# Notion / TMDB アクセス
# ============================================================

def load_notion_data() -> list:
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    all_results, has_more, next_cursor = [], True, None
    st.session_state.last_notion_load_ok = True
    while has_more:
        payload = {"page_size": 100}
        if next_cursor:
            payload["start_cursor"] = next_cursor
        res = api_request("post", url, headers=NOTION_HEADERS, json=payload)
        if res is None:
            detail = "Notion API応答がありませんでした（timeout/通信断の可能性）"
            st.warning(f"Notion取得失敗: {detail}")
            push_runtime_api_error("Notion取得失敗", detail, where="load_notion_data")
            st.session_state.last_notion_load_ok = False
            break
        if res.status_code != 200:
            body_snip = _truncate_text(res.text or "")
            detail = f"HTTP {res.status_code} / {body_snip or '(no body)'}"
            st.warning(f"Notion取得失敗: {detail}")
            push_runtime_api_error(
                "Notion取得失敗",
                detail,
                where="load_notion_data",
                status_code=res.status_code,
            )
            st.session_state.last_notion_load_ok = False
            break
        data = res.json()
        all_results.extend(data.get("results", []))
        has_more    = data.get("has_more", False)
        next_cursor = data.get("next_cursor")
    return all_results

def query_notion_database_all(database_id: str) -> list:
    return _query_notion_database_all_service(api_request, NOTION_HEADERS, database_id)

# ユニークキーを持つ媒体（自動補填対象）
UNIQUE_KEY_MEDIA = {"映画", "ドラマ", "アニメ", "書籍", "漫画", "音楽アルバム", "ゲーム"}

def get_page_media(page) -> str | None:
    """ページの媒体ラベルを返す"""
    ms = page["properties"].get("媒体", {}).get("multi_select", [])
    raw = ms[0]["name"] if ms else None
    return MEDIA_LABEL_ALIASES.get(raw, raw)

def migrate_media_label_in_notion(old_label: str, new_label: str) -> tuple[int, int]:
    pages = load_notion_data()
    if not st.session_state.get("last_notion_load_ok", True):
        return 0, 0
    total = 0
    updated = 0
    for p in pages:
        pid = p.get("id")
        props = p.get("properties", {})
        ms = (props.get("媒体") or {}).get("multi_select", [])
        names = [m.get("name") for m in ms if m.get("name")]
        if not names or old_label not in names:
            continue
        total += 1
        new_names = [new_label if n == old_label else n for n in names]
        # 重複排除しつつ順序維持
        seen = set()
        uniq = []
        for n in new_names:
            if n not in seen:
                uniq.append(n)
                seen.add(n)
        res = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{pid}",
            headers=NOTION_HEADERS,
            json={"properties": {"媒体": {"multi_select": [{"name": n} for n in uniq]}}},
        )
        if res is not None and res.status_code == 200:
            updated += 1
    return total, updated

def filter_target_pages(all_pages: list) -> list:
    """データ管理・自動同期対象：全媒体"""
    return list(all_pages)

def filter_sync_pages(all_pages: list) -> list:
    """自動補填対象：ユニークキーを持つ媒体のみ"""
    return [p for p in all_pages if get_page_media(p) in UNIQUE_KEY_MEDIA]

def get_tmdb_id_from_notion(props) -> tuple:
    tmdb_id_val    = props.get("TMDB_ID", {}).get("number")
    media_label    = (props.get("媒体") or {}).get("multi_select", [])
    media_label    = media_label[0]["name"] if media_label else None
    media_type     = "movie" if media_label == "映画" else "tv" if media_label == "ドラマ" else None
    return (int(tmdb_id_val) if tmdb_id_val else None), media_type

def save_tmdb_id_to_notion(page_id: str, tmdb_id: int, media_type: str) -> bool:
    res = api_request(
        "patch",
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {
            "TMDB_ID":    {"number": tmdb_id},
        }},
    )
    if res is None or res.status_code != 200:
        st.warning(f"TMDB_ID保存失敗 ({tmdb_id}): {res.status_code if res else 'None'}")
        return False
    return True

def save_season_to_notion(page_id: str, season_number: int) -> bool:
    res = api_request(
        "patch",
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {"SEASON": {"number": season_number}}},
    )
    return res is not None and res.status_code == 200

def fetch_tmdb_ja_title(tmdb_id: int, media_type: str) -> str:
    """TMDBから日本語タイトルを取得"""
    res = api_request(
        "get",
        f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}",
        params={"api_key": TMDB_API_KEY, "language": "ja-JP"},
    )
    if res is None or res.status_code != 200:
        return ""
    data = res.json()
    return data.get("title") or data.get("name") or ""

def fetch_tmdb_by_id(tmdb_id: int, media_type: str) -> dict | None:
    res = api_request(
        "get",
        f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}",
        params={"api_key": TMDB_API_KEY, "language": "en-US"},
    )
    if res is None or res.status_code != 200:
        return None
    data = res.json()
    if not data.get("poster_path"):
        return None
    data["media_type"] = media_type
    return data

def search_tmdb(query: str, year=None, media_type: str = "multi") -> list:
    """TMDB検索。media_type: 'movie' / 'tv' / 'multi'（multiは現在未使用・汎用性のため残存）"""
    params = {"api_key": TMDB_API_KEY, "query": query, "language": "en-US"}
    if media_type == "movie":
        if year:
            params["primary_release_year"] = year
        endpoint = "https://api.themoviedb.org/3/search/movie"
    elif media_type == "tv":
        endpoint = "https://api.themoviedb.org/3/search/tv"
    else:
        if year:
            params["primary_release_year"] = year
        endpoint = "https://api.themoviedb.org/3/search/multi"
    res = api_request("get", endpoint, params=params)
    if res is None:
        return []
    results = res.json().get("results", [])
    if media_type in ("movie", "tv"):
        for r in results:
            r.setdefault("media_type", media_type)
    return [r for r in results if r.get("poster_path") and r.get("media_type") in ["movie", "tv"]]


def search_tmdb_by_person(person_query: str, media_type: str = "multi") -> list:
    """クリエイター/キャスト名でTMDB人物検索→その人の作品一覧を返す。media_type: 'movie' / 'tv' / 'multi'（multiは現在未使用・汎用性のため残存）"""
    res = api_request("get", "https://api.themoviedb.org/3/search/person",
                      params={"api_key": TMDB_API_KEY, "query": person_query, "language": "en-US"})
    if res is None:
        return []
    people = res.json().get("results", [])
    if not people:
        return []
    person_id = people[0]["id"]
    if media_type == "movie":
        res2 = api_request("get", f"https://api.themoviedb.org/3/person/{person_id}/movie_credits",
                           params={"api_key": TMDB_API_KEY, "language": "en-US"})
        mt_filter = ["movie"]
    elif media_type == "tv":
        res2 = api_request("get", f"https://api.themoviedb.org/3/person/{person_id}/tv_credits",
                           params={"api_key": TMDB_API_KEY, "language": "en-US"})
        mt_filter = ["tv"]
    else:
        res2 = api_request("get", f"https://api.themoviedb.org/3/person/{person_id}/combined_credits",
                           params={"api_key": TMDB_API_KEY, "language": "en-US"})
        mt_filter = ["movie", "tv"]
    if res2 is None:
        return []
    credits = res2.json()
    works = credits.get("cast", []) + credits.get("crew", [])
    for w in works:
        if "media_type" not in w:
            w["media_type"] = media_type if media_type != "multi" else "movie"
    seen_ids = set()
    results = []
    for w in sorted(works, key=lambda x: x.get("popularity", 0), reverse=True):
        if w.get("poster_path") and w.get("media_type") in mt_filter and w["id"] not in seen_ids:
            seen_ids.add(w["id"])
            results.append(w)
        if len(results) >= 20:
            break
    return results


def parse_rakuten_date(date_str: str) -> str:
    """楽天APIの日付文字列をISO形式に変換 例: '2004年01月' -> '2004-01-01'"""
    if not date_str:
        return ""
    m = re.match(r'(\d{4})年(\d{2})月?', date_str)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    return date_str[:10] if len(date_str) >= 10 else date_str

def normalize_isbn(isbn: str) -> str:
    if not isbn:
        return ""
    s = re.sub(r'[^0-9Xx]', '', isbn).upper()
    return s

def isbn10_to13(isbn10: str) -> str:
    if len(isbn10) != 10:
        return ""
    core = "978" + isbn10[:9]
    total = 0
    for i, ch in enumerate(core):
        total += int(ch) * (1 if i % 2 == 0 else 3)
    check = (10 - (total % 10)) % 10
    return core + str(check)

def expand_isbn_variants(isbn: str) -> list[str]:
    s = normalize_isbn(isbn)
    if len(s) == 10:
        s13 = isbn10_to13(s)
        return [s, s13] if s13 else [s]
    if len(s) == 13:
        return [s]
    return []

@st.cache_data(ttl=86400)
def get_openlibrary_cover(isbn: str) -> str:
    """ISBNからOpen Libraryの高解像度カバー画像URLを返す。取得できなければ空文字。"""
    if not isbn:
        return ""
    for v in expand_isbn_variants(isbn):
        try:
            check_url = f"https://covers.openlibrary.org/b/isbn/{v}-L.jpg?default=false"
            res = api_request("get", check_url)
            if res and res.status_code == 200 and res.headers.get("Content-Type", "").startswith("image"):
                return f"https://covers.openlibrary.org/b/isbn/{v}-L.jpg"
        except Exception:
            pass
    return ""

@st.cache_data(ttl=86400)
def get_openlibrary_cover_by_search(title: str, author: str | None = None) -> str:
    if not title:
        return ""
    try:
        params = {"title": title, "limit": 5}
        if author:
            params["author"] = author
        res = api_request("get", "https://openlibrary.org/search.json", params=params)
        if not res or res.status_code != 200:
            return ""
        docs = res.json().get("docs", [])
        for d in docs:
            cover_id = d.get("cover_i")
            if cover_id:
                return f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg"
    except Exception:
        pass
    return ""

@st.cache_data(ttl=86400)
def get_openbd_cover(isbn: str) -> str:
    if not isbn:
        return ""
    try:
        v = expand_isbn_variants(isbn)
        if not v:
            return ""
        res = api_request("get", "https://api.openbd.jp/v1/get", params={"isbn": ",".join(v)})
        if not res or res.status_code != 200:
            return ""
        data = res.json()
        if not data:
            return ""
        for entry in data:
            if not entry:
                continue
            summary = entry.get("summary", {})
            cover = summary.get("cover") or ""
            if cover:
                return cover
    except Exception:
        pass
    return ""

def _try_import_pil():
    try:
        from PIL import Image  # type: ignore
        return Image
    except Exception:
        return None

@st.cache_data(ttl=86400)
def probe_image(url: str) -> tuple[int, int, int]:
    if not url:
        return (0, 0, 0)
    img_res = api_request("get", url)
    if img_res is None or img_res.status_code != 200:
        return (0, 0, 0)
    content = img_res.content
    size_bytes = len(content)
    Image = _try_import_pil()
    if Image is None:
        return (0, 0, size_bytes)
    try:
        with Image.open(io.BytesIO(content)) as im:
            w, h = im.size
            return (w, h, size_bytes)
    except Exception:
        return (0, 0, size_bytes)

def choose_best_cover(candidates: list[str]) -> str:
    candidates = [c for c in dict.fromkeys(candidates) if c]
    if not candidates:
        return ""
    best_url = candidates[0]
    best_score = (0, 0, 0)
    for url in candidates:
        w, h, size_bytes = probe_image(url)
        score = (w * h, max(w, h), size_bytes)
        if score > best_score:
            best_score = score
            best_url = url
    return best_url

def get_fast_book_cover(isbn: str, rakuten_cover: str) -> str:
    """高速検索用: 追加リクエストを出さずに最小限のカバーURLを返す"""
    if rakuten_cover:
        return rakuten_cover
    if isbn:
        return f"https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"
    return ""

def collect_book_cover_candidates(isbn: str, title: str, author: str | None, rakuten_cover: str) -> list[str]:
    candidates = []
    if isbn:
        candidates.append(get_openlibrary_cover(isbn))
        candidates.append(get_openbd_cover(isbn))
    if title:
        candidates.append(get_openlibrary_cover_by_search(title, author))
    if rakuten_cover:
        candidates.append(rakuten_cover)
    return [c for c in candidates if c]


def search_books(query: str, author: str = None, page: int = 1, fast: bool = True) -> list:
    """楽天ブックスAPIで書籍検索"""
    rk_params = {
        "applicationId": RAKUTEN_APP_ID,
        "accessKey":      st.secrets.get("RAKUTEN_ACCESS_KEY", ""),
        "hits":          30,
        "page":          page,
        "formatVersion": 2,
        "sort":          "sales",
        "outOfStockFlag": 1,
    }
    if query:  rk_params["title"]  = query
    if author: rk_params["author"] = author
    rk_headers = {
        "Referer":        "https://artemis-cers.streamlit.app",
        "Origin":         "https://artemis-cers.streamlit.app",
        "User-Agent":     "Mozilla/5.0",
        "Authorization": f"Bearer {st.secrets.get('RAKUTEN_ACCESS_KEY', '')}",
    }
    try:
        res = requests.get(
            "https://openapi.rakuten.co.jp/services/api/BooksBook/Search/20170404",
            params=rk_params, headers=rk_headers, timeout=8,
        )
    except Exception as e:
        st.warning(f"⚠️ 楽天ブックスAPI エラー: {e}")
        return []
    if res.status_code != 200:
        st.warning(f"⚠️ 楽天ブックスAPI {res.status_code}: {res.text[:200]}")
        return []
    results = []
    for item in res.json().get("Items", []):
        rakuten_cover = item.get("largeImageUrl") or item.get("mediumImageUrl") or item.get("smallImageUrl", "")
        # --- 修正箇所：URLの整形とクエリパラメータの除去 ---
        if rakuten_cover:
            rakuten_cover = rakuten_cover.replace("http://", "https://").split('?')[0]
        else:
            rakuten_cover = ""
        # ----------------------------------------------
        raw_authors = [a.strip() for a in (item.get("author", "") or "").split("/") if a.strip()]
        authors = [clean_author(a) for a in raw_authors]
        isbn_val = item.get("isbn", "")
        if fast:
            cover = get_fast_book_cover(isbn_val, rakuten_cover)
        else:
            cover_candidates = collect_book_cover_candidates(isbn_val, item.get("title", ""), " / ".join(authors) if authors else None, rakuten_cover)
            cover = choose_best_cover(cover_candidates) or ""
        # --- 念押しで最終的なURLの末尾をカット ---
        if cover:
            cover = cover.split('?')[0]
        # --------------------------------------
        results.append({
            "id":         isbn_val or item.get("title", ""),
            "isbn":        isbn_val,
            "title":      item.get("title", ""),
            "authors":    authors,
            "publisher":  item.get("publisherName", ""),
            "published":  parse_rakuten_date(item.get("salesDate", "") or ""),
            "genres":     [],
            "cover_url":  cover,
            "media_type": "book",
        })
    return results



# ============================================================
# MusicBrainz（演奏曲）
# ============================================================
MB_HEADERS = {
    "User-Agent": "ArteMisCERS/2.0 (https://github.com/attituderko-design/artemis-cers)",
    "Accept": "application/json",
}
MB_DEFAULT_COVER = "https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/icons/music-score.svg"

def make_portrait_filename(composer_name: str) -> str:
    return f"portrait_{sanitize_filename(composer_name)}.jpg"

def save_manual_portrait_for_composer(
    composer_name: str,
    image_bytes: bytes,
    mimetype: str,
    custom_basename: str = "",
) -> str | None:
    """手動アップロード時は標準名を必ず更新し、必要なら別名にも保存する。"""
    canonical_name = make_portrait_filename(composer_name)
    file_id = save_bytes_to_drive(canonical_name, image_bytes, mimetype, make_public=True)
    custom = sanitize_filename((custom_basename or "").strip())
    if custom:
        custom_name = f"portrait_{custom}.jpg"
        if custom_name != canonical_name:
            save_bytes_to_drive(custom_name, image_bytes, mimetype, make_public=True)
    return file_id

def _extract_mb_wiki_relations(relations: list) -> tuple[list[str], str | None]:
    wiki_urls = []
    qid = None
    for rel in relations or []:
        resource = rel.get("url", {}).get("resource", "")
        if not resource:
            continue
        rel_type = (rel.get("type") or "").lower()
        if rel_type == "wikipedia":
            wiki_urls.append(resource)
        elif rel_type == "wikidata":
            m = re.search(r"/(Q\d+)$", resource)
            if m:
                qid = m.group(1)
    return wiki_urls, qid

def _wiki_image_from_page(wiki_url: str) -> tuple[str | None, str | None]:
    """WikipediaページURLから画像URLとWikidata QIDを取得"""
    try:
        parsed = urlparse(wiki_url)
        if not parsed.scheme or not parsed.netloc:
            return None, None
        title = unquote(parsed.path.rsplit("/", 1)[-1]).strip()
        if not title:
            return None, None
        api_url = f"{parsed.scheme}://{parsed.netloc}/w/api.php"
        res = wikimedia_get(api_url,
            params={
                "action": "query",
                "titles": title,
                "prop": "pageimages|pageprops",
                "piprop": "original|thumbnail",
                "pithumbsize": 1200,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if res.status_code != 200:
            return None, None
        pages = (res.json().get("query") or {}).get("pages") or {}
        for page in pages.values():
            pageprops = page.get("pageprops") or {}
            if pageprops.get("disambiguation") is not None:
                continue
            img = ((page.get("original") or {}).get("source")
                   or (page.get("thumbnail") or {}).get("source"))
            qid = pageprops.get("wikibase_item")
            if img:
                return img, qid
            if qid:
                return None, qid
    except Exception:
        return None, None
    return None, None

def _wiki_search_image(query: str, lang: str = "ja") -> tuple[str | None, str | None]:
    """Wikipedia検索結果の上位ページから画像URLとWikidata QIDを取得"""
    if not query:
        return None, None
    try:
        api_url = f"https://{lang}.wikipedia.org/w/api.php"
        sres = wikimedia_get(api_url,
            params={
                "action": "query",
                "list": "search",
                "srsearch": query,
                "srlimit": 3,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if sres.status_code != 200:
            return None, None
        items = (sres.json().get("query") or {}).get("search") or []
        for item in items:
            title = item.get("title", "")
            if not title:
                continue
            pres = wikimedia_get(api_url,
                params={
                    "action": "query",
                    "titles": title,
                    "prop": "pageimages|pageprops",
                    "piprop": "original|thumbnail",
                    "pithumbsize": 1200,
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if pres.status_code != 200:
                continue
            pages = (pres.json().get("query") or {}).get("pages") or {}
            for page in pages.values():
                pageprops = page.get("pageprops") or {}
                if pageprops.get("disambiguation") is not None:
                    continue
                img = ((page.get("original") or {}).get("source")
                       or (page.get("thumbnail") or {}).get("source"))
                qid = pageprops.get("wikibase_item")
                if img:
                    return img, qid
                if qid:
                    return None, qid
    except Exception:
        return None, None
    return None, None

def _wikidata_p18_image_url(qid: str) -> str | None:
    """Wikidata(QID)からP18画像のCommons直リンクを取得"""
    if not qid:
        return None
    try:
        entity = _wikidata_entity(qid)
        if not entity:
            return None
        claims = entity.get("claims") or {}
        p18 = claims.get("P18") or []
        if not p18:
            return None
        filename = ((((p18[0].get("mainsnak") or {}).get("datavalue") or {}).get("value")) or "").strip()
        if not filename:
            return None
        cres = wikimedia_get(
            "https://commons.wikimedia.org/w/api.php",
            params={
                "action": "query",
                "titles": f"File:{filename}",
                "prop": "imageinfo",
                "iiprop": "url",
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if cres.status_code != 200:
            return None
        pages = (cres.json().get("query") or {}).get("pages") or {}
        for page in pages.values():
            infos = page.get("imageinfo") or []
            if infos:
                url = infos[0].get("url")
                if url:
                    return url
        return f"https://commons.wikimedia.org/wiki/Special:FilePath/{quote(filename)}"
    except Exception:
        return None


def _wikidata_entity(qid: str) -> dict:
    if not qid:
        return {}
    try:
        dres = wikimedia_get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            timeout=DEFAULT_TIMEOUT,
        )
        if dres.status_code != 200:
            return {}
        return ((dres.json().get("entities") or {}).get(qid)) or {}
    except Exception:
        return {}


def _wikidata_sitelink_page_images(qid: str, langs: tuple[str, ...] = ("ja", "en", "de", "fr")) -> list[str]:
    entity = _wikidata_entity(qid)
    if not entity:
        return []
    sitelinks = entity.get("sitelinks") or {}
    out = []
    for lang in langs:
        site = f"{lang}wiki"
        title = ((sitelinks.get(site) or {}).get("title") or "").strip()
        if not title:
            continue
        img, _ = _wiki_image_from_page(f"https://{lang}.wikipedia.org/wiki/{quote(title)}")
        if img:
            out.append(img)
    return _dedupe_keep_order(out)


def _wikidata_commons_category_images(qid: str, limit: int = 8) -> list[str]:
    entity = _wikidata_entity(qid)
    if not entity:
        return []
    claims = entity.get("claims") or {}
    p373 = claims.get("P373") or []
    if not p373:
        return []
    cat = ((((p373[0].get("mainsnak") or {}).get("datavalue") or {}).get("value")) or "").strip()
    if not cat:
        return []
    try:
        cm = wikimedia_get(
            "https://commons.wikimedia.org/w/api.php",
            params={
                "action": "query",
                "list": "categorymembers",
                "cmtitle": f"Category:{cat}",
                "cmtype": "file",
                "cmlimit": limit,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if cm.status_code != 200:
            return []
        files = [x.get("title", "") for x in (cm.json().get("query", {}) or {}).get("categorymembers", []) if x.get("title")]
        out = []
        for ft in files:
            cres = wikimedia_get(
                "https://commons.wikimedia.org/w/api.php",
                params={
                    "action": "query",
                    "titles": ft,
                    "prop": "imageinfo",
                    "iiprop": "url",
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if cres.status_code != 200:
                continue
            pages = (cres.json().get("query") or {}).get("pages") or {}
            for page in pages.values():
                infos = page.get("imageinfo") or []
                if infos and infos[0].get("url"):
                    out.append(infos[0]["url"])
        return _dedupe_keep_order(out)
    except Exception:
        return []

def _download_image_bytes(url: str) -> tuple[bytes | None, str | None, str]:
    if not url:
        return None, None, "empty-url"

    def _normalize_url(raw: str) -> str:
        try:
            u = urlparse(raw)
            host = (u.netloc or "").lower()
            path = unquote(u.path or "")
            if host.endswith("wikipedia.org") or host.endswith("wikimedia.org"):
                if path.startswith("/wiki/File:"):
                    fname = path.split("/wiki/File:", 1)[1]
                    return f"https://commons.wikimedia.org/wiki/Special:FilePath/{quote(fname)}"
            return raw
        except Exception:
            return raw

    def _extract_og_image_url(html_text: str) -> str | None:
        try:
            m = re.search(
                r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
                html_text,
                flags=re.IGNORECASE,
            )
            if not m:
                m = re.search(
                    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                    html_text,
                    flags=re.IGNORECASE,
                )
            return m.group(1).strip() if m else None
        except Exception:
            return None

    target = _normalize_url(url)
    hdrs = {
        "User-Agent": WIKIMEDIA_HEADERS.get("User-Agent", "ArteMisCERS/9.x"),
        "Accept": "image/*,*/*;q=0.8",
    }
    last_err = "unknown"
    for _ in range(3):
        try:
            res = api_request("get", target, headers=hdrs, allow_redirects=True, max_retries=3)
            if res is None:
                last_err = "request-failed"
                continue
            if res.status_code != 200:
                last_err = f"status={res.status_code}"
                continue
            if not res.content:
                last_err = "empty-content"
                continue
            ctype = (res.headers.get("Content-Type") or "").split(";")[0].strip().lower()
            if ctype.startswith("text/html"):
                og = _extract_og_image_url(res.text or "")
                if og and og != target:
                    target = og
                    last_err = "html-redirect-og-image"
                    continue
                last_err = "html-response"
                continue
            if not ctype.startswith("image/"):
                ctype = "image/jpeg"
            return res.content, ctype, "ok"
        except Exception as e:
            last_err = f"exception={type(e).__name__}"
    return None, None, last_err

def _composer_query_variants(name: str) -> list[str]:
    base = (name or "").strip()
    if not base:
        return []
    variants = [base]
    for sep in [" / ", "/", "・", ",", " and "]:
        if sep in base:
            variants.append(base.split(sep, 1)[0].strip())
    variants.append(re.sub(r"\s*\([^)]*\)\s*", " ", base).strip())
    out, seen = [], set()
    for v in variants:
        if v and v not in seen:
            out.append(v)
            seen.add(v)
    return out

def _wikidata_search_qids(query: str, lang: str = "en", limit: int = 5) -> list[str]:
    q = (query or "").strip()
    if not q:
        return []
    try:
        res = wikimedia_get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbsearchentities",
                "search": q,
                "language": lang,
                "type": "item",
                "limit": limit,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if res.status_code != 200:
            return []
        out = []
        for x in res.json().get("search", []) or []:
            qid = (x.get("id") or "").strip()
            if qid and qid.startswith("Q"):
                out.append(qid)
        return out
    except Exception:
        return []

@st.cache_data(ttl=86400)
def collect_composer_portrait_candidates(composer_name: str, artist_id: str, limit: int = 12) -> list[str]:
    """作曲家肖像の候補URL一覧を返す（選び直し用）。"""
    artist_id = (artist_id or "").strip()
    if not artist_id:
        return []
    image_candidates = []
    try:
        time.sleep(1.1)
        res = requests.get(
            f"https://musicbrainz.org/ws/2/artist/{artist_id}",
            params={"inc": "url-rels", "fmt": "json"},
            headers=MB_HEADERS,
            timeout=8,
        )
        if res.status_code != 200:
            return []
        artist_data = res.json()
        relations = artist_data.get("relations", [])
        artist_name = (artist_data.get("name") or "").strip()
        wiki_urls, qid = _extract_mb_wiki_relations(relations)
        if not qid:
            for wurl in wiki_urls:
                _img, qid_from_wiki = _wiki_image_from_page(wurl)
                if qid_from_wiki:
                    qid = qid_from_wiki
                    break
        wd_img = _wikidata_p18_image_url(qid) if qid else None
        if wd_img:
            image_candidates.append(wd_img)
        if qid:
            image_candidates.extend(_wikidata_sitelink_page_images(qid))
        for wurl in wiki_urls:
            img_url, qid_from_wiki = _wiki_image_from_page(wurl)
            if img_url:
                image_candidates.append(img_url)
            if not qid and qid_from_wiki:
                qid = qid_from_wiki
        if qid:
            image_candidates.extend(_wikidata_commons_category_images(qid, limit=10))

        all_names = _composer_query_variants(composer_name)
        if artist_name:
            for n in _composer_query_variants(artist_name):
                if n not in all_names:
                    all_names.append(n)
        for lang in ("ja", "en", "de", "fr"):
            for cand_name in all_names:
                img_url, qid_from_search = _wiki_search_image(cand_name, lang)
                if img_url:
                    image_candidates.append(img_url)
                if not qid and qid_from_search:
                    qid = qid_from_search
                if len(image_candidates) >= limit:
                    break
            if len(image_candidates) >= limit:
                break
    except Exception:
        pass

    uniq = []
    seen = set()
    for u in image_candidates:
        if u and u not in seen:
            uniq.append(u)
            seen.add(u)
    uniq.sort(key=lambda x: _rank_portrait_candidate_url(x), reverse=True)
    return uniq[:limit]

def get_composer_portrait_url(composer_name: str, artist_id: str, force_refresh: bool = False) -> str | None:
    """
    1. Driveに既存の肖像画があればそのURLを返す
    2. なければMusicBrainz → Wikipedia/Wikidata/Commonsで取得してDriveに保存
    3. 取得できなければNoneを返す
    """
    fname = make_portrait_filename(composer_name)
    st.session_state["mb_portrait_last_reason"] = ""
    files = get_drive_files()

    # Drive既存チェック（表記ゆれ: 姓名順・カンマ有無など）
    fname_candidates = [fname]
    for n in _composer_query_variants(composer_name):
        fn = make_portrait_filename(n)
        if fn not in fname_candidates:
            fname_candidates.append(fn)
    if not force_refresh:
        for cand_fname in fname_candidates:
            if cand_fname not in files:
                continue
            file_id = files[cand_fname]
            try:
                service = get_drive_service_safe()
                if service:
                    service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
                st.session_state["mb_portrait_last_reason"] = f"Drive既存画像を使用: {cand_fname}"
                return drive_image_url(file_id)
            except Exception:
                pass

    # MusicBrainzからWikipedia/Wikidata情報取得（失敗しても名前検索フォールバックに進む）
    try:
        time.sleep(1.1)
        artist_data = {}
        relations = []
        artist_name = ""
        res = api_request(
            "get",
            f"https://musicbrainz.org/ws/2/artist/{artist_id}",
            params={"inc": "url-rels", "fmt": "json"},
            headers=MB_HEADERS,
            timeout=8,
            max_retries=5,
        )
        if res is not None and res.status_code == 200:
            artist_data = res.json() or {}
            relations = artist_data.get("relations", []) or []
            artist_name = (artist_data.get("name") or "").strip()
        else:
            st.session_state["mb_portrait_last_reason"] = (
                f"musicbrainz artist取得失敗: {res.status_code if res is not None else 'None'}"
            )
        wiki_urls, qid = _extract_mb_wiki_relations(relations)
        image_candidates = []

        # 1) QID確定（最優先: MBのWikidata、次点: Wikipediaページのwikibase_item）
        if not qid:
            for wurl in wiki_urls:
                _img, qid_from_wiki = _wiki_image_from_page(wurl)
                if qid_from_wiki:
                    qid = qid_from_wiki
                    break

        # 2) Wikidata(P18)を最優先
        wd_img = _wikidata_p18_image_url(qid) if qid else None
        if wd_img:
            image_candidates.append(wd_img)

        # 3) QIDの各言語Wikipediaページ画像（人物写真を拾える確率を上げる）
        if qid:
            image_candidates.extend(_wikidata_sitelink_page_images(qid))

        # 4) MusicBrainzが持つWikipediaリンクの画像
        for wurl in wiki_urls:
            img_url, qid_from_wiki = _wiki_image_from_page(wurl)
            if img_url:
                image_candidates.append(img_url)
            if not qid and qid_from_wiki:
                qid = qid_from_wiki

        # 5) Commonsカテゴリ画像（P373）も候補に加える
        if qid:
            image_candidates.extend(_wikidata_commons_category_images(qid, limit=10))

        # 3) 名前検索フォールバック（日本語→英語）
        all_names = _composer_query_variants(composer_name)
        if artist_name:
            for n in _composer_query_variants(artist_name):
                if n not in all_names:
                    all_names.append(n)
        if not image_candidates:
            for cand_name in all_names:
                img_ja, qid_ja = _wiki_search_image(cand_name, "ja")
                if img_ja:
                    image_candidates.append(img_ja)
                if not qid and qid_ja:
                    qid = qid_ja
                if image_candidates:
                    break
        if not image_candidates:
            for cand_name in all_names:
                img_en, qid_en = _wiki_search_image(cand_name, "en")
                if img_en:
                    image_candidates.append(img_en)
                if not qid and qid_en:
                    qid = qid_en
                if image_candidates:
                    break
        if not image_candidates:
            for cand_name in all_names:
                img_de, qid_de = _wiki_search_image(cand_name, "de")
                if img_de:
                    image_candidates.append(img_de)
                if not qid and qid_de:
                    qid = qid_de
                if image_candidates:
                    break
        if not image_candidates:
            for cand_name in all_names:
                img_fr, qid_fr = _wiki_search_image(cand_name, "fr")
                if img_fr:
                    image_candidates.append(img_fr)
                if not qid and qid_fr:
                    qid = qid_fr
                if image_candidates:
                    break
        if not qid:
            for cand_name in all_names:
                qids = _wikidata_search_qids(cand_name, "en", limit=3)
                if qids:
                    qid = qids[0]
                    break
        if not image_candidates and qid:
            wd_img = _wikidata_p18_image_url(qid)
            if wd_img:
                image_candidates.append(wd_img)
            image_candidates.extend(_wikidata_sitelink_page_images(qid))
            image_candidates.extend(_wikidata_commons_category_images(qid, limit=10))

        if not image_candidates:
            st.session_state["mb_portrait_last_reason"] = "候補URLを生成できませんでした"
            return None

        # 同一URLへの再試行を避ける
        uniq_candidates = []
        seen = set()
        for c in image_candidates:
            if c and c not in seen:
                uniq_candidates.append(c)
                seen.add(c)

        image_bytes, mimetype = None, None
        picked_url = None
        for cand in uniq_candidates:
            image_bytes, mimetype, _why = _download_image_bytes(cand)
            if image_bytes:
                picked_url = cand
                break
        if not image_bytes:
            st.session_state["mb_portrait_last_reason"] = (
                f"候補{len(uniq_candidates)}件を試行しましたが、画像DLに失敗しました"
            )
            return None

        try:
            service = get_drive_service_safe()
            if not service:
                # Driveが使えない環境でも、外部URLで表示は継続する
                st.session_state["mb_portrait_last_reason"] = "Drive接続不可のため外部URLを直接使用"
                return picked_url
            if not mimetype:
                mimetype = "image/jpeg"
            media   = MediaIoBaseUpload(io.BytesIO(image_bytes), mimetype=mimetype, resumable=False)
            result  = service.files().create(
                body={"name": fname, "parents": [DRIVE_FOLDER_ID]},
                media_body=media, fields="id",
            ).execute()
            file_id = result["id"]
            cache = st.session_state.get("drive_files_cache")
            if not isinstance(cache, dict):
                cache = {}
                st.session_state["drive_files_cache"] = cache
            cache[fname] = file_id
            service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
            st.session_state["mb_portrait_last_reason"] = "取得成功"
            return drive_image_url(file_id)
        except Exception:
            # Drive保存のみ失敗した場合は取得済みURLで継続
            st.session_state["mb_portrait_last_reason"] = "Drive保存失敗のため外部URLを使用"
            return picked_url

    except Exception as e:
        st.session_state["mb_portrait_last_reason"] = f"例外: {type(e).__name__}"
        st.warning(f"⚠️ 肖像画取得エラー ({composer_name}): {e}")
    return None


def search_mb_composer(name: str) -> tuple[list, str | None]:
    """作曲家名でMusicBrainzのartistを検索。(results, error_msg)を返す"""
    try:
        res = requests.get(
            "https://musicbrainz.org/ws/2/artist",
            params={"query": name, "fmt": "json", "limit": 10},
            headers=MB_HEADERS, timeout=8,
        )
        if res.status_code != 200:
            return [], f"MusicBrainz API {res.status_code}: {res.text[:100]}"
        artists = res.json().get("artists", [])
        return [
            {
                "id":             a["id"],
                "name":           a["name"],
                "sort_name":      (a.get("sort-name") or "").strip(),
                "artist_type":    (a.get("type") or "").strip(),
                "disambiguation": a.get("disambiguation", ""),
                "life_span":      a.get("life-span", {}).get("begin", "")[:4],
                "country":        (a.get("country") or "").strip().upper(),
            }
            for a in artists
        ], None
    except Exception as e:
        return [], str(e)


def format_mb_composer_label(c: dict) -> str:
    name = (c.get("name") or "").strip()
    sort_name = (c.get("sort_name") or "").strip()
    disamb = (c.get("disambiguation") or "").strip()
    life = (c.get("life_span") or "").strip()
    # キリル/漢字等で表示される場合に、読める表記(主にsort-name)を先頭に出す
    has_non_ascii = any(ord(ch) > 127 for ch in name)
    if has_non_ascii and sort_name and sort_name.lower() != name.lower():
        base = f"{sort_name} / {name}"
    else:
        base = name or sort_name
    if disamb:
        base += f"（{disamb}）"
    if life:
        base += f" [{life}–]"
    return base


def canonical_mb_composer_name(c: dict) -> str:
    """登録保存に使う作曲家名を返す（非ASCII名はsort-name優先）。"""
    name = (c.get("name") or "").strip()
    sort_name = (c.get("sort_name") or "").strip()
    has_non_ascii = any(ord(ch) > 127 for ch in name)
    if has_non_ascii and sort_name and sort_name.lower() != name.lower():
        return sort_name
    return name or sort_name

@st.cache_data(ttl=86400)
def get_composer_country_code(composer_name: str) -> str:
    """作曲家名からMusicBrainz/Wikidata経由で国コード(ISO2)を推定。"""
    # キャッシュ更新用バージョン（国コード解決ロジック変更時に更新）
    _resolver_version = "2026-03-16f"
    name = (composer_name or "").strip()
    if not name:
        return ""
    comps, err = search_mb_composer(name)
    if err or not comps:
        return normalize_country_code_for_flag(_wikidata_country_iso2_by_person_name(name))
    norm = name.lower().strip()

    def _norm_name(s: str) -> str:
        txt = (s or "").lower()
        txt = re.sub(r"[^\w\s]", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def _token_set(s: str) -> set[str]:
        return {t for t in _norm_name(s).split(" ") if t}

    query_tokens = _token_set(name)

    def _composer_score(c: dict) -> int:
        cand_name = (c.get("name") or "").strip()
        cand_sort = (c.get("sort_name") or "").strip()
        disamb = (c.get("disambiguation") or "").lower()
        typ = (c.get("artist_type") or "").lower()
        score = 0

        if _norm_name(cand_name) == _norm_name(name) or _norm_name(cand_sort) == _norm_name(name):
            score += 100

        cand_tokens = _token_set(cand_name) | _token_set(cand_sort)
        overlap = len(query_tokens & cand_tokens)
        score += overlap * 10
        if query_tokens and query_tokens.issubset(cand_tokens):
            score += 30

        if typ == "person":
            score += 20
        if "composer" in disamb or "作曲" in disamb:
            score += 20
        if c.get("country"):
            score += 3
        return score

    def _pick_candidates() -> list[dict]:
        ranked = sorted(comps, key=lambda c: _composer_score(c), reverse=True)
        if not ranked:
            return []
        top = _composer_score(ranked[0])
        # 上位候補から大きく離れた同姓/別人を除外（誤国旗防止）
        floor = max(40, top - 15)
        narrowed = [c for c in ranked if _composer_score(c) >= floor]
        return narrowed[:5]

    def _sanitize_cc(cc: str) -> str:
        return normalize_country_code_for_flag(cc)

    # まずは artist詳細で「国籍系(Wikidata)優先」で解決
    for c in _pick_candidates()[:5]:
        mbid = (c.get("id") or "").strip()
        if not mbid:
            continue
        cc = _sanitize_cc(_get_mb_artist_country_code_by_id(mbid))
        if cc:
            return cc

    # MBで取れない場合は、名称からWikidata人名検索で救済
    cc = _sanitize_cc(_wikidata_country_iso2_by_person_name(name))
    if cc:
        return cc
    return ""


def trace_get_composer_country_code(composer_name: str) -> dict:
    """国コード解決の経路を可視化するデバッグ用トレース。"""
    name = (composer_name or "").strip()
    out = {
        "query": name,
        "search_error": "",
        "search_count": 0,
        "rank_floor": None,
        "candidates": [],
        "selected_path": "",
        "final_country_code": "",
    }
    if not name:
        out["selected_path"] = "empty-query"
        return out

    comps, err = search_mb_composer(name)
    out["search_error"] = err or ""
    out["search_count"] = len(comps or [])
    if err or not comps:
        cc = normalize_country_code_for_flag(_wikidata_country_iso2_by_person_name(name))
        out["selected_path"] = "fallback:wikidata-by-person-name"
        out["final_country_code"] = cc or ""
        return out

    def _norm_name(s: str) -> str:
        txt = (s or "").lower()
        txt = re.sub(r"[^\w\s]", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def _token_set(s: str) -> set[str]:
        return {t for t in _norm_name(s).split(" ") if t}

    query_tokens = _token_set(name)

    def _composer_score(c: dict) -> int:
        cand_name = (c.get("name") or "").strip()
        cand_sort = (c.get("sort_name") or "").strip()
        disamb = (c.get("disambiguation") or "").lower()
        typ = (c.get("artist_type") or "").lower()
        score = 0
        if _norm_name(cand_name) == _norm_name(name) or _norm_name(cand_sort) == _norm_name(name):
            score += 100
        cand_tokens = _token_set(cand_name) | _token_set(cand_sort)
        overlap = len(query_tokens & cand_tokens)
        score += overlap * 10
        if query_tokens and query_tokens.issubset(cand_tokens):
            score += 30
        if typ == "person":
            score += 20
        if "composer" in disamb or "作曲" in disamb:
            score += 20
        if c.get("country"):
            score += 3
        return score

    ranked = sorted(comps, key=lambda c: _composer_score(c), reverse=True)
    top = _composer_score(ranked[0]) if ranked else 0
    floor = max(40, top - 15)
    out["rank_floor"] = floor
    narrowed = [c for c in ranked if _composer_score(c) >= floor][:5]

    for c in narrowed:
        mbid = (c.get("id") or "").strip()
        cc = ""
        if mbid:
            cc = normalize_country_code_for_flag(_get_mb_artist_country_code_by_id(mbid))
        row = {
            "id": mbid,
            "name": c.get("name", ""),
            "sort_name": c.get("sort_name", ""),
            "artist_type": c.get("artist_type", ""),
            "disambiguation": c.get("disambiguation", ""),
            "mb_country_field": c.get("country", ""),
            "score": _composer_score(c),
            "resolved_country_code": cc or "",
        }
        out["candidates"].append(row)
        if cc and not out["final_country_code"]:
            out["final_country_code"] = cc
            out["selected_path"] = f"mb-candidate:{mbid}"

    if not out["final_country_code"]:
        cc = normalize_country_code_for_flag(_wikidata_country_iso2_by_person_name(name))
        out["final_country_code"] = cc or ""
        out["selected_path"] = "fallback:wikidata-by-person-name"
    return out


@st.cache_data(ttl=86400)
def _get_mb_area_iso2(area_id: str) -> str:
    aid = (area_id or "").strip()
    if not aid:
        return ""
    try:
        res = requests.get(
            f"https://musicbrainz.org/ws/2/area/{aid}",
            params={"fmt": "json", "inc": "iso-3166-1-codes"},
            headers=MB_HEADERS,
            timeout=8,
        )
        if res.status_code != 200:
            return ""
        data = res.json() or {}
        codes = data.get("iso-3166-1-codes") or []
        if isinstance(codes, list):
            for c in codes:
                cc = normalize_country_code_for_flag(c)
                if cc:
                    return cc
    except Exception:
        return ""
    return ""


@st.cache_data(ttl=86400)
def _get_wikidata_country_iso2(qid: str, preferred_cc: str = "") -> str:
    q = (qid or "").strip().upper()
    if not re.fullmatch(r"Q[0-9]+", q):
        return ""
    try:
        res = wikimedia_get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbgetentities",
                "ids": q,
                "props": "claims",
                "format": "json",
            },
            timeout=10,
        )
        if res.status_code != 200:
            return ""
        ent = (((res.json() or {}).get("entities") or {}).get(q) or {})
        claims = ent.get("claims") or {}
        p27 = claims.get("P27") or []   # country of citizenship（演奏曲では採用しない）
        p495 = claims.get("P495") or [] # country of origin
        had_nationality_claim = bool(p27 or p495)

        def _claim_country_codes(claims_list: list) -> list[str]:
            out = []
            seen = set()
            for claim in claims_list:
                dv = (((claim or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {}
                cid = dv.get("id")
                if not cid:
                    continue
                cc = _country_qid_to_modern_iso2(cid)
                if cc and cc not in seen:
                    seen.add(cc)
                    out.append(cc)
            return out

        p495_codes = _claim_country_codes(p495)
        p27_codes = _claim_country_codes(p27)

        preferred_cc = normalize_country_code_for_flag(preferred_cc or "")
        # MB側で国コードが取れている場合は最優先。
        # （Wikidataの歴史国家/多重国籍ノイズで誤判定するのを防ぐ）
        if preferred_cc:
            return preferred_cc
        # 次点: 本人の国籍(P27)
        if p27_codes:
            return p27_codes[0]
        # 次点: 出自(P495)
        if p495_codes:
            return p495_codes[0]

        # 上記で未解決の場合のみ、出生地の現代主権国を探索
        birth_place_qid = ""
        for claim in (claims.get("P19") or []):  # place of birth
            dv = (((claim or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {}
            pq = (dv.get("id") or "").strip().upper()
            if re.fullmatch(r"Q[0-9]+", pq):
                birth_place_qid = pq
                break
        if birth_place_qid:
            birth_cc = _resolve_country_iso2_from_place_qid(birth_place_qid, max_depth=8)
            if birth_cc:
                return birth_cc
            # 国籍/出自があるのに現代ISOへ落ちない場合は、出生地フォールバックで誤判定しない
            if had_nationality_claim:
                return ""

        # 歴史人物でP27/P495が現代ISOに落ちない場合:
        # 出生/死亡/拠点地などの場所QIDから P17(国) を辿って補完
        place_claim_keys = ("P19", "P20", "P937", "P551", "P740")
        place_qids = []
        for pk in place_claim_keys:
            for claim in (claims.get(pk) or []):
                dv = (((claim or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {}
                pq = dv.get("id")
                if pq and re.fullmatch(r"Q[0-9]+", str(pq).upper()):
                    place_qids.append(str(pq).upper())
        for pq in place_qids:
            cc = _resolve_country_iso2_from_place_qid(pq, max_depth=8)
            if cc:
                return cc
    except Exception:
        return ""
    return ""


@st.cache_data(ttl=86400)
def _get_wikidata_country_iso2_by_country_qid(country_qid: str) -> str:
    cq = (country_qid or "").strip().upper()
    if not re.fullmatch(r"Q[0-9]+", cq):
        return ""
    try:
        res = wikimedia_get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbgetentities",
                "ids": cq,
                "props": "claims",
                "format": "json",
            },
            timeout=10,
        )
        if res.status_code != 200:
            return ""
        ent = (((res.json() or {}).get("entities") or {}).get(cq) or {})
        claims = ent.get("claims") or {}
        p297 = claims.get("P297") or []  # ISO 3166-1 alpha-2 code
        for claim in p297:
            v = (((claim or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value")
            cc = normalize_country_code_for_flag(v or "")
            if cc:
                return cc
    except Exception:
        return ""
    return ""


@st.cache_data(ttl=86400)
def _is_wikidata_historical_country_qid(country_qid: str) -> bool:
    cq = (country_qid or "").strip().upper()
    if not re.fullmatch(r"Q[0-9]+", cq):
        return False
    ent = _wikidata_entity(cq) or {}
    claims = ent.get("claims") or {}
    # 失効/廃止/終了が付いている国家は歴史国家として扱う
    if claims.get("P576") or claims.get("P582"):
        return True
    # instance of historical classes（代表例）
    historical_classes = {"Q3024240", "Q28171280"}  # historical country / former country
    for c in (claims.get("P31") or []):
        dv = ((((c or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {})
        if (dv.get("id") or "").strip().upper() in historical_classes:
            return True
    return False


@st.cache_data(ttl=86400)
def _resolve_modern_iso2_from_country_qid(country_qid: str, max_depth: int = 8) -> str:
    """
    歴史国家QIDから現行ISO2へ寄せる。
    代表的に P1366(replaced by) / P155(follows) を辿って P297 を探す。
    """
    start = (country_qid or "").strip().upper()
    if not re.fullmatch(r"Q[0-9]+", start):
        return ""
    # クラシックで頻出する歴史国家の最小マッピング
    # （網羅ではなく実運用での誤判定抑制を優先）
    qid_map = {
        "Q34266": "RU",  # Russian Empire
        "Q15180": "RU",  # Soviet Union
        "Q15290": "DE",  # Prussia
        "Q12548": "DE",  # Holy Roman Empire
        "Q28513": "AT",  # Austria-Hungary
        "Q39193": "CZ",  # Bohemia / Kingdom of Bohemia
    }
    if start in qid_map:
        return qid_map[start]
    visited = set()
    queue = [(start, 0)]
    while queue:
        qid, depth = queue.pop(0)
        if qid in visited or depth > max_depth:
            continue
        visited.add(qid)
        cc = _get_wikidata_country_iso2_by_country_qid(qid)
        if cc:
            return cc
        ent = _wikidata_entity(qid) or {}
        claims = ent.get("claims") or {}
        for prop in ("P1366", "P155"):  # replaced by / follows
            for claim in (claims.get(prop) or []):
                dv = (((claim or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {}
                nq = (dv.get("id") or "").strip().upper()
                if re.fullmatch(r"Q[0-9]+", nq):
                    queue.append((nq, depth + 1))
    return ""


@st.cache_data(ttl=86400)
def _country_qid_to_modern_iso2(country_qid: str) -> str:
    """国QIDを現代ISO2へ変換（直接ISO→歴史国家変換の順で解決）。"""
    cq = (country_qid or "").strip().upper()
    if not re.fullmatch(r"Q[0-9]+", cq):
        return ""
    cc = _get_wikidata_country_iso2_by_country_qid(cq)
    if cc:
        return cc
    return _resolve_modern_iso2_from_country_qid(cq)


@st.cache_data(ttl=86400)
def _resolve_country_iso2_from_place_qid(place_qid: str, max_depth: int = 8) -> str:
    """場所QIDから国ISO2を再帰的に解決する（出生地の現在主権国を優先）。"""
    start = (place_qid or "").strip().upper()
    if not re.fullmatch(r"Q[0-9]+", start):
        return ""
    visited = set()
    queue = [(start, 0)]
    while queue:
        qid, depth = queue.pop(0)
        if qid in visited or depth > max_depth:
            continue
        visited.add(qid)
        ent = _wikidata_entity(qid) or {}
        claims = ent.get("claims") or {}
        # そのものが国でISO2を持つ場合
        cc_self = _get_wikidata_country_iso2_by_country_qid(qid)
        if cc_self:
            if not _is_wikidata_historical_country_qid(qid):
                return cc_self
            # 歴史国家でも後継を辿って現代ISOへ寄せる
            mapped = _resolve_modern_iso2_from_country_qid(qid)
            if mapped:
                return mapped
        # 行政単位(P131)を優先して辿る（現代国家に着地しやすい）
        for claim in (claims.get("P131") or []):
            dv = (((claim or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {}
            pid = (dv.get("id") or "").strip().upper()
            if re.fullmatch(r"Q[0-9]+", pid):
                queue.append((pid, depth + 1))
        # 直接の国(P17)は補助扱い
        for claim in (claims.get("P17") or []):
            dv = (((claim or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {}
            cid = (dv.get("id") or "").strip().upper()
            if not re.fullmatch(r"Q[0-9]+", cid):
                continue
            if _is_wikidata_historical_country_qid(cid):
                mapped = _resolve_modern_iso2_from_country_qid(cid)
                if mapped:
                    return mapped
            else:
                cc = _get_wikidata_country_iso2_by_country_qid(cid)
                if cc:
                    return cc
            queue.append((cid, depth + 1))
    return ""


@st.cache_data(ttl=86400)
def _get_mb_artist_country_code_by_id(artist_id: str) -> str:
    aid = (artist_id or "").strip()
    if not aid:
        return ""
    try:
        res = requests.get(
            f"https://musicbrainz.org/ws/2/artist/{aid}",
            params={"fmt": "json", "inc": "url-rels"},
            headers=MB_HEADERS,
            timeout=8,
        )
        if res.status_code != 200:
            return ""
        data = res.json() or {}
        mb_country_pref = normalize_country_code_for_flag((data.get("country") or "").strip().upper())
        # 国籍はWikidata(P27/P495)を最優先
        for rel in data.get("relations") or []:
            if (rel.get("type") or "").lower() != "wikidata":
                continue
            resource = ((rel.get("url") or {}).get("resource") or "").strip()
            m = re.search(r"/wiki/(Q[0-9]+)$", resource)
            if not m:
                continue
            cc = _get_wikidata_country_iso2(m.group(1), preferred_cc=mb_country_pref)
            if cc:
                return cc

        # 関連が弱い/無い場合は人名検索でWikidata救済
        for n in ((data.get("name") or "").strip(), (data.get("sort-name") or "").strip()):
            if not n:
                continue
            cc = _wikidata_country_iso2_by_person_name(n)
            if cc:
                return cc

        # Wikidataで解決不能な場合のみ、MB countryを最終フォールバックとして採用
        # （活動地由来のノイズはあり得るが、未解決よりは運用上有益）
        mb_cc = mb_country_pref
        if mb_cc:
            return mb_cc
    except Exception:
        return ""
    return ""


@st.cache_data(ttl=86400)
def _wikidata_country_iso2_by_person_name(person_name: str) -> str:
    q = (person_name or "").strip()
    if not q:
        return ""

    variants = [q]
    if "," in q:
        parts = [p.strip() for p in q.split(",") if p.strip()]
        if len(parts) >= 2:
            variants.append(" ".join(parts[1:] + [parts[0]]))
    q_tokens = set(re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿĀ-žА-Яа-яЁё]+", q.lower()))

    best_cc, best_score = "", -1
    seen_qids = set()

    for v in variants:
        try:
            sres = wikimedia_get(
                "https://www.wikidata.org/w/api.php",
                params={
                    "action": "wbsearchentities",
                    "search": v,
                    "language": "en",
                    "type": "item",
                    "limit": 8,
                    "format": "json",
                },
                timeout=10,
            )
            if sres.status_code != 200:
                continue
            items = (sres.json() or {}).get("search") or []
        except Exception:
            continue

        for it in items:
            qid = (it.get("id") or "").strip().upper()
            if not re.fullmatch(r"Q[0-9]+", qid) or qid in seen_qids:
                continue
            seen_qids.add(qid)
            ent = _wikidata_entity(qid) or {}
            claims = ent.get("claims") or {}

            # human (P31=Q5) 以外は除外
            is_human = False
            for c in (claims.get("P31") or []):
                dv = ((((c or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {})
                if (dv.get("id") or "").upper() == "Q5":
                    is_human = True
                    break
            if not is_human:
                continue

            # composer (P106=Q36834) を優遇
            is_composer = False
            for c in (claims.get("P106") or []):
                dv = ((((c or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {})
                if (dv.get("id") or "").upper() == "Q36834":
                    is_composer = True
                    break

            cc = _get_wikidata_country_iso2(qid)
            if not cc:
                continue

            label = (it.get("label") or "").lower()
            desc = (it.get("description") or "").lower()
            lbl_tokens = set(re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿĀ-žА-Яа-яЁё]+", label))
            overlap = len(q_tokens & lbl_tokens)

            score = overlap * 20
            if is_composer:
                score += 40
            if "composer" in desc or "作曲" in desc:
                score += 20
            if label == q.lower():
                score += 25

            if score > best_score:
                best_score = score
                best_cc = cc

    return normalize_country_code_for_flag(best_cc)


def debug_wikidata_country_resolution(qid: str) -> dict:
    q = (qid or "").strip().upper()
    out = {
        "qid": q,
        "valid_qid": bool(re.fullmatch(r"Q[0-9]+", q)),
        "p19_birth_place_qids": [],
        "p27_citizenship_qids": [],
        "p495_origin_qids": [],
        "resolved_country_code": "",
    }
    if not out["valid_qid"]:
        return out
    ent = _wikidata_entity(q) or {}
    claims = ent.get("claims") or {}

    def _extract_qids(p: str) -> list[str]:
        vals = []
        for c in (claims.get(p) or []):
            dv = ((((c or {}).get("mainsnak") or {}).get("datavalue") or {}).get("value") or {})
            cq = (dv.get("id") or "").strip().upper()
            if re.fullmatch(r"Q[0-9]+", cq):
                vals.append(cq)
        return vals

    out["p19_birth_place_qids"] = _extract_qids("P19")
    out["p27_citizenship_qids"] = _extract_qids("P27")
    out["p495_origin_qids"] = _extract_qids("P495")
    out["resolved_country_code"] = _get_wikidata_country_iso2(q) or ""
    return out


def search_mb_works_by_title(title: str, limit: int = 10) -> tuple[list, str | None]:
    """曲名でMusicBrainz Workを検索。(results, error_msg)"""
    q = (title or "").strip()
    if not q:
        return [], None
    try:
        res = requests.get(
            "https://musicbrainz.org/ws/2/work",
            params={"query": f'work:"{q}"', "fmt": "json", "limit": limit},
            headers=MB_HEADERS, timeout=8,
        )
        if res.status_code != 200:
            return [], f"MusicBrainz API {res.status_code}: {res.text[:100]}"
        works = res.json().get("works", [])
        out = []
        for w in works:
            out.append({
                "id": w.get("id", ""),
                "title": w.get("title", ""),
                "disambiguation": w.get("disambiguation", ""),
            })
        return out, None
    except Exception as e:
        return [], str(e)


@st.cache_data(ttl=3600)
def search_mb_works(artist_id: str, title_filter: str = "") -> list:
    """作曲家MBIDで作品一覧を取得（上限なし・ページング）"""
    def _norm_text(s: str) -> str:
        x = (s or "").lower()
        x = re.sub(r"\b(no|no\.|nr|nr\.|number)\b", " no ", x)
        x = re.sub(r"[^0-9a-zA-Z\u00C0-\u024F\u3040-\u30FF\u3400-\u9FFF]+", " ", x)
        x = re.sub(r"\s+", " ", x).strip()
        return x

    def _title_match(query: str, title: str) -> bool:
        q = _norm_text(query)
        t = _norm_text(title)
        if not q:
            return True
        if q in t:
            return True
        q_tokens = [tok for tok in q.split() if tok not in {"no", "opus", "op"}]
        t_tokens = set(t.split())
        return bool(q_tokens) and all(tok in t_tokens for tok in q_tokens)

    works = []
    offset = 0
    limit  = 100
    while True:
        try:
            time.sleep(1.1)  # レート制限: 1秒1リクエスト
            res = requests.get(
                "https://musicbrainz.org/ws/2/work",
                params={
                    "artist": artist_id,
                    "fmt":    "json",
                    "limit":  limit,
                    "offset": offset,
                },
                headers=MB_HEADERS, timeout=10,
            )
            if res.status_code != 200:
                break
            data     = res.json()
            batch    = data.get("works", [])
            works   += batch
            total    = data.get("work-count", 0)
            offset  += limit
            if offset >= total or not batch:
                break
        except Exception:
            break

    results = []
    for w in works:
        title = w.get("title", "")
        if title_filter and not _title_match(title_filter, title):
            continue
        disambiguation = w.get("disambiguation", "")
        results.append({
            "id":             w["id"],
            "title":          title,
            "disambiguation": disambiguation,
            "type":           w.get("type", ""),
            "first_release_date": (w.get("first-release-date") or "").strip(),
        })
    # タイトルでソート
    results.sort(key=lambda x: x["title"])
    return results

def _format_wikidata_time(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    m = re.match(r"^[+-]?(\d{4})-(\d{2})-(\d{2})", raw)
    if not m:
        return ""
    y, mm, dd = m.group(1), m.group(2), m.group(3)
    if mm == "00":
        return y
    if dd == "00":
        return f"{y}-{mm}"
    return f"{y}-{mm}-{dd}"

def _normalize_human_date(text: str) -> str:
    raw = re.sub(r"\s+", " ", (text or "").strip())
    if not raw:
        return ""
    # yyyy-mm-dd / yyyy/mm/dd
    m = re.search(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b", raw)
    if m:
        y, mm, dd = m.group(1), int(m.group(2)), int(m.group(3))
        return f"{y}-{mm:02d}-{dd:02d}"
    # yyyy年m月d日
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", raw)
    if m:
        y, mm, dd = m.group(1), int(m.group(2)), int(m.group(3))
        return f"{y}-{mm:02d}-{dd:02d}"
    # yyyy年m月
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月", raw)
    if m:
        y, mm = m.group(1), int(m.group(2))
        return f"{y}-{mm:02d}"
    # English date formats
    for fmt in ("%d %B %Y", "%d %b %Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    # year only
    m = re.search(r"\b(1[5-9]\d{2}|20\d{2})\b", raw)
    if m:
        return m.group(1)
    return ""

def _date_precision(dt: str) -> str:
    s = (dt or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return "day"
    if re.match(r"^\d{4}-\d{2}$", s):
        return "month"
    if re.match(r"^\d{4}$", s):
        return "year"
    return "unknown"

def _normalize_notion_date_input(value: str) -> str:
    """手入力日付をNotion向けISO日付(YYYY-MM-DD)へ補正。"""
    s = (value or "").strip()
    if not s:
        return ""
    s = s.replace("/", "-").replace(".", "-")
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m:
        y, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return date(y, mm, dd).isoformat()
        except Exception:
            return ""
    # 既にISO datetimeの場合
    m2 = re.match(r"^(\d{4}-\d{2}-\d{2})T", s)
    if m2:
        return m2.group(1)
    return ""

def _strip_wiki_markup(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return ""
    s = re.sub(r"<ref[^>]*>.*?</ref>", " ", s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\{\{[^{}]*\}\}", " ", s)
    s = re.sub(r"\[\[(?:[^|\]]+\|)?([^\]]+)\]\]", r"\1", s)
    s = re.sub(r"\[[^\]]+\]", " ", s)
    s = re.sub(r"''+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _wiki_premiere_candidates(work_title: str, composer_name: str = "", limit: int = 6) -> list[dict]:
    title = (work_title or "").strip()
    if not title:
        return []
    queries = []
    if composer_name:
        queries.append(f"{title} {composer_name}")
    queries.append(title)
    out = []
    seen = set()
    for lang in ("ja", "en"):
        api_url = f"https://{lang}.wikipedia.org/w/api.php"
        for q in queries:
            try:
                sres = wikimedia_get(
                    api_url,
                    params={
                        "action": "query",
                        "list": "search",
                        "srsearch": q,
                        "srlimit": 5,
                        "format": "json",
                    },
                    timeout=DEFAULT_TIMEOUT,
                )
                if sres.status_code != 200:
                    continue
                items = (sres.json().get("query") or {}).get("search") or []
                for item in items:
                    ptitle = (item.get("title") or "").strip()
                    if not ptitle:
                        continue
                    k = f"{lang}:{ptitle}"
                    if k in seen:
                        continue
                    seen.add(k)
                    pres = wikimedia_get(
                        api_url,
                        params={
                            "action": "query",
                            "titles": ptitle,
                            "prop": "revisions",
                            "rvslots": "main",
                            "rvprop": "content",
                            "format": "json",
                        },
                        timeout=DEFAULT_TIMEOUT,
                    )
                    if pres.status_code != 200:
                        continue
                    pages = (pres.json().get("query") or {}).get("pages") or {}
                    wikitext = ""
                    for page in pages.values():
                        revs = page.get("revisions") or []
                        if revs:
                            wikitext = (((revs[0].get("slots") or {}).get("main") or {}).get("*") or "")
                            break
                    if not wikitext:
                        continue
                    lines = wikitext.splitlines()
                    candidate_line = ""
                    for ln in lines[:400]:
                        if re.search(r"^\s*\|\s*(premiere|初演)\s*=", ln, flags=re.IGNORECASE):
                            candidate_line = ln.split("=", 1)[1].strip()
                            break
                    if not candidate_line:
                        # prose fallback
                        m = re.search(r"(premier(?:ed|e)\b[^.\n]{0,120}|\b初演[^。\n]{0,120})", wikitext, flags=re.IGNORECASE)
                        if m:
                            candidate_line = m.group(1)
                    if not candidate_line:
                        continue
                    cleaned = _strip_wiki_markup(candidate_line)
                    dt = _normalize_human_date(cleaned)
                    if not dt:
                        continue
                    url = f"https://{lang}.wikipedia.org/wiki/{quote(ptitle)}"
                    out.append(
                        {
                            "qid": "",
                            "title": ptitle,
                            "date": dt,
                            "precision": _date_precision(dt),
                            "urls": [url],
                            "score": 500 if lang == "ja" else 450,
                        }
                    )
                    if len(out) >= limit:
                        return out
            except Exception:
                continue
    return out[:limit]

@st.cache_data(ttl=86400)
def get_mb_work_premiere_info(work_id: str, work_title: str = "", composer_name: str = "") -> tuple[str, str]:
    def _extract_dates_from_qid(qid: str, strict_first_perf: bool = False) -> list[str]:
        if not qid:
            return []
        dres = wikimedia_get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            timeout=DEFAULT_TIMEOUT,
        )
        if dres.status_code != 200:
            return []
        entity = ((dres.json().get("entities") or {}).get(qid)) or {}
        claims = entity.get("claims") or {}
        candidates = []
        # P1191: date of first performance（最優先）
        pids = ["P1191"] if strict_first_perf else ["P1191", "P571", "P577"]
        for pid in pids:
            for c in claims.get(pid, []) or []:
                val = ((((c.get("mainsnak") or {}).get("datavalue") or {}).get("value")) or {}).get("time")
                dt = _format_wikidata_time(val)
                if dt:
                    candidates.append(dt)
        return candidates

    work_id = (work_id or "").strip()
    if not work_id:
        return "", "work-id-empty"
    try:
        time.sleep(1.1)
        # MusicBrainz側の解釈差に備えて include 指定を段階的にフォールバック
        wres = requests.get(
            f"https://musicbrainz.org/ws/2/work/{work_id}",
            params={"inc": "url-rels recordings", "fmt": "json"},
            headers=MB_HEADERS,
            timeout=DEFAULT_TIMEOUT,
        )
        if wres.status_code == 400:
            time.sleep(1.1)
            wres = requests.get(
                f"https://musicbrainz.org/ws/2/work/{work_id}",
                params={"fmt": "json"},
                headers=MB_HEADERS,
                timeout=DEFAULT_TIMEOUT,
            )
        if wres.status_code == 400:
            # UUID直指定が拒否されるケース向けに検索APIで再取得
            time.sleep(1.1)
            sres = requests.get(
                "https://musicbrainz.org/ws/2/work",
                params={"query": f"wid:{work_id}", "fmt": "json", "limit": 1},
                headers=MB_HEADERS,
                timeout=DEFAULT_TIMEOUT,
            )
            if sres.status_code == 200:
                sworks = (sres.json() or {}).get("works", []) or []
                if sworks:
                    work_data = sworks[0]
                else:
                    return "", "mb-work-400-nohit"
            else:
                return "", f"mb-work-{sres.status_code}"
        elif wres.status_code != 200:
            return "", f"mb-work-{wres.status_code}"
        else:
            work_data = wres.json()
        # premiere関連リレーションの日付を優先採用
        rel_dates = []
        for rel in work_data.get("relations", []) or []:
            rtype = (rel.get("type") or "").strip().lower()
            if "premiere" in rtype or "first performance" in rtype:
                for key in ("begin", "end"):
                    d = (rel.get(key) or "").strip()
                    if d:
                        rel_dates.append(d)
        if rel_dates:
            return sorted(rel_dates)[0], "musicbrainz-relation"

        # まずはMusicBrainz側で取得できる最古日付を優先
        rec_dates = []
        for rec in work_data.get("recordings", []) or []:
            d = (rec.get("first-release-date") or "").strip()
            if d:
                rec_dates.append(d)
        if rec_dates:
            return sorted(rec_dates)[0], "musicbrainz-recording"
        relations = work_data.get("relations", [])
        wiki_urls, qid = _extract_mb_wiki_relations(relations)
        if not qid:
            for wurl in wiki_urls:
                _, qid_from_wiki = _wiki_image_from_page(wurl)
                if qid_from_wiki:
                    qid = qid_from_wiki
                    break
        candidates = []
        if qid:
            candidates.extend(_extract_dates_from_qid(qid))
            if candidates:
                return sorted(candidates)[0], "wikidata-qid"
        if not candidates:
            title = (work_title or "").strip()
            comp = (composer_name or "").strip()
            search_queries = []
            if title and comp:
                search_queries.append(f"{title} {comp}")
            if title:
                search_queries.append(title)
            for sq in search_queries:
                for sq_qid in _wikidata_search_qids(sq, "en", limit=4):
                    # 検索解決は誤QID混入の可能性があるため、初演日(P1191)のみ採用
                    candidates.extend(_extract_dates_from_qid(sq_qid, strict_first_perf=True))
                if candidates:
                    break
        if candidates:
            return sorted(candidates)[0], "wikidata-search"
        return "", "not-found"
    except Exception:
        return "", "exception"

def get_mb_work_premiere_date(work_id: str, work_title: str = "", composer_name: str = "") -> str:
    return get_mb_work_premiere_info(work_id, work_title, composer_name)[0]

@st.cache_data(ttl=86400)
def search_premiere_candidates(work_title: str, composer_name: str = "", limit: int = 8) -> list[dict]:
    """Wikidata(P1191)から初演候補を取得（半自動選択用）"""
    title_raw = (work_title or "").strip()
    comp_raw = (composer_name or "").strip()
    if not title_raw:
        return []

    def _clean_title(t: str) -> str:
        x = re.sub(r"\([^)]*\)", " ", t or "")
        x = re.sub(r"\[[^\]]*\]", " ", x)
        x = re.sub(r"\s+", " ", x).strip()
        return x

    def _norm_tokens(s: str) -> set[str]:
        x = re.sub(r"[^0-9A-Za-z\u00C0-\u024F\u3040-\u30FF\u3400-\u9FFF]+", " ", (s or "").lower())
        return {tok for tok in x.split() if tok}

    title_clean = _clean_title(title_raw)
    qlist = []
    if title_raw and comp_raw:
        qlist.append(f"{title_raw} {comp_raw}")
    if title_clean and comp_raw and title_clean != title_raw:
        qlist.append(f"{title_clean} {comp_raw}")
    qlist.append(title_raw)
    if title_clean != title_raw:
        qlist.append(title_clean)

    qids = []
    for q in qlist:
        qids.extend(_wikidata_search_qids(q, "en", limit=6))
        qids.extend(_wikidata_search_qids(q, "ja", limit=4))
    seen_qid = set()
    uniq_qids = []
    for qid in qids:
        if qid and qid not in seen_qid:
            uniq_qids.append(qid)
            seen_qid.add(qid)

    title_tokens = _norm_tokens(title_clean or title_raw)
    comp_tokens = _norm_tokens(comp_raw)

    out = []
    for qid in uniq_qids:
        entity = _wikidata_entity(qid)
        if not entity:
            continue
        claims = entity.get("claims") or {}
        dates = []
        for c in (claims.get("P1191") or []):
            val = ((((c.get("mainsnak") or {}).get("datavalue") or {}).get("value")) or {}).get("time")
            dt = _format_wikidata_time(val)
            if dt:
                dates.append(dt)
        if not dates:
            continue

        labels = entity.get("labels") or {}
        title = ((labels.get("ja") or {}).get("value") or (labels.get("en") or {}).get("value") or qid).strip()
        sitelinks = entity.get("sitelinks") or {}
        ja_title = (sitelinks.get("jawiki") or {}).get("title")
        en_title = (sitelinks.get("enwiki") or {}).get("title")
        urls = []
        if ja_title:
            urls.append(f"https://ja.wikipedia.org/wiki/{quote(ja_title)}")
        if en_title:
            urls.append(f"https://en.wikipedia.org/wiki/{quote(en_title)}")
        if not urls:
            urls.append(f"https://www.wikidata.org/wiki/{qid}")

        cand_tokens = _norm_tokens(title)
        score = len(title_tokens & cand_tokens) * 3
        if comp_tokens and comp_tokens & _norm_tokens(" ".join(urls)):
            score += 1
        out.append(
            {
                "qid": qid,
                "title": title,
                "date": sorted(dates)[0],
                "precision": _date_precision(sorted(dates)[0]),
                "urls": urls,
                "score": score,
            }
        )

    out.sort(key=lambda x: (-x.get("score", 0), x.get("date", "9999-99-99"), x.get("title", "")))
    if out:
        return out[:limit]
    # 構造化データで見つからない場合はWikipediaの初演記述を探索
    wiki_fallback = _wiki_premiere_candidates(work_title, composer_name, limit=limit)
    return wiki_fallback[:limit]

@st.cache_data(ttl=86400)
def search_premiere_candidates_from_work(
    work_id: str,
    work_title: str = "",
    composer_name: str = "",
    limit: int = 8,
) -> list[dict]:
    """MusicBrainz Work IDを起点に初演候補を収集（URL付き）。"""
    wid = (work_id or "").strip()
    if not wid:
        return search_premiere_candidates(work_title, composer_name, limit=limit)

    def _from_qid(qid: str) -> list[dict]:
        entity = _wikidata_entity(qid)
        if not entity:
            return []
        claims = entity.get("claims") or {}
        dates = []
        for c in (claims.get("P1191") or []):
            val = ((((c.get("mainsnak") or {}).get("datavalue") or {}).get("value")) or {}).get("time")
            dt = _format_wikidata_time(val)
            if dt:
                dates.append(dt)
        if not dates:
            return []
        labels = entity.get("labels") or {}
        title = ((labels.get("ja") or {}).get("value") or (labels.get("en") or {}).get("value") or qid).strip()
        sitelinks = entity.get("sitelinks") or {}
        ja_title = (sitelinks.get("jawiki") or {}).get("title")
        en_title = (sitelinks.get("enwiki") or {}).get("title")
        urls = []
        if ja_title:
            urls.append(f"https://ja.wikipedia.org/wiki/{quote(ja_title)}")
        if en_title:
            urls.append(f"https://en.wikipedia.org/wiki/{quote(en_title)}")
        if not urls:
            urls.append(f"https://www.wikidata.org/wiki/{qid}")
        return [{
            "qid": qid,
            "title": title,
            "date": sorted(dates)[0],
            "precision": _date_precision(sorted(dates)[0]),
            "urls": urls,
            "score": 1000,  # Work ID由来は最優先
        }]

    try:
        time.sleep(1.1)
        wres = requests.get(
            f"https://musicbrainz.org/ws/2/work/{wid}",
            params={"inc": "url-rels", "fmt": "json"},
            headers=MB_HEADERS,
            timeout=DEFAULT_TIMEOUT,
        )
        if wres.status_code == 400:
            time.sleep(1.1)
            wres = requests.get(
                f"https://musicbrainz.org/ws/2/work/{wid}",
                params={"fmt": "json"},
                headers=MB_HEADERS,
                timeout=DEFAULT_TIMEOUT,
            )
        if wres.status_code == 200:
            work_data = wres.json()
            wiki_urls, qid = _extract_mb_wiki_relations(work_data.get("relations", []))
            if not qid:
                for wurl in wiki_urls:
                    _, qid_from_wiki = _wiki_image_from_page(wurl)
                    if qid_from_wiki:
                        qid = qid_from_wiki
                        break
            out = _from_qid(qid) if qid else []
            if out:
                return out[:limit]
    except Exception:
        pass

    # Work IDで取れない場合のみ、従来検索へフォールバック
    out = search_premiere_candidates(work_title, composer_name, limit=limit)
    return out[:limit]


# ============================================================
# IGDB（ゲーム）
# ============================================================
@st.cache_data(ttl=3600)
def get_igdb_token() -> str:
    """TwitchからIGDB用アクセストークンを取得（1時間キャッシュ）"""
    res = requests.post(
        "https://id.twitch.tv/oauth2/token",
        params={
            "client_id":     IGDB_CLIENT_ID,
            "client_secret": IGDB_CLIENT_SECRET,
            "grant_type":    "client_credentials",
        }
    )
    if res.status_code == 200:
        return res.json().get("access_token", "")
    return ""

def search_games(query: str) -> list:
    def _norm(s: str) -> str:
        return re.sub(r"[\s:\-_'\"!！?？・、。]+", "", (s or "").lower())

    def _title_rank_score(title: str, base_queries: list[str], en_hint: str = "") -> int:
        t = _norm(title)
        score = 0
        for qx in base_queries:
            qn = _norm(qx)
            if not qn:
                continue
            if t == qn:
                score += 300
            elif t.startswith(qn):
                score += 220
            elif qn in t:
                score += 140
        if en_hint:
            en = _norm(en_hint)
            if t == en:
                score += 280
            elif t.startswith(en):
                score += 200
            elif en in t:
                score += 120
        bad_words = [
            "patch", "mod", "multiplayer", "mottzilla", "unreal engine",
            "bonus disc", "master quest", "second wind", "netherforce",
            "expansion pass", "dlc", "season pass", "bundle", "collector's edition",
            "definitive edition", "complete edition",
        ]
        lowered = (title or "").lower()
        for bw in bad_words:
            if bw in lowered:
                score -= 180
        return score

    def _jp_game_query_variants(text: str) -> list[str]:
        q = (text or "").strip()
        if not q:
            return []
        variants = [q]
        compact = re.sub(r"\s+", "", q)
        if compact != q:
            variants.append(compact)
        spaced = q
        for src, dst in [("オブザ", " オブ ザ "), ("オブ", " オブ "), ("ザ", " ザ ")]:
            spaced = spaced.replace(src, dst)
        spaced = re.sub(r"\s+", " ", spaced).strip()
        if spaced and spaced != q:
            variants.append(spaced)
        out, seen = [], set()
        for v in variants:
            if v and v not in seen:
                out.append(v)
                seen.add(v)
        return out

    def _search_igdb_once(q: str, headers: dict) -> list:
        safe_q = (q or "").replace('"', "").strip()
        if not safe_q:
            return []
        fields = "name,alternative_names.name,alternative_names.comment,game_localizations.name,game_localizations.region,cover.url,artworks.url,screenshots.url,first_release_date,genres.name,involved_companies.company.name,involved_companies.developer,involved_companies.publisher,platforms.name,summary,total_rating_count,rating,category"
        bodies = [
            f'search "{safe_q}"; fields {fields}; limit 100;',
            f'fields {fields}; where name ~ *"{safe_q}"*; limit 100;',
            f'fields {fields}; where alternative_names.name ~ *"{safe_q}"*; limit 100;',
        ]
        if _contains_japanese(safe_q):
            # 日本語検索は title だけだと漏れるので、別名/ローカライズ名でも探索
            bodies.extend([
                f'fields {fields}; where game_localizations.name ~ *"{safe_q}"*; limit 100;',
            ])
        raw_items = []
        for body in bodies:
            res = requests.post("https://api.igdb.com/v4/games", headers=headers, data=body, timeout=DEFAULT_TIMEOUT)
            if res.status_code != 200:
                continue
            raw_items.extend(res.json() or [])
            if raw_items:
                break
        if not raw_items:
            return []
        rows = []
        seen_row = set()
        for item in raw_items:
            gid = item.get("id")
            if gid in seen_row:
                continue
            seen_row.add(gid)
            cover_url = ""
            if item.get("cover", {}).get("url"):
                cover_url = "https:" + item["cover"]["url"].replace("t_thumb", "t_cover_big")
            artwork_urls = []
            for a in item.get("artworks", []) or []:
                u = (a.get("url") or "").strip()
                if u:
                    artwork_urls.append(("https:" + u).replace("t_thumb", "t_cover_big"))
            screenshot_urls = []
            for s in item.get("screenshots", []) or []:
                u = (s.get("url") or "").strip()
                if u:
                    screenshot_urls.append(("https:" + u).replace("t_thumb", "t_cover_big"))
            release_year = ""
            if item.get("first_release_date"):
                release_year = datetime.utcfromtimestamp(item["first_release_date"]).strftime("%Y-%m-%d")
            genres = [g["name"] for g in item.get("genres", [])]
            platforms = [p.get("name", "") for p in item.get("platforms", []) if p.get("name")]
            developer, publisher = "", ""
            for c in item.get("involved_companies", []):
                name = c.get("company", {}).get("name", "")
                if c.get("developer"):
                    developer = name
                if c.get("publisher"):
                    publisher = name
            alt_titles = [a.get("name", "") for a in item.get("alternative_names", []) if isinstance(a, dict) and a.get("name")]
            jp_name, jp_source, jp_conf = _extract_jp_name_from_igdb_item(item)
            rows.append({
                "id":          item["id"],
                "title":       item.get("name", ""),
                "jp_title":    jp_name,
                "jp_source":   jp_source,
                "jp_confidence": jp_conf,
                "cover_url":   cover_url,
                "artwork_urls": _dedupe_keep_order(artwork_urls),
                "screenshot_urls": _dedupe_keep_order(screenshot_urls),
                "release":     release_year,
                "genres":      genres,
                "developer":   developer,
                "publisher":   publisher,
                "media_type":  "game",
                "platforms":   normalize_platform_names(platforms),
                "rating_count": int(item.get("total_rating_count") or 0),
                "rating": float(item.get("rating") or 0.0),
                "category": int(item.get("category") or -1),
                "alt_titles":  alt_titles,
            })
        return rows

    token = get_igdb_token()
    if not token:
        return []
    headers = {
        "Client-ID":     IGDB_CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }
    q = (query or "").strip()
    if not q:
        return []
    queries = [q]
    en_hint = ""
    if re.search(r"[\u3040-\u30ff\u3400-\u9fff]", q):
        for qv in _jp_game_query_variants(q):
            if qv not in queries:
                queries.append(qv)
        # 日本語クエリは、Wikipedia言語リンクから英題候補を複数取り込む
        en_title_candidates = _wikipedia_en_title_candidates_from_japanese(q, limit=8)
        # さらにシリーズ候補のENも合流（固有名詞辞書に依存しない）
        series_candidates = search_game_series_candidates(q, limit=8)
        for sc in series_candidates:
            en_s = (sc.get("en") or "").strip()
            if en_s and en_s not in en_title_candidates:
                en_title_candidates.append(en_s)
        if en_title_candidates:
            en_hint = en_title_candidates[0]
        # 旧フォールバック（単一候補）
        if not en_hint:
            for qv in _jp_game_query_variants(q):
                en_try = _wikipedia_en_title_from_japanese(qv)
                if en_try:
                    en_hint = en_try
                    break
        if en_hint and en_hint not in queries:
            queries.append(en_hint)
            for c in _build_wiki_title_candidates(en_hint):
                if c not in queries:
                    queries.append(c)
        for en_title in en_title_candidates:
            if en_title not in queries:
                queries.append(en_title)
            for c in _build_wiki_title_candidates(en_title):
                if c not in queries:
                    queries.append(c)
    if q.lower().startswith("the "):
        queries.append(q[4:].strip())
    all_results, seen = [], set()
    for q_try in queries:
        for row in _search_igdb_once(q_try, headers):
            gid = row.get("id")
            if gid in seen:
                continue
            seen.add(gid)
            all_results.append(row)
            if len(all_results) >= 220:
                break
        if len(all_results) >= 220:
            break
    # 1件しか取れず、かつ特装/同梱系なら本編候補を追加探索
    if len(all_results) == 1:
        only = all_results[0]
        if _game_variant_label(only.get("title", "")) != "本編候補":
            for bq in _game_base_title_candidates(only.get("title", "")):
                for row in _search_igdb_once(bq, headers):
                    gid = row.get("id")
                    if gid in seen:
                        continue
                    seen.add(gid)
                    all_results.append(row)
    if all_results:
        base_queries = [q] + [x for x in queries if x != q]
        def _row_sort_key(r: dict):
            title = r.get("title", "")
            score = _title_rank_score(title, base_queries, en_hint=en_hint)
            cat = r.get("category", -1)
            cat_bonus = 30 if cat == 0 else (10 if cat in (8, 9) else 0)
            pop = min(int(r.get("rating_count") or 0), 5000) // 25
            rt = int(float(r.get("rating") or 0.0))
            rel_bonus = 20 if r.get("release") else 0
            return -(score + cat_bonus + pop + rt + rel_bonus)
        all_results = sorted(all_results, key=_row_sort_key)
    return all_results

def fetch_game_by_id(game_id: int) -> dict | None:
    token = get_igdb_token()
    if not token:
        return None
    headers = {
        "Client-ID":     IGDB_CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }
    body = (
        "fields name,cover.url,first_release_date,genres.name,"
        "involved_companies.company.name,involved_companies.developer,involved_companies.publisher,"
        "summary,alternative_names.name,alternative_names.comment,game_localizations.name,game_localizations.region,"
        "total_rating_count,rating,category;"
        f" where id = {int(game_id)};"
    )
    res = requests.post("https://api.igdb.com/v4/games", headers=headers, data=body, timeout=DEFAULT_TIMEOUT)
    if res.status_code != 200:
        return None
    items = res.json()
    if not items:
        return None
    item = items[0]
    jp_name, jp_source, jp_conf = _extract_jp_name_from_igdb_item(item)
    cover_url = ""
    if item.get("cover", {}).get("url"):
        cover_url = "https:" + item["cover"]["url"].replace("t_thumb", "t_cover_big")
    release_year = ""
    if item.get("first_release_date"):
        release_year = datetime.utcfromtimestamp(item["first_release_date"]).strftime("%Y-%m-%d")
    genres = [g["name"] for g in item.get("genres", [])]
    developer, publisher = "", ""
    for c in item.get("involved_companies", []):
        name = c.get("company", {}).get("name", "")
        if c.get("developer"):   developer = name
        if c.get("publisher"):   publisher = name
    return {
        "id":          item["id"],
        "title":       item.get("name", ""),
        "jp_title":    jp_name,
        "jp_source":   jp_source,
        "jp_confidence": jp_conf,
        "cover_url":   cover_url,
        "release":     release_year,
        "genres":      genres,
        "developer":   developer,
        "publisher":   publisher,
        "rating_count": int(item.get("total_rating_count") or 0),
        "rating": float(item.get("rating") or 0.0),
        "category": int(item.get("category") or -1),
        "media_type":  "game",
    }

# ============================================================
# AniList API（アニメ）
# ============================================================
ANILIST_URL = "https://graphql.anilist.co"

def search_anime(query: str) -> list:
    """AniList GraphQL APIでアニメ検索"""
    gql = """
    query ($search: String) {
      Page(perPage: 20) {
        media(search: $search, type: ANIME, sort: POPULARITY_DESC) {
          id
          title { native romaji english }
          coverImage { large }
          genres
          startDate { year month day }
          averageScore
          staff(perPage: 5, sort: RELEVANCE) {
            edges { role node { name { full } } }
          }
        }
      }
    }
    """
    try:
        res = requests.post(
            ANILIST_URL,
            json={"query": gql, "variables": {"search": query}},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if res.status_code != 200:
            st.warning(f"⚠️ AniList API {res.status_code}")
            return []
        media_list = res.json().get("data", {}).get("Page", {}).get("media", [])
        results = []
        for m in media_list:
            title    = m["title"]
            native   = title.get("native") or ""
            romaji   = title.get("romaji") or ""
            english  = title.get("english") or ""
            sd       = m.get("startDate", {})
            release  = ""
            if sd.get("year"):
                mm = sd.get("month") or 1
                dd = sd.get("day") or 1
                release = f"{sd['year']}-{mm:02d}-{dd:02d}"
            # 監督抽出（Director / Series Director）
            director = ""
            for edge in m.get("staff", {}).get("edges", []):
                if edge.get("role", "") in ("Director", "Series Director"):
                    director = edge["node"]["name"]["full"]
                    break
            results.append({
                "id":          m["id"],
                "title":       native or romaji,
                "title_romaji": romaji,
                "title_en":    english,
                "cover_url":   m.get("coverImage", {}).get("large", ""),
                "release":     release,
                "genres":      m.get("genres", []),
                "director":    director,
                "score":       round(m["averageScore"] / 10, 1) if m.get("averageScore") else None,
                "media_type":  "anime",
            })
        return results
    except Exception as e:
        st.warning(f"⚠️ AniList API エラー: {e}")
        return []

def fetch_anime_by_id(anilist_id: int) -> dict | None:
    gql = """
    query ($id: Int) {
      Media(id: $id, type: ANIME) {
        id
        title { native romaji english }
        coverImage { large }
        genres
        startDate { year month day }
        averageScore
        staff(perPage: 5, sort: RELEVANCE) {
          edges { role node { name { full } } }
        }
      }
    }
    """
    try:
        res = requests.post(
            ANILIST_URL,
            json={"query": gql, "variables": {"id": int(anilist_id)}},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if res.status_code != 200:
            return None
        m = res.json().get("data", {}).get("Media")
        if not m:
            return None
        title = m.get("title") or {}
        native = title.get("native") or ""
        romaji = title.get("romaji") or ""
        english = title.get("english") or ""
        sd = m.get("startDate", {}) or {}
        release = ""
        if sd.get("year"):
            mm = sd.get("month") or 1
            dd = sd.get("day") or 1
            release = f"{sd['year']}-{mm:02d}-{dd:02d}"
        director = ""
        for edge in m.get("staff", {}).get("edges", []):
            if edge.get("role", "") in ("Director", "Series Director"):
                director = edge["node"]["name"]["full"]
                break
        return {
            "id":          m["id"],
            "title":       native or romaji,
            "title_romaji": romaji,
            "title_en":    english,
            "cover_url":   (m.get("coverImage") or {}).get("large", ""),
            "release":     release,
            "genres":      m.get("genres", []),
            "director":    director,
            "score":       round(m["averageScore"] / 10, 1) if m.get("averageScore") else None,
            "media_type":  "anime",
        }
    except Exception:
        return None

# ============================================================
# iTunes Search API（音楽アルバム）
# ============================================================
def search_albums(query: str, artist: str = None) -> list:
    search_term = f"{artist} {query}".strip() if artist else query
    res = requests.get(
        "https://itunes.apple.com/search",
        params={
            "term":    search_term,
            "media":   "music",
            "entity":  "album",
            "country": "JP",
            "lang":    "ja_jp",
            "limit":   20,
        },
        headers={"User-Agent": "ArteMis/1.0"},
    )
    if res.status_code != 200:
        return []
    results = []
    for item in res.json().get("results", []):
        cover_url = item.get("artworkUrl100", "").replace("100x100bb", "600x600bb")
        release   = (item.get("releaseDate", "") or "")[:10]
        results.append({
            "id":         item.get("collectionId", 0),
            "title":      item.get("collectionName", ""),
            "artist":     item.get("artistName", ""),
            "release":    release,
            "cover_url":  cover_url,
            "media_type": "album",
        })
    return results

def search_itunes_jp_album_title(title: str, artist: str = None) -> str:
    """iTunesで日本語タイトル候補を検索（見つからなければ空文字）"""
    def _has_japanese(text: str) -> bool:
        return bool(re.search(r"[\u3040-\u30ff\u4e00-\u9fff]", text or ""))

    search_term = " ".join(filter(None, [artist, title])).strip()
    if not search_term:
        return ""
    try:
        res = requests.get(
            "https://itunes.apple.com/search",
            params={
                "term":    search_term,
                "media":   "music",
                "entity":  "album",
                "country": "JP",
                "lang":    "ja_jp",
                "limit":   5,
            },
            headers={"User-Agent": "ArteMis/1.0"},
            timeout=DEFAULT_TIMEOUT,
        )
        if res.status_code != 200:
            return ""
        items = res.json().get("results", [])
        if not items:
            return ""
        # まずアーティスト一致候補を優先
        scoped = items
        if artist:
            artist_l = artist.strip().lower()
            matched = [x for x in items if artist_l and artist_l in (x.get("artistName", "").lower())]
            if matched:
                scoped = matched
        # 日本語を含むタイトルを優先
        for x in scoped:
            name = x.get("collectionName", "") or ""
            if name and _has_japanese(name):
                return name
        # 次点: 非空の先頭候補
        for x in scoped:
            name = x.get("collectionName", "") or ""
            if name:
                return name
        return ""
    except Exception:
        return ""


def _build_wiki_title_candidates(title: str) -> list[str]:
    """Wikipedia検索向けにゲームタイトルを簡易正規化して候補を作る"""
    t = (title or "").strip()
    if not t:
        return []
    candidates = [t]
    # 末尾の括弧補足を除去
    t1 = re.sub(r"\s*[\(\[].*?[\)\]]\s*$", "", t).strip()
    if t1 and t1 != t:
        candidates.append(t1)
    # バンドル/複数タイトルを分割
    splitters = [" and ", " + ", " / ", " & ", " Bundle", " Pack", " DLC", " Expansion", " Complete", " Edition", " Collection"]
    for base in [t1 or t]:
        for sep in splitters:
            if sep in base:
                candidates.append(base.split(sep)[0].strip())
    # エディション系末尾（大小文字無視）
    t2 = re.sub(r"\s*-\s*[^-]*edition.*$", "", (t1 or t), flags=re.IGNORECASE).strip()
    if t2 and t2 != (t1 or t):
        candidates.append(t2)
    t3 = re.sub(r"\b(collector'?s|complete|definitive|ultimate|deluxe|game of the year)\s+edition\b", "", (t1 or t), flags=re.IGNORECASE).strip(" -")
    if t3 and t3 != (t1 or t):
        candidates.append(t3)
    # サブタイトルを外した短縮版
    if ":" in (t1 or t):
        candidates.append((t1 or t).split(":")[0].strip())
    # 重複排除（順序保持）
    uniq = []
    seen = set()
    for c in candidates:
        if c and c not in seen:
            uniq.append(c)
            seen.add(c)
    return uniq


def _wikidata_ja_label_from_en_wikipedia_title(title: str) -> str:
    """EN WikipediaタイトルからWikidataを引いてJAラベル/JAサイトリンクを取得"""
    t = (title or "").strip()
    if not t:
        return ""
    try:
        r = wikimedia_get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "prop": "pageprops",
                "titles": t,
                "redirects": 1,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if r.status_code != 200:
            return ""
        pages = (r.json().get("query") or {}).get("pages") or {}
        qid = ""
        for p in pages.values():
            qid = ((p.get("pageprops") or {}).get("wikibase_item") or "").strip()
            if qid:
                break
        if not qid:
            return ""
        dres = wikimedia_get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            timeout=DEFAULT_TIMEOUT,
        )
        if dres.status_code != 200:
            return ""
        entity = ((dres.json().get("entities") or {}).get(qid)) or {}
        sitelinks = entity.get("sitelinks") or {}
        jawiki = ((sitelinks.get("jawiki") or {}).get("title") or "").strip()
        if jawiki:
            return jawiki
        labels = entity.get("labels") or {}
        ja_label = ((labels.get("ja") or {}).get("value") or "").strip()
        if ja_label:
            return ja_label
    except Exception:
        return ""
    return ""


def _wikidata_en_title_from_ja_wikipedia_title(title: str) -> str:
    t = (title or "").strip()
    if not t:
        return ""
    try:
        r = wikimedia_get(
            "https://ja.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "prop": "pageprops",
                "titles": t,
                "redirects": 1,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if r.status_code != 200:
            return ""
        pages = (r.json().get("query") or {}).get("pages") or {}
        qid = ""
        for p in pages.values():
            qid = ((p.get("pageprops") or {}).get("wikibase_item") or "").strip()
            if qid:
                break
        if not qid:
            return ""
        dres = wikimedia_get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            timeout=DEFAULT_TIMEOUT,
        )
        if dres.status_code != 200:
            return ""
        entity = ((dres.json().get("entities") or {}).get(qid)) or {}
        sitelinks = entity.get("sitelinks") or {}
        return ((sitelinks.get("enwiki") or {}).get("title") or "").strip()
    except Exception:
        return ""


def search_wikipedia_jp_title(title: str) -> str:
    """Wikipediaの言語リンク/検索から日本語タイトルを取得（見つからなければ空文字）"""
    candidates = _build_wiki_title_candidates(title)
    if not candidates:
        return ""
    try:
        for cand in candidates:
            # 1) まずENタイトルをそのまま引いて langlinks を確認（最も精度が高い）
            direct_res = wikimedia_get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "prop": "langlinks",
                    "lllang": "ja",
                    "titles": cand,
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if direct_res.status_code == 200:
                pages = direct_res.json().get("query", {}).get("pages", {})
                for page in pages.values():
                    langlinks = page.get("langlinks", [])
                    if langlinks:
                        return langlinks[0].get("*", "") or ""

            # 2) Wikidata経由でJAタイトルを取得（langlinks欠損時の汎用フォールバック）
            wd_jp = _wikidata_ja_label_from_en_wikipedia_title(cand)
            if wd_jp:
                return wd_jp

            search_res = wikimedia_get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action":  "query",
                    "list":    "search",
                    "srsearch": cand,
                    "srlimit":  5,
                    "format":  "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if search_res.status_code == 200:
                items = search_res.json().get("query", {}).get("search", [])
                for itm in items:
                    page_title = itm.get("title")
                    if page_title:
                        ll_res = wikimedia_get(
                            "https://en.wikipedia.org/w/api.php",
                            params={
                                "action": "query",
                                "prop":   "langlinks",
                                "lllang": "ja",
                                "titles": page_title,
                                "format": "json",
                            },
                            timeout=DEFAULT_TIMEOUT,
                        )
                        if ll_res.status_code == 200:
                            pages = ll_res.json().get("query", {}).get("pages", {})
                            for page in pages.values():
                                langlinks = page.get("langlinks", [])
                                if langlinks:
                                    return langlinks[0].get("*", "") or ""

            ja_res = wikimedia_get(
                "https://ja.wikipedia.org/w/api.php",
                params={
                    "action":  "query",
                    "list":    "search",
                    "srsearch": cand,
                    "srlimit":  5,
                    "format":  "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if ja_res.status_code == 200:
                items = ja_res.json().get("query", {}).get("search", [])
                if items:
                    return items[0].get("title", "") or ""
    except Exception:
        return ""
    return ""

def _wikipedia_en_title_from_japanese(title: str) -> str:
    q = (title or "").strip()
    if not q:
        return ""
    try:
        ja_res = wikimedia_get(
            "https://ja.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "list": "search",
                "srsearch": q,
                "srlimit": 5,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if ja_res.status_code != 200:
            return ""
        items = ja_res.json().get("query", {}).get("search", [])
        if not items:
            return ""
        for item in items:
            page_title = (item.get("title") or "").strip()
            if not page_title:
                continue
            ll_res = wikimedia_get(
                "https://ja.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "prop": "langlinks",
                    "lllang": "en",
                    "titles": page_title,
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if ll_res.status_code != 200:
                continue
            pages = ll_res.json().get("query", {}).get("pages", {})
            for page in pages.values():
                langlinks = page.get("langlinks", [])
                if langlinks:
                    return (langlinks[0].get("*") or "").strip()
    except Exception:
        return ""
    return ""

def _wikipedia_en_title_candidates_from_japanese(title: str, limit: int = 8) -> list[str]:
    q = (title or "").strip()
    if not q:
        return []
    out, seen = [], set()
    def _collect_from_ja_search(sr: str):
        try:
            ja_res = wikimedia_get(
                "https://ja.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": sr,
                    "srlimit": max(1, min(limit, 10)),
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if ja_res.status_code != 200:
                return
            items = ja_res.json().get("query", {}).get("search", [])
            for item in items:
                page_title = (item.get("title") or "").strip()
                if not page_title:
                    continue
                ll_res = wikimedia_get(
                    "https://ja.wikipedia.org/w/api.php",
                    params={
                        "action": "query",
                        "prop": "langlinks",
                        "lllang": "en",
                        "titles": page_title,
                        "format": "json",
                    },
                    timeout=DEFAULT_TIMEOUT,
                )
                if ll_res.status_code != 200:
                    continue
                pages = ll_res.json().get("query", {}).get("pages", {})
                found = False
                for page in pages.values():
                    langlinks = page.get("langlinks", [])
                    for ll in langlinks:
                        en = (ll.get("*") or "").strip()
                        if en and en not in seen:
                            out.append(en)
                            seen.add(en)
                            found = True
                            if len(out) >= limit:
                                return
                if not found:
                    en_wd = _wikidata_en_title_from_ja_wikipedia_title(page_title)
                    if en_wd and en_wd not in seen:
                        out.append(en_wd)
                        seen.add(en_wd)
                        if len(out) >= limit:
                            return
        except Exception:
            return

    def _collect_from_opensearch(sr: str):
        try:
            ores = wikimedia_get(
                "https://ja.wikipedia.org/w/api.php",
                params={
                    "action": "opensearch",
                    "search": sr,
                    "limit": max(1, min(limit, 10)),
                    "namespace": 0,
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if ores.status_code != 200:
                return
            arr = ores.json() or []
            titles = arr[1] if len(arr) > 1 else []
            for page_title in titles:
                page_title = (page_title or "").strip()
                if not page_title:
                    continue
                ll_res = wikimedia_get(
                    "https://ja.wikipedia.org/w/api.php",
                    params={
                        "action": "query",
                        "prop": "langlinks",
                        "lllang": "en",
                        "titles": page_title,
                        "format": "json",
                    },
                    timeout=DEFAULT_TIMEOUT,
                )
                if ll_res.status_code != 200:
                    continue
                pages = ll_res.json().get("query", {}).get("pages", {})
                found = False
                for page in pages.values():
                    for ll in page.get("langlinks", []) or []:
                        en = (ll.get("*") or "").strip()
                        if en and en not in seen:
                            out.append(en)
                            seen.add(en)
                            found = True
                            if len(out) >= limit:
                                return
                if not found:
                    en_wd = _wikidata_en_title_from_ja_wikipedia_title(page_title)
                    if en_wd and en_wd not in seen:
                        out.append(en_wd)
                        seen.add(en_wd)
                        if len(out) >= limit:
                            return
        except Exception:
            return

    bases = _dedupe_keep_order([q, q.replace(" ", ""), q.replace("　", "")])
    for b in bases:
        _collect_from_ja_search(b)
        _collect_from_opensearch(b)
        if len(out) >= limit:
            break
    if len(out) < limit:
        suffixes = ["ゲーム", "シリーズ", "の伝説", "伝説", "作品", "ビデオゲーム"]
        for b in bases:
            for suffix in suffixes:
                _collect_from_ja_search(f"{b} {suffix}")
                _collect_from_ja_search(f"{b}{suffix}")
                _collect_from_opensearch(f"{b} {suffix}")
                _collect_from_opensearch(f"{b}{suffix}")
                if len(out) >= limit:
                    break
            if len(out) >= limit:
                break
    if out:
        return out
    # Wikipedia検索で拾えない短い別称向け: Wikidata検索 -> enwiki sitelink
    try:
        wres = wikimedia_get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbsearchentities",
                "search": q,
                "language": "ja",
                "type": "item",
                "limit": max(1, min(limit, 10)),
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if wres.status_code == 200:
            for item in wres.json().get("search", []) or []:
                qid = (item.get("id") or "").strip()
                if not qid:
                    continue
                dres = wikimedia_get(
                    f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
                    timeout=DEFAULT_TIMEOUT,
                )
                if dres.status_code != 200:
                    continue
                ent = ((dres.json().get("entities") or {}).get(qid)) or {}
                sitelinks = ent.get("sitelinks") or {}
                enwiki = ((sitelinks.get("enwiki") or {}).get("title") or "").strip()
                if enwiki and enwiki not in seen:
                    out.append(enwiki)
                    seen.add(enwiki)
                    if len(out) >= limit:
                        return out
    except Exception:
        return out
    if out:
        return out
    # 最終フォールバック: 英語側検索でも候補取得を試す
    try:
        eres = wikimedia_get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbsearchentities",
                "search": q,
                "language": "en",
                "type": "item",
                "limit": max(1, min(limit, 10)),
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if eres.status_code == 200:
            for item in eres.json().get("search", []) or []:
                label = (item.get("label") or "").strip()
                if label and label not in seen:
                    out.append(label)
                    seen.add(label)
                    if len(out) >= limit:
                        return out
    except Exception:
        return out
    return out

def search_game_series_candidates(query: str, limit: int = 8) -> list[dict]:
    q = (query or "").strip()
    if not q:
        return []
    seeds = _expand_game_query_aliases(q)
    series_queries = []
    for s in seeds:
        series_queries.extend([s, f"{s} シリーズ", f"{s} の伝説", f"{s} ゲーム", s.replace(" ", ""), s.replace("　", "")])
    series_queries = _dedupe_keep_order(series_queries)
    out = []
    seen = set()
    for sq in series_queries:
        try:
            res = wikimedia_get(
                "https://ja.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": sq,
                    "srlimit": limit,
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if res.status_code != 200:
                continue
            items = res.json().get("query", {}).get("search", []) or []
            for item in items:
                ja_title = (item.get("title") or "").strip()
                if not ja_title:
                    continue
                ll = wikimedia_get(
                    "https://ja.wikipedia.org/w/api.php",
                    params={
                        "action": "query",
                        "prop": "langlinks",
                        "lllang": "en",
                        "titles": ja_title,
                        "format": "json",
                    },
                    timeout=DEFAULT_TIMEOUT,
                )
                if ll.status_code != 200:
                    continue
                pages = ll.json().get("query", {}).get("pages", {}) or {}
                for p in pages.values():
                    for l in p.get("langlinks", []) or []:
                        en_title = (l.get("*") or "").strip()
                        if not en_title:
                            continue
                        key = (ja_title, en_title)
                        if key in seen:
                            continue
                        seen.add(key)
                        out.append({"ja": ja_title, "en": en_title})
                        if len(out) >= limit:
                            return out
        except Exception:
            continue
    return out

def search_game_jp_title_precise(en_title: str) -> str:
    title = (en_title or "").strip()
    if not title:
        return ""
    learned = _lookup_game_jp_learned(title)
    if learned:
        return learned
    key = _norm_game_title_key(title)
    if key in GAME_TITLE_JP_MANUAL:
        return GAME_TITLE_JP_MANUAL[key]
    # 版情報付きタイトルを正規化して再判定
    base = re.sub(r"\s*-\s*[^-]*(edition|bundle|collection|pack).*$", "", title, flags=re.IGNORECASE).strip()
    base_key = _norm_game_title_key(base)
    if base_key in GAME_TITLE_JP_MANUAL:
        return GAME_TITLE_JP_MANUAL[base_key]
    jp = search_wikipedia_jp_title(title)
    if jp:
        return jp
    # Wikidata label fallback
    try:
        wres = wikimedia_get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbsearchentities",
                "search": title,
                "language": "en",
                "type": "item",
                "limit": 5,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if wres.status_code != 200:
            return ""
        for item in wres.json().get("search", []) or []:
            qid = (item.get("id") or "").strip()
            if not qid:
                continue
            dres = wikimedia_get(
                f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
                timeout=DEFAULT_TIMEOUT,
            )
            if dres.status_code != 200:
                continue
            entity = ((dres.json().get("entities") or {}).get(qid)) or {}
            labels = entity.get("labels") or {}
            ja_label = ((labels.get("ja") or {}).get("value") or "").strip()
            if ja_label:
                return ja_label
    except Exception:
        return ""
    return ""


@st.cache_data(ttl=3600)
def resolve_game_jp_titles_bulk(en_titles: tuple[str, ...]) -> dict[str, str]:
    titles = [t.strip() for t in en_titles if (t or "").strip()]
    if not titles:
        return {}
    out: dict[str, str] = {}
    out_norm: dict[str, str] = {}
    # 1) learned cache
    for t in titles:
        learned = _lookup_game_jp_learned(t)
        if learned:
            out[t] = learned
            out_norm[_norm_game_title_key(t)] = learned
    unresolved = [t for t in titles if t not in out]
    if not unresolved:
        return out
    # 2) Wikipedia langlinks一括（高速）
    try:
        # URL長で取りこぼさないように小さめチャンクで処理
        chunk = 5
        for i in range(0, len(unresolved), chunk):
            part = unresolved[i:i + chunk]
            res = wikimedia_get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "prop": "langlinks",
                    "lllang": "ja",
                    "lllimit": "max",
                    "redirects": 1,
                    "titles": "|".join(part),
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if res.status_code != 200:
                continue
            pages = (res.json().get("query") or {}).get("pages") or {}
            for p in pages.values():
                en_title = (p.get("title") or "").strip()
                if not en_title:
                    continue
                ll = p.get("langlinks") or []
                if not ll:
                    continue
                ja = (ll[0].get("*") or "").strip()
                if ja:
                    out[en_title] = ja
                    out_norm[_norm_game_title_key(en_title)] = ja
    except Exception:
        pass
    # 3) 正規化キーで再照合
    for t in titles:
        if t in out:
            continue
        ja = out_norm.get(_norm_game_title_key(t), "")
        if ja:
            out[t] = ja
    # 4) Wikidata sitelinks/labels（ENタイトル直指定）
    still = [t for t in titles if t not in out]
    try:
        # titles結合が長いと失敗しやすいので小分け
        chunk = 5
        for i in range(0, len(still), chunk):
            part = still[i:i + chunk]
            wres = wikimedia_get(
                "https://www.wikidata.org/w/api.php",
                params={
                    "action": "wbgetentities",
                    "sites": "enwiki",
                    "titles": "|".join(part),
                    "props": "labels|sitelinks",
                    "languages": "ja",
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if wres.status_code != 200:
                continue
            entities = (wres.json().get("entities") or {})
            for ent in entities.values():
                sitelinks = ent.get("sitelinks") or {}
                enwiki = ((sitelinks.get("enwiki") or {}).get("title") or "").strip()
                if not enwiki:
                    continue
                jawiki = ((sitelinks.get("jawiki") or {}).get("title") or "").strip()
                ja_label = (((ent.get("labels") or {}).get("ja") or {}).get("value") or "").strip()
                ja = jawiki or ja_label
                if ja:
                    out[enwiki] = ja
                    out_norm[_norm_game_title_key(enwiki)] = ja
        for t in titles:
            if t in out:
                continue
            ja = out_norm.get(_norm_game_title_key(t), "")
            if ja:
                out[t] = ja
    except Exception:
        pass
    # 5) それでも未解決なら限定件数で個別精査（精度優先）
    still = [t for t in titles if t not in out]
    for t in still[:30]:
        try:
            ja = search_game_jp_title_precise(t)
            if ja:
                out[t] = ja
        except Exception:
            continue
    return out

@st.cache_data(ttl=86400)
def search_game_jp_title_from_query(jp_query: str, en_title: str = "") -> str:
    q = (jp_query or "").strip()
    if not q:
        return ""
    probes = [q, f"{q} ゲーム"]
    probes = [p for p in _dedupe_keep_order(probes) if p]
    try:
        for p in probes:
            res = wikimedia_get(
                "https://ja.wikipedia.org/w/api.php",
                params={
                    "action": "query",
                    "list": "search",
                    "srsearch": p,
                    "srlimit": 3,
                    "format": "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if res.status_code != 200:
                continue
            items = res.json().get("query", {}).get("search", []) or []
            for item in items:
                t = (item.get("title") or "").strip()
                if not t:
                    continue
                if not en_title:
                    return t
                # ENタイトルがある場合は逆言語リンクで照合し、誤マッチを避ける
                ll = wikimedia_get(
                    "https://ja.wikipedia.org/w/api.php",
                    params={
                        "action": "query",
                        "prop": "langlinks",
                        "lllang": "en",
                        "titles": t,
                        "format": "json",
                    },
                    timeout=DEFAULT_TIMEOUT,
                )
                if ll.status_code != 200:
                    continue
                pages = ll.json().get("query", {}).get("pages", {}) or {}
                for page in pages.values():
                    for l in (page.get("langlinks") or []):
                        en_link = (l.get("*") or "").strip().lower()
                        en_ref = (en_title or "").strip().lower()
                        if not en_link or not en_ref:
                            continue
                        if en_link == en_ref or en_ref in en_link or en_link in en_ref:
                            return t
    except Exception:
        return ""
    return ""


def diagnose_game_jp_resolution(en_title: str, jp_query: str = "") -> tuple[str, str]:
    """
    戻り値: (jp_title, reason)
    reason は jp_title が空の時のみ意味を持つ:
      - Wikipedia不可
      - Wikidata不可
      - 一致なし
    """
    title = (en_title or "").strip()
    if not title:
        return "", "一致なし"
    learned = _lookup_game_jp_learned(title)
    if learned:
        return learned, ""

    wiki_ok = True
    wd_ok = True

    # 1) Wikipedia langlinks（ENタイトル直指定）
    try:
        r = wikimedia_get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "prop": "langlinks",
                "lllang": "ja",
                "redirects": 1,
                "titles": title,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if r.status_code == 200:
            pages = (r.json().get("query") or {}).get("pages") or {}
            for p in pages.values():
                ll = p.get("langlinks") or []
                if ll:
                    jp = (ll[0].get("*") or "").strip()
                    if jp:
                        return jp, ""
        else:
            wiki_ok = False
    except Exception:
        wiki_ok = False

    # 2) Wikidata（EN wiki title -> jawiki/ja label）
    try:
        wres = wikimedia_get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbgetentities",
                "sites": "enwiki",
                "titles": title,
                "props": "labels|sitelinks",
                "languages": "ja",
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if wres.status_code == 200:
            entities = (wres.json().get("entities") or {})
            for ent in entities.values():
                sitelinks = ent.get("sitelinks") or {}
                jawiki = ((sitelinks.get("jawiki") or {}).get("title") or "").strip()
                ja_label = (((ent.get("labels") or {}).get("ja") or {}).get("value") or "").strip()
                if jawiki:
                    return jawiki, ""
                if ja_label:
                    return ja_label, ""
        else:
            wd_ok = False
    except Exception:
        wd_ok = False

    # 3) クエリヒント経由
    if jp_query:
        jp = search_game_jp_title_from_query(jp_query, title)
        if jp:
            return jp, ""

    if not wiki_ok:
        return "", "Wikipedia不可"
    if not wd_ok:
        return "", "Wikidata不可"
    return "", "一致なし"

@st.cache_data(ttl=86400)
def _wiki_page_image_from_title(title: str, lang: str = "ja") -> str:
    t = (title or "").strip()
    if not t:
        return ""
    try:
        res = wikimedia_get(
            f"https://{lang}.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "titles": t,
                "prop": "pageimages|pageprops",
                "piprop": "original|thumbnail",
                "pithumbsize": 1200,
                "format": "json",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        if res.status_code != 200:
            return ""
        pages = (res.json().get("query") or {}).get("pages") or {}
        for page in pages.values():
            pageprops = page.get("pageprops") or {}
            if pageprops.get("disambiguation") is not None:
                continue
            img = ((page.get("original") or {}).get("source")
                   or (page.get("thumbnail") or {}).get("source"))
            if img:
                return img
    except Exception:
        return ""
    return ""

def _contains_japanese(text: str) -> bool:
    return bool(re.search(r"[\u3040-\u30ff\u3400-\u9fff]", (text or "")))

def _dedupe_keep_order(seq: list[str]) -> list[str]:
    out, seen = [], set()
    for x in seq:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out

GAME_QUERY_ALIASES = {}

GAME_TITLE_JP_MANUAL = {}

def _norm_game_title_key(title: str) -> str:
    t = (title or "").strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t


def _plain_text_from_rich(prop: dict) -> str:
    vals = (prop or {}).get("rich_text", []) or []
    return "".join((x.get("plain_text") or "") for x in vals).strip()


def _title_text_from_prop(prop: dict) -> str:
    vals = (prop or {}).get("title", []) or []
    return "".join((x.get("plain_text") or "") for x in vals).strip()


def _resolve_game_jp_dict_schema() -> tuple[dict, str, str, str]:
    """
    returns: (db_props, jp_prop, en_prop, id_prop)
    """
    db_props: dict = {}
    if not NOTION_GAME_JP_DICT_DB_ID:
        return db_props, "", "", "IGDB_ID"
    db_meta = api_request("get", f"https://api.notion.com/v1/databases/{NOTION_GAME_JP_DICT_DB_ID}", headers=NOTION_HEADERS)
    if db_meta and db_meta.status_code == 200:
        db_props = (db_meta.json().get("properties", {}) or {})
    rt_props = [k for k, v in db_props.items() if (v or {}).get("type") == "rich_text"]
    jp_prop = next((k for k, v in db_props.items() if (v or {}).get("type") == "rich_text" and ("日本語" in k or "JP" in k.upper())), "")
    en_prop = next((k for k, v in db_props.items() if (v or {}).get("type") == "rich_text" and ("英語" in k or "EN" in k.upper())), "")
    if not jp_prop and rt_props:
        jp_prop = rt_props[0]
    if not en_prop:
        en_prop = rt_props[1] if len(rt_props) > 1 else (rt_props[0] if rt_props else "")
    id_prop = next((k for k, v in db_props.items() if (v or {}).get("type") == "number" and ("IGDB" in k.upper() or "ID" in k.upper())), "IGDB_ID")
    return db_props, jp_prop, en_prop, id_prop


def _query_game_jp_dict_rows() -> list[dict]:
    if not NOTION_GAME_JP_DICT_DB_ID:
        st.session_state["_game_jp_dict_last_error"] = "NOTION_GAME_JP_DICT_DB_ID が未設定"
        return []
    try:
        rows = _query_notion_database_all_service(api_request, NOTION_HEADERS, NOTION_GAME_JP_DICT_DB_ID) or []
        st.session_state["_game_jp_dict_last_error"] = ""
        return rows
    except Exception as e:
        st.session_state["_game_jp_dict_last_error"] = str(e)
        return []


def _pick_best_game_jp_dict_page(pages: list[dict], jp_prop: str, en_prop: str, id_prop: str) -> dict:
    def _score(page: dict) -> tuple[int, int, int]:
        props = page.get("properties", {}) or {}
        jp = _plain_text_from_rich(props.get(jp_prop, {})) if jp_prop else _plain_text_from_rich(props.get("日本語タイトル", {}))
        en = _plain_text_from_rich(props.get(en_prop, {})) if en_prop else _plain_text_from_rich(props.get("英語タイトル", {}))
        has_id = 1 if (props.get(id_prop, {}) or {}).get("number") is not None else 0
        edited = page.get("last_edited_time", "") or ""
        return (1 if jp else 0, 1 if en else 0, has_id * 10 + (1 if edited else 0))
    return sorted(pages, key=_score, reverse=True)[0]


def _dedupe_game_jp_dict_rows(igdb_id: int) -> tuple[str, int]:
    """
    Returns canonical page id and archived duplicate count.
    """
    rows = _query_game_jp_dict_rows()
    if not rows:
        return "", 0
    _, jp_prop, en_prop, id_prop = _resolve_game_jp_dict_schema()
    matched = []
    for p in rows:
        props = p.get("properties", {}) or {}
        v = (props.get(id_prop, {}) or {}).get("number")
        if v is not None and int(v) == int(igdb_id):
            matched.append(p)
    if not matched:
        return "", 0
    canonical = _pick_best_game_jp_dict_page(matched, jp_prop, en_prop, id_prop)
    archived = 0
    for p in matched:
        pid = p.get("id", "")
        if not pid or pid == canonical.get("id", ""):
            continue
        try:
            r = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"archived": True})
            if r is not None and r.status_code == 200:
                archived += 1
        except Exception:
            continue
    return canonical.get("id", ""), archived


def _dedupe_game_jp_dict_all(max_groups: int = 300) -> int:
    rows = _query_game_jp_dict_rows()
    if not rows:
        return 0
    _, jp_prop, en_prop, id_prop = _resolve_game_jp_dict_schema()
    grouped: dict[str, list[dict]] = {}
    for p in rows:
        props = p.get("properties", {}) or {}
        v = (props.get(id_prop, {}) or {}).get("number")
        en = _plain_text_from_rich(props.get(en_prop, {})) if en_prop else ""
        if not en:
            name_title = _title_text_from_prop(props.get("名前", {}))
            if ":" in name_title:
                en = name_title.split(":", 1)[1].strip()
        if v is not None:
            key = f"id:{int(v)}"
        elif en:
            key = f"en:{_norm_game_title_key(en)}"
        else:
            continue
        grouped.setdefault(key, []).append(p)
    archived_total = 0
    processed = 0
    for _dedupe_key, pages in grouped.items():
        if len(pages) <= 1:
            continue
        canonical = _pick_best_game_jp_dict_page(pages, jp_prop, en_prop, id_prop)
        for p in pages:
            pid = p.get("id", "")
            if not pid or pid == canonical.get("id", ""):
                continue
            try:
                r = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"archived": True})
                if r is not None and r.status_code == 200:
                    archived_total += 1
            except Exception:
                continue
        processed += 1
        if processed >= max_groups:
            break
    return archived_total


def cleanup_game_jp_dict_noise(max_rows: int = 200) -> dict[str, int]:
    """
    ゲームJP辞書DBの軽量クリーンアップ。
    - 重複整理（IGDB_ID/英語タイトル）
    - 空/未解決JPのアーカイブ
    - IGDB_IDあり行はIGDB由来JPで上書き補正（手動確定は温存）
    """
    stats = {"archived": 0, "patched": 0, "scanned": 0, "rows": 0}
    if not NOTION_GAME_JP_DICT_DB_ID:
        return stats
    stats["archived"] += _dedupe_game_jp_dict_all(max_groups=max_rows)
    rows = _query_game_jp_dict_rows()
    if not rows:
        return stats
    stats["rows"] = len(rows)
    db_props, jp_prop, en_prop, id_prop = _resolve_game_jp_dict_schema()
    conf_prop = next((k for k, v in db_props.items() if (v or {}).get("type") == "select" and ("信頼" in k or "CONF" in k.upper())), "")
    upd_prop = next((k for k, v in db_props.items() if (v or {}).get("type") == "date" and ("更新" in k or "DATE" in k.upper())), "")
    high_en_keys = set()
    for p in rows:
        props0 = p.get("properties", {}) or {}
        en0 = _plain_text_from_rich(props0.get(en_prop, {})) if en_prop else _plain_text_from_rich(props0.get("英語タイトル", {}))
        if not en0:
            name_title0 = _title_text_from_prop(props0.get("名前", {}))
            if ":" in name_title0:
                en0 = name_title0.split(":", 1)[1].strip()
        conf0 = (((props0.get(conf_prop, {}) or {}).get("select") or {}).get("name") or "").strip() if conf_prop else ""
        if en0 and conf0 in ("高", "手動"):
            high_en_keys.add(_norm_game_title_key(en0))

    scanned = 0
    for p in rows:
        if scanned >= max_rows:
            break
        scanned += 1
        props = p.get("properties", {}) or {}
        pid = p.get("id", "")
        if not pid:
            continue
        jp = _plain_text_from_rich(props.get(jp_prop, {})) if jp_prop else _plain_text_from_rich(props.get("日本語タイトル", {}))
        en = _plain_text_from_rich(props.get(en_prop, {})) if en_prop else _plain_text_from_rich(props.get("英語タイトル", {}))
        if not en:
            name_title = _title_text_from_prop(props.get("名前", {}))
            if ":" in name_title:
                en = name_title.split(":", 1)[1].strip()
        conf = (((props.get(conf_prop, {}) or {}).get("select") or {}).get("name") or "").strip() if conf_prop else ""
        igdb_val = (props.get(id_prop, {}) or {}).get("number")
        if not jp or jp == "（JP未解決）":
            r = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"archived": True})
            if r is not None and r.status_code == 200:
                stats["archived"] += 1
            continue
        if igdb_val is None:
            # IGDB IDなし・低信頼は辞書ノイズとして退避
            if conf in ("", "中", "低"):
                r = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"archived": True})
                if r is not None and r.status_code == 200:
                    stats["archived"] += 1
            continue
        # 同一英題で高信頼/手動が既にある中低信頼は整理
        if conf in ("", "中", "低") and en and _norm_game_title_key(en) in high_en_keys:
            r = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"archived": True})
            if r is not None and r.status_code == 200:
                stats["archived"] += 1
            continue
        # 中/低信頼でノイズ語を含むタイトルは辞書用途から除外
        if conf in ("", "中", "低") and _is_noisy_game_title(en):
            r = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"archived": True})
            if r is not None and r.status_code == 200:
                stats["archived"] += 1
            continue
        # 手動確定は尊重。それ以外（高/中/低）はIGDB由来JPで補正対象
        if conf == "手動":
            continue
        game = fetch_game_by_id(int(igdb_val))
        if not game:
            continue
        has_company = bool((game.get("developer") or "").strip() or (game.get("publisher") or "").strip())
        cat = int(game.get("category") or -1)
        # 中低信頼で、公式性が乏しいレコード（会社情報なし or 非本編カテゴリ）は辞書から除外
        if conf in ("", "中", "低"):
            if (not has_company) or (cat not in (0, 8, 9)):
                r = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"archived": True})
                if r is not None and r.status_code == 200:
                    stats["archived"] += 1
                continue
        igdb_jp = (game.get("jp_title") or "").strip()
        igdb_en = (game.get("title") or "").strip()
        if conf in ("", "中", "低") and _is_noisy_game_title(igdb_en or en):
            r = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"archived": True})
            if r is not None and r.status_code == 200:
                stats["archived"] += 1
            continue
        if not igdb_jp:
            continue
        if igdb_jp == jp and (not igdb_en or not en or igdb_en == en):
            continue
        patch_props = {}
        if jp_prop:
            patch_props[jp_prop] = {"rich_text": [{"type": "text", "text": {"content": igdb_jp}}]}
        if en_prop and igdb_en and igdb_en != en:
            patch_props[en_prop] = {"rich_text": [{"type": "text", "text": {"content": igdb_en}}]}
        if conf_prop:
            patch_props[conf_prop] = {"select": {"name": "高"}}
        if upd_prop:
            patch_props[upd_prop] = {"date": {"start": date.today().isoformat()}}
        if not patch_props:
            continue
        r = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"properties": patch_props})
        if r is not None and r.status_code == 200:
            stats["patched"] += 1
    stats["scanned"] = scanned
    _invalidate_game_jp_dict_cache()
    return stats


def _load_game_jp_dict_from_notion() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """
    returns:
      by_id: {igdb_id(str): jp_title}
      by_title: {normalized_en_title: jp_title}
      id_to_page: {igdb_id(str): notion_page_id}
    """
    by_id: dict[str, str] = {}
    by_title: dict[str, str] = {}
    id_to_page: dict[str, str] = {}
    if not NOTION_GAME_JP_DICT_DB_ID:
        return by_id, by_title, id_to_page
    try:
        db_props, jp_prop, en_prop, id_prop = _resolve_game_jp_dict_schema()
        conf_prop = next((k for k, v in db_props.items() if (v or {}).get("type") == "select" and ("信頼" in k or "CONF" in k.upper())), "")
        res = _query_game_jp_dict_rows()
        for page in res or []:
            props = page.get("properties", {}) or {}
            jp = _plain_text_from_rich(props.get(jp_prop, {})) if jp_prop else _plain_text_from_rich(props.get("日本語タイトル", {}))
            en = _plain_text_from_rich(props.get(en_prop, {})) if en_prop else _plain_text_from_rich(props.get("英語タイトル", {}))
            conf = (((props.get(conf_prop, {}) or {}).get("select") or {}).get("name") or "").strip() if conf_prop else ""
            if not jp:
                continue
            igdb_val = (props.get(id_prop, {}) or {}).get("number")
            if igdb_val is not None:
                igdb_key = str(int(igdb_val))
                # 既存重複がある場合は「先勝ち」にして upsert 側で整理
                by_id.setdefault(igdb_key, jp)
                id_to_page.setdefault(igdb_key, page.get("id", ""))
            # タイトル辞書は高信頼/手動のみ採用（誤学習の波及防止）
            if en and conf in ("高", "手動") and not _is_noisy_game_title(en):
                by_title[_norm_game_title_key(en)] = jp
            name_title = _title_text_from_prop(props.get("名前", {}))
            if name_title and ":" in name_title:
                maybe_en = name_title.split(":", 1)[1].strip()
                if maybe_en:
                    by_title.setdefault(_norm_game_title_key(maybe_en), jp)
    except Exception:
        return {}, {}, {}
    return by_id, by_title, id_to_page


def _get_game_jp_dict_cache() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    key = "_game_jp_notion_cache"
    if key not in st.session_state:
        st.session_state[key] = _load_game_jp_dict_from_notion()
    return st.session_state[key]


def _invalidate_game_jp_dict_cache() -> None:
    st.session_state.pop("_game_jp_notion_cache", None)


def _upsert_game_jp_dict_notion(igdb_id: int | None, en_title: str, jp_title: str, confidence: str = "手動") -> bool:
    if not NOTION_GAME_JP_DICT_DB_ID:
        return False
    en = (en_title or "").strip()
    jp = (jp_title or "").strip()
    if not en or not jp:
        return False

    page_id = ""
    if igdb_id:
        canonical_id, archived = _dedupe_game_jp_dict_rows(int(igdb_id))
        if archived > 0:
            st.session_state["_game_jp_dedupe_archived"] = st.session_state.get("_game_jp_dedupe_archived", 0) + archived
        page_id = canonical_id
    if not page_id:
        _, _, id_to_page = _get_game_jp_dict_cache()
        if igdb_id:
            page_id = id_to_page.get(str(int(igdb_id)), "")

    # DB実プロパティ名に合わせる（ユーザー側の命名差異を吸収）
    db_props, jp_prop, en_prop, id_prop = _resolve_game_jp_dict_schema()
    title_prop = next((k for k, v in db_props.items() if (v or {}).get("type") == "title"), "名前")
    conf_prop = next((k for k, v in db_props.items() if (v or {}).get("type") == "select" and ("信頼" in k or "CONF" in k.upper())), "信頼度")
    upd_prop = next((k for k, v in db_props.items() if (v or {}).get("type") == "date" and ("更新" in k or "DATE" in k.upper())), "更新日")

    props = {title_prop: {"title": [{"type": "text", "text": {"content": f"{igdb_id if igdb_id else '-'}:{en}"}}]}}
    if en_prop:
        props[en_prop] = {"rich_text": [{"type": "text", "text": {"content": en}}]}
    if jp_prop:
        props[jp_prop] = {"rich_text": [{"type": "text", "text": {"content": jp}}]}
    if conf_prop in db_props:
        props[conf_prop] = {"select": {"name": confidence or "手動"}}
    if upd_prop in db_props:
        props[upd_prop] = {"date": {"start": date.today().isoformat()}}
    if igdb_id and id_prop in db_props:
        props[id_prop] = {"number": int(igdb_id)}

    try:
        res = None
        if page_id:
            res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS, json={"properties": props})
        else:
            res = api_request(
                "post",
                "https://api.notion.com/v1/pages",
                headers=NOTION_HEADERS,
                json={
                    "parent": {"database_id": NOTION_GAME_JP_DICT_DB_ID},
                    "properties": props,
                },
            )
        if res is None or res.status_code not in (200, 201):
            if not st.session_state.get("_game_dict_upsert_warned"):
                st.session_state["_game_dict_upsert_warned"] = True
                st.warning(f"⚠️ ゲームJP辞書DB保存失敗: {res.status_code if res else 'None'}")
            return False
        _invalidate_game_jp_dict_cache()
        return True
    except Exception:
        # 失敗を握りつぶさず、1run中1回だけ表示
        if not st.session_state.get("_game_dict_upsert_warned"):
            st.session_state["_game_dict_upsert_warned"] = True
            st.warning("⚠️ ゲームJP辞書DBへの保存に失敗しました。プロパティ名/型をご確認ください。")
        return False


def _load_game_jp_learned_map() -> dict[str, str]:
    try:
        if GAME_JP_LEARNED_MAP_PATH.exists():
            data = json.loads(GAME_JP_LEARNED_MAP_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items() if str(k).strip() and str(v).strip()}
    except Exception:
        pass
    return {}


def _get_game_jp_learned_map() -> dict[str, str]:
    key = "_game_jp_learned_map"
    if key not in st.session_state:
        st.session_state[key] = _load_game_jp_learned_map()
    return st.session_state[key]


def _lookup_game_jp_learned(en_title: str, igdb_id: int | None = None, allow_title_fallback: bool = False) -> str:
    title = (en_title or "").strip()
    if not title:
        return ""
    # 1) Notion辞書（本番永続）
    try:
        by_id, by_title, _ = _get_game_jp_dict_cache()
        if igdb_id is not None:
            jp = by_id.get(str(int(igdb_id)), "")
            if jp:
                return jp
            # 同名別ID（移植/再販）向け: 高信頼タイトル辞書のみ限定で参照
            jp_same_title = by_title.get(_norm_game_title_key(title), "")
            if jp_same_title:
                return jp_same_title
        if allow_title_fallback:
            jp = by_title.get(_norm_game_title_key(title), "")
            if jp:
                return jp
    except Exception:
        pass
    # 2) ローカル学習（フォールバック）
    if not allow_title_fallback:
        return ""
    learned = _get_game_jp_learned_map()
    if not learned:
        return ""
    key = _norm_game_title_key(title)
    if key in learned:
        return learned[key]
    base = re.sub(r"\s*-\s*[^-]*(edition|bundle|collection|pack).*$", "", title, flags=re.IGNORECASE).strip()
    base_key = _norm_game_title_key(base)
    return learned.get(base_key, "")


def _learn_game_jp_title(en_title: str, jp_title: str, igdb_id: int | None = None, confidence: str = "手動", persist_notion: bool = True) -> bool:
    en = (en_title or "").strip()
    jp = (jp_title or "").strip()
    if not en or not jp or not _contains_japanese(jp):
        return False
    key = _norm_game_title_key(en)
    learned = dict(_get_game_jp_learned_map())
    conf = (confidence or "").strip()
    can_persist = (
        conf == "手動"
        or (igdb_id is not None and (conf.startswith("IGDB") or conf in ("高",)))
    )
    if learned.get(key) == jp:
        if persist_notion and can_persist:
            return _upsert_game_jp_dict_notion(igdb_id, en, jp, confidence=confidence)
        return True
    learned[key] = jp
    st.session_state["_game_jp_learned_map"] = learned
    try:
        GAME_JP_LEARNED_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
        GAME_JP_LEARNED_MAP_PATH.write_text(
            json.dumps(learned, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass
    if persist_notion and can_persist:
        return _upsert_game_jp_dict_notion(igdb_id, en, jp, confidence=confidence)
    return True

def _expand_game_query_aliases(query: str) -> list[str]:
    q = (query or "").strip()
    if not q:
        return []
    expanded = [q]
    for k, vals in GAME_QUERY_ALIASES.items():
        if k in q:
            expanded.extend(vals)
    return _dedupe_keep_order(expanded)

def _derive_game_series_title(title: str) -> str:
    t = (title or "").strip()
    if not t:
        return "その他"
    if ":" in t:
        return t.split(":", 1)[0].strip()
    return t

def _game_variant_label(title: str) -> str:
    low = (title or "").lower()
    if any(k in low for k in ["dlc", "expansion", "season pass", "episode"]):
        return "追加コンテンツ"
    if any(k in low for k in ["edition", "bundle", "collection", "pack"]):
        return "特装/同梱"
    return "本編候補"

PLATFORM_NAME_MAP = {
    "SNES": "スーパーファミコン",
    "Super Nintendo Entertainment System": "スーパーファミコン",
    "NES": "ファミリーコンピュータ",
    "Nintendo Entertainment System": "ファミリーコンピュータ",
    "Nintendo 64": "Nintendo 64",
    "Nintendo GameCube": "ゲームキューブ",
    "Game Boy": "ゲームボーイ",
    "Game Boy Color": "ゲームボーイカラー",
    "Game Boy Advance": "ゲームボーイアドバンス",
    "Nintendo DS": "ニンテンドーDS",
    "Nintendo 3DS": "ニンテンドー3DS",
    "Wii": "Wii",
    "Wii U": "Wii U",
    "Nintendo Switch": "Nintendo Switch",
    "Nintendo Switch 2": "Nintendo Switch 2",
}

def normalize_platform_names(names: list[str]) -> list[str]:
    out = []
    for n in names or []:
        nn = (n or "").strip()
        if not nn:
            continue
        out.append(PLATFORM_NAME_MAP.get(nn, nn))
    return _dedupe_keep_order(out)

def _game_base_title_candidates(title: str) -> list[str]:
    t = (title or "").strip()
    if not t:
        return []
    cands = [t]
    t1 = re.sub(r"\s*-\s*[^-]*(edition|bundle|collection|pack).*$", "", t, flags=re.IGNORECASE).strip()
    if t1 and t1 not in cands:
        cands.append(t1)
    t2 = re.sub(r"\s*-\s*[^-]*(dlc|expansion|season pass).*$", "", t, flags=re.IGNORECASE).strip()
    if t2 and t2 not in cands:
        cands.append(t2)
    if ":" in t:
        t3 = t.split(":", 1)[0].strip()
        if t3 and t3 not in cands:
            cands.append(t3)
    return cands

def _is_noisy_game_title(title: str) -> bool:
    low = (title or "").lower()
    if not low:
        return True
    noisy_terms = [
        "randomizer", "redux", "mod", "patch", "uncensored", "overhaul", "rebalance",
        "multiplayer", "online", "hack", "edition", "version", "bundle", "collection",
        "pack", "expansion", "season pass", "dlc",
    ]
    return any(t in low for t in noisy_terms)

def _is_official_game_candidate_for_learning(row: dict) -> bool:
    title = (row.get("title") or "").strip()
    if not title or _is_noisy_game_title(title):
        return False
    if row.get("variant_label") in ("追加コンテンツ", "特装/同梱"):
        return False
    has_rel = bool((row.get("release") or "").strip())
    cat = int(row.get("category") or -1)
    is_main_cat = cat in (0, 8, 9)
    has_company = bool((row.get("developer") or "").strip() or (row.get("publisher") or "").strip())
    return has_rel and is_main_cat and has_company

def _norm_game_match_key(text: str) -> str:
    t = (text or "").strip().lower()
    t = re.sub(r"[\s:\-_'\"!！?？・、。]+", "", t)
    return t

def _is_specific_game_query(query: str) -> bool:
    q = (query or "").strip()
    if not q:
        return False
    # 作品単位を示しやすい: 数字 / ローマ数字
    if re.search(r"\d", q):
        return True
    if re.search(r"\b(i|ii|iii|iv|v|vi|vii|viii|ix|x|xi|xii|xiii|xiv|xv|xvi)\b", q, re.IGNORECASE):
        return True
    return False

def _game_query_match_keys(query: str) -> set[str]:
    q = (query or "").strip()
    keys = set()
    if not q:
        return keys
    for v in _expand_game_query_aliases(q):
        k = _norm_game_match_key(v)
        if k:
            keys.add(k)
    # 作品特定クエリでは、Wikipedia候補を広く取りすぎると別作品混入の原因になるため
    # 「単一の最有力ENタイトル」だけを採用する。
    if _contains_japanese(q):
        try:
            en_best = _wikipedia_en_title_from_japanese(q)
            if en_best:
                k = _norm_game_match_key(en_best)
                if k:
                    keys.add(k)
        except Exception:
            pass
    return keys

def _extract_jp_name_from_igdb_item(item: dict) -> tuple[str, str, str]:
    # 1) game_localizations（最優先）
    for loc in item.get("game_localizations", []) or []:
        name = (loc.get("name") or "").strip() if isinstance(loc, dict) else ""
        if name and _contains_japanese(name):
            return name, "IGDB-localization", "高"
    # 2) alternative_names（commentに日本語注記があれば優先）
    jp_with_tag = []
    jp_plain = []
    for alt in item.get("alternative_names", []) or []:
        if not isinstance(alt, dict):
            continue
        name = (alt.get("name") or "").strip()
        comment = (alt.get("comment") or "").strip().lower()
        if not name or not _contains_japanese(name):
            continue
        if any(k in comment for k in ["japanese", "japan", "日本", "jp title", "jp"]):
            jp_with_tag.append(name)
        else:
            jp_plain.append(name)
    if jp_with_tag:
        return jp_with_tag[0], "IGDB-alt(JP注記)", "高"
    if jp_plain:
        return jp_plain[0], "IGDB-alt", "中"
    return "", "", ""

def _build_game_cover_candidates(cand: dict, query_hint: str = "") -> list[str]:
    en_title = (cand.get("title") or "").strip()
    jp_title = (cand.get("jp_title") or "").strip()
    if not jp_title and query_hint and _contains_japanese(query_hint):
        jp_title = search_wikipedia_jp_title(en_title) or query_hint
    ja_img = _wiki_page_image_from_title(jp_title, "ja") if jp_title else ""
    en_img = _wiki_page_image_from_title(en_title, "en") if en_title else ""
    igdb_img = cand.get("cover_url", "")
    artwork_imgs = [u for u in (cand.get("artwork_urls") or []) if u]
    screenshot_imgs = [u for u in (cand.get("screenshot_urls") or []) if u]
    related = [u for u in (cand.get("related_cover_urls") or []) if u]
    existing = [u for u in (cand.get("cover_candidates") or []) if u]
    return _dedupe_keep_order([ja_img, igdb_img, en_img] + artwork_imgs + related + screenshot_imgs + existing)

def _search_games_for_ui(query: str, include_images: bool = False) -> list:
    q = (query or "").strip()
    if not q:
        return []
    expanded_queries = _expand_game_query_aliases(q)
    base = []
    seen_ids = set()
    for eq in expanded_queries:
        for r in search_games(eq):
            rid = r.get("id")
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            base.append(r)
    # JP入力でIGDBが弱い時は、Wikipedia言語リンク候補から英題検索を追加
    if _contains_japanese(q):
        en_candidates = _wikipedia_en_title_candidates_from_japanese(q, limit=8)
        if not en_candidates:
            one = _wikipedia_en_title_from_japanese(q)
            if one:
                en_candidates = [one]
        if en_candidates:
            seen = {r.get("id") for r in base}
            for en in en_candidates:
                for r in search_games(en):
                    rid = r.get("id")
                    if rid in seen:
                        continue
                    base.append(r)
                    seen.add(rid)
        # シリーズ語などで0件の場合、英語候補から主要トークン検索を試す
        if not base and en_candidates:
            token_queries = []
            for en in en_candidates:
                words = re.findall(r"[A-Za-z][A-Za-z0-9']{3,}", en)
                # 一般語を間引いて、固有語を優先
                drop = {"legend", "game", "video", "the", "of", "and"}
                for w in words:
                    lw = w.lower()
                    if lw in drop:
                        continue
                    token_queries.append(w)
            token_queries = _dedupe_keep_order(token_queries)[:6]
            seen = set()
            for tq in token_queries:
                for r in search_games(tq):
                    rid = r.get("id")
                    if rid in seen:
                        continue
                    base.append(r)
                    seen.add(rid)
                if len(base) >= 120:
                    break
    # 作品特定クエリ（数字/ローマ数字あり）は、タイトル厳格一致を優先してノイズ候補を削減
    if _is_specific_game_query(q):
        match_keys = {_norm_game_match_key(v) for v in expanded_queries if v}
        if _contains_japanese(q):
            try:
                for e in _wikipedia_en_title_candidates_from_japanese(q, limit=6):
                    if e:
                        match_keys.add(_norm_game_match_key(e))
            except Exception:
                pass
        strict = []
        for r in base:
            t = (r.get("title") or "").strip()
            tk = _norm_game_match_key(t)
            alt_keys = {_norm_game_match_key(a) for a in (r.get("alt_titles") or []) if (a or "").strip()}
            if tk in match_keys or bool(match_keys & alt_keys):
                strict.append(r)
        if strict:
            base = strict
    enriched = []
    for cand in base:
        en_title = (cand.get("title") or "").strip()
        row = dict(cand)
        # IGDBが返すJPタイトルは最優先で保持（精度優先）
        row["jp_title"] = (cand.get("jp_title") or "").strip()
        row["jp_source"] = (cand.get("jp_source") or "").strip()
        row["jp_confidence"] = (cand.get("jp_confidence") or "").strip()
        row["series_title"] = _derive_game_series_title(en_title)
        row["variant_label"] = _game_variant_label(en_title)
        if include_images:
            cover_candidates = _build_game_cover_candidates(row, q)
            row["cover_candidates"] = cover_candidates
            if cover_candidates:
                row["cover_url"] = cover_candidates[0]
        enriched.append(row)
    return enriched

def fetch_album_by_id(collection_id: int) -> dict | None:
    res = requests.get(
        "https://itunes.apple.com/lookup",
        params={"id": collection_id, "entity": "album", "country": "JP", "lang": "ja_jp"},
        headers={"User-Agent": "ArteMis/1.0"},
    )
    if res.status_code != 200:
        return None
    items = res.json().get("results", [])
    if not items:
        return None
    item = items[0]
    cover_url = item.get("artworkUrl100", "").replace("100x100bb", "600x600bb")
    release = (item.get("releaseDate", "") or "")[:10]
    return {
        "id":         item.get("collectionId", 0),
        "title":      item.get("collectionName", ""),
        "artist":     item.get("artistName", ""),
        "release":    release,
        "cover_url":  cover_url,
        "media_type": "album",
    }

# ============================================================
# 漫画（楽天ブックス コミックジャンル）
# ============================================================
def fetch_itunes_tracks(collection_id: int) -> list:
    """iTunesのアルバムIDからトラックリストを取得"""
    res = requests.get(
        "https://itunes.apple.com/lookup",
        params={"id": collection_id, "entity": "song", "country": "JP", "lang": "ja_jp"},
        headers={"User-Agent": "ArteMis/1.0"},
    )
    if res.status_code != 200:
        return []
    tracks = []
    for item in res.json().get("results", []):
        if item.get("wrapperType") == "track":
            tracks.append({
                "no":   item.get("trackNumber", 0),
                "name": item.get("trackName", ""),
            })
    return sorted(tracks, key=lambda x: x["no"])

# ============================================================
# 漫画（楽天ブックス コミックジャンル）
# ============================================================
def search_manga(query: str, author: str = None, page: int = 1, fast: bool = True) -> list:
    rk_params = {
        "applicationId": RAKUTEN_APP_ID,
        "accessKey":     st.secrets.get("RAKUTEN_ACCESS_KEY", ""),
        "booksGenreId":  "001001",   # コミック・ラノベ
        "hits":          30,
        "page":          page,
        "formatVersion": 2,
        "sort":          "sales",
        "outOfStockFlag": 1,
    }
    if query:  rk_params["title"]  = query
    if author: rk_params["author"] = author
    rk_headers = {
        "Referer":       "https://artemis-cers.streamlit.app",
        "Origin":        "https://artemis-cers.streamlit.app",
        "User-Agent":    "Mozilla/5.0",
        "Authorization": f"Bearer {st.secrets.get('RAKUTEN_ACCESS_KEY', '')}",
    }
    try:
        res = requests.get(
            "https://openapi.rakuten.co.jp/services/api/BooksBook/Search/20170404",
            params=rk_params, headers=rk_headers, timeout=8,
        )
    except Exception as e:
        st.warning(f"⚠️ 楽天ブックスAPI エラー: {e}")
        return []
    if res.status_code != 200:
        st.warning(f"⚠️ 楽天ブックスAPI {res.status_code}: {res.text[:200]}")
        return []
    results = []
    seen = set()
    for item in res.json().get("Items", []):
        rakuten_cover = item.get("largeImageUrl") or item.get("mediumImageUrl") or item.get("smallImageUrl", "")
        rakuten_cover = rakuten_cover.replace("http://", "https://") if rakuten_cover else ""
        raw_authors = [a.strip() for a in (item.get("author", "") or "").split("/") if a.strip()]
        authors = [clean_author(a) for a in raw_authors]
        # 巻数を除いたタイトルを作品単位として使う
        base_title = re.sub(r'\s*[\(（]?\d+[\)）]?\s*$', '', item.get("title", "")).strip()
        if base_title in seen:
            continue
        seen.add(base_title)
        isbn_val = item.get("isbn", "")
        if fast:
            cover = get_fast_book_cover(isbn_val, rakuten_cover)
        else:
            cover_candidates = collect_book_cover_candidates(isbn_val, base_title, " / ".join(authors) if authors else None, rakuten_cover)
            cover = choose_best_cover(cover_candidates) or ""
        results.append({
            "id":         isbn_val or base_title,
            "isbn":       isbn_val,
            "title":      base_title,
            "authors":    authors,
            "published":  parse_rakuten_date(item.get("salesDate", "") or ""),
            "cover_url":  cover,
            "media_type": "manga",
        })
    return results


# ============================================================
# TMDB詳細取得
# ============================================================
def fetch_tmdb_details(tmdb_id: int, media_type: str, season_number: int | None = None) -> dict:
    base      = "https://api.themoviedb.org/3"
    params_ja = {"api_key": TMDB_API_KEY, "language": "ja-JP"}
    params_en = {"api_key": TMDB_API_KEY, "language": "en-US"}

    detail_res = api_request("get", f"{base}/{media_type}/{tmdb_id}", params=params_ja)
    genres = []
    if detail_res and detail_res.status_code == 200:
        genres = [g["name"] for g in detail_res.json().get("genres", [])]

    season_poster, season_air_date = None, None
    if media_type == "tv" and season_number:
        season_res = api_request("get", f"{base}/tv/{tmdb_id}/season/{season_number}", params=params_ja)
        if season_res and season_res.status_code == 200:
            season_data     = season_res.json()
            season_poster   = season_data.get("poster_path")
            season_air_date = season_data.get("air_date")
            cast_names      = [m.get("name", "") for m in season_data.get("credits", {}).get("cast", [])[:3]]
        else:
            cast_names = []
        director_name = ""
        tv_res = api_request("get", f"{base}/tv/{tmdb_id}", params=params_ja)
        if tv_res and tv_res.status_code == 200:
            creators = tv_res.json().get("created_by", [])
            if creators:
                director_name = creators[0].get("name", "")
    else:
        credit_endpoint = "credits" if media_type == "movie" else "aggregate_credits"
        credit_res = api_request("get", f"{base}/{media_type}/{tmdb_id}/{credit_endpoint}", params=params_ja)
        cast_names, director_name = [], ""
        if credit_res and credit_res.status_code == 200:
            data = credit_res.json()
            for member in data.get("cast", [])[:3]:
                cast_names.append(member.get("name", ""))
            if media_type == "movie":
                for member in data.get("crew", []):
                    if member.get("job") == "Director":
                        director_name = member.get("name", "")
                        break
            else:
                tv_res = api_request("get", f"{base}/tv/{tmdb_id}", params=params_ja)
                if tv_res and tv_res.status_code == 200:
                    creators = tv_res.json().get("created_by", [])
                    if creators:
                        director_name = creators[0].get("name", "")

    score = None
    score_res = api_request("get", f"{base}/{media_type}/{tmdb_id}", params=params_en)
    if score_res and score_res.status_code == 200:
        score = score_res.json().get("vote_average")

    return {
        "genres":          genres,
        "cast":            " / ".join(cast_names),
        "director":        director_name,
        "score":           round(score, 1) if score else None,
        "season_poster":   season_poster,
        "season_air_date": season_air_date,
    }

def update_notion_metadata(page_id: str, details: dict, force: bool = False, props: dict = None) -> tuple:
    properties = {}
    updated    = []

    def needs_update(key):
        if force or props is None: return True
        if key == "ジャンル":     return not props.get("ジャンル", {}).get("multi_select")
        if key == "キャスト・関係者":  return not props.get("キャスト・関係者", {}).get("rich_text")
        if key == "クリエイター":  return not props.get("クリエイター", {}).get("rich_text")
        if key == "TMDB_score":   return props.get("TMDB_score", {}).get("number") is None
        return False

    if details["genres"] and needs_update("ジャンル"):
        properties["ジャンル"] = {"multi_select": [{"name": g} for g in details["genres"]]}
        updated.append("ジャンル")
    if details["cast"] and needs_update("キャスト・関係者"):
        cleaned_cast = " / ".join(clean_author(a) for a in details["cast"].split("/") if a.strip())
        properties["キャスト・関係者"] = {"rich_text": [{"type": "text", "text": {"content": cleaned_cast}}]}
        updated.append("出演者")
    if details["director"] and needs_update("クリエイター"):
        cleaned_director = clean_author(details["director"])
        properties["クリエイター"] = {"rich_text": [{"type": "text", "text": {"content": cleaned_director}}]}
        updated.append("監督")
    if details.get("score") is not None and needs_update("TMDB_score"):
        properties["TMDB_score"] = {"number": details["score"]}
        updated.append(f"スコア({details['score']})")

    if not properties:
        return True, []

    res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS, json={"properties": properties})
    return (res is not None and res.status_code == 200), updated

def update_notion_cover(page_id: str, cover_url: str, tmdb_release, existing_release, is_refresh: bool = False) -> bool:
    payload = {"cover": {"type": "external", "external": {"url": cover_url}}}
    if tmdb_release and (not existing_release or is_refresh):
        payload["properties"] = {"リリース日": {"date": {"start": tmdb_release}}}
    res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS, json=payload)
    return res is not None and res.status_code == 200

def update_all(page_id, cover_url, tmdb_release, existing_release,
               title, tmdb_id, media_type, need_notion, need_drive,
               force_meta=False, props=None, season_number=None, is_refresh=False) -> tuple:
    actual_cover_url = cover_url
    if media_type == "tv" and season_number:
        details_pre = fetch_tmdb_details(tmdb_id, media_type, season_number)
        if details_pre.get("season_poster"):
            actual_cover_url = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{details_pre['season_poster']}"
        if details_pre.get("season_air_date") and (not existing_release or is_refresh):
            tmdb_release = details_pre["season_air_date"]

    notion_ok = update_notion_cover(page_id, actual_cover_url, tmdb_release, existing_release, is_refresh) if need_notion else True
    # アイコンを媒体に応じて更新
    if props is not None:
        media_labels = [m["name"] for m in props.get("媒体", {}).get("multi_select", [])]
        media_label  = media_labels[0] if media_labels else None
        if media_label:
            icon_payload = get_media_icon_payload(media_label)
            api_request("patch", f"https://api.notion.com/v1/pages/{page_id}",
                        headers=NOTION_HEADERS,
                        json={"icon": icon_payload})
    drive_ok  = bool(save_to_drive(actual_cover_url, title, tmdb_id)) if need_drive else True

    if props is not None:
        old_tmdb_id = props.get("TMDB_ID", {}).get("number")
        if old_tmdb_id and int(old_tmdb_id) != tmdb_id:
            delete_from_drive(title, int(old_tmdb_id))

    save_tmdb_id_to_notion(page_id, tmdb_id, media_type)
    if season_number:
        save_season_to_notion(page_id, season_number)

    meta_ok, updated = False, []
    try:
        details          = fetch_tmdb_details(tmdb_id, media_type, season_number)
        meta_ok, updated = update_notion_metadata(page_id, details, force=force_meta, props=props)
    except Exception as e:
        st.warning(f"メタデータ更新失敗 ({title}): {e}")
    return notion_ok, drive_ok, meta_ok, updated

# ============================================================
# Nominatim ジオコーディング
# ============================================================

NOMINATIM_HEADERS = {
    "User-Agent": "ArteMisCERS/3.1 (https://github.com/attituderko-design/artemis-cers)"
}

def geocode_nominatim(query: str) -> list[dict]:
    """場所名から候補一覧を取得（lat/lon/name/address）"""
    try:
        res = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "addressdetails": 1, "limit": 5, "accept-language": "ja"},
            headers=NOMINATIM_HEADERS,
            timeout=10,
        )
        time.sleep(1)  # レート制限: 1秒/リクエスト
        if res.status_code != 200:
            return []
        results = res.json()
        candidates = []
        for r in results:
            addr = r.get("address", {})
            address_parts = [
                addr.get("country", ""),
                addr.get("state", ""),
                addr.get("city", "") or addr.get("town", "") or addr.get("village", ""),
                addr.get("suburb", "") or addr.get("neighbourhood", ""),
                addr.get("road", ""),
            ]
            address_str = " ".join(p for p in address_parts if p)
            candidates.append({
                "name":    r.get("display_name", query).split(",")[0].strip(),
                "address": address_str,
                "lat":     float(r["lat"]),
                "lon":     float(r["lon"]),
                "display": r.get("display_name", ""),
            })
        return candidates
    except Exception:
        return []


def location_search_ui(key_prefix: str, media_label: str, initial_location: dict | None = None) -> dict | None:
    """ロケーション検索UIコンポーネント。選択済みlocation dictを返す（未選択はNone）"""
    LOCATION_LABELS = {
        "映画":          ("📍 鑑賞した場所（任意）", "例: TOHOシネマズ梅田"),
        "ドラマ":        ("📍 鑑賞した場所（任意）", "例: 自宅 / Netflix"),
        "演奏会（鑑賞）": ("📍 会場（任意）",          "例: フェニーチェ堺"),
        "出演":          ("📍 会場（任意）",          "例: フェニーチェ堺"),
        "展示会":        ("📍 会場（任意）",          "例: 国立国際美術館"),
        "ライブ/ショー": ("📍 会場（任意）",          "例: 大阪城ホール"),
        "イベント":      ("📍 会場（任意）",          "例: 淀川花火大会"),
        "書籍":          ("📍 読んだ場所や購入した場所（任意）",  "例: 梅田 蔦屋書店"),
        "漫画":          ("📍 読んだ場所や購入した場所（任意）",  "例: とらのあな"),
        "音楽アルバム":  ("📍 聴いた場所や購入した場所（任意）",  "例: タワーレコード梅田"),
        "ゲーム":        ("📍 プレイした場所や購入した場所（任意）",  "例: ヨドバシカメラ梅田"),
        "演奏曲":        ("📍 演奏会場（任意）",      "例: ザ・シンフォニーホール"),
        "アニメ":        ("📍 視聴した場所（任意）",  "例: 自宅 / 映画館"),
    }
    label, placeholder = LOCATION_LABELS.get(media_label, ("📍 場所（任意）", "例: 大阪"))

    loc_key      = f"{key_prefix}_loc_query"
    result_key   = f"{key_prefix}_loc_results"
    selected_key = f"{key_prefix}_loc_selected"

    if loc_key not in st.session_state:
        st.session_state[loc_key]      = ""
    if result_key not in st.session_state:
        st.session_state[result_key]   = []
    if selected_key not in st.session_state:
        st.session_state[selected_key] = None
    if initial_location and st.session_state[selected_key] is None:
        st.session_state[selected_key] = initial_location
        if not st.session_state[loc_key]:
            st.session_state[loc_key] = initial_location.get("name") or initial_location.get("address") or ""

    st.caption(label)
    col_input, col_btn = st.columns([4, 1])
    query = col_input.text_input(
        label, value=st.session_state[loc_key],
        placeholder=placeholder, label_visibility="collapsed", key=f"{key_prefix}_loc_input"
    )
    search_clicked = col_btn.button("🔍", key=f"{key_prefix}_loc_search_btn")

    if search_clicked and query:
        with st.spinner("検索中..."):
            results = geocode_nominatim(query)
        st.session_state[loc_key]      = query
        st.session_state[result_key]   = results
        st.session_state[selected_key] = None
        if not results:
            st.warning("見つかりませんでした。候補がない場合はNotion上で入力してください。")

    results = st.session_state[result_key]
    current = st.session_state.get(selected_key)
    if current and current.get("lat") and current.get("lon"):
        cname = current.get("name") or current.get("address") or "設定済み"
        st.caption(f"現在の場所: {cname} （{current['lat']:.5f}, {current['lon']:.5f}）")

    if results:
        options = ["（選択してください）"] + [r["display"] for r in results]
        choice  = st.selectbox("候補を選択", options, key=f"{key_prefix}_loc_select")
        if choice != "（選択してください）":
            chosen = results[options.index(choice) - 1]
            st.session_state[selected_key] = chosen
            st.caption(f"✅ {chosen['name']} （{chosen['lat']:.5f}, {chosen['lon']:.5f}）")

    return st.session_state.get(selected_key)


def create_notion_page(jp_title: str, en_title: str, media_type_label: str,
                       tmdb_id: int, media_type: str, cover_url: str,
                       tmdb_release: str, details: dict,
                       wlflg: bool = False, watched_date: str | None = None,
                       rating: str | None = None,
                       isbn: str | None = None,
                       location: str | None = None,
                       event_end: str | None = None,
                       memo: str | None = None,
                       igdb_id: int | None = None,
                       itunes_id: int | None = None,
                       anilist_id: int | None = None,
                       is_concerto: bool = False,
                       soloists: str | None = None,
                       icon_emoji: str | None = None,
                       relation_prop: str | None = None,
                       relation_ids: list[str] | None = None) -> bool:
    """Notionに新規ページを作成してポスター・メタデータも一括登録"""
    # テスト登録モード: タイトルにプレフィックスし、メモへタグを埋める
    test_mode = bool(st.session_state.get("test_register_mode", False))
    test_tag = (st.session_state.get("test_register_tag") or "").strip()
    if test_mode:
        if not jp_title.startswith("[TEST] "):
            jp_title = f"[TEST] {jp_title}"
        if not en_title.startswith("[TEST] "):
            en_title = f"[TEST] {en_title}"
        if test_tag:
            marker = f"[TEST_TAG:{test_tag}]"
            memo = ((memo or "").strip() + ("\n" if memo else "") + marker).strip()

    properties = {
        "タイトル":            {"title": [{"type": "text", "text": {"content": jp_title}}]},
        "International Title": {"rich_text": [{"type": "text", "text": {"content": en_title}, "annotations": {"italic": True}}]},
        "媒体":               {"multi_select": [{"name": media_type_label}]},
        **({"TMDB_ID": {"number": tmdb_id}} if tmdb_id else {}),
        "WLflg":              {"checkbox": wlflg},
    }
    release_date_str = _normalize_notion_date_input(str(tmdb_release))
    if release_date_str:
        date_prop = {"start": release_date_str}
        end_date_str = _normalize_notion_date_input(str(event_end))
        if end_date_str:
            date_prop["end"] = end_date_str
        properties["リリース日"] = {"date": date_prop}
    watched_date_str = _normalize_notion_date_input(str(watched_date))
    if watched_date_str:
        watched_prop_name = get_experience_date_property_name()
        properties[watched_prop_name] = {"date": {"start": watched_date_str}}
    if rating:
        properties["評価"] = {"select": {"name": rating}}
    if details.get("genres"):
        properties["ジャンル"] = {"multi_select": [{"name": g} for g in details["genres"]]}
    if details.get("cast"):
        properties["キャスト・関係者"] = {"rich_text": [{"type": "text", "text": {"content": details["cast"]}}]}
    if details.get("director"):
        properties["クリエイター"] = {"rich_text": [{"type": "text", "text": {"content": details["director"]}}]}
    if details.get("score") is not None:
        properties["TMDB_score"] = {"number": details["score"]}
    if isbn:
        properties["ISBN"] = {"rich_text": [{"type": "text", "text": {"content": isbn}}]}
    if igdb_id:
        properties["IGDB_ID"] = {"number": igdb_id}
    if itunes_id:
        properties["iTunes_ID"] = {"number": itunes_id}
    if anilist_id:
        properties["AniList_ID"] = {"number": anilist_id}
    if memo:
        properties["メモ"] = {"rich_text": [{"type": "text", "text": {"content": memo}}]}
    # 演奏曲向け: 協奏曲フラグ / ソリスト（プロパティが存在する場合のみ）
    if media_type == "score":
        type_map = get_notion_db_property_types(NOTION_DB_ID)
        concerto_prop = "協奏曲" if "協奏曲" in type_map else ("Concerto" if "Concerto" in type_map else "")
        soloist_prop = "ソリスト" if "ソリスト" in type_map else ("Soloists" if "Soloists" in type_map else "")
        if concerto_prop:
            _put_notion_prop(properties, type_map, concerto_prop, bool(is_concerto))
        if soloist_prop and (soloists is not None):
            _put_notion_prop(properties, type_map, soloist_prop, str(soloists or "").strip())
    if location and location.get("lat") and location.get("lon"):
        place_payload = {
            "lat":  location["lat"],
            "lon":  location["lon"],
            "name": location.get("name", ""),
        }
        if location.get("address"):
            place_payload["address"] = location["address"]
        properties["ロケーション"] = {"place": place_payload}
    rel_ids = _clean_relation_ids(relation_ids)
    if relation_prop and rel_ids:
        properties[relation_prop] = {"relation": [{"id": rid} for rid in rel_ids]}

    icon_payload = get_media_icon_payload(media_type_label)
    # 演奏曲は親DB側では媒体アイコンを維持する（国旗は演奏曲DB側で扱う）
    if icon_emoji and media_type != "score":
        icon_payload = {"type": "emoji", "emoji": icon_emoji}
    payload = {
        "parent":     {"database_id": NOTION_DB_ID},
        "icon":       icon_payload,
        "properties": properties,
    }
    # 媒体アイコンURLは cover には使わず、icon のみ適用する
    if cover_url and not is_media_icon_url(cover_url):
        payload["cover"] = {"type": "external", "external": {"url": cover_url}}
    res = api_request("post", "https://api.notion.com/v1/pages", headers=NOTION_HEADERS, json=payload)
    if res is None:
        return False
    if res.status_code != 200:
        try:
            err = res.json()
            st.error(f"Notion API エラー {res.status_code}: {err.get('message','')}\ncode: {err.get('code','')}")
        except Exception:
            st.error(f"Notion API エラー {res.status_code}: {res.text[:300]}")
        return False
    try:
        created = res.json()
        st.session_state.last_created_page = created
        st.session_state.last_created_page_id = created.get("id")
        st.session_state.created_pages.append(created)
    except Exception:
        pass
    return True

def find_test_pages_by_tag(tag: str, max_pages: int = 200) -> list[dict]:
    t = (tag or "").strip()
    if not t or not NOTION_DB_ID:
        return []
    body = {
        "page_size": min(max_pages, 100),
        "filter": {
            "property": "メモ",
            "rich_text": {"contains": f"[TEST_TAG:{t}]"},
        },
    }
    found = []
    next_cursor = None
    while len(found) < max_pages:
        if next_cursor:
            body["start_cursor"] = next_cursor
        res = api_request("post", f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query", headers=NOTION_HEADERS, json=body)
        if res is None or res.status_code != 200:
            break
        data = res.json() or {}
        found.extend(data.get("results", []))
        if not data.get("has_more"):
            break
        next_cursor = data.get("next_cursor")
        if not next_cursor:
            break
    return found[:max_pages]

def collect_related_score_ids_from_parent_pages(parent_pages: list[dict]) -> list[str]:
    ids = []
    seen = set()
    for pg in parent_pages or []:
        props = (pg.get("properties") or {}) if isinstance(pg, dict) else {}
        # 親DB -> 演奏曲DB の relation 列（想定名: 演奏曲）
        rel = ((props.get("演奏曲") or {}).get("relation") or [])
        for r in rel:
            rid = (r or {}).get("id")
            if rid and rid not in seen:
                seen.add(rid)
                ids.append(rid)
    return ids

def archive_pages_by_id(page_ids: list[str]) -> tuple[int, int]:
    ok = 0
    ng = 0
    for pid in page_ids:
        if not pid:
            continue
        res = api_request("patch", f"https://api.notion.com/v1/pages/{pid}", headers=NOTION_HEADERS, json={"archived": True})
        if res is not None and res.status_code == 200:
            ok += 1
        else:
            ng += 1
    return ok, ng

@st.cache_data(ttl=600)
def get_notion_db_property_types(database_id: str) -> dict:
    if not database_id:
        return {}
    props = {}
    res = api_request("get", f"https://api.notion.com/v1/databases/{database_id}", headers=NOTION_HEADERS)
    if res is not None and res.status_code == 200:
        props = (res.json() or {}).get("properties", {}) or {}
    else:
        # Notion Data Source API fallback (for IDs issued from data-source URL context)
        ds_res = api_request("get", f"https://api.notion.com/v1/data-sources/{database_id}", headers=NOTION_HEADERS)
        if ds_res is not None and ds_res.status_code == 200:
            ds_json = ds_res.json() or {}
            props = (ds_json.get("properties", {}) or {})
    if not props:
        return {}
    return {name: (meta.get("type") if isinstance(meta, dict) else None) for name, meta in props.items()}

def _put_notion_prop(properties: dict, type_map: dict, name: str, value):
    p_type = type_map.get(name)
    if not p_type:
        return
    if p_type == "title":
        text = str(value or "")
        properties[name] = {"title": ([{"type": "text", "text": {"content": text}}] if text else [])}
    elif p_type == "rich_text":
        text = str(value or "")
        properties[name] = {"rich_text": ([{"type": "text", "text": {"content": text}}] if text else [])}
    elif p_type == "select":
        text = str(value or "").strip()
        properties[name] = {"select": {"name": text} if text else None}
    elif p_type == "multi_select":
        if isinstance(value, list):
            names = [str(v).strip() for v in value if str(v).strip()]
        else:
            text = str(value or "").strip()
            names = [text] if text else []
        properties[name] = {"multi_select": [{"name": n} for n in names]}
    elif p_type == "checkbox":
        properties[name] = {"checkbox": bool(value)}
    elif p_type == "number":
        properties[name] = {"number": value if value is not None else None}
    elif p_type == "date":
        text = _normalize_notion_date_input(str(value or "").strip())
        properties[name] = {"date": {"start": text} if text else None}
    elif p_type == "relation":
        if value is None:
            properties[name] = {"relation": []}
        elif isinstance(value, list):
            properties[name] = {"relation": [{"id": rid} for rid in value if rid]}
        else:
            properties[name] = {"relation": [{"id": value}] if value else []}

def _split_instruments(part: str) -> list[str]:
    return [x.strip() for x in re.split(r'[/／,、・\s]+', part or "") if x.strip()]

def _int_to_roman(n: int) -> str:
    if not isinstance(n, int) or n <= 0:
        return ""
    table = [
        (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"),
        (100, "C"), (90, "XC"), (50, "L"), (40, "XL"),
        (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I"),
    ]
    out = []
    v = n
    for base, sym in table:
        while v >= base:
            out.append(sym)
            v -= base
    return "".join(out)

def _roman_to_int(s: str) -> int | None:
    txt = (s or "").strip().upper()
    if not txt or not re.fullmatch(r"[IVXLCDM]+", txt):
        return None
    val = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    total = 0
    prev = 0
    for ch in reversed(txt):
        cur = val.get(ch, 0)
        if cur < prev:
            total -= cur
        else:
            total += cur
            prev = cur
    # 妥当性再検証（IV 以外の不正並びを弾く）
    if _int_to_roman(total) != txt:
        return None
    return total

def _infer_movement_from_title(title: str) -> dict:
    """
    曲名から楽章番号/ローマ数字を推定（高信頼パターンのみ）。
    例:
      - 第2楽章 -> 2 / II
      - Movement 3, Mvt. 3 -> 3 / III
      - "II. Adagio"（先頭ローマ数字）-> 2 / II
    """
    txt = (title or "").strip()
    if not txt:
        return {"movement_name": "", "movement_no": None, "movement_order": None, "movement_roman": ""}

    # 1) 日本語: 第N楽章
    m = re.search(r"第\s*([0-9]{1,3})\s*楽章", txt, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return {"movement_name": "", "movement_no": n, "movement_order": n, "movement_roman": _int_to_roman(n)}

    # 2) 英語: Movement / Mvt
    m = re.search(r"\b(?:movement|mvt\.?)\s*(?:no\.?|#)?\s*([0-9]{1,3})\b", txt, flags=re.IGNORECASE)
    if m:
        n = int(m.group(1))
        return {"movement_name": "", "movement_no": n, "movement_order": n, "movement_roman": _int_to_roman(n)}

    # 3) 先頭ローマ数字: "II. Adagio" / "IV - Allegro"
    m = re.match(r"^\s*([IVXLCDM]{1,8})(?:[\.\)\-:\s]|$)", txt, flags=re.IGNORECASE)
    if m:
        roman = m.group(1).upper()
        n = _roman_to_int(roman)
        if n:
            return {"movement_name": "", "movement_no": n, "movement_order": n, "movement_roman": roman}

    # 4) タイトル中ローマ数字: "Symphony ... : I. Allegro" / "... - IV Finale"
    m = re.search(r"(?:[:：/\-]\s*)([IVXLCDM]{1,8})(?:[\.\)\-:\s]|$)", txt, flags=re.IGNORECASE)
    if m:
        roman = m.group(1).upper()
        n = _roman_to_int(roman)
        if n:
            return {"movement_name": "", "movement_no": n, "movement_order": n, "movement_roman": roman}

    return {"movement_name": "", "movement_no": None, "movement_order": None, "movement_roman": ""}


def _normalize_work_title_for_group(title: str) -> str:
    """
    楽章違いでも同一作品として束ねるための正規化タイトル。
    例:
      - Symphony No.41 ... I. Allegro  -> Symphony No.41 ...
      - 交響曲第41番 第2楽章            -> 交響曲第41番
    """
    txt = (title or "").strip()
    if not txt:
        return ""
    base = re.sub(r"\s*\([^)]*\)\s*$", "", txt).strip()
    # 日本語: 第N楽章 以降を切り落とし
    base = re.sub(r"\s*第\s*[0-9]{1,3}\s*楽章.*$", "", base, flags=re.IGNORECASE).strip()
    # 英語: movement/mvt 以降を切り落とし
    base = re.sub(r"\s*(?:[-:：/]\s*)?\b(?:movement|mvt\.?)\s*(?:no\.?|#)?\s*[0-9]{1,3}\b.*$", "", base, flags=re.IGNORECASE).strip()
    # ローマ数字節（"...: II. Andante" / "... - IV Allegro"）を末尾から切り落とし
    # 重要: セパレータ必須にして、"in C major" の C を誤って楽章番号扱いしない
    base = re.sub(
        r"\s*(?:[-:：/]\s*)[IVXLCDM]{1,8}(?:[\.\)\-:\s]+.*)?$",
        "",
        base,
        flags=re.IGNORECASE,
    ).strip()
    # 追加: "..., II. Andante" 形式にも対応（カンマ区切り）
    base = re.sub(
        r"\s*,\s*[IVXLCDM]{1,8}(?:[\.\)\-:\s]+.*)?$",
        "",
        base,
        flags=re.IGNORECASE,
    ).strip()
    return base or txt

def _normalize_person_name(name: str) -> str:
    return (name or "").strip().lower()

def _find_or_create_performer_id(name: str) -> str | None:
    performer_name = (name or "").strip()
    if not performer_name or not NOTION_PERFORMER_DB_ID:
        return None
    res = api_request(
        "post",
        f"https://api.notion.com/v1/databases/{NOTION_PERFORMER_DB_ID}/query",
        headers=NOTION_HEADERS,
        json={"filter": {"property": "名前", "title": {"equals": performer_name}}, "page_size": 1},
    )
    if res is not None and res.status_code == 200:
        rows = (res.json() or {}).get("results", [])
        if rows:
            return rows[0].get("id")
    props = get_notion_db_property_types(NOTION_PERFORMER_DB_ID)
    payload_props = {}
    _put_notion_prop(payload_props, props, "名前", performer_name)
    if not payload_props:
        return None
    cres = api_request(
        "post",
        "https://api.notion.com/v1/pages",
        headers=NOTION_HEADERS,
        json={"parent": {"database_id": NOTION_PERFORMER_DB_ID}, "properties": payload_props},
    )
    if cres is not None and cres.status_code == 200:
        return (cres.json() or {}).get("id")
    return None

def _extract_name_title(page: dict) -> str:
    props = (page or {}).get("properties", {})
    for key in ("名前", "タイトル", "Name"):
        p = props.get(key, {})
        if (p.get("type") == "title") or ("title" in p):
            txt = plain_text_join((p or {}).get("title", []))
            if txt.strip():
                return txt.strip()
    return ""

def get_performer_master_names() -> list[str]:
    dbid = NOTION_PERFORMER_MASTER_DB_ID or NOTION_PERFORMER_DB_ID
    pages = query_notion_database_all(dbid)
    names = []
    for pg in pages:
        nm = _extract_name_title(pg)
        if nm:
            names.append(nm)
    # preserve order while unique
    seen = set()
    out = []
    for n in names:
        k = _normalize_person_name(n)
        if k in seen:
            continue
        seen.add(k)
        out.append(n)
    return out

def sync_performer_master_from_performer_db() -> tuple[int, int, str]:
    if not NOTION_PERFORMER_MASTER_DB_ID:
        return 0, 0, "NOTION_PERFORMER_MASTER_DB_ID 未設定"
    src_pages = query_notion_database_all(NOTION_PERFORMER_DB_ID)
    dst_pages = query_notion_database_all(NOTION_PERFORMER_MASTER_DB_ID)
    src_names = []
    for pg in src_pages:
        nm = _extract_name_title(pg)
        if nm:
            src_names.append(nm)
    dst_keys = {_normalize_person_name(_extract_name_title(pg)) for pg in dst_pages if _extract_name_title(pg)}
    type_map = get_notion_db_property_types(NOTION_PERFORMER_MASTER_DB_ID)
    created = 0
    skipped = 0
    for nm in src_names:
        key = _normalize_person_name(nm)
        if not key or key in dst_keys:
            skipped += 1
            continue
        props = {}
        _put_notion_prop(props, type_map, "名前", nm)
        _put_notion_prop(props, type_map, "タイトル", nm)
        _put_notion_prop(props, type_map, "Name", nm)
        if not props:
            skipped += 1
            continue
        res = api_request(
            "post",
            "https://api.notion.com/v1/pages",
            headers=NOTION_HEADERS,
            json={"parent": {"database_id": NOTION_PERFORMER_MASTER_DB_ID}, "properties": props},
        )
        if res is not None and res.status_code == 200:
            created += 1
            dst_keys.add(key)
        else:
            skipped += 1
    return created, skipped, ""

def create_performance_participant_rows(
    performance_page_id: str,
    performance_title: str,
    participants: list[dict],
) -> tuple[int, int, str, dict]:
    ctx = {
        "NOTION_PERFORMANCE_CAST_DB_ID": NOTION_PERFORMANCE_CAST_DB_ID,
        "get_notion_db_property_types": get_notion_db_property_types,
        "normalize_person_name": _normalize_person_name,
        "split_instruments": _split_instruments,
        "find_or_create_performer_id": _find_or_create_performer_id,
        "put_notion_prop": _put_notion_prop,
        "api_request": api_request,
        "NOTION_HEADERS": NOTION_HEADERS,
    }
    return _create_participants_service(ctx, performance_page_id, performance_title, participants)

def create_setlist_rows_for_performance(
    performance_page_id: str,
    performance_title: str,
    performance_date: str,
    main_items: list[dict],
    encore_items: list[dict],
    selected_scores: list[dict],
    score_pages: list[dict],
) -> tuple[int, int, str, list[dict]]:
    ctx = {
        "NOTION_SCORE_DB_ID": NOTION_SCORE_DB_ID,
        "NOTION_COUNTRY_MASTER_DB_ID": NOTION_COUNTRY_MASTER_DB_ID,
        "NOTION_WORK_DB_ID": NOTION_WORK_DB_ID,
        "NOTION_COMPOSER_DB_ID": NOTION_COMPOSER_DB_ID,
        "NOTION_MOVEMENT_DB_ID": NOTION_MOVEMENT_DB_ID,
        "get_notion_db_property_types": get_notion_db_property_types,
        "find_score_page_by_title": _find_score_page_by_title,
        "put_notion_prop": _put_notion_prop,
        "split_instruments": _split_instruments,
        "query_notion_database_all": query_notion_database_all,
        "api_request": api_request,
        "NOTION_HEADERS": NOTION_HEADERS,
        "get_composer_country_code": get_composer_country_code,
        "normalize_country_code_for_flag": normalize_country_code_for_flag,
        "country_code_to_flag": country_code_to_flag,
        "get_media_icon_url": get_media_icon_url,
    }
    return _create_setlist_service(
        ctx,
        performance_page_id,
        performance_title,
        performance_date,
        main_items,
        encore_items,
        selected_scores,
        score_pages,
    )

def create_song_assignment_rows(
    score_rows: list[dict],
    cast_row_map: dict,
) -> tuple[int, int, str]:
    ctx = {
        "NOTION_SONG_ASSIGN_DB_ID": NOTION_SONG_ASSIGN_DB_ID,
        "get_notion_db_property_types": get_notion_db_property_types,
        "normalize_person_name": _normalize_person_name,
        "put_notion_prop": _put_notion_prop,
        "api_request": api_request,
        "NOTION_HEADERS": NOTION_HEADERS,
    }
    return _create_song_assign_service(ctx, score_rows, cast_row_map)


def upsert_score_master_links(
    score_page_id: str,
    song_title: str,
    composer_name: str = "",
    composer_country: str = "",
    movement_name: str = "",
    movement_no=None,
    movement_order=None,
    movement_roman: str = "",
) -> tuple[bool, str]:
    ctx = {
        "NOTION_SCORE_DB_ID": NOTION_SCORE_DB_ID,
        "NOTION_WORK_DB_ID": NOTION_WORK_DB_ID,
        "NOTION_COMPOSER_DB_ID": NOTION_COMPOSER_DB_ID,
        "NOTION_MOVEMENT_DB_ID": NOTION_MOVEMENT_DB_ID,
        "get_notion_db_property_types": get_notion_db_property_types,
        "put_notion_prop": _put_notion_prop,
        "api_request": api_request,
        "NOTION_HEADERS": NOTION_HEADERS,
        "normalize_country_code_for_flag": normalize_country_code_for_flag,
    }
    return _upsert_score_master_links_service(
        ctx=ctx,
        score_page_id=score_page_id,
        song_title=song_title,
        composer_name=composer_name,
        composer_country=composer_country,
        movement_name=movement_name,
        movement_no=movement_no,
        movement_order=movement_order,
        movement_roman=movement_roman,
    )

def relink_existing_score_master_links(
    max_rows: int = 300,
    only_missing: bool = True,
) -> tuple[dict, list[dict]]:
    """
    APOLLO既存行を 作品マスタ / 作品楽章マスタ に一括再連動する。
    """
    stats = {
        "scanned": 0,
        "targeted": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
    }
    failures: list[dict] = []

    if not NOTION_SCORE_DB_ID:
        stats["error"] = "NOTION_SCORE_DB_ID 未設定"
        return stats, failures
    if not NOTION_WORK_DB_ID:
        stats["error"] = "NOTION_WORK_DB_ID 未設定"
        return stats, failures

    type_map = get_notion_db_property_types(NOTION_SCORE_DB_ID) or {}
    if not type_map:
        stats["error"] = "APOLLOのプロパティ取得失敗"
        return stats, failures

    work_rel_prop = next((k for k in ("作品マスタ", "作品", "Work") if type_map.get(k) == "relation"), None)
    movement_rel_prop = next((k for k in ("作品楽章", "作品楽章マスタ", "楽章マスタ", "Movement") if type_map.get(k) == "relation"), None)
    parent_rel_prop = next((k for k in ("演奏曲", "作品", "関連演奏曲", "Score") if type_map.get(k) == "relation"), None)

    rows = query_notion_database_all(NOTION_SCORE_DB_ID)
    if not rows:
        return stats, failures

    max_rows = max(1, int(max_rows or 1))
    parent_creator_cache: dict[str, str] = {}

    def _text_from_prop(meta: dict | None) -> str:
        if not isinstance(meta, dict):
            return ""
        ptype = meta.get("type")
        if ptype == "rich_text":
            return plain_text_join(meta.get("rich_text", []))
        if ptype == "title":
            return plain_text_join(meta.get("title", []))
        if ptype == "select":
            return ((meta.get("select") or {}).get("name") or "").strip()
        if ptype == "multi_select":
            names = [((x or {}).get("name") or "").strip() for x in (meta.get("multi_select") or [])]
            return " / ".join([x for x in names if x])
        return ""

    def _read_country_code(props_local: dict) -> str:
        for key in ("国コード", "CountryCode", "country_code"):
            cc = normalize_country_code_for_flag(_text_from_prop(props_local.get(key)))
            if cc:
                return cc
        return ""

    def _read_composer(props_local: dict) -> str:
        comp = plain_text_join((props_local.get("クリエイター") or {}).get("rich_text", []))
        if comp:
            return comp.strip()
        for key, meta in (props_local or {}).items():
            if ("クリエイター" in str(key)) or ("作曲家" in str(key)):
                txt = _text_from_prop(meta)
                if txt:
                    return txt.strip()
        return ""

    processed = 0
    for row in rows:
        stats["scanned"] += 1
        if processed >= max_rows:
            break

        row_id = (row or {}).get("id") or ""
        props = (row or {}).get("properties") or {}
        if not row_id:
            stats["skipped"] += 1
            continue

        title = (get_title(props) or ("", "", ""))[0].strip()
        if not title:
            stats["skipped"] += 1
            continue

        existing_work_rel = ((props.get(work_rel_prop) or {}).get("relation") or []) if work_rel_prop else []
        existing_mv_rel = ((props.get(movement_rel_prop) or {}).get("relation") or []) if movement_rel_prop else []
        if only_missing and existing_work_rel and (not movement_rel_prop or existing_mv_rel):
            stats["skipped"] += 1
            continue

        composer_name = _read_composer(props)
        if not composer_name and parent_rel_prop:
            rels = ((props.get(parent_rel_prop) or {}).get("relation") or [])
            parent_id = ((rels[0] or {}).get("id") or "") if rels else ""
            if parent_id:
                if parent_id not in parent_creator_cache:
                    pres = api_request(
                        "get",
                        f"https://api.notion.com/v1/pages/{parent_id}",
                        headers=NOTION_HEADERS,
                    )
                    if pres is not None and pres.status_code == 200:
                        pprops = ((pres.json() or {}).get("properties") or {})
                        parent_creator_cache[parent_id] = plain_text_join((pprops.get("クリエイター") or {}).get("rich_text", [])).strip()
                    else:
                        parent_creator_cache[parent_id] = ""
                composer_name = parent_creator_cache.get(parent_id, "")

        movement_name = _text_from_prop(props.get("楽章名"))
        movement_no = (props.get("楽章番号") or {}).get("number")
        movement_order = (props.get("表示順") or {}).get("number")
        movement_roman = _text_from_prop(props.get("ローマ数字表示"))
        if movement_order is None and movement_no is not None:
            movement_order = movement_no
        if not movement_roman and isinstance(movement_no, (int, float)):
            movement_roman = _int_to_roman(int(movement_no))
        if not movement_name and movement_no is None and not movement_roman:
            guessed = _infer_movement_from_title(title)
            movement_name = guessed.get("movement_name", "") or ""
            movement_no = guessed.get("movement_no", None)
            movement_order = guessed.get("movement_order", None)
            movement_roman = guessed.get("movement_roman", "") or ""

        processed += 1
        stats["targeted"] += 1
        ok, msg = upsert_score_master_links(
            score_page_id=row_id,
            song_title=title,
            composer_name=composer_name,
            composer_country=_read_country_code(props),
            movement_name=movement_name,
            movement_no=movement_no,
            movement_order=movement_order,
            movement_roman=movement_roman,
        )
        if ok:
            stats["updated"] += 1
        else:
            stats["failed"] += 1
            failures.append(
                {
                    "id": row_id,
                    "title": title,
                    "composer": composer_name,
                    "error": msg or "unknown",
                }
            )

    return stats, failures

def repair_apollo_grouped_work_links(
    max_groups: int = 200,
    normalize_apollo_title: bool = True,
) -> tuple[dict, list[dict]]:
    """
    APOLLO既存データを「出演 + 曲順 (+作曲家)」で束ね、
    同一グループ内の行を同一作品マスタへ再連動する補正処理。
    """
    stats = {
        "scanned": 0,
        "grouped": 0,
        "processed_groups": 0,
        "updated_rows": 0,
        "title_normalized": 0,
        "skipped_rows": 0,
        "failed_rows": 0,
    }
    failures: list[dict] = []
    if not NOTION_SCORE_DB_ID:
        stats["error"] = "NOTION_SCORE_DB_ID 未設定"
        return stats, failures
    if not NOTION_WORK_DB_ID:
        stats["error"] = "NOTION_WORK_DB_ID 未設定"
        return stats, failures

    type_map = get_notion_db_property_types(NOTION_SCORE_DB_ID) or {}
    if not type_map:
        stats["error"] = "APOLLOのプロパティ取得失敗"
        return stats, failures

    perf_rel_prop = next((k for k in ("出演", "演奏会", "公演") if type_map.get(k) == "relation"), None)
    parent_rel_prop = next((k for k in ("演奏曲", "作品", "関連演奏曲", "Score") if type_map.get(k) == "relation"), None)
    order_prop = "曲順" if type_map.get("曲順") == "number" else None
    score_title_prop = next((k for k, v in (type_map or {}).items() if v == "title"), None)

    rows = query_notion_database_all(NOTION_SCORE_DB_ID) or []
    if not rows:
        return stats, failures

    parent_creator_cache: dict[str, str] = {}

    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip()).lower()

    def _text_from_prop(meta: dict | None) -> str:
        if not isinstance(meta, dict):
            return ""
        ptype = meta.get("type")
        if ptype == "rich_text":
            return plain_text_join(meta.get("rich_text", []))
        if ptype == "title":
            return plain_text_join(meta.get("title", []))
        if ptype == "select":
            return ((meta.get("select") or {}).get("name") or "").strip()
        if ptype == "multi_select":
            vals = [((x or {}).get("name") or "").strip() for x in (meta.get("multi_select") or [])]
            return " / ".join([x for x in vals if x])
        return ""

    def _composer_from_row(row_props: dict) -> str:
        comp = plain_text_join((row_props.get("クリエイター") or {}).get("rich_text", []))
        if comp:
            return comp.strip()
        for key, meta in (row_props or {}).items():
            if ("クリエイター" in str(key)) or ("作曲家" in str(key)):
                txt = _text_from_prop(meta)
                if txt:
                    return txt.strip()
        if parent_rel_prop:
            rels = ((row_props.get(parent_rel_prop) or {}).get("relation") or [])
            parent_id = ((rels[0] or {}).get("id") or "") if rels else ""
            if parent_id:
                if parent_id not in parent_creator_cache:
                    pres = api_request("get", f"https://api.notion.com/v1/pages/{parent_id}", headers=NOTION_HEADERS)
                    if pres is not None and pres.status_code == 200:
                        pprops = ((pres.json() or {}).get("properties") or {})
                        parent_creator_cache[parent_id] = plain_text_join((pprops.get("クリエイター") or {}).get("rich_text", [])).strip()
                    else:
                        parent_creator_cache[parent_id] = ""
                return parent_creator_cache.get(parent_id, "")
        return ""

    def _country_from_row(row_props: dict) -> str:
        for key in ("国コード", "CountryCode", "country_code"):
            cc = normalize_country_code_for_flag(_text_from_prop(row_props.get(key)))
            if cc:
                return cc
        return ""

    groups: dict[tuple[str, int, str], list[dict]] = {}
    for row in rows:
        stats["scanned"] += 1
        props = (row or {}).get("properties") or {}
        row_id = (row or {}).get("id") or ""
        if not row_id or not order_prop:
            stats["skipped_rows"] += 1
            continue
        order_no = (props.get(order_prop) or {}).get("number")
        if order_no is None:
            stats["skipped_rows"] += 1
            continue
        order_no = int(order_no)
        perf_id = ""
        if perf_rel_prop:
            perf_rels = ((props.get(perf_rel_prop) or {}).get("relation") or [])
            perf_id = ((perf_rels[0] or {}).get("id") or "") if perf_rels else ""
        composer_name = _composer_from_row(props)
        key = (perf_id, order_no, _norm(composer_name))
        groups.setdefault(key, []).append(row)

    grouped_items = [(k, v) for k, v in groups.items() if len(v) >= 2]
    stats["grouped"] = len(grouped_items)
    max_groups = max(1, int(max_groups or 1))

    for idx, (_, g_rows) in enumerate(grouped_items):
        if idx >= max_groups:
            break
        stats["processed_groups"] += 1

        titles = []
        for r in g_rows:
            rprops = (r.get("properties") or {})
            rtitle = (get_title(rprops) or ("", "", ""))[0].strip()
            base = _normalize_work_title_for_group(rtitle)
            if base:
                titles.append(base)
        canonical_title = ""
        if titles:
            canonical_title = sorted(titles, key=lambda x: (len(x), x))[0]

        for r in g_rows:
            r_id = (r.get("id") or "")
            rprops = (r.get("properties") or {})
            original_title = (get_title(rprops) or ("", "", ""))[0].strip()
            if not canonical_title:
                canonical_title = _normalize_work_title_for_group(original_title) or original_title
            if not canonical_title:
                stats["skipped_rows"] += 1
                continue
            composer_name = _composer_from_row(rprops)
            cc = _country_from_row(rprops)

            movement_name = _text_from_prop(rprops.get("楽章名"))
            movement_no = (rprops.get("楽章番号") or {}).get("number")
            movement_order = (rprops.get("表示順") or {}).get("number")
            movement_roman = _text_from_prop(rprops.get("ローマ数字表示"))
            if movement_order is None and movement_no is not None:
                movement_order = movement_no
            if not movement_name and movement_no is None and not movement_roman:
                guessed = _infer_movement_from_title(original_title)
                movement_name = guessed.get("movement_name", "") or ""
                movement_no = guessed.get("movement_no", None)
                movement_order = guessed.get("movement_order", None)
                movement_roman = guessed.get("movement_roman", "") or ""

            ok, msg = upsert_score_master_links(
                score_page_id=r_id,
                song_title=canonical_title,
                composer_name=composer_name,
                composer_country=cc,
                movement_name=movement_name,
                movement_no=movement_no,
                movement_order=movement_order,
                movement_roman=movement_roman,
            )
            if ok:
                stats["updated_rows"] += 1
                if normalize_apollo_title and score_title_prop:
                    if _norm(original_title) != _norm(canonical_title):
                        tpatch = {"properties": {score_title_prop: {"title": [{"type": "text", "text": {"content": canonical_title}}]}}}
                        tres = api_request(
                            "patch",
                            f"https://api.notion.com/v1/pages/{r_id}",
                            headers=NOTION_HEADERS,
                            json=tpatch,
                        )
                        if tres is not None and tres.status_code == 200:
                            stats["title_normalized"] += 1
                        else:
                            stats["failed_rows"] += 1
                            failures.append(
                                {
                                    "id": r_id,
                                    "title": original_title,
                                    "canonical_title": canonical_title,
                                    "composer": composer_name,
                                    "error": f"APOLLOタイトル更新失敗: {msg or 'unknown'}",
                                }
                            )
            else:
                stats["failed_rows"] += 1
                failures.append(
                    {
                        "id": r_id,
                        "title": original_title,
                        "canonical_title": canonical_title,
                        "composer": composer_name,
                        "error": msg or "unknown",
                    }
                )

    return stats, failures

def consolidate_apollo_duplicate_rows(
    max_groups: int = 200,
) -> tuple[dict, list[dict]]:
    """
    APOLLO内の同一グループ重複行（出演+曲順+作品）を1行に統合し、
    楽章relationを集約した上で重複行をアーカイブする。
    """
    stats = {
        "scanned": 0,
        "duplicate_groups": 0,
        "processed_groups": 0,
        "keeper_patched": 0,
        "archived_rows": 0,
        "failed": 0,
    }
    failures: list[dict] = []

    if not NOTION_SCORE_DB_ID:
        stats["error"] = "NOTION_SCORE_DB_ID 未設定"
        return stats, failures

    type_map = get_notion_db_property_types(NOTION_SCORE_DB_ID) or {}
    if not type_map:
        stats["error"] = "APOLLOのプロパティ取得失敗"
        return stats, failures

    perf_rel_prop = next((k for k in ("出演", "演奏会", "公演") if type_map.get(k) == "relation"), None)
    order_prop = "曲順" if type_map.get("曲順") == "number" else None
    work_rel_prop = next((k for k in ("作品マスタ", "作品", "Work") if type_map.get(k) == "relation"), None)
    movement_rel_prop = next((k for k in ("作品楽章", "作品楽章マスタ", "楽章マスタ", "Movement") if type_map.get(k) == "relation"), None)

    rows = query_notion_database_all(NOTION_SCORE_DB_ID) or []
    if not rows:
        return stats, failures

    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip()).lower()

    grouped: dict[tuple[str, int, str, str], list[dict]] = {}
    for row in rows:
        stats["scanned"] += 1
        props = (row.get("properties") or {})
        row_id = (row.get("id") or "")
        if not row_id:
            continue
        order_no = (props.get(order_prop) or {}).get("number") if order_prop else None
        if order_no is None:
            continue
        order_no = int(order_no)
        perf_id = ""
        if perf_rel_prop:
            rels = ((props.get(perf_rel_prop) or {}).get("relation") or [])
            perf_id = ((rels[0] or {}).get("id") or "") if rels else ""
        work_id = ""
        if work_rel_prop:
            wrels = ((props.get(work_rel_prop) or {}).get("relation") or [])
            work_id = ((wrels[0] or {}).get("id") or "") if wrels else ""
        title = (get_title(props) or ("", "", ""))[0]
        key = (perf_id, order_no, work_id, _norm(_normalize_work_title_for_group(title)))
        grouped.setdefault(key, []).append(row)

    dup_groups = [(k, v) for k, v in grouped.items() if len(v) >= 2]
    stats["duplicate_groups"] = len(dup_groups)
    max_groups = max(1, int(max_groups or 1))

    for idx, (_, g_rows) in enumerate(dup_groups):
        if idx >= max_groups:
            break
        stats["processed_groups"] += 1
        # 代表行: 楽章relationを最も多く持つ行を優先
        def _mv_len(r: dict) -> int:
            rprops = (r.get("properties") or {})
            return len(((rprops.get(movement_rel_prop) or {}).get("relation") or [])) if movement_rel_prop else 0
        keeper = sorted(g_rows, key=lambda r: _mv_len(r), reverse=True)[0]
        keeper_id = keeper.get("id") or ""
        keeper_props = (keeper.get("properties") or {})

        movement_ids = []
        mv_seen = set()
        if movement_rel_prop:
            for r in g_rows:
                rprops = (r.get("properties") or {})
                for rel in ((rprops.get(movement_rel_prop) or {}).get("relation") or []):
                    rid = (rel or {}).get("id")
                    if rid and rid not in mv_seen:
                        mv_seen.add(rid)
                        movement_ids.append(rid)

        if keeper_id and movement_rel_prop and movement_ids:
            patch = {"properties": {movement_rel_prop: {"relation": [{"id": rid} for rid in movement_ids]}}}
            pres = api_request(
                "patch",
                f"https://api.notion.com/v1/pages/{keeper_id}",
                headers=NOTION_HEADERS,
                json=patch,
            )
            if pres is not None and pres.status_code == 200:
                stats["keeper_patched"] += 1
            else:
                stats["failed"] += 1
                failures.append(
                    {
                        "id": keeper_id,
                        "title": (get_title(keeper_props) or ("", "", ""))[0],
                        "error": "代表行の楽章集約に失敗",
                    }
                )
                continue

        archive_ids = [r.get("id") for r in g_rows if (r.get("id") or "") and (r.get("id") != keeper_id)]
        ok, ng = archive_pages_by_id(archive_ids)
        stats["archived_rows"] += ok
        if ng > 0:
            stats["failed"] += ng
            for r in g_rows:
                rid = r.get("id") or ""
                if rid and rid != keeper_id:
                    failures.append(
                        {
                            "id": rid,
                            "title": (get_title((r.get("properties") or {})) or ("", "", ""))[0],
                            "error": "重複行のアーカイブ失敗",
                        }
                    )

    return stats, failures

def _pick_prop_name(type_map: dict, candidates: list[str], p_type: str) -> str | None:
    for c in candidates:
        if type_map.get(c) == p_type:
            return c
    return None

def _extract_relation_ids(props: dict, prop_name: str | None) -> list[str]:
    if not prop_name:
        return []
    rel = ((props.get(prop_name) or {}).get("relation") or [])
    out = []
    for r in rel:
        rid = r.get("id")
        if rid:
            out.append(rid)
    return out

def _extract_page_title_by_type(props: dict, type_map: dict, fallbacks: list[str]) -> str:
    for key in fallbacks:
        if type_map.get(key) == "title":
            return plain_text_join((props.get(key) or {}).get("title", []))
    for k, v in (type_map or {}).items():
        if v == "title":
            return plain_text_join((props.get(k) or {}).get("title", []))
    return ""

def _tail_person_name(text: str) -> str:
    parts = [x.strip() for x in (text or "").split("/") if x.strip()]
    return parts[-1] if parts else ""

def _get_cast_row_map_for_performance(performance_page_id: str) -> dict:
    ctx = {
        "NOTION_PERFORMANCE_CAST_DB_ID": NOTION_PERFORMANCE_CAST_DB_ID,
        "NOTION_PERFORMER_DB_ID": NOTION_PERFORMER_DB_ID,
        "query_notion_database_all": query_notion_database_all,
        "get_notion_db_property_types": get_notion_db_property_types,
        "pick_prop_name": _pick_prop_name,
        "extract_relation_ids": _extract_relation_ids,
        "extract_page_title_by_type": _extract_page_title_by_type,
        "tail_person_name": _tail_person_name,
        "plain_text_join": plain_text_join,
        "normalize_person_name": _normalize_person_name,
    }
    return _get_cast_row_map_service(ctx, performance_page_id)

def _get_performance_cast_names(performance_page_id: str) -> list[str]:
    """演奏会参加者DBから、指定公演の参加者名候補を取得する。"""
    if not performance_page_id or not NOTION_PERFORMANCE_CAST_DB_ID:
        return []
    type_map = get_notion_db_property_types(NOTION_PERFORMANCE_CAST_DB_ID)
    if not type_map:
        return []
    perf_rel_prop = _pick_prop_name(type_map, ["出演", "演奏会", "公演"], "relation")
    pages = query_notion_database_all(NOTION_PERFORMANCE_CAST_DB_ID)
    names = []
    for pg in pages:
        props = (pg or {}).get("properties", {})
        rel_ids = _extract_relation_ids(props, perf_rel_prop) if perf_rel_prop else []
        if performance_page_id not in rel_ids:
            continue
        title = _extract_page_title_by_type(props, type_map, ["タイトル", "Name"])
        nm = _tail_person_name(title) or title
        if nm and nm.strip():
            names.append(nm.strip())
    seen = set()
    out = []
    for n in names:
        k = _normalize_person_name(n)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(n)
    return out

def normalize_performance_score_relations(pages: list[dict]) -> dict:
    """演奏曲/出演ページの自己リレーション向きを整理する。"""
    stats = {"scanned": 0, "patched": 0, "moved": 0, "failed": 0}
    for p in (pages or []):
        media = get_page_media(p)
        if media not in ("演奏曲", "出演"):
            continue
        props = (p.get("properties") or {})
        perf_ids = _clean_relation_ids(_extract_relation_ids(props, "出演履歴"))
        score_ids = _clean_relation_ids(_extract_relation_ids(props, "演奏曲"))
        if not perf_ids and not score_ids:
            continue
        stats["scanned"] += 1

        patch_props = {}
        if media == "演奏曲":
            # 演奏曲ページは「出演履歴」に寄せる
            target = perf_ids or score_ids
            if target != perf_ids:
                stats["moved"] += 1
            patch_props["出演履歴"] = {"relation": [{"id": rid} for rid in target]}
            patch_props["演奏曲"] = {"relation": []}
        else:
            # 出演ページは「演奏曲」に寄せる
            target = score_ids or perf_ids
            if target != score_ids:
                stats["moved"] += 1
            patch_props["演奏曲"] = {"relation": [{"id": rid} for rid in target]}
            patch_props["出演履歴"] = {"relation": []}

        res = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{p.get('id')}",
            headers=NOTION_HEADERS,
            json={"properties": patch_props},
        )
        if res is not None and res.status_code == 200:
            stats["patched"] += 1
            try:
                upsert_page_in_state(res.json())
            except Exception:
                pass
        else:
            stats["failed"] += 1
    return stats


def refresh_score_db_composer_flag_icons() -> dict:
    """演奏曲DBのアイコンを、作曲家の国コードに基づく国旗へ更新する。"""
    stats = {"scanned": 0, "flagged": 0, "fallback": 0, "unresolved": 0, "skipped": 0, "failed": 0}
    if not NOTION_SCORE_DB_ID:
        stats["error"] = "NOTION_SCORE_DB_ID 未設定"
        append_operation_audit_log("refresh_score_db_composer_flag_icons", stats)
        return stats

    type_map = get_notion_db_property_types(NOTION_SCORE_DB_ID)
    if not type_map:
        stats["error"] = "演奏曲DBのプロパティ取得失敗"
        append_operation_audit_log("refresh_score_db_composer_flag_icons", stats)
        return stats

    relation_candidates = ["演奏曲", "曲", "関連演奏曲", "作品"]
    rel_prop = next((k for k in relation_candidates if type_map.get(k) == "relation"), None)
    fallback_icon_url = get_media_icon_url("演奏曲")
    score_rows = query_notion_database_all(NOTION_SCORE_DB_ID)
    if not score_rows:
        append_operation_audit_log("refresh_score_db_composer_flag_icons", stats)
        return stats

    parent_cache: dict[str, dict | None] = {}
    country_cache: dict[str, str] = {}
    score_parent_creator_by_title: dict[str, str] = {}

    def _norm_title(t: str) -> str:
        return re.sub(r"\s+", " ", (t or "").strip()).lower()

    def _text_from_prop(meta: dict | None) -> str:
        if not isinstance(meta, dict):
            return ""
        ptype = meta.get("type")
        if ptype == "rich_text":
            return plain_text_join(meta.get("rich_text", []))
        if ptype == "title":
            return plain_text_join(meta.get("title", []))
        if ptype == "rollup":
            roll = meta.get("rollup") or {}
            rtype = roll.get("type")
            if rtype == "array":
                parts = []
                for item in roll.get("array", []):
                    if not isinstance(item, dict):
                        continue
                    it = item.get("type")
                    if it == "rich_text":
                        txt = plain_text_join(item.get("rich_text", []))
                    elif it == "title":
                        txt = plain_text_join(item.get("title", []))
                    elif it == "people":
                        txt = " / ".join([(p.get("name") or "").strip() for p in (item.get("people") or []) if (p.get("name") or "").strip()])
                    else:
                        txt = ""
                    if txt:
                        parts.append(txt)
                return " / ".join(parts)
            if rtype == "rich_text":
                return plain_text_join((roll.get("rich_text") or []))
            if rtype == "number":
                v = roll.get("number")
                return "" if v is None else str(v)
            if rtype == "date":
                return (((roll.get("date") or {}).get("start")) or "")
        return ""

    def _manual_country_code(row_props: dict) -> str:
        for key in ("国コード", "CountryCode", "country_code"):
            meta = row_props.get(key)
            if not isinstance(meta, dict):
                continue
            ptype = meta.get("type")
            if ptype in ("rich_text", "title", "rollup"):
                txt = _text_from_prop(meta)
            elif ptype == "select":
                txt = ((meta.get("select") or {}).get("name") or "").strip()
            elif ptype == "multi_select":
                vals = [((x or {}).get("name") or "").strip() for x in (meta.get("multi_select") or [])]
                txt = (vals[0] if vals else "")
            else:
                txt = ""
            cc = normalize_country_code_for_flag((txt or "").strip().upper())
            if cc:
                return cc
        return ""

    # 演奏曲DBのrelationが未整備でも、親DB(媒体=演奏曲)の同名タイトルから作曲家を補完する
    for pp in st.session_state.get("all_pages", []) or []:
        if get_page_media(pp) != "演奏曲":
            continue
        pprops = pp.get("properties", {}) or {}
        ptitle = get_title(pprops)[0]
        pcreator = plain_text_join((pprops.get("クリエイター") or {}).get("rich_text", []))
        nk = _norm_title(ptitle)
        if nk and pcreator and nk not in score_parent_creator_by_title:
            score_parent_creator_by_title[nk] = pcreator

    def _resolve_country_code(composer_name: str) -> str:
        key = (composer_name or "").strip().lower()
        if not key:
            return ""
        if key in country_cache:
            return country_cache[key]
        cc = get_composer_country_code(composer_name)
        country_cache[key] = cc or ""
        return country_cache[key]

    for row in score_rows:
        stats["scanned"] += 1
        row_id = row.get("id")
        props = (row.get("properties") or {})
        if not row_id:
            stats["skipped"] += 1
            continue

        composer = plain_text_join((props.get("クリエイター") or {}).get("rich_text", []))
        if not composer:
            for key, meta in props.items():
                if ("クリエイター" in key) or ("作曲家" in key):
                    txt = _text_from_prop(meta)
                    if txt:
                        composer = txt
                        break
        linked_score_id = None
        if rel_prop:
            rels = ((props.get(rel_prop) or {}).get("relation") or [])
            linked_score_id = next((r.get("id") for r in rels if r.get("id")), None)

        if not composer and linked_score_id:
            if linked_score_id not in parent_cache:
                pres = api_request(
                    "get",
                    f"https://api.notion.com/v1/pages/{linked_score_id}",
                    headers=NOTION_HEADERS,
                )
                parent_cache[linked_score_id] = pres.json() if (pres is not None and pres.status_code == 200) else None
            parent_page = parent_cache.get(linked_score_id)
            if parent_page:
                parent_props = parent_page.get("properties", {}) or {}
                composer = plain_text_join((parent_props.get("クリエイター") or {}).get("rich_text", []))

        if not composer:
            row_title = plain_text_join((props.get("タイトル") or {}).get("title", []))
            composer = score_parent_creator_by_title.get(_norm_title(row_title), "")

        icon_payload = None
        manual_cc = _manual_country_code(props)
        if manual_cc:
            cc = manual_cc
        elif composer:
            cc = _resolve_country_code(composer)
            flag = country_code_to_flag(cc) if cc else ""
            if flag:
                icon_payload = {"type": "emoji", "emoji": flag}
            else:
                stats["unresolved"] += 1
        else:
            stats["unresolved"] += 1

        if icon_payload is None and manual_cc:
            flag = country_code_to_flag(manual_cc)
            if flag:
                icon_payload = {"type": "emoji", "emoji": flag}
            else:
                stats["unresolved"] += 1

        if not icon_payload:
            stats["skipped"] += 1
            continue

        current_icon = row.get("icon") or {}
        if current_icon == icon_payload:
            stats["skipped"] += 1
            continue

        ures = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{row_id}",
            headers=NOTION_HEADERS,
            json={"icon": icon_payload},
        )
        if ures is not None and ures.status_code == 200:
            stats["flagged"] += 1
        else:
            stats["failed"] += 1

    append_operation_audit_log("refresh_score_db_composer_flag_icons", stats)
    return stats


def backfill_score_db_country_codes(dry_run: bool = True, fill_only_empty: bool = True) -> dict:
    """演奏曲DBの国コード列を、作曲家名から推定して補完する。"""
    stats = {
        "scanned": 0,
        "filled": 0,
        "candidates": 0,
        "has_code": 0,
        "unresolved": 0,
        "skipped": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
    }
    if not NOTION_SCORE_DB_ID:
        stats["error"] = "NOTION_SCORE_DB_ID 未設定"
        append_operation_audit_log("backfill_score_db_country_codes", stats)
        return stats

    type_map = get_notion_db_property_types(NOTION_SCORE_DB_ID)
    if not type_map:
        stats["error"] = "演奏曲DBのプロパティ取得失敗"
        append_operation_audit_log("backfill_score_db_country_codes", stats)
        return stats

    relation_candidates = ["演奏曲", "曲", "関連演奏曲", "作品"]
    rel_prop = next((k for k in relation_candidates if type_map.get(k) == "relation"), None)
    code_prop_candidates = ["国コード", "CountryCode", "country_code"]
    code_prop = next((k for k in code_prop_candidates if k in type_map), None)
    if not code_prop:
        stats["error"] = "国コード列が見つかりません"
        append_operation_audit_log("backfill_score_db_country_codes", stats)
        return stats
    code_prop_type = type_map.get(code_prop) or ""
    if code_prop_type not in ("rich_text", "title", "select", "multi_select"):
        stats["error"] = f"国コード列({code_prop})は書き込み非対応の型です: {code_prop_type}"
        append_operation_audit_log("backfill_score_db_country_codes", stats)
        return stats

    score_rows = query_notion_database_all(NOTION_SCORE_DB_ID)
    if not score_rows:
        append_operation_audit_log("backfill_score_db_country_codes", stats)
        return stats

    parent_cache: dict[str, dict | None] = {}
    country_cache: dict[str, str] = {}
    score_parent_creator_by_title: dict[str, str] = {}

    def _norm_title(t: str) -> str:
        return re.sub(r"\s+", " ", (t or "").strip()).lower()

    def _text_from_prop(meta: dict | None) -> str:
        if not isinstance(meta, dict):
            return ""
        ptype = meta.get("type")
        if ptype == "rich_text":
            return plain_text_join(meta.get("rich_text", []))
        if ptype == "title":
            return plain_text_join(meta.get("title", []))
        if ptype == "select":
            return ((meta.get("select") or {}).get("name") or "").strip()
        if ptype == "multi_select":
            vals = [((x or {}).get("name") or "").strip() for x in (meta.get("multi_select") or [])]
            return vals[0] if vals else ""
        if ptype == "rollup":
            roll = meta.get("rollup") or {}
            rtype = roll.get("type")
            if rtype == "array":
                parts = []
                for item in (roll.get("array") or []):
                    if not isinstance(item, dict):
                        continue
                    it = item.get("type")
                    if it == "rich_text":
                        txt = plain_text_join(item.get("rich_text", []))
                    elif it == "title":
                        txt = plain_text_join(item.get("title", []))
                    else:
                        txt = ""
                    if txt:
                        parts.append(txt)
                return " / ".join(parts)
            if rtype == "rich_text":
                return plain_text_join((roll.get("rich_text") or []))
        return ""

    # relationが未整備でも、親DB(媒体=演奏曲)同名タイトルで作曲家を補完
    for pp in st.session_state.get("all_pages", []) or []:
        if get_page_media(pp) != "演奏曲":
            continue
        pprops = pp.get("properties", {}) or {}
        ptitle = get_title(pprops)[0]
        pcreator = plain_text_join((pprops.get("クリエイター") or {}).get("rich_text", []))
        nk = _norm_title(ptitle)
        if nk and pcreator and nk not in score_parent_creator_by_title:
            score_parent_creator_by_title[nk] = pcreator

    def _resolve_country_code(composer_name: str) -> str:
        key = (composer_name or "").strip().lower()
        if not key:
            return ""
        if key in country_cache:
            return country_cache[key]
        cc = (get_composer_country_code(composer_name) or "").strip().upper()
        cc = normalize_country_code_for_flag(cc)
        country_cache[key] = cc
        return cc

    for row in score_rows:
        stats["scanned"] += 1
        row_id = row.get("id")
        props = (row.get("properties") or {})
        if not row_id:
            stats["skipped"] += 1
            continue

        existing_cc = normalize_country_code_for_flag(_text_from_prop(props.get(code_prop)))
        if fill_only_empty and existing_cc:
            stats["has_code"] += 1
            stats["skipped"] += 1
            continue

        composer = plain_text_join((props.get("クリエイター") or {}).get("rich_text", []))
        if not composer:
            for key, meta in props.items():
                if ("クリエイター" in key) or ("作曲家" in key):
                    txt = _text_from_prop(meta)
                    if txt:
                        composer = txt
                        break
        linked_score_id = None
        if rel_prop:
            rels = ((props.get(rel_prop) or {}).get("relation") or [])
            linked_score_id = next((r.get("id") for r in rels if r.get("id")), None)

        if not composer and linked_score_id:
            if linked_score_id not in parent_cache:
                pres = api_request("get", f"https://api.notion.com/v1/pages/{linked_score_id}", headers=NOTION_HEADERS)
                parent_cache[linked_score_id] = pres.json() if (pres is not None and pres.status_code == 200) else None
            parent_page = parent_cache.get(linked_score_id)
            if parent_page:
                parent_props = parent_page.get("properties", {}) or {}
                composer = plain_text_join((parent_props.get("クリエイター") or {}).get("rich_text", []))

        if not composer:
            row_title = plain_text_join((props.get("タイトル") or {}).get("title", []))
            composer = score_parent_creator_by_title.get(_norm_title(row_title), "")

        cc = _resolve_country_code(composer) if composer else ""
        if not cc:
            stats["unresolved"] += 1
            stats["skipped"] += 1
            continue

        stats["candidates"] += 1
        if dry_run:
            continue

        if code_prop_type == "rich_text":
            patch = {code_prop: {"rich_text": [{"type": "text", "text": {"content": cc}}]}}
        elif code_prop_type == "title":
            patch = {code_prop: {"title": [{"type": "text", "text": {"content": cc}}]}}
        elif code_prop_type == "select":
            patch = {code_prop: {"select": {"name": cc}}}
        else:  # multi_select
            patch = {code_prop: {"multi_select": [{"name": cc}]}}

        res = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{row_id}",
            headers=NOTION_HEADERS,
            json={"properties": patch},
        )
        if res is not None and res.status_code == 200:
            stats["filled"] += 1
        else:
            stats["failed"] += 1

    append_operation_audit_log("backfill_score_db_country_codes", stats)
    return stats


def sync_score_country_master_relations(dry_run: bool = True, fill_only_empty: bool = True) -> dict:
    """演奏曲DBの国コードをキーに、国名マスタRelationを自動紐付けする。"""
    stats = {
        "scanned": 0,
        "candidates": 0,
        "linked": 0,
        "already": 0,
        "no_code": 0,
        "no_master": 0,
        "skipped": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
    }
    if not NOTION_SCORE_DB_ID:
        stats["error"] = "NOTION_SCORE_DB_ID 未設定"
        append_operation_audit_log("sync_score_country_master_relations", stats)
        return stats
    if not NOTION_COUNTRY_MASTER_DB_ID:
        stats["error"] = "NOTION_COUNTRY_MASTER_DB_ID 未設定"
        append_operation_audit_log("sync_score_country_master_relations", stats)
        return stats

    def _extract_dbid(raw: str) -> str:
        s = (raw or "").strip()
        m = re.search(r"([0-9a-fA-F]{32})", s)
        return m.group(1) if m else s

    def _fetch_db_types_verbose(database_id: str) -> tuple[dict, str]:
        did = _extract_dbid(database_id)
        if not did:
            return {}, "DB IDが空です"
        res = api_request("get", f"https://api.notion.com/v1/databases/{did}", headers=NOTION_HEADERS)
        if res is None:
            return {}, "Notion応答なし（接続エラー）"
        if res.status_code != 200:
            msg = ""
            try:
                body = res.json() or {}
                msg = (body.get("message") or body.get("code") or "").strip()
            except Exception:
                msg = (res.text or "").strip()
            msg = (msg[:220] + "...") if len(msg) > 220 else msg
            return {}, f"Notion {res.status_code}" + (f" / {msg}" if msg else "")
        props = (res.json() or {}).get("properties", {}) or {}
        type_map = {name: (meta.get("type") if isinstance(meta, dict) else None) for name, meta in props.items()}
        return type_map, ""

    score_db_id = _extract_dbid(NOTION_SCORE_DB_ID)
    master_db_id = _extract_dbid(NOTION_COUNTRY_MASTER_DB_ID)
    score_type_map, score_err = _fetch_db_types_verbose(score_db_id)
    master_type_map, master_err = _fetch_db_types_verbose(master_db_id)
    if not score_type_map:
        stats["error"] = "演奏曲DBのプロパティ取得失敗" + (f"（{score_err}）" if score_err else "")
        append_operation_audit_log("sync_score_country_master_relations", stats)
        return stats
    if not master_type_map:
        stats["error"] = "国名マスタのプロパティ取得失敗" + (f"（{master_err}）" if master_err else "")
        append_operation_audit_log("sync_score_country_master_relations", stats)
        return stats

    relation_candidates = ["国名マスタ", "CountryMaster", "Country Master", "国マスタ"]
    relation_prop = next((k for k in relation_candidates if score_type_map.get(k) == "relation"), None)
    if not relation_prop:
        relation_prop = next(
            (k for k, v in score_type_map.items() if v == "relation" and ("国" in k or "country" in k.lower())),
            None,
        )
    if not relation_prop:
        stats["error"] = "演奏曲DBに国名マスタ用Relation列が見つかりません"
        append_operation_audit_log("sync_score_country_master_relations", stats)
        return stats

    code_prop_candidates = ["国コード", "CountryCode", "country_code"]
    score_code_prop = next((k for k in code_prop_candidates if k in score_type_map), None)
    master_code_prop = next((k for k in code_prop_candidates if k in master_type_map), None)
    if not score_code_prop:
        stats["error"] = "演奏曲DBの国コード列が見つかりません"
        append_operation_audit_log("sync_score_country_master_relations", stats)
        return stats
    if not master_code_prop:
        stats["error"] = "国名マスタの国コード列が見つかりません"
        append_operation_audit_log("sync_score_country_master_relations", stats)
        return stats

    def _text_from_prop(meta: dict | None) -> str:
        if not isinstance(meta, dict):
            return ""
        ptype = meta.get("type")
        if ptype == "rich_text":
            return plain_text_join(meta.get("rich_text", []))
        if ptype == "title":
            return plain_text_join(meta.get("title", []))
        if ptype == "select":
            return ((meta.get("select") or {}).get("name") or "").strip()
        if ptype == "multi_select":
            vals = [((x or {}).get("name") or "").strip() for x in (meta.get("multi_select") or [])]
            return vals[0] if vals else ""
        if ptype == "rollup":
            roll = meta.get("rollup") or {}
            rtype = roll.get("type")
            if rtype == "rich_text":
                return plain_text_join((roll.get("rich_text") or []))
        return ""

    master_rows = query_notion_database_all(master_db_id)
    code_to_master_id = {}
    for row in master_rows:
        rid = row.get("id")
        if not rid:
            continue
        cc = normalize_country_code_for_flag(_text_from_prop((row.get("properties") or {}).get(master_code_prop)))
        if cc and cc not in code_to_master_id:
            code_to_master_id[cc] = rid

    if not code_to_master_id:
        stats["error"] = "国名マスタに有効な国コードデータがありません"
        append_operation_audit_log("sync_score_country_master_relations", stats)
        return stats

    score_rows = query_notion_database_all(score_db_id)
    for row in score_rows:
        stats["scanned"] += 1
        row_id = row.get("id")
        props = (row.get("properties") or {})
        if not row_id:
            stats["skipped"] += 1
            continue
        cc = normalize_country_code_for_flag(_text_from_prop(props.get(score_code_prop)))
        if not cc:
            stats["no_code"] += 1
            stats["skipped"] += 1
            continue
        target_id = code_to_master_id.get(cc)
        if not target_id:
            stats["no_master"] += 1
            stats["skipped"] += 1
            continue
        stats["candidates"] += 1
        existing = [r.get("id") for r in ((props.get(relation_prop) or {}).get("relation") or []) if r.get("id")]
        if target_id in existing and len(existing) == 1:
            stats["already"] += 1
            stats["skipped"] += 1
            continue
        if fill_only_empty and existing:
            stats["skipped"] += 1
            continue
        if dry_run:
            continue
        res = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{row_id}",
            headers=NOTION_HEADERS,
            json={"properties": {relation_prop: {"relation": [{"id": target_id}]}}},
        )
        if res is not None and res.status_code == 200:
            stats["linked"] += 1
        else:
            stats["failed"] += 1
    append_operation_audit_log("sync_score_country_master_relations", stats)
    return stats


def restore_parent_score_media_icons() -> dict:
    """親DB(芸術鑑賞記録DB)の媒体=演奏曲アイコンを媒体アイコンへ戻す。"""
    stats = {"scanned": 0, "patched": 0, "skipped": 0, "failed": 0}
    fallback_icon = get_media_icon_payload("演奏曲")

    pages = query_notion_database_all(NOTION_DB_ID)
    if not pages:
        return stats

    target_icon = fallback_icon
    for p in pages:
        if get_page_media(p) != "演奏曲":
            continue
        stats["scanned"] += 1
        page_id = p.get("id")
        if not page_id:
            stats["skipped"] += 1
            continue
        current_icon = p.get("icon") or {}
        if icon_semantically_matches(current_icon, target_icon):
            stats["skipped"] += 1
            continue
        res = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=NOTION_HEADERS,
            json={"icon": target_icon},
        )
        if res is not None and res.status_code == 200:
            stats["patched"] += 1
        else:
            stats["failed"] += 1
    return stats


def emergency_restore_all_media_icons(progress_bar=None, progress_text=None, limit: int = 120, force_reapply: bool = False) -> dict:
    """
    親DBのアイコンを媒体アイコンに強制復旧する。
    ※ 演奏曲DBは国旗運用のため、ここでは触らない。
    """
    stats = {
        "parent_scanned": 0,
        "parent_patched": 0,
        "parent_emoji_fallback": 0,
        "parent_failed": 0,
        "details": [],
    }

    # 1) 親DB: 媒体ごとのアイコンに復旧（未復旧分のみを抽出→上限件数だけ実行）
    parent_pages = query_notion_database_all(NOTION_DB_ID) or []
    total_pages = len(parent_pages)
    work_items = []
    for p in parent_pages:
        media = get_page_media(p)
        target_icon, icon_meta = resolve_media_icon_payload(
            media,
            allow_external_fallback=False,
            allow_emoji_fallback=False,
        )
        page_id = p.get("id")
        if not page_id:
            continue
        if target_icon is None:
            work_items.append((p, media, page_id, None, icon_meta))
            continue
        if (not force_reapply) and icon_semantically_matches((p.get("icon") or {}), target_icon):
            continue
        work_items.append((p, media, page_id, target_icon, icon_meta))

    pending_all = len(work_items)
    if limit and limit > 0:
        work_items = work_items[:limit]

    if progress_bar is not None:
        progress_bar.progress(0.0)
    total_work = len(work_items)
    for idx, (p, media, page_id, target_icon, icon_meta) in enumerate(work_items, start=1):
        stats["parent_scanned"] += 1
        if progress_bar is not None:
            ratio = min(1.0, idx / max(1, total_work))
            progress_bar.progress(ratio)
        if progress_text is not None:
            progress_text.caption(
                f"処理中... 対象 {stats['parent_scanned']} / {total_work} 件 / "
                f"アイコン更新 {stats['parent_patched']} / "
                f"絵文字暫定 {stats['parent_emoji_fallback']} / "
                f"失敗 {stats['parent_failed']}"
            )
        if target_icon is None:
            stats["parent_failed"] += 1
            stats["details"].append({
                "status": "failed-no-custom-emoji",
                "media": media,
                "title": get_title(p.get("properties", {}))[0] or get_title(p.get("properties", {}))[1] or "(無題)",
                "page_id": page_id,
                "reason": icon_meta.get("error", "custom-emoji-unresolved"),
                "normalized": icon_meta.get("normalized", ""),
            })
            continue
        res = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=NOTION_HEADERS,
            json={"icon": target_icon},
        )
        if res is not None and res.status_code == 200:
            stats["parent_patched"] += 1
            stats["details"].append({
                "status": "external_ok",
                "icon_type": (target_icon.get("type") if isinstance(target_icon, dict) else ""),
                "media": media,
                "title": get_title(p.get("properties", {}))[0] or get_title(p.get("properties", {}))[1] or "(無題)",
                "page_id": page_id,
            })
        else:
            stats["parent_failed"] += 1
            err_text = ""
            if res is not None:
                try:
                    body = res.json() or {}
                    err_text = body.get("message", "") or body.get("code", "")
                except Exception:
                    err_text = (res.text or "")[:180]
            stats["details"].append({
                "status": "failed",
                "media": media,
                "title": get_title(p.get("properties", {}))[0] or get_title(p.get("properties", {}))[1] or "(無題)",
                "page_id": page_id,
                "status_code": getattr(res, "status_code", None) if res is not None else None,
                "reason": err_text,
                "normalized": icon_meta.get("normalized", ""),
                "custom_emoji_id": (target_icon.get("custom_emoji") or {}).get("id") if isinstance(target_icon, dict) else "",
            })

    stats["limit"] = limit
    stats["total_in_db"] = total_pages
    stats["pending_before_run"] = len(work_items)
    stats["pending_total"] = pending_all
    if progress_bar is not None:
        progress_bar.progress(1.0)
    if progress_text is not None:
        progress_text.caption(
            f"完了: 対象 {stats['parent_scanned']} 件 / "
            f"アイコン更新 {stats['parent_patched']} / "
            f"絵文字暫定 {stats['parent_emoji_fallback']} / "
            f"失敗 {stats['parent_failed']}"
        )
    append_operation_audit_log("emergency_restore_all_media_icons", {
        "parent_scanned": stats.get("parent_scanned", 0),
        "parent_patched": stats.get("parent_patched", 0),
        "parent_emoji_fallback": stats.get("parent_emoji_fallback", 0),
        "parent_failed": stats.get("parent_failed", 0),
        "limit": stats.get("limit", 0),
        "pending_total": stats.get("pending_total", 0),
    })
    return stats

def force_restore_parent_media_icons_as_emoji(progress_bar=None, progress_text=None, limit: int = 120) -> dict:
    """親DBを媒体絵文字で強制復旧（外部URLが視覚的に壊れている時の最終手段）。"""
    stats = {"scanned": 0, "patched": 0, "failed": 0, "total_in_db": 0}
    parent_pages = query_notion_database_all(NOTION_DB_ID) or []
    stats["total_in_db"] = len(parent_pages)
    work = []
    for p in parent_pages:
        media = get_page_media(p)
        emoji = get_media_icon_emoji(media)
        page_id = p.get("id")
        if not (emoji and page_id):
            continue
        work.append((p, media, page_id, emoji))
    if limit and limit > 0:
        work = work[:limit]
    total = len(work)
    if progress_bar is not None:
        progress_bar.progress(0.0)
    for idx, (p, media, page_id, emoji) in enumerate(work, start=1):
        stats["scanned"] += 1
        if progress_bar is not None:
            progress_bar.progress(min(1.0, idx / max(1, total)))
        if progress_text is not None:
            progress_text.caption(f"絵文字復旧中... {idx}/{total}")
        res = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=NOTION_HEADERS,
            json={"icon": {"type": "emoji", "emoji": emoji}},
        )
        if res is not None and res.status_code == 200:
            stats["patched"] += 1
        else:
            stats["failed"] += 1
    append_operation_audit_log("force_restore_parent_media_icons_as_emoji", stats)
    return stats

def migrate_drive_cover_urls(pages: list[dict]) -> dict:
    """既存ページのDriveカバーURLをNotion表示安定形式に更新する。"""
    stats = {"scanned": 0, "patched": 0, "failed": 0}
    for p in (pages or []):
        cover_url = (((p.get("cover") or {}).get("external") or {}).get("url") or "").strip()
        if not cover_url:
            continue
        # すでに安定形式ならスキップ
        if "drive.google.com/thumbnail?id=" in cover_url:
            continue

        file_id = None
        m = re.search(r"[?&]id=([A-Za-z0-9_-]+)", cover_url)
        if m:
            file_id = m.group(1)
        if not file_id:
            m = re.search(r"/file/d/([A-Za-z0-9_-]+)", cover_url)
            if m:
                file_id = m.group(1)
        if not file_id:
            continue

        stats["scanned"] += 1
        new_url = drive_image_url(file_id)
        res = api_request(
            "patch",
            f"https://api.notion.com/v1/pages/{p.get('id')}",
            headers=NOTION_HEADERS,
            json={"cover": {"type": "external", "external": {"url": new_url}}},
        )
        if res is not None and res.status_code == 200:
            stats["patched"] += 1
            try:
                upsert_page_in_state(res.json())
            except Exception:
                pass
        else:
            stats["failed"] += 1
    return stats

def analyze_performance_relation_integrity(force_refresh: bool = False) -> dict:
    ctx = {
        "NOTION_PERFORMANCE_CAST_DB_ID": NOTION_PERFORMANCE_CAST_DB_ID,
        "NOTION_SCORE_DB_ID": NOTION_SCORE_DB_ID,
        "NOTION_SONG_ASSIGN_DB_ID": NOTION_SONG_ASSIGN_DB_ID,
        "NOTION_PERFORMER_DB_ID": NOTION_PERFORMER_DB_ID,
        "get_performance_pages": _get_performance_pages,
        "query_notion_database_all": query_notion_database_all,
        "get_notion_db_property_types": get_notion_db_property_types,
        "pick_prop_name": _pick_prop_name,
        "extract_relation_ids": _extract_relation_ids,
        "extract_page_title_by_type": _extract_page_title_by_type,
        "extract_name_title": _extract_name_title,
        "normalize_person_name": _normalize_person_name,
        "tail_person_name": _tail_person_name,
        "plain_text_join": plain_text_join,
    }
    return _analyze_reconcile_service(ctx, force_refresh=force_refresh)

def run_performance_relation_repair(report: dict, mode: str = "partial") -> tuple[dict, list[str]]:
    ctx = {
        "api_request": api_request,
        "NOTION_HEADERS": NOTION_HEADERS,
    }
    return _run_reconcile_repair_service(ctx, report, mode=mode)

def is_japanese_name(name: str) -> bool:
    """漢字・ひらがな・カタカナを含む場合は日本語著者名とみなす"""
    return bool(re.search(r'[\u3000-\u9fff\uff00-\uffef]', name))

def normalize_name_for_compare(name: str) -> str:
    """比較用正規化: 括弧・接尾語除去後、日本語著者名のみ空白を全除去"""
    name = clean_author(name)  # 括弧・接尾語除去＋スペース半角化
    if is_japanese_name(name):
        name = re.sub(r'\s+', '', name)  # 空白全除去
    return name.lower()

def get_registered_ids(pages: list) -> dict:
    """登録済みIDと書籍/漫画/音楽の正規化済みキーをまとめて返す"""
    tmdb, anilist, igdb, itunes, isbn = set(), set(), set(), set(), set()
    book_keys   = set()  # (正規化タイトル, 正規化著者) — 書籍・漫画
    album_keys  = set()  # (正規化タイトル, 正規化アーティスト) — 音楽アルバム
    for p in pages:
        pr = p["properties"]
        v = (pr.get("TMDB_ID") or {}).get("number")
        if v: tmdb.add(int(v))
        v = (pr.get("AniList_ID") or {}).get("number")
        if v: anilist.add(int(v))
        v = (pr.get("IGDB_ID") or {}).get("number")
        if v: igdb.add(int(v))
        v = (pr.get("iTunes_ID") or {}).get("number")
        if v: itunes.add(int(v))
        v = plain_text_join((pr.get("ISBN") or {}).get("rich_text", []))
        if v: isbn.add(v)
        media_val = get_page_media(p)
        if media_val in ("書籍", "漫画"):
            raw_title = plain_text_join((pr.get("タイトル") or {}).get("title", []))
            raw_creator = plain_text_join((pr.get("クリエイター") or {}).get("rich_text", []))
            norm_title = re.sub(r'\s*[\(（]?\d+[\)）]?\s*$', '', raw_title).strip().lower()
            for author in re.split(r'[/／・]', raw_creator):
                author = author.strip()
                if author:
                    book_keys.add((norm_title, normalize_name_for_compare(author)))
        elif media_val == "音楽アルバム":
            raw_title = plain_text_join((pr.get("タイトル") or {}).get("title", []))
            raw_creator = plain_text_join((pr.get("クリエイター") or {}).get("rich_text", []))
            norm_title = raw_title.strip().lower()
            norm_artist = normalize_name_for_compare(raw_creator)
            if norm_title and norm_artist:
                album_keys.add((norm_title, norm_artist))
    return {"tmdb": tmdb, "anilist": anilist, "igdb": igdb, "itunes": itunes,
            "isbn": isbn, "book_keys": book_keys, "album_keys": album_keys}

def filter_registered(results: list, media_label: str, reg_ids: dict):
    """検索結果から登録済みを除外。(filtered, excluded_titles) を返す"""
    filtered, excluded = [], []
    for cand in results:
        cid   = cand.get("id")
        title = cand.get("title") or cand.get("name") or ""
        dup   = False
        if media_label in ("映画", "ドラマ"):
            dup = bool(cid and int(cid) in reg_ids["tmdb"])
        elif media_label == "アニメ":
            dup = bool(cid and int(cid) in reg_ids["anilist"])
        elif media_label == "ゲーム":
            dup = bool(cid and int(cid) in reg_ids["igdb"])
        elif media_label == "音楽アルバム":
            # iTunes_IDで除外、なければタイトル×アーティスト正規化で除外
            if cid and int(cid) in reg_ids["itunes"]:
                dup = True
            else:
                norm_title  = title.strip().lower()
                norm_artist = normalize_name_for_compare(cand.get("artist", ""))
                dup = (norm_title, norm_artist) in reg_ids["album_keys"]
        elif media_label in ("書籍", "漫画"):
            # ISBNで除外、なければタイトル×著者正規化で除外（日本語著者名のみ空白除去）
            if cand.get("isbn") and cand.get("isbn") in reg_ids["isbn"]:
                dup = True
            else:
                norm_title = re.sub(r'\s*[\(（]?\d+[\)）]?\s*$', '', title).strip().lower()
                for author in cand.get("authors", []):
                    norm_author = normalize_name_for_compare(author)
                    if (norm_title, norm_author) in reg_ids["book_keys"]:
                        dup = True
                        break
        if dup:
            excluded.append(title)
        else:
            filtered.append(cand)
    return filtered, excluded


def _get_score_pages(force_refresh: bool = False) -> list[dict]:
    """演奏曲ページ一覧を取得（[{id, title}]）。セッションキャッシュあり。"""
    if (not force_refresh) and "score_pages_cache" in st.session_state:
        return st.session_state.score_pages_cache
    if force_refresh:
        pages = load_notion_data()
        if st.session_state.get("last_notion_load_ok", True):
            st.session_state.all_pages = pages
            st.session_state.pages = filter_target_pages(pages)
            st.session_state.pages_loaded = True
    elif st.session_state.get("pages_loaded") and st.session_state.get("pages"):
        pages = st.session_state.pages
    else:
        pages = load_notion_data()
        if st.session_state.get("last_notion_load_ok", True):
            st.session_state.all_pages = pages
            st.session_state.pages = filter_target_pages(pages)
            st.session_state.pages_loaded = True
    score_pages = []
    for p in pages:
        if get_page_media(p) == "演奏曲":
            title = get_title(p["properties"])[0]
            score_pages.append({"id": p["id"], "title": title})
    st.session_state.score_pages_cache = score_pages
    return score_pages


def _add_score_page_cache(page_id: str, title: str):
    if not page_id:
        return
    cache = st.session_state.get("score_pages_cache", [])
    if not any(x.get("id") == page_id for x in cache):
        cache.append({"id": page_id, "title": title})
        st.session_state.score_pages_cache = cache


def _find_score_page_by_title(score_pages: list[dict], title: str) -> dict | None:
    t = (title or "").strip().lower()
    if not t:
        return None
    for p in score_pages:
        if (p.get("title") or "").strip().lower() == t:
            return p
    return None


def _get_performance_pages(force_refresh: bool = False) -> list[dict]:
    """出演ページ一覧を取得（[{id, title}]）。セッションキャッシュあり。"""
    if (not force_refresh) and "performance_pages_cache" in st.session_state:
        return st.session_state.performance_pages_cache
    if force_refresh:
        pages = load_notion_data()
        if st.session_state.get("last_notion_load_ok", True):
            st.session_state.all_pages = pages
            st.session_state.pages = filter_target_pages(pages)
            st.session_state.pages_loaded = True
    elif st.session_state.get("pages_loaded") and st.session_state.get("pages"):
        pages = st.session_state.pages
    else:
        pages = load_notion_data()
        if st.session_state.get("last_notion_load_ok", True):
            st.session_state.all_pages = pages
            st.session_state.pages = filter_target_pages(pages)
            st.session_state.pages_loaded = True
    perf_pages = []
    for p in pages:
        if get_page_media(p) == "出演":
            title = get_title(p["properties"])[0]
            perf_pages.append({"id": p["id"], "title": title})
    st.session_state.performance_pages_cache = perf_pages
    return perf_pages


def _add_performance_page_cache(page_id: str, title: str):
    if not page_id:
        return
    cache = st.session_state.get("performance_pages_cache", [])
    if not any(x.get("id") == page_id for x in cache):
        cache.append({"id": page_id, "title": title})
        st.session_state.performance_pages_cache = cache


def _clean_relation_ids(ids: list | None) -> list[str]:
    return _clean_relation_ids_service(ids)

def _prune_selected_relations(selected: list[dict], valid_pages: list[dict]) -> list[dict]:
    return _prune_selected_relations_service(selected, valid_pages)


def _get_page_from_state_or_api(page_id: str, force_api: bool = False) -> dict | None:
    if not page_id:
        return None
    if not force_api:
        for p in st.session_state.get("pages", []):
            if p.get("id") == page_id:
                return p
    res = api_request("get", f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS)
    if res is not None and res.status_code == 200:
        page = res.json()
        upsert_page_in_state(page)
        return page
    return None


def _extract_performance_defaults(page: dict | None) -> tuple[str, str, str, dict | None]:
    """演奏会（出演）ページから演奏曲用の初期値を抽出"""
    if not page:
        return "", "", "", None
    props = page.get("properties", {})
    release = ((props.get("リリース日") or {}).get("date") or {}).get("start", "") or ""
    watched = get_experience_date_from_props(props)
    rating = ((props.get("評価") or {}).get("select") or {}).get("name", "") or ""
    place = (props.get("ロケーション") or {}).get("place") or None
    location = None
    if isinstance(place, dict) and place.get("lat") and place.get("lon"):
        location = {
            "lat": place.get("lat"),
            "lon": place.get("lon"),
            "name": place.get("name", ""),
            "address": place.get("address", ""),
        }
    return release, watched, rating, location

def _suggest_next_setlist_order(performance_page_id: str) -> int:
    """既存の演奏曲DBから該当出演の最大曲順+1を返す（区分に依らず）。"""
    perf_id = (performance_page_id or "").strip()
    if not perf_id or not NOTION_SCORE_DB_ID:
        return 1
    try:
        rows = query_notion_database_all(NOTION_SCORE_DB_ID)
    except Exception:
        return 1

    max_order = 0
    for row in rows:
        props = row.get("properties", {}) or {}
        linked = False
        for meta in props.values():
            if isinstance(meta, dict) and meta.get("type") == "relation":
                rels = meta.get("relation") or []
                if any((r.get("id") or "") == perf_id for r in rels):
                    linked = True
                    break
        if not linked:
            continue
        order_num = None
        if isinstance(props.get("曲順"), dict) and (props.get("曲順") or {}).get("type") == "number":
            order_num = (props.get("曲順") or {}).get("number")
        if order_num is None:
            for key, meta in props.items():
                if not (isinstance(meta, dict) and meta.get("type") == "number"):
                    continue
                if ("順" in key) or ("order" in key.lower()):
                    order_num = meta.get("number")
                    break
        try:
            if order_num is not None:
                max_order = max(max_order, int(order_num))
        except Exception:
            continue
    return max(max_order + 1, 1)


def _focus_management_page(page_id: str, title: str, media_label: str | None = None):
    if not page_id:
        return
    st.session_state.focus_page_id = page_id
    st.session_state.pending_focus_page_id = page_id
    st.session_state.manual_page = 0
    st.session_state.pending_force_scroll_top = True
    # manual_search_query(widget key) は生成後に直接更新できないため、次runで反映する
    st.session_state["pending_manual_search_query"] = title or ""
    # サイドバー媒体フィルタで新規作成ページが隠れないようにする
    current_filter = st.session_state.get("sidebar_media_filter", [])
    if media_label:
        if current_filter and media_label not in current_filter:
            st.session_state["pending_sidebar_media_filter"] = list(current_filter) + [media_label]

def check_duplicate(tmdb_id: int, pages: list) -> list:
    """TMDB_IDが一致する既存ページを返す"""
    return [p for p in pages if p["properties"].get("TMDB_ID", {}).get("number") == tmdb_id]

def build_update_log(log_title, src, need_notion, notion_ok, need_drive, drive_ok, meta_ok, updated, is_refresh=False) -> str:
    return _build_update_log_service(
        log_title=log_title,
        src=src,
        need_notion=need_notion,
        notion_ok=notion_ok,
        need_drive=need_drive,
        drive_ok=drive_ok,
        meta_ok=meta_ok,
        updated=updated,
        is_refresh=is_refresh,
    )

# ============================================================
# アプリ初期化
# ============================================================

st.set_page_config(page_title="ArtéMis MUSE", page_icon=get_asset_path_or_url("favicon.png"), layout="wide")

# ── PWA対応 metaタグ ──
st.markdown("""
<head>
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="ArtéMis MUSE">
<meta name="theme-color" content="#0e1117">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<link rel="apple-touch-icon" href="https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/favicon.png">
</head>
""", unsafe_allow_html=True)

st.image(get_asset_path_or_url("logo.png"), width=320)
st.markdown(
    "<em><strong>ArtéMis</strong></em> — named after the goddess of the hunt and the moon. "
    "She keeps track of everything you've ever experienced — and everything you've ever played.",
    unsafe_allow_html=True
)
st.caption(
    "MUSE: Media Unified Sourcing Engine / "
    "ATLAS: Archive of Titles, Life, Art and Sound / "
    "APOLLO: Archive of Performed Opuses, Live Logs, and Occasions / "
    "HARMONIA: Harmonized Assignment and Resource Management for Orchestral Needs, Instruments, and Attendance"
)
st.caption("MUSE collects. ATLAS archives. APOLLO performs. HARMONIA orchestrates.")
st.caption(f"v{APP_VERSION}")

# ── 奏者フォームモード（?concert=TOKEN&cid=CONCERT_ID） ──────
_qp = st.query_params
if "concert" in _qp and "cid" in _qp:
    try:
        from concert.pages.form import verify_form_token, render_form
        from concert.services.notion_client import build_concert_ctx
        _token = _qp["concert"]
        _cid   = _qp["cid"]
        if verify_form_token(_token, _cid):
            try:
                _form_ctx = build_concert_ctx()
                render_form(_form_ctx, _cid)
            except Exception as _e:
                st.error(f"フォームの読み込みに失敗しました: {_e}")
        else:
            st.error("URLが無効です。正しいURLを使用してください。")
    except Exception as _e:
        st.error(f"フォームの初期化に失敗しました: {_e}")
    st.stop()

# ============================================================
# システム切替（通常 / Concert）
# ============================================================
if st.session_state.get("system_mode") == "通常":
    st.session_state["system_mode"] = "MUSE"
elif st.session_state.get("system_mode") == "Concert":
    st.session_state["system_mode"] = "HARMONIA"

_prev_mode = st.session_state.get("_prev_system_mode", "")
system_mode = st.sidebar.radio("システム切替", ["MUSE", "HARMONIA"], key="system_mode")
if system_mode != _prev_mode:
    # モード切替時にHARMONIA関連のセッションキャッシュをクリア
    for _k in list(st.session_state.keys()):
        if any(_k.startswith(p) for p in (
            "practice_list_", "concert_list", "rental_concert_list",
            "rental_calc_results", "confirmed_rows_", "song_list_",
            "partdef_list_", "pi_list_", "attendance_list_",
            "participant_list_", "instrument_list",
            "schedule_list_", "practice_editor_version",
            "rental_list_",
        )):
            st.session_state.pop(_k, None)
    st.session_state["_prev_system_mode"] = system_mode
if system_mode == "HARMONIA":
    st.sidebar.caption("ArtéMis HARMONIA")
    st.sidebar.divider()
    concert_page = st.sidebar.radio(
        "ページ",
        [
            "🏠 ホーム",
            "練習管理",
            "楽曲・楽器管理",
            "奏者・出欠・持参楽器",
            "アサイン検討",
            "レンタル管理",
            "収支・振込管理",
            "🧪 テストデータ管理",
        ],
        key="concert_page_radio",
    )
    if not CONCERT_SYSTEM_AVAILABLE:
        st.error("HARMONIA System のモジュールを読み込めませんでした。")
        if CONCERT_IMPORT_ERROR:
            st.caption(f"詳細: {CONCERT_IMPORT_ERROR}")
        st.stop()
    try:
        concert_ctx = build_concert_ctx()
    except KeyError as e:
        st.error(f"HARMONIA System の設定が不足しています。secrets.toml を確認してください。（{e}）")
        st.stop()

    # HARMONIA契約チェック（品質重視: 先に設定齟齬を可視化）
    _contract_check = {}
    try:
        _contract_check = concert_ctx.get("validate_contract", lambda: {})() or {}
    except Exception as _contract_e:
        st.warning(f"⚠️ HARMONIA契約チェックの実行に失敗しました: {_contract_e}")
    if _contract_check:
        _contract_errors = _contract_check.get("errors", []) or []
        _contract_warnings = _contract_check.get("warnings", []) or []
        if _contract_errors:
            st.error("⚠️ HARMONIA設定に不整合があります。保存系処理で失敗する可能性があります。")
            with st.expander("詳細（契約チェック）", expanded=False):
                for _m in _contract_errors:
                    st.write(f"- {_m}")
                for _m in _contract_warnings:
                    st.write(f"- {_m}")
        elif _contract_warnings:
            with st.expander("ℹ️ HARMONIA設定チェック（警告）", expanded=False):
                for _m in _contract_warnings:
                    st.write(f"- {_m}")

    # HARMONIA共通: 演奏会を先に1つ選び、各画面はその演奏会だけを対象にする

    @st.cache_data(ttl=300, show_spinner=False)
    def _load_harmonia_concerts(_api_key: str, _db_id: str) -> list[dict]:
        """ATLASから媒体=出演のみをAPI側フィルタで取得（キャッシュ付き）。"""
        from concert.services.notion_client import query_concert_db_all, get_concert_headers
        headers = get_concert_headers(_api_key)
        return query_concert_db_all(_db_id, headers, {
            "filter": {
                "property": "媒体",
                "multi_select": {"contains": "出演"},
            }
        })

    def _harmony_concert_name(page: dict) -> str:
        name = (
            concert_ctx["extract_prop_text_any"](page, ["名称", "演奏会名", "タイトル", "PK名称"])
            or concert_ctx["extract_title"](page)
        )
        dt = concert_ctx["extract_prop_text_any"](page, ["日時", "日付", "出演日", "体験日", "リリース日"])
        return f"{name}（{dt[:10] if dt else '日時未設定'}）"

    def _cleanup_harmonia_smoketest_pages() -> dict:
        result = {
            "archived_atlas": 0,
            "archived_apollo": 0,
            "archived_assign": 0,
            "archived_cast": 0,
            "archived_performer": 0,
            "failed": 0,
            "atlas_target_ids": [],
            "apollo_target_ids": [],
            "assign_target_ids": [],
            "cast_target_ids": [],
            "performer_target_ids": [],
        }

        def _archive_page(page_id: str) -> bool:
            res = api_request(
                "patch",
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=NOTION_HEADERS,
                json={"archived": True},
            )
            return res is not None and res.status_code == 200

        def _collect_targets(db_id: str) -> list[dict]:
            if not db_id:
                return []
            return query_notion_database_all(db_id) or []

        for pg in _collect_targets(NOTION_DB_ID):
            props = pg.get("properties", {}) or {}
            title, _, _ = get_title(props)
            if not str(title or "").startswith("[SMOKETEST] "):
                continue
            pid = pg.get("id", "")
            if not pid:
                continue
            result["atlas_target_ids"].append(pid)
            if _archive_page(pid):
                result["archived_atlas"] += 1
            else:
                result["failed"] += 1

        for pg in _collect_targets(NOTION_SCORE_DB_ID):
            props = pg.get("properties", {}) or {}
            title, _, _ = get_title(props)
            if not str(title or "").startswith("[SMOKETEST] "):
                continue
            pid = pg.get("id", "")
            if not pid:
                continue
            result["apollo_target_ids"].append(pid)
            if _archive_page(pid):
                result["archived_apollo"] += 1
            else:
                result["failed"] += 1

        for pg in _collect_targets(NOTION_SONG_ASSIGN_DB_ID):
            props = pg.get("properties", {}) or {}
            title, _, _ = get_title(props)
            if not str(title or "").startswith("[SMOKETEST] "):
                continue
            pid = pg.get("id", "")
            if not pid:
                continue
            result["assign_target_ids"].append(pid)
            if _archive_page(pid):
                result["archived_assign"] += 1
            else:
                result["failed"] += 1

        for pg in _collect_targets(NOTION_PERFORMANCE_CAST_DB_ID):
            props = pg.get("properties", {}) or {}
            title, _, _ = get_title(props)
            title = title or plain_text_join(((props.get("タイトル") or {}).get("title") or []))
            if not str(title or "").startswith("[SMOKETEST] "):
                continue
            pid = pg.get("id", "")
            if not pid:
                continue
            result["cast_target_ids"].append(pid)
            if _archive_page(pid):
                result["archived_cast"] += 1
            else:
                result["failed"] += 1

        for pg in _collect_targets(NOTION_PERFORMER_DB_ID):
            props = pg.get("properties", {}) or {}
            title, _, _ = get_title(props)
            title = title or extract_name_title(pg) or ""
            if not str(title or "").startswith("[SMOKETEST] "):
                continue
            pid = pg.get("id", "")
            if not pid:
                continue
            result["performer_target_ids"].append(pid)
            if _archive_page(pid):
                result["archived_performer"] += 1
            else:
                result["failed"] += 1

        return result

    def _run_performance_registration_e2e_smoketest() -> dict:
        """
        既存の『出演』登録フローを最小構成でE2E試験する。
        1) ATLAS に演奏曲ページを作成
        2) ATLAS に出演演奏会ページを作成（演奏曲 relation 付き）
        3) APOLLO 演奏曲DB作成を実行
        4) 演奏会参加者DBを1件作成
        5) 楽曲別担当者DBを1件作成できるか確認
        """
        result = {
            "ok": False,
            "error": "",
            "score_page_id": "",
            "performance_page_id": "",
            "performer_name": "",
            "selected_scores": [],
            "main_items": [],
            "encore_items": [],
            "created_setlist": 0,
            "failed_setlist": 0,
            "setlist_reason": "",
            "created_rows": [],
            "created_cast": 0,
            "failed_cast": 0,
            "cast_reason": "",
            "cast_row_map": {},
            "created_assign": 0,
            "failed_assign": 0,
            "assign_reason": "",
            "title": "",
            "score_title": "",
        }

        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        perf_title = f"[SMOKETEST] HARMONIA E2E {stamp}"
        score_title = f"[SMOKETEST] Test Piece {stamp}"
        performer_name = f"[SMOKETEST] Player {stamp}"
        result["title"] = perf_title
        result["score_title"] = score_title
        result["performer_name"] = performer_name

        score_ok = create_notion_page(
            jp_title=score_title,
            en_title=score_title,
            media_type_label="演奏曲",
            tmdb_id=None,
            media_type="score",
            cover_url=get_media_icon_url("演奏曲"),
            tmdb_release="1913-05-29",
            details={"genres": [], "cast": "", "director": "Igor Stravinsky", "score": None},
            wlflg=False,
            watched_date=None,
            rating=None,
            memo="[SMOKETEST] score seed",
        )
        if not score_ok:
            result["error"] = "ATLAS 演奏曲ページの作成に失敗しました。"
            return result

        score_page_id = st.session_state.get("last_created_page_id", "")
        result["score_page_id"] = score_page_id
        if not score_page_id:
            result["error"] = "ATLAS 演奏曲ページIDを取得できませんでした。"
            return result

        perf_ok = create_notion_page(
            jp_title=perf_title,
            en_title=perf_title,
            media_type_label="出演",
            tmdb_id=None,
            media_type="event",
            cover_url=get_media_icon_url("出演"),
            tmdb_release="2099-12-31",
            details={"genres": ["テスト"], "cast": "", "director": "HARMONIA SmokeTest", "score": None},
            wlflg=False,
            watched_date="2099-12-31",
            rating=None,
            event_end="2099-12-31",
            location={"name": "HARMONIA SmokeTest Hall", "lat": None, "lon": None},
            memo="[SMOKETEST] performance seed",
            relation_prop="演奏曲",
            relation_ids=[score_page_id],
        )
        if not perf_ok:
            result["error"] = "ATLAS 出演演奏会ページの作成に失敗しました。"
            return result

        performance_page_id = st.session_state.get("last_created_page_id", "")
        result["performance_page_id"] = performance_page_id
        if not performance_page_id:
            result["error"] = "ATLAS 出演演奏会ページIDを取得できませんでした。"
            return result

        selected_scores = [{
            "id": score_page_id,
            "title": score_title,
            "composer": "Igor Stravinsky",
            "composer_country": "",
        }]
        main_items = [{
            "title": score_title,
            "order": 1,
            "part": "Timpani",
            "played": True,
            "players": [performer_name],
            "section": "本編",
            "composer": "Igor Stravinsky",
            "composer_country": "",
            "movement_name": "",
            "movement_no": None,
            "movement_order": None,
            "movement_roman": "",
        }]
        encore_items = []

        result["selected_scores"] = selected_scores
        result["main_items"] = main_items
        result["encore_items"] = encore_items

        created_setlist, failed_setlist, setlist_reason, created_rows = create_setlist_rows_for_performance(
            performance_page_id=performance_page_id,
            performance_title=perf_title,
            performance_date="2099-12-31",
            main_items=main_items,
            encore_items=encore_items,
            selected_scores=selected_scores,
            score_pages=[],
        )
        result["created_setlist"] = created_setlist
        result["failed_setlist"] = failed_setlist
        result["setlist_reason"] = setlist_reason
        result["created_rows"] = created_rows

        participants = [{
            "name": performer_name,
            "instruments": "Timpani",
            "memo": "[SMOKETEST] cast seed",
        }]
        created_cast, failed_cast, cast_reason, cast_row_map = create_performance_participant_rows(
            performance_page_id=performance_page_id,
            performance_title=perf_title,
            participants=participants,
        )
        result["created_cast"] = created_cast
        result["failed_cast"] = failed_cast
        result["cast_reason"] = cast_reason
        result["cast_row_map"] = cast_row_map

        created_assign = 0
        failed_assign = 0
        assign_reason = ""
        if created_rows and cast_row_map:
            created_assign, failed_assign, assign_reason = create_song_assignment_rows(
                score_rows=created_rows,
                cast_row_map=cast_row_map,
            )
        result["created_assign"] = created_assign
        result["failed_assign"] = failed_assign
        result["assign_reason"] = assign_reason

        result["ok"] = (
            created_setlist > 0
            and failed_setlist == 0
            and created_cast > 0
            and failed_cast == 0
            and created_assign > 0
            and failed_assign == 0
        )
        return result


    selected_concert_id = concert_ctx.get("SELECTED_CONCERT_ID", "").strip()
    selected_concert_row = next((r for r in concert_rows if r.get("id", "") == selected_concert_id), None)

    if concert_page == "🏠 ホーム":
        st.header("🏠 HARMONIAホーム")
        st.caption("演奏会の選択・変更はサイドバーで行います。")
        if selected_concert_row:
            st.markdown("### 現在選択中の演奏会")
            st.markdown(f"**{_harmony_concert_name(selected_concert_row)}**")
        else:
            st.info("サイドバーの『演奏会フィルタ』で演奏会を選択してください。")
        st.stop()

    if concert_page == "🧪 テストデータ管理":
        st.header("🧪 テストデータ管理")
        st.caption("演奏会未選択でも利用できます。")
        with st.expander("🧪 出演登録E2Eテスト", expanded=False):
            st.caption("既存の『出演』登録フローを最小構成で1ボタン実行し、ATLASの出演演奏会/演奏曲と APOLLO 演奏曲DBの同時登録を確認します。")
            col_run, col_cleanup = st.columns(2)
            if col_run.button("▶ 出演登録E2Eテストを実行", type="primary", use_container_width=True, key="harmonia_e2e_smoketest_run"):
                smoke = _run_performance_registration_e2e_smoketest()
                st.session_state["harmonia_e2e_smoketest_result"] = smoke
                if smoke.get("ok"):
                    st.success(f"✅ E2Eテスト成功: APOLLO {smoke.get('created_setlist', 0)} 件")
                else:
                    st.warning(
                        f"⚠️ E2Eテスト: APOLLO created={smoke.get('created_setlist', 0)} / failed={smoke.get('failed_setlist', 0)}"
                        + (f" / {smoke.get('setlist_reason','')}" if smoke.get('setlist_reason') else "")
                    )
                    if smoke.get("error"):
                        st.error(smoke["error"])
            if col_cleanup.button("🧹 SMOKETESTデータ削除", use_container_width=True, key="harmonia_e2e_smoketest_cleanup"):
                clean = _cleanup_harmonia_smoketest_pages()
                st.session_state["harmonia_e2e_smoketest_cleanup_result"] = clean
                if clean.get("failed", 0) == 0:
                    st.success(f"✅ SMOKETESTデータをアーカイブしました（ATLAS {clean.get('archived_atlas', 0)} / APOLLO {clean.get('archived_apollo', 0)} / ASSIGN {clean.get('archived_assign', 0)} / CAST {clean.get('archived_cast', 0)} / PERFORMER {clean.get('archived_performer', 0)}）")
                else:
                    st.warning(f"⚠️ アーカイブ（ATLAS {clean.get('archived_atlas', 0)} / APOLLO {clean.get('archived_apollo', 0)} / ASSIGN {clean.get('archived_assign', 0)} / CAST {clean.get('archived_cast', 0)} / PERFORMER {clean.get('archived_performer', 0)}） / 失敗 {clean.get('failed', 0)} 件")

            smoke_result = st.session_state.get("harmonia_e2e_smoketest_result") or {}
            if smoke_result:
                st.json(smoke_result)
            cleanup_result = st.session_state.get("harmonia_e2e_smoketest_cleanup_result") or {}
            if cleanup_result:
                with st.expander("SMOKETEST削除結果", expanded=False):
                    st.json(cleanup_result)

        test_data.render(concert_ctx)
        st.stop()

    # ここから先は演奏会選択必須
    if not selected_concert_id:
        st.info("サイドバーの「演奏会フィルタ」で演奏会を選択してください。")
        st.stop()

    # サイドバー：演奏会サマリPDF出力
    concert_mgmt.render_sidebar_summary_pdf(concert_ctx)
    try:
        from concert.pages.form import render_url_generator
        with st.sidebar.expander("📋 奏者フォームURL", expanded=False):
            render_url_generator(concert_ctx,
                                 concert_ctx.get("SELECTED_CONCERT_ID",""),
                                 concert_ctx.get("SELECTED_CONCERT_NAME",""))
    except Exception:
        pass

    if concert_page == "練習管理":
        concert_mgmt.render(concert_ctx)
    elif concert_page == "楽曲・楽器管理":
        songs.render(concert_ctx)
    elif concert_page == "奏者・出欠・持参楽器":
        players.render(concert_ctx)
    elif concert_page == "アサイン検討":
        assign.render(concert_ctx)
    elif concert_page == "レンタル管理":
        rental.render(concert_ctx)
    elif concert_page == "収支・振込管理":
        finance.render(concert_ctx)

    st.stop()

if is_drive_skip_mode():
    st.info("⏭ Driveデータスキップ機能ON: Drive保存/照合はスキップして動作中です。")
if "pending_notice" in st.session_state:
    st.success(st.session_state.pop("pending_notice"))
    emit_scroll_top_script()
if "pending_warning" in st.session_state:
    st.warning(st.session_state.pop("pending_warning"))

if "api_connection_error_hint" in st.session_state:
    st.warning(st.session_state.pop("api_connection_error_hint"))
recent_api_errors = st.session_state.get("runtime_api_errors", [])
if recent_api_errors:
    with st.expander("⚠️ 直近APIエラー（最新20件）", expanded=False):
        for ev in reversed(recent_api_errors[-20:]):
            code_part = f"HTTP {ev.get('status_code')} / " if ev.get("status_code") else ""
            st.caption(f"{ev.get('time', '--:--:--')} [{ev.get('where', 'unknown')}] {code_part}{ev.get('message', '')}")
recent_ops = st.session_state.get("operation_reports", [])
if recent_ops:
    with st.expander("🧾 直近の処理ログ（最新10件）", expanded=False):
        for ev in reversed(recent_ops[-10:]):
            st.caption(f"{ev.get('time', '--:--:--')} [{ev.get('operation', 'operation')}] {ev.get('summary', '')}")
if st.session_state.pop("pending_force_scroll_top", False):
    emit_scroll_top_script()

for key, default in {
    "is_running":         False,
    "pages_loaded":       False,
    "new_search_excluded": [],
    "rakuten_page":        1,
    "rakuten_query_key":   "",
    "pages":              [],
    "all_pages":          [],
    "search_results":     {},
    "tmdb_id_cache":      {},
    "manual_page":        0,
    "sync_mode":          "normal",
    "new_search_results": [],
    "new_search_raw_count": 0,
    "new_search_done":    False,
    "confirm_reg":        None,
    "registering":        False,
    "mb_composers":       [],
    "mb_works":           [],
    "mb_checked":         {},
    "mb_selected_comp":   None,
    "mb_title_filter":    "",
    "mb_portrait_url":    None,
    "mb_portrait_comp":   None,
    "reg_cart":           [],
    "prev_media_label":   None,
    "bulk_checked":       {},
    "album_tracks_cache": [],
    "album_tracks_id":    None,
    "ev_setlist_main":    [],
    "ev_setlist_encore":  [],
    "ev_participants":    [],
    "refresh_targets_ids": [],
    "refresh_cursor":      0,
    "refresh_success_log": [],
    "refresh_maintain_log": [],
    "refresh_error_log":   [],
    "auto_reload_mode":    "partial",
    "created_pages":       [],
    "drive_skip_mode":     False,
    "drive_files_cache":   {},
    "drive_blocked_until": 0,
    "drive_last_error":    "",
    "test_register_mode":  False,
    "test_register_tag":   datetime.now().strftime("%Y%m%d-%H%M"),
    "app_mode":            "新規登録",
    "app_mode_widget":     "新規登録",
    "reconcile_report":    None,
    "reconcile_repair_mode": "partial",
    "refresh_maintenance_enabled": True,
    "refresh_maintenance_mode": "partial",
    "refresh_maintenance_scope": "auto",
    "refresh_touched_performance": False,
    "refresh_started_at": None,
    "refresh_last_seconds": None,
    "refresh_last_count": 0,
    "refresh_last_maintenance_seconds": None,
    "refresh_last_maintenance_applied": False,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.header("ナビゲーション")
    with st.expander("📘 操作ガイド", expanded=False):
        guide_md = load_user_guide_markdown()
        if guide_md:
            st.markdown(guide_md)
        else:
            st.info("`docs/USER_GUIDE.md` が見つかりません。")
        st.markdown("[GitHubで見る](https://github.com/attituderko-design/artemis-cers/blob/main/docs/USER_GUIDE.md)")

    st.divider()
    st.toggle("Drive連携を一時停止", key="drive_skip_mode")
    if st.session_state.get("drive_skip_mode"):
        st.caption("Drive連携は停止中です（判定/保存/一覧取得をスキップ）。")
    with st.expander("🧪 テスト登録モード", expanded=False):
        st.toggle("テスト登録を有効化", key="test_register_mode")
        st.text_input("テストタグ", key="test_register_tag", help="例: 20260319-001")
        tag_now = (st.session_state.get("test_register_tag") or "").strip()
        if st.session_state.get("test_register_mode"):
            st.caption(f"有効中: 新規登録に `[TEST]` と `TEST_TAG:{tag_now}` を付与します。")
        if st.button("🧹 このテストタグのデータを一括削除", key="delete_test_by_tag", use_container_width=True):
            if not tag_now:
                st.warning("テストタグを入力してください。")
            else:
                with st.spinner("テストデータを検索中..."):
                    pages = find_test_pages_by_tag(tag_now, max_pages=500)
                if not pages:
                    st.info("対象データは見つかりませんでした。")
                else:
                    parent_ids = [p.get("id") for p in pages if p.get("id")]
                    linked_score_ids = collect_related_score_ids_from_parent_pages(pages)
                    with st.spinner(f"ATLAS {len(parent_ids)} 件 + APOLLO関連 {len(linked_score_ids)} 件をアーカイブ中..."):
                        ok_parent, ng_parent = archive_pages_by_id(parent_ids)
                        ok_score, ng_score = archive_pages_by_id(linked_score_ids)
                    st.success(
                        f"完了: "
                        f"ATLAS {ok_parent} 件（失敗 {ng_parent}） / "
                        f"APOLLO関連 {ok_score} 件（失敗 {ng_score}）"
                    )
                    st.session_state.pop("score_pages_cache", None)
                    st.session_state.pop("performance_pages_cache", None)
    if "auto_reload_mode" not in st.session_state:
        st.session_state.auto_reload_mode = "partial"
    current_label = (
        "手動" if st.session_state.auto_reload_mode == "manual"
        else "自動（全件）" if st.session_state.auto_reload_mode == "full"
        else "半自動（該当ページ）"
    )
    st.radio(
        "保存後の反映方法",
        options=["手動", "自動（全件）", "半自動（該当ページ）"],
        index=["手動", "自動（全件）", "半自動（該当ページ）"].index(current_label),
        key="auto_reload_mode_display",
    )
    # normalize stored value to internal keys (do not write to widget key)
    display = st.session_state.get("auto_reload_mode_display", current_label)
    if display == "手動":
        st.session_state.auto_reload_mode = "manual"
    elif display == "自動（全件）":
        st.session_state.auto_reload_mode = "full"
    elif display == "半自動（該当ページ）":
        st.session_state.auto_reload_mode = "partial"
    if st.button("📥 最新データを読み込む", use_container_width=True, key="load_notion", type="primary"):
        with st.spinner("Notionからデータ取得中..."):
            all_pages = load_notion_data()
            if not st.session_state.get("last_notion_load_ok", True):
                st.session_state.pages_loaded = False
                st.error("Notion取得に失敗しました。接続設定やAPI制限をご確認ください。")
            else:
                st.session_state.all_pages      = all_pages
                st.session_state.pages          = filter_target_pages(all_pages)
                st.session_state.pages_loaded   = True
                st.session_state.search_results = {}
                st.session_state.manual_page    = 0
                st.session_state.pop("score_pages_cache", None)
                st.session_state.pop("performance_pages_cache", None)
                st.success(f"{len(st.session_state.pages)} 件取得しました（全媒体: {len(st.session_state.all_pages)} 件）")
    with st.expander("🧪 本番APIセルフテスト", expanded=False):
        st.caption("本番環境で Notion/Drive の疎通を確認します。書き込みテストはテストページを作成後すぐアーカイブします。")
        tcol1, tcol2 = st.columns(2)
        if tcol1.button("🔍 読み取りテスト", use_container_width=True, key="prod_selftest_read"):
            with st.spinner("本番API 読み取りテスト実行中..."):
                st.session_state["prod_selftest_report"] = run_production_api_selftest(enable_write=False)
        if tcol2.button("✍️ 書き込みテスト", use_container_width=True, key="prod_selftest_write"):
            with st.spinner("本番API 書き込みテスト実行中..."):
                st.session_state["prod_selftest_report"] = run_production_api_selftest(enable_write=True)
        rep = st.session_state.get("prod_selftest_report")
        if rep:
            st.json(rep)
    # 整備ツールは機能過多になりUXを損なっていたためUIからオミット

    if not st.session_state.pages_loaded:
        st.caption("👆 まず「最新データを読み込む」を実行してください")
        mode = "新規登録"  # dummy
        sync_scope = "欠損のみ補填"
        selected_media_filter = []
        diff_filter = "フィルタなし"
        delete_btn  = False
    else:
        loaded_count = len(st.session_state.pages)
        all_count    = len(st.session_state.all_pages)
        st.caption(f"✅ {loaded_count} 件取得済（全媒体: {all_count} 件）")

        st.divider()
        st.header("やりたいこと")
        if st.session_state.get("pending_focus_page_id"):
            st.session_state.app_mode = "データ管理"
            st.session_state.app_mode_widget = "データ管理"
        if "pending_app_mode" in st.session_state:
            st.session_state.app_mode = st.session_state.pop("pending_app_mode")
            st.session_state.app_mode_widget = st.session_state.app_mode
        if "app_mode_widget" not in st.session_state:
            st.session_state.app_mode_widget = st.session_state.get("app_mode", "新規登録")
        mode = st.radio("モード", ["新規登録", "出演アーカイブ", "データ管理", "出演情報管理", "自動同期"], key="app_mode_widget")
        st.session_state.app_mode = mode

        sync_scope = "欠損のみ補填"  # legacy compat
        if mode == "データ管理":
            if "manual_sort_order" not in st.session_state:
                st.session_state.manual_sort_order = EXPERIENCE_SORT_NEW
            current_sort = st.session_state.get("manual_sort_order", EXPERIENCE_SORT_NEW)
            st.session_state.manual_sort_order = LEGACY_SORT_LABEL_MAP.get(current_sort, current_sort)
            st.selectbox(
                "一覧ソート",
                options=[
                    EXPERIENCE_SORT_NEW,
                    EXPERIENCE_SORT_OLD,
                    "リリース日（新しい順）",
                    "リリース日（古い順）",
                    "更新日時（新しい順）",
                    "タイトル（A-Z）",
                    "媒体 → タイトル",
                ],
                key="manual_sort_order",
            )

        if mode in ("データ管理", "自動同期"):
            st.divider()
            st.header("媒体フィルタ")
            media_filter_options = list(MEDIA_ICON_MAP.keys())
            if "pending_sidebar_media_filter" in st.session_state:
                st.session_state["sidebar_media_filter"] = st.session_state.pop("pending_sidebar_media_filter")
            selected_media_filter = st.multiselect(
                "媒体を絞り込む",
                options=media_filter_options,
                default=[],
                label_visibility="collapsed",
                key="sidebar_media_filter",
            )
        else:
            selected_media_filter = []

        diff_filter = "フィルタなし"
        delete_btn  = False

        if mode == "自動同期":
            st.divider()
            st.toggle("リフレッシュ後に出演リンクを自動で整える", key="refresh_maintenance_enabled")
            rm_scope_label = (
                "常に実行" if st.session_state.get("refresh_maintenance_scope") == "always"
                else "出演/演奏曲を更新した時だけ実行（推奨）"
            )
            st.radio(
                "リンク整備の実行条件",
                options=["出演/演奏曲を更新した時だけ実行（推奨）", "常に実行"],
                index=["出演/演奏曲を更新した時だけ実行（推奨）", "常に実行"].index(rm_scope_label),
                key="refresh_maintenance_scope_display",
            )
            st.session_state.refresh_maintenance_scope = (
                "always"
                if st.session_state.get("refresh_maintenance_scope_display") == "常に実行"
                else "auto"
            )
            rm_label = (
                "手動（実行のみ）" if st.session_state.get("refresh_maintenance_mode") == "manual"
                else "自動（高確度＋重複整理）" if st.session_state.get("refresh_maintenance_mode") == "full"
                else "半自動（高確度のみ）"
            )
            st.radio(
                "リンク整備モード",
                options=["手動（実行のみ）", "半自動（高確度のみ）", "自動（高確度＋重複整理）"],
                index=["手動（実行のみ）", "半自動（高確度のみ）", "自動（高確度＋重複整理）"].index(rm_label),
                key="refresh_maintenance_mode_display",
            )
            disp_rm = st.session_state.get("refresh_maintenance_mode_display", rm_label)
            if disp_rm == "手動（実行のみ）":
                st.session_state.refresh_maintenance_mode = "manual"
            elif disp_rm == "自動（高確度＋重複整理）":
                st.session_state.refresh_maintenance_mode = "full"
            else:
                st.session_state.refresh_maintenance_mode = "partial"
            if st.button("🚀 自動同期", use_container_width=True):
                st.session_state.is_running = True
                st.session_state.sync_mode  = "normal"
                st.rerun()
            st.caption("IDを持つデータの不足項目だけを補います")
            if st.button("🔄 リフレッシュ", use_container_width=True):
                st.session_state.is_running = True
                st.session_state.sync_mode  = "refresh"
                st.session_state.refresh_cursor = 0
                st.session_state.refresh_targets_ids = []
                st.session_state.refresh_success_log = []
                st.session_state.refresh_maintain_log = []
                st.session_state.refresh_error_log = []
                st.session_state.refresh_touched_performance = False
                st.session_state.refresh_started_at = time.time()
                st.session_state.refresh_last_seconds = None
                st.session_state.refresh_last_count = 0
                st.session_state.refresh_last_maintenance_seconds = None
                st.session_state.refresh_last_maintenance_applied = False
                st.rerun()
            st.caption("IDを基に情報を再取得し、カバー/メタデータを再整備します")
            last_sec = st.session_state.get("refresh_last_seconds")
            if isinstance(last_sec, (int, float)):
                mm = int(last_sec // 60)
                ss = int(last_sec % 60)
                maint = st.session_state.get("refresh_last_maintenance_seconds")
                count = st.session_state.get("refresh_last_count", 0)
                if isinstance(maint, (int, float)):
                    st.caption(f"前回リフレッシュ: {mm:02d}:{ss:02d} / 対象 {count} 件 / 整合修復 {maint:.1f}s")
                else:
                    st.caption(f"前回リフレッシュ: {mm:02d}:{ss:02d} / 対象 {count} 件")
            if st.button("⏹ 停止", use_container_width=True):
                st.session_state.is_running = False
                st.rerun()

# ============================================================
# データ未取得ガード
# ============================================================
if not st.session_state.pages_loaded:
    st.stop()

target_pages = st.session_state.pages
if st.session_state.get("pending_focus_page_id"):
    mode = "データ管理"

# ============================================================
# 新規登録モード
# ============================================================
if mode == "新規登録":
    st.subheader("➕ 新規登録")
    tab_reg, tab_csv = st.tabs(["通常登録", "📥 CSVインポート"])

    # ============================================================
    # CSVインポートタブ
    # ============================================================
    with tab_csv:
        VALID_MEDIA   = list(MEDIA_ICON_MAP.keys())
        VALID_RATINGS = RATING_OPTIONS

        # ── テンプレートダウンロード ──
        import csv, io as _io
        CSV_COLUMNS = ["媒体", "タイトル", "英語タイトル", "体験日", "評価", "メモ", "場所"]
        template_buf = _io.StringIO()
        writer = csv.writer(template_buf)
        writer.writerow(CSV_COLUMNS)
        hint_row = [
            "／".join(VALID_MEDIA[:4]) + "／…",
            "例: 千と千尋の神隠し", "例: Spirited Away",
            "YYYY-MM-DD", "／".join(r for r in VALID_RATINGS if r),
            "自由記述", "例: 梅田ブルク7",
        ]
        writer.writerow(hint_row)
        template_bytes = template_buf.getvalue().encode("utf-8-sig")
        st.download_button(
            "📄 テンプレートをダウンロード",
            data=template_bytes,
            file_name="artemis_import_template.csv",
            mime="text/csv",
            use_container_width=True,
        )
        st.caption("2行目はヒント行です。削除してからデータを入力してください。")
        st.caption(f"有効な媒体値: {' / '.join(VALID_MEDIA)}")
        st.caption(f"有効な評価値: {' / '.join(r for r in VALID_RATINGS if r)}")

        st.divider()

        # ── CSVアップロード ──
        uploaded_csv = st.file_uploader("CSVファイルをアップロード", type=["csv"], key="csv_import_file")
        if uploaded_csv:
            import pandas as pd
            try:
                df = pd.read_csv(uploaded_csv, encoding="utf-8-sig", dtype=str).fillna("")
            except Exception as e:
                st.error(f"CSV読み込みエラー: {e}")
                df = None

            if df is not None:
                required_cols = ["媒体", "タイトル", "英語タイトル", "評価", "メモ", "場所"]
                missing_cols = [c for c in required_cols if c not in df.columns]
                has_date_col = ("体験日" in df.columns) or ("鑑賞日" in df.columns)
                if not has_date_col:
                    missing_cols.append("体験日(または鑑賞日)")
                if missing_cols:
                    st.error(f"列が不足しています: {missing_cols}")
                else:
                    # ── バリデーション ──
                    errors, ok_rows = [], []
                    for i, row in df.iterrows():
                        row_num = i + 2  # ヘッダー行=1
                        media = row["媒体"].strip()
                        title = row["タイトル"].strip()
                        date  = str(row.get("体験日", "") or row.get("鑑賞日", "")).strip()
                        rating = row["評価"].strip()
                        if not media:
                            errors.append(f"行{row_num}: 媒体が空です")
                            continue
                        if media not in VALID_MEDIA:
                            errors.append(f"行{row_num}: 無効な媒体「{media}」")
                            continue
                        if not title:
                            errors.append(f"行{row_num}: タイトルが空です")
                            continue
                        if date:
                            import re as _re
                            if not _re.match(r"^\d{4}-\d{2}-\d{2}$", date):
                                errors.append(f"行{row_num}: 体験日のフォーマットが不正「{date}」（YYYY-MM-DD）")
                                continue
                        if rating and rating not in VALID_RATINGS:
                            errors.append(f"行{row_num}: 無効な評価「{rating}」")
                            continue
                        ok_rows.append({
                            "media":   media,
                            "title":   title,
                            "en":      row["英語タイトル"].strip(),
                            "date":    date or None,
                            "rating":  rating or None,
                            "memo":    row["メモ"].strip() or None,
                            "location": row["場所"].strip() or None,
                        })

                    if errors:
                        st.error(f"❌ {len(errors)} 件のエラー")
                        for e in errors:
                            st.caption(f"・{e}")

                    st.success(f"✅ {len(ok_rows)} 件が登録可能")

                    # ── プレビュー ──
                    if ok_rows:
                        with st.expander(f"登録内容プレビュー（{len(ok_rows)} 件）"):
                            preview_df = pd.DataFrame(ok_rows).rename(columns={
                                "media": "媒体", "title": "タイトル", "en": "英語タイトル",
                                "date": "体験日", "rating": "評価", "memo": "メモ", "location": "場所",
                            })
                            st.dataframe(preview_df, use_container_width=True)

                        if st.button(f"🚀 {len(ok_rows)} 件をNotionに登録", type="primary", key="csv_bulk_register"):
                            prog    = st.progress(0)
                            success = 0
                            fail    = 0
                            reg_ids = get_registered_ids(st.session_state.pages)
                            for n, r in enumerate(ok_rows):
                                # 重複チェック（タイトル×媒体）
                                existing = [
                                    p for p in st.session_state.pages
                                    if get_page_media(p) == r["media"]
                                    and get_title(p["properties"])[1] == r["title"]
                                ]
                                if existing:
                                    st.caption(f"⏩ スキップ（登録済み）: {r['title']}")
                                    prog.progress((n + 1) / len(ok_rows))
                                    continue
                                icon_url = get_media_icon_url(r["media"])
                                ok = create_notion_page(
                                    jp_title=r["title"], en_title=r["en"],
                                    media_type_label=r["media"],
                                    tmdb_id=0, media_type="",
                                    cover_url=icon_url,
                                    tmdb_release="", details={"genres": [], "cast": "", "director": "", "score": None},
                                    watched_date=r["date"],
                                    rating=r["rating"],
                                    memo=r["memo"],
                                    location=None,  # TODO: 文字列→lat/lon変換は未実装、インポート後にデータ管理で設定
                                )
                            if ok:
                                success += 1
                            else:
                                fail += 1
                                st.caption(f"❌ 登録失敗: {r['title']}")
                            prog.progress((n + 1) / len(ok_rows))
                            import time as _time
                            _time.sleep(0.3)
                        st.success(f"✅ {success} 件登録完了" + (f"　❌ {fail} 件失敗" if fail else ""))
                        if st.session_state.get("auto_reload_mode") == "partial":
                            for p in st.session_state.get("created_pages", []):
                                upsert_page_in_state(p)
                            st.session_state.created_pages = []
                        else:
                            sync_notion_after_update()

    # ============================================================
    # 通常登録タブ
    # ============================================================
    with tab_reg:

    # ── 媒体選択 ──
        MEDIA_SELECT_PLACEHOLDER = "（媒体を選択してください）"
        media_options = [MEDIA_SELECT_PLACEHOLDER] + [v[0] for v in MEDIA_ICON_MAP.values()]
        media_display = st.selectbox("媒体 *", media_options, key="reg_media")

        if media_display == MEDIA_SELECT_PLACEHOLDER:
            st.stop()

        media_label = next(k for k, v in MEDIA_ICON_MAP.items() if v[0] == media_display)

        # ── 媒体変更時にリセット ──
        if st.session_state.prev_media_label != media_label:
            st.session_state.new_search_results = []
            st.session_state.new_search_done    = False
            st.session_state.confirm_reg        = None
            st.session_state.bulk_checked       = {}
            st.session_state.reg_cart           = []
            st.session_state.prev_media_label   = media_label

        EVENT_MEDIA = ["演奏会（鑑賞）", "出演", "展示会", "ライブ/ショー", "イベント"]

        # ============================================================
        # イベント系（演奏会（鑑賞）・出演・展示会・ライブ/ショー・イベント）- 単体登録のみ
        # ============================================================
        if media_label in EVENT_MEDIA:
            st.divider()
            is_performance  = (media_label == "出演")
            is_concert      = media_label in ("演奏会（鑑賞）", "出演")
            is_live         = (media_label == "ライブ/ショー")
            has_setlist     = is_concert or is_live

            # ── 基本情報 ──
            event_title_placeholder = {
                "出演": "例: Osaka Pastoral Symphony Orchestra 第5回演奏会",
                "演奏会（鑑賞）": "例: 大阪フィルハーモニー交響楽団 第588回定期演奏会",
                "展示会": "例: モネ展（大阪中之島美術館）",
                "ライブ/ショー": "例: 〇〇 LIVE TOUR 2025",
                "イベント": "例: 〇〇花火大会 2026",
            }
            creator_label = (
                "指揮者" if is_concert else
                "アーティスト" if is_live else
                "主催者・キュレーター" if media_label == "展示会" else
                "主催者"
            )
            creator_placeholder = (
                "例: 井上道義" if is_concert else
                "例: Queen / 米津玄師" if is_live else
                "例: ○○美術館 / ○○実行委員会" if media_label == "展示会" else
                "例: ○○実行委員会"
            )
            cast_label = (
                "演奏団体" if is_concert else
                "出演者・バンド" if is_live else
                "関係者（任意）" if media_label == "展示会" else
                "出演者・登壇者（任意）"
            )
            cast_placeholder = (
                "例: 大阪フィルハーモニー交響楽団" if is_concert else
                "例: Queen" if is_live else
                "例: 学芸員 / 監修者" if media_label == "展示会" else
                "例: アーティスト / ゲスト"
            )
            genre_placeholder = (
                "例: クラシック / 室内楽" if is_concert else
                "例: ロック / J-POP" if is_live else
                "例: 絵画 / 写真 / 現代美術" if media_label == "展示会" else
                "例: 祭り / 花火 / フェス"
            )
            event_title = clearable_text_input(
                "公演名 *",
                placeholder=event_title_placeholder.get(media_label, "例: タイトルを入力"),
                key="ev_title",
            )
            event_creator = clearable_text_input(
                creator_label,
                placeholder=creator_placeholder,
                key="ev_creator",
            )
            col_cast, col_genre = st.columns([1, 1])
            event_cast = clearable_text_input(
                cast_label,
                "ev_cast",
                placeholder=cast_placeholder,
                container=col_cast,
            )
            event_genre = clearable_text_input(
                "ジャンル",
                "ev_genre",
                placeholder=genre_placeholder,
                container=col_genre,
            )
            if media_label in ("展示会", "イベント"):
                col_start, col_end, col_watch = st.columns([1, 1, 1])
                event_start = col_start.date_input("開催開始日", value=None, key="ev_start")
                event_end   = col_end.date_input("開催終了日",   value=None, key="ev_end")
                watch_label = "体験日" if media_label == "展示会" else "参加日"
                event_watch = col_watch.date_input(watch_label, value=None, key="ev_watch")
            else:
                col_watch2, _ = st.columns([1, 1])
                date_label_ev = "出演日" if is_performance else ("体験日" if is_concert else "参加日")
                event_watch = col_watch2.date_input(date_label_ev, value=None, key="ev_watch2")
                event_start = event_watch
                event_end   = None
            col_rating, col_wl = st.columns([2, 1])
            rating_sel = col_rating.selectbox("評価", RATING_OPTIONS, key="ev_rating")
            wlflg      = col_wl.checkbox("WLflg", value=False, key="ev_wl")

            # ── セットリスト / プログラム楽曲 ──
            if has_setlist:
                st.divider()
                MAX_MAIN   = 25
                MAX_ENCORE = 5
                if "ev_participants" not in st.session_state:
                    st.session_state.ev_participants = []
                default_self = (DEFAULT_PERFORMER_NAME or "").strip()
                if is_performance and default_self:
                    exists_self = any(
                        _normalize_person_name(x.get("name", "")) == _normalize_person_name(default_self)
                        for x in st.session_state.ev_participants
                    )
                    if not exists_self:
                        st.session_state.ev_participants.insert(0, {"name": default_self, "instruments": "", "memo": ""})

                # セッションステート構造: [{"title":"曲名","part":"Vn.","played":True,"players":["山田太郎"]}]
                def render_song_list(slot_key, max_count, label):
                    """曲リストUIを描画し、現在のリストを返す"""
                    songs = st.session_state[slot_key]
                    st.caption(label)
                    new_list = []
                    for i, item in enumerate(songs):
                        if is_performance:
                            c_num, c_inp, c_play, c_part, c_del = st.columns([0.3, 3.2, 0.8, 1.2, 0.5])
                        else:
                            c_num, c_inp, c_del = st.columns([0.3, 4, 0.5])
                        c_num.markdown(f"**{i+1}.**")
                        t = c_inp.text_input("", value=item["title"], key=f"{slot_key}_t_{i}", label_visibility="collapsed")
                        if is_performance:
                            played = c_play.checkbox("演奏", value=bool(item.get("played", True)), key=f"{slot_key}_played_{i}", label_visibility="collapsed")
                            p = c_part.text_input("", value=item.get("part", ""), key=f"{slot_key}_p_{i}", placeholder="担当楽器", label_visibility="collapsed")
                            pl = []
                        else:
                            played = False
                            p = ""
                            pl = []
                        new_list.append({"title": t, "part": p, "played": played, "players": pl})
                        if c_del.button("✕", key=f"{slot_key}_del_{i}"):
                            st.session_state[slot_key] = [x for j, x in enumerate(new_list) if j != i]
                            st.rerun()
                    if new_list:
                        st.session_state[slot_key] = new_list
                    filled = [x for x in new_list if x["title"].strip()]
                    last_empty = new_list and not new_list[-1]["title"].strip()
                    if len(filled) < max_count and not last_empty:
                        if st.button(f"＋ 曲を追加", key=f"{slot_key}_add"):
                            st.session_state[slot_key] = filled + [{"title": "", "part": "", "played": True, "players": []}]
                            st.rerun()

                def add_songs_to_slot(slot_key, titles, max_count):
                    """タイトルリストをセトリに追加（重複除外）"""
                    current = [x for x in st.session_state[slot_key] if x["title"].strip()]
                    for title in titles:
                        if len(current) < max_count and title not in [x["title"] for x in current]:
                            current.append({"title": title, "part": "", "played": True, "players": []})
                    st.session_state[slot_key] = current

                setlist_tab = st.segmented_control(
                    "セットリスト入力ビュー",
                    options=["楽曲検索・追加", "セットリスト確認"],
                    key="ev_setlist_ui_tab",
                )
                main_count = len([x for x in st.session_state.get("ev_setlist_main", []) if (x.get("title") or "").strip()])
                enc_count = len([x for x in st.session_state.get("ev_setlist_encore", []) if (x.get("title") or "").strip()])
                c_info, c_btn = st.columns([4, 1])
                c_info.caption(f"現在の登録: 通常 {main_count} 曲 / アンコール {enc_count} 曲")
                if c_btn.button("確認へ", key="ev_setlist_goto_review"):
                    st.session_state.ev_setlist_ui_tab = "セットリスト確認"
                    st.rerun()

                if setlist_tab == "セットリスト確認":
                    # ── 通常セットリスト ──
                    render_song_list("ev_setlist_main", MAX_MAIN, f"📋 通常セットリスト（最大{MAX_MAIN}曲）")
                    # ── アンコール ──
                    render_song_list("ev_setlist_encore", MAX_ENCORE, f"🎊 アンコール（最大{MAX_ENCORE}曲）")

                # ── 楽曲検索（出演はクラシック/ポピュラーを切替可）──
                use_mb = is_concert
                use_itunes = is_live
                if is_performance:
                    search_mode = st.segmented_control(
                        "楽曲検索方式",
                        options=["クラシック（MusicBrainz）", "ポピュラー（iTunes）", "両方"],
                        default="両方",
                        key="ev_song_search_mode",
                    )
                    use_mb = search_mode in ("クラシック（MusicBrainz）", "両方")
                    use_itunes = search_mode in ("ポピュラー（iTunes）", "両方")
                show_song_search = setlist_tab == "楽曲検索・追加" and (use_mb or use_itunes)
                if show_song_search:
                    st.divider()

                if show_song_search and use_mb:
                    st.caption("🔍 楽曲検索（MusicBrainz）")
                    st.caption("1) 作曲家を検索 → 2) 作曲家を確定 → 3) 曲名で検索 → 4) 曲を追加")
                    ev_composer_input = clearable_text_input(
                        "1. 作曲家を検索",
                        "ev_composer",
                        placeholder="例: Beethoven / ベートーヴェン",
                        on_change=queue_action,
                        args=("ev_mb_search_enter",),
                    )
                    ev_mb_search_clicked = st.button("🔍 作曲家を検索", key="ev_mb_search")
                    ev_mb_search_enter = bool(st.session_state.pop("ev_mb_search_enter", False))
                    if ev_mb_search_clicked or ev_mb_search_enter:
                        if ev_composer_input.strip():
                            with st.spinner("作曲家を検索中..."):
                                ev_composers, ev_err = search_mb_composer(ev_composer_input.strip())
                            if ev_err:
                                st.error(f"⚠️ MusicBrainz API エラー: {ev_err}")
                            st.session_state.ev_mb_composers = ev_composers
                            st.session_state.ev_mb_works = []
                            st.session_state.ev_mb_selected_comp = None
                            if not ev_composers and not ev_err:
                                st.warning("作曲家が見つかりませんでした。")
                        else:
                            st.warning("作曲家名を入力してください")

                    ev_selected_comp = st.session_state.get("ev_mb_selected_comp")
                    if st.session_state.get("ev_mb_composers"):
                        ev_composers = st.session_state.ev_mb_composers
                        ev_comp_labels = [format_mb_composer_label(c) for c in ev_composers]
                        ev_sel_idx = st.radio("2. 作曲家を特定", range(len(ev_comp_labels)), format_func=lambda i: ev_comp_labels[i], key="ev_mb_comp_radio")
                        if st.button("✅ この作曲家で進める", key="ev_mb_pick_comp"):
                            st.session_state.ev_mb_selected_comp = ev_composers[ev_sel_idx]
                            st.session_state.ev_mb_works = []
                            st.rerun()
                        ev_selected_comp = st.session_state.get("ev_mb_selected_comp")
                        if ev_selected_comp:
                            st.success(f"作曲家を確定: {format_mb_composer_label(ev_selected_comp)}")

                    if ev_selected_comp:
                        ev_title_filter = clearable_text_input(
                            "3. 検索ワード（曲名）",
                            "ev_title_filter",
                            placeholder="例: Symphony No.5",
                            on_change=queue_action,
                            args=("ev_mb_works_enter",),
                        )
                        c_mb1, c_mb2 = st.columns([1, 1])
                        ev_mb_works_clicked = c_mb1.button("🔍 曲名で検索", key="ev_mb_fetch_works")
                        ev_mb_works_enter = bool(st.session_state.pop("ev_mb_works_enter", False))
                        if ev_mb_works_clicked or ev_mb_works_enter:
                            if not ev_title_filter.strip():
                                st.warning("曲名を入力してください。")
                            else:
                                with st.spinner(f"{ev_selected_comp['name']} の作品を検索中..."):
                                    ev_works = search_mb_works(ev_selected_comp["id"], ev_title_filter.strip())
                                st.session_state.ev_mb_works = ev_works
                                if not ev_works:
                                    st.info("作品候補が見つかりませんでした。作曲家情報のみで作品登録がない可能性があります。")
                        if c_mb2.button("📚 全作品を取得（重い）", key="ev_mb_fetch_all"):
                            with st.spinner(f"{ev_selected_comp['name']} の全作品を取得中..."):
                                ev_works = search_mb_works(ev_selected_comp["id"], "")
                            st.session_state.ev_mb_works = ev_works
                            if not ev_works:
                                st.info("全作品を取得しましたが候補がありませんでした。作曲家情報のみで作品登録がない可能性があります。")

                    if st.session_state.get("ev_mb_works"):
                        ev_works = st.session_state.ev_mb_works
                        st.caption(f"{len(ev_works)} 件の作品　— ボタンで直接追加")
                        for w in ev_works:
                            label = w["title"] + (f"　{w['disambiguation']}" if w.get("disambiguation") else "")
                            col_title, col_main, col_enc = st.columns([4, 1.2, 1.2])
                            col_title.markdown(label)
                            if col_main.button("📋 通常", key=f"ev_mb_add_main_{w['id']}"):
                                add_songs_to_slot("ev_setlist_main", [w["title"]], MAX_MAIN)
                                st.rerun()
                            if col_enc.button("🎊 ENC", key=f"ev_mb_add_enc_{w['id']}"):
                                add_songs_to_slot("ev_setlist_encore", [w["title"]], MAX_ENCORE)
                                st.rerun()

                if show_song_search and use_itunes:
                    st.caption("🔍 楽曲検索（iTunes）")
                    col_it_art, col_it_title = st.columns([1, 1])
                    it_artist_input = clearable_text_input(
                        "アーティスト名",
                        "ev_it_artist",
                        placeholder="例: Queen / 米津玄師",
                        container=col_it_art,
                        on_change=queue_action,
                        args=("ev_it_search_enter",),
                    )
                    it_title_input  = clearable_text_input(
                        "曲名",
                        "ev_it_title",
                        placeholder="例: Bohemian Rhapsody",
                        container=col_it_title,
                        on_change=queue_action,
                        args=("ev_it_search_enter",),
                    )

                    ev_it_search_clicked = st.button("🔍 曲を検索", key="ev_it_search")
                    ev_it_search_enter = bool(st.session_state.pop("ev_it_search_enter", False))
                    if ev_it_search_clicked or ev_it_search_enter:
                        q = " ".join(filter(None, [it_artist_input, it_title_input]))
                        if q:
                            with st.spinner("検索中..."):
                                res = api_request("get", "https://itunes.apple.com/search",
                                    params={"term": q, "entity": "song", "limit": 20, "lang": "ja_jp"})
                            if res:
                                it_results = res.json().get("results", [])
                                st.session_state.ev_it_results = it_results
                                if not it_results:
                                    st.warning("曲が見つかりませんでした。")
                            else:
                                st.warning("⚠️ iTunes API エラー")
                        else:
                            st.warning("アーティスト名または曲名を入力してください")

                    if st.session_state.get("ev_it_results"):
                        it_results = st.session_state.ev_it_results
                        st.caption(f"{len(it_results)} 件　— ボタンで直接追加")
                        for i, track in enumerate(it_results):
                            name   = track.get("trackName", "")
                            artist = track.get("artistName", "")
                            col_title, col_main, col_enc = st.columns([4, 1.2, 1.2])
                            col_title.markdown(f"{name}  —  {artist}")
                            if col_main.button("📋 通常", key=f"ev_it_add_main_{i}"):
                                add_songs_to_slot("ev_setlist_main", [name], MAX_MAIN)
                                st.rerun()
                            if col_enc.button("🎊 ENC", key=f"ev_it_add_enc_{i}"):
                                add_songs_to_slot("ev_setlist_encore", [name], MAX_ENCORE)
                                st.rerun()

            related_score_ids = []
            if is_performance:
                st.divider()
                st.subheader("🎼 演奏曲の関連付け")
                if "ev_score_selected" not in st.session_state:
                    st.session_state.ev_score_selected = []
                score_pages = _get_score_pages()
                if not st.session_state.get("last_notion_load_ok", True):
                    st.warning("⚠️ 演奏曲の取得に失敗しました。手動で再読み込みしてください。")
                score_query = clearable_text_input("演奏曲名で検索", "ev_score_query", placeholder="例: 交響曲第5番")
                matches = []
                if score_query:
                    q = score_query.strip().lower()
                    matches = [p for p in score_pages if q in (p.get("title") or "").strip().lower()]

                def add_selected_score(pid, title):
                    selected = st.session_state.ev_score_selected
                    if not any(x["id"] == pid for x in selected):
                        selected.append({"id": pid, "title": title})
                        st.session_state.ev_score_selected = selected

                if matches:
                    options = ["（選択してください）"] + [p["title"] for p in matches]
                    sel = st.selectbox("候補", options, key="ev_score_pick")
                    if sel != "（選択してください）":
                        picked = matches[options.index(sel) - 1]
                        if st.button("🎼 曲を追加", key="ev_score_add"):
                            add_selected_score(picked["id"], picked["title"])
                            st.rerun()
                elif score_query:
                    st.caption("候補が見つかりませんでした。")
                    if st.button("🆕 演奏曲を新規登録して追加", key="ev_score_create"):
                        with st.spinner("演奏曲を新規作成中..."):
                            ok = create_notion_page(
                                jp_title=score_query, en_title=score_query,
                                media_type_label="演奏曲",
                                tmdb_id=None, media_type="score",
                                cover_url=MB_DEFAULT_COVER,
                                tmdb_release="",
                                details={"genres": [], "cast": "", "director": "", "score": None},
                            )
                        if ok:
                            new_id = st.session_state.get("last_created_page_id")
                            _add_score_page_cache(new_id, score_query)
                            add_selected_score(new_id, score_query)
                            st.success("✅ 演奏曲を追加しました")
                            st.rerun()
                        else:
                            st.error("❌ 演奏曲の作成に失敗しました")

                # セットリストから一括追加（未登録は新規作成）
                main_items = [x for x in st.session_state.get("ev_setlist_main", []) if x["title"].strip()]
                encore_items = [x for x in st.session_state.get("ev_setlist_encore", []) if x["title"].strip()]
                setlist_titles = [x["title"] for x in (main_items + encore_items)]
                if setlist_titles:
                    if st.button("📋 セットリストから一括追加（未登録は新規作成）", key="ev_score_bulk"):
                        with st.spinner("関連付け中..."):
                            for t in setlist_titles:
                                t = t.strip()
                                if not t:
                                    continue
                                found = _find_score_page_by_title(score_pages, t)
                                if found:
                                    add_selected_score(found["id"], found["title"])
                                    continue
                                ok = create_notion_page(
                                    jp_title=t, en_title=t,
                                    media_type_label="演奏曲",
                                    tmdb_id=None, media_type="score",
                                    cover_url=MB_DEFAULT_COVER,
                                    tmdb_release="",
                                    details={"genres": [], "cast": "", "director": "", "score": None},
                                )
                                if ok:
                                    new_id = st.session_state.get("last_created_page_id")
                                    _add_score_page_cache(new_id, t)
                                    add_selected_score(new_id, t)
                        st.rerun()

                if st.session_state.ev_score_selected:
                    st.caption("✅ 関連付け済み")
                    for i, item in enumerate(st.session_state.ev_score_selected):
                        col_t, col_del = st.columns([4, 1])
                        col_t.write(item["title"])
                        if col_del.button("✕", key=f"ev_score_rm_{i}"):
                            st.session_state.ev_score_selected = [
                                x for j, x in enumerate(st.session_state.ev_score_selected) if j != i
                            ]
                            st.rerun()
                    related_score_ids = [x["id"] for x in st.session_state.ev_score_selected]

            st.divider()
            event_location = location_search_ui("event", media_label)
            register_clicked = st.button(
                "📥 登録する",
                type="primary",
                key="event_register",
                disabled=(not event_title) or st.session_state.get("event_registering", False),
            )
            if register_clicked and not st.session_state.get("event_registering", False):
                st.session_state.event_registering = True
                st.rerun()
            if st.session_state.get("event_registering", False):
                watch_str = event_watch.isoformat() if event_watch else None
                start_str = event_start.isoformat() if event_start else None
                end_str   = event_end.isoformat()   if event_end   else None

                # ── メモ生成 ──
                memo_text = None
                if has_setlist:
                    main_items   = [x for x in st.session_state.get("ev_setlist_main",   []) if x["title"].strip()]
                    encore_items = [x for x in st.session_state.get("ev_setlist_encore", []) if x["title"].strip()]
                    def fmt(i, item):
                        suffix = f" [{item['part']}]" if is_performance and item.get("played", False) and item.get("part","").strip() else ""
                        return f"{i+1}. {item['title']}{suffix}"
                    lines = [fmt(i, x) for i, x in enumerate(main_items)]
                    if encore_items:
                        lines.append("")
                        lines.append("[Encore]")
                        lines += [fmt(i, x) for i, x in enumerate(encore_items)]
                    memo_text = "\n".join(lines) if lines else None

                try:
                    ok = create_notion_page(
                        jp_title=event_title, en_title=event_title,
                        media_type_label=media_label,
                        tmdb_id=None, media_type="event",
                        cover_url=get_media_icon_url(media_label),
                        tmdb_release=start_str or "",
                        details={"genres": [event_genre] if event_genre else [], "cast": event_cast, "director": event_creator, "score": None},
                        wlflg=wlflg,
                        watched_date=watch_str,
                        rating=rating_sel if rating_sel else None,
                        event_end=end_str,
                        location=event_location,
                        memo=memo_text,
                        relation_prop="演奏曲" if is_performance and related_score_ids else None,
                        relation_ids=related_score_ids if is_performance else None,
                    )
                    if ok:
                        created_setlist = failed_setlist = 0
                        created_setlist_rows = []
                        created_assign = failed_assign = 0
                        assign_reason = ""
                        setlist_reason = ""
                        if is_performance and NOTION_SCORE_DB_ID:
                            setlist_main = [x for x in st.session_state.get("ev_setlist_main", []) if (x.get("title") or "").strip()]
                            setlist_encore = [x for x in st.session_state.get("ev_setlist_encore", []) if (x.get("title") or "").strip()]
                            if setlist_main or setlist_encore:
                                perf_page_id = st.session_state.get("last_created_page_id", "")
                                perf_date = watch_str or start_str or ""
                                score_pages_for_link = _get_score_pages()
                                selected_scores_for_link = st.session_state.get("ev_score_selected", [])
                                created_setlist, failed_setlist, setlist_reason, created_setlist_rows = create_setlist_rows_for_performance(
                                    performance_page_id=perf_page_id,
                                    performance_title=event_title,
                                    performance_date=perf_date,
                                    main_items=setlist_main,
                                    encore_items=setlist_encore,
                                    selected_scores=selected_scores_for_link,
                                    score_pages=score_pages_for_link,
                                )
                                if NOTION_SONG_ASSIGN_DB_ID and created_setlist_rows:
                                    cast_row_map = _get_cast_row_map_for_performance(perf_page_id)
                                    created_assign, failed_assign, assign_reason = create_song_assignment_rows(
                                        score_rows=created_setlist_rows,
                                        cast_row_map=cast_row_map,
                                    )
                            else:
                                setlist_reason = "セットリスト入力なし"
                        for key in ["ev_mb_composers", "ev_mb_works", "ev_mb_filter",
                                    "ev_it_results", "ev_setlist_main", "ev_setlist_encore"]:
                            st.session_state.pop(key, None)
                        reset_new_register_state()
                        sync_notion_after_update(
                            page_id=st.session_state.get("last_created_page_id"),
                            updated_page=st.session_state.get("last_created_page"),
                        )
                        if is_performance and NOTION_SCORE_DB_ID:
                            if created_setlist > 0 and failed_setlist == 0:
                                st.success(f"✅ 演奏曲DBに {created_setlist} 件登録しました")
                            elif created_setlist > 0 and failed_setlist > 0:
                                st.warning(f"⚠️ 演奏曲DB登録: 成功 {created_setlist} 件 / 失敗 {failed_setlist} 件")
                        elif failed_setlist > 0:
                            st.warning(f"⚠️ 演奏曲DB登録に失敗しました（{failed_setlist} 件）")
                        elif setlist_reason:
                            st.info(f"ℹ️ 演奏曲DB連携: {setlist_reason}")
                        if is_performance and NOTION_SONG_ASSIGN_DB_ID:
                            if created_assign > 0 and failed_assign == 0:
                                st.success(f"✅ 楽曲別担当者DBに {created_assign} 件登録しました")
                            elif created_assign > 0 and failed_assign > 0:
                                st.warning(f"⚠️ 楽曲別担当者DB登録: 成功 {created_assign} 件 / 失敗 {failed_assign} 件")
                            elif failed_assign > 0:
                                st.warning(f"⚠️ 楽曲別担当者DB登録に失敗しました（{failed_assign} 件）")
                            elif assign_reason:
                                st.info(f"ℹ️ 楽曲別担当者DB連携: {assign_reason}")
                        show_post_register_ui()
                    else:
                        st.error("❌ 登録失敗")
                finally:
                    st.session_state.event_registering = False
            st.stop()

        # ============================================================
        # 演奏曲 - MusicBrainzカート登録
        # ============================================================
        if media_label == "演奏曲":
            score_tab_options = ["検索", "登録リスト"]
            if "active_score_tab_next" in st.session_state:
                st.session_state.active_score_tab = st.session_state.pop("active_score_tab_next")
            if "active_score_tab" not in st.session_state:
                st.session_state.active_score_tab = "検索"
            if st.session_state.active_score_tab not in score_tab_options:
                st.session_state.active_score_tab = "検索"
            active_score_tab = st.segmented_control(
                "表示",
                options=score_tab_options,
                key="active_score_tab",
                label_visibility="collapsed",
            )
            score_cart_count = len(st.session_state.get("reg_cart", []))
            score_step = "1/2 検索" if active_score_tab == "検索" else "2/2 登録リスト"
            st.caption(f"進捗: {score_step}  |  登録予定 {score_cart_count} 件")
            current_score_tab = st.session_state.get("active_score_tab")
            prev_score_tab = st.session_state.get("_prev_active_score_tab")
            if prev_score_tab is not None and prev_score_tab != current_score_tab:
                emit_scroll_top_script()
            st.session_state["_prev_active_score_tab"] = current_score_tab
            s_nav1, s_nav2 = st.columns(2)
            if s_nav1.button("🔎 検索へ", key="score_nav_search"):
                st.session_state.active_score_tab_next = "検索"
                st.rerun()
            if s_nav2.button("🧺 登録リストへ", key="score_nav_cart"):
                st.session_state.active_score_tab_next = "登録リスト"
                st.rerun()

            if active_score_tab == "登録リスト":
                reg_cart = st.session_state.get("reg_cart", [])
                if "reg_cart" not in st.session_state:
                    st.session_state.reg_cart = reg_cart
                if reg_cart:
                    st.divider()
                    st.subheader(f"登録リスト（{len(reg_cart)} 件）")
                    fallback_perf_ids = _clean_relation_ids(st.session_state.get("score_perf_selected_ids", []))
                    remove_indices = []
                    for idx, item in enumerate(reg_cart):
                        item_uid = item.get("cart_uid") or f"score_{idx}"
                        item["cart_uid"] = item_uid
                        if item.get("media_type") == "score":
                            rel_ids = _clean_relation_ids(item.get("relation_ids"))
                            if not rel_ids and fallback_perf_ids:
                                rel_ids = fallback_perf_ids
                                item["relation_ids"] = rel_ids
                                item["relation_prop"] = "出演履歴"
                            if rel_ids:
                                perf_page = _get_page_from_state_or_api(rel_ids[0])
                                d_release, d_watched, d_rating, d_location = _extract_performance_defaults(perf_page)
                                if not item.get("release"):
                                    # 演奏曲のリリース日は「初演情報」を優先し、
                                    # 出演履歴（公演日/公演リリース日）では補完しない
                                    item["release"] = item.get("release", "")
                                if not item.get("watched"):
                                    item["watched"] = d_watched or ""
                                if not item.get("rating"):
                                    item["rating"] = d_rating or ""
                                if not item.get("location") and d_location:
                                    item["location"] = d_location
                        item_media = item.get("media_label", media_label)
                        with st.expander(f"{idx+1}. {item['jp_title']}", expanded=True):
                            if item.get("media_type") == "score":
                                st.caption(f"関連出演履歴: {len(_clean_relation_ids(item.get('relation_ids')))} 件")
                                src = item.get("premiere_source", "")
                                if item.get("premiere_missing"):
                                    st.caption("ℹ️ 初演情報を確認できなかったため、リリース日は空欄です（必要なら手入力してください）")
                                    st.caption(f"ℹ️ 取得状況: {format_premiere_source_message(src)}")
                                    if item.get("premiere_partial"):
                                        st.caption(f"ℹ️ 取得値: {item.get('premiere_partial_value','')}（年月日不足のため自動入力しません）")
                                    cand_state_key = f"premiere_cands_{item_uid}"
                                    cand_select_key = f"premiere_cand_idx_{item_uid}"
                                    composer_name = (
                                        ((item.get("details") or {}).get("director") or "").strip()
                                        or ((item.get("details") or {}).get("creator") or "").strip()
                                    )
                                    if st.button("🔎 初演候補を検索", key=f"premiere_search_btn_{item_uid}"):
                                        with st.spinner("初演候補を検索中..."):
                                            candidates = search_premiere_candidates_from_work(
                                                item.get("mb_work_id", ""),
                                                work_title=item.get("en_title") or item.get("jp_title") or "",
                                                composer_name=composer_name,
                                                limit=8,
                                            )
                                        st.session_state[cand_state_key] = candidates
                                        st.session_state[cand_select_key] = 0
                                    candidates = st.session_state.get(cand_state_key, [])
                                    if candidates:
                                        st.markdown("**初演候補（半自動選択）**")
                                        picked_idx = st.radio(
                                            "候補",
                                            options=list(range(len(candidates))),
                                            index=min(int(st.session_state.get(cand_select_key, 0) or 0), len(candidates) - 1),
                                            format_func=lambda i: f"{candidates[i].get('date','')} / {candidates[i].get('title','(無題)')}",
                                            key=f"premiere_pick_{item_uid}",
                                        )
                                        picked = candidates[picked_idx]
                                        picked_precision = (picked.get("precision") or _date_precision(picked.get("date", ""))).strip()
                                        if picked_precision in ("year", "month"):
                                            st.caption("ℹ️ この候補は年月日が不足しています。リリース日は手入力してください。")
                                        urls = [u for u in (picked.get("urls") or []) if u]
                                        if urls:
                                            st.markdown(f"🔗 ソース: [リンクを開く]({urls[0]})")
                                            if len(urls) > 1:
                                                for extra_u in urls[1:]:
                                                    st.markdown(f"- [追加ソース]({extra_u})")
                                        if st.button("✅ この初演日をリリース日に反映", key=f"premiere_apply_{item_uid}"):
                                            if picked_precision == "day":
                                                applied_release = picked.get("date", "")
                                                item["release"] = applied_release
                                                item["premiere_missing"] = False
                                                item["premiere_source"] = "wikidata-candidate"
                                            else:
                                                applied_release = ""
                                                item["release"] = ""
                                                item["premiere_missing"] = True
                                                item["premiere_source"] = "wikidata-candidate-partial"
                                                item["premiere_partial"] = True
                                                item["premiere_partial_value"] = picked.get("date", "")
                                            if urls:
                                                item["premiere_source_url"] = urls[0]
                                            st.session_state.pop(f"cart_rel_{item_uid}", None)
                                            st.success("初演候補を反映しました（年月日不足の候補は手入力が必要です）")
                                            st.rerun()
                                    elif cand_state_key in st.session_state:
                                        st.caption("ℹ️ 初演候補は見つかりませんでした")
                                else:
                                    if src:
                                        st.caption(f"ℹ️ 初演情報ソース: {format_premiere_source_message(src)}")
                                src_url = item.get("premiere_source_url")
                                if src_url:
                                    st.markdown(f"🔗 参照ソース: [リンクを開く]({src_url})")
                            cols = st.columns([2, 1, 2, 2, 1, 1])
                            item["jp_title"] = cols[0].text_input("日本語タイトル", value=item["jp_title"], key=f"cart_jp_{item_uid}")
                            rel_key = f"cart_rel_{item_uid}"
                            rel_date_val = None
                            rel_norm = _normalize_notion_date_input(str(item.get("release") or ""))
                            if rel_norm:
                                try:
                                    rel_date_val = date.fromisoformat(rel_norm)
                                except Exception:
                                    rel_date_val = None
                            release_input = cols[1].date_input(
                                "リリース日",
                                value=rel_date_val,
                                min_value=date(1500, 1, 1),
                                max_value=date(2100, 12, 31),
                                key=rel_key,
                            )
                            item["release"] = release_input.isoformat() if release_input else ""
                            date_val = None
                            if item.get("watched"):
                                try:
                                    date_val = date.fromisoformat(item["watched"])
                                except Exception:
                                    date_val = None
                            watched_input = cols[2].date_input("演奏日", value=date_val, key=f"cart_watch_{item_uid}")
                            item["watched"] = watched_input.isoformat() if watched_input else ""
                            item["rating"] = cols[3].selectbox(
                                "評価",
                                RATING_OPTIONS,
                                index=RATING_OPTIONS.index(item.get("rating", "")) if item.get("rating", "") in RATING_OPTIONS else 0,
                                key=f"cart_rating_{item_uid}",
                            )
                            item["wlflg"] = cols[4].checkbox("WL", value=item.get("wlflg", False), key=f"cart_wl_{item_uid}")
                            if cols[5].button("削除", key=f"cart_del_{item_uid}"):
                                remove_indices.append(idx)
                            selected_loc = location_search_ui(
                                f"cart_{item_uid}",
                                item_media,
                                initial_location=item.get("location"),
                            )
                            if selected_loc:
                                item["location"] = selected_loc
                            if item.get("media_type") == "score":
                                st.markdown("**演奏情報（連動）**")
                                pcols = st.columns([1, 1, 1, 2])
                                item["setlist_order"] = int(
                                    pcols[0].number_input(
                                        "曲順",
                                        min_value=1,
                                        value=max(int(item.get("setlist_order", 1) or 1), 1),
                                        step=1,
                                        key=f"cart_ord_{item_uid}",
                                    )
                                )
                                section_options = ["幕前", "ロビー", "本編", "Encore", "ソリストEncore"]
                                current_section = item.get("setlist_section", "本編")
                                if current_section not in section_options:
                                    current_section = "本編"
                                item["setlist_section"] = pcols[1].selectbox(
                                    "区分",
                                    section_options,
                                    index=section_options.index(current_section),
                                    key=f"cart_sec_{item_uid}",
                                )
                                item["played"] = pcols[2].checkbox(
                                    "演奏した",
                                    value=bool(item.get("played", True)),
                                    key=f"cart_played_{item_uid}",
                                )
                                item["part"] = pcols[3].text_input(
                                    "担当楽器（複数は / 区切り）",
                                    value=item.get("part", ""),
                                    key=f"cart_part_{item_uid}",
                                )
                                mv_cols = st.columns([2, 1, 1, 1])
                                item["movement_name"] = mv_cols[0].text_input(
                                    "楽章名（任意）",
                                    value=item.get("movement_name", ""),
                                    key=f"cart_mv_name_{item_uid}",
                                )
                                movement_no_default = item.get("movement_no")
                                movement_no_default = int(movement_no_default) if isinstance(movement_no_default, int) and movement_no_default > 0 else 1
                                movement_order_default = item.get("movement_order")
                                movement_order_default = int(movement_order_default) if isinstance(movement_order_default, int) and movement_order_default > 0 else movement_no_default
                                movement_no_val = int(
                                    mv_cols[1].number_input(
                                        "楽章番号",
                                        min_value=1,
                                        value=movement_no_default,
                                        step=1,
                                        key=f"cart_mv_no_{item_uid}",
                                    )
                                )
                                movement_order_val = int(
                                    mv_cols[2].number_input(
                                        "表示順",
                                        min_value=1,
                                        value=movement_order_default,
                                        step=1,
                                        key=f"cart_mv_order_{item_uid}",
                                    )
                                )
                                item["movement_roman"] = mv_cols[3].text_input(
                                    "ローマ数字",
                                    value=item.get("movement_roman", ""),
                                    key=f"cart_mv_roman_{item_uid}",
                                    placeholder="例: III",
                                )
                                item["movement_no"] = movement_no_val
                                item["movement_order"] = movement_order_val
                                if not (item.get("movement_roman") or "").strip():
                                    item["movement_roman"] = _int_to_roman(movement_no_val)
                                cc1, cc2 = st.columns([1, 3])
                                item["is_concerto"] = cc1.checkbox(
                                    "協奏曲",
                                    value=bool(item.get("is_concerto", False)),
                                    key=f"cart_concerto_{item_uid}",
                                )
                                item["soloists"] = cc2.text_input(
                                    "ソリスト名（複数は / 区切り）",
                                    value=item.get("soloists", ""),
                                    key=f"cart_soloists_{item_uid}",
                                )
                                rel_ids_for_cast = _clean_relation_ids(item.get("relation_ids"))
                                cast_options = _get_performance_cast_names(rel_ids_for_cast[0]) if rel_ids_for_cast else []
                                if cast_options:
                                    selected_players = st.multiselect(
                                        "奏者（演奏会参加者DBから）",
                                        options=cast_options,
                                        default=[p for p in (item.get("players") or []) if p in cast_options],
                                        key=f"cart_players_{item_uid}",
                                    )
                                    item["players"] = selected_players
                                else:
                                    item["players"] = _split_instruments(
                                        st.text_input(
                                            "奏者（候補なし時は手入力 / 区切り）",
                                            value=" / ".join(item.get("players", [])),
                                            key=f"cart_players_text_{item_uid}",
                                        )
                                    )

                    for i in sorted(remove_indices, reverse=True):
                        st.session_state.reg_cart.pop(i)
                    if remove_indices:
                        st.rerun()

                    col_reg, col_clear = st.columns([2, 1])
                    with col_reg:
                        if st.button(f"{len(st.session_state.reg_cart)} 件を一括登録", type="primary", key="bulk_register_score"):
                            total_count = len(st.session_state.get("reg_cart", []))
                            # 同一作品（楽章違い）で担当情報を共通化
                            # 例: 交響曲4楽章を別行で保持していても、担当楽器/奏者/ソリストをまとめて反映
                            score_groups = {}
                            for idx, _it in enumerate(st.session_state.get("reg_cart", [])):
                                if (_it.get("media_type") or "") != "score":
                                    continue
                                title_norm = _normalize_work_title_for_group(_it.get("jp_title", ""))
                                comp_norm = (((_it.get("details") or {}).get("director") or "")).strip().lower()
                                gk = f"{comp_norm}::{(title_norm or (_it.get('jp_title') or '')).strip().lower()}"
                                score_groups.setdefault(gk, []).append(idx)
                            for _gk, idxs in score_groups.items():
                                if len(idxs) <= 1:
                                    continue
                                rows = [st.session_state.reg_cart[i] for i in idxs]
                                src = next((r for r in rows if (r.get("part") or "").strip()), rows[0])
                                shared_part = (src.get("part") or "").strip()
                                shared_players = src.get("players", []) or []
                                shared_soloists = (src.get("soloists") or "").strip()
                                shared_is_concerto = bool(src.get("is_concerto", False))
                                shared_played = bool(src.get("played", True))
                                section_non_empty = [((r.get("setlist_section") or "").strip()) for r in rows if (r.get("setlist_section") or "").strip()]
                                shared_section = section_non_empty[0] if section_non_empty else "本編"
                                order_vals = [int(r.get("setlist_order", 1) or 1) for r in rows]
                                shared_order = max(1, min(order_vals) if order_vals else 1)
                                for i in idxs:
                                    it = st.session_state.reg_cart[i]
                                    if shared_part:
                                        it["part"] = shared_part
                                    if shared_players:
                                        it["players"] = shared_players
                                    if shared_soloists:
                                        it["soloists"] = shared_soloists
                                    if shared_is_concerto:
                                        it["is_concerto"] = True
                                    it["played"] = shared_played
                                    it["setlist_section"] = shared_section
                                    it["setlist_order"] = shared_order
                            if not st.session_state.pages_loaded:
                                with st.spinner("Notionデータ取得中..."):
                                    all_pages = load_notion_data()
                                    st.session_state.pages = filter_target_pages(all_pages)
                                    st.session_state.pages_loaded = True
                            success_count = 0
                            linked_setlist_created = 0
                            linked_setlist_failed = 0
                            linked_setlist_reasons = []
                            linked_assign_created = 0
                            linked_assign_failed = 0
                            score_primary_page_by_group = {}
                            prog = st.progress(0)
                            fallback_perf_ids = _clean_relation_ids(st.session_state.get("score_perf_selected_ids", []))
                            for n, item in enumerate(st.session_state.reg_cart):
                                if item.get("media_type") == "score":
                                    score_title = (item.get("jp_title") or "").strip()
                                    score_comp = (((item.get("details") or {}).get("director") or "")).strip().lower()
                                    score_base = _normalize_work_title_for_group(score_title) or score_title
                                    score_group_key = f"{score_comp}::{score_base.strip().lower()}"
                                    primary_score_id = score_primary_page_by_group.get(score_group_key)
                                    if primary_score_id:
                                        # グルーピング時の2件目以降はATLAS側の親ページ作成を省略。
                                        # マスタ連動はAPOLLO行作成時に一元処理する。
                                        success_count += 1
                                        prog.progress((n + 1) / len(st.session_state.reg_cart))
                                        time.sleep(0.1)
                                        continue
                                rel_prop = item.get("relation_prop")
                                rel_ids = _clean_relation_ids(item.get("relation_ids"))
                                if item.get("media_type") == "score" and not rel_ids and fallback_perf_ids:
                                    rel_prop = "出演履歴"
                                    rel_ids = fallback_perf_ids
                                if item.get("media_type") == "score" and not rel_ids:
                                    st.warning(f"関連出演履歴なし: {item.get('jp_title','(無題)')}（紐付けなしで登録）")
                                ok = create_notion_page(
                                    jp_title=item["jp_title"], en_title=item.get("en_title", ""),
                                    media_type_label=item.get("media_label", media_label),
                                    tmdb_id=item["tmdb_id"], media_type=item["media_type"],
                                    cover_url=item["cover_url"], tmdb_release=item.get("release", ""),
                                    details=item["details"], wlflg=item.get("wlflg", False),
                                    watched_date=item["watched"] or None,
                                    rating=item["rating"] or None,
                                    isbn=item.get("isbn") or None,
                                    igdb_id=item.get("igdb_id"),
                                    itunes_id=item.get("itunes_id"),
                                    anilist_id=item.get("anilist_id"),
                                    is_concerto=bool(item.get("is_concerto", False)),
                                    soloists=item.get("soloists", ""),
                                    icon_emoji=None if item.get("media_type") == "score" else country_code_to_flag(item.get("composer_country", "")),
                                    location=item.get("location"),
                                    relation_prop=rel_prop,
                                    relation_ids=rel_ids,
                                )
                                if ok:
                                    if item.get("media_type") == "score":
                                        created_id_for_master = st.session_state.get("last_created_page_id")
                                        if created_id_for_master:
                                            score_primary_page_by_group[score_group_key] = created_id_for_master
                                            # ここはATLAS側ページIDのため、マスタ連動の実処理は
                                            # APOLLO行作成後（create_setlist_rows_for_performance）に一元化する。
                                    if item.get("media_type") == "score" and rel_ids:
                                        created_id = st.session_state.get("last_created_page_id")
                                        if created_id:
                                            rel_patch = {"relation": [{"id": rid} for rid in rel_ids]}
                                            patch_prop = rel_prop or "出演履歴"
                                            rel_res = api_request(
                                                "patch",
                                                f"https://api.notion.com/v1/pages/{created_id}",
                                                headers=NOTION_HEADERS,
                                                json={"properties": {patch_prop: rel_patch}},
                                            )
                                            if rel_res is None or rel_res.status_code != 200:
                                                st.warning(f"関連付け追記に失敗: {rel_res.status_code if rel_res else 'None'}")
                                            # 演奏曲DB / 楽曲別担当者DB 連動
                                            if NOTION_SCORE_DB_ID:
                                                perf_date = item.get("watched") or item.get("release") or ""
                                                selected_scores_for_link = [{
                                                    "id": created_id,
                                                    "title": item.get("jp_title", ""),
                                                    "composer": ((item.get("details") or {}).get("director") or "").strip(),
                                                    "composer_country": normalize_country_code_for_flag(item.get("composer_country", "")),
                                                }]
                                                score_pages_for_link = _get_score_pages()
                                                for perf_id in rel_ids:
                                                    perf_page = _get_page_from_state_or_api(perf_id) or {}
                                                    perf_props = (perf_page.get("properties") or {})
                                                    perf_title = get_title(perf_props)[0] or item.get("jp_title", "")
                                                    section = (item.get("setlist_section") or "本編").strip()
                                                    if section not in ("幕前", "ロビー", "本編", "Encore", "ソリストEncore"):
                                                        section = "本編"
                                                    score_item = {
                                                        "title": item.get("jp_title", ""),
                                                        "order": max(int(item.get("setlist_order", 1) or 1), 1),
                                                        "part": item.get("part", ""),
                                                        "played": bool(item.get("played", True)),
                                                        "players": item.get("players", []) or [],
                                                        "section": section,
                                                        "movement_name": item.get("movement_name", ""),
                                                        "movement_no": item.get("movement_no"),
                                                        "movement_order": item.get("movement_order"),
                                                        "movement_roman": item.get("movement_roman", ""),
                                                    }
                                                    main_items = [score_item] if section in ("幕前", "ロビー", "本編") else []
                                                    encore_items = [score_item] if section in ("Encore", "ソリストEncore") else []
                                                    c_set, f_set, reason_set, created_rows = create_setlist_rows_for_performance(
                                                        performance_page_id=perf_id,
                                                        performance_title=perf_title,
                                                        performance_date=perf_date,
                                                        main_items=main_items,
                                                        encore_items=encore_items,
                                                        selected_scores=selected_scores_for_link,
                                                        score_pages=score_pages_for_link,
                                                    )
                                                    linked_setlist_created += c_set
                                                    linked_setlist_failed += f_set
                                                    if reason_set:
                                                        linked_setlist_reasons.append(reason_set)
                                                    if NOTION_SONG_ASSIGN_DB_ID and created_rows:
                                                        cast_row_map = _get_cast_row_map_for_performance(perf_id)
                                                        c_asg, f_asg, _reason_asg = create_song_assignment_rows(
                                                            score_rows=created_rows,
                                                            cast_row_map=cast_row_map,
                                                        )
                                                        linked_assign_created += c_asg
                                                        linked_assign_failed += f_asg
                                    success_count += 1
                                prog.progress((n + 1) / len(st.session_state.reg_cart))
                                time.sleep(0.2)
                            fail_count = max(0, total_count - success_count)
                            for key in ["reg_cart", "mb_works", "mb_checked", "mb_composers"]:
                                st.session_state.pop(key, None)
                            if success_count > 0:
                                st.success(f"✅ {success_count} 件登録完了" + (f"　❌ {fail_count} 件失敗" if fail_count else ""))
                            else:
                                st.error("❌ 登録できませんでした（0 件）")
                            reset_new_register_state()
                            if st.session_state.get("auto_reload_mode") == "partial":
                                for p in st.session_state.get("created_pages", []):
                                    upsert_page_in_state(p)
                                st.session_state.created_pages = []
                            else:
                                sync_notion_after_update()
                            if linked_setlist_created > 0 or linked_setlist_failed > 0:
                                if linked_setlist_failed == 0:
                                    st.success(f"✅ 演奏曲DB連動: {linked_setlist_created} 件")
                                else:
                                    st.warning(f"⚠️ 演奏曲DB連動: 成功 {linked_setlist_created} 件 / 失敗 {linked_setlist_failed} 件")
                                    if linked_setlist_reasons:
                                        reason_text = " / ".join(list(dict.fromkeys(linked_setlist_reasons))[:3])
                                        st.caption(f"ℹ️ 失敗理由: {reason_text}")
                            if linked_assign_created > 0 or linked_assign_failed > 0:
                                if linked_assign_failed == 0:
                                    st.success(f"✅ 楽曲別担当者DB連動: {linked_assign_created} 件")
                                else:
                                    st.warning(f"⚠️ 楽曲別担当者DB連動: 成功 {linked_assign_created} 件 / 失敗 {linked_assign_failed} 件")
                            if success_count > 0:
                                show_post_register_ui()
                    with col_clear:
                        if st.button("登録リストをクリア", key="cart_clear_score"):
                            st.session_state.reg_cart = []
                            st.rerun()
                else:
                    st.caption("登録リストは空です。検索タブで追加してください。")
                st.stop()

            st.divider()
            st.caption("1) 作曲家を検索 → 2) 作曲家を確定 → 3) 曲名で検索 → 4) 曲を選択")
            c_reset1, _ = st.columns([1, 3])
            if c_reset1.button("🧹 検索状態リセット", key="score_search_reset"):
                reset_score_search_state(clear_cache=True)
                st.success("演奏曲検索の状態とキャッシュをリセットしました。")
                st.rerun()

            with st.form("mb_composer_form", clear_on_submit=False):
                composer_input = st.text_input(
                    "1. 作曲家を検索",
                    key="mb_composer_query",
                    placeholder="例: Beethoven / ベートーヴェン",
                )
                mb_composer_submit = st.form_submit_button("🔍 作曲家を検索")
            if mb_composer_submit:
                composer_query = (composer_input or "").strip()
                if composer_query:
                    with st.spinner("作曲家を検索中..."):
                        composers, err = search_mb_composer(composer_query)
                    if err:
                        st.error(f"⚠️ MusicBrainz API エラー: {err}")
                    st.session_state.mb_composers = composers
                    st.session_state.mb_works = []
                    st.session_state.mb_checked = {}
                    st.session_state.mb_selected_comp = None
                    st.session_state.mb_work_title_filter = ""
                    if not composers and not err:
                        st.warning("作曲家が見つかりませんでした。")
                else:
                    st.warning("作曲家名を入力してください")

            selected_comp = st.session_state.get("mb_selected_comp")
            if st.session_state.get("mb_composers"):
                composers = st.session_state.mb_composers
                comp_labels = [format_mb_composer_label(c) for c in composers]
                selected_idx = st.radio(
                    "2. 作曲家を特定",
                    range(len(comp_labels)),
                    format_func=lambda i: comp_labels[i],
                    key="mb_comp_radio",
                )
                if st.button("✅ この作曲家で進める", key="mb_pick_comp"):
                    st.session_state.mb_selected_comp = composers[selected_idx]
                    st.session_state.mb_works = []
                    st.session_state.mb_checked = {}
                    st.rerun()
                selected_comp = st.session_state.get("mb_selected_comp")
                if selected_comp:
                    st.success(f"作曲家を確定: {format_mb_composer_label(selected_comp)}")
                    with st.expander("🔎 国コード判定ログ（デバッグ）", expanded=False):
                        if st.button("この作曲家の判定経路を表示", key="mb_country_trace_run"):
                            cname = canonical_mb_composer_name(selected_comp)
                            st.session_state["mb_country_trace_result"] = trace_get_composer_country_code(cname)
                        trace_result = st.session_state.get("mb_country_trace_result")
                        if trace_result:
                            st.json(trace_result)

                        st.caption("Wikidata QID を直接検証（例: Q131861）")
                        qid_text = st.text_input("QID", key="mb_wikidata_debug_qid", value="Q131861")
                        if st.button("QIDの解決経路を表示", key="mb_wikidata_trace_run"):
                            st.session_state["mb_wikidata_trace_result"] = debug_wikidata_country_resolution(qid_text)
                        qid_result = st.session_state.get("mb_wikidata_trace_result")
                        if qid_result:
                            st.json(qid_result)

            if selected_comp:
                with st.form("mb_work_form", clear_on_submit=False):
                    work_title_filter = st.text_input(
                        "3. 検索ワード（曲名）",
                        key="mb_work_title_filter",
                        placeholder="例: Symphony no.5 / Piano Concerto",
                    )
                    col_work_search, col_work_all = st.columns([1, 1])
                    mb_work_submit = col_work_search.form_submit_button("🔍 曲名で検索")
                    mb_fetch_all_submit = col_work_all.form_submit_button("📚 全作品を取得（重い）")
                if mb_work_submit:
                    work_query = (work_title_filter or "").strip()
                    if not work_query:
                        st.warning("曲名の検索ワードを入力してください。")
                    else:
                        st.session_state.mb_title_filter = work_query
                        with st.spinner(f"{selected_comp['name']} の作品を検索中..."):
                            works = search_mb_works(selected_comp["id"], work_query)
                        st.session_state.mb_works = works
                        st.session_state.mb_checked = {}
                        if not works:
                            st.info("作品候補が見つかりませんでした。作曲家情報のみで作品登録がない可能性があります。")
                if mb_fetch_all_submit:
                    st.session_state.mb_title_filter = ""
                    with st.spinner(f"{selected_comp['name']} の全作品を取得中...（数分かかることがあります）"):
                        works = search_mb_works(selected_comp["id"], "")
                    st.session_state.mb_works = works
                    st.session_state.mb_checked = {}
                    if not works:
                        st.info("全作品を取得しましたが候補がありませんでした。作曲家情報のみで作品登録がない可能性があります。")

            selected_works = []
            if st.session_state.get("mb_works"):
                works = st.session_state.mb_works
                comp  = st.session_state.mb_selected_comp or {}
                st.caption(f"{len(works)} 件の作品")
                col_all, col_none = st.columns([1, 1])
                if col_all.button("全選択",  key="mb_all"):
                    for w in works: st.session_state.mb_checked[w["id"]] = True
                if col_none.button("全解除", key="mb_none"):
                    st.session_state.mb_checked = {}

                for w in works:
                    label = w["title"] + (f"　{w['disambiguation']}" if w["disambiguation"] else "")
                    checked = st.checkbox(label, key=f"mb_{w['id']}", value=st.session_state.mb_checked.get(w["id"], False))
                    if checked:
                        st.session_state.mb_checked[w["id"]] = True
                    else:
                        st.session_state.mb_checked.pop(w["id"], None)

                selected_works = [w for w in works if st.session_state.mb_checked.get(w["id"])]
                if selected_works:
                    st.info(f"{len(selected_works)} 件選択中")

                    # 肖像画取得
                    comp_name  = canonical_mb_composer_name(comp) or comp.get("name", "")
                    artist_id  = comp.get("id", "")
                    force_refresh = st.session_state.get("mb_portrait_force_refresh_once", False)
                    if st.button("🔄 肖像画を再取得（Drive既存を無視）", key="mb_portrait_force_refresh_btn"):
                        st.session_state.mb_portrait_force_refresh_once = True
                        st.session_state.mb_portrait_url = None
                        st.session_state.mb_portrait_comp = ""
                        st.rerun()
                    cover_url_final = MB_DEFAULT_COVER
                    if force_refresh or "mb_portrait_url" not in st.session_state or st.session_state.get("mb_portrait_comp") != artist_id:
                        with st.spinner(f"🖼️ {comp_name} の肖像画を取得中..."):
                            portrait_url = get_composer_portrait_url(comp_name, artist_id, force_refresh=force_refresh)
                        st.session_state.mb_portrait_url  = portrait_url
                        st.session_state.mb_portrait_comp = artist_id
                        st.session_state.mb_portrait_force_refresh_once = False
                    else:
                        portrait_url = st.session_state.mb_portrait_url

                    if portrait_url:
                        if portrait_url.startswith("https://drive.google.com"):
                            try:
                                preview_url = with_cache_bust(portrait_url)
                                img_res = requests.get(preview_url, timeout=8)
                                st.image(io.BytesIO(img_res.content), width=120, caption=comp_name)
                            except Exception:
                                st.image(with_cache_bust(portrait_url), width=120, caption=comp_name)
                        else:
                            st.image(portrait_url, width=120, caption=comp_name)
                        is_drive_portrait = portrait_url.startswith("https://drive.google.com")
                        st.caption("保存先: Drive" if is_drive_portrait else "保存先: 外部URL（Drive未保存の可能性）")
                        last_reason = (st.session_state.get("mb_portrait_last_reason") or "").strip()
                        if last_reason:
                            st.caption(f"取得状況: {last_reason}")
                        if (not is_drive_portrait) and st.button("💾 現在画像をDriveに保存", key="mb_portrait_save_current_to_drive"):
                            img_bytes_now, mimetype_now, why_now = _download_image_bytes(portrait_url)
                            if not img_bytes_now:
                                st.warning(f"画像取得に失敗しました: {why_now}")
                            else:
                                save_name = make_portrait_filename(comp_name)
                                file_id = save_bytes_to_drive(
                                    save_name,
                                    img_bytes_now,
                                    mimetype_now or "image/jpeg",
                                    make_public=True,
                                )
                                if file_id:
                                    st.session_state.mb_portrait_url = drive_image_url(file_id)
                                    st.session_state.mb_portrait_comp = artist_id
                                    st.success("Driveに保存しました")
                                    st.rerun()
                                else:
                                    st.warning("Drive保存に失敗しました")
                        cover_url_final = portrait_url
                        with st.expander("🛠 肖像画を別候補に変更", expanded=False):
                            cand_key = "mb_portrait_candidates"
                            cand_comp_key = "mb_portrait_candidates_comp"
                            if st.button("🔁 別候補を検索", key="mb_portrait_find_alts"):
                                with st.spinner("肖像候補を収集中..."):
                                    cands = collect_composer_portrait_candidates(comp_name, artist_id, limit=10)
                                st.session_state[cand_key] = cands
                                st.session_state[cand_comp_key] = artist_id
                            candidates = st.session_state.get(cand_key, [])
                            if st.session_state.get(cand_comp_key) != artist_id:
                                candidates = []
                            if candidates:
                                st.caption(f"候補を一括表示中: {len(candidates)} 件")
                                for row_start in range(0, len(candidates), 5):
                                    row = candidates[row_start:row_start + 5]
                                    cols = st.columns(5)
                                    for i, url in enumerate(row):
                                        idx = row_start + i
                                        with cols[i]:
                                            st.image(url, width=120)
                                            st.markdown(f"[🔗 ソース]({url})")
                                            if st.button("✅ 採用", key=f"mb_portrait_use_alt_{idx}"):
                                                img_bytes, mimetype, why = _download_image_bytes(url)
                                                if not img_bytes:
                                                    st.warning(f"候補画像の取得に失敗しました: {why}")
                                                else:
                                                    save_name = make_portrait_filename(comp_name)
                                                    file_id = save_bytes_to_drive(
                                                        save_name,
                                                        img_bytes,
                                                        mimetype or "image/jpeg",
                                                        make_public=True,
                                                    )
                                                    if file_id:
                                                        new_url = with_cache_bust(drive_image_url(file_id))
                                                        st.session_state["mb_portrait_last_reason"] = "候補画像を採用"
                                                    else:
                                                        new_url = url
                                                    st.session_state.mb_portrait_url = new_url
                                                    st.session_state.mb_portrait_comp = artist_id
                                                    st.success("肖像画を更新しました")
                                                    st.rerun()
                            elif cand_key in st.session_state:
                                st.caption("候補が見つかりませんでした。手動アップロードをご利用ください。")
                            st.divider()
                            st.caption("または、手動アップロードで上書き")
                            uploaded_alt = st.file_uploader("肖像画をアップロード", type=["jpg", "jpeg", "png"], key="mb_portrait_upload_alt")
                            if uploaded_alt:
                                default_fname = sanitize_filename(comp_name)
                                custom_fname = st.text_input(
                                    "Drive保存名（変更可）",
                                    value=default_fname,
                                    key="mb_portrait_fname_alt",
                                )
                                if st.button("📤 手動アップロード画像を適用", key="mb_portrait_upload_apply_alt"):
                                    img_bytes_alt = uploaded_alt.getvalue()
                                    mimetype_alt = "image/png" if uploaded_alt.name.lower().endswith(".png") else "image/jpeg"
                                    file_id = save_manual_portrait_for_composer(
                                        comp_name,
                                        img_bytes_alt,
                                        mimetype_alt,
                                        custom_basename=custom_fname,
                                    )
                                    if file_id:
                                        new_url = with_cache_bust(drive_image_url(file_id))
                                        st.session_state["mb_portrait_last_reason"] = "手動アップロードを適用"
                                        st.success("手動アップロード画像を適用しました")
                                    else:
                                        st.warning("Drive保存に失敗しました。通信安定後に再度お試しください。")
                                        new_url = st.session_state.get("mb_portrait_url") or MB_DEFAULT_COVER
                                    st.session_state.mb_portrait_url = new_url
                                    st.session_state.mb_portrait_comp = artist_id
                                    st.rerun()
                    else:
                        st.warning(f"⚠️ {comp_name} の肖像画が見つかりませんでした。画像をアップロードしてください。")
                        last_reason = (st.session_state.get("mb_portrait_last_reason") or "").strip()
                        if last_reason:
                            st.caption(f"取得状況: {last_reason}")
                        uploaded = st.file_uploader("肖像画をアップロード", type=["jpg", "jpeg", "png"], key="mb_portrait_upload")
                        if uploaded:
                            default_fname = sanitize_filename(comp_name)
                            custom_fname  = st.text_input(
                                "Drive保存名（変更可）",
                                value=default_fname,
                                key="mb_portrait_fname",
                                help="この名前でDriveに保存されます。デフォルト名のままにしておくと、次回以降この作曲家を選択した際に自動で使いまわせます。",
                            )
                            if custom_fname != default_fname:
                                st.caption("⚠️ 名前を変更すると次回自動使用されません。このセッションのみ有効です。")
                            if st.button("📤 手動アップロード画像を適用", key="mb_portrait_upload_apply_default"):
                                img_bytes = uploaded.getvalue()
                                mimetype  = "image/png" if uploaded.name.lower().endswith(".png") else "image/jpeg"
                                with st.spinner("Driveに保存中..."):
                                    file_id = save_manual_portrait_for_composer(
                                        comp_name,
                                        img_bytes,
                                        mimetype,
                                        custom_basename=custom_fname,
                                    )
                                    if file_id:
                                        cover_url_final = with_cache_bust(drive_image_url(file_id))
                                        st.session_state.mb_portrait_url = cover_url_final
                                        st.session_state.mb_portrait_comp = artist_id
                                        st.session_state["mb_portrait_last_reason"] = "手動アップロードを適用"
                                    else:
                                        st.warning("Drive保存に失敗しました。今回のみアップロード画像を利用します。")
                                        cover_url_final = MB_DEFAULT_COVER
                                st.image(io.BytesIO(img_bytes), width=120, caption=comp_name)
                        else:
                            cover_url_final = MB_DEFAULT_COVER

                    # ── 出演との関連付け ──
                    st.divider()
                    st.subheader("🎤 出演履歴の関連付け")
                    if "score_perf_selected" not in st.session_state:
                        st.session_state.score_perf_selected = []
                    if "score_perf_selected_ids" not in st.session_state:
                        st.session_state.score_perf_selected_ids = []
                    perf_pages = _get_performance_pages()
                    st.session_state.score_perf_selected = _prune_selected_relations(
                        st.session_state.get("score_perf_selected", []),
                        perf_pages,
                    )
                    st.session_state.score_perf_selected_ids = _clean_relation_ids(
                        [x.get("id") for x in st.session_state.score_perf_selected]
                    )
                    if not st.session_state.get("last_notion_load_ok", True):
                        st.warning("⚠️ 出演データの取得に失敗しました。手動で再読み込みしてください。")
                    if st.button("🔄 出演候補を再読み込み", key="score_perf_reload"):
                        perf_pages = _get_performance_pages(force_refresh=True)
                        st.session_state.score_perf_selected = _prune_selected_relations(
                            st.session_state.get("score_perf_selected", []),
                            perf_pages,
                        )
                        st.session_state.score_perf_selected_ids = _clean_relation_ids(
                            [x.get("id") for x in st.session_state.score_perf_selected]
                        )
                        st.rerun()
                    perf_query = st.text_input("公演名で検索", key="score_perf_query", placeholder="例: 定期演奏会")
                    perf_matches = []
                    if perf_query:
                        q = perf_query.strip().lower()
                        perf_matches = [p for p in perf_pages if q in (p.get("title") or "").strip().lower()]

                    def add_selected_perf(pid, title):
                        selected = st.session_state.score_perf_selected
                        if not any(x["id"] == pid for x in selected):
                            selected.append({"id": pid, "title": title})
                            st.session_state.score_perf_selected = selected
                            st.session_state.score_perf_selected_ids = _clean_relation_ids([x.get("id") for x in selected])

                    if perf_matches:
                        options = ["（選択してください）"] + [p["title"] for p in perf_matches]
                        sel = st.selectbox("候補", options, key="score_perf_pick")
                        if sel != "（選択してください）":
                            picked = perf_matches[options.index(sel) - 1]
                            if st.button("🎻 出演を追加", key="score_perf_add"):
                                add_selected_perf(picked["id"], picked["title"])
                                st.rerun()
                    elif perf_query:
                        st.caption("候補が見つかりませんでした。")
                    if perf_query:
                        if st.button("🆕 演奏会（出演）を新規登録して追加", key="score_perf_create"):
                            new_title = perf_query.strip()
                            if not new_title:
                                st.warning("新規作成するタイトルを入力してください。")
                            else:
                                with st.spinner("出演データを新規作成中..."):
                                    ok = create_notion_page(
                                        jp_title=new_title, en_title=new_title,
                                        media_type_label="出演",
                                        tmdb_id=None, media_type="event",
                                        cover_url=get_media_icon_url("出演"),
                                        tmdb_release="",
                                        details={"genres": [], "cast": "", "director": "", "score": None},
                                    )
                                if ok:
                                    new_id = st.session_state.get("last_created_page_id")
                                    upsert_page_in_state(st.session_state.get("last_created_page"))
                                    _add_performance_page_cache(new_id, new_title)
                                    add_selected_perf(new_id, new_title)
                                    sync_notion_after_update(
                                        page_id=new_id,
                                        updated_page=st.session_state.get("last_created_page"),
                                    )
                                    _focus_management_page(new_id, new_title, "出演")
                                    st.session_state.pending_app_mode = "データ管理"
                                    st.session_state.pending_notice = "✅ 出演データを追加しました（詳細編集を開きます）"
                                    st.rerun()
                                else:
                                    st.error("❌ 出演データの作成に失敗しました")

                    if st.session_state.score_perf_selected:
                        st.session_state.score_perf_selected_ids = _clean_relation_ids(
                            [x.get("id") for x in st.session_state.score_perf_selected]
                        )
                        st.caption("✅ 関連付け済み")
                        for i, item in enumerate(st.session_state.score_perf_selected):
                            col_t, col_del = st.columns([4, 1])
                            col_t.write(item["title"])
                            if col_del.button("✕", key=f"score_perf_rm_{i}"):
                                st.session_state.score_perf_selected = [
                                    x for j, x in enumerate(st.session_state.score_perf_selected) if j != i
                                ]
                                st.session_state.score_perf_selected_ids = _clean_relation_ids([x.get("id") for x in st.session_state.score_perf_selected])
                                st.rerun()
                    else:
                        st.session_state.score_perf_selected_ids = []
                    st.caption(f"紐付け対象ID: {len(_clean_relation_ids(st.session_state.get('score_perf_selected_ids', [])))} 件")

                    st.markdown("**① 共通設定**")
                    group_selected_works = False
                    manual_group_title = ""
                    if len(selected_works) > 1:
                        group_mode = st.radio(
                            "この複数曲を同一作品としてグルーピングしますか？",
                            ["はい（同一作品として扱う）", "いいえ（個別作品として扱う）"],
                            index=0,
                            key="mb_group_mode",
                            horizontal=True,
                        )
                        group_selected_works = group_mode.startswith("はい")
                        if group_selected_works:
                            default_group_title = _normalize_work_title_for_group(
                                (selected_works[0].get("title") or "").strip()
                            ) or (selected_works[0].get("title") or "").strip()
                            manual_group_title = st.text_input(
                                "グルーピング時の作品名（共通）",
                                value=default_group_title,
                                key="mb_group_title",
                                placeholder="例: Symphony no.41 in C major, K. 551",
                            ).strip()
                        else:
                            st.caption("個別作品として登録します（自動グルーピングは行いません）。")

                    suggested_order = 1
                    preview_perf_ids = _clean_relation_ids(st.session_state.get("score_perf_selected_ids", []))
                    if not preview_perf_ids:
                        preview_perf_ids = _clean_relation_ids(
                            [x.get("id") for x in st.session_state.get("score_perf_selected", [])]
                        )
                    if preview_perf_ids:
                        suggested_order = _suggest_next_setlist_order(preview_perf_ids[0])

                    section_options = ["幕前", "ロビー", "本編", "Encore", "ソリストEncore"]
                    common_cols = st.columns([2, 1])
                    common_section = common_cols[0].selectbox(
                        "区分（共通）",
                        section_options,
                        index=section_options.index("本編"),
                        key="mb_common_section",
                    )
                    common_order = int(
                        common_cols[1].number_input(
                            "曲順（開始）",
                            min_value=1,
                            value=max(int(suggested_order or 1), 1),
                            step=1,
                            key="mb_common_order",
                        )
                    )
                    common_part = st.text_input(
                        "担当楽器（共通・任意）",
                        key="mb_common_part",
                        placeholder="例: Timp. / Fl. / Perc.",
                    ).strip()
                    common_release_text = st.text_input(
                        "リリース日（共通・任意 / YYYY-MM-DD）",
                        key="mb_common_release",
                        placeholder="例: 1921-06-14",
                    ).strip()
                    common_release_norm = _normalize_notion_date_input(common_release_text) if common_release_text else ""
                    if common_release_text and not common_release_norm:
                        st.caption("ℹ️ 共通リリース日は YYYY-MM-DD 形式で入力してください（未適用になります）。")

                    st.markdown("**② 曲データを確認（タイトル/楽章）**")
                    selected_ids_text = ",".join(sorted([(w.get("id") or "") for w in selected_works]))
                    editor_scope = uuid.uuid5(uuid.NAMESPACE_DNS, f"{artist_id}:{selected_ids_text}").hex[:10]
                    for idx, w in enumerate(selected_works):
                        work_id = (w.get("id") or f"work_{idx}").strip()
                        work_title = (w.get("title") or "").strip()
                        work_disamb = (w.get("disambiguation") or "").strip()
                        register_title_default = f"{work_title} ({work_disamb})" if work_disamb else work_title
                        movement_guess = _infer_movement_from_title(work_title)
                        default_mv_no = movement_guess.get("movement_no")
                        if not (isinstance(default_mv_no, int) and default_mv_no > 0):
                            default_mv_no = (idx + 1) if (group_selected_works and len(selected_works) > 1) else 0
                        default_mv_roman = (movement_guess.get("movement_roman") or "").strip()
                        if not default_mv_roman and default_mv_no > 0:
                            default_mv_roman = _int_to_roman(default_mv_no)

                        t_key = f"mb_edit_title_{editor_scope}_{work_id}"
                        n_key = f"mb_edit_mv_name_{editor_scope}_{work_id}"
                        no_key = f"mb_edit_mv_no_{editor_scope}_{work_id}"
                        r_key = f"mb_edit_mv_roman_{editor_scope}_{work_id}"
                        a_key = f"mb_edit_mv_roman_action_{editor_scope}_{work_id}"
                        if t_key not in st.session_state:
                            st.session_state[t_key] = register_title_default
                        if n_key not in st.session_state:
                            st.session_state[n_key] = movement_guess.get("movement_name", "") or ""
                        if no_key not in st.session_state:
                            st.session_state[no_key] = int(default_mv_no)
                        if r_key not in st.session_state:
                            st.session_state[r_key] = default_mv_roman
                        pending_roman_action = st.session_state.pop(a_key, None)
                        if pending_roman_action in ("inc", "dec"):
                            current_no = _roman_to_int((st.session_state.get(r_key) or "").strip())
                            if current_no is None:
                                current_no = int(st.session_state.get(no_key) or 0)
                            current_no = max(int(current_no), 1)
                            next_no = current_no + 1 if pending_roman_action == "inc" else max(current_no - 1, 1)
                            st.session_state[r_key] = _int_to_roman(next_no)
                            st.session_state[no_key] = next_no

                        row_order_preview = common_order if group_selected_works else (common_order + idx)
                        with st.expander(f"{idx + 1}. {register_title_default}", expanded=(idx == 0)):
                            meta_left, meta_right = st.columns([1, 1])
                            meta_left.caption(f"区分: {common_section}")
                            meta_right.caption(f"曲順: {row_order_preview}")
                            t_cols = st.columns([3, 2, 1.2, 1.1, 0.4, 0.4])
                            t_cols[0].text_input(
                                "タイトル",
                                key=t_key,
                                disabled=bool(group_selected_works),
                                help="グルーピング時は共通作品名が適用されます。",
                            )
                            t_cols[1].text_input("楽章名", key=n_key, placeholder="例: Allegro con brio")
                            t_cols[2].number_input("楽章No.", min_value=0, step=1, key=no_key)
                            t_cols[3].text_input("ローマ数字", key=r_key, placeholder="I / II / III")
                            if t_cols[4].button("－", key=f"mb_mv_roman_dec_{editor_scope}_{work_id}"):
                                st.session_state[a_key] = "dec"
                                st.rerun()
                            if t_cols[5].button("＋", key=f"mb_mv_roman_inc_{editor_scope}_{work_id}"):
                                st.session_state[a_key] = "inc"
                                st.rerun()

                    if st.button(f"📋 {len(selected_works)} 件を登録リストに追加", key="mb_add_cart"):
                        if len(selected_works) > 1 and group_selected_works and not manual_group_title:
                            st.warning("グルーピングする場合は共通作品名を入力してください。")
                            st.stop()
                        selected_perf_ids = _clean_relation_ids(st.session_state.get("score_perf_selected_ids", []))
                        if not selected_perf_ids:
                            selected_perf_ids = _clean_relation_ids(
                                [x.get("id") for x in st.session_state.get("score_perf_selected", [])]
                            )
                        # 候補を選んだだけで「＋追加」を押していないケースを救済
                        if not selected_perf_ids:
                            picked_label = st.session_state.get("score_perf_pick", "（選択してください）")
                            if picked_label and picked_label != "（選択してください）":
                                picked = next((p for p in perf_pages if p.get("title") == picked_label), None)
                                if picked:
                                    add_selected_perf(picked["id"], picked["title"])
                                    selected_perf_ids = [picked["id"]]
                                    st.session_state.score_perf_selected_ids = selected_perf_ids
                        if not selected_perf_ids:
                            st.warning("出演履歴が未選択です。出演データを紐付ける場合は先に追加してください。")
                        perf_release, perf_watched, perf_rating, perf_location = "", "", "", None
                        suggested_order = 1
                        if selected_perf_ids:
                            perf_page = _get_page_from_state_or_api(selected_perf_ids[0])
                            perf_release, perf_watched, perf_rating, perf_location = _extract_performance_defaults(perf_page)
                            suggested_order = _suggest_next_setlist_order(selected_perf_ids[0])
                        for idx, w in enumerate(selected_works):
                            work_id = (w.get("id") or f"work_{idx}").strip()
                            work_title = (w.get("title") or "").strip()
                            movement_guess = _infer_movement_from_title(work_title)
                            t_key = f"mb_edit_title_{editor_scope}_{work_id}"
                            n_key = f"mb_edit_mv_name_{editor_scope}_{work_id}"
                            no_key = f"mb_edit_mv_no_{editor_scope}_{work_id}"
                            r_key = f"mb_edit_mv_roman_{editor_scope}_{work_id}"

                            register_title = ((st.session_state.get(t_key) or "").strip() if not group_selected_works else manual_group_title.strip())
                            if not register_title:
                                register_title = (w.get("title") or "").strip()

                            if len(selected_works) > 1 and group_selected_works:
                                work_group_base = manual_group_title.strip()
                                work_group_key = f"{(comp_name or '').strip().lower()}::manual::{work_group_base.strip().lower()}"
                                row_order = common_order
                            else:
                                work_group_base = work_title
                                work_group_key = f"{(comp_name or '').strip().lower()}::single::{w.get('id') or work_group_base.strip().lower()}"
                                row_order = common_order + idx

                            movement_name_input = (st.session_state.get(n_key) or "").strip()
                            movement_no_input = int(st.session_state.get(no_key) or 0)
                            movement_roman_input = (st.session_state.get(r_key) or "").strip()
                            movement_no_val = movement_no_input if movement_no_input > 0 else None
                            movement_order_val = movement_no_val if movement_no_val is not None else movement_guess.get("movement_order")
                            if movement_order_val is None and len(selected_works) > 1 and group_selected_works:
                                movement_order_val = idx + 1
                            if not movement_roman_input and movement_no_val:
                                movement_roman_input = _int_to_roman(movement_no_val)

                            work_release = (w.get("first_release_date") or "").strip()
                            premiere_source = "musicbrainz-work"
                            if not work_release:
                                work_release, premiere_source = get_mb_work_premiere_info(
                                    w.get("id", ""),
                                    work_title=work_title,
                                    composer_name=comp_name,
                                )
                            release_precision = _date_precision(work_release)
                            release_partial = release_precision in ("year", "month")
                            if release_partial:
                                # 日付が年/月までしかない場合はNotion日付誤差を避けるため未設定にする
                                partial_value = work_release
                                work_release = ""
                            else:
                                partial_value = ""
                            if group_selected_works and common_release_norm:
                                work_release = common_release_norm
                                partial_value = ""
                                release_partial = False
                                premiere_source = "manual-common"
                            st.session_state.reg_cart.append({
                                "cart_uid":    f"score_{uuid.uuid4().hex[:10]}",
                                "jp_title":    register_title,
                                "en_title":    register_title,
                                "cover_url":   cover_url_final,
                                "release":     work_release or "",
                                "watched":     perf_watched or "",
                                "rating":      perf_rating or "",
                                "wlflg":       False,
                                "media_type":  "score",
                                "tmdb_id":     0,
                                "details":     {"genres": [], "cast": "", "director": comp_name, "score": None},
                                "composer_country": normalize_country_code_for_flag(get_composer_country_code(comp_name) or ""),
                                "isbn":        "",
                                "location":    perf_location,
                                "media_label": media_label,
                                "relation_prop": "出演履歴" if selected_perf_ids else None,
                                "relation_ids":  selected_perf_ids,
                                "premiere_missing": (not bool(work_release)),
                                "premiere_source": premiere_source if work_release else (premiere_source or "not-found"),
                                "premiere_partial": release_partial,
                                "premiere_partial_value": partial_value,
                                "setlist_order": row_order,
                                "setlist_section": common_section,
                                "played": True,
                                "part": common_part if group_selected_works else "",
                                "is_concerto": False,
                                "soloists": "",
                                "players": [],
                                "movement_name": movement_name_input,
                                "movement_no": movement_no_val,
                                "movement_order": movement_order_val,
                                "movement_roman": movement_roman_input,
                                "mb_work_id": w.get("id", ""),
                                "manual_group_title": work_group_base if (len(selected_works) > 1 and group_selected_works) else "",
                            })
                        st.session_state.mb_checked = {}
                        st.success(f"✅ {len(selected_works)} 件を登録リストに追加しました")
                        st.session_state.active_score_tab_next = "登録リスト"
                        st.rerun()

            if not selected_works and selected_comp:
                comp_name_now = canonical_mb_composer_name(selected_comp) or selected_comp.get("name", "")
                artist_id_now = selected_comp.get("id", "")
                st.caption("作品未選択でも、手入力登録に備えて肖像画を先に設定できます。")
                col_np1, col_np2 = st.columns([1, 1])
                if col_np1.button("🖼️ 肖像画を取得", key="mb_portrait_fetch_no_work"):
                    with st.spinner(f"{comp_name_now} の肖像画を取得中..."):
                        portrait_now = get_composer_portrait_url(comp_name_now, artist_id_now)
                    st.session_state["mb_portrait_url"] = portrait_now
                    st.session_state["mb_portrait_comp"] = artist_id_now
                    st.rerun()
                if col_np2.button("🔄 肖像画を再取得（Drive既存を無視）", key="mb_portrait_force_fetch_no_work"):
                    with st.spinner(f"{comp_name_now} の肖像画を再取得中..."):
                        portrait_now = get_composer_portrait_url(comp_name_now, artist_id_now, force_refresh=True)
                    st.session_state["mb_portrait_url"] = portrait_now
                    st.session_state["mb_portrait_comp"] = artist_id_now
                    st.rerun()

                portrait_cached = st.session_state.get("mb_portrait_url")
                if portrait_cached and st.session_state.get("mb_portrait_comp") == artist_id_now:
                    st.image(portrait_cached, width=120, caption=comp_name_now)

                uploaded_no_work = st.file_uploader(
                    "肖像画をアップロード（手入力用）",
                    type=["jpg", "jpeg", "png"],
                    key="mb_portrait_upload_no_work",
                )
                if uploaded_no_work and st.button("📤 手動アップロード画像を適用", key="mb_portrait_upload_apply_no_work"):
                    img_bytes_now = uploaded_no_work.getvalue()
                    mimetype_now = "image/png" if uploaded_no_work.name.lower().endswith(".png") else "image/jpeg"
                    file_id_now = save_manual_portrait_for_composer(comp_name_now, img_bytes_now, mimetype_now)
                    if file_id_now:
                        st.session_state["mb_portrait_url"] = with_cache_bust(drive_image_url(file_id_now))
                        st.session_state["mb_portrait_comp"] = artist_id_now
                        st.success("手入力用の肖像画を保存しました")
                        st.rerun()
                    else:
                        st.warning("Drive保存に失敗しました。通信状況を確認して再実行してください。")
                st.divider()

            manual_composer_default = ""
            manual_country_default = ""
            if selected_comp:
                manual_composer_default = canonical_mb_composer_name(selected_comp) or selected_comp.get("name", "")
                manual_country_default = (selected_comp.get("country") or "").strip().upper()
            else:
                manual_composer_default = (st.session_state.get("mb_composer_query") or "").strip()
            if (
                "mb_manual_comp_name" not in st.session_state
                or (
                    selected_comp
                    and st.session_state.get("mb_manual_comp_source_id") != selected_comp.get("id")
                )
            ):
                st.session_state["mb_manual_comp_name"] = manual_composer_default
                st.session_state["mb_manual_country_code"] = manual_country_default
                st.session_state["mb_manual_comp_source_id"] = selected_comp.get("id") if selected_comp else ""

            st.divider()
            with st.expander("📝 MusicBrainzにない曲を手入力で追加", expanded=False):
                    if "score_perf_selected" not in st.session_state:
                        st.session_state.score_perf_selected = []
                    if "score_perf_selected_ids" not in st.session_state:
                        st.session_state.score_perf_selected_ids = []
                    perf_pages_manual = _get_performance_pages()
                    st.markdown("**出演履歴を関連付け（任意）**")
                    perf_q_manual = st.text_input(
                        "公演名で検索",
                        key="mb_manual_perf_query",
                        placeholder="例: 定期演奏会",
                    )
                    perf_matches_manual = []
                    if perf_q_manual:
                        ql = perf_q_manual.strip().lower()
                        perf_matches_manual = [p for p in perf_pages_manual if ql in (p.get("title") or "").strip().lower()]
                    if perf_matches_manual:
                        perf_opts = ["（選択してください）"] + [p["title"] for p in perf_matches_manual]
                        picked_title = st.selectbox("候補", perf_opts, key="mb_manual_perf_pick")
                        if picked_title != "（選択してください）":
                            picked_perf_now = perf_matches_manual[perf_opts.index(picked_title) - 1]
                            st.session_state["mb_manual_perf_last_id"] = picked_perf_now.get("id", "")
                            st.session_state["mb_manual_perf_last_title"] = picked_perf_now.get("title", "")
                        if picked_title != "（選択してください）" and st.button("🎻 出演を追加", key="mb_manual_perf_add"):
                            picked_perf = perf_matches_manual[perf_opts.index(picked_title) - 1]
                            selected = st.session_state.get("score_perf_selected", [])
                            if not any((x.get("id") or "") == picked_perf["id"] for x in selected):
                                selected.append({"id": picked_perf["id"], "title": picked_perf["title"]})
                                st.session_state.score_perf_selected = selected
                                st.session_state.score_perf_selected_ids = _clean_relation_ids([x.get("id") for x in selected])
                            st.rerun()
                    elif perf_q_manual:
                        st.caption("候補が見つかりませんでした。")
                    selected_manual = st.session_state.get("score_perf_selected", [])
                    if selected_manual:
                        st.caption("✅ 関連付け済み")
                        for i, rel in enumerate(selected_manual):
                            c1, c2 = st.columns([4, 1])
                            c1.write(rel.get("title", ""))
                            if c2.button("✕", key=f"mb_manual_perf_rm_{i}"):
                                st.session_state.score_perf_selected = [x for j, x in enumerate(selected_manual) if j != i]
                                st.session_state.score_perf_selected_ids = _clean_relation_ids(
                                    [x.get("id") for x in st.session_state.get("score_perf_selected", [])]
                                )
                                st.rerun()
                    st.caption(f"紐付け対象ID: {len(_clean_relation_ids(st.session_state.get('score_perf_selected_ids', [])))} 件")
                    st.divider()
                    manual_comp_name = st.text_input(
                        "作曲家名（手入力）",
                        key="mb_manual_comp_name",
                        placeholder="例: Takashi Yoshimatsu / 吉松 隆",
                    )
                    manual_comp_key = (manual_comp_name or "").strip()
                    if manual_comp_key:
                        st.caption("作曲家情報が未登録でも、ここで肖像画を先に設定できます。")
                        mcol1, mcol2 = st.columns([1, 1])
                        if mcol1.button("🖼️ 作曲家肖像を取得", key="mb_manual_portrait_fetch"):
                            with st.spinner(f"{manual_comp_key} の肖像画を取得中..."):
                                manual_portrait = get_composer_portrait_url(manual_comp_key, "")
                            st.session_state["mb_portrait_url"] = manual_portrait
                            st.session_state["mb_portrait_comp"] = f"manual:{manual_comp_key.lower()}"
                            st.rerun()
                        if mcol2.button("🔄 肖像画を再取得（Drive既存を無視）", key="mb_manual_portrait_refresh"):
                            with st.spinner(f"{manual_comp_key} の肖像画を再取得中..."):
                                manual_portrait = get_composer_portrait_url(manual_comp_key, "", force_refresh=True)
                            st.session_state["mb_portrait_url"] = manual_portrait
                            st.session_state["mb_portrait_comp"] = f"manual:{manual_comp_key.lower()}"
                            st.rerun()
                        manual_cached = st.session_state.get("mb_portrait_url")
                        if manual_cached and st.session_state.get("mb_portrait_comp") in ("", f"manual:{manual_comp_key.lower()}"):
                            st.image(manual_cached, width=120, caption=manual_comp_key)
                        manual_uploaded = st.file_uploader(
                            "肖像画をアップロード（手入力作曲家）",
                            type=["jpg", "jpeg", "png"],
                            key="mb_manual_portrait_upload",
                        )
                        if manual_uploaded and st.button("📤 手動アップロード画像を適用", key="mb_manual_portrait_upload_apply"):
                            up_bytes = manual_uploaded.getvalue()
                            up_mime = "image/png" if manual_uploaded.name.lower().endswith(".png") else "image/jpeg"
                            up_file_id = save_manual_portrait_for_composer(manual_comp_key, up_bytes, up_mime)
                            if up_file_id:
                                st.session_state["mb_portrait_url"] = with_cache_bust(drive_image_url(up_file_id))
                                st.session_state["mb_portrait_comp"] = f"manual:{manual_comp_key.lower()}"
                                st.success("手入力作曲家の肖像画を保存しました")
                                st.rerun()
                            else:
                                st.warning("Drive保存に失敗しました。通信状況を確認して再実行してください。")
                        st.divider()
                    if "mb_manual_country_code_next" in st.session_state:
                        st.session_state["mb_manual_country_code"] = st.session_state.pop("mb_manual_country_code_next")
                    cc_col_inp, cc_col_btn = st.columns([3, 1])
                    manual_country_code = cc_col_inp.text_input(
                        "国コード（任意, 2文字）",
                        key="mb_manual_country_code",
                        placeholder="例: JP / DE / FR",
                    )
                    if cc_col_btn.button("🌐 自動取得", key="mb_manual_country_auto"):
                        guessed_cc = normalize_country_code_for_flag(get_composer_country_code(manual_comp_key) or "")
                        if guessed_cc:
                            st.session_state["mb_manual_country_code_next"] = guessed_cc
                            st.success(f"国コード候補: {guessed_cc}")
                            st.rerun()
                        else:
                            st.warning("国コードを自動取得できませんでした。必要なら手入力してください。")
                    manual_title = st.text_input(
                        "曲名（手入力）",
                        key="mb_manual_work_title",
                        placeholder="例: 委嘱新作 / 初演作品 / 現代作品",
                    )
                    manual_note = st.text_input(
                        "補足（任意）",
                        key="mb_manual_work_note",
                        placeholder="例: world premiere / revised 2024",
                    )
                    if st.button(
                        "➕ 手入力曲を登録リストに追加",
                        key="mb_manual_add_to_cart",
                        disabled=(not (manual_comp_name or "").strip() or not (manual_title or "").strip()),
                    ):
                        composer_raw = (manual_comp_name or "").strip()
                        country_raw = (manual_country_code or "").strip().upper()
                        title_raw = (manual_title or "").strip()
                        note_raw = (manual_note or "").strip()
                        if not composer_raw:
                            st.warning("作曲家名を入力してください。")
                        elif not title_raw:
                            st.warning("曲名を入力してください。")
                        else:
                            selected_perf_ids = _clean_relation_ids(st.session_state.get("score_perf_selected_ids", []))
                            # 候補を選んだだけで「🎻 出演を追加」を押していないケースを救済
                            if not selected_perf_ids:
                                picked_title = st.session_state.get("mb_manual_perf_pick", "（選択してください）")
                                if picked_title and picked_title != "（選択してください）":
                                    picked_perf = next((p for p in perf_pages_manual if p.get("title") == picked_title), None)
                                    if picked_perf:
                                        selected_perf_ids = [picked_perf["id"]]
                                        selected = st.session_state.get("score_perf_selected", [])
                                        if not any((x.get("id") or "") == picked_perf["id"] for x in selected):
                                            selected.append({"id": picked_perf["id"], "title": picked_perf["title"]})
                                            st.session_state.score_perf_selected = selected
                                        st.session_state.score_perf_selected_ids = _clean_relation_ids(
                                            [x.get("id") for x in st.session_state.get("score_perf_selected", [])]
                                        )
                            if not selected_perf_ids:
                                last_pid = (st.session_state.get("mb_manual_perf_last_id") or "").strip()
                                last_ptitle = (st.session_state.get("mb_manual_perf_last_title") or "").strip()
                                if last_pid:
                                    selected_perf_ids = [last_pid]
                                    selected = st.session_state.get("score_perf_selected", [])
                                    if not any((x.get("id") or "") == last_pid for x in selected):
                                        selected.append({"id": last_pid, "title": last_ptitle})
                                        st.session_state.score_perf_selected = selected
                                    st.session_state.score_perf_selected_ids = _clean_relation_ids(
                                        [x.get("id") for x in st.session_state.get("score_perf_selected", [])]
                                    )
                            perf_release, perf_watched, perf_rating, perf_location = "", "", "", None
                            suggested_order = 1
                            if selected_perf_ids:
                                perf_page = _get_page_from_state_or_api(selected_perf_ids[0])
                                perf_release, perf_watched, perf_rating, perf_location = _extract_performance_defaults(perf_page)
                                suggested_order = _suggest_next_setlist_order(selected_perf_ids[0])
                            if len(country_raw) != 2 or not country_raw.isalpha():
                                country_raw = ""
                            register_title = f"{title_raw} ({note_raw})" if note_raw else title_raw
                            movement_guess = _infer_movement_from_title(title_raw)
                            st.session_state.reg_cart.append({
                                "cart_uid":    f"score_{uuid.uuid4().hex[:10]}",
                                "jp_title":    register_title,
                                "en_title":    register_title,
                                "cover_url":   st.session_state.get("mb_portrait_url") or MB_DEFAULT_COVER,
                                "release":     "",
                                "watched":     perf_watched or "",
                                "rating":      perf_rating or "",
                                "wlflg":       False,
                                "media_type":  "score",
                                "tmdb_id":     0,
                                "details":     {"genres": [], "cast": "", "director": composer_raw, "score": None},
                                "composer_country": country_raw,
                                "isbn":        "",
                                "location":    perf_location,
                                "media_label": media_label,
                                "relation_prop": "出演履歴" if selected_perf_ids else None,
                                "relation_ids":  selected_perf_ids,
                                "premiere_missing": True,
                                "premiere_source": "manual-input",
                                "premiere_partial": False,
                                "premiere_partial_value": "",
                                "setlist_order": suggested_order,
                                "setlist_section": "本編",
                                "played": True,
                                "part": "",
                                "is_concerto": False,
                                "soloists": "",
                                "players": [],
                                "movement_name": movement_guess.get("movement_name", ""),
                                "movement_no": movement_guess.get("movement_no"),
                                "movement_order": movement_guess.get("movement_order"),
                                "movement_roman": movement_guess.get("movement_roman", ""),
                                "mb_work_id": "",
                            })
                            st.success("✅ 手入力曲を登録リストに追加しました")
                            st.session_state.active_score_tab_next = "登録リスト"
                            st.rerun()
            st.stop()

        # ============================================================
        # 通常媒体（映画・ドラマ・書籍・漫画・音楽アルバム・ゲーム・アニメ）
        # ============================================================
        st.divider()
        tab_options = ["検索", "候補", "登録リスト"]
        if st.session_state.get("confirm_reg") is not None:
            tab_options.append("確認")
        if "active_reg_tab_next" in st.session_state:
            st.session_state.active_reg_tab = st.session_state.pop("active_reg_tab_next")
        if "active_reg_tab" not in st.session_state:
            st.session_state.active_reg_tab = "検索"
        if st.session_state.active_reg_tab not in tab_options:
            st.session_state.active_reg_tab = "検索"
        if st.session_state.get("confirm_reg") is None and st.session_state.active_reg_tab == "確認":
            st.session_state.active_reg_tab = "候補"
        active_tab = st.segmented_control(
            "表示",
            options=tab_options,
            key="active_reg_tab",
            label_visibility="collapsed",
        )
        reg_cart_count = len(st.session_state.get("reg_cart", []))
        current_reg_tab = st.session_state.get("active_reg_tab")
        prev_reg_tab = st.session_state.get("_prev_active_reg_tab")
        if prev_reg_tab is not None and prev_reg_tab != current_reg_tab:
            emit_scroll_top_script()
        st.session_state["_prev_active_reg_tab"] = current_reg_tab
        if active_tab == "検索":
            reg_step = "1/4 検索"
        elif active_tab == "候補":
            reg_step = "2/4 候補"
        elif active_tab == "登録リスト":
            reg_step = "3/4 登録リスト"
        else:
            reg_step = "4/4 確認"
        st.caption(f"進捗: {reg_step}  |  登録予定 {reg_cart_count} 件")
        nav_labels = ["検索", "候補", "登録リスト"] + (["確認"] if "確認" in tab_options else [])
        nav_cols = st.columns(len(nav_labels))
        for i, label in enumerate(nav_labels):
            icon = "✅" if label == active_tab else "➡"
            if nav_cols[i].button(f"{icon} {label}", key=f"reg_nav_{label}"):
                st.session_state.active_reg_tab_next = label
                st.rerun()

        if active_tab == "検索":
            reg_cart_hint = st.session_state.get("reg_cart", [])
            if reg_cart_hint:
                st.info(f"🧺 登録リストに {len(reg_cart_hint)} 件あります。右の「登録リスト」タブで確認できます。")
            enter_search_kwargs = {"on_change": queue_new_search_from_enter}

            fast_book_search = True
            if media_label in ("書籍", "漫画"):
                fast_book_search = st.checkbox("高速検索（カバー簡易）", value=True, key="fast_book_search")

            if media_label in ["音楽アルバム"]:
                col_jp, col_en = st.columns([1, 1])
                jp_input      = clearable_text_input("アルバム名", "inp_jp_album", placeholder="例: 千のナイフ", container=col_jp, **enter_search_kwargs)
                creator_input = clearable_text_input("アーティスト名", "inp_creator_album", placeholder="例: 坂本龍一", container=col_en, **enter_search_kwargs)
                cast_input    = ""
                en_input      = ""
            elif media_label == "ゲーム":
                jp_input      = clearable_text_input("ゲームタイトル", "inp_jp_game", placeholder="例: ペルソナ5", **enter_search_kwargs)
                creator_input = ""
                cast_input    = ""
                en_input      = jp_input
            elif media_label == "アニメ":
                jp_input      = clearable_text_input("アニメタイトル", "inp_jp_anime", placeholder="例: 鬼滅の刃 / Demon Slayer", **enter_search_kwargs)
                creator_input = ""
                cast_input    = ""
                en_input      = jp_input
            elif media_label == "漫画":
                col_jp, col_en = st.columns([1, 1])
                jp_input      = clearable_text_input("タイトル", "inp_jp_manga", placeholder="例: 鬼滅の刃", container=col_jp, **enter_search_kwargs)
                creator_input = clearable_text_input("著者名", "inp_creator_manga", placeholder="例: 吾峠呼世晴", container=col_en, **enter_search_kwargs)
                cast_input    = ""
                en_input      = ""
            else:
                jp_ph = "例: 千と千尋の神隠し"
                en_ph = "例: Spirited Away"
                creator_ph = "例: 宮崎駿 / 道尾秀介"
                cast_ph = "例: 木村拓哉"
                if media_label in ("映画", "ドラマ"):
                    jp_ph = "例: ガーディアンズ・オブ・ギャラクシー"
                    en_ph = "例: Guardians of the Galaxy"
                    creator_ph = "例: ジェームズ・ガン"
                    cast_ph = "例: クリス・プラット"
                elif media_label == "書籍":
                    jp_ph = "例: 蜜蜂と遠雷"
                    en_ph = "例: Honeybees and Distant Thunder"
                    creator_ph = "例: 恩田陸"
                    cast_ph = "例: 出版社名（任意）"
                col_jp, col_en = st.columns([1, 1])
                jp_input      = clearable_text_input("日本語タイトル", "inp_jp_main", placeholder=jp_ph, container=col_jp, **enter_search_kwargs)
                en_input      = clearable_text_input("英語タイトル（検索用）", "inp_en_main", placeholder=en_ph, container=col_en, **enter_search_kwargs)
                col_creator, col_cast = st.columns([1, 1])
                creator_input = clearable_text_input("クリエイター（著者・監督）", "inp_creator_main", placeholder=creator_ph, container=col_creator, **enter_search_kwargs)
                cast_input    = clearable_text_input("キャスト・関係者", "inp_cast_main", placeholder=cast_ph, container=col_cast, **enter_search_kwargs)

            search_clicked = st.button("🔍 検索", key="new_search")
            search_from_enter = bool(st.session_state.pop("_pending_new_search_enter", False))
            if search_clicked or search_from_enter:
                query = en_input if en_input else jp_input
                if query or creator_input or cast_input:
                    if media_label == "書籍":
                        results = search_books(query or "", author=creator_input or None, fast=fast_book_search)
                    elif media_label == "漫画":
                        results = search_manga(query or "", author=creator_input or None, fast=fast_book_search)
                    elif media_label == "音楽アルバム":
                        results = search_albums(query or "", artist=creator_input or None)
                    elif media_label == "ゲーム":
                        gq = query or jp_input
                        st.session_state.last_game_query_jp = jp_input or ""
                        if not st.session_state.get("_game_jp_dict_dedupe_done"):
                            archived = _dedupe_game_jp_dict_all(max_groups=500)
                            st.session_state["_game_jp_dict_dedupe_done"] = True
                            if archived > 0:
                                st.info(f"🧹 ゲームJP辞書DBの重複を {archived} 件整理しました")
                        # ゲーム検索ごとに辞書保存トライ状態を初期化
                        st.session_state.pop("game_jp_autosaved", None)
                        st.session_state.pop("_game_dict_upsert_warned", None)
                        results = _search_games_for_ui(gq)
                        if not results and _contains_japanese(gq):
                            st.session_state.game_series_suggestions = search_game_series_candidates(gq, limit=10)
                        else:
                            st.session_state.pop("game_series_suggestions", None)
                    elif media_label == "アニメ":
                        results = search_anime(query or jp_input)
                    else:
                        tmdb_mt = "movie" if media_label == "映画" else "tv"
                        if creator_input or cast_input:
                            results = search_tmdb_by_person(creator_input or cast_input, media_type=tmdb_mt)
                        else:
                            results = search_tmdb(query, media_type=tmdb_mt)
                    reg_ids = get_registered_ids(st.session_state.pages)
                    filtered, excluded = filter_registered(results, media_label, reg_ids)
                    st.session_state.new_search_raw_count = len(results or [])
                    display_cap = 200 if media_label == "ゲーム" else 20
                    st.session_state.new_search_results  = filtered[:display_cap]
                    st.session_state.new_search_excluded = excluded
                    st.session_state.new_search_done     = True
                    st.session_state.confirm_reg         = None
                    st.session_state.bulk_checked        = {}
                    st.session_state.pop("game_work_selected", None)
                    st.session_state.rakuten_page        = 1
                    st.session_state.rakuten_query_key   = f"{media_label}|{query}|{creator_input}"
                    st.session_state.active_reg_tab_next = "候補"
                    st.rerun()
                else:
                    st.warning("タイトルまたはクリエイター名を入力してください")

        if active_tab == "確認":
            # ── 単体確認画面 ──
    
            if st.session_state.confirm_reg is not None:
                reg = st.session_state.confirm_reg
                reg_key = (
                    reg.get("media_type"),
                    reg.get("tmdb_id"),
                    reg.get("itunes_id"),
                    reg.get("anilist_id"),
                    reg.get("igdb_id"),
                )
                if st.session_state.get("confirm_reg_key") != reg_key:
                    st.session_state["confirm_reg_key"] = reg_key
                    st.session_state["_cti_final_jp"] = reg.get("jp_input") or ""
                    st.session_state["_cti_final_en"] = reg.get("cand_en") or ""
                    st.session_state.pop("final_isbn", None)
                    st.session_state.pop("_cti_final_isbn", None)
                    st.session_state.pop("confirm_date", None)
                    st.session_state.pop("confirm_rating", None)
                    st.session_state.pop("confirm_wl", None)
                if "final_jp_next" in st.session_state:
                    reg["jp_input"] = st.session_state.pop("final_jp_next")
                    st.session_state["_cti_final_jp"] = reg["jp_input"]
                st.divider()
                st.subheader("📝 登録内容の確認・修正")
                c1, c2 = st.columns([1, 2])
                with c1:
                    if reg.get("cover_url"):
                        st.image(reg["cover_url"])
                    st.caption(f"{reg['cand_en']} ({reg['media_type']}) {reg['tmdb_release']} 🆔 {reg.get('tmdb_id','')}")
                with c2:
                    jp_cols = st.columns([4, 1])
                    with jp_cols[0]:
                        final_jp = clearable_text_input(
                            "日本語タイトル（修正可）",
                            "final_jp",
                            value=reg.get("jp_input") or "",
                            refresh_on_value_change=True,
                        )
                    with jp_cols[1]:
                        can_jp_search = media_label in ("映画", "ドラマ", "アニメ", "音楽アルバム", "ゲーム")
                        jp_search_clicked = st.button("日本語タイトルを検索", key="search_jp_title", disabled=not can_jp_search)
                    final_en = clearable_text_input(
                        "英語タイトル（修正可）",
                        "final_en",
                        value=reg["cand_en"],
                        refresh_on_value_change=True,
                    )
                    jp_feedback = st.session_state.pop("jp_search_feedback", "")
                    if jp_feedback:
                        st.info(jp_feedback)
                    if media_label in ("書籍", "漫画"):
                        final_isbn = st.text_input("ISBN", value=reg.get("isbn", ""), key="final_isbn")
                    else:
                        final_isbn = None
    
                    if jp_search_clicked:
                        with st.spinner("日本語タイトル取得中..."):
                            new_jp = ""
                            if media_label in ("映画", "ドラマ"):
                                tmdb_id = reg.get("tmdb_id") or 0
                                if tmdb_id:
                                    tmdb_mt = "movie" if media_label == "映画" else "tv"
                                    new_jp = fetch_tmdb_ja_title(int(tmdb_id), tmdb_mt)
                            elif media_label == "アニメ":
                                anilist_id = reg.get("anilist_id") or 0
                                if anilist_id:
                                    anime = fetch_anime_by_id(int(anilist_id))
                                    if anime:
                                        new_jp = anime.get("title", "")
                            elif media_label == "音楽アルバム":
                                title = reg.get("cand_en") or reg.get("jp_input") or ""
                                authors = reg.get("book_authors") or []
                                artist = authors[0] if authors else None
                                new_jp = search_itunes_jp_album_title(title, artist)
                                if not new_jp:
                                    new_jp = search_wikipedia_jp_title(f"{title} album")
                            elif media_label == "ゲーム":
                                title = reg.get("cand_en") or reg.get("jp_input") or ""
                                new_jp = search_game_jp_title_precise(title)
                                if not new_jp:
                                    q_hint = reg.get("jp_input") or st.session_state.get("last_game_query_jp") or ""
                                    new_jp = search_game_jp_title_from_query(q_hint, title)

                            if new_jp:
                                current_jp = (reg.get("jp_input") or "").strip()
                                if current_jp and new_jp.strip() == current_jp:
                                    st.session_state.jp_search_feedback = "日本語タイトル候補は現在値と同じでした"
                                    st.rerun()
                                else:
                                    st.session_state.final_jp_next = new_jp
                                    st.session_state.jp_search_feedback = f"日本語タイトル候補を反映しました: {new_jp}"
                                    st.rerun()
                            else:
                                st.warning("日本語タイトルが見つかりませんでした")
                    if media_label == "ゲーム":
                        cover_cands = reg.get("cover_candidates") or [reg.get("cover_url", "")]
                        cover_cands = [u for u in cover_cands if u]
                        if cover_cands:
                            if len(cover_cands) == 1:
                                st.caption("カバー候補: 1件")
                            else:
                                selected_cover = st.selectbox(
                                    "カバー画像を選択",
                                    options=list(range(len(cover_cands))),
                                    format_func=lambda i: f"{i+1}. {format_cover_url(cover_cands[i], max_len=80)}",
                                    key=f"game_cover_pick_confirm_{reg.get('igdb_id') or reg.get('cand_en','')}",
                                )
                                reg["cover_url"] = cover_cands[selected_cover]
                            st.image(reg["cover_url"])
    
                    include_tracks = False
                    tracks_text    = ""
                    if media_label == "音楽アルバム" and reg.get("tmdb_id") == 0:
                        collection_id = reg.get("itunes_id", 0)
                        if collection_id:
                            if "album_tracks_cache" not in st.session_state or st.session_state.get("album_tracks_id") != collection_id:
                                with st.spinner("トラックリスト取得中..."):
                                    tracks = fetch_itunes_tracks(collection_id)
                                    st.session_state.album_tracks_cache = tracks
                                    st.session_state.album_tracks_id    = collection_id
                            else:
                                tracks = st.session_state.album_tracks_cache
                            if tracks:
                                tracks_text    = "\n".join(f"{t['no']}. {t['name']}" for t in tracks)
                                include_tracks = st.checkbox("トラックリストをメモに追加", value=True, key="include_tracks")
                                if include_tracks:
                                    st.caption(tracks_text)
    
                    if not st.session_state.pages_loaded:
                        with st.spinner("重複チェック中..."):
                            all_pages = load_notion_data()
                            st.session_state.pages        = filter_target_pages(all_pages)
                            st.session_state.pages_loaded = True
                    dupes = check_duplicate(reg.get("tmdb_id", 0), st.session_state.pages)
                    if dupes:
                        dupe_titles = "、".join([get_title(d["properties"])[0] for d in dupes])
                        st.warning(f"⚠️ 登録済のデータがあります：{dupe_titles}\nそれでも登録しますか？")
    
                    date_label   = {"ゲーム": "クリア日", "音楽アルバム": "聴いた日", "書籍": "読了日", "漫画": "読了日", "アニメ": "視聴日"}.get(media_label, "体験日")
                    col_wl, col_date, col_rating = st.columns([1, 2, 2])
                    wlflg        = col_wl.checkbox("WLflg", value=False, key="confirm_wl")
                    watched_date = col_date.date_input(date_label, value=None, key="confirm_date")
                    rating_sel   = col_rating.selectbox("評価", RATING_OPTIONS, key="confirm_rating")
                    st.divider()
                    confirm_location = location_search_ui("confirm", media_label)
    
                    col_ok, col_cancel = st.columns([1, 1])
                    with col_ok:
                        if st.button("✅ 登録する", key="confirm_ok", disabled=st.session_state.registering):
                            st.session_state.registering = True
                            st.rerun()
                    with col_cancel:
                        if st.button("❌ キャンセル", key="confirm_cancel", disabled=st.session_state.registering):
                            st.session_state.confirm_reg = None
                            st.rerun()
    
                    if st.session_state.registering:
                        try:
                            with st.spinner("登録中..."):
                                if reg["media_type"] in ("book", "manga"):
                                    details = {"genres": reg.get("book_genres", []), "cast": "", "director": clean_author_list(reg.get("book_authors", [])), "score": None}
                                elif reg["media_type"] in ("album", "game"):
                                    details = {"genres": reg.get("book_genres", []), "cast": reg.get("game_publisher", ""), "director": clean_author_list(reg.get("book_authors", [])), "score": None}
                                elif reg["media_type"] == "anime":
                                    details = {"genres": reg.get("book_genres", []), "cast": "", "director": clean_author_list(reg.get("book_authors", [])), "score": reg.get("anime_score")}
                                else:
                                    details = fetch_tmdb_details(reg["tmdb_id"], reg["media_type"])
                                watched_str  = watched_date.isoformat() if watched_date else None
                                page_tmdb_id = 0 if reg["media_type"] not in ("movie", "tv") else reg["tmdb_id"]
                                memo_text = None
                                if reg["media_type"] == "album" and st.session_state.get("include_tracks", False):
                                    tracks = st.session_state.get("album_tracks_cache", [])
                                    if tracks:
                                        memo_text = "\n".join(f"{t['no']}. {t['name']}" for t in tracks)
                                ok = create_notion_page(
                                    jp_title=final_jp, en_title=final_en,
                                    media_type_label=media_label,
                                    tmdb_id=page_tmdb_id, media_type=reg["media_type"],
                                    cover_url=reg["cover_url"], tmdb_release=reg["tmdb_release"],
                                    details=details, wlflg=wlflg,
                                    watched_date=watched_str,
                                    rating=rating_sel if rating_sel else None,
                                    isbn=final_isbn or None,
                                    memo=memo_text,
                                    location=confirm_location,
                                    igdb_id=reg.get("igdb_id"),
                                    itunes_id=reg.get("itunes_id"),
                                    anilist_id=reg.get("anilist_id"),
                                )
                                if ok:
                                    if reg["media_type"] == "game":
                                        _learn_game_jp_title(final_en, final_jp, igdb_id=reg.get("igdb_id"), confidence="手動")
                                    if reg["media_type"] in ("movie", "tv"):
                                        save_to_drive(reg["cover_url"], final_jp or final_en, reg["tmdb_id"])
                                    st.session_state.confirm_reg        = None
                                    st.session_state.new_search_results = []
                                    st.session_state.new_search_done    = False
                                    for key in ["confirm_reg", "new_search_results", "new_search_done", "bulk_checked"]:
                                        st.session_state.pop(key, None)
                                    reset_new_register_state()
                                    sync_notion_after_update(
                                        page_id=st.session_state.get("last_created_page_id"),
                                        updated_page=st.session_state.get("last_created_page"),
                                    )
                                    show_post_register_ui()
                                else:
                                    st.error("❌ 登録失敗")
                        finally:
                            st.session_state.registering = False
            else:
                st.caption("候補を選択すると、ここに確認画面が表示されます。")
    
    
        if active_tab == "候補":
            # ── 検索結果一覧（カード＋チェック）──
    
            if st.session_state.get("new_search_done", False):
                results_list = st.session_state.new_search_results
                excluded_list = st.session_state.get("new_search_excluded", [])
                if not results_list:
                    if excluded_list:
                        st.warning(f"検索結果はすべて登録済みのため除外されました（{len(excluded_list)} 件）")
                        with st.expander("除外されたタイトルを表示"):
                            for t in excluded_list:
                                st.caption(f"・{t}")
                    else:
                        st.warning("候補が見つかりませんでした")
                        if media_label == "ゲーム":
                            series_sugs = st.session_state.get("game_series_suggestions", [])
                            if series_sugs:
                                st.info("シリーズ候補が見つかりました。先にシリーズを確定して作品を取得できます。")
                                labels = [f"{s.get('ja','')} / {s.get('en','')}" for s in series_sugs]
                                pick = st.selectbox("シリーズ候補", options=list(range(len(labels))), format_func=lambda i: labels[i], key="game_series_pick_fallback")
                                if st.button("シリーズで作品候補を取得", key="game_series_fetch_fallback"):
                                    series_en = series_sugs[pick].get("en", "")
                                    series_results = _search_games_for_ui(series_en)
                                    reg_ids = get_registered_ids(st.session_state.pages)
                                    filtered, excluded = filter_registered(series_results, media_label, reg_ids)
                                    st.session_state.new_search_raw_count = len(series_results or [])
                                    st.session_state.new_search_results = filtered[:200]
                                    st.session_state.new_search_excluded = excluded
                                    st.session_state.new_search_done = True
                                    st.rerun()
                    st.caption("検索すると、ここに候補が表示されます。")
                    results_list = []
    
                st.caption(f"{len(results_list)} 件の候補　チェックして登録リストに追加できます")
                if media_label == "ゲーム":
                    st.caption(
                        f"検索結果: 取得 {st.session_state.get('new_search_raw_count', 0)} 件 / "
                        f"登録済み除外 {len(excluded_list)} 件 / 表示 {len(results_list)} 件"
                    )
                if excluded_list:
                    st.caption(f"⚠️ {len(excluded_list)} 件は登録済みのため除外")
                    with st.expander("除外されたタイトルを表示"):
                        for t in excluded_list:
                            st.caption(f"・{t}")
    
                if results_list and media_label == "ゲーム":
                    # ゲームは「タイトル候補→作品」の2段階で候補を絞る（画像は作品確定後に取得）
                    series_order = []
                    seen_series = set()
                    for g in results_list:
                        stitle = g.get("series_title") or _derive_game_series_title(g.get("title", ""))
                        if stitle not in seen_series:
                            series_order.append(stitle)
                            seen_series.add(stitle)
                    # シリーズ名JPはキャッシュで段階的に補完（速度と可読性を両立）
                    if "game_series_jp_cache" not in st.session_state:
                        st.session_state.game_series_jp_cache = {}
                    series_jp_cache = st.session_state.game_series_jp_cache
                    unresolved_series = [s for s in series_order if s and not series_jp_cache.get(s)]
                    if unresolved_series:
                        fill = resolve_game_jp_titles_bulk(tuple(unresolved_series[:8]))
                        if fill:
                            series_jp_cache.update(fill)
                            st.session_state.game_series_jp_cache = series_jp_cache
                    series_labels = [f"{series_jp_cache.get(s)} / {s}" if series_jp_cache.get(s) else s for s in series_order]
                    selected_series = st.selectbox(
                        "① タイトル候補",
                        options=list(range(len(series_order))),
                        format_func=lambda i: series_labels[i],
                        key="game_series_pick",
                    )
                    selected_series_name = series_order[selected_series]
                    work_list = [g for g in results_list if (g.get("series_title") or _derive_game_series_title(g.get("title", ""))) == selected_series_name]
                    # 作品特定クエリでは、シリーズ跨ぎでも「クエリ一致」候補を残す
                    q_raw = (st.session_state.get("last_game_query_jp") or st.session_state.get("inp_jp_main") or "").strip()
                    if _is_specific_game_query(q_raw):
                        q_keys = _game_query_match_keys(q_raw)
                        if q_keys:
                            extra = []
                            seen_id = {x.get("id") for x in work_list}
                            for g in results_list:
                                if g.get("id") in seen_id:
                                    continue
                                tkey = _norm_game_match_key(g.get("title", ""))
                                akeys = {_norm_game_match_key(a) for a in (g.get("alt_titles") or []) if (a or "").strip()}
                                # 取り込みは厳格一致（部分一致はノイズ混入の原因）
                                if (tkey and tkey in q_keys) or bool(akeys & q_keys):
                                    extra.append(g)
                                    seen_id.add(g.get("id"))
                            if extra:
                                work_list.extend(extra)
                    # 表示順をクエリ適合度で調整（誤混入を下位に）
                    if work_list:
                        q_keys = _game_query_match_keys(q_raw)
                        def _date_rank(v: str) -> int:
                            s = (v or "").strip()
                            if not s:
                                return 99999999
                            try:
                                return int(s.replace("-", ""))
                            except Exception:
                                return 99999999
                        def _work_rank(x: dict):
                            tkey = _norm_game_match_key(x.get("title", ""))
                            akeys = {_norm_game_match_key(a) for a in (x.get("alt_titles") or []) if (a or "").strip()}
                            exact = 0 if (tkey in q_keys or bool(akeys & q_keys)) else 1
                            noisy = 1 if _is_noisy_game_title(x.get("title", "")) else 0
                            return (exact, noisy, _date_rank(x.get("release", "")))
                        work_list = sorted(work_list, key=_work_rank)
                    official_only = st.checkbox("公式寄り候補のみ表示", value=True, key="game_official_only")
                    if official_only:
                        def _is_official_like(x: dict) -> bool:
                            low = (x.get("title") or "").lower()
                            if any(k in low for k in ["randomizer", "redux", "mod", "multiplayer", "online", "hack"]):
                                return False
                            if x.get("variant_label") in ("追加コンテンツ", "特装/同梱"):
                                return False
                            has_rel = bool(x.get("release"))
                            cat = int(x.get("category") or -1)
                            is_main_cat = cat in (0, 8, 9)
                            has_company = bool((x.get("developer") or "").strip() or (x.get("publisher") or "").strip())
                            return is_main_cat and has_rel and has_company
                        filtered = [g for g in work_list if _is_official_like(g)]
                        if filtered:
                            work_list = filtered
                    st.caption(f"② 作品候補: {len(work_list)}件（タイトル一覧）")
                    if work_list:
                        game_work_filter = st.text_input(
                            "作品名で絞り込み（任意）",
                            key="game_work_filter",
                            placeholder="例: Persona",
                        )
                        if game_work_filter.strip():
                            gf = game_work_filter.strip().lower()
                            work_list = [w for w in work_list if gf in (w.get("title", "").lower())]
                        max_show = 80
                        if len(work_list) > max_show:
                            st.caption(f"表示上限: {max_show}件（{len(work_list)}件中）")
                            work_list = work_list[:max_show]
                        if work_list:
                            user_jp_query = (st.session_state.get("inp_jp_main") or st.session_state.get("last_game_query_jp") or "").strip()
                            if "game_jp_resolve_cache" not in st.session_state:
                                st.session_state.game_jp_resolve_cache = {}
                            jp_resolve_cache = st.session_state.game_jp_resolve_cache
                            # 一覧では誤補完を避けるため、IGDB/学習済みのみ表示（未解決は明示）
                            # 補完進捗の内部情報は画面に出さない
                            jp_infos = []
                            for w in work_list:
                                en_t = w.get("title", "")
                                jp_t = (
                                    w.get("jp_title")
                                    or _lookup_game_jp_learned(en_t, w.get("id"))
                                    or jp_resolve_cache.get(en_t, "")
                                )
                                src = (w.get("jp_source") or "").strip()
                                conf = (w.get("jp_confidence") or "").strip()
                                if not src and jp_t:
                                    src = "学習済み"
                                    conf = conf or "中"
                                if jp_t and not conf:
                                    conf = "中"
                                jp_infos.append(
                                    {
                                        "jp": jp_t if jp_t else "（JP未解決）",
                                        "src": src if jp_t else "",
                                        "conf": conf if jp_t else "",
                                    }
                                )
                            # 候補表示時の自動学習（高信頼かつ公式性チェック通過のみ）
                            if "game_jp_autosaved" not in st.session_state:
                                st.session_state.game_jp_autosaved = set()
                            autosaved = st.session_state.game_jp_autosaved
                            for i, w in enumerate(work_list):
                                if not _is_official_game_candidate_for_learning(w):
                                    continue
                                jp_t = (jp_infos[i].get("jp") or "").strip()
                                if not jp_t or jp_t == "（JP未解決）":
                                    continue
                                src = (jp_infos[i].get("src") or "").strip()
                                conf = (jp_infos[i].get("conf") or "").strip()
                                # IGDB由来の高信頼のみ自動学習
                                if not src.startswith("IGDB"):
                                    continue
                                if conf not in ("高", "IGDB-localization", "IGDB-alt(JP注記)"):
                                    continue
                                en_t = (w.get("title") or "").strip()
                                igdb_id = w.get("id")
                                if not en_t or not igdb_id:
                                    continue
                                key = f"{igdb_id}:{en_t}:{jp_t}"
                                if key in autosaved:
                                    continue
                                if _learn_game_jp_title(en_t, jp_t, igdb_id=igdb_id, confidence=src, persist_notion=True):
                                    autosaved.add(key)
                            st.session_state.game_jp_autosaved = autosaved
                            pick_idx = st.radio(
                                "作品を選択",
                                options=list(range(len(work_list))),
                                format_func=lambda i: (
                                    f"{jp_infos[i]['jp']}  /  {work_list[i].get('release','不明')}  /  {('・'.join((work_list[i].get('platforms') or [])[:3]) or 'ハード不明')}  /  {work_list[i].get('variant_label') or _game_variant_label(work_list[i].get('title',''))}"
                                    if jp_infos[i]['jp'] != "（JP未解決）"
                                    else f"{jp_infos[i]['jp']}  /  {work_list[i].get('title','')}  /  {work_list[i].get('release','不明')}  /  {('・'.join((work_list[i].get('platforms') or [])[:3]) or 'ハード不明')}  /  {work_list[i].get('variant_label') or _game_variant_label(work_list[i].get('title',''))}"
                                ),
                                key="game_work_pick",
                            )
                            picked = dict(work_list[pick_idx])
                            picked["jp_title"] = jp_infos[pick_idx]["jp"] if jp_infos[pick_idx]["jp"] != "（JP未解決）" else picked.get("jp_title", "")
                            picked["jp_source"] = jp_infos[pick_idx].get("src", "")
                            picked["jp_confidence"] = jp_infos[pick_idx].get("conf", "")
                            # 同名候補のカバーも候補に含める（地域版差異の救済）
                            same_title_covers = []
                            picked_key = _norm_game_match_key(picked.get("title", ""))
                            for w in work_list:
                                if _norm_game_match_key(w.get("title", "")) == picked_key:
                                    cu = (w.get("cover_url") or "").strip()
                                    if cu:
                                        same_title_covers.append(cu)
                            if same_title_covers:
                                picked["related_cover_urls"] = _dedupe_keep_order(same_title_covers)
                            if (not picked.get("jp_title")) and st.button("🇯🇵 選択作品のJP候補を取得", key="game_resolve_selected_jp"):
                                resolved, reason = diagnose_game_jp_resolution(picked.get("title", ""), user_jp_query)
                                if resolved:
                                    jp_resolve_cache[picked.get("title", "")] = resolved
                                    st.session_state.game_jp_resolve_cache = jp_resolve_cache
                                    st.success(f"JP候補を取得: {resolved}")
                                    st.rerun()
                                else:
                                    st.warning(f"この作品の日本語タイトル候補は見つかりませんでした（{reason or '一致なし'}）")
                            if st.button("🖼 画像候補を取得", key="game_fetch_cover_cands"):
                                q_hint = (st.session_state.get("inp_en_main") or st.session_state.get("inp_jp_main") or "")
                                cands = _build_game_cover_candidates(picked, query_hint=q_hint)
                                picked["cover_candidates"] = cands
                                picked["cover_url"] = cands[0] if cands else picked.get("cover_url", "")
                                st.session_state.game_work_selected = picked
                                st.rerun()
                        else:
                            st.info("絞り込み条件に一致する作品がありません。")
                    selected_work = st.session_state.get("game_work_selected")
                    if selected_work:
                        cover_cands = selected_work.get("cover_candidates") or []
                        if cover_cands:
                            cv_idx = st.selectbox(
                                "③ ジャケットを選択",
                                options=list(range(len(cover_cands))),
                                format_func=lambda i: f"{i+1}. {format_cover_url(cover_cands[i], max_len=90)}",
                                key="game_cover_pick",
                            )
                            selected_work["cover_url"] = cover_cands[cv_idx]
                            st.image(selected_work["cover_url"], width=240)
                        c1, c2 = st.columns(2)
                        if c1.button("✅ 単体登録", key="game_single_from_selected"):
                            user_jp_query = (st.session_state.get("inp_jp_main") or st.session_state.get("last_game_query_jp") or "").strip()
                            picked_jp = (
                                selected_work.get("jp_title")
                                or search_game_jp_title_precise(selected_work.get("title", ""))
                                or search_game_jp_title_from_query(user_jp_query, selected_work.get("title", ""))
                                or (user_jp_query if _contains_japanese(user_jp_query) else "")
                                or selected_work.get("title", "")
                            )
                            st.session_state.confirm_reg = {
                                "tmdb_id": 0, "cover_url": selected_work.get("cover_url", ""),
                                "tmdb_release": selected_work.get("release", ""), "media_type": "game",
                                "cand_en": selected_work.get("title", ""), "jp_input": picked_jp,
                                "book_authors": [selected_work.get("developer", "")], "book_genres": selected_work.get("genres", []),
                                "isbn": "", "game_publisher": selected_work.get("publisher", ""),
                                "igdb_id": selected_work.get("id"),
                                "cover_candidates": selected_work.get("cover_candidates", []),
                            }
                            st.session_state.active_reg_tab_next = "確認"
                            st.rerun()
                        if c2.button("📋 登録リストに追加", key="game_cart_from_selected"):
                            user_jp_query = (st.session_state.get("inp_jp_main") or st.session_state.get("last_game_query_jp") or "").strip()
                            picked_jp = (
                                selected_work.get("jp_title")
                                or search_game_jp_title_precise(selected_work.get("title", ""))
                                or search_game_jp_title_from_query(user_jp_query, selected_work.get("title", ""))
                                or (user_jp_query if _contains_japanese(user_jp_query) else "")
                                or selected_work.get("title", "")
                            )
                            st.session_state.reg_cart.append({
                                "jp_title":   picked_jp,
                                "en_title":   selected_work.get("title", ""),
                                "cover_url":  selected_work.get("cover_url", ""),
                                "cover_candidates": selected_work.get("cover_candidates", []),
                                "release":    selected_work.get("release", ""),
                                "watched": "", "rating": "", "wlflg": False,
                                "media_type": "game", "tmdb_id": 0,
                                "details":    {"genres": selected_work.get("genres", []), "cast": selected_work.get("publisher", ""), "director": clean_author(selected_work.get("developer", "")), "score": None},
                                "isbn": "", "igdb_id": selected_work.get("id"),
                                "location": None, "media_label": media_label,
                            })
                            st.success("✅ 登録リストに追加しました")
                            st.session_state.active_reg_tab_next = "登録リスト"
                            st.rerun()
                elif results_list:
                    for row_start in range(0, len(results_list), 3):
                        cols = st.columns(3)
                        for col_idx, cand in enumerate(results_list[row_start:row_start + 3]):
                            abs_idx = row_start + col_idx
                            with cols[col_idx]:
                                cand_type = cand.get("media_type", "")
                                if media_label in ("書籍", "漫画"):
                                    cover_url     = cand["cover_url"]
                                    tmdb_release  = cand.get("published", "")
                                    media_type    = cand_type
                                    cand_en       = ""
                                    display_title = cand["title"]
                                    authors       = " / ".join(cand.get("authors", []))
                                elif media_label == "音楽アルバム":
                                    cover_url     = cand["cover_url"]
                                    tmdb_release  = cand.get("release", "")
                                    media_type    = "album"
                                    cand_en       = ""
                                    display_title = cand["title"]
                                    authors       = cand.get("artist", "")
                                elif media_label == "ゲーム":
                                    cover_url     = cand["cover_url"]
                                    tmdb_release  = cand.get("release", "")
                                    media_type    = "game"
                                    cand_en       = cand["title"]
                                    display_title = cand.get("jp_title") or cand["title"]
                                    authors       = cand.get("developer", "")
                                elif media_label == "アニメ":
                                    cover_url     = cand["cover_url"]
                                    tmdb_release  = cand.get("release", "")
                                    media_type    = "anime"
                                    cand_en       = cand.get("title_en") or cand.get("title_romaji", "")
                                    display_title = cand["title"]
                                    authors       = cand.get("director", "")
                                else:
                                    cover_url    = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{cand['poster_path']}"
                                    tmdb_release = cand.get("release_date") or cand.get("first_air_date") or ""
                                    media_type   = cand.get("media_type", "movie")
                                    cand_en      = cand.get("title") or cand.get("name", "")
                                    display_title = cand_en
                                    authors      = ""

                                checked = st.checkbox(
                                    f"**{display_title}**",
                                    key=f"chk_{abs_idx}",
                                    value=st.session_state.bulk_checked.get(abs_idx, False),
                                )
                                st.session_state.bulk_checked[abs_idx] = checked

                                if cover_url:
                                    try:
                                        st.image(cover_url)
                                    except Exception:
                                        st.caption("📷 画像取得失敗")
                                else:
                                    st.caption("📷 画像なし")
                                if authors:      st.caption(f"{'著者' if media_label in ('書籍','漫画') else 'アーティスト' if media_label == '音楽アルバム' else '開発'}: {authors}")
                                if tmdb_release: st.caption(f"{'出版' if media_label in ('書籍','漫画') else 'リリース'}: {tmdb_release}")
                                if media_label == "ゲーム" and cand.get("jp_title") and cand.get("jp_title") != cand.get("title"):
                                    st.caption(f"英題: {cand.get('title')}")
                                if media_label not in ("書籍", "漫画", "音楽アルバム", "ゲーム", "アニメ"):
                                    st.caption(f"🆔 {cand['id']}")

                                if st.button("✅ 単体登録", key=f"new_reg_{abs_idx}"):
                                    if media_label in ("書籍", "漫画"):
                                        st.session_state.confirm_reg = {
                                            "tmdb_id": cand["id"], "cover_url": cand["cover_url"],
                                            "tmdb_release": cand.get("published", ""), "media_type": cand_type,
                                            "cand_en": "", "jp_input": cand["title"],
                                            "book_authors": cand.get("authors", []), "book_genres": cand.get("genres", []),
                                            "isbn": cand.get("isbn", ""),
                                        }
                                    elif media_label == "音楽アルバム":
                                        st.session_state.confirm_reg = {
                                            "tmdb_id": 0, "itunes_id": cand["id"], "cover_url": cand["cover_url"],
                                            "tmdb_release": cand.get("release", ""), "media_type": "album",
                                            "cand_en": cand["title"], "jp_input": cand["title"],
                                            "book_authors": [cand.get("artist", "")], "book_genres": [], "isbn": "",
                                        }
                                    elif media_label == "ゲーム":
                                        st.session_state.confirm_reg = {
                                            "tmdb_id": 0, "cover_url": cand["cover_url"],
                                            "tmdb_release": cand.get("release", ""), "media_type": "game",
                                            "cand_en": cand["title"], "jp_input": cand.get("jp_title") or cand["title"],
                                            "book_authors": [cand.get("developer", "")], "book_genres": cand.get("genres", []),
                                            "isbn": "", "game_publisher": cand.get("publisher", ""),
                                            "igdb_id": cand.get("id"),
                                            "cover_candidates": cand.get("cover_candidates", []),
                                        }
                                    elif media_label == "アニメ":
                                        st.session_state.confirm_reg = {
                                            "tmdb_id": 0, "cover_url": cand["cover_url"],
                                            "tmdb_release": cand.get("release", ""), "media_type": "anime",
                                            "cand_en": cand.get("title_en") or cand.get("title_romaji", ""),
                                            "jp_input": cand["title"],
                                            "book_authors": [cand.get("director", "")],
                                            "book_genres": cand.get("genres", []),
                                            "isbn": "", "anime_score": cand.get("score"),
                                            "anilist_id": cand.get("id"),
                                        }
                                    else:
                                        with st.spinner("日本語タイトル取得中..."):
                                            ja_title = fetch_tmdb_ja_title(cand["id"], media_type)
                                        st.session_state.confirm_reg = {
                                            "tmdb_id": cand["id"], "cover_url": cover_url,
                                            "tmdb_release": tmdb_release, "media_type": media_type,
                                            "cand_en": cand_en, "jp_input": ja_title or jp_input,
                                        }
                                    st.session_state.active_reg_tab_next = "確認"
                                    st.rerun()
    
            # ── 登録リストに追加ボタン ──
            checked_indices = [i for i, v in st.session_state.bulk_checked.items() if v]
            if checked_indices:
                st.divider()
                st.caption(f"✅ {len(checked_indices)} 件選択中")
                if st.button(f"📋 {len(checked_indices)} 件を登録リストに追加", type="primary", key="add_to_cart"):
                    for i in checked_indices:
                        cand   = results_list[i]
                        c_type = cand.get("media_type", "")
                        if media_label in ("書籍", "漫画"):
                            cart_item = {
                                "jp_title":   cand["title"], "en_title": "",
                                "cover_url":  cand["cover_url"], "release": cand.get("published", ""),
                                "watched": "", "rating": "", "wlflg": False,
                                "media_type": c_type, "tmdb_id": 0,
                                "details":    {"genres": cand.get("genres", []), "cast": "", "director": clean_author_list(cand.get("authors", [])), "score": None},
                                "isbn":       cand.get("isbn", ""),
                                "location":   None, "media_label": media_label,
                            }
                        elif media_label == "音楽アルバム":
                            cart_item = {
                                "jp_title":   cand["title"], "en_title": cand.get("title", ""),
                                "cover_url":  cand["cover_url"], "release": cand.get("release", ""),
                                "watched": "", "rating": "", "wlflg": False,
                                "media_type": "album", "tmdb_id": 0,
                                "details":    {"genres": [], "cast": "", "director": clean_author(cand.get("artist", "")), "score": None},
                                "isbn":       "", "itunes_id": cand.get("id"),
                                "location":   None, "media_label": media_label,
                            }
                        elif media_label == "ゲーム":
                            cart_item = {
                                "jp_title":   cand.get("jp_title") or cand["title"], "en_title": cand["title"],
                                "cover_url":  cand["cover_url"], "release": cand.get("release", ""),
                                "watched": "", "rating": "", "wlflg": False,
                                "media_type": "game", "tmdb_id": 0,
                                "details":    {"genres": cand.get("genres", []), "cast": cand.get("publisher", ""), "director": clean_author(cand.get("developer", "")), "score": None},
                                "isbn":       "", "igdb_id": cand.get("id"),
                                "cover_candidates": cand.get("cover_candidates", []),
                                "location":   None, "media_label": media_label,
                            }
                        elif media_label == "アニメ":
                            cart_item = {
                                "jp_title":   cand["title"],
                                "en_title":   cand.get("title_en") or cand.get("title_romaji", ""),
                                "cover_url":  cand["cover_url"], "release": cand.get("release", ""),
                                "watched": "", "rating": "", "wlflg": False,
                                "media_type": "anime", "tmdb_id": 0,
                                "details":    {"genres": cand.get("genres", []), "cast": "", "director": cand.get("director", ""), "score": cand.get("score")},
                                "isbn":       "", "anilist_id": cand.get("id"),
                                "location":   None, "media_label": media_label,
                            }
                        else:
                            c_cover   = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{cand['poster_path']}"
                            c_release = cand.get("release_date") or cand.get("first_air_date") or ""
                            c_en      = cand.get("title") or cand.get("name", "")
                            with st.spinner(f"日本語タイトル取得中... ({i+1}/{len(checked_indices)})"):
                                c_jp = fetch_tmdb_ja_title(cand["id"], cand.get("media_type","movie")) or c_en
                            cart_item = {
                                "jp_title":   c_jp, "en_title": c_en,
                                "cover_url":  c_cover, "release": c_release,
                                "watched": "", "rating": "", "wlflg": False,
                                "media_type": cand.get("media_type", "movie"), "tmdb_id": cand["id"],
                                "details":    fetch_tmdb_details(cand["id"], cand.get("media_type","movie")),
                                "isbn":       "",
                                "location":   None, "media_label": media_label,
                            }
                        st.session_state.reg_cart.append(cart_item)
                    st.session_state.bulk_checked = {}
                    st.success(f"✅ {len(checked_indices)} 件を登録リストに追加しました")
                    st.session_state.active_reg_tab_next = "登録リスト"
                    st.rerun()
            # ── 書籍・漫画：次のページ取得 ──
            if st.session_state.get("new_search_done", False) and media_label in ("書籍", "漫画"):
                st.divider()
                next_page = st.session_state.rakuten_page + 1
                if st.button(f"📖 次の30件を取得（{next_page}ページ目）", key="rakuten_next_page"):
                    with st.spinner(f"{next_page}ページ目を取得中..."):
                        q_key = st.session_state.rakuten_query_key
                        parts = q_key.split("|") if q_key else ["", "", ""]
                        _media, _query, _author = parts[0], parts[1], parts[2] if len(parts) > 2 else ""
                        if media_label == "書籍":
                            new_results = search_books(_query or "", author=_author or None, page=next_page, fast=st.session_state.get("fast_book_search", True))
                        else:
                            new_results = search_manga(_query or "", author=_author or None, page=next_page, fast=st.session_state.get("fast_book_search", True))
                        if new_results:
                            reg_ids = get_registered_ids(st.session_state.pages)
                            filtered, excluded = filter_registered(new_results, media_label, reg_ids)
                            # 既存結果に追記（タイトル重複除去）
                            existing_titles = {c.get("title", "") for c in st.session_state.new_search_results}
                            added = [c for c in filtered if c.get("title", "") not in existing_titles]
                            st.session_state.new_search_results  = st.session_state.new_search_results + added
                            st.session_state.new_search_excluded = st.session_state.new_search_excluded + excluded
                            st.session_state.rakuten_page        = next_page
                            if added:
                                st.success(f"✅ {len(added)} 件追加（除外: {len(excluded)} 件）")
                            else:
                                st.info("新しい結果はありませんでした")
                            st.rerun()
                        else:
                            st.info("これ以上の結果はありません")
    
    
        if active_tab == "登録リスト":
            # ── 登録リスト確認・編集・一括登録 ──
    
            reg_cart = st.session_state.get("reg_cart", [])
            if "reg_cart" not in st.session_state:
                st.session_state.reg_cart = reg_cart
            if reg_cart:
                st.divider()
                st.subheader(f"📋 登録リスト（{len(reg_cart)} 件）")
                date_label = {"ゲーム": "クリア日", "音楽アルバム": "聴いた日", "書籍": "読了日", "漫画": "読了日", "演奏曲": "演奏日", "アニメ": "視聴日"}.get(media_label, "体験日")
    
                remove_indices = []
                for idx, item in enumerate(reg_cart):
                    item_media = item.get("media_label", media_label)
                    with st.expander(f"{idx+1}. {item['jp_title']}", expanded=True):
                        cols = st.columns([2, 1, 2, 2, 1, 1])
                        item["jp_title"] = cols[0].text_input("日本語タイトル", value=item["jp_title"], key=f"cart_jp_{idx}")
                        rel_key = f"cart_rel_{idx}"
                        rel_date_val = None
                        rel_norm = _normalize_notion_date_input(str(item.get("release") or ""))
                        if rel_norm:
                            try:
                                rel_date_val = date.fromisoformat(rel_norm)
                            except Exception:
                                rel_date_val = None
                        release_input = cols[1].date_input(
                            "リリース日",
                            value=rel_date_val,
                            min_value=date(1500, 1, 1),
                            max_value=date(2100, 12, 31),
                            key=rel_key,
                        )
                        item["release"] = release_input.isoformat() if release_input else ""
                        date_val = None
                        if item.get("watched"):
                            try: date_val = date.fromisoformat(item["watched"])
                            except: pass
                        item_date_label  = {"ゲーム": "クリア日", "音楽アルバム": "聴いた日", "書籍": "読了日", "漫画": "読了日", "演奏曲": "演奏日", "アニメ": "視聴日"}.get(item_media, "体験日")
                        watched_input    = cols[2].date_input(item_date_label, value=date_val, key=f"cart_watch_{idx}")
                        item["watched"]  = watched_input.isoformat() if watched_input else ""
                        item["rating"]   = cols[3].selectbox("評価", RATING_OPTIONS, index=RATING_OPTIONS.index(item.get("rating","")) if item.get("rating","") in RATING_OPTIONS else 0, key=f"cart_rating_{idx}")
                        item["wlflg"]    = cols[4].checkbox("WL", value=item.get("wlflg", False), key=f"cart_wl_{idx}")
                        if cols[5].button("🗑", key=f"cart_del_{idx}"):
                            remove_indices.append(idx)
                        if item_media == "ゲーム":
                            cc = [u for u in item.get("cover_candidates", []) if u]
                            if cc:
                                picked = st.selectbox(
                                    "ジャケット候補",
                                    options=list(range(len(cc))),
                                    format_func=lambda i: f"{i+1}. {format_cover_url(cc[i], max_len=80)}",
                                    key=f"cart_game_cover_{idx}",
                                )
                                item["cover_url"] = cc[picked]
                                st.image(item["cover_url"], width=220)
                        selected_loc = location_search_ui(
                            f"cart_{idx}",
                            item_media,
                            initial_location=item.get("location"),
                        )
                        if selected_loc:
                            item["location"] = selected_loc
    
                for i in sorted(remove_indices, reverse=True):
                    st.session_state.reg_cart.pop(i)
                if remove_indices:
                    st.rerun()
    
                col_reg, col_clear = st.columns([2, 1])
                with col_reg:
                    if st.button(f"📥 {len(st.session_state.reg_cart)} 件を一括登録", type="primary", key="bulk_register"):
                        total_count = len(st.session_state.get("reg_cart", []))
                        if not st.session_state.pages_loaded:
                            with st.spinner("Notionデータ取得中..."):
                                all_pages = load_notion_data()
                                st.session_state.pages        = filter_target_pages(all_pages)
                                st.session_state.pages_loaded = True
                        success_count = 0
                        prog = st.progress(0)
                        for n, item in enumerate(st.session_state.reg_cart):
                            ok = create_notion_page(
                                jp_title=item["jp_title"], en_title=item.get("en_title",""),
                                media_type_label=item.get("media_label", media_label),
                                tmdb_id=item["tmdb_id"], media_type=item["media_type"],
                                cover_url=item["cover_url"], tmdb_release=item.get("release",""),
                                details=item["details"], wlflg=item.get("wlflg", False),
                                watched_date=item["watched"] or None,
                                rating=item["rating"] or None,
                                isbn=item.get("isbn") or None,
                                igdb_id=item.get("igdb_id"),
                                itunes_id=item.get("itunes_id"),
                                anilist_id=item.get("anilist_id"),
                                location=item.get("location"),
                                relation_prop=item.get("relation_prop"),
                                relation_ids=item.get("relation_ids"),
                            )
                            if ok:
                                if item["media_type"] == "game":
                                    _learn_game_jp_title(item.get("en_title", ""), item.get("jp_title", ""), igdb_id=item.get("igdb_id"), confidence="手動")
                                if item["media_type"] in ("movie", "tv"):
                                    save_to_drive(item["cover_url"], item["jp_title"] or item.get("en_title",""), item["tmdb_id"])
                                success_count += 1
                            prog.progress((n + 1) / len(st.session_state.reg_cart))
                            time.sleep(0.3)
                        fail_count = max(0, total_count - success_count)
                        for key in ["reg_cart", "new_search_results", "new_search_done",
                                    "bulk_checked", "album_tracks_cache", "album_tracks_id"]:
                            st.session_state.pop(key, None)
                        if success_count > 0:
                            st.success(f"✅ {success_count} 件登録完了" + (f"　❌ {fail_count} 件失敗" if fail_count else ""))
                        else:
                            st.error("❌ 登録できませんでした（0 件）")
                        reset_new_register_state()
                        if st.session_state.get("auto_reload_mode") == "partial":
                            for p in st.session_state.get("created_pages", []):
                                upsert_page_in_state(p)
                            st.session_state.created_pages = []
                        else:
                            sync_notion_after_update()
                        if success_count > 0:
                            show_post_register_ui()
                with col_clear:
                    if st.button("🗑 登録リストをクリア", key="cart_clear"):
                        st.session_state.reg_cart = []
                        st.rerun()
    
    
        st.stop()

target_pages = st.session_state.pages

def get_display_pages():
    if mode == "自動同期":
        # 自動同期は補填対象媒体のみ
        base = filter_sync_pages(target_pages)
        if sync_scope == "欠損のみ補填":
            base = [p for p in base if is_incomplete(p)]
    else:
        base = target_pages
    # 媒体フィルタ
    if selected_media_filter:
        base = [p for p in base if get_page_media(p) in selected_media_filter]
    base = apply_diff_filter(base, diff_filter)
    if mode == "データ管理":
        sort_mode = st.session_state.get("manual_sort_order", EXPERIENCE_SORT_NEW)
        sort_mode = LEGACY_SORT_LABEL_MAP.get(sort_mode, sort_mode)
        def _d(page, prop):
            return (((page.get("properties", {}).get(prop) or {}).get("date") or {}).get("start") or "")
        def _experience_date(page):
            return get_experience_date_from_props(page.get("properties", {}))
        def _t(page):
            return (get_title(page.get("properties", {}))[0] or "").lower()
        if sort_mode == EXPERIENCE_SORT_NEW:
            base = sorted(base, key=lambda p: (_experience_date(p), _t(p)), reverse=True)
        elif sort_mode == EXPERIENCE_SORT_OLD:
            base = sorted(base, key=lambda p: (_experience_date(p), _t(p)))
        elif sort_mode == "リリース日（新しい順）":
            base = sorted(base, key=lambda p: (_d(p, "リリース日"), _t(p)), reverse=True)
        elif sort_mode == "リリース日（古い順）":
            base = sorted(base, key=lambda p: (_d(p, "リリース日"), _t(p)))
        elif sort_mode == "タイトル（A-Z）":
            base = sorted(base, key=lambda p: _t(p))
        elif sort_mode == "媒体 → タイトル":
            base = sorted(base, key=lambda p: ((get_page_media(p) or ""), _t(p)))
        elif sort_mode == "更新日時（新しい順）":
            base = sorted(base, key=lambda p: ((p.get("last_edited_time") or ""), _t(p)), reverse=True)
    return base

def resolve_needs(notion_ok_now, drive_ok_now):
    if diff_filter == "Notionのみ更新（Driveあり・Notionカバーなし）": return True, False
    if diff_filter == "Driveのみ更新（Notionカバーあり・Driveなし）":  return False, True
    return not notion_ok_now, not drive_ok_now

# ============================================================
# 出演情報管理モード
# ============================================================
if mode in ("出演者管理", "出演情報管理"):
    st.subheader("👥 出演情報管理")

    loaded_media_keys = sorted(MEDIA_ICON_CUSTOM_EMOJI_IDS.keys())
    st.caption(f"custom_emoji設定: {len(loaded_media_keys)}件")

    st.caption("整備・修復系の個別ボタンは整理済みです。通常の出演登録フローをご利用ください。")

    with st.expander("🧩 APOLLO既存データを作品/楽章マスタに一括再連動", expanded=False):
        c_relink_1, c_relink_2 = st.columns([1, 1])
        relink_only_missing = c_relink_1.checkbox("未連動のみ対象", value=True, key="relink_master_only_missing")
        relink_max_rows = int(
            c_relink_2.number_input(
                "一度に処理する上限",
                min_value=1,
                max_value=2000,
                value=300,
                step=50,
                key="relink_master_max_rows",
            )
        )
        if st.button("🔁 作品/楽章マスタを一括再連動", key="relink_master_run_btn"):
            with st.spinner("APOLLO既存データを再連動中..."):
                relink_stats, relink_failures = relink_existing_score_master_links(
                    max_rows=relink_max_rows,
                    only_missing=relink_only_missing,
                )
            if relink_stats.get("error"):
                st.error(f"❌ 一括再連動に失敗: {relink_stats['error']}")
            else:
                st.success(
                    "✅ 一括再連動完了: "
                    f"走査 {relink_stats.get('scanned', 0)} / "
                    f"対象 {relink_stats.get('targeted', 0)} / "
                    f"更新 {relink_stats.get('updated', 0)} / "
                    f"スキップ {relink_stats.get('skipped', 0)} / "
                    f"失敗 {relink_stats.get('failed', 0)}"
                )
                if relink_failures:
                    st.warning(f"⚠️ 失敗 {len(relink_failures)} 件（先頭100件を表示）")
                    st.dataframe(relink_failures[:100], use_container_width=True, hide_index=True)
                    lines = ["id,title,composer,error"]
                    for f in relink_failures:
                        lines.append(
                            "\"{id}\",\"{title}\",\"{composer}\",\"{error}\"".format(
                                id=str(f.get("id", "")).replace("\"", "\"\""),
                                title=str(f.get("title", "")).replace("\"", "\"\""),
                                composer=str(f.get("composer", "")).replace("\"", "\"\""),
                                error=str(f.get("error", "")).replace("\"", "\"\""),
                            )
                        )
                    st.download_button(
                        "📄 失敗一覧CSVをダウンロード",
                        data="\n".join(lines).encode("utf-8-sig"),
                        file_name=f"apollo_master_relink_failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key="relink_master_failures_dl",
                    )
        st.divider()
        st.caption("既に行単位で連動済みのデータを、出演+曲順単位で1作品へ補正したい場合に使用します。")
        group_max = int(
            st.number_input(
                "補正するグループ上限",
                min_value=1,
                max_value=1000,
                value=200,
                step=20,
                key="repair_group_max_rows",
            )
        )
        normalize_group_title = st.checkbox("APOLLOタイトルを作品名に揃える", value=True, key="repair_group_normalize_title")
        if st.button("🎼 曲順グルーピング補正を実行", key="repair_grouped_relink_btn"):
            with st.spinner("グルーピング補正を実行中..."):
                gp_stats, gp_failures = repair_apollo_grouped_work_links(
                    max_groups=group_max,
                    normalize_apollo_title=normalize_group_title,
                )
            if gp_stats.get("error"):
                st.error(f"❌ グルーピング補正に失敗: {gp_stats['error']}")
            else:
                st.success(
                    "✅ グルーピング補正完了: "
                    f"走査 {gp_stats.get('scanned', 0)} / "
                    f"対象グループ {gp_stats.get('grouped', 0)} / "
                    f"処理グループ {gp_stats.get('processed_groups', 0)} / "
                    f"更新行 {gp_stats.get('updated_rows', 0)} / "
                    f"タイトル正規化 {gp_stats.get('title_normalized', 0)} / "
                    f"失敗行 {gp_stats.get('failed_rows', 0)}"
                )
                if gp_failures:
                    st.warning(f"⚠️ 失敗 {len(gp_failures)} 件（先頭100件を表示）")
                    st.dataframe(gp_failures[:100], use_container_width=True, hide_index=True)
                    lines = ["id,title,canonical_title,composer,error"]
                    for f in gp_failures:
                        lines.append(
                            "\"{id}\",\"{title}\",\"{canon}\",\"{composer}\",\"{error}\"".format(
                                id=str(f.get("id", "")).replace("\"", "\"\""),
                                title=str(f.get("title", "")).replace("\"", "\"\""),
                                canon=str(f.get("canonical_title", "")).replace("\"", "\"\""),
                                composer=str(f.get("composer", "")).replace("\"", "\"\""),
                                error=str(f.get("error", "")).replace("\"", "\"\""),
                            )
                        )
                    st.download_button(
                        "📄 グルーピング補正失敗CSVをダウンロード",
                        data="\n".join(lines).encode("utf-8-sig"),
                        file_name=f"apollo_group_repair_failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key="repair_group_failures_dl",
                    )
        st.divider()
        st.caption("同一グループ重複行を1行に統合（代表行へ楽章集約し、重複行はアーカイブ）")
        consolidate_max = int(
            st.number_input(
                "統合する重複グループ上限",
                min_value=1,
                max_value=1000,
                value=200,
                step=20,
                key="consolidate_group_max_rows",
            )
        )
        if st.button("🧹 APOLLO重複行を統合", key="consolidate_apollo_rows_btn"):
            with st.spinner("APOLLO重複行を統合中..."):
                cs, c_fail = consolidate_apollo_duplicate_rows(max_groups=consolidate_max)
            if cs.get("error"):
                st.error(f"❌ APOLLO重複統合に失敗: {cs['error']}")
            else:
                st.success(
                    "✅ APOLLO重複統合完了: "
                    f"走査 {cs.get('scanned', 0)} / "
                    f"重複グループ {cs.get('duplicate_groups', 0)} / "
                    f"処理グループ {cs.get('processed_groups', 0)} / "
                    f"代表更新 {cs.get('keeper_patched', 0)} / "
                    f"アーカイブ {cs.get('archived_rows', 0)} / "
                    f"失敗 {cs.get('failed', 0)}"
                )
                if c_fail:
                    st.warning(f"⚠️ 失敗 {len(c_fail)} 件（先頭100件を表示）")
                    st.dataframe(c_fail[:100], use_container_width=True, hide_index=True)
                    lines = ["id,title,error"]
                    for f in c_fail:
                        lines.append(
                            "\"{id}\",\"{title}\",\"{error}\"".format(
                                id=str(f.get("id", "")).replace("\"", "\"\""),
                                title=str(f.get("title", "")).replace("\"", "\"\""),
                                error=str(f.get("error", "")).replace("\"", "\"\""),
                            )
                        )
                    st.download_button(
                        "📄 APOLLO重複統合失敗CSVをダウンロード",
                        data="\n".join(lines).encode("utf-8-sig"),
                        file_name=f"apollo_consolidate_failures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key="consolidate_failures_dl",
                    )

    perf_pages = _get_performance_pages(force_refresh=False)
    if not perf_pages:
        st.info("出演データが見つかりません。先にNotionデータ取得を実行してください。")
        st.stop()

    q = clearable_text_input("出演を検索", "cast_mode_perf_query", placeholder="例: 第1回演奏会")
    matches = [p for p in perf_pages if q.strip().lower() in (p.get("title") or "").lower()] if q.strip() else perf_pages[:200]
    options = ["（選択してください）"] + [p["title"] for p in matches]
    sel = st.selectbox("出演を選択", options, key="cast_mode_perf_pick")
    selected_perf = matches[options.index(sel) - 1] if sel != "（選択してください）" else None

    if "cast_mode_master_names_cache" not in st.session_state:
        st.session_state.cast_mode_master_names_cache = get_performer_master_names()
    master_names = st.session_state.get("cast_mode_master_names_cache", [])
    if selected_perf:
        st.caption(f"対象出演: {selected_perf.get('title','')}")
        if "cast_mode_participants" not in st.session_state:
            st.session_state.cast_mode_participants = []
        perf_switch_key = "cast_mode_selected_perf_id"
        if st.session_state.get(perf_switch_key) != selected_perf.get("id"):
            st.session_state[perf_switch_key] = selected_perf.get("id")
            st.session_state.cast_mode_participants = []
            st.session_state.pop("cast_mode_last_submit_report", None)
        if st.button("既定参加者をクリア", key="cast_mode_clear"):
            st.session_state.cast_mode_participants = []
            st.rerun()

        default_self = (DEFAULT_PERFORMER_NAME or "").strip()
        if default_self and not any(
            _normalize_person_name(x.get("name", "")) == _normalize_person_name(default_self)
            for x in st.session_state.cast_mode_participants
        ):
            st.session_state.cast_mode_participants.insert(0, {"name": default_self, "instruments": "", "memo": ""})

        perf_title = selected_perf.get("title", "")
        tab_input, tab_csv, tab_submit = st.tabs(["🧑‍🤝‍🧑 参加者入力", "📄 CSV一括取込", "📥 登録"])

        with tab_input:
            if master_names:
                mq = st.text_input("演奏者マスタ検索", key="cast_mode_master_q", placeholder="例: 喜田")
                mm = [n for n in master_names if mq.lower() in n.lower()] if mq.strip() else master_names[:100]
                pick = st.selectbox("マスタ候補", ["（選択してください）"] + mm, key="cast_mode_master_pick")
                if pick != "（選択してください）" and st.button("＋マスタから追加", key="cast_mode_master_add"):
                    k = _normalize_person_name(pick)
                    if not any(_normalize_person_name(x.get("name", "")) == k for x in st.session_state.cast_mode_participants):
                        st.session_state.cast_mode_participants.append({"name": pick, "instruments": "", "memo": ""})
                        st.rerun()
            c1, c2, c3, c4 = st.columns([2, 2, 2, 1])
            pn = c1.text_input("出演者名", key="cast_mode_name")
            pi = c2.text_input("担当楽器", key="cast_mode_inst")
            pm = c3.text_input("メモ", key="cast_mode_memo")
            if c4.button("👤 出演者を追加", key="cast_mode_add"):
                if pn.strip():
                    nk = _normalize_person_name(pn)
                    dup = next((i for i, x in enumerate(st.session_state.cast_mode_participants) if _normalize_person_name(x.get("name", "")) == nk), None)
                    row = {"name": pn.strip(), "instruments": pi.strip(), "memo": pm.strip()}
                    if dup is None:
                        st.session_state.cast_mode_participants.append(row)
                    else:
                        st.session_state.cast_mode_participants[dup] = row
                    for k in ["cast_mode_name", "cast_mode_inst", "cast_mode_memo"]:
                        st.session_state.pop(k, None)
                    st.rerun()
                else:
                    st.warning("出演者名を入力してください。")

            if st.session_state.cast_mode_participants:
                st.caption("登録予定")
                for i, row in enumerate(st.session_state.cast_mode_participants):
                    rc1, rc2, rc3, rc4 = st.columns([2, 2, 2, 1])
                    row["name"] = rc1.text_input("出演者名", value=row.get("name", ""), key=f"cast_mode_row_name_{i}", label_visibility="collapsed")
                    row["instruments"] = rc2.text_input("担当楽器", value=row.get("instruments", ""), key=f"cast_mode_row_inst_{i}", label_visibility="collapsed")
                    row["memo"] = rc3.text_input("メモ", value=row.get("memo", ""), key=f"cast_mode_row_memo_{i}", label_visibility="collapsed")
                    if rc4.button("✕", key=f"cast_mode_rm_{i}"):
                        st.session_state.cast_mode_participants = [x for j, x in enumerate(st.session_state.cast_mode_participants) if j != i]
                        st.rerun()

        with tab_csv:
            template_lines = ["演奏会名,出演者名,担当楽器,メモ"]
            for _ in range(100):
                template_lines.append(f"\"{perf_title}\",,,")
            template_csv = "\n".join(template_lines).encode("utf-8-sig")
            st.download_button(
                "テンプレートCSVをダウンロード（100行）",
                data=template_csv,
                file_name=f"cast_template_{perf_title[:24] or 'performance'}.csv",
                mime="text/csv",
                key="cast_mode_csv_template_dl",
            )
            csv_file = st.file_uploader(
                "出演者CSVを読み込む",
                type=["csv"],
                key="cast_mode_csv_uploader",
                help="必須列: 演奏会名, 出演者名。演奏会名は現在選択中の出演と一致する行のみ取り込みます。",
            )
            if csv_file is not None and st.button("CSVを登録予定に取り込む", key="cast_mode_csv_import_btn"):
                try:
                    raw = csv_file.getvalue()
                    try:
                        text = raw.decode("utf-8-sig")
                    except Exception:
                        text = raw.decode("cp932", errors="ignore")
                    import csv as _csv
                    rows = list(_csv.DictReader(io.StringIO(text)))
                    added = updated = skipped = 0
                    perf_norm = _normalize_person_name(perf_title)
                    for r in rows:
                        row_perf = (r.get("演奏会名") or "").strip()
                        row_name = (r.get("出演者名") or "").strip()
                        if not row_perf or not row_name:
                            skipped += 1
                            continue
                        if _normalize_person_name(row_perf) != perf_norm:
                            skipped += 1
                            continue
                        row_inst = (r.get("担当楽器") or "").strip()
                        row_memo = (r.get("メモ") or "").strip()
                        key = _normalize_person_name(row_name)
                        idx = next(
                            (i for i, x in enumerate(st.session_state.cast_mode_participants) if _normalize_person_name(x.get("name", "")) == key),
                            None,
                        )
                        row_obj = {"name": row_name, "instruments": row_inst, "memo": row_memo}
                        if idx is None:
                            st.session_state.cast_mode_participants.append(row_obj)
                            added += 1
                        else:
                            st.session_state.cast_mode_participants[idx] = row_obj
                            updated += 1
                    st.success(f"✅ CSV取込: 追加 {added} 件 / 更新 {updated} 件 / スキップ {skipped} 件")
                    st.rerun()
                except Exception as e:
                    st.error(f"CSV取込に失敗しました: {e}")

        with tab_submit:
            st.caption(f"登録予定人数: {len(st.session_state.cast_mode_participants)} 件")
            if st.button("📥 この出演に参加者を登録", type="primary", key="cast_mode_submit"):
                submitted_rows = []
                seen_submit = set()
                for row in st.session_state.cast_mode_participants:
                    nm = (row.get("name") or "").strip()
                    if not nm:
                        continue
                    nk = _normalize_person_name(nm)
                    if not nk or nk in seen_submit:
                        continue
                    seen_submit.add(nk)
                    submitted_rows.append(
                        {
                            "name": nm,
                            "instruments": (row.get("instruments") or "").strip(),
                            "memo": (row.get("memo") or "").strip(),
                            "norm": nk,
                        }
                    )
                self_name = (DEFAULT_PERFORMER_NAME or "").strip()
                if self_name:
                    self_row = next(
                        (x for x in st.session_state.cast_mode_participants if _normalize_person_name(x.get("name", "")) == _normalize_person_name(self_name)),
                        None,
                    )
                    if self_row is not None and not (self_row.get("instruments", "") or "").strip():
                        st.warning("自分の担当楽器が未入力です。登録予定欄で入力してから保存してください。")
                        st.stop()
                with st.spinner("登録中..."):
                    c, f, msg, cast_row_map = create_performance_participant_rows(
                        performance_page_id=selected_perf["id"],
                        performance_title=selected_perf.get("title", ""),
                        participants=st.session_state.cast_mode_participants,
                    )
                success_norm = set((cast_row_map or {}).keys())
                result_rows = []
                for r in submitted_rows:
                    result_rows.append(
                        {
                            "name": r["name"],
                            "instruments": r["instruments"],
                            "memo": r["memo"],
                            "status": "success" if r["norm"] in success_norm else "failed",
                        }
                    )
                st.session_state["cast_mode_last_submit_report"] = {
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "created": c,
                    "failed": f,
                    "message": msg or "",
                    "rows": result_rows,
                }
                if c > 0 and f == 0:
                    st.success(f"✅ 演奏会参加者DBに {c} 件登録しました")
                elif c > 0 and f > 0:
                    st.warning(f"⚠️ 成功 {c} 件 / 失敗 {f} 件")
                elif f > 0:
                    st.error(f"❌ 失敗 {f} 件")
                elif msg:
                    st.info(msg)
            report = st.session_state.get("cast_mode_last_submit_report") or {}
            if report.get("rows"):
                st.divider()
                st.caption(
                    f"直近登録結果: {report.get('time', '')} / "
                    f"成功 {report.get('created', 0)} 件 / 失敗 {report.get('failed', 0)} 件"
                )
                if report.get("message"):
                    st.caption(f"補足: {report.get('message')}")
                st.dataframe(
                    [
                        {
                            "出演者名": x.get("name", ""),
                            "担当楽器": x.get("instruments", ""),
                            "メモ": x.get("memo", ""),
                            "結果": x.get("status", ""),
                        }
                        for x in (report.get("rows") or [])
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
                csv_lines = ["name,instruments,memo,status"]
                for x in (report.get("rows") or []):
                    csv_lines.append(
                        "\"{name}\",\"{inst}\",\"{memo}\",\"{status}\"".format(
                            name=str(x.get("name", "")).replace("\"", "\"\""),
                            inst=str(x.get("instruments", "")).replace("\"", "\"\""),
                            memo=str(x.get("memo", "")).replace("\"", "\"\""),
                            status=str(x.get("status", "")).replace("\"", "\"\""),
                        )
                    )
                st.download_button(
                    "📄 登録結果CSVをダウンロード",
                    data="\n".join(csv_lines).encode("utf-8-sig"),
                    file_name=f"cast_submit_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="cast_mode_result_csv_dl",
                )
    st.stop()

# ============================================================
# カバー全削除
# ============================================================
if delete_btn:
    with st.status("🗑️ 全削除中...", expanded=True) as status:
        pbar = st.progress(0)
        for i, page in enumerate(target_pages):
            api_request("patch", f"https://api.notion.com/v1/pages/{page['id']}",
                        headers=NOTION_HEADERS, json={"cover": None})
            pbar.progress((i + 1) / len(target_pages))
        status.update(label="削除完了！", state="complete")
    with st.spinner("データ再取得中..."):
        all_pages = load_notion_data()
        st.session_state.pages          = filter_target_pages(all_pages)
        st.session_state.search_results = {}
    st.rerun()

# ============================================================
# 自動同期 / リフレッシュ
# ============================================================
if mode == "自動同期" and st.session_state.is_running:
    is_refresh   = (st.session_state.sync_mode == "refresh")
    if is_refresh:
        base_targets = st.session_state.all_pages if st.session_state.all_pages else target_pages
        media_filter = st.session_state.get("sidebar_media_filter", [])
        if media_filter:
            sync_targets = [
                p for p in base_targets
                if any(m["name"] in media_filter for m in p["properties"].get("媒体", {}).get("multi_select", []))
            ]
        else:
            sync_targets = base_targets
        if not st.session_state.refresh_targets_ids:
            st.session_state.refresh_targets_ids = [p["id"] for p in sync_targets]
        sync_target_ids = st.session_state.refresh_targets_ids
        page_by_id = {p["id"]: p for p in sync_targets}
        total_count = len(sync_target_ids)
        start_index = st.session_state.refresh_cursor
        end_index = min(start_index + REFRESH_BATCH_SIZE, total_count)
        current_targets = sync_target_ids[start_index:end_index]
        success_log = st.session_state.refresh_success_log
        maintain_log = st.session_state.refresh_maintain_log
        error_log = st.session_state.refresh_error_log
    else:
        sync_targets = get_display_pages()
        total_count = len(sync_targets)
        start_index = 0
        end_index = total_count
        current_targets = sync_targets
        success_log: list[str] = []
        maintain_log: list[str] = []
        error_log:   list[str] = []
    label_mode   = "🔄 リフレッシュ" if is_refresh else "⚙️ 自動同期"

    processed_start = start_index if is_refresh else 0
    with st.status(f"{label_mode}中... {processed_start} / {total_count} 件", expanded=True) as status:
        pbar, count = st.progress(0), processed_start
        _loop_started_at = time.time()

        def add_error(item_id: str | None, title: str, reason: str, media_label: str | None):
            error_log.append({
                "id": item_id,
                "title": title,
                "reason": reason,
                "media": media_label,
            })

        for i, item in enumerate(current_targets):
            if not st.session_state.is_running:
                break
            if is_refresh:
                page_id = item
                item = page_by_id.get(page_id)
                if item is None:
                    msg = f"⚠️ データ取得失敗: {page_id}"
                    st.write(msg)
                    add_error(page_id, page_id, msg, None)
                    count += 1
                    pbar.progress((count) / total_count if total_count else 1)
                    status.update(label=f"{label_mode}中... {count} / {total_count} 件", state="running")
                    time.sleep(0.05)
                    continue

            props     = item["properties"]
            log_title, jp, en = get_title(props)
            media_label_val = get_page_media(item)
            if is_refresh and media_label_val in ("出演", "演奏曲"):
                st.session_state.refresh_touched_performance = True
            notion_ok_now, drive_ok_now = get_diff_status(item)
            is_movie_drama = any(
                m["name"] in ["映画", "ドラマ"]
                for m in props.get("媒体", {}).get("multi_select", [])
            )

            if not is_refresh and media_label_val not in ("映画", "ドラマ"):
                cover_url = ""
                src = "🆔 ID参照"
                isbn_val = ""
                if media_label_val == "アニメ":
                    anilist_id = props.get("AniList_ID", {}).get("number")
                    if anilist_id:
                        anime = fetch_anime_by_id(int(anilist_id))
                        cover_url = anime.get("cover_url", "") if anime else ""
                elif media_label_val == "ゲーム":
                    igdb_id = props.get("IGDB_ID", {}).get("number")
                    if igdb_id:
                        game = fetch_game_by_id(int(igdb_id))
                        cover_url = game.get("cover_url", "") if game else ""
                elif media_label_val == "音楽アルバム":
                    itunes_id = props.get("iTunes_ID", {}).get("number")
                    if itunes_id:
                        album = fetch_album_by_id(int(itunes_id))
                        cover_url = album.get("cover_url", "") if album else ""
                elif media_label_val in ("書籍", "漫画"):
                    isbn_val = plain_text_join((props.get("ISBN") or {}).get("rich_text", []))
                    author_val = plain_text_join((props.get("クリエイター") or {}).get("rich_text", []))
                    title_val = jp or en or log_title
                    cover_candidates = collect_book_cover_candidates(isbn_val, title_val, author_val or None, "")
                    cover_url = choose_best_cover(cover_candidates) or ""
                else:
                    cover_url = ""

                if not cover_url:
                    msg = f"⏸️ ID未設定 or 取得失敗: {log_title}"
                    st.write(msg)
                    maintain_log.append(log_title)
                    count += 1
                    pbar.progress((count) / total_count if total_count else 1)
                    status.update(label=f"{label_mode}中... {count} / {total_count} 件", state="running")
                    time.sleep(0.1)
                    continue

                need_notion = not notion_ok_now
                need_drive  = (not drive_ok_now) and (not is_drive_skip_mode())
                n_ok = update_notion_cover(item["id"], cover_url, None, None, is_refresh=False) if need_notion else True
                drive_id = isbn_val if media_label_val in ("書籍", "漫画") and isbn_val else (props.get("AniList_ID", {}).get("number") or props.get("IGDB_ID", {}).get("number") or props.get("iTunes_ID", {}).get("number") or item["id"])
                d_ok = bool(save_to_drive(cover_url, log_title, drive_id)) if need_drive else True
                entry = build_update_log(log_title, src, need_notion, n_ok, need_drive, d_ok, True, [], is_refresh=False)
                if (not need_notion or n_ok) and (not need_drive or d_ok):
                    st.write(entry)
                    success_log.append(entry)
                else:
                    fail_parts = []
                    if need_notion and not n_ok: fail_parts.append("Notion更新失敗")
                    if need_drive  and not d_ok: fail_parts.append("Drive保存失敗")
                    msg = f"❌ {log_title}（{' / '.join(fail_parts)}）"
                    st.write(msg)
                    add_error(item["id"], log_title, msg, media_label_val)
                count += 1
                pbar.progress((count) / total_count if total_count else 1)
                status.update(label=f"{label_mode}中... {count} / {total_count} 件", state="running")
                time.sleep(0.1)
                continue

            if is_refresh and not has_any_id(props):
                current_cover = get_current_notion_url(item)
                if current_cover and not current_cover.startswith("https://drive.google.com"):
                    noid_fname = make_noid_filename(log_title, item["id"])
                    if noid_fname not in get_drive_files():
                        file_id = save_cover_to_drive_noid(current_cover, log_title, item["id"])
                        if file_id:
                            msg = f"🧷 Driveバックアップ(no-id): {log_title}"
                            st.write(msg)
                            success_log.append(msg)
                        else:
                            msg = f"⚠️ Driveバックアップ失敗(no-id): {log_title}"
                            st.write(msg)
                            error_log.append(msg)
            if is_refresh and not is_movie_drama:
                # 映画・ドラマ以外: アイコン更新 + 媒体別の追加処理
                media_labels    = [m["name"] for m in props.get("媒体", {}).get("multi_select", [])]
                media_label_val = media_labels[0] if media_labels else None
                patch_body      = {}
                # 親DB(芸術鑑賞記録DB)の演奏曲アイコンは、リフレッシュで変更しない
                if media_label_val == "演奏曲":
                    pass
                elif media_label_val:
                    patch_body["icon"] = get_media_icon_payload(media_label_val)

                # クリエイター名正規化（書籍・漫画・音楽・ゲーム共通）
                if media_label_val in ("書籍", "漫画", "音楽アルバム", "ゲーム"):
                    # 書籍はISBNがある場合のみ、それ以外は無条件
                    isbn_val = plain_text_join(props.get("ISBN", {}).get("rich_text", []))
                    should_normalize = (media_label_val != "書籍") or bool(isbn_val)
                    if should_normalize:
                        raw_creator = plain_text_join(props.get("クリエイター", {}).get("rich_text", []))
                        if raw_creator:
                            cleaned = " / ".join(clean_author(a) for a in raw_creator.split("/") if a.strip())
                            if cleaned != raw_creator:
                                patch_body.setdefault("properties", {})["クリエイター"] = {
                                    "rich_text": [{"type": "text", "text": {"content": cleaned}}]
                                }

                # 音楽アルバム: iTunesからカバー再取得（英語タイトル優先）
                if media_label_val == "音楽アルバム":
                    en_title_str  = plain_text_join(props.get("International Title", {}).get("rich_text", []))
                    jp_title_str  = plain_text_join(props.get("タイトル", {}).get("title", []))
                    title_str     = en_title_str or jp_title_str
                    artist_str    = plain_text_join(props.get("クリエイター", {}).get("rich_text", []))
                    if title_str:
                        albums = search_albums(title_str, artist=artist_str or None)
                        if albums:
                            new_cover = albums[0]["cover_url"]
                            if new_cover:
                                patch_body["cover"] = {"type": "external", "external": {"url": new_cover}}

                # ゲーム: IGDBからカバー再取得
                elif media_label_val == "ゲーム":
                    en_title = plain_text_join(props.get("International Title", {}).get("rich_text", []))
                    jp_title = plain_text_join(props.get("タイトル", {}).get("title", []))
                    query_str = en_title or jp_title
                    if query_str:
                        games = search_games(query_str)
                        if games:
                            new_cover = games[0]["cover_url"]
                            if new_cover:
                                patch_body["cover"] = {"type": "external", "external": {"url": new_cover}}

                if patch_body:
                    api_request("patch", f"https://api.notion.com/v1/pages/{item['id']}",
                                headers=NOTION_HEADERS, json=patch_body)
                msg = f"🎨 アイコン更新: {log_title}"
                st.write(msg)
                success_log.append(msg)
                count += 1
                pbar.progress((count) / total_count if total_count else 1)
                status.update(label=f"{label_mode}中... {count} / {total_count} 件", state="running")
                time.sleep(0.1)
                continue
            need_notion = True if is_refresh else not notion_ok_now
            need_drive  = (True if is_refresh else not drive_ok_now) and (not is_drive_skip_mode())

            date_prop        = props.get("リリース日", {}).get("date")
            existing_release = date_prop.get("start") if date_prop else None
            query            = en if en else jp

            try:
                saved_tmdb_id, saved_media_type = get_tmdb_id_from_notion(props)
                season_number = get_season_number(props)

                if saved_tmdb_id and saved_media_type:
                    top = fetch_tmdb_by_id(saved_tmdb_id, saved_media_type)
                    src = "🆔 ID参照"
                else:
                    results = search_tmdb(query, existing_release[:4] if existing_release else None)
                    top     = results[0] if results else None
                    src     = "🔍 検索"

                if not top:
                    msg = f"候補なし ({src}): {log_title}"
                    st.write(f"⚠️ {msg}")
                    add_error(item["id"], log_title, msg, media_label_val)
                    count += 1
                    pbar.progress((count) / total_count if total_count else 1)
                    time.sleep(0.1)
                    continue

                tmdb_id      = top["id"]
                media_type   = top.get("media_type", saved_media_type or "movie")
                cover_url    = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{top['poster_path']}"
                tmdb_release = top.get("release_date") or top.get("first_air_date")
                st.session_state.tmdb_id_cache[item["id"]] = tmdb_id

                current_url = get_current_notion_url(item)
                url_matched = (current_url == cover_url)

                if not is_refresh and url_matched and not need_drive and not is_incomplete(item):
                    msg = f"⏸️ 維持(OK): {log_title}"
                    st.write(msg)
                    maintain_log.append(log_title)
                    count += 1
                    pbar.progress((count) / total_count if total_count else 1)
                    time.sleep(0.1)
                    continue

                if not is_refresh and url_matched and not need_drive:
                    save_tmdb_id_to_notion(item["id"], tmdb_id, media_type)
                    need_meta = is_incomplete(item)
                    meta_ok, updated = True, []
                    if need_meta:
                        try:
                            details          = fetch_tmdb_details(tmdb_id, media_type, season_number)
                            meta_ok, updated = update_notion_metadata(item["id"], details, force=False, props=props)
                        except Exception:
                            pass
                    entry = build_update_log(log_title, src, False, True, False, True, meta_ok, updated)
                    if updated:
                        entry += f"　↳ {' / '.join(updated)}"
                    st.write(entry)
                    success_log.append(entry)
                    count += 1
                    pbar.progress((count) / total_count if total_count else 1)
                    time.sleep(0.1)
                    continue

                n_ok, d_ok, meta_ok, updated = update_all(
                    item["id"], cover_url, tmdb_release, existing_release,
                    log_title, tmdb_id, media_type, need_notion, need_drive,
                    force_meta=is_refresh, props=props, season_number=season_number,
                    is_refresh=is_refresh,
                )
                entry = build_update_log(log_title, src, need_notion, n_ok, need_drive, d_ok, meta_ok, updated, is_refresh)
                if updated:
                    entry += f"　↳ {' / '.join(updated)}"

                all_ok = (not need_notion or n_ok) and (not need_drive or d_ok) and meta_ok
                if all_ok:
                    st.write(entry)
                    success_log.append(entry)
                else:
                    fail_parts = []
                    if need_notion and not n_ok: fail_parts.append("Notion更新失敗")
                    if need_drive  and not d_ok: fail_parts.append("Drive保存失敗")
                    if not meta_ok:              fail_parts.append("メタデータ失敗")
                    msg = f"❌ {log_title}（{' / '.join(fail_parts)}）"
                    st.write(msg)
                    add_error(item["id"], log_title, msg, media_label_val)

            except Exception as e:
                msg = f"⚠️ エラー: {log_title}（{e}）"
                st.write(msg)
                add_error(item["id"], log_title, msg, media_label_val)

            count += 1
            pbar.progress((count) / total_count if total_count else 1)
            elapsed = max(0.001, time.time() - _loop_started_at)
            processed = max(1, count - processed_start)
            rate = processed / elapsed
            remaining = max(0, total_count - count)
            eta_sec = int(remaining / rate) if rate > 0 else 0
            status.update(
                label=f"{label_mode}中... {count} / {total_count} 件（残り約 {eta_sec} 秒）",
                state="running"
            )
            time.sleep(0.1)

        status.update(
            label=f"{label_mode}完了！✅ {len(success_log)}件　⏸️ {len(maintain_log)}件　❌ {len(error_log)}件",
            state="complete"
        )
        if is_refresh and end_index < total_count:
            status.update(
                label=f"{label_mode}続行中... {end_index} / {total_count} 件",
                state="running"
            )

    # 完了後にexpanderで仕分け表示
    if success_log or error_log:
        media_counts = {}
        for entry in error_log:
            if isinstance(entry, dict):
                m = entry.get("media") or "(不明)"
                media_counts[m] = media_counts.get(m, 0) + 1
        if media_counts:
            summary = " / ".join([f"{k}:{v}" for k, v in sorted(media_counts.items(), key=lambda x: x[0])])
            st.caption(f"失敗媒体サマリ: {summary}")

    if success_log:
        with st.expander(f"✅ 更新成功 （{len(success_log)} 件）", expanded=False):
            for msg in success_log:
                st.write(msg)
            try:
                success_rows = [{"result": "success", "detail": str(m)} for m in success_log]
                success_csv = "result,detail\n" + "\n".join(
                    [f"\"{r['result']}\",\"{str(r['detail']).replace('\"','\"\"')}\"" for r in success_rows]
                )
                st.download_button(
                    "✅ 成功ログCSVをダウンロード",
                    data=success_csv.encode("utf-8-sig"),
                    file_name=f"sync_success_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="sync_success_csv_dl",
                )
            except Exception:
                pass
    if maintain_log:
        with st.expander(f"⏸️ 維持 （{len(maintain_log)} 件）", expanded=False):
            for msg in maintain_log:
                st.write(msg)
    if error_log:
        with st.expander(f"❌ 失敗・要確認 （{len(error_log)} 件）", expanded=True):
            try:
                err_rows = []
                for entry in error_log:
                    if isinstance(entry, str):
                        err_rows.append({"id": "", "title": "", "media": "", "reason": entry})
                    else:
                        err_rows.append({
                            "id": entry.get("id", ""),
                            "title": entry.get("title", ""),
                            "media": entry.get("media", ""),
                            "reason": entry.get("reason", ""),
                        })
                err_csv_lines = ["id,title,media,reason"]
                for r in err_rows:
                    err_csv_lines.append(
                        "\"{id}\",\"{title}\",\"{media}\",\"{reason}\"".format(
                            id=str(r["id"]).replace("\"", "\"\""),
                            title=str(r["title"]).replace("\"", "\"\""),
                            media=str(r["media"]).replace("\"", "\"\""),
                            reason=str(r["reason"]).replace("\"", "\"\""),
                        )
                    )
                st.download_button(
                    "❌ 失敗ログCSVをダウンロード",
                    data="\n".join(err_csv_lines).encode("utf-8-sig"),
                    file_name=f"sync_error_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="sync_error_csv_dl",
                )
            except Exception:
                pass
            for entry in error_log:
                if isinstance(entry, str):
                    st.write(entry)
                    continue
                page_id = entry.get("id")
                title = entry.get("title", "")
                reason = entry.get("reason", "")
                media_label = entry.get("media")
                with st.expander(f"{title}"):
                    st.caption(reason)
                    if not page_id:
                        st.warning("ページIDが取得できないため、個別編集できません。")
                        continue
                    page = next((p for p in st.session_state.pages if p["id"] == page_id), None)
                    if page is None:
                        st.warning("ページが見つかりません。Notionを再取得してください。")
                        continue

                    props = page["properties"]
                    if media_label in ("映画", "ドラマ"):
                        st.caption("🔧 TMDB_ID/種類を調整して再実行")
                        id_col, type_col, run_col = st.columns([2, 2, 1])
                        current_tmdb = (props.get("TMDB_ID") or {}).get("number") or 0
                        tried_both = False
                        new_tmdb_id = id_col.number_input(
                            "TMDB_ID",
                            value=int(current_tmdb) if current_tmdb else 0,
                            min_value=0,
                            step=1,
                            key=f"err_tmdb_id_{page_id}",
                        )
                        new_media_type = type_col.selectbox(
                            "種別",
                            options=["movie", "tv"],
                            index=0 if media_label == "映画" else 1,
                            key=f"err_tmdb_type_{page_id}",
                        )
                        if run_col.button("自動判定", key=f"err_autofix_{page_id}"):
                            if new_tmdb_id > 0:
                                with st.spinner("自動判定中..."):
                                    results = []
                                    for mt in ["movie", "tv"]:
                                        top = fetch_tmdb_by_id(int(new_tmdb_id), mt)
                                        if not top:
                                            results.append((mt, False, "not_found"))
                                            continue
                                        cover_url    = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{top['poster_path']}"
                                        tmdb_release = top.get("release_date") or top.get("first_air_date")
                                        date_prop        = props.get("リリース日", {}).get("date")
                                        existing_release = date_prop.get("start") if date_prop else None
                                        season_number    = get_season_number(props)
                                        save_tmdb_id_to_notion(page_id, int(new_tmdb_id), mt)
                                        n_ok, d_ok, meta_ok, updated = update_all(
                                            page_id, cover_url, tmdb_release, existing_release,
                                            title, int(new_tmdb_id), mt, True, (not is_drive_skip_mode()),
                                            force_meta=True, props=props, season_number=season_number,
                                        )
                                        results.append((mt, n_ok and d_ok and meta_ok, "ok" if n_ok and d_ok and meta_ok else "failed"))
                                        if n_ok and d_ok and meta_ok:
                                            st.success(f"✅ 自動判定: {mt} で成功しました")
                                            sync_notion_after_update(page_id=page_id)
                                            st.rerun()
                                    st.error("❌ 自動判定で成功しませんでした（movie/tv 両方）")
                            else:
                                st.warning("TMDB_IDを入力してください")
                        if run_col.button("再実行", key=f"err_retry_{page_id}"):
                            if new_tmdb_id > 0:
                                with st.spinner("再実行中..."):
                                    top = fetch_tmdb_by_id(int(new_tmdb_id), new_media_type)
                                    if top:
                                        cover_url    = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{top['poster_path']}"
                                        tmdb_release = top.get("release_date") or top.get("first_air_date")
                                        date_prop        = props.get("リリース日", {}).get("date")
                                        existing_release = date_prop.get("start") if date_prop else None
                                        season_number    = get_season_number(props)
                                        save_tmdb_id_to_notion(page_id, int(new_tmdb_id), new_media_type)
                                        n_ok, d_ok, meta_ok, updated = update_all(
                                            page_id, cover_url, tmdb_release, existing_release,
                                            title, int(new_tmdb_id), new_media_type, True, (not is_drive_skip_mode()),
                                            force_meta=True, props=props, season_number=season_number,
                                        )
                                        if n_ok and d_ok and meta_ok:
                                            st.success("✅ 再実行に成功しました")
                                            sync_notion_after_update(page_id=page_id)
                                            st.rerun()
                                        else:
                                            st.error("❌ 再実行に失敗しました")
                                    else:
                                        st.error("TMDBでIDが見つかりませんでした")
                            else:
                                st.warning("TMDB_IDを入力してください")
                    elif media_label == "アニメ":
                        st.caption("🔧 AniList_IDを調整して再実行")
                        id_col, run_col = st.columns([3, 1])
                        current_anilist = (props.get("AniList_ID") or {}).get("number") or 0
                        new_anilist_id = id_col.number_input(
                            "AniList_ID",
                            value=int(current_anilist) if current_anilist else 0,
                            min_value=0,
                            step=1,
                            key=f"err_anilist_id_{page_id}",
                        )
                        if run_col.button("再実行", key=f"err_retry_anilist_{page_id}"):
                            if new_anilist_id > 0:
                                with st.spinner("再実行中..."):
                                    anime = fetch_anime_by_id(int(new_anilist_id))
                                    if anime:
                                        res = api_request(
                                            "patch",
                                            f"https://api.notion.com/v1/pages/{page_id}",
                                            headers=NOTION_HEADERS,
                                            json={"properties": {"AniList_ID": {"number": int(new_anilist_id)}}},
                                        )
                                        n_ok = res is not None and res.status_code == 200
                                        meta_ok, _ = update_notion_metadata(
                                            page_id,
                                            {
                                                "genres": anime.get("genres", []),
                                                "cast": "",
                                                "director": anime.get("director", ""),
                                                "score": anime.get("score"),
                                            },
                                            force=True,
                                            props=props,
                                        )
                                        c_ok = update_notion_cover(page_id, anime.get("cover_url", ""), None, None, is_refresh=False)
                                        if n_ok and meta_ok and c_ok:
                                            st.success("✅ 再実行に成功しました")
                                            sync_notion_after_update(page_id=page_id)
                                            st.rerun()
                                        else:
                                            st.error("❌ 再実行に失敗しました")
                                    else:
                                        st.error("AniListでIDが見つかりませんでした")
                            else:
                                st.warning("AniList_IDを入力してください")
                    else:
                        st.info("この媒体は個別再実行の対象外です。")
    if not error_log and (not is_refresh or end_index >= total_count):
        st.success("すべて正常に処理されました ✅")

    if is_refresh:
        if st.session_state.is_running:
            st.session_state.refresh_cursor = end_index
        if st.session_state.is_running and st.session_state.refresh_cursor < total_count:
            st.rerun()
        if st.session_state.refresh_cursor >= total_count:
            should_run_maintenance = False
            maintenance_elapsed = None
            if st.session_state.get("refresh_maintenance_enabled", True):
                scope = st.session_state.get("refresh_maintenance_scope", "auto")
                should_run_maintenance = (scope == "always") or bool(st.session_state.get("refresh_touched_performance", False))
            if should_run_maintenance:
                with st.spinner("整合チェック修復を実行中..."):
                    _maint_t0 = time.time()
                    report = analyze_performance_relation_integrity(force_refresh=False)
                    if report.get("error"):
                        st.warning(f"整合修復をスキップ: {report.get('error')}")
                    else:
                        m_mode = st.session_state.get("refresh_maintenance_mode", "partial")
                        stats, errs = run_performance_relation_repair(report, mode=m_mode)
                        msg = (
                            "🔧 整合修復: "
                            f"出演者補完 {stats.get('cast_missing_performer_fixed', 0)} 件 / "
                            f"楽曲別担当者補完 {stats.get('assign_missing_cast_fixed', 0)} 件 / "
                            f"重複整理 {stats.get('duplicates_archived', 0)} 件"
                        )
                        if stats.get("failed", 0) > 0:
                            msg += f" / 失敗 {stats.get('failed', 0)} 件"
                        st.session_state.pending_notice = msg
                        if errs:
                            st.session_state.pending_warning = "整合修復で一部失敗があります（出演情報管理の整合チェックで要確認）"
                    icon_stats = refresh_score_db_composer_flag_icons()
                    if icon_stats.get("error"):
                        st.session_state.pending_warning = f"演奏曲DBアイコン更新をスキップ: {icon_stats.get('error')}"
                    else:
                        icon_msg = (
                            "🏳️ 演奏曲DBアイコン更新: "
                            f"国旗 {icon_stats.get('flagged', 0)} 件 / "
                            f"媒体アイコン {icon_stats.get('fallback', 0)} 件 / "
                            f"未解決 {icon_stats.get('unresolved', 0)} 件 / "
                            f"失敗 {icon_stats.get('failed', 0)} 件"
                        )
                        st.session_state.pending_notice = (
                            f"{st.session_state.get('pending_notice', '')}\n{icon_msg}".strip()
                            if st.session_state.get("pending_notice")
                            else icon_msg
                        )
                    maintenance_elapsed = round(time.time() - _maint_t0, 2)
            elif st.session_state.get("refresh_maintenance_enabled", True):
                st.session_state.pending_notice = "⏭ 整合修復を省略: 今回の更新対象に出演/演奏曲が含まれなかったため"
            started_at = st.session_state.get("refresh_started_at")
            if isinstance(started_at, (int, float)):
                st.session_state.refresh_last_seconds = max(0, round(time.time() - started_at, 2))
            st.session_state.refresh_last_count = total_count
            st.session_state.refresh_last_maintenance_seconds = maintenance_elapsed
            st.session_state.refresh_last_maintenance_applied = bool(should_run_maintenance)
            st.session_state.refresh_started_at = None
            st.session_state.is_running = False
            st.session_state.refresh_targets_ids = []
            st.session_state.refresh_touched_performance = False
    else:
        st.session_state.is_running = False

# ============================================================
# 出演アーカイブモード
# ============================================================
if mode == "出演アーカイブ":
    st.subheader("🗃 出演アーカイブ（ArtéMis APOLLO連携）")
    archive_media = ("出演", "演奏会（鑑賞）", "ライブ/ショー", "イベント")
    archive_pages = [p for p in target_pages if get_page_media(p) in archive_media]
    all_pages_for_archive = st.session_state.get("all_pages") or target_pages
    id_to_title = {p.get("id"): get_title((p.get("properties") or {}))[1] for p in all_pages_for_archive}
    id_to_page = {p.get("id"): p for p in all_pages_for_archive}
    # 出演アーカイブの演奏情報（曲順/担当楽器/Playflg）は演奏曲DBにあるため、
    # まず演奏曲DBを参照し、未設定時のみ親DB(媒体=演奏曲)へフォールバックする。
    score_rows = []
    if NOTION_SCORE_DB_ID:
        try:
            score_rows = query_notion_database_all(NOTION_SCORE_DB_ID)
        except Exception:
            score_rows = []
    if not score_rows:
        score_rows = [p for p in target_pages if get_page_media(p) == "演奏曲"]

    movement_id_to_label: dict[str, str] = {}
    if NOTION_MOVEMENT_DB_ID:
        try:
            movement_rows = query_notion_database_all(NOTION_MOVEMENT_DB_ID)
            for m in movement_rows:
                mid = m.get("id")
                if not mid:
                    continue
                mprops = m.get("properties") or {}
                m_title = (get_title(mprops)[1] or "").strip()
                m_name = plain_text_join((mprops.get("楽章名") or {}).get("rich_text", [])).strip()
                m_roman = plain_text_join((mprops.get("ローマ数字表示") or {}).get("rich_text", [])).strip()
                if m_roman and m_name:
                    label = f"{m_roman}. {m_name}"
                elif m_name:
                    label = m_name
                elif m_roman:
                    label = m_roman
                else:
                    label = m_title or mid[:8]
                movement_id_to_label[mid] = label
        except Exception:
            movement_id_to_label = {}

    def _archive_prop_text(meta: dict | None) -> str:
        if not isinstance(meta, dict):
            return ""
        ptype = meta.get("type")
        if ptype == "rich_text":
            return plain_text_join(meta.get("rich_text") or [])
        if ptype == "title":
            return plain_text_join(meta.get("title") or [])
        if ptype == "select":
            return ((meta.get("select") or {}).get("name") or "").strip()
        if ptype == "multi_select":
            vals = [((x or {}).get("name") or "").strip() for x in (meta.get("multi_select") or [])]
            vals = [v for v in vals if v]
            return " / ".join(vals)
        if ptype == "formula":
            f = meta.get("formula") or {}
            if f.get("type") == "string":
                return (f.get("string") or "").strip()
            if f.get("type") == "number" and f.get("number") is not None:
                return str(f.get("number"))
            return ""
        if ptype == "rollup":
            r = meta.get("rollup") or {}
            rtype = r.get("type")
            if rtype == "array":
                chunks = []
                for a in (r.get("array") or []):
                    if not isinstance(a, dict):
                        continue
                    atype = a.get("type")
                    if atype == "rich_text":
                        txt = plain_text_join(a.get("rich_text") or [])
                        if txt:
                            chunks.append(txt)
                    elif atype == "title":
                        txt = plain_text_join(a.get("title") or [])
                        if txt:
                            chunks.append(txt)
                    elif atype == "select":
                        txt = ((a.get("select") or {}).get("name") or "").strip()
                        if txt:
                            chunks.append(txt)
                    elif atype == "multi_select":
                        vals = [((x or {}).get("name") or "").strip() for x in (a.get("multi_select") or [])]
                        vals = [v for v in vals if v]
                        if vals:
                            chunks.extend(vals)
                    else:
                        txt = (a.get("plain_text") or "").strip()
                        if txt:
                            chunks.append(txt)
                return " / ".join(list(dict.fromkeys([c for c in chunks if c])))
            if rtype == "number" and r.get("number") is not None:
                return str(r.get("number"))
            return ""
        return ""

    def _extract_urls_from_text(raw: str) -> list[str]:
        txt = (raw or "").strip()
        if not txt:
            return []
        found = re.findall(r"https?://[^\s\)\]＞>]+", txt)
        uniq = []
        seen = set()
        for u in found:
            if u not in seen:
                uniq.append(u)
                seen.add(u)
        return uniq

    def _to_youtube_embed_url(url: str) -> str:
        u = (url or "").strip()
        if not u:
            return ""
        try:
            from urllib.parse import urlparse, parse_qs
            p = urlparse(u)
            host = (p.netloc or "").lower()
            path = (p.path or "").strip("/")
            if "youtu.be" in host and path:
                vid = path.split("/")[0]
                return f"https://www.youtube.com/embed/{vid}"
            if "youtube.com" in host:
                if path.startswith("watch"):
                    vid = (parse_qs(p.query).get("v") or [""])[0]
                    if vid:
                        return f"https://www.youtube.com/embed/{vid}"
                if path.startswith("shorts/"):
                    vid = path.split("/", 1)[1].split("/")[0]
                    if vid:
                        return f"https://www.youtube.com/embed/{vid}"
                if path.startswith("embed/"):
                    vid = path.split("/", 1)[1].split("/")[0]
                    if vid:
                        return f"https://www.youtube.com/embed/{vid}"
        except Exception:
            return ""
        return ""
    perf_score_info: dict[str, dict[str, dict]] = {}
    for row in score_rows:
        rprops = row.get("properties") or {}
        perf_ids = []
        score_ids = []
        movement_ids = []
        for k, meta in rprops.items():
            if not isinstance(meta, dict) or meta.get("type") != "relation":
                continue
            rel_ids = _clean_relation_ids([x.get("id") for x in (meta.get("relation") or [])])
            kl = str(k).lower()
            if ("出演" in str(k)) or ("演奏会" in str(k)) or ("公演" in str(k)):
                perf_ids.extend(rel_ids)
            elif ("演奏曲" in str(k)) or ("楽曲" in str(k)) or ("score" in kl):
                score_ids.extend(rel_ids)
            elif ("作品楽章" in str(k)) or ("楽章マスタ" in str(k)) or ("movement" in kl):
                movement_ids.extend(rel_ids)
        if not perf_ids or not score_ids:
            continue
        sec = _archive_prop_text(rprops.get("区分"))
        order_num = (rprops.get("曲順") or {}).get("number")
        try:
            order_num = int(order_num) if order_num is not None else None
        except Exception:
            order_num = None
        play_val = None
        for pfk in ("Playflg", "PlayFlg", "playflg", "演奏した"):
            if isinstance(rprops.get(pfk), dict) and (rprops.get(pfk) or {}).get("type") == "checkbox":
                play_val = bool((rprops.get(pfk) or {}).get("checkbox"))
                break
        inst_txt = _archive_prop_text(rprops.get("担当楽器"))
        inst_vals = [v.strip() for v in re.split(r"\s*/\s*|、|,|\n", inst_txt) if v.strip()]
        if play_val is None:
            play_val = bool(inst_vals)
        movement_labels = [movement_id_to_label.get(mid, mid[:8]) for mid in _clean_relation_ids(movement_ids)]
        movement_labels = list(dict.fromkeys([x for x in movement_labels if x]))
        info = {
            "played": bool(play_val),
            "part": " / ".join(inst_vals),
            "section": sec,
            "order": order_num,
            "movements": movement_labels,
        }
        for pid in perf_ids:
            perf_score_info.setdefault(pid, {})
            for sid in score_ids:
                prev = perf_score_info[pid].get(sid) or {}
                prev_parts = [x.strip() for x in str(prev.get("part") or "").split("/") if x.strip()]
                merged_parts = list(dict.fromkeys(prev_parts + inst_vals))
                prev_movs = prev.get("movements") or []
                merged_movs = list(dict.fromkeys([x for x in (prev_movs + movement_labels) if x]))
                prev_order = prev.get("order")
                merged_order = order_num if prev_order is None else prev_order
                try:
                    if order_num is not None and prev_order is not None:
                        merged_order = min(int(prev_order), int(order_num))
                except Exception:
                    merged_order = prev_order if prev_order is not None else order_num
                perf_score_info[pid][sid] = {
                    "played": bool(prev.get("played")) or bool(play_val),
                    "part": " / ".join(merged_parts),
                    "section": str(prev.get("section") or sec or ""),
                    "order": merged_order,
                    "movements": merged_movs,
                }

    q = clearable_text_input(
        "🔎 横断検索（タイトル / クリエイター / キャスト / 演奏曲 / 動画URL）",
        "archive_search_query",
        placeholder="例: 定期演奏会 / Beethoven / URLの一部",
    )
    if q:
        ql = q.strip().lower()
        filtered = []
        for p in archive_pages:
            props = p.get("properties") or {}
            _log_title, jp, en = get_title(props)
            creator = plain_text_join((props.get("クリエイター") or {}).get("rich_text", []))
            cast = plain_text_join((props.get("キャスト・関係者") or {}).get("rich_text", []))
            rel_ids = _clean_relation_ids([r.get("id") for r in ((props.get("演奏曲") or {}).get("relation", []))])
            rel_titles = " ".join([id_to_title.get(rid, "") for rid in rel_ids]).strip()
            video_vals = []
            for k, meta in props.items():
                if "動画" not in str(k):
                    continue
                if isinstance(meta, dict):
                    if meta.get("type") == "url":
                        video_vals.append((meta.get("url") or "").strip())
                    elif meta.get("type") == "rich_text":
                        video_vals.append(plain_text_join((meta.get("rich_text") or [])))
            direct_url = ((props.get("URL") or {}).get("url") or "").strip()
            if direct_url:
                video_vals.append(direct_url)
            hay = " ".join([jp or "", en or "", creator, cast, rel_titles, " ".join(video_vals)]).lower()
            if ql in hay:
                filtered.append(p)
        archive_pages = filtered
        st.caption(f"「{q}」に一致: {len(archive_pages)} 件")

    sort_opt = st.selectbox("並び順", ["体験日（新しい順）", "体験日（古い順）", "タイトル（A-Z）"], key="archive_sort")

    def _archive_date_key(page: dict) -> str:
        return get_experience_date_from_props((page.get("properties") or {})) or "0001-01-01"

    if sort_opt == "体験日（古い順）":
        archive_pages = sorted(archive_pages, key=lambda p: _archive_date_key(p))
    elif sort_opt == "タイトル（A-Z）":
        archive_pages = sorted(archive_pages, key=lambda p: (get_title((p.get("properties") or {}))[1] or "").lower())
    else:
        archive_pages = sorted(archive_pages, key=lambda p: _archive_date_key(p), reverse=True)

    st.caption(f"表示: {len(archive_pages)} 件")
    if not archive_pages:
        st.info("該当データがありません。")
    else:
        for p in archive_pages:
            props = p.get("properties") or {}
            page_id = p.get("id")
            _log_title, jp, en = get_title(props)
            media = get_page_media(p)
            creator = plain_text_join((props.get("クリエイター") or {}).get("rich_text", []))
            cast = plain_text_join((props.get("キャスト・関係者") or {}).get("rich_text", []))
            exp_date = get_experience_date_from_props(props) or "—"
            rel_ids = _clean_relation_ids([r.get("id") for r in ((props.get("演奏曲") or {}).get("relation", []))])
            rel_titles = [id_to_title.get(rid, rid[:8]) for rid in rel_ids]
            rel_info_map = perf_score_info.get(page_id, {})
            played_titles = [id_to_title.get(rid, rid[:8]) for rid in rel_ids if (rel_info_map.get(rid) or {}).get("played")]
            # 動画は親DBの「URL」プロパティのみ参照（運用固定）
            def _normalize_video_url(u: str) -> str:
                s = (u or "").strip()
                if not s:
                    return ""
                if re.match(r"^https?://", s, flags=re.I):
                    return s
                if s.startswith("//"):
                    return "https:" + s
                sl = s.lower()
                if sl.startswith("youtu.be/") or sl.startswith("www.youtube.com/") or sl.startswith("youtube.com/"):
                    return "https://" + s
                return s

            def _extract_video_urls_from_url_prop(_props: dict) -> list[str]:
                out = []
                props_dict = (_props or {})
                # 1) まず type=url の値を拾う（プロパティ名に依存しない）
                for _, v in props_dict.items():
                    if isinstance(v, dict) and v.get("type") == "url":
                        direct_url = _normalize_video_url((v.get("url") or ""))
                        if direct_url:
                            out.append(direct_url)
                # 2) 念のため「URL」名プロパティからテキスト抽出
                if not out:
                    url_prop = None
                    for k, v in props_dict.items():
                        k_norm = re.sub(r"\s+", "", str(k or "")).upper()
                        if k_norm == "URL":
                            url_prop = v
                            break
                    if isinstance(url_prop, dict):
                        out.extend([_normalize_video_url(x) for x in _extract_urls_from_text(_archive_prop_text(url_prop))])
                return list(dict.fromkeys([u for u in out if u]))

            video_urls = _extract_video_urls_from_url_prop(props)
            if not video_urls and page_id:
                # stateキャッシュが古い場合に備えて、ページAPIから再取得して再判定
                fresh_page = _get_page_from_state_or_api(page_id, force_api=True)
                fresh_props = (fresh_page or {}).get("properties") or {}
                if fresh_props:
                    video_urls = _extract_video_urls_from_url_prop(fresh_props)
            # 出演ページ側にURLがない場合、関連する演奏曲ページのURLを拾う
            if not video_urls and rel_ids:
                for rid in rel_ids:
                    score_page = id_to_page.get(rid) or {}
                    score_props = score_page.get("properties") or {}
                    video_urls.extend(_extract_video_urls_from_url_prop(score_props))
                video_urls = list(dict.fromkeys([u for u in video_urls if u]))
            place = (props.get("ロケーション") or {}).get("place") or {}
            venue = ""
            venue_lat = None
            venue_lon = None
            if isinstance(place, dict):
                venue = (place.get("name") or place.get("address") or "").strip()
                venue_lat = place.get("lat", place.get("latitude"))
                venue_lon = place.get("lon", place.get("longitude"))

            with st.expander(f"{jp}  ({media} / {exp_date})", expanded=False):
                c1, c2 = st.columns([1, 2])
                with c1:
                    cu = get_current_notion_url(p)
                    if cu:
                        st.image(cu, use_container_width=True)
                    else:
                        st.caption("（フライヤー未設定）")
                with c2:
                    st.caption(f"ID: `{page_id}`")
                    st.caption(f"クリエイター: {creator or '—'}")
                    st.caption(f"キャスト・関係者: {cast or '—'}")
                    st.caption(f"会場: {venue or '—'}")
                    if venue_lat is not None and venue_lon is not None:
                        try:
                            st.map(pd.DataFrame([{"lat": float(venue_lat), "lon": float(venue_lon)}]), size=20)
                        except Exception:
                            pass
                    st.caption(f"自分が演奏した曲: {len(played_titles)} 件")
                    if rel_titles:
                        st.markdown("**プログラム（演奏曲）**")
                        section_order = ["幕前", "ロビー", "本編", "ソリストEncore", "Encore"]
                        grouped_rows = {}
                        for rid in rel_ids:
                            t = id_to_title.get(rid, rid[:8])
                            extra = rel_info_map.get(rid) or {}
                            sec = extra.get("section") or ""
                            ordv = extra.get("order")
                            part = extra.get("part") or ""
                            played = bool(extra.get("played"))
                            score_page = id_to_page.get(rid)
                            score_props = (score_page or {}).get("properties", {}) if score_page else {}
                            is_concerto = bool((score_props.get("協奏曲") or {}).get("checkbox", False))
                            soloists = plain_text_join((score_props.get("ソリスト") or {}).get("rich_text", []))
                            sec_key = sec if sec else "本編"
                            ord_num = 9999
                            try:
                                ord_num = int(str(ordv).strip())
                            except Exception:
                                ord_num = 9999
                            grouped_rows.setdefault(sec_key, []).append({
                                "title": t,
                                "order": ord_num,
                                "soloists": soloists,
                                "is_concerto": is_concerto,
                                "played": played,
                                "part": part,
                                "movements": extra.get("movements") or [],
                            })

                        display_sections = [s for s in section_order if s in grouped_rows] + [
                            s for s in grouped_rows.keys() if s not in section_order
                        ]
                        for sec_name in display_sections:
                            if sec_name == "ソリストEncore":
                                st.markdown("**＜ソリストEncore＞**")
                            else:
                                st.markdown(f"**【{sec_name}】**")
                            rows = sorted(
                                grouped_rows.get(sec_name, []),
                                key=lambda r: (r["order"], (r["title"] or "").casefold()),
                            )
                            for row in rows:
                                st.write(f"- {row['title']}")
                                if row.get("movements"):
                                    st.caption(f"Movements: {' / '.join(row.get('movements') or [])}")
                                if row.get("is_concerto") and row.get("soloists"):
                                    st.caption(f"Soloist: {row['soloists']}")
                                if row.get("played") and row.get("part"):
                                    st.caption(f"Assigned: {row['part']}")
                    st.caption(f"動画URL検出: {len(video_urls)} 件")
                    if video_urls:
                        st.markdown("**動画URL**")
                        yt_urls = []
                        other_urls = []
                        for u in video_urls:
                            ul = (u or "").lower()
                            embed_u = _to_youtube_embed_url(u)
                            if embed_u or ("youtube.com" in ul) or ("youtu.be" in ul):
                                yt_urls.append((u, embed_u))
                            else:
                                other_urls.append(u)
                        for raw_u, embed_u in yt_urls:
                            # YouTube は iframe 埋め込みを優先（st.video で表示されない環境対策）
                            video_src = embed_u or raw_u
                            st.components.v1.html(
                                f'<iframe width="100%" height="315" src="{video_src}" '
                                'frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" '
                                "allowfullscreen></iframe>",
                                height=330,
                            )
                            st.caption(raw_u)
                        for u in other_urls:
                            st.markdown(f"- [リンクを開く]({u})")
                    else:
                        st.caption("（URL/動画列に有効なリンクが見つかりませんでした）")

# ============================================================
# 手動確認モード
# ============================================================
if mode == "データ管理":
    display_pages = get_display_pages()
    pending_focus_id = st.session_state.pop("pending_focus_page_id", None)
    if pending_focus_id:
        st.session_state.focus_page_id = pending_focus_id
        st.session_state.pending_force_scroll_top = True
        # フォーカス遷移時は検索欄も対象タイトルへ同期（前回検索の残りで隠れないようにする）
        pending_q = st.session_state.get("pending_manual_search_query", "")
        st.session_state["_cti_manual_search_query"] = pending_q
    focus_id = st.session_state.get("focus_page_id")
    if focus_id:
        display_pages = sorted(display_pages, key=lambda p: 0 if p.get("id") == focus_id else 1)
        st.session_state.manual_page = 0

    st.subheader(f"🗂 データ管理　表示: {len(display_pages)} 件 / 全 {len(target_pages)} 件")
    if diff_filter != "フィルタなし":
        st.caption(f"差分フィルタ適用中: {diff_filter}")
    if "pending_manual_search_query" in st.session_state:
        pending_q = st.session_state.pop("pending_manual_search_query", "")
        st.session_state["_cti_manual_search_query"] = pending_q

    search_query = clearable_text_input(
        "🔎 タイトルで絞り込む", "manual_search_query",
        placeholder="日本語・英語どちらでも可（部分一致）",
    )
    if search_query:
        q_lower = search_query.lower()
        display_pages = [
            p for p in display_pages
            if q_lower in get_title(p["properties"])[1].lower()
            or q_lower in get_title(p["properties"])[2].lower()
        ]
        st.caption(f"「{search_query}」に一致: {len(display_pages)} 件")
        st.session_state.manual_page = 0
        if len(display_pages) == 1:
            st.session_state.focus_page_id = display_pages[0].get("id")
    # 検索条件でフォーカス対象が落ちても先頭に差し込んで編集導線を維持
    if focus_id and not any(p.get("id") == focus_id for p in display_pages):
        focus_page = next((p for p in target_pages if p.get("id") == focus_id), None)
        if focus_page is not None:
            display_pages = [focus_page] + display_pages

    PAGE_SIZE         = 20
    total_pages_count = max(1, (len(display_pages) + PAGE_SIZE - 1) // PAGE_SIZE)
    if st.session_state.manual_page >= total_pages_count:
        st.session_state.manual_page = 0

    col_prev, col_info, col_next = st.columns([1, 3, 1])
    with col_prev:
        if st.button("◀ 前へ") and st.session_state.manual_page > 0:
            st.session_state.manual_page -= 1
            st.rerun()
    with col_info:
        st.write(f"ページ {st.session_state.manual_page + 1} / {total_pages_count}")
    with col_next:
        if st.button("次へ ▶") and st.session_state.manual_page < total_pages_count - 1:
            st.session_state.manual_page += 1
            st.rerun()

    start      = st.session_state.manual_page * PAGE_SIZE
    page_items = display_pages[start : start + PAGE_SIZE]

    for item in page_items:
        props     = item["properties"]
        log_title, jp, en = get_title(props)
        page_id   = item["id"]
        notion_ok_now, drive_ok_now = get_diff_status(item)
        saved_tmdb_id, saved_media_type = get_tmdb_id_from_notion(props)

        page_media = get_page_media(item)
        is_tmdb_media = page_media in ("映画", "ドラマ")
        is_event_media = page_media in ("出演", "演奏会（鑑賞）", "ライブ/ショー", "展示会", "イベント")

        with st.expander(
            f"{log_title}",
            expanded=(st.session_state.get("focus_page_id") == page_id or len(page_items) == 1),
        ):
            # 候補反映でセットしたタイトルを、次runで入力欄へ確実に反映
            pending_jp_key = f"pending_edit_jp_{page_id}"
            pending_en_key = f"pending_edit_en_{page_id}"
            if pending_jp_key in st.session_state:
                st.session_state[f"_cti_edit_jp_{page_id}"] = st.session_state.pop(pending_jp_key)
                st.session_state.pop(f"edit_jp_{page_id}", None)
            if pending_en_key in st.session_state:
                st.session_state[f"_cti_edit_en_{page_id}"] = st.session_state.pop(pending_en_key)
                st.session_state.pop(f"edit_en_{page_id}", None)

            def run_single_refresh():
                media = page_media
                existing_release = ((props.get("リリース日") or {}).get("date") or {}).get("start") or None
                if media in ("映画", "ドラマ"):
                    season_number = get_season_number(props)
                    if saved_tmdb_id and saved_media_type:
                        top = fetch_tmdb_by_id(saved_tmdb_id, saved_media_type)
                        src = "🆔 ID参照"
                    else:
                        query = en if en else jp
                        results = search_tmdb(query, existing_release[:4] if existing_release else None)
                        top = results[0] if results else None
                        src = "🔍 検索"
                    if not top:
                        return False, f"候補なし ({src})"
                    tmdb_id = top["id"]
                    media_type = top.get("media_type", saved_media_type or ("movie" if media == "映画" else "tv"))
                    cover_url = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{top['poster_path']}"
                    tmdb_release = top.get("release_date") or top.get("first_air_date")
                    n_ok, d_ok, meta_ok, updated = update_all(
                        page_id, cover_url, tmdb_release, existing_release,
                        log_title, tmdb_id, media_type, True, (not is_drive_skip_mode()),
                        force_meta=True, props=props, season_number=season_number,
                        is_refresh=True,
                    )
                    all_ok = n_ok and d_ok and meta_ok
                    return all_ok, f"TMDB: {('成功' if all_ok else '失敗')}"
                if media == "アニメ":
                    anilist_id = (props.get("AniList_ID") or {}).get("number")
                    if not anilist_id:
                        return False, "AniList_IDが未設定"
                    anime = fetch_anime_by_id(int(anilist_id))
                    if not anime:
                        return False, "AniList取得失敗"
                    cover_url = anime.get("cover_url", "")
                    n_ok = update_notion_cover(page_id, cover_url, None, None, is_refresh=False) if cover_url else True
                    meta_ok, _ = update_notion_metadata(
                        page_id,
                        {"genres": anime.get("genres", []), "cast": "", "director": anime.get("director", ""), "score": anime.get("score")},
                        force=True, props=props,
                    )
                    d_ok = bool(save_to_drive(cover_url, log_title, anilist_id)) if cover_url else True
                    all_ok = n_ok and meta_ok and d_ok
                    return all_ok, f"AniList: {('成功' if all_ok else '失敗')}"
                if media == "ゲーム":
                    igdb_id = (props.get("IGDB_ID") or {}).get("number")
                    if not igdb_id:
                        return False, "IGDB_IDが未設定"
                    game = fetch_game_by_id(int(igdb_id))
                    if not game:
                        return False, "IGDB取得失敗"
                    cover_url = game.get("cover_url", "")
                    n_ok = update_notion_cover(page_id, cover_url, None, None, is_refresh=False) if cover_url else True
                    d_ok = bool(save_to_drive(cover_url, log_title, igdb_id)) if cover_url else True
                    all_ok = n_ok and d_ok
                    return all_ok, f"IGDB: {('成功' if all_ok else '失敗')}"
                if media == "音楽アルバム":
                    itunes_id = (props.get("iTunes_ID") or {}).get("number")
                    if not itunes_id:
                        return False, "iTunes_IDが未設定"
                    album = fetch_album_by_id(int(itunes_id))
                    if not album:
                        return False, "iTunes取得失敗"
                    cover_url = album.get("cover_url", "")
                    n_ok = update_notion_cover(page_id, cover_url, None, None, is_refresh=False) if cover_url else True
                    d_ok = bool(save_to_drive(cover_url, log_title, itunes_id)) if cover_url else True
                    all_ok = n_ok and d_ok
                    return all_ok, f"iTunes: {('成功' if all_ok else '失敗')}"
                if media in ("書籍", "漫画"):
                    isbn_val = plain_text_join((props.get("ISBN") or {}).get("rich_text", []))
                    author_val = plain_text_join((props.get("クリエイター") or {}).get("rich_text", []))
                    title_val = jp or en or log_title
                    cover_candidates = collect_book_cover_candidates(isbn_val, title_val, author_val or None, "")
                    cover_url = choose_best_cover(cover_candidates) or ""
                    if not cover_url:
                        return False, "書影取得失敗"
                    n_ok = update_notion_cover(page_id, cover_url, None, None, is_refresh=False)
                    drive_id = isbn_val or page_id
                    d_ok = bool(save_to_drive(cover_url, log_title, drive_id))
                    all_ok = n_ok and d_ok
                    return all_ok, f"書影: {('成功' if all_ok else '失敗')}"
                return False, "対象外"

            # ── ステータス行 ──
            stat_c1, stat_c2, stat_c3 = st.columns(3)
            stat_c1.metric("媒体", page_media or "不明")
            with stat_c2:
                st.caption(f"Notionカバー: {'🟢' if notion_ok_now else '🔴'}")
            with stat_c3:
                if is_drive_skip_mode():
                    st.caption("Drive画像: ⏭ スキップ中")
                else:
                    st.caption(f"Drive画像: {'🟢' if drive_ok_now else '🔴'}")
            if is_drive_skip_mode():
                st.caption("🟢=登録済 / 🔴=未登録 / ⏭=スキップ中")
            else:
                st.caption("🟢=登録済 / 🔴=未登録")
            if st.button("🔄 このページをリフレッシュ", key=f"refresh_one_{page_id}"):
                with st.spinner("リフレッシュ中..."):
                    ok, msg = run_single_refresh()
                if ok:
                    st.success(f"✅ {msg}")
                    sync_notion_after_update(page_id=page_id)
                    st.rerun()
                else:
                    st.error(f"❌ {msg}")

            # ── カバー画像プレビュー ──
            current_url = get_current_notion_url(item)
            if current_url:
                img_c, info_c = st.columns([1, 3])
                img_c.image(current_url, use_container_width=True)
                with info_c:
                    st.caption(f"カバーURL: `{format_cover_url(current_url)}`")
                    if "?" in current_url:
                        with st.expander("フルURLを表示"):
                            st.code(current_url, language="text")
                    # 読み取り専用フィールド表示
                    release_val = ((props.get("リリース日") or {}).get("date") or {}).get("start", "") or "—"
                    genre_items = (props.get("ジャンル") or {}).get("multi_select", [])
                    genre_val   = "　".join(g["name"] for g in genre_items) if genre_items else "—"
                    creator_val = plain_text_join((props.get("クリエイター") or {}).get("rich_text", [])) or "—"
                    cast_val    = plain_text_join((props.get("キャスト・関係者") or {}).get("rich_text", [])) or "—"
                    tmdb_score  = (props.get("TMDB_score") or {}).get("number")
                    isbn_val    = plain_text_join((props.get("ISBN") or {}).get("rich_text", [])) or "—"
                    igdb_id_val = (props.get("IGDB_ID") or {}).get("number")
                    itunes_id_val = (props.get("iTunes_ID") or {}).get("number")
                    anilist_id_val = (props.get("AniList_ID") or {}).get("number")

                    st.caption(f"📅 リリース日: {release_val}")
                    if is_tmdb_media:
                        st.caption(f"🎭 ジャンル: {genre_val}")
                        st.caption(f"🎬 クリエイター: {creator_val}")
                        st.caption(f"🎭 キャスト・関係者: {cast_val[:80] + '…' if len(cast_val) > 80 else cast_val}")
                        if tmdb_score is not None:
                            st.caption(f"⭐ TMDBスコア: {tmdb_score}")
                        st.caption(f"🆔 TMDB_ID: {saved_tmdb_id or '—'}")
                    if page_media in ("書籍", "漫画"):
                        st.caption(f"📚 クリエイター: {creator_val}")
                        st.caption(f"🔢 ISBN: {isbn_val}")
                    if page_media == "音楽アルバム":
                        st.caption(f"🎵 クリエイター: {creator_val}")
                        if itunes_id_val: st.caption(f"🆔 iTunes_ID: {itunes_id_val}")
                    if page_media == "ゲーム":
                        if igdb_id_val: st.caption(f"🆔 IGDB_ID: {igdb_id_val}")
                    if page_media == "アニメ":
                        if anilist_id_val: st.caption(f"🆔 AniList_ID: {anilist_id_val}")

            # ── 基本 ──
            st.divider()
            with st.expander("✏️ 基本", expanded=False):
                existing_rating = (props.get("評価") or {}).get("select") or {}
                existing_rating = existing_rating.get("name", "") if isinstance(existing_rating, dict) else ""
                existing_memo   = plain_text_join((props.get("メモ") or {}).get("rich_text", []))
                existing_date_prop = "体験日" if "体験日" in props else ("鑑賞日" if "鑑賞日" in props else get_experience_date_property_name())
                existing_date_start = get_experience_date_from_props(props)
                edit_col1, edit_col2, edit_col3 = st.columns([1.5, 3, 1.2])
                new_rating = edit_col1.selectbox(
                    "評価", RATING_OPTIONS,
                    index=RATING_OPTIONS.index(existing_rating) if existing_rating in RATING_OPTIONS else 0,
                    key=f"edit_rating_{page_id}",
                )
                new_memo   = edit_col2.text_input("メモ", value=existing_memo, key=f"edit_memo_{page_id}")
                new_date   = edit_col3.text_input(existing_date_prop, value=existing_date_start, placeholder="YYYY-MM-DD", key=f"edit_date_{page_id}")

                # タイトル編集（全媒体）
                existing_jp = jp or ""
                existing_en = en or ""
                title_c1, title_c2 = st.columns(2)
                new_jp = clearable_text_input("日本語タイトル", f"edit_jp_{page_id}", value=existing_jp, container=title_c1)
                new_en = clearable_text_input("英語タイトル",   f"edit_en_{page_id}", value=existing_en, container=title_c2)
                if page_media == "演奏曲":
                    st.caption("🔎 作曲家→作品で候補検索（MusicBrainz）")
                    comp_col, title_col = st.columns(2)
                    comp_query = comp_col.text_input("作曲家名", key=f"edit_score_comp_{page_id}", placeholder="例: Beethoven / ベートーヴェン")
                    work_filter = title_col.text_input("作品名で絞り込み（任意）", key=f"edit_score_work_filter_{page_id}", placeholder="例: Symphony No. 5")
                    if comp_query.strip() or work_filter.strip():
                        st.session_state.focus_page_id = page_id
                    comp_key = f"edit_score_composers_{page_id}"
                    works_key = f"edit_score_works_{page_id}"
                    if comp_key not in st.session_state:
                        st.session_state[comp_key] = []
                    if works_key not in st.session_state:
                        st.session_state[works_key] = []

                    c1, c2 = st.columns([1, 1])
                    if c1.button("作曲家を検索", key=f"edit_score_comp_search_{page_id}"):
                        st.session_state.focus_page_id = page_id
                        if comp_query.strip():
                            with st.spinner("作曲家を検索中..."):
                                composers, err = search_mb_composer(comp_query.strip())
                            if err:
                                st.warning(f"作曲家検索失敗: {err}")
                            st.session_state[comp_key] = composers
                            st.session_state[works_key] = []
                        else:
                            st.warning("作曲家名を入力してください")

                    composers = st.session_state.get(comp_key, [])
                    if composers:
                        labels = [format_mb_composer_label(c) for c in composers]
                        sel_idx = st.selectbox("作曲家候補", options=list(range(len(labels))), format_func=lambda i: labels[i], key=f"edit_score_comp_pick_{page_id}")
                        if c2.button("この作曲家の作品を取得", key=f"edit_score_work_fetch_{page_id}"):
                            st.session_state.focus_page_id = page_id
                            composer = composers[sel_idx]
                            with st.spinner("作品一覧を取得中..."):
                                works = search_mb_works(composer["id"], work_filter.strip())
                            st.session_state[works_key] = works

                    works = st.session_state.get(works_key, [])
                    if works:
                        w_options = [w["title"] + (f"　{w['disambiguation']}" if w.get("disambiguation") else "") for w in works]
                        w_pick = st.selectbox("作品候補", w_options, key=f"edit_score_work_pick_{page_id}")
                        if st.button("候補を反映", key=f"edit_score_work_apply_{page_id}"):
                            st.session_state.focus_page_id = page_id
                            picked = works[w_options.index(w_pick)]
                            title_val = picked.get("title", "")
                            composer_name = ""
                            comp_idx_key = f"edit_score_comp_pick_{page_id}"
                            idx = st.session_state.get(comp_idx_key, 0)
                            if composers and isinstance(idx, int) and 0 <= idx < len(composers):
                                composer_name = canonical_mb_composer_name(composers[idx]) or composers[idx].get("name", "")
                            st.session_state[f"pending_edit_jp_{page_id}"] = title_val
                            st.session_state[f"pending_edit_en_{page_id}"] = title_val
                            # 反映後の再描画で確実に見えるよう、Notionにも即保存する
                            patch = {
                                "タイトル": {"title": [{"type": "text", "text": {"content": title_val}}]},
                                "International Title": {"rich_text": [{"type": "text", "text": {"content": title_val}, "annotations": {"italic": True}}]},
                            }
                            if composer_name:
                                patch["クリエイター"] = {"rich_text": [{"type": "text", "text": {"content": composer_name}}]}
                            res = api_request(
                                "patch",
                                f"https://api.notion.com/v1/pages/{page_id}",
                                headers=NOTION_HEADERS,
                                json={"properties": patch},
                            )
                            if res is not None and res.status_code == 200:
                                for p in st.session_state.pages:
                                    if p.get("id") == page_id:
                                        p["properties"]["タイトル"] = patch["タイトル"]
                                        p["properties"]["International Title"] = patch["International Title"]
                                        if "クリエイター" in patch:
                                            p["properties"]["クリエイター"] = patch["クリエイター"]
                                st.session_state.pending_notice = "✅ タイトル欄に反映して保存しました"
                            else:
                                st.session_state.pending_warning = "候補は反映しましたが保存に失敗しました。基本を保存を押してください。"
                            st.rerun()
                    elif work_filter.strip() and st.button("タイトルのみで候補検索", key=f"edit_score_title_only_{page_id}"):
                        st.session_state.focus_page_id = page_id
                        with st.spinner("タイトル候補を検索中..."):
                            cands, err = search_mb_works_by_title(work_filter.strip(), limit=12)
                        if err:
                            st.warning(f"候補検索失敗: {err}")
                        else:
                            st.session_state[works_key] = [{"title": c["title"], "disambiguation": c.get("disambiguation", "")} for c in cands]

                # リリース日編集（全媒体）
                existing_release_edit = ((props.get("リリース日") or {}).get("date") or {}).get("start", "") or ""
                new_release = st.text_input("📅 リリース日", value=existing_release_edit, placeholder="YYYY-MM-DD", key=f"edit_release_{page_id}")

                if edit_col1.button("💾 基本を保存", key=f"save_basic_{page_id}"):
                    patch_props = {}
                    if new_rating != existing_rating:
                        patch_props["評価"] = {"select": {"name": new_rating} if new_rating else None}
                    if new_memo != existing_memo:
                        patch_props["メモ"] = {"rich_text": [{"type": "text", "text": {"content": new_memo}}]}
                    if new_date != existing_date_start and new_date:
                        patch_props[existing_date_prop] = {"date": {"start": new_date}}
                    if new_jp != existing_jp:
                        patch_props["タイトル"] = {"title": [{"type": "text", "text": {"content": new_jp}}]}
                    if new_en != existing_en:
                        patch_props["International Title"] = {"rich_text": [{"type": "text", "text": {"content": new_en}, "annotations": {"italic": True}}]}
                    if new_release != existing_release_edit and new_release:
                        patch_props["リリース日"] = {"date": {"start": new_release}}
                    if patch_props:
                        res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}",
                                          headers=NOTION_HEADERS, json={"properties": patch_props})
                        if res and res.status_code == 200:
                            st.success("✅ 更新しました")
                            for p in st.session_state.pages:
                                if p["id"] == page_id:
                                    for k, v in patch_props.items():
                                        p["properties"][k] = v
                            sync_notion_after_update(page_id=page_id)
                        else:
                            st.error("❌ 更新失敗")
                    else:
                        st.info("変更なし")

            # ── 関連（出演 ↔ 演奏曲）──
            st.divider()
            with st.expander("🔗 関連", expanded=False):
              if page_media in ("出演", "演奏曲"):
                rel_prop = "演奏曲" if page_media == "出演" else "出演履歴"
                rel_target_pages = _get_score_pages() if page_media == "出演" else _get_performance_pages()
                rel_state_key = f"edit_rel_{page_id}"
                id_to_title = {p["id"]: p["title"] for p in rel_target_pages}
                live_page = _get_page_from_state_or_api(page_id, force_api=True)
                live_props = (live_page or {}).get("properties", {}) or props

                def build_rel_items(rel_ids: list[str]) -> list[dict]:
                    items = []
                    for rid in _clean_relation_ids(rel_ids):
                        title = id_to_title.get(rid, "（不明）")
                        if title == "（不明）":
                            page_obj = _get_page_from_state_or_api(rid)
                            if page_obj:
                                title = get_title(page_obj.get("properties", {}))[0] or "（不明）"
                        items.append({"id": rid, "title": title})
                    return items

                existing_rel_ids = [r.get("id") for r in ((live_props.get(rel_prop) or {}).get("relation", [])) if r.get("id")]
                alt_prop = "出演履歴" if rel_prop == "演奏曲" else "演奏曲"
                alt_rel_ids = [r.get("id") for r in ((live_props.get(alt_prop) or {}).get("relation", [])) if r.get("id")]
                # 主/逆の両方を統合（既存データ互換）
                existing_rel_ids = _clean_relation_ids(existing_rel_ids + alt_rel_ids)
                if rel_state_key not in st.session_state:
                    st.session_state[rel_state_key] = build_rel_items(existing_rel_ids)
                else:
                    # 既存関連が増えている場合はUIにも追従
                    selected_ids = {x.get("id") for x in st.session_state.get(rel_state_key, []) if x.get("id")}
                    missing_ids = [rid for rid in existing_rel_ids if rid not in selected_ids]
                    if missing_ids:
                        st.session_state[rel_state_key].extend(build_rel_items(missing_ids))
                if st.button("🔄 関連候補を再読み込み", key=f"edit_rel_reload_{page_id}"):
                    rel_target_pages = _get_score_pages(force_refresh=True) if page_media == "出演" else _get_performance_pages(force_refresh=True)
                    refreshed_page = _get_page_from_state_or_api(page_id, force_api=True)
                    refreshed_props = (refreshed_page or {}).get("properties", {}) or props
                    refreshed_rel_ids = [r.get("id") for r in ((refreshed_props.get(rel_prop) or {}).get("relation", [])) if r.get("id")]
                    alt_prop = "出演履歴" if rel_prop == "演奏曲" else "演奏曲"
                    refreshed_alt_ids = [r.get("id") for r in ((refreshed_props.get(alt_prop) or {}).get("relation", [])) if r.get("id")]
                    refreshed_rel_ids = _clean_relation_ids(refreshed_rel_ids + refreshed_alt_ids)
                    id_to_title = {p["id"]: p["title"] for p in rel_target_pages}
                    st.session_state[rel_state_key] = build_rel_items(refreshed_rel_ids)
                    st.rerun()

                rel_query = st.text_input(
                    "関連先を検索",
                    key=f"edit_rel_query_{page_id}",
                    placeholder="例: 交響曲第5番 / 定期演奏会",
                )
                rel_matches = []
                if rel_query:
                    q = rel_query.strip().lower()
                    rel_matches = [p for p in rel_target_pages if q in (p.get("title") or "").strip().lower()]
                else:
                    rel_matches = rel_target_pages[:20]

                def add_rel(pid, title):
                    selected = st.session_state[rel_state_key]
                    if not any(x["id"] == pid for x in selected):
                        selected.append({"id": pid, "title": title})
                        st.session_state[rel_state_key] = selected

                def persist_relations() -> bool:
                    rel_ids = [x["id"] for x in st.session_state[rel_state_key] if x.get("id")]
                    rel_patch = {"relation": [{"id": rid} for rid in rel_ids]}
                    patch_props = {rel_prop: rel_patch}
                    # 自己リレーションの片側が書き込み不可扱いでも反映されるように両名へセット
                    if rel_prop == "出演履歴":
                        patch_props["演奏曲"] = rel_patch
                    elif rel_prop == "演奏曲":
                        patch_props["出演履歴"] = rel_patch
                    res = api_request(
                        "patch",
                        f"https://api.notion.com/v1/pages/{page_id}",
                        headers=NOTION_HEADERS,
                        json={"properties": patch_props},
                    )
                    if res and res.status_code == 200:
                        for p in st.session_state.pages:
                            if p["id"] == page_id:
                                for k, v in patch_props.items():
                                    p["properties"][k] = v
                        return True
                    return False

                def add_or_create_score_relation(title: str, creator: str = ""):
                    name = (title or "").strip()
                    if not name:
                        return False
                    found = _find_score_page_by_title(rel_target_pages, name)
                    if found:
                        add_rel(found["id"], found["title"])
                        return persist_relations()
                    ok_new = create_notion_page(
                        jp_title=name, en_title=name,
                        media_type_label="演奏曲",
                        tmdb_id=None, media_type="score",
                        cover_url=MB_DEFAULT_COVER,
                        tmdb_release="",
                        details={"genres": [], "cast": "", "director": creator or "", "score": None},
                    )
                    if not ok_new:
                        return False
                    new_id = st.session_state.get("last_created_page_id")
                    upsert_page_in_state(st.session_state.get("last_created_page"))
                    _add_score_page_cache(new_id, name)
                    rel_target_pages.append({"id": new_id, "title": name})
                    add_rel(new_id, name)
                    if persist_relations():
                        sync_notion_after_update(page_id=page_id)
                    sync_notion_after_update(
                        page_id=new_id,
                        updated_page=st.session_state.get("last_created_page"),
                    )
                    return True

                if page_media == "出演":
                    st.caption("🎼 関連曲検索（出演）")
                    rel_song_mode = st.segmented_control(
                        "検索方式",
                        options=["クラシック（MusicBrainz）", "ポピュラー（iTunes）", "両方"],
                        default="両方",
                        key=f"edit_rel_song_mode_{page_id}",
                    )
                    use_mb_rel = rel_song_mode in ("クラシック（MusicBrainz）", "両方")
                    use_it_rel = rel_song_mode in ("ポピュラー（iTunes）", "両方")

                    if use_mb_rel:
                        st.caption("1) 作曲家検索 → 2) 作曲家確定 → 3) 曲名検索 → 4) 追加")
                        comp_query = st.text_input("作曲家名", key=f"edit_rel_mb_comp_{page_id}", placeholder="例: Beethoven / ベートーヴェン")
                        comp_list_key = f"edit_rel_mb_comps_{page_id}"
                        comp_sel_key = f"edit_rel_mb_sel_{page_id}"
                        work_list_key = f"edit_rel_mb_works_{page_id}"
                        work_filter = st.text_input("曲名で絞り込み", key=f"edit_rel_mb_filter_{page_id}", placeholder="例: Symphony No. 5")
                        if comp_query.strip() or work_filter.strip():
                            st.session_state.focus_page_id = page_id
                        if comp_list_key not in st.session_state:
                            st.session_state[comp_list_key] = []
                        if work_list_key not in st.session_state:
                            st.session_state[work_list_key] = []
                        if st.button("🔍 作曲家を検索", key=f"edit_rel_mb_search_{page_id}"):
                            if comp_query.strip():
                                st.session_state.focus_page_id = page_id
                                with st.spinner("作曲家を検索中..."):
                                    comps, err = search_mb_composer(comp_query.strip())
                                if err:
                                    st.warning(f"作曲家検索失敗: {err}")
                                st.session_state[comp_list_key] = comps
                                st.session_state[work_list_key] = []
                                st.session_state.pop(comp_sel_key, None)
                                st.rerun()
                            else:
                                st.warning("作曲家名を入力してください")
                        comps = st.session_state.get(comp_list_key, [])
                        selected_comp = None
                        if comps:
                            labels = [format_mb_composer_label(c) for c in comps]
                            idx = st.selectbox("作曲家候補", list(range(len(labels))), format_func=lambda i: labels[i], key=f"edit_rel_mb_pick_{page_id}")
                            if st.button("✅ この作曲家で進める", key=f"edit_rel_mb_pick_btn_{page_id}"):
                                st.session_state[comp_sel_key] = comps[idx]
                                st.session_state[work_list_key] = []
                                st.session_state.focus_page_id = page_id
                                st.rerun()
                            selected_comp = st.session_state.get(comp_sel_key)
                        if selected_comp:
                            st.success(f"作曲家を確定: {format_mb_composer_label(selected_comp)}")
                            c_mb1, c_mb2 = st.columns([1, 1])
                            if c_mb1.button("🔍 曲名で検索", key=f"edit_rel_mb_work_search_{page_id}"):
                                st.session_state.focus_page_id = page_id
                                with st.spinner("作品を検索中..."):
                                    works = search_mb_works(selected_comp["id"], work_filter.strip())
                                st.session_state[work_list_key] = works
                                st.rerun()
                            if c_mb2.button("📚 全作品を取得（重い）", key=f"edit_rel_mb_work_all_{page_id}"):
                                st.session_state.focus_page_id = page_id
                                with st.spinner("全作品を取得中..."):
                                    works = search_mb_works(selected_comp["id"], "")
                                st.session_state[work_list_key] = works
                                st.rerun()
                        rel_works = st.session_state.get(work_list_key, [])
                        if rel_works:
                            st.caption(f"{len(rel_works)} 件")
                            for i, w in enumerate(rel_works):
                                title_w = w.get("title", "")
                                label = title_w + (f"　{w.get('disambiguation','')}" if w.get("disambiguation") else "")
                                c_t, c_b = st.columns([5, 1.2])
                                c_t.write(label)
                                if c_b.button("🎼 曲を追加", key=f"edit_rel_mb_add_{page_id}_{i}"):
                                    st.session_state.focus_page_id = page_id
                                    creator_name = canonical_mb_composer_name(selected_comp or {}) or (selected_comp or {}).get("name", "")
                                    if add_or_create_score_relation(title_w, creator_name):
                                        st.session_state.pending_notice = f"✅ 関連を追加: {title_w}"
                                    else:
                                        st.session_state.pending_warning = f"関連追加に失敗: {title_w}"
                                    st.rerun()

                    if use_it_rel:
                        st.caption("🔍 ポピュラー曲検索（iTunes）")
                        c_art, c_title = st.columns([1, 1])
                        it_artist = c_art.text_input("アーティスト名", key=f"edit_rel_it_art_{page_id}", placeholder="例: Queen")
                        it_title = c_title.text_input("曲名", key=f"edit_rel_it_title_{page_id}", placeholder="例: Bohemian Rhapsody")
                        it_res_key = f"edit_rel_it_res_{page_id}"
                        if it_artist.strip() or it_title.strip():
                            st.session_state.focus_page_id = page_id
                        if st.button("🔍 曲を検索", key=f"edit_rel_it_search_{page_id}"):
                            q = " ".join(filter(None, [it_artist, it_title])).strip()
                            if q:
                                st.session_state.focus_page_id = page_id
                                with st.spinner("検索中..."):
                                    res = api_request("get", "https://itunes.apple.com/search", params={"term": q, "entity": "song", "limit": 20, "lang": "ja_jp"})
                                st.session_state[it_res_key] = (res.json().get("results", []) if res else [])
                                st.rerun()
                            else:
                                st.warning("アーティスト名または曲名を入力してください")
                        for i, track in enumerate(st.session_state.get(it_res_key, [])):
                            track_name = track.get("trackName", "")
                            artist_name = track.get("artistName", "")
                            c_t, c_b = st.columns([5, 1.2])
                            c_t.write(f"{track_name} — {artist_name}")
                            if c_b.button("🎵 曲を追加", key=f"edit_rel_it_add_{page_id}_{i}"):
                                st.session_state.focus_page_id = page_id
                                if add_or_create_score_relation(track_name, artist_name):
                                    st.session_state.pending_notice = f"✅ 関連を追加: {track_name}"
                                else:
                                    st.session_state.pending_warning = f"関連追加に失敗: {track_name}"
                                st.rerun()
                    st.divider()

                if rel_matches:
                    if not rel_query:
                        st.caption("検索語未入力のため候補を先頭20件表示しています。")
                    options = ["（選択してください）"] + [p["title"] for p in rel_matches]
                    sel = st.selectbox("候補", options, key=f"edit_rel_pick_{page_id}")
                    if sel != "（選択してください）":
                        picked = rel_matches[options.index(sel) - 1]
                        if st.button("🔗 関連先を追加", key=f"edit_rel_add_{page_id}"):
                            add_rel(picked["id"], picked["title"])
                            if persist_relations():
                                sync_notion_after_update(page_id=page_id)
                                st.success("✅ 関連を保存しました")
                            else:
                                st.error("❌ 関連保存に失敗しました")
                            st.rerun()
                elif rel_query:
                    st.caption("候補が見つかりませんでした。")
                if rel_query:
                    if st.button("🆕 新規登録して追加", key=f"edit_rel_create_{page_id}"):
                        new_title = rel_query.strip()
                        if not new_title:
                            st.warning("新規作成するタイトルを入力してください。")
                        elif page_media == "出演":
                            # 検索語をそのままタイトルにせず、仮タイトルで作成して詳細編集で確定する
                            draft_title = "（演奏曲・要編集）"
                            ok = create_notion_page(
                                jp_title=draft_title, en_title=draft_title,
                                media_type_label="演奏曲",
                                tmdb_id=None, media_type="score",
                                cover_url=MB_DEFAULT_COVER,
                                tmdb_release="",
                                details={"genres": [], "cast": "", "director": "", "score": None},
                            )
                            if ok:
                                new_id = st.session_state.get("last_created_page_id")
                                upsert_page_in_state(st.session_state.get("last_created_page"))
                                _add_score_page_cache(new_id, draft_title)
                                add_rel(new_id, draft_title)
                                if persist_relations():
                                    sync_notion_after_update(page_id=page_id)
                                sync_notion_after_update(
                                    page_id=new_id,
                                    updated_page=st.session_state.get("last_created_page"),
                                )
                                _focus_management_page(new_id, draft_title, "演奏曲")
                                st.session_state.pending_notice = f"✅ 演奏曲を追加しました（検索語: {new_title} / 仮タイトルで作成。候補検索で確定してください）"
                                st.rerun()
                        else:
                            ok = create_notion_page(
                                jp_title=new_title, en_title=new_title,
                                media_type_label="出演",
                                tmdb_id=None, media_type="event",
                                cover_url=get_media_icon_url("出演"),
                                tmdb_release="",
                                details={"genres": [], "cast": "", "director": "", "score": None},
                            )
                            if ok:
                                new_id = st.session_state.get("last_created_page_id")
                                upsert_page_in_state(st.session_state.get("last_created_page"))
                                _add_performance_page_cache(new_id, new_title)
                                add_rel(new_id, new_title)
                                if persist_relations():
                                    sync_notion_after_update(page_id=page_id)
                                sync_notion_after_update(
                                    page_id=new_id,
                                    updated_page=st.session_state.get("last_created_page"),
                                )
                                _focus_management_page(new_id, new_title, "出演")
                                st.session_state.pending_notice = "✅ 出演データを追加しました（詳細編集を開きます）"
                                st.rerun()

                if st.session_state[rel_state_key]:
                    st.caption("✅ 関連付け済み")
                    for i, item in enumerate(st.session_state[rel_state_key]):
                        col_t, col_del = st.columns([4, 1])
                        col_t.write(item["title"])
                        if col_del.button("✕", key=f"edit_rel_rm_{page_id}_{i}"):
                            st.session_state[rel_state_key] = [
                                x for j, x in enumerate(st.session_state[rel_state_key]) if j != i
                            ]
                            if persist_relations():
                                sync_notion_after_update(page_id=page_id)
                            st.rerun()

                if st.button("💾 関連を保存", key=f"save_rel_{page_id}"):
                    if persist_relations():
                        st.success("✅ 更新しました")
                        sync_notion_after_update(page_id=page_id)
                    else:
                        st.error("❌ 更新失敗")
              else:
                st.caption("この媒体には関連編集はありません。")

            # ── メタ / ID ──
            st.divider()
            with st.expander("🧩 メタ / ID", expanded=False):
                existing_genres = " / ".join(g["name"] for g in (props.get("ジャンル") or {}).get("multi_select", []))
                existing_creator = plain_text_join((props.get("クリエイター") or {}).get("rich_text", []))
                existing_cast = plain_text_join((props.get("キャスト・関係者") or {}).get("rich_text", []))
                meta_c1, meta_c2 = st.columns(2)
                new_genres = meta_c1.text_input("ジャンル（区切り: / または , ）", value=existing_genres, key=f"edit_genres_{page_id}")
                new_creator = meta_c2.text_input("クリエイター", value=existing_creator, key=f"edit_creator_{page_id}")
                new_cast = st.text_input("キャスト・関係者", value=existing_cast, key=f"edit_cast_{page_id}")

                media_c1, media_c2 = st.columns([1.2, 2.8])
                with media_c1:
                    new_media = st.selectbox(
                        "媒体",
                        options=list(MEDIA_ICON_MAP.keys()),
                        index=list(MEDIA_ICON_MAP.keys()).index(page_media) if page_media in MEDIA_ICON_MAP else 0,
                        key=f"edit_media_{page_id}",
                    )
                tmdb_input = None
                with media_c2:
                    if new_media in ("映画", "ドラマ"):
                        current_tmdb = (props.get("TMDB_ID") or {}).get("number") or 0
                        tmdb_input = st.number_input(
                            "TMDB_ID",
                            value=int(current_tmdb) if current_tmdb else 0,
                            min_value=0,
                            step=1,
                            key=f"edit_tmdb_{page_id}",
                        )

                id_c1, id_c2, id_c3, id_c4 = st.columns(4)
                current_isbn = plain_text_join((props.get("ISBN") or {}).get("rich_text", []))
                new_isbn = id_c1.text_input("ISBN", value=current_isbn, key=f"edit_isbn_{page_id}")
                current_anilist = (props.get("AniList_ID") or {}).get("number") or 0
                new_anilist = id_c2.number_input("AniList_ID", value=int(current_anilist) if current_anilist else 0, min_value=0, step=1, key=f"edit_anilist_{page_id}")
                current_igdb = (props.get("IGDB_ID") or {}).get("number") or 0
                new_igdb = id_c3.number_input("IGDB_ID", value=int(current_igdb) if current_igdb else 0, min_value=0, step=1, key=f"edit_igdb_{page_id}")
                current_itunes = (props.get("iTunes_ID") or {}).get("number") or 0
                new_itunes = id_c4.number_input("iTunes_ID", value=int(current_itunes) if current_itunes else 0, min_value=0, step=1, key=f"edit_itunes_{page_id}")

                if st.button("💾 メタ/IDを保存", key=f"save_meta_{page_id}"):
                    patch_props = {}
                    patch_icon = None
                    if new_media != page_media:
                        patch_props["媒体"] = {"multi_select": [{"name": new_media}]}
                        patch_icon = get_media_icon_payload(new_media)
                    if new_genres != existing_genres:
                        genres_list = [g.strip() for g in re.split(r'[/,、]', new_genres) if g.strip()]
                        patch_props["ジャンル"] = {"multi_select": [{"name": g} for g in genres_list]}
                    if new_creator != existing_creator:
                        patch_props["クリエイター"] = {"rich_text": [{"type": "text", "text": {"content": new_creator}}]}
                    if new_cast != existing_cast:
                        patch_props["キャスト・関係者"] = {"rich_text": [{"type": "text", "text": {"content": new_cast}}]}
                    if tmdb_input is not None:
                        if tmdb_input != ((props.get("TMDB_ID") or {}).get("number") or 0):
                            patch_props["TMDB_ID"] = {"number": int(tmdb_input)} if tmdb_input else {"number": None}
                    if new_isbn != current_isbn:
                        patch_props["ISBN"] = {"rich_text": [{"type": "text", "text": {"content": new_isbn}}]} if new_isbn else {"rich_text": []}
                    if new_anilist != current_anilist:
                        patch_props["AniList_ID"] = {"number": int(new_anilist)} if new_anilist else {"number": None}
                    if new_igdb != current_igdb:
                        patch_props["IGDB_ID"] = {"number": int(new_igdb)} if new_igdb else {"number": None}
                    if new_itunes != current_itunes:
                        patch_props["iTunes_ID"] = {"number": int(new_itunes)} if new_itunes else {"number": None}
                    if patch_props:
                        payload = {"properties": patch_props}
                        if patch_icon:
                            payload["icon"] = patch_icon
                        res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}",
                                          headers=NOTION_HEADERS, json=payload)
                        if res and res.status_code == 200:
                            st.success("✅ 更新しました")
                            for p in st.session_state.pages:
                                if p["id"] == page_id:
                                    for k, v in patch_props.items():
                                        p["properties"][k] = v
                                    if patch_icon:
                                        p["icon"] = patch_icon
                                    if "TMDB_ID" in patch_props:
                                        if tmdb_input:
                                            st.session_state.tmdb_id_cache[page_id] = int(tmdb_input)
                                        else:
                                            st.session_state.tmdb_id_cache.pop(page_id, None)
                            sync_notion_after_update(page_id=page_id)
                        else:
                            st.error("❌ 更新失敗")
                    else:
                        st.info("変更なし")

            # ── 出演セットリスト編集 ──
            if page_media == "出演":
                st.divider()
                st.caption("🎻 セットリスト編集")
                existing_memo_full = plain_text_join((props.get("メモ") or {}).get("rich_text", []))
                new_setlist = st.text_area(
                    "セットリスト（メモ欄に保存）",
                    value=existing_memo_full,
                    height=200,
                    key=f"setlist_edit_{page_id}",
                    help="1曲1行で入力。[Encore] を区切りとして使用できます",
                )
                if st.button("💾 セットリスト保存", key=f"save_setlist_{page_id}"):
                    res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}",
                                      headers=NOTION_HEADERS,
                                      json={"properties": {"メモ": {"rich_text": [{"type": "text", "text": {"content": new_setlist}}]}}})
                    if res and res.status_code == 200:
                        st.success("✅ セットリスト保存完了")
                        sync_notion_after_update(page_id=page_id)
                    else:
                        st.error("❌ 保存失敗")

            # ── ロケーション編集 ──
            st.divider()
            with st.expander("📍 ロケーション", expanded=False):
                place_prop = (props.get("ロケーション") or {}).get("place") or {}
                if place_prop:
                    place_label = place_prop.get("name") or place_prop.get("address") or "設定済み"
                    st.caption(f"現在: {place_label}")
                else:
                    st.caption("現在: 未設定")

                new_location = location_search_ui(f"mgmt_{page_id}", page_media)
                loc_c1, loc_c2 = st.columns([1, 1])
                with loc_c1:
                    if st.button("💾 ロケーションを保存", key=f"save_loc_{page_id}"):
                        if new_location and new_location.get("lat") and new_location.get("lon"):
                            place_payload = {
                                "lat":  new_location["lat"],
                                "lon":  new_location["lon"],
                                "name": new_location.get("name", ""),
                            }
                            if new_location.get("address"):
                                place_payload["address"] = new_location["address"]
                            res = api_request(
                                "patch",
                                f"https://api.notion.com/v1/pages/{page_id}",
                                headers=NOTION_HEADERS,
                                json={"properties": {"ロケーション": {"place": place_payload}}},
                            )
                            if res and res.status_code == 200:
                                st.success("✅ ロケーションを更新しました")
                                for p in st.session_state.pages:
                                    if p["id"] == page_id:
                                        p["properties"]["ロケーション"] = {"place": place_payload}
                                sync_notion_after_update(page_id=page_id)
                            else:
                                st.error("❌ ロケーション更新失敗")
                        else:
                            st.warning("ロケーションを選択してください")
                with loc_c2:
                    if st.button("🗑 ロケーションをクリア", key=f"clear_loc_{page_id}"):
                        res = api_request(
                            "patch",
                            f"https://api.notion.com/v1/pages/{page_id}",
                            headers=NOTION_HEADERS,
                            json={"properties": {"ロケーション": {"place": None}}},
                        )
                        if res and res.status_code == 200:
                            st.success("✅ ロケーションをクリアしました")
                            for p in st.session_state.pages:
                                if p["id"] == page_id:
                                    p["properties"]["ロケーション"] = {"place": None}
                            sync_notion_after_update(page_id=page_id)
                        else:
                            st.error("❌ ロケーション更新失敗")

            # ── カバー画像アップロード ──
            st.divider()
            with st.expander("🖼 カバー画像を差し替え", expanded=False):
                uploaded_cover = st.file_uploader(
                    "画像をアップロード（JPG / PNG）",
                    type=["jpg", "jpeg", "png"],
                    key=f"cover_upload_{page_id}",
                )
                if uploaded_cover is not None:
                    img_bytes = uploaded_cover.read()
                    mimetype  = "image/jpeg" if uploaded_cover.type == "image/jpeg" else "image/png"
                    st.image(img_bytes, width=160, caption="プレビュー")
                    if st.button("💾 Driveに保存してNotionに反映", key=f"save_cover_{page_id}"):
                        with st.spinner("アップロード中..."):
                            file_id = save_to_drive("", log_title, page_id, image_bytes=img_bytes, mimetype=mimetype)
                            if file_id:
                                public_url = drive_image_url(file_id)
                                # Drive公開設定
                                try:
                                    svc = get_drive_service_safe()
                                    svc.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
                                except Exception:
                                    st.warning("Driveの公開設定に失敗しました。")
                                # Notionカバー更新
                                res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}",
                                                  headers=NOTION_HEADERS,
                                                  json={"cover": {"type": "external", "external": {"url": public_url}}})
                                if res and res.status_code == 200:
                                    st.success(f"✅ カバーを更新しました")
                                    for p in st.session_state.pages:
                                        if p["id"] == page_id:
                                            p["cover"] = {"type": "external", "external": {"url": public_url}}
                                    sync_notion_after_update(page_id=page_id)
                                    st.rerun()
                                else:
                                    st.error("❌ Notion更新失敗")
                            else:
                                st.error("❌ Drive保存失敗")

            # TMDB検索UI（映画・ドラマのみ）
            if is_tmdb_media:
              default_query = re.sub(r'[Ss]eason\s*\d+', '', en if en else jp).strip()
              search_col, btn_col = st.columns([4, 1])
              custom_query = clearable_text_input(
                  "🔍 検索ワード", f"custom_query_{page_id}",
                  value=default_query,
                  placeholder="英語タイトルで検索すると精度UP",
                  container=search_col,
              )
              with btn_col:
                  st.write("")
                  st.write("")
                  do_search = st.button("検索", key=f"search_{page_id}")

              if do_search:
                  try:
                      candidates = []
                      if saved_tmdb_id and saved_media_type:
                          top = fetch_tmdb_by_id(saved_tmdb_id, saved_media_type)
                          if top:
                              candidates.append(top)
                      search_results = search_tmdb(custom_query)
                      for r in search_results:
                          if len(candidates) >= 10:
                              break
                          if not any(c["id"] == r["id"] for c in candidates):
                              candidates.append(r)
                      st.session_state.search_results[page_id] = candidates[:10]
                  except Exception as e:
                      st.error(f"検索エラー: {e}")
                      st.session_state.search_results[page_id] = []

              candidates = st.session_state.search_results.get(page_id)
              if candidates is not None:
                  if not candidates:
                      st.warning("候補が見つかりませんでした")
                  else:
                      for row_start in range(0, len(candidates), 3):
                          cols = st.columns(3)
                          for col_idx, cand in enumerate(candidates[row_start:row_start + 3]):
                              abs_idx = row_start + col_idx
                              with cols[col_idx]:
                                  tmdb_id       = cand["id"]
                                  cover_url     = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{cand['poster_path']}"
                                  tmdb_release  = cand.get("release_date") or cand.get("first_air_date") or "不明"
                                  url_match     = (current_url == cover_url)
                                  is_current_id = (saved_tmdb_id == tmdb_id)

                                  if is_current_id:
                                      st.markdown('<div style="border: 3px solid red; padding: 4px; border-radius: 6px;">', unsafe_allow_html=True)
                                  st.image(cover_url)
                                  if is_current_id:
                                      st.markdown('</div>', unsafe_allow_html=True)

                                  st.caption(
                                      f"{'🔴 現在のID ' if is_current_id else ''}"
                                      f"{'✅ 同じURL ' if url_match else ''}"
                                      f"{cand.get('title') or cand.get('name', '?')} "
                                      f"({cand.get('media_type','?')}) {tmdb_release} "
                                      f"🆔 {tmdb_id}"
                                  )
                                  if st.button("✅ 決定", key=f"sel_{page_id}_{abs_idx}"):
                                      date_prop        = props.get("リリース日", {}).get("date")
                                      existing_release = date_prop.get("start") if date_prop else None
                                      media_type       = cand.get("media_type", "movie")
                                      season_number    = get_season_number(props)
                                      need_notion, need_drive = True, (not is_drive_skip_mode())
                                      st.session_state.tmdb_id_cache[page_id] = tmdb_id
                                      n_ok, d_ok, meta_ok, updated = update_all(
                                          page_id, cover_url, tmdb_release, existing_release,
                                          log_title, tmdb_id, media_type, need_notion, need_drive,
                                          force_meta=True, props=props, season_number=season_number,
                                      )
                                      parts = []
                                      if need_notion: parts.append("Notion " + ("✅" if n_ok else "❌失敗"))
                                      if need_drive:  parts.append("Drive "  + ("✅" if d_ok else "❌失敗"))
                                      if updated:     parts.append("メタデータ[" + " / ".join(updated) + "] " + ("✅" if meta_ok else "❌失敗"))
                                      if n_ok and d_ok and meta_ok:
                                          st.success("保存完了！ " + "　".join(parts))
                                          st.session_state.search_results.pop(page_id, None)
                                          for p in st.session_state.pages:
                                              if p["id"] == page_id:
                                                  p["cover"]                    = {"type": "external", "external": {"url": cover_url}}
                                                  p["properties"]["TMDB_ID"]    = {"number": tmdb_id}
                                          time.sleep(1.5)
                                          st.rerun()
                                      else:
                                          st.error("一部失敗しました: " + "　".join(parts))

            # ── AniList_ID修正（アニメのみ） ──
            if page_media == "アニメ":
                st.divider()
                st.caption("🔧 AniList_ID を手動で修正")
                if saved_tmdb_id:
                    st.warning("⚠️ このページは TMDB_ID を持っています。Notion側で削除してください。")
                id_col, save_col = st.columns([3, 1])
                current_anilist = (props.get("AniList_ID") or {}).get("number") or 0
                new_anilist_id = id_col.number_input(
                    "AniList_ID",
                    value=int(current_anilist) if current_anilist else 0,
                    min_value=0,
                    step=1,
                    key=f"anilist_id_input_{page_id}",
                )
                with save_col:
                    st.write("")
                    st.write("")
                    if st.button("💾 保存", key=f"save_anilist_{page_id}"):
                        if new_anilist_id > 0:
                            with st.spinner("更新中..."):
                                anime = fetch_anime_by_id(int(new_anilist_id))
                                if anime:
                                    cover_url = anime.get("cover_url", "")
                                    res = api_request(
                                        "patch",
                                        f"https://api.notion.com/v1/pages/{page_id}",
                                        headers=NOTION_HEADERS,
                                        json={"properties": {"AniList_ID": {"number": int(new_anilist_id)}}},
                                    )
                                    n_ok = res is not None and res.status_code == 200
                                    meta_ok, updated = update_notion_metadata(
                                        page_id,
                                        {
                                            "genres": anime.get("genres", []),
                                            "cast": "",
                                            "director": anime.get("director", ""),
                                            "score": anime.get("score"),
                                        },
                                        force=True,
                                        props=props,
                                    )
                                    c_ok = update_notion_cover(page_id, cover_url, None, None, is_refresh=False) if cover_url else True
                                    if n_ok and meta_ok and c_ok:
                                        st.success("✅ AniList更新完了")
                                        for p in st.session_state.pages:
                                            if p["id"] == page_id:
                                                p["properties"]["AniList_ID"] = {"number": int(new_anilist_id)}
                                                if cover_url:
                                                    p["cover"] = {"type": "external", "external": {"url": cover_url}}
                                        sync_notion_after_update(page_id=page_id)
                                        st.rerun()
                                    else:
                                        st.error("❌ 一部更新に失敗しました")
                                else:
                                    st.error("AniListでIDが見つかりませんでした")
                        else:
                            st.warning("AniList_IDを入力してください")

                st.divider()
                st.caption("🔍 AniList 検索")
                default_query = (en or jp or "").strip()
                search_col, btn_col = st.columns([4, 1])
                anime_query = clearable_text_input(
                    "検索ワード", f"anilist_query_{page_id}",
                    value=default_query,
                    placeholder="タイトルで検索",
                    container=search_col,
                )
                with btn_col:
                    st.write("")
                    st.write("")
                    do_search = st.button("検索", key=f"anilist_search_{page_id}")
                if do_search and anime_query:
                    try:
                        results = search_anime(anime_query)
                        st.session_state.search_results[page_id] = results[:9]
                    except Exception as e:
                        st.error(f"検索エラー: {e}")
                        st.session_state.search_results[page_id] = []
                candidates = st.session_state.search_results.get(page_id)
                if candidates is not None:
                    if not candidates:
                        st.warning("候補が見つかりませんでした")
                    else:
                        for row_start in range(0, len(candidates), 3):
                            cols = st.columns(3)
                            for col_idx, cand in enumerate(candidates[row_start:row_start + 3]):
                                abs_idx = row_start + col_idx
                                with cols[col_idx]:
                                    st.image(cand.get("cover_url", ""))
                                    st.caption(
                                        f"{cand.get('title','?')} "
                                        f"{cand.get('release','')} "
                                        f"🆔 {cand.get('id')}"
                                    )
                                    if st.button("✅ 決定", key=f"sel_anilist_{page_id}_{abs_idx}"):
                                        anime_id = cand.get("id")
                                        if anime_id:
                                            with st.spinner("更新中..."):
                                                res = api_request(
                                                    "patch",
                                                    f"https://api.notion.com/v1/pages/{page_id}",
                                                    headers=NOTION_HEADERS,
                                                    json={"properties": {"AniList_ID": {"number": int(anime_id)}}},
                                                )
                                                n_ok = res is not None and res.status_code == 200
                                                meta_ok, updated = update_notion_metadata(
                                                    page_id,
                                                    {
                                                        "genres": cand.get("genres", []),
                                                        "cast": "",
                                                        "director": cand.get("director", ""),
                                                        "score": cand.get("score"),
                                                    },
                                                    force=True,
                                                    props=props,
                                                )
                                                c_ok = update_notion_cover(page_id, cand.get("cover_url", ""), None, None, is_refresh=False)
                                                if n_ok and meta_ok and c_ok:
                                                    st.success("✅ AniList更新完了")
                                                    for p in st.session_state.pages:
                                                        if p["id"] == page_id:
                                                            p["properties"]["AniList_ID"] = {"number": int(anime_id)}
                                                            if cand.get("cover_url"):
                                                                p["cover"] = {"type": "external", "external": {"url": cand.get("cover_url")}}
                                                    sync_notion_after_update(page_id=page_id)
                                                    st.rerun()
                                                else:
                                                    st.error("❌ 一部更新に失敗しました")




