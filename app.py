import re
import requests
import time
import streamlit as st
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote, unquote, urlparse
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
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
APP_VERSION = "9.01"

# ============================================================
# 媒体マッピング
# ============================================================
MEDIA_ICON_MAP = {
    "映画":          ("🎬 映画",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/camera-reels.svg"),
    "ドラマ":        ("📺 ドラマ",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/display.svg"),
    "演奏会（鑑賞）": ("🎼 演奏会（鑑賞）", "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/music-note-beamed.svg"),
    "出演":          ("🎻 出演",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/music-note-list.svg"),
    "展示会":        ("🖼️ 展示会",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/exhibition.svg"),
    "ライブ/ショー": ("🎤 ライブ/ショー", "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/mic.svg"),
    "イベント":      ("🎆 イベント",      "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/exhibition.svg"),
    "書籍":          ("📖 書籍",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/book.svg"),
    "漫画":          ("📚 漫画",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/book-manga.svg"),
    "音楽アルバム":  ("🎵 音楽アルバム",  "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/disc.svg"),
    "ゲーム":        ("🎮 ゲーム",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/controller.svg"),
    "演奏曲":        ("🎼 演奏曲",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/music-score.svg"),
    "アニメ":        ("🌟 アニメ",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/anime.svg"),
}

MEDIA_LABEL_ALIASES = {
    "演奏会（出演）": "出演",
}

RATING_OPTIONS = ["", "★", "★★", "★★★", "★★★★", "★★★★★"]

def get_media_icon_url(media_label: str) -> str:
    normalized = MEDIA_LABEL_ALIASES.get(media_label, media_label)
    return MEDIA_ICON_MAP.get(normalized, ("", ""))[1]

def is_media_icon_url(url: str | None) -> bool:
    if not url:
        return False
    icon_urls = {v[1] for v in MEDIA_ICON_MAP.values() if len(v) > 1 and v[1]}
    return url in icon_urls

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
    ]
    for k in keys:
        st.session_state.pop(k, None)
        st.session_state.pop(f"_cti_{k}", None)

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
            st.session_state.drive_blocked_until = now + 45
            last_err = st.session_state.get("drive_last_error", "")
            msg = f"Google Drive 接続エラー: {e2 or e}"
            if msg != last_err:
                st.warning(msg)
                st.session_state.drive_last_error = msg
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

def save_bytes_to_drive(filename: str, image_bytes: bytes, mimetype: str, make_public: bool = False) -> str | None:
    service = get_drive_service_safe()
    if service is None:
        return None
    files = get_drive_files()
    media = MediaIoBaseUpload(io.BytesIO(image_bytes), mimetype=mimetype, resumable=False)
    if filename in files:
        file_id = files[filename]
        service.files().update(fileId=file_id, media_body=media).execute()
    else:
        result = service.files().create(
            body={"name": filename, "parents": [DRIVE_FOLDER_ID]},
            media_body=media,
            fields="id",
        ).execute()
        file_id = result["id"]
        st.session_state.drive_files_cache[filename] = file_id
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
    cover = item.get("cover")
    if cover and cover.get("type") == "external":
        return cover.get("external", {}).get("url")
    if cover and cover.get("type") == "file":
        return cover.get("file", {}).get("url")
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
        if "drive_files_cache" not in st.session_state:
            st.session_state.drive_files_cache = {}
        return st.session_state.drive_files_cache
    if "drive_files_cache" not in st.session_state or not isinstance(st.session_state.drive_files_cache, dict):
        refresh_drive_files()
    return st.session_state.drive_files_cache

def refresh_drive_files():
    if is_drive_skip_mode():
        if "drive_files_cache" not in st.session_state:
            st.session_state.drive_files_cache = {}
        return
    service = get_drive_service_safe()
    if service is None:
        if "drive_files_cache" not in st.session_state:
            st.session_state.drive_files_cache = {}
        return
    try:
        results = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
            fields="files(id, name)",
            pageSize=1000,
        ).execute()
        st.session_state.drive_files_cache = {f["name"]: f["id"] for f in results.get("files", [])}
        st.session_state.drive_blocked_until = 0
    except Exception as e:
        st.warning(f"Drive一覧取得失敗: {e}")
        st.session_state.drive_blocked_until = time.time() + 60
        if "drive_files_cache" not in st.session_state:
            st.session_state.drive_files_cache = {}

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
        return f"https://drive.google.com/uc?id={file_id}"
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
        del st.session_state.drive_files_cache[fname]
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
    for attempt in range(max_retries):
        try:
            res = fn(url, **kwargs)
            if res.status_code == 429:
                retry_after = res.headers.get("Retry-After", 5)
                try:
                    retry_after = int(retry_after)
                except Exception:
                    retry_after = 5
                time.sleep(retry_after)
                continue
            if res.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            return res
        except requests.exceptions.RequestException:
            time.sleep(2 ** attempt)
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
        if res is None or res.status_code != 200:
            st.warning(f"Notion取得失敗: {res.status_code if res else 'None'}")
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
MB_DEFAULT_COVER = "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/music-score.svg"

def make_portrait_filename(composer_name: str) -> str:
    return f"portrait_{sanitize_filename(composer_name)}.jpg"

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
        res = requests.get(
            api_url,
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
            img = ((page.get("original") or {}).get("source")
                   or (page.get("thumbnail") or {}).get("source"))
            qid = ((page.get("pageprops") or {}).get("wikibase_item"))
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
        sres = requests.get(
            api_url,
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
            pres = requests.get(
                api_url,
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
                img = ((page.get("original") or {}).get("source")
                       or (page.get("thumbnail") or {}).get("source"))
                qid = ((page.get("pageprops") or {}).get("wikibase_item"))
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
        dres = requests.get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            timeout=DEFAULT_TIMEOUT,
        )
        if dres.status_code != 200:
            return None
        entity = ((dres.json().get("entities") or {}).get(qid)) or {}
        claims = entity.get("claims") or {}
        p18 = claims.get("P18") or []
        if not p18:
            return None
        filename = ((((p18[0].get("mainsnak") or {}).get("datavalue") or {}).get("value")) or "").strip()
        if not filename:
            return None
        cres = requests.get(
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

def _download_image_bytes(url: str) -> tuple[bytes | None, str | None]:
    if not url:
        return None, None
    try:
        res = requests.get(url, timeout=DEFAULT_TIMEOUT)
        if res.status_code != 200 or not res.content:
            return None, None
        ctype = (res.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if not ctype.startswith("image/"):
            ctype = "image/jpeg"
        return res.content, ctype
    except Exception:
        return None, None

def get_composer_portrait_url(composer_name: str, artist_id: str) -> str | None:
    """
    1. Driveに既存の肖像画があればそのURLを返す
    2. なければMusicBrainz → Wikipedia/Wikidata/Commonsで取得してDriveに保存
    3. 取得できなければNoneを返す
    """
    fname = make_portrait_filename(composer_name)
    files = get_drive_files()

    # Drive既存チェック
    if fname in files:
        file_id = files[fname]
        try:
            service = get_drive_service_safe()
            service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
            return f"https://drive.google.com/uc?id={file_id}"
        except Exception:
            pass

    # MusicBrainzからWikipedia/Wikidata情報取得
    try:
        time.sleep(1.1)
        res = requests.get(
            f"https://musicbrainz.org/ws/2/artist/{artist_id}",
            params={"inc": "url-rels", "fmt": "json"},
            headers=MB_HEADERS, timeout=8,
        )
        if res.status_code != 200:
            return None
        relations = res.json().get("relations", [])
        wiki_urls, qid = _extract_mb_wiki_relations(relations)
        image_candidates = []

        # 1) MusicBrainzが持つWikipediaリンクから画像取得（言語問わず）
        for wurl in wiki_urls:
            img_url, qid_from_wiki = _wiki_image_from_page(wurl)
            if img_url:
                image_candidates.append(img_url)
            if not qid and qid_from_wiki:
                qid = qid_from_wiki

        # 2) Wikidata(P18)の原画像
        wd_img = _wikidata_p18_image_url(qid) if qid else None
        if wd_img:
            image_candidates.append(wd_img)

        # 3) 名前検索フォールバック（日本語→英語）
        if not image_candidates:
            img_ja, qid_ja = _wiki_search_image(composer_name, "ja")
            if img_ja:
                image_candidates.append(img_ja)
            if not qid and qid_ja:
                qid = qid_ja
        if not image_candidates:
            img_en, qid_en = _wiki_search_image(composer_name, "en")
            if img_en:
                image_candidates.append(img_en)
            if not qid and qid_en:
                qid = qid_en
        if not image_candidates and qid:
            wd_img = _wikidata_p18_image_url(qid)
            if wd_img:
                image_candidates.append(wd_img)

        if not image_candidates:
            return None

        # 同一URLへの再試行を避ける
        uniq_candidates = []
        seen = set()
        for c in image_candidates:
            if c and c not in seen:
                uniq_candidates.append(c)
                seen.add(c)

        image_bytes, mimetype = None, None
        for cand in uniq_candidates:
            image_bytes, mimetype = _download_image_bytes(cand)
            if image_bytes:
                break
        if not image_bytes:
            return None

        service = get_drive_service_safe()
        if not service:
            return None
        if not mimetype:
            mimetype = "image/jpeg"
        media   = MediaIoBaseUpload(io.BytesIO(image_bytes), mimetype=mimetype, resumable=False)
        result  = service.files().create(
            body={"name": fname, "parents": [DRIVE_FOLDER_ID]},
            media_body=media, fields="id",
        ).execute()
        file_id = result["id"]
        st.session_state.drive_files_cache[fname] = file_id
        service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
        return f"https://drive.google.com/uc?id={file_id}"

    except Exception as e:
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
                "disambiguation": a.get("disambiguation", ""),
                "life_span":      a.get("life-span", {}).get("begin", "")[:4],
            }
            for a in artists
        ], None
    except Exception as e:
        return [], str(e)


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
        if title_filter and title_filter.lower() not in title.lower():
            continue
        disambiguation = w.get("disambiguation", "")
        results.append({
            "id":             w["id"],
            "title":          title,
            "disambiguation": disambiguation,
            "type":           w.get("type", ""),
        })
    # タイトルでソート
    results.sort(key=lambda x: x["title"])
    return results


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
    token = get_igdb_token()
    if not token:
        return []
    headers = {
        "Client-ID":     IGDB_CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }
    body = f'search "{query}"; fields name,cover.url,first_release_date,genres.name,involved_companies.company.name,involved_companies.developer,involved_companies.publisher,summary; limit 20;'
    res = requests.post("https://api.igdb.com/v4/games", headers=headers, data=body)
    if res.status_code != 200:
        return []
    results = []
    for item in res.json():
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
        results.append({
            "id":          item["id"],
            "title":       item.get("name", ""),
            "cover_url":   cover_url,
            "release":     release_year,
            "genres":      genres,
            "developer":   developer,
            "publisher":   publisher,
            "media_type":  "game",
        })
    return results

def fetch_game_by_id(game_id: int) -> dict | None:
    token = get_igdb_token()
    if not token:
        return None
    headers = {
        "Client-ID":     IGDB_CLIENT_ID,
        "Authorization": f"Bearer {token}",
    }
    body = f'fields name,cover.url,first_release_date,genres.name,involved_companies.company.name,involved_companies.developer,involved_companies.publisher,summary; where id = {int(game_id)};'
    res = requests.post("https://api.igdb.com/v4/games", headers=headers, data=body)
    if res.status_code != 200:
        return None
    items = res.json()
    if not items:
        return None
    item = items[0]
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
        "cover_url":   cover_url,
        "release":     release_year,
        "genres":      genres,
        "developer":   developer,
        "publisher":   publisher,
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


def search_wikipedia_jp_title(title: str) -> str:
    """Wikipediaの言語リンク/検索から日本語タイトルを取得（見つからなければ空文字）"""
    candidates = _build_wiki_title_candidates(title)
    if not candidates:
        return ""
    try:
        for cand in candidates:
            search_res = requests.get(
                "https://en.wikipedia.org/w/api.php",
                params={
                    "action":  "query",
                    "list":    "search",
                    "srsearch": cand,
                    "srlimit":  1,
                    "format":  "json",
                },
                timeout=DEFAULT_TIMEOUT,
            )
            if search_res.status_code == 200:
                items = search_res.json().get("query", {}).get("search", [])
                if items:
                    page_title = items[0].get("title")
                    if page_title:
                        ll_res = requests.get(
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

            ja_res = requests.get(
                "https://ja.wikipedia.org/w/api.php",
                params={
                    "action":  "query",
                    "list":    "search",
                    "srsearch": cand,
                    "srlimit":  1,
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
        icon_url     = get_media_icon_url(media_label) if media_label else None
        if icon_url:
            api_request("patch", f"https://api.notion.com/v1/pages/{page_id}",
                        headers=NOTION_HEADERS,
                        json={"icon": {"type": "external", "external": {"url": icon_url}}})
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
                       relation_prop: str | None = None,
                       relation_ids: list[str] | None = None) -> bool:
    """Notionに新規ページを作成してポスター・メタデータも一括登録"""
    properties = {
        "タイトル":            {"title": [{"type": "text", "text": {"content": jp_title}}]},
        "International Title": {"rich_text": [{"type": "text", "text": {"content": en_title}, "annotations": {"italic": True}}]},
        "媒体":               {"multi_select": [{"name": media_type_label}]},
        **({"TMDB_ID": {"number": tmdb_id}} if tmdb_id else {}),
        "WLflg":              {"checkbox": wlflg},
    }
    if tmdb_release and str(tmdb_release)[:10]:
        release_date_str = str(tmdb_release)[:10]
        date_prop = {"start": release_date_str}
        if event_end:
            date_prop["end"] = str(event_end)[:10]
        properties["リリース日"] = {"date": date_prop}
    if watched_date:
        properties["鑑賞日"] = {"date": {"start": watched_date}}
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
        # 自己リレーションの片側が書き込み不可扱いでも反映されるように両名へセット
        if relation_prop == "出演履歴":
            properties["演奏曲"] = {"relation": [{"id": rid} for rid in rel_ids]}
        elif relation_prop == "演奏曲":
            properties["出演履歴"] = {"relation": [{"id": rid} for rid in rel_ids]}

    icon_url = get_media_icon_url(media_type_label)
    payload = {
        "parent":     {"database_id": NOTION_DB_ID},
        "icon":       {"type": "external", "external": {"url": icon_url}},
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

@st.cache_data(ttl=600)
def get_notion_db_property_types(database_id: str) -> dict:
    if not database_id:
        return {}
    res = api_request("get", f"https://api.notion.com/v1/databases/{database_id}", headers=NOTION_HEADERS)
    if res is None or res.status_code != 200:
        return {}
    props = (res.json() or {}).get("properties", {}) or {}
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
        text = str(value or "").strip()
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
        "get_notion_db_property_types": get_notion_db_property_types,
        "find_score_page_by_title": _find_score_page_by_title,
        "put_notion_prop": _put_notion_prop,
        "split_instruments": _split_instruments,
        "api_request": api_request,
        "NOTION_HEADERS": NOTION_HEADERS,
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
    watched = ((props.get("鑑賞日") or {}).get("date") or {}).get("start", "") or ""
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

st.set_page_config(page_title="ArtéMis", page_icon=get_asset_path_or_url("favicon.png"), layout="wide")

# ── PWA対応 metaタグ ──
st.markdown("""
<head>
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<meta name="apple-mobile-web-app-title" content="ArtéMis">
<meta name="theme-color" content="#0e1117">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<link rel="apple-touch-icon" href="https://raw.githubusercontent.com/attituderko-design/artemis-cers/main/assets/favicon.png">
</head>
""", unsafe_allow_html=True)

st.image(get_asset_path_or_url("logo.png"), width=320)
st.markdown(
    "<em><strong>ArtéMis</strong></em> — named after the goddess of the hunt and the moon. She keeps track of everything you've ever experienced.",
    unsafe_allow_html=True
)
st.caption(f"v{APP_VERSION}")
if is_drive_skip_mode():
    st.info("⏭ Driveデータスキップ機能ON: Drive保存/照合はスキップして動作中です。")
if "pending_notice" in st.session_state:
    st.success(st.session_state.pop("pending_notice"))
    emit_scroll_top_script()
if "pending_warning" in st.session_state:
    st.warning(st.session_state.pop("pending_warning"))
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
    "app_mode":            "新規登録",
    "app_mode_widget":     "新規登録",
    "reconcile_report":    None,
    "reconcile_repair_mode": "partial",
    "refresh_maintenance_enabled": True,
    "refresh_maintenance_mode": "partial",
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.header("操作パネル")
    with st.expander("📘 操作ガイド", expanded=False):
        guide_md = load_user_guide_markdown()
        if guide_md:
            st.markdown(guide_md)
        else:
            st.info("`docs/USER_GUIDE.md` が見つかりません。")
        st.markdown("[GitHubで見る](https://github.com/attituderko-design/artemis-cers/blob/main/docs/USER_GUIDE.md)")

    st.divider()
    st.toggle("Driveデータスキップ機能ON", key="drive_skip_mode")
    if st.session_state.get("drive_skip_mode"):
        st.caption("Drive連携はスキップ中です（判定/保存/一覧取得）。")
    if "auto_reload_mode" not in st.session_state:
        st.session_state.auto_reload_mode = "partial"
    current_label = (
        "手動" if st.session_state.auto_reload_mode == "manual"
        else "自動（全件）" if st.session_state.auto_reload_mode == "full"
        else "半自動（該当ページ）"
    )
    st.radio(
        "更新後の同期方式",
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
    if st.button("📥 Notionデータ取得", use_container_width=True, key="load_notion", type="primary"):
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

    if not st.session_state.pages_loaded:
        st.caption("👆 まずデータを取得してください")
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
        st.header("動作モード")
        if st.session_state.get("pending_focus_page_id"):
            st.session_state.app_mode = "データ管理"
            st.session_state.app_mode_widget = "データ管理"
        if "pending_app_mode" in st.session_state:
            st.session_state.app_mode = st.session_state.pop("pending_app_mode")
            st.session_state.app_mode_widget = st.session_state.app_mode
        if "app_mode_widget" not in st.session_state:
            st.session_state.app_mode_widget = st.session_state.get("app_mode", "新規登録")
        mode = st.radio("モード", ["新規登録", "データ管理", "出演者管理", "自動同期"], key="app_mode_widget")
        st.session_state.app_mode = mode
        sync_scope = "欠損のみ補填"  # legacy compat
        if mode == "データ管理":
            if "manual_sort_order" not in st.session_state:
                st.session_state.manual_sort_order = "鑑賞日（新しい順）"
            st.selectbox(
                "一覧ソート",
                options=[
                    "鑑賞日（新しい順）",
                    "鑑賞日（古い順）",
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
            st.toggle("リフレッシュ時に整合チェック修復を実行", key="refresh_maintenance_enabled")
            rm_label = (
                "手動（実行のみ）" if st.session_state.get("refresh_maintenance_mode") == "manual"
                else "自動（高確度＋重複整理）" if st.session_state.get("refresh_maintenance_mode") == "full"
                else "半自動（高確度のみ）"
            )
            st.radio(
                "整合修復モード",
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
            st.caption("IDを持つ媒体の欠損フィールドを補填します")
            if st.button("🔄 リフレッシュ", use_container_width=True):
                st.session_state.is_running = True
                st.session_state.sync_mode  = "refresh"
                st.session_state.refresh_cursor = 0
                st.session_state.refresh_targets_ids = []
                st.session_state.refresh_success_log = []
                st.session_state.refresh_maintain_log = []
                st.session_state.refresh_error_log = []
                st.rerun()
            st.caption("IDを基にすべてのフィールドを強制上書きします\nIDのないデータは情報の正規化のみ実施")
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
        CSV_COLUMNS = ["媒体", "タイトル", "英語タイトル", "鑑賞日", "評価", "メモ", "場所"]
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
                missing_cols = [c for c in CSV_COLUMNS if c not in df.columns]
                if missing_cols:
                    st.error(f"列が不足しています: {missing_cols}")
                else:
                    # ── バリデーション ──
                    errors, ok_rows = [], []
                    for i, row in df.iterrows():
                        row_num = i + 2  # ヘッダー行=1
                        media = row["媒体"].strip()
                        title = row["タイトル"].strip()
                        date  = row["鑑賞日"].strip()
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
                                errors.append(f"行{row_num}: 鑑賞日のフォーマットが不正「{date}」（YYYY-MM-DD）")
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
                                "date": "鑑賞日", "rating": "評価", "memo": "メモ", "location": "場所",
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
            event_title = st.text_input(
                "公演名 *",
                placeholder="例: 〇〇室内楽演奏会 / 定期演奏会" if is_performance else
                            "例: 大阪フィルハーモニー交響楽団 第588回定期演奏会" if is_concert else
                            "例: 〇〇 LIVE TOUR 2025",
                key="ev_title",
            )
            event_creator = st.text_input(
                "指揮者" if is_performance else ("アーティスト" if is_live else "クリエイター"),
                placeholder="例: 井上道義" if is_performance else
                            "例: Queen / 米津玄師" if is_live else
                            "例: 指揮者・キュレーターなど",
                key="ev_creator",
            )
            col_cast, col_genre = st.columns([1, 1])
            event_cast = col_cast.text_input(
                "演奏団体" if is_concert else "出演者・バンド",
                placeholder="例: 大阪フィルハーモニー交響楽団" if is_concert else "例: Queen",
                key="ev_cast",
            )
            event_genre = col_genre.text_input(
                "ジャンル",
                placeholder="例: クラシック / 室内楽" if is_concert else "例: ロック / J-POP",
                key="ev_genre",
            )
            if media_label == "展示会":
                col_start, col_end, col_watch = st.columns([1, 1, 1])
                event_start = col_start.date_input("開催開始日", value=None, key="ev_start")
                event_end   = col_end.date_input("開催終了日",   value=None, key="ev_end")
                event_watch = col_watch.date_input("鑑賞日",     value=None, key="ev_watch")
            else:
                col_watch2, _ = st.columns([1, 1])
                date_label_ev = "出演日" if is_performance else ("鑑賞日" if is_concert else "参加日")
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

                # ── 通常セットリスト ──
                render_song_list("ev_setlist_main", MAX_MAIN, f"📋 通常セットリスト（最大{MAX_MAIN}曲）")

                # ── アンコール ──
                render_song_list("ev_setlist_encore", MAX_ENCORE, f"🎊 アンコール（最大{MAX_ENCORE}曲）")

                # ── 楽曲検索（出演はクラシック/ポピュラーを切替可）──
                st.divider()
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

                if use_mb:
                    st.caption("🔍 楽曲検索（MusicBrainz）")
                    st.caption("1) 作曲家を検索 → 2) 作曲家を確定 → 3) 曲名で検索 → 4) 曲を追加")
                    ev_composer_input = st.text_input(
                        "1. 作曲家を検索",
                        placeholder="例: Beethoven / ベートーヴェン",
                        key="ev_composer",
                    )
                    if st.button("🔍 作曲家を検索", key="ev_mb_search"):
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
                        ev_comp_labels = [
                            f"{c['name']}"
                            + (f"（{c['disambiguation']}）" if c.get('disambiguation') else "")
                            + (f" [{c['life_span']}–]" if c.get('life_span') else "")
                            for c in ev_composers
                        ]
                        ev_sel_idx = st.radio("2. 作曲家を特定", range(len(ev_comp_labels)), format_func=lambda i: ev_comp_labels[i], key="ev_mb_comp_radio")
                        if st.button("✅ この作曲家で進める", key="ev_mb_pick_comp"):
                            st.session_state.ev_mb_selected_comp = ev_composers[ev_sel_idx]
                            st.session_state.ev_mb_works = []
                            st.rerun()
                        ev_selected_comp = st.session_state.get("ev_mb_selected_comp")
                        if ev_selected_comp:
                            st.success(f"作曲家を確定: {ev_selected_comp.get('name', '')}")

                    if ev_selected_comp:
                        ev_title_filter = st.text_input("3. 検索ワード（曲名）", placeholder="例: Symphony No.5", key="ev_title_filter")
                        c_mb1, c_mb2 = st.columns([1, 1])
                        if c_mb1.button("🔍 曲名で検索", key="ev_mb_fetch_works"):
                            if not ev_title_filter.strip():
                                st.warning("曲名を入力してください。")
                            else:
                                with st.spinner(f"{ev_selected_comp['name']} の作品を検索中..."):
                                    ev_works = search_mb_works(ev_selected_comp["id"], ev_title_filter.strip())
                                st.session_state.ev_mb_works = ev_works
                        if c_mb2.button("📚 全作品を取得（重い）", key="ev_mb_fetch_all"):
                            with st.spinner(f"{ev_selected_comp['name']} の全作品を取得中..."):
                                ev_works = search_mb_works(ev_selected_comp["id"], "")
                            st.session_state.ev_mb_works = ev_works

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

                if use_itunes:
                    st.caption("🔍 楽曲検索（iTunes）")
                    col_it_art, col_it_title = st.columns([1, 1])
                    it_artist_input = col_it_art.text_input("アーティスト名", placeholder="例: Queen / 米津玄師", key="ev_it_artist")
                    it_title_input  = col_it_title.text_input("曲名", placeholder="例: Bohemian Rhapsody", key="ev_it_title")

                    if st.button("🔍 曲を検索", key="ev_it_search"):
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
                score_query = st.text_input("演奏曲名で検索", key="ev_score_query", placeholder="例: 交響曲第5番")
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
                        if st.button("＋ 追加", key="ev_score_add"):
                            add_selected_score(picked["id"], picked["title"])
                            st.rerun()
                elif score_query:
                    st.caption("候補が見つかりませんでした。")
                    if st.button("＋ 新規作成して追加", key="ev_score_create"):
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
                                    item["release"] = d_release or ""
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
                            cols = st.columns([2, 1, 2, 2, 1, 1])
                            item["jp_title"] = cols[0].text_input("日本語タイトル", value=item["jp_title"], key=f"cart_jp_{item_uid}")
                            item["release"]  = cols[1].text_input("リリース日", value=item.get("release", ""), key=f"cart_rel_{item_uid}")
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

                    for i in sorted(remove_indices, reverse=True):
                        st.session_state.reg_cart.pop(i)
                    if remove_indices:
                        st.rerun()

                    col_reg, col_clear = st.columns([2, 1])
                    with col_reg:
                        if st.button(f"{len(st.session_state.reg_cart)} 件を一括登録", type="primary", key="bulk_register_score"):
                            if not st.session_state.pages_loaded:
                                with st.spinner("Notionデータ取得中..."):
                                    all_pages = load_notion_data()
                                    st.session_state.pages = filter_target_pages(all_pages)
                                    st.session_state.pages_loaded = True
                            success_count = 0
                            prog = st.progress(0)
                            fallback_perf_ids = _clean_relation_ids(st.session_state.get("score_perf_selected_ids", []))
                            for n, item in enumerate(st.session_state.reg_cart):
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
                                    location=item.get("location"),
                                    relation_prop=rel_prop,
                                    relation_ids=rel_ids,
                                )
                                if ok:
                                    if item.get("media_type") == "score" and rel_ids:
                                        created_id = st.session_state.get("last_created_page_id")
                                        if created_id:
                                            rel_patch = {"relation": [{"id": rid} for rid in rel_ids]}
                                            rel_res = api_request(
                                                "patch",
                                                f"https://api.notion.com/v1/pages/{created_id}",
                                                headers=NOTION_HEADERS,
                                                json={"properties": {"出演履歴": rel_patch, "演奏曲": rel_patch}},
                                            )
                                            if rel_res is None or rel_res.status_code != 200:
                                                st.warning(f"関連付け追記に失敗: {rel_res.status_code if rel_res else 'None'}")
                                    success_count += 1
                                prog.progress((n + 1) / len(st.session_state.reg_cart))
                                time.sleep(0.2)
                            for key in ["reg_cart", "mb_works", "mb_checked", "mb_composers"]:
                                st.session_state.pop(key, None)
                            st.success(f"{success_count} 件登録完了")
                            reset_new_register_state()
                            if st.session_state.get("auto_reload_mode") == "partial":
                                for p in st.session_state.get("created_pages", []):
                                    upsert_page_in_state(p)
                                st.session_state.created_pages = []
                            else:
                                sync_notion_after_update()
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

            composer_input = st.text_input(
                "1. 作曲家を検索",
                key="mb_composer_query",
                placeholder="例: Beethoven / ベートーヴェン",
            )
            if st.button("🔍 作曲家を検索", key="mb_composer_search"):
                if composer_input.strip():
                    with st.spinner("作曲家を検索中..."):
                        composers, err = search_mb_composer(composer_input.strip())
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
                comp_labels = [
                    f"{c['name']}"
                    + (f"（{c['disambiguation']}）" if c['disambiguation'] else "")
                    + (f" [{c['life_span']}–]" if c['life_span'] else "")
                    for c in composers
                ]
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
                    st.success(f"作曲家を確定: {selected_comp.get('name', '')}")

            if selected_comp:
                work_title_filter = st.text_input(
                    "3. 検索ワード（曲名）",
                    key="mb_work_title_filter",
                    placeholder="例: Symphony no.5 / Piano Concerto",
                )
                col_work_search, col_work_all = st.columns([1, 1])
                if col_work_search.button("🔍 曲名で検索", key="mb_fetch_works"):
                    if not work_title_filter.strip():
                        st.warning("曲名の検索ワードを入力してください。")
                    else:
                        st.session_state.mb_title_filter = work_title_filter.strip()
                        with st.spinner(f"{selected_comp['name']} の作品を検索中..."):
                            works = search_mb_works(selected_comp["id"], work_title_filter.strip())
                        st.session_state.mb_works = works
                        st.session_state.mb_checked = {}
                if col_work_all.button("📚 全作品を取得（重い）", key="mb_fetch_works_all"):
                    st.session_state.mb_title_filter = ""
                    with st.spinner(f"{selected_comp['name']} の全作品を取得中...（数分かかることがあります）"):
                        works = search_mb_works(selected_comp["id"], "")
                    st.session_state.mb_works = works
                    st.session_state.mb_checked = {}

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
                    comp_name  = comp.get("name", "")
                    artist_id  = comp.get("id", "")
                    if "mb_portrait_url" not in st.session_state or st.session_state.get("mb_portrait_comp") != artist_id:
                        with st.spinner(f"🖼️ {comp_name} の肖像画を取得中..."):
                            portrait_url = get_composer_portrait_url(comp_name, artist_id)
                        st.session_state.mb_portrait_url  = portrait_url
                        st.session_state.mb_portrait_comp = artist_id
                    else:
                        portrait_url = st.session_state.mb_portrait_url

                    if portrait_url:
                        if portrait_url.startswith("https://drive.google.com"):
                            try:
                                img_res = requests.get(portrait_url, timeout=8)
                                st.image(io.BytesIO(img_res.content), width=120, caption=comp_name)
                            except Exception:
                                st.image(portrait_url, width=120, caption=comp_name)
                        else:
                            st.image(portrait_url, width=120, caption=comp_name)
                        cover_url_final = portrait_url
                    else:
                        st.warning(f"⚠️ {comp_name} の肖像画が見つかりませんでした。画像をアップロードしてください。")
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
                            save_fname = f"portrait_{custom_fname}.jpg"
                            img_bytes = uploaded.read()
                            mimetype  = "image/png" if uploaded.name.endswith(".png") else "image/jpeg"
                            with st.spinner("Driveに保存中..."):
                                service = get_drive_service_safe()
                                media   = MediaIoBaseUpload(io.BytesIO(img_bytes), mimetype=mimetype, resumable=False)
                                result  = service.files().create(
                                    body={"name": save_fname, "parents": [DRIVE_FOLDER_ID]},
                                    media_body=media, fields="id",
                                ).execute()
                                file_id = result["id"]
                                st.session_state.drive_files_cache[save_fname] = file_id
                                service.permissions().create(fileId=file_id, body={"role": "reader", "type": "anyone"}).execute()
                                cover_url_final = f"https://drive.google.com/uc?id={file_id}"
                                if custom_fname == default_fname:
                                    st.session_state.mb_portrait_url  = cover_url_final
                                    st.session_state.mb_portrait_comp = artist_id
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
                            if st.button("＋ 追加", key="score_perf_add"):
                                add_selected_perf(picked["id"], picked["title"])
                                st.rerun()
                    elif perf_query:
                        st.caption("候補が見つかりませんでした。")
                    if perf_query:
                        if st.button("＋ 新規作成して追加", key="score_perf_create"):
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

                    if st.button(f"📋 {len(selected_works)} 件を登録リストに追加", key="mb_add_cart"):
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
                        if selected_perf_ids:
                            perf_page = _get_page_from_state_or_api(selected_perf_ids[0])
                            perf_release, perf_watched, perf_rating, perf_location = _extract_performance_defaults(perf_page)
                        for w in selected_works:
                            st.session_state.reg_cart.append({
                                "cart_uid":    f"score_{uuid.uuid4().hex[:10]}",
                                "jp_title":    w["title"],
                                "en_title":    w["title"],
                                "cover_url":   cover_url_final,
                                "release":     perf_release or "",
                                "watched":     perf_watched or "",
                                "rating":      perf_rating or "",
                                "wlflg":       False,
                                "media_type":  "score",
                                "tmdb_id":     0,
                                "details":     {"genres": [], "cast": "", "director": comp_name, "score": None},
                                "isbn":        "",
                                "location":    perf_location,
                                "media_label": media_label,
                                "relation_prop": "出演履歴" if selected_perf_ids else None,
                                "relation_ids":  selected_perf_ids,
                            })
                        st.session_state.mb_checked = {}
                        st.success(f"✅ {len(selected_works)} 件を登録リストに追加しました")
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

        if active_tab == "検索":
            reg_cart_hint = st.session_state.get("reg_cart", [])
            if reg_cart_hint:
                st.info(f"🧺 登録リストに {len(reg_cart_hint)} 件あります。右の「登録リスト」タブで確認できます。")

            fast_book_search = True
            if media_label in ("書籍", "漫画"):
                fast_book_search = st.checkbox("高速検索（カバー簡易）", value=True, key="fast_book_search")

            if media_label in ["音楽アルバム"]:
                col_jp, col_en = st.columns([1, 1])
                jp_input      = clearable_text_input("アルバム名", "inp_jp_album", placeholder="例: 千のナイフ", container=col_jp)
                creator_input = clearable_text_input("アーティスト名", "inp_creator_album", placeholder="例: 坂本龍一", container=col_en)
                cast_input    = ""
                en_input      = ""
            elif media_label == "ゲーム":
                jp_input      = clearable_text_input("ゲームタイトル", "inp_jp_game", placeholder="例: ゼルダの伝説")
                creator_input = ""
                cast_input    = ""
                en_input      = jp_input
            elif media_label == "アニメ":
                jp_input      = clearable_text_input("アニメタイトル", "inp_jp_anime", placeholder="例: 鬼滅の刃 / Demon Slayer")
                creator_input = ""
                cast_input    = ""
                en_input      = jp_input
            elif media_label == "漫画":
                col_jp, col_en = st.columns([1, 1])
                jp_input      = clearable_text_input("タイトル", "inp_jp_manga", placeholder="例: 鬼滅の刃", container=col_jp)
                creator_input = clearable_text_input("著者名", "inp_creator_manga", placeholder="例: 吾峠呼世晴", container=col_en)
                cast_input    = ""
                en_input      = ""
            else:
                col_jp, col_en = st.columns([1, 1])
                jp_input      = clearable_text_input("日本語タイトル", "inp_jp_main", placeholder="例: 千と千尋の神隠し", container=col_jp)
                en_input      = clearable_text_input("英語タイトル（検索用）", "inp_en_main", placeholder="例: Spirited Away", container=col_en)
                col_creator, col_cast = st.columns([1, 1])
                creator_input = clearable_text_input("クリエイター（著者・監督）", "inp_creator_main", placeholder="例: 宮崎駿 / 道尾秀介", container=col_creator)
                cast_input    = clearable_text_input("キャスト・関係者", "inp_cast_main", placeholder="例: 木村拓哉", container=col_cast)

            if st.button("🔍 検索", key="new_search"):
                query = en_input if en_input else jp_input
                if query or creator_input or cast_input:
                    if media_label == "書籍":
                        results = search_books(query or "", author=creator_input or None, fast=fast_book_search)
                    elif media_label == "漫画":
                        results = search_manga(query or "", author=creator_input or None, fast=fast_book_search)
                    elif media_label == "音楽アルバム":
                        results = search_albums(query or "", artist=creator_input or None)
                    elif media_label == "ゲーム":
                        results = search_games(query or jp_input)
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
                    st.session_state.new_search_results  = filtered[:20]
                    st.session_state.new_search_excluded = excluded
                    st.session_state.new_search_done     = True
                    st.session_state.confirm_reg         = None
                    st.session_state.bulk_checked        = {}
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
                                new_jp = search_wikipedia_jp_title(title)

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
    
                    date_label   = {"ゲーム": "クリア日", "音楽アルバム": "聴いた日", "書籍": "読了日", "漫画": "読了日", "アニメ": "視聴日"}.get(media_label, "鑑賞日")
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
                    st.caption("検索すると、ここに候補が表示されます。")
                    results_list = []
    
                st.caption(f"{len(results_list)} 件の候補　チェックして登録リストに追加できます")
                if excluded_list:
                    st.caption(f"⚠️ {len(excluded_list)} 件は登録済みのため除外")
                    with st.expander("除外されたタイトルを表示"):
                        for t in excluded_list:
                            st.caption(f"・{t}")
    
                if results_list:
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
                                    display_title = cand["title"]
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
                                            "cand_en": cand["title"], "jp_input": cand["title"],
                                            "book_authors": [cand.get("developer", "")], "book_genres": cand.get("genres", []),
                                            "isbn": "", "game_publisher": cand.get("publisher", ""),
                                            "igdb_id": cand.get("id"),
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
                                "jp_title":   cand["title"], "en_title": "",
                                "cover_url":  cand["cover_url"], "release": cand.get("release", ""),
                                "watched": "", "rating": "", "wlflg": False,
                                "media_type": "album", "tmdb_id": 0,
                                "details":    {"genres": [], "cast": "", "director": clean_author(cand.get("artist", "")), "score": None},
                                "isbn":       "", "itunes_id": cand.get("id"),
                                "location":   None, "media_label": media_label,
                            }
                        elif media_label == "ゲーム":
                            cart_item = {
                                "jp_title":   cand["title"], "en_title": cand["title"],
                                "cover_url":  cand["cover_url"], "release": cand.get("release", ""),
                                "watched": "", "rating": "", "wlflg": False,
                                "media_type": "game", "tmdb_id": 0,
                                "details":    {"genres": cand.get("genres", []), "cast": cand.get("publisher", ""), "director": clean_author(cand.get("developer", "")), "score": None},
                                "isbn":       "", "igdb_id": cand.get("id"),
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
                date_label = {"ゲーム": "クリア日", "音楽アルバム": "聴いた日", "書籍": "読了日", "漫画": "読了日", "演奏曲": "演奏日", "アニメ": "視聴日"}.get(media_label, "鑑賞日")
    
                remove_indices = []
                for idx, item in enumerate(reg_cart):
                    item_media = item.get("media_label", media_label)
                    with st.expander(f"{idx+1}. {item['jp_title']}", expanded=True):
                        cols = st.columns([2, 1, 2, 2, 1, 1])
                        item["jp_title"] = cols[0].text_input("日本語タイトル", value=item["jp_title"], key=f"cart_jp_{idx}")
                        item["release"]  = cols[1].text_input("リリース日", value=item.get("release",""), key=f"cart_rel_{idx}")
                        date_val = None
                        if item.get("watched"):
                            try: date_val = date.fromisoformat(item["watched"])
                            except: pass
                        item_date_label  = {"ゲーム": "クリア日", "音楽アルバム": "聴いた日", "書籍": "読了日", "漫画": "読了日", "演奏曲": "演奏日", "アニメ": "視聴日"}.get(item_media, "鑑賞日")
                        watched_input    = cols[2].date_input(item_date_label, value=date_val, key=f"cart_watch_{idx}")
                        item["watched"]  = watched_input.isoformat() if watched_input else ""
                        item["rating"]   = cols[3].selectbox("評価", RATING_OPTIONS, index=RATING_OPTIONS.index(item.get("rating","")) if item.get("rating","") in RATING_OPTIONS else 0, key=f"cart_rating_{idx}")
                        item["wlflg"]    = cols[4].checkbox("WL", value=item.get("wlflg", False), key=f"cart_wl_{idx}")
                        if cols[5].button("🗑", key=f"cart_del_{idx}"):
                            remove_indices.append(idx)
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
                                if item["media_type"] in ("movie", "tv"):
                                    save_to_drive(item["cover_url"], item["jp_title"] or item.get("en_title",""), item["tmdb_id"])
                                success_count += 1
                            prog.progress((n + 1) / len(st.session_state.reg_cart))
                            time.sleep(0.3)
                        for key in ["reg_cart", "new_search_results", "new_search_done",
                                    "bulk_checked", "album_tracks_cache", "album_tracks_id"]:
                            st.session_state.pop(key, None)
                        st.success(f"✅ {success_count} 件登録完了！")
                        reset_new_register_state()
                        if st.session_state.get("auto_reload_mode") == "partial":
                            for p in st.session_state.get("created_pages", []):
                                upsert_page_in_state(p)
                            st.session_state.created_pages = []
                        else:
                            sync_notion_after_update()
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
        sort_mode = st.session_state.get("manual_sort_order", "鑑賞日（新しい順）")
        def _d(page, prop):
            return (((page.get("properties", {}).get(prop) or {}).get("date") or {}).get("start") or "")
        def _t(page):
            return (get_title(page.get("properties", {}))[0] or "").lower()
        if sort_mode == "鑑賞日（新しい順）":
            base = sorted(base, key=lambda p: (_d(p, "鑑賞日"), _t(p)), reverse=True)
        elif sort_mode == "鑑賞日（古い順）":
            base = sorted(base, key=lambda p: (_d(p, "鑑賞日"), _t(p)))
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
# 出演者管理モード
# ============================================================
if mode == "出演者管理":
    st.subheader("👥 出演者管理")

    with st.expander("🧪 演奏会リンク整合チェック（演奏会単位サマリ）", expanded=False):
        st.caption("人数が多い運用向けに、明細ではなく演奏会単位の件数サマリで表示します。")
        if st.button("🔍 整合チェックを実行", key="reconcile_run"):
            with st.spinner("整合チェック実行中..."):
                st.session_state.reconcile_report = analyze_performance_relation_integrity(force_refresh=False)
        report = st.session_state.get("reconcile_report")
        if report:
            if report.get("error"):
                st.error(report.get("error"))
            else:
                totals = report.get("totals", {})
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("対象演奏会", totals.get("performance_count", 0))
                c2.metric("要確認演奏会", totals.get("issue_performance_count", 0))
                c3.metric("出演者欠損", totals.get("cast_missing_performer", 0))
                c4.metric("重複候補", totals.get("duplicate_archive_candidates", 0))
                st.caption(
                    f"高確度修復候補: 出演者補完 {totals.get('fixable_cast_missing_performer', 0)} 件 / "
                    f"楽曲別担当者補完 {totals.get('fixable_assign_missing_cast', 0)} 件"
                )
                if totals.get("assign_missing_score_unresolved", 0) > 0:
                    st.caption(f"未解決の楽曲別担当者（演奏曲リンク欠損）: {totals.get('assign_missing_score_unresolved', 0)} 件")
                mode_label = st.radio(
                    "修復モード",
                    ["手動", "半自動（高確度のみ）", "自動（高確度＋重複整理）"],
                    index=["手動", "半自動（高確度のみ）", "自動（高確度＋重複整理）"].index(
                        "手動" if st.session_state.get("reconcile_repair_mode") == "manual"
                        else "自動（高確度＋重複整理）" if st.session_state.get("reconcile_repair_mode") == "full"
                        else "半自動（高確度のみ）"
                    ),
                    key="reconcile_repair_mode_display",
                )
                if mode_label == "手動":
                    st.session_state.reconcile_repair_mode = "manual"
                elif mode_label == "自動（高確度＋重複整理）":
                    st.session_state.reconcile_repair_mode = "full"
                else:
                    st.session_state.reconcile_repair_mode = "partial"
                if st.button("🛠 高確度修復を実行", key="reconcile_apply"):
                    with st.spinner("修復実行中..."):
                        stats, errs = run_performance_relation_repair(
                            report,
                            mode=st.session_state.get("reconcile_repair_mode", "partial"),
                        )
                    st.success(
                        "✅ 修復完了: "
                        f"出演者補完 {stats.get('cast_missing_performer_fixed', 0)} 件 / "
                        f"楽曲別担当者補完 {stats.get('assign_missing_cast_fixed', 0)} 件 / "
                        f"重複整理 {stats.get('duplicates_archived', 0)} 件"
                    )
                    if stats.get("failed", 0) > 0:
                        st.warning(f"⚠️ 一部失敗: {stats.get('failed', 0)} 件")
                    if errs:
                        st.caption("失敗例（先頭10件）")
                        for e in errs[:10]:
                            st.write(f"- {e}")
                    with st.spinner("再チェック中..."):
                        st.session_state.reconcile_report = analyze_performance_relation_integrity(force_refresh=False)

                qsum = clearable_text_input("要確認演奏会を検索", "reconcile_filter_query", placeholder="例: 第1回演奏会")
                rows = report.get("rows", [])
                rows = [r for r in rows if r.get("issue_count", 0) > 0]
                if qsum.strip():
                    ql = qsum.strip().lower()
                    rows = [r for r in rows if ql in (r.get("title") or "").lower()]
                if not rows:
                    st.info("要確認の演奏会はありません。")
                else:
                    st.caption(f"要確認: {len(rows)} 件（最大100件表示）")
                    for r in rows[:100]:
                        st.write(
                            f"- {r.get('title','(無題)')}  | 出演者 {r.get('cast_total',0)}件 / "
                            f"欠損 {r.get('cast_missing_performer',0)} / 重複 {r.get('cast_duplicates',0)} / "
                            f"楽曲別担当者欠損 {r.get('assign_missing_cast',0)}"
                        )

    col_reload_perf, col_reload_master = st.columns([1, 1])
    if col_reload_perf.button("🔄 出演一覧を再読込", key="cast_mode_reload_perf"):
        st.session_state.pop("performance_pages_cache", None)
    if col_reload_master.button("🔄 演奏者マスタ再読込", key="cast_mode_reload_master"):
        st.session_state.pop("cast_mode_master_names_cache", None)
    perf_pages = _get_performance_pages(force_refresh=False)
    if not perf_pages:
        st.info("出演データが見つかりません。先にNotionデータ取得を実行してください。")
        st.stop()

    q = clearable_text_input("出演を検索", "cast_mode_perf_query", placeholder="例: 第1回演奏会")
    matches = [p for p in perf_pages if q.strip().lower() in (p.get("title") or "").lower()] if q.strip() else perf_pages[:200]
    options = ["（選択してください）"] + [p["title"] for p in matches]
    sel = st.selectbox("出演を選択", options, key="cast_mode_perf_pick")
    selected_perf = matches[options.index(sel) - 1] if sel != "（選択してください）" else None

    if st.button("🔄 出演者DB→演奏者マスタ同期", key="cast_mode_sync_master"):
        with st.spinner("同期中..."):
            c, s, e = sync_performer_master_from_performer_db()
        if e:
            st.warning(e)
        else:
            st.success(f"✅ 追加 {c} 件 / 既存 {s} 件")

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
        if st.button("既定参加者をクリア", key="cast_mode_clear"):
            st.session_state.cast_mode_participants = []
            st.rerun()

        default_self = (DEFAULT_PERFORMER_NAME or "").strip()
        if default_self and not any(
            _normalize_person_name(x.get("name", "")) == _normalize_person_name(default_self)
            for x in st.session_state.cast_mode_participants
        ):
            st.session_state.cast_mode_participants.insert(0, {"name": default_self, "instruments": "", "memo": ""})

        with st.expander("📄 CSVで一括登録", expanded=False):
            perf_title = selected_perf.get("title", "")
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

        with st.expander("参加者を追加", expanded=True):
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
            if c4.button("＋追加", key="cast_mode_add"):
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

        if st.button("📥 この出演に参加者を登録", type="primary", key="cast_mode_submit"):
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
                c, f, msg, _ = create_performance_participant_rows(
                    performance_page_id=selected_perf["id"],
                    performance_title=selected_perf.get("title", ""),
                    participants=st.session_state.cast_mode_participants,
                )
            if c > 0 and f == 0:
                st.success(f"✅ 演奏会参加者DBに {c} 件登録しました")
            elif c > 0 and f > 0:
                st.warning(f"⚠️ 成功 {c} 件 / 失敗 {f} 件")
            elif f > 0:
                st.error(f"❌ 失敗 {f} 件")
            elif msg:
                st.info(msg)
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
                icon_url        = get_media_icon_url(media_label_val) if media_label_val else None
                patch_body      = {}
                if icon_url:
                    patch_body["icon"] = {"type": "external", "external": {"url": icon_url}}

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
            status.update(label=f"{label_mode}中... {count} / {total_count} 件", state="running")
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
    if success_log:
        with st.expander(f"✅ 更新成功 （{len(success_log)} 件）", expanded=False):
            for msg in success_log:
                st.write(msg)
    if maintain_log:
        with st.expander(f"⏸️ 維持 （{len(maintain_log)} 件）", expanded=False):
            for msg in maintain_log:
                st.write(msg)
    if error_log:
        with st.expander(f"❌ 失敗・要確認 （{len(error_log)} 件）", expanded=True):
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
            if st.session_state.get("refresh_maintenance_enabled", True):
                with st.spinner("整合チェック修復を実行中..."):
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
                            st.session_state.pending_warning = "整合修復で一部失敗があります（出演者管理の整合チェックで要確認）"
            st.session_state.is_running = False
            st.session_state.refresh_targets_ids = []
    else:
        st.session_state.is_running = False

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
                existing_date_start = ((props.get("鑑賞日") or {}).get("date") or {}).get("start", "") or ""
                edit_col1, edit_col2, edit_col3 = st.columns([1.5, 3, 1.2])
                new_rating = edit_col1.selectbox(
                    "評価", RATING_OPTIONS,
                    index=RATING_OPTIONS.index(existing_rating) if existing_rating in RATING_OPTIONS else 0,
                    key=f"edit_rating_{page_id}",
                )
                new_memo   = edit_col2.text_input("メモ", value=existing_memo, key=f"edit_memo_{page_id}")
                new_date   = edit_col3.text_input("鑑賞日", value=existing_date_start, placeholder="YYYY-MM-DD", key=f"edit_date_{page_id}")

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
                        labels = [
                            f"{c['name']}" + (f"（{c['disambiguation']}）" if c.get("disambiguation") else "")
                            for c in composers
                        ]
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
                                composer_name = composers[idx].get("name", "")
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
                        patch_props["鑑賞日"] = {"date": {"start": new_date}}
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
                            labels = [
                                f"{c['name']}" + (f"（{c['disambiguation']}）" if c.get("disambiguation") else "")
                                for c in comps
                            ]
                            idx = st.selectbox("作曲家候補", list(range(len(labels))), format_func=lambda i: labels[i], key=f"edit_rel_mb_pick_{page_id}")
                            if st.button("✅ この作曲家で進める", key=f"edit_rel_mb_pick_btn_{page_id}"):
                                st.session_state[comp_sel_key] = comps[idx]
                                st.session_state[work_list_key] = []
                                st.session_state.focus_page_id = page_id
                                st.rerun()
                            selected_comp = st.session_state.get(comp_sel_key)
                        if selected_comp:
                            st.success(f"作曲家を確定: {selected_comp.get('name','')}")
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
                                if c_b.button("＋追加", key=f"edit_rel_mb_add_{page_id}_{i}"):
                                    st.session_state.focus_page_id = page_id
                                    creator_name = (selected_comp or {}).get("name", "")
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
                            if c_b.button("＋追加", key=f"edit_rel_it_add_{page_id}_{i}"):
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
                        if st.button("＋ 追加", key=f"edit_rel_add_{page_id}"):
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
                    if st.button("＋ 新規作成して追加", key=f"edit_rel_create_{page_id}"):
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
                        icon_url = get_media_icon_url(new_media)
                        if icon_url:
                            patch_icon = {"type": "external", "external": {"url": icon_url}}
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
                                public_url = f"https://drive.google.com/uc?id={file_id}"
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



