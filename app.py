import re
import requests
import time
import streamlit as st
from datetime import date, datetime
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

# ============================================================
# 設定（secrets.toml から読み込み）
# ============================================================
NOTION_API_KEY  = st.secrets["NOTION_API_KEY"]
NOTION_DB_ID    = st.secrets["NOTION_DB_ID"]
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

# ============================================================
# 媒体マッピング
# ============================================================
MEDIA_ICON_MAP = {
    "映画":          ("🎬 映画",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/camera-reels.svg"),
    "ドラマ":        ("📺 ドラマ",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/display.svg"),
    "演奏会（鑑賞）": ("🎼 演奏会（鑑賞）", "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/music-note-beamed.svg"),
    "演奏会（出演）": ("🎻 演奏会（出演）", "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/music-note-list.svg"),
    "展示会":        ("🖼️ 展示会",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/exhibition.svg"),
    "ライブ/ショー": ("🎤 ライブ/ショー", "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/mic.svg"),
    "書籍":          ("📖 書籍",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/book.svg"),
    "漫画":          ("📚 漫画",          "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/book-manga.svg"),
    "音楽アルバム":  ("🎵 音楽アルバム",  "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/disc.svg"),
    "ゲーム":        ("🎮 ゲーム",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/controller.svg"),
    "演奏曲":        ("🎼 演奏曲",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/music-score.svg"),
    "アニメ":        ("🌟 アニメ",        "https://raw.githubusercontent.com/attituderko-design/artemis-cers/refs/heads/main/assets/icons/anime.svg"),
}

RATING_OPTIONS = ["", "★", "★★", "★★★", "★★★★", "★★★★★"]

def get_media_icon_url(media_label: str) -> str:
    return MEDIA_ICON_MAP.get(media_label, ("", ""))[1]

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
    return build("drive", "v3", credentials=creds)

def get_drive_service_safe():
    """トークン期限切れ時にキャッシュをクリアして再取得するラッパー"""
    try:
        service = get_drive_service()
        service.about().get(fields="user").execute()
        return service
    except Exception:
        get_drive_service.clear()
        try:
            return get_drive_service()
        except Exception as e:
            st.error(f"Google Drive 認証エラー: {e}")
            return None

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
    isbn_val = "".join(t["plain_text"] for t in (props.get("ISBN") or {}).get("rich_text", []))
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

def save_bytes_to_drive(filename: str, image_bytes: bytes, mimetype: str) -> str | None:
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
    return file_id

def save_cover_to_drive_noid(cover_url: str, title: str, page_id: str) -> str | None:
    if not cover_url or cover_url.startswith("https://drive.google.com"):
        return None
    image_bytes, mimetype = fetch_image_bytes(cover_url)
    if not image_bytes or not mimetype:
        return None
    filename = make_noid_filename(title, page_id)
    return save_bytes_to_drive(filename, image_bytes, mimetype)

def clearable_text_input(label: str, key: str, placeholder: str = "", value: str = "", container=None, **kwargs) -> str:
    """× ボタン付き text_input。セッションステートで値を管理する。"""
    ss_key = f"_cti_{key}"
    if ss_key not in st.session_state:
        st.session_state[ss_key] = value
    # 外部から value が明示的に渡された場合（初期値設定）は上書きしない
    inp_col, btn_col = (container or st).columns([10, 1])
    val = inp_col.text_input(label, value=st.session_state[ss_key],
                             placeholder=placeholder, key=key, **kwargs)
    st.session_state[ss_key] = val
    if btn_col.button("×", key=f"_clr_{key}", help="クリア"):
        st.session_state[ss_key] = ""
        st.rerun()
    return st.session_state[ss_key]

def clean_author(name: str) -> str:
    """著者名クリーニング: 接尾語除去 + スペース正規化（半角スペース1個に統一）"""
    name = re.sub(r'[（(][^）)]*[）)]', '', name)           # 括弧内除去
    name = re.sub(r'\s*(著|訳|編|著者|監修|イラスト)$', '', name)  # 接尾語除去
    name = re.sub(r'[\s\u3000]+', ' ', name).strip()        # 全角スペース含む連続スペースを半角1個に
    return name

def clean_author_list(authors: list) -> str:
    """著者リストをクリーニングして ' / ' 結合"""
    return " / ".join(clean_author(a) for a in authors if a.strip())

def make_filename(title: str, tmdb_id) -> str:
    return f"{sanitize_filename(title)}_{tmdb_id}.jpg"

def get_title(props):
    jp = "".join([t["plain_text"] for t in props.get("タイトル", {}).get("title", [])])
    en = "".join([t["plain_text"] for t in props.get("International Title", {}).get("rich_text", [])])
    return (jp if jp else en), jp, en

def get_season_number(props) -> int | None:
    en = "".join([t["plain_text"] for t in props.get("International Title", {}).get("rich_text", [])])
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
    if "drive_files_cache" not in st.session_state or not isinstance(st.session_state.drive_files_cache, dict):
        refresh_drive_files()
    return st.session_state.drive_files_cache

def refresh_drive_files():
    service = get_drive_service_safe()
    if service is None:
        st.session_state.drive_files_cache = {}
        return
    try:
        results = service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
            fields="files(id, name)",
            pageSize=1000,
        ).execute()
        st.session_state.drive_files_cache = {f["name"]: f["id"] for f in results.get("files", [])}
    except Exception as e:
        st.warning(f"Drive一覧取得失敗: {e}")
        st.session_state.drive_files_cache = {}

def drive_exists(title: str, tmdb_id) -> bool:
    return make_filename(title, tmdb_id) in get_drive_files()

def drive_exists_fuzzy(title: str) -> bool:
    prefix = sanitize_filename(title) + "_"
    return any(name.startswith(prefix) and name.endswith(".jpg") for name in get_drive_files())

def save_to_drive(cover_url: str, title: str, tmdb_id, image_bytes: bytes | None = None, mimetype: str = "image/jpeg") -> str | None:
    """Drive保存成功時はfile_idを返す、失敗時はNone"""
    try:
        if image_bytes is None:
            if not cover_url:
                return None
            image_bytes, fetched_mime = fetch_image_bytes(cover_url)
            if image_bytes is None:
                return None
            mimetype = fetched_mime or "image/jpeg"
        fname = make_filename(title, tmdb_id)
        return save_bytes_to_drive(fname, image_bytes, mimetype)
    except Exception as e:
        st.warning(f"Drive保存失敗 ({title}): {e}")
        return None

def get_drive_public_url(title: str, tmdb_id) -> str | None:
    """Drive上のファイルIDから公開URLを返す"""
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
    tmdb_id   = st.session_state.get("tmdb_id_cache", {}).get(item["id"])
    drive_ok  = drive_exists(log_title, tmdb_id) if tmdb_id else drive_exists_fuzzy(log_title)
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
    badge = ("🟢Notion" if notion_ok else "🔴Notion") + " " + ("🟢Drive" if drive_ok else "🔴Drive")
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

# ユニークキーを持つ媒体（自動補填対象）
UNIQUE_KEY_MEDIA = {"映画", "ドラマ", "アニメ", "書籍", "漫画", "音楽アルバム", "ゲーム"}

def get_page_media(page) -> str | None:
    """ページの媒体ラベルを返す"""
    ms = page["properties"].get("媒体", {}).get("multi_select", [])
    return ms[0]["name"] if ms else None

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


def search_books(query: str, author: str = None, page: int = 1) -> list:
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

def get_composer_portrait_url(composer_name: str, artist_id: str) -> str | None:
    """
    1. Driveに既存の肖像画があればそのURLを返す
    2. なければMusicBrainz → Wikipedia APIで取得してDriveに保存
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

    # MusicBrainzからWikipedia URL取得
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
        wiki_url = None
        for r in relations:
            if r.get("type") == "wikipedia" and r.get("url", {}).get("resource", ""):
                wiki_url = r["url"]["resource"]
                break
        if not wiki_url:
            return None

        # Wikipedia APIで肖像画取得（URLからページタイトルを抽出）
        wiki_title = wiki_url.rstrip("/").split("/")[-1]
        wiki_res = requests.get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action":      "query",
                "titles":      wiki_title,
                "prop":        "pageimages",
                "pithumbsize": 600,
                "format":      "json",
            },
            timeout=8,
        )
        if wiki_res.status_code != 200:
            return None
        pages = wiki_res.json().get("query", {}).get("pages", {})
        img_url = None
        for page in pages.values():
            img_url = page.get("thumbnail", {}).get("source")
            if img_url:
                break
        if not img_url:
            return None

        # Driveに保存してパブリックURLを返す
        img_data = requests.get(img_url, timeout=8)
        if img_data.status_code != 200:
            return None
        service = get_drive_service_safe()
        media   = MediaIoBaseUpload(io.BytesIO(img_data.content), mimetype="image/jpeg", resumable=False)
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
def search_manga(query: str, author: str = None, page: int = 1) -> list:
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


def location_search_ui(key_prefix: str, media_label: str) -> dict | None:
    """ロケーション検索UIコンポーネント。選択済みlocation dictを返す（未選択はNone）"""
    LOCATION_LABELS = {
        "映画":          ("📍 鑑賞した場所（任意）", "例: TOHOシネマズ梅田"),
        "ドラマ":        ("📍 鑑賞した場所（任意）", "例: 自宅 / Netflix"),
        "演奏会（鑑賞）": ("📍 会場（任意）",          "例: フェニーチェ堺"),
        "演奏会（出演）": ("📍 会場（任意）",          "例: フェニーチェ堺"),
        "展示会":        ("📍 会場（任意）",          "例: 国立国際美術館"),
        "ライブ/ショー": ("📍 会場（任意）",          "例: 大阪城ホール"),
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
                       anilist_id: int | None = None) -> bool:
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

    icon_url = get_media_icon_url(media_type_label)
    payload = {
        "parent":     {"database_id": NOTION_DB_ID},
        "icon":       {"type": "external", "external": {"url": icon_url}},
        "properties": properties,
    }
    if cover_url:
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
        v = "".join(t["plain_text"] for t in (pr.get("ISBN") or {}).get("rich_text", []))
        if v: isbn.add(v)
        media_val = get_page_media(p)
        if media_val in ("書籍", "漫画"):
            raw_title = "".join(t["plain_text"] for t in (pr.get("タイトル") or {}).get("title", []))
            raw_creator = "".join(t["plain_text"] for t in (pr.get("クリエイター") or {}).get("rich_text", []))
            norm_title = re.sub(r'\s*[\(（]?\d+[\)）]?\s*$', '', raw_title).strip().lower()
            for author in re.split(r'[/／・]', raw_creator):
                author = author.strip()
                if author:
                    book_keys.add((norm_title, normalize_name_for_compare(author)))
        elif media_val == "音楽アルバム":
            raw_title = "".join(t["plain_text"] for t in (pr.get("タイトル") or {}).get("title", []))
            raw_creator = "".join(t["plain_text"] for t in (pr.get("クリエイター") or {}).get("rich_text", []))
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

def check_duplicate(tmdb_id: int, pages: list) -> list:
    """TMDB_IDが一致する既存ページを返す"""
    return [p for p in pages if p["properties"].get("TMDB_ID", {}).get("number") == tmdb_id]

def build_update_log(log_title, src, need_notion, notion_ok, need_drive, drive_ok, meta_ok, updated, is_refresh=False) -> str:
    parts = []
    if is_refresh:
        parts.append("🔄 リフレッシュ")
    if need_notion:
        parts.append("Notion " + ("✅" if notion_ok else "❌"))
    if need_drive:
        parts.append("Drive " + ("✅" if drive_ok else "❌"))
    if updated:
        parts.append("メタデータ[" + " / ".join(updated) + "] " + ("✅" if meta_ok else "❌"))
    if not parts:
        return f"⏸️ 維持(OK): {log_title}"
    return f"{log_title}　{src}　{'　'.join(parts)}"

# ============================================================
# アプリ初期化
# ============================================================

st.set_page_config(page_title="ArtéMis", page_icon="assets/favicon.png", layout="wide")

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

st.image("assets/logo.png", width=320)
st.caption("ArtéMis — named after the goddess of the hunt and the moon. She keeps track of everything you've ever experienced.")
st.caption("v5.12")

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
    "refresh_targets_ids": [],
    "refresh_cursor":      0,
    "refresh_success_log": [],
    "refresh_maintain_log": [],
    "refresh_error_log":   [],
    "auto_reload_mode":    "manual",
    "created_pages":       [],
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.header("操作パネル")

    st.divider()
    if "auto_reload_mode" not in st.session_state:
        st.session_state.auto_reload_mode = "manual"
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
                refresh_drive_files()
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
        mode = st.radio("モード", ["新規登録", "データ管理", "自動同期"])
        sync_scope = "欠損のみ補填"  # legacy compat

        if mode != "新規登録":
            st.divider()
            st.header("媒体フィルタ")
            media_filter_options = list(MEDIA_ICON_MAP.keys())
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

        EVENT_MEDIA = ["演奏会（鑑賞）", "演奏会（出演）", "展示会", "ライブ/ショー"]

        # ============================================================
        # イベント系（演奏会（鑑賞）・演奏会（出演）・展示会・ライブ/ショー）- 単体登録のみ
        # ============================================================
        if media_label in EVENT_MEDIA:
            st.divider()
            is_performance  = (media_label == "演奏会（出演）")
            is_concert      = media_label in ("演奏会（鑑賞）", "演奏会（出演）")
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

                # セッションステート構造: [{"title": "曲名", "part": "Vn."}]  ※part は演奏会（出演）のみ使用
                def render_song_list(slot_key, max_count, label):
                    """曲リストUIを描画し、現在のリストを返す"""
                    songs = st.session_state[slot_key]
                    st.caption(label)
                    new_list = []
                    for i, item in enumerate(songs):
                        if is_performance:
                            c_num, c_inp, c_part, c_del = st.columns([0.3, 3.5, 1.2, 0.5])
                        else:
                            c_num, c_inp, c_del = st.columns([0.3, 4, 0.5])
                        c_num.markdown(f"**{i+1}.**")
                        t = c_inp.text_input("", value=item["title"], key=f"{slot_key}_t_{i}", label_visibility="collapsed")
                        p = c_part.text_input("", value=item["part"], key=f"{slot_key}_p_{i}", placeholder="楽器", label_visibility="collapsed") if is_performance else ""
                        new_list.append({"title": t, "part": p})
                        if c_del.button("✕", key=f"{slot_key}_del_{i}"):
                            st.session_state[slot_key] = [x for j, x in enumerate(new_list) if j != i]
                            st.rerun()
                    if new_list:
                        st.session_state[slot_key] = new_list
                    filled = [x for x in new_list if x["title"].strip()]
                    last_empty = new_list and not new_list[-1]["title"].strip()
                    if len(filled) < max_count and not last_empty:
                        if st.button(f"＋ 曲を追加", key=f"{slot_key}_add"):
                            st.session_state[slot_key] = filled + [{"title": "", "part": ""}]
                            st.rerun()

                def add_songs_to_slot(slot_key, titles, max_count):
                    """タイトルリストをセトリに追加（重複除外）"""
                    current = [x for x in st.session_state[slot_key] if x["title"].strip()]
                    for title in titles:
                        if len(current) < max_count and title not in [x["title"] for x in current]:
                            current.append({"title": title, "part": ""})
                    st.session_state[slot_key] = current

                # ── 通常セットリスト ──
                render_song_list("ev_setlist_main", MAX_MAIN, f"📋 通常セットリスト（最大{MAX_MAIN}曲）")

                # ── アンコール ──
                render_song_list("ev_setlist_encore", MAX_ENCORE, f"🎊 アンコール（最大{MAX_ENCORE}曲）")

                # ── 楽曲検索（検索→ボタンで直接追加、チェックボックスなし）──
                st.divider()
                if is_concert:
                    st.caption("🔍 楽曲検索（MusicBrainz）")
                    col_ev_comp, col_ev_title = st.columns([1, 1])
                    ev_composer_input = col_ev_comp.text_input("作曲家名", placeholder="例: Beethoven / ベートーヴェン", key="ev_composer")
                    ev_title_filter   = col_ev_title.text_input("曲名で絞り込み（任意）", placeholder="例: Symphony", key="ev_title_filter")

                    if st.button("🔍 作曲家を検索", key="ev_mb_search"):
                        if ev_composer_input:
                            with st.spinner("作曲家を検索中..."):
                                ev_composers, ev_err = search_mb_composer(ev_composer_input)
                            if ev_err:
                                st.error(f"⚠️ MusicBrainz API エラー: {ev_err}")
                            st.session_state.ev_mb_composers = ev_composers
                            st.session_state.ev_mb_works     = []
                            st.session_state.ev_mb_filter    = ev_title_filter
                            if not ev_composers and not ev_err:
                                st.warning("作曲家が見つかりませんでした。")
                        else:
                            st.warning("作曲家名を入力してください")

                    if st.session_state.get("ev_mb_composers"):
                        ev_composers   = st.session_state.ev_mb_composers
                        ev_comp_labels = [
                            f"{c['name']}" + (f"（{c['disambiguation']}）" if c['disambiguation'] else "") + (f" [{c['life_span']}–]" if c['life_span'] else "")
                            for c in ev_composers
                        ]
                        ev_sel_idx = st.radio("作曲家を選択", range(len(ev_comp_labels)), format_func=lambda i: ev_comp_labels[i], key="ev_mb_comp_radio")
                        if st.button("この作曲家の作品一覧を取得", key="ev_mb_fetch_works"):
                            ev_sel_comp = ev_composers[ev_sel_idx]
                            with st.spinner(f"{ev_sel_comp['name']} の作品を取得中..."):
                                ev_works = search_mb_works(ev_sel_comp["id"], st.session_state.get("ev_mb_filter", ""))
                            st.session_state.ev_mb_works = ev_works

                    if st.session_state.get("ev_mb_works"):
                        ev_works = st.session_state.ev_mb_works
                        st.caption(f"{len(ev_works)} 件の作品　— ボタンで直接追加")
                        for w in ev_works:
                            label = w["title"] + (f"　{w['disambiguation']}" if w["disambiguation"] else "")
                            col_title, col_main, col_enc = st.columns([4, 1.2, 1.2])
                            col_title.markdown(label)
                            if col_main.button("📋 通常", key=f"ev_mb_add_main_{w['id']}"):
                                add_songs_to_slot("ev_setlist_main", [w["title"]], MAX_MAIN)
                                st.rerun()
                            if col_enc.button("🎊 ENC", key=f"ev_mb_add_enc_{w['id']}"):
                                add_songs_to_slot("ev_setlist_encore", [w["title"]], MAX_ENCORE)
                                st.rerun()

                elif is_live:
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

            st.divider()
            event_location = location_search_ui("event", media_label)
            if st.button("📥 登録する", type="primary", key="event_register", disabled=not event_title):
                watch_str = event_watch.isoformat() if event_watch else None
                start_str = event_start.isoformat() if event_start else None
                end_str   = event_end.isoformat()   if event_end   else None

                # ── メモ生成 ──
                memo_text = None
                if has_setlist:
                    main_items   = [x for x in st.session_state.get("ev_setlist_main",   []) if x["title"].strip()]
                    encore_items = [x for x in st.session_state.get("ev_setlist_encore", []) if x["title"].strip()]
                    def fmt(i, item):
                        suffix = f" [{item['part']}]" if is_performance and item.get("part","").strip() else ""
                        return f"{i+1}. {item['title']}{suffix}"
                    lines = [fmt(i, x) for i, x in enumerate(main_items)]
                    if encore_items:
                        lines.append("")
                        lines.append("[Encore]")
                        lines += [fmt(i, x) for i, x in enumerate(encore_items)]
                    memo_text = "\n".join(lines) if lines else None

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
                )
                if ok:
                    for key in ["ev_mb_composers", "ev_mb_works", "ev_mb_filter",
                                "ev_it_results", "ev_setlist_main", "ev_setlist_encore"]:
                        st.session_state.pop(key, None)
                    reset_new_register_state()
                    sync_notion_after_update(
                        page_id=st.session_state.get("last_created_page_id"),
                        updated_page=st.session_state.get("last_created_page"),
                    )
                    show_post_register_ui()
                else:
                    st.error("❌ 登録失敗")
            st.stop()

        # ============================================================
        # 演奏曲 - MusicBrainzカート登録
        # ============================================================
        if media_label == "演奏曲":
            st.divider()
            col_composer, col_title = st.columns([1, 1])
            composer_input = col_composer.text_input("作曲家名", placeholder="例: Beethoven / ベートーヴェン")
            title_filter   = col_title.text_input("曲名で絞り込み（任意）", placeholder="例: Symphony")

            if st.button("🔍 検索", key="mb_composer_search"):
                if composer_input:
                    with st.spinner("作曲家を検索中..."):
                        composers, err = search_mb_composer(composer_input)
                    if err:
                        st.error(f"⚠️ MusicBrainz API エラー: {err}")
                    st.session_state.mb_composers     = composers
                    st.session_state.mb_works         = []
                    st.session_state.mb_title_filter  = title_filter
                    st.session_state.mb_selected_comp = None
                    if not composers and not err:
                        st.warning("作曲家が見つかりませんでした。")
                else:
                    st.warning("作曲家名を入力してください")

            if st.session_state.get("mb_composers"):
                composers   = st.session_state.mb_composers
                comp_labels = [
                    f"{c['name']}" + (f"（{c['disambiguation']}）" if c['disambiguation'] else "") + (f" [{c['life_span']}–]" if c['life_span'] else "")
                    for c in composers
                ]
                selected_idx = st.radio("作曲家を選択", range(len(comp_labels)), format_func=lambda i: comp_labels[i], key="mb_comp_radio")
                if st.button("この作曲家の作品一覧を取得", key="mb_fetch_works"):
                    selected_comp = composers[selected_idx]
                    st.session_state.mb_selected_comp = selected_comp
                    with st.spinner(f"{selected_comp['name']} の作品を取得中...（数分かかることがあります）"):
                        works = search_mb_works(selected_comp["id"], st.session_state.mb_title_filter)
                    st.session_state.mb_works   = works
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

                    if st.button(f"📋 {len(selected_works)} 件を登録リストに追加", key="mb_add_cart"):
                        for w in selected_works:
                            st.session_state.reg_cart.append({
                                "jp_title":    w["title"],
                                "en_title":    w["title"],
                                "cover_url":   cover_url_final,
                                "release":     "",
                                "watched":     "",
                                "rating":      "",
                                "wlflg":       False,
                                "media_type":  "score",
                                "tmdb_id":     0,
                                "details":     {"genres": [], "cast": "", "director": comp_name, "score": None},
                                "isbn":        "",
                                "location":    None,
                                "media_label": media_label,
                            })
                        st.session_state.mb_checked = {}
                        st.success(f"✅ {len(selected_works)} 件を登録リストに追加しました")
            st.stop()

        # ============================================================
        # 通常媒体（映画・ドラマ・書籍・漫画・音楽アルバム・ゲーム・アニメ）
        # ============================================================
        st.divider()

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
                    results = search_books(query or "", author=creator_input or None)
                elif media_label == "漫画":
                    results = search_manga(query or "", author=creator_input or None)
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
            else:
                st.warning("タイトルまたはクリエイター名を入力してください")

        # ── 単体確認画面 ──
        if st.session_state.confirm_reg is not None:
            reg = st.session_state.confirm_reg
            st.divider()
            st.subheader("📝 登録内容の確認・修正")
            c1, c2 = st.columns([1, 2])
            with c1:
                if reg.get("cover_url"):
                    st.image(reg["cover_url"])
                st.caption(f"{reg['cand_en']} ({reg['media_type']}) {reg['tmdb_release']} 🆔 {reg.get('tmdb_id','')}")
            with c2:
                final_jp = clearable_text_input("日本語タイトル（修正可）", "final_jp", value=reg.get("jp_input", jp_input))
                final_en = clearable_text_input("英語タイトル（修正可）", "final_en", value=reg["cand_en"])
                if media_label in ("書籍", "漫画"):
                    final_isbn = st.text_input("ISBN", value=reg.get("isbn", ""), key="final_isbn")
                else:
                    final_isbn = None

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
                        st.session_state.registering = False
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

        # ── 検索結果一覧（カード＋チェック）──
        elif st.session_state.get("new_search_done", False):
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
                st.stop()

            st.caption(f"{len(results_list)} 件の候補　チェックして登録リストに追加できます")
            if excluded_list:
                st.caption(f"⚠️ {len(excluded_list)} 件は登録済みのため除外")
                with st.expander("除外されたタイトルを表示"):
                    for t in excluded_list:
                        st.caption(f"・{t}")

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
                                    "cand_en": cand["title"], "jp_input": "",
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
                        new_results = search_books(_query or "", author=_author or None, page=next_page)
                    else:
                        new_results = search_manga(_query or "", author=_author or None, page=next_page)
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

        # ── 登録リスト確認・編集・一括登録 ──
        if st.session_state.reg_cart:
            st.divider()
            st.subheader(f"📋 登録リスト（{len(st.session_state.reg_cart)} 件）")
            date_label = {"ゲーム": "クリア日", "音楽アルバム": "聴いた日", "書籍": "読了日", "漫画": "読了日", "演奏曲": "演奏日", "アニメ": "視聴日"}.get(media_label, "鑑賞日")

            remove_indices = []
            for idx, item in enumerate(st.session_state.reg_cart):
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
                    item["location"] = location_search_ui(f"cart_{idx}", item_media)

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
    return apply_diff_filter(base, diff_filter)

def resolve_needs(notion_ok_now, drive_ok_now):
    if diff_filter == "Notionのみ更新（Driveあり・Notionカバーなし）": return True, False
    if diff_filter == "Driveのみ更新（Notionカバーあり・Driveなし）":  return False, True
    return not notion_ok_now, not drive_ok_now

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
                    isbn_val = "".join(t["plain_text"] for t in (props.get("ISBN") or {}).get("rich_text", []))
                    author_val = "".join(t["plain_text"] for t in (props.get("クリエイター") or {}).get("rich_text", []))
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
                need_drive  = not drive_ok_now
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
                    isbn_val = "".join(t["plain_text"] for t in props.get("ISBN", {}).get("rich_text", []))
                    should_normalize = (media_label_val != "書籍") or bool(isbn_val)
                    if should_normalize:
                        raw_creator = "".join(t["plain_text"] for t in props.get("クリエイター", {}).get("rich_text", []))
                        if raw_creator:
                            cleaned = " / ".join(clean_author(a) for a in raw_creator.split("/") if a.strip())
                            if cleaned != raw_creator:
                                patch_body.setdefault("properties", {})["クリエイター"] = {
                                    "rich_text": [{"type": "text", "text": {"content": cleaned}}]
                                }

                # 音楽アルバム: iTunesからカバー再取得（英語タイトル優先）
                if media_label_val == "音楽アルバム":
                    en_title_str  = "".join(t["plain_text"] for t in props.get("International Title", {}).get("rich_text", []))
                    jp_title_str  = "".join(t["plain_text"] for t in props.get("タイトル", {}).get("title", []))
                    title_str     = en_title_str or jp_title_str
                    artist_str    = "".join(t["plain_text"] for t in props.get("クリエイター", {}).get("rich_text", []))
                    if title_str:
                        albums = search_albums(title_str, artist=artist_str or None)
                        if albums:
                            new_cover = albums[0]["cover_url"]
                            if new_cover:
                                patch_body["cover"] = {"type": "external", "external": {"url": new_cover}}

                # ゲーム: IGDBからカバー再取得
                elif media_label_val == "ゲーム":
                    en_title = "".join(t["plain_text"] for t in props.get("International Title", {}).get("rich_text", []))
                    jp_title = "".join(t["plain_text"] for t in props.get("タイトル", {}).get("title", []))
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
            need_drive  = True if is_refresh else not drive_ok_now

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
                                            title, int(new_tmdb_id), mt, True, True,
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
                                            title, int(new_tmdb_id), new_media_type, True, True,
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
            st.session_state.is_running = False
            st.session_state.refresh_targets_ids = []
    else:
        st.session_state.is_running = False

# ============================================================
# 手動確認モード
# ============================================================
if mode == "データ管理":
    display_pages = get_display_pages()

    st.subheader(f"🗂 データ管理　表示: {len(display_pages)} 件 / 全 {len(target_pages)} 件")
    if diff_filter != "フィルタなし":
        st.caption(f"差分フィルタ適用中: {diff_filter}")

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
        is_event_media = page_media in ("演奏会（出演）", "演奏会（鑑賞）", "ライブ/ショー", "展示会")

        with st.expander(f"{diff_badge(item)}  {log_title}"):
            # ── ステータス行 ──
            stat_c1, stat_c2, stat_c3 = st.columns(3)
            stat_c1.metric("媒体", page_media or "不明")
            stat_c2.metric("Notionカバー", "登録済" if notion_ok_now else "未登録")
            stat_c3.metric("Drive画像",   "あり"   if drive_ok_now  else "なし")

            # ── カバー画像プレビュー ──
            current_url = get_current_notion_url(item)
            if current_url:
                img_c, info_c = st.columns([1, 3])
                img_c.image(current_url, use_container_width=True)
                with info_c:
                    st.caption(f"カバーURL: `{current_url}`")
                    # 読み取り専用フィールド表示
                    release_val = ((props.get("リリース日") or {}).get("date") or {}).get("start", "") or "—"
                    genre_items = (props.get("ジャンル") or {}).get("multi_select", [])
                    genre_val   = "　".join(g["name"] for g in genre_items) if genre_items else "—"
                    creator_val = "".join(t["plain_text"] for t in (props.get("クリエイター") or {}).get("rich_text", [])) or "—"
                    cast_val    = "".join(t["plain_text"] for t in (props.get("キャスト・関係者") or {}).get("rich_text", [])) or "—"
                    tmdb_score  = (props.get("TMDB_score") or {}).get("number")
                    isbn_val    = "".join(t["plain_text"] for t in (props.get("ISBN") or {}).get("rich_text", [])) or "—"
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
            st.caption("✏️ 基本")
            existing_rating = (props.get("評価") or {}).get("select") or {}
            existing_rating = existing_rating.get("name", "") if isinstance(existing_rating, dict) else ""
            existing_memo   = "".join(t["plain_text"] for t in (props.get("メモ") or {}).get("rich_text", []))
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
                    patch_props["International Title"] = {"rich_text": [{"type": "text", "text": {"content": new_en}}]}
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

            # ── メタ / ID ──
            st.divider()
            st.caption("🧩 メタ / ID")
            existing_genres = " / ".join(g["name"] for g in (props.get("ジャンル") or {}).get("multi_select", []))
            existing_creator = "".join(t["plain_text"] for t in (props.get("クリエイター") or {}).get("rich_text", []))
            existing_cast = "".join(t["plain_text"] for t in (props.get("キャスト・関係者") or {}).get("rich_text", []))
            meta_c1, meta_c2 = st.columns(2)
            new_genres = meta_c1.text_input("ジャンル（区切り: / または , ）", value=existing_genres, key=f"edit_genres_{page_id}")
            new_creator = meta_c2.text_input("クリエイター", value=existing_creator, key=f"edit_creator_{page_id}")
            new_cast = st.text_input("キャスト・関係者", value=existing_cast, key=f"edit_cast_{page_id}")

            id_c1, id_c2, id_c3, id_c4 = st.columns(4)
            current_isbn = "".join(t["plain_text"] for t in (props.get("ISBN") or {}).get("rich_text", []))
            new_isbn = id_c1.text_input("ISBN", value=current_isbn, key=f"edit_isbn_{page_id}")
            current_anilist = (props.get("AniList_ID") or {}).get("number") or 0
            new_anilist = id_c2.number_input("AniList_ID", value=int(current_anilist) if current_anilist else 0, min_value=0, step=1, key=f"edit_anilist_{page_id}")
            current_igdb = (props.get("IGDB_ID") or {}).get("number") or 0
            new_igdb = id_c3.number_input("IGDB_ID", value=int(current_igdb) if current_igdb else 0, min_value=0, step=1, key=f"edit_igdb_{page_id}")
            current_itunes = (props.get("iTunes_ID") or {}).get("number") or 0
            new_itunes = id_c4.number_input("iTunes_ID", value=int(current_itunes) if current_itunes else 0, min_value=0, step=1, key=f"edit_itunes_{page_id}")

            if st.button("💾 メタ/IDを保存", key=f"save_meta_{page_id}"):
                patch_props = {}
                if new_genres != existing_genres:
                    genres_list = [g.strip() for g in re.split(r'[/,、]', new_genres) if g.strip()]
                    patch_props["ジャンル"] = {"multi_select": [{"name": g} for g in genres_list]}
                if new_creator != existing_creator:
                    patch_props["クリエイター"] = {"rich_text": [{"type": "text", "text": {"content": new_creator}}]}
                if new_cast != existing_cast:
                    patch_props["キャスト・関係者"] = {"rich_text": [{"type": "text", "text": {"content": new_cast}}]}
                if new_isbn != current_isbn:
                    patch_props["ISBN"] = {"rich_text": [{"type": "text", "text": {"content": new_isbn}}]} if new_isbn else {"rich_text": []}
                if new_anilist != current_anilist:
                    patch_props["AniList_ID"] = {"number": int(new_anilist)} if new_anilist else {"number": None}
                if new_igdb != current_igdb:
                    patch_props["IGDB_ID"] = {"number": int(new_igdb)} if new_igdb else {"number": None}
                if new_itunes != current_itunes:
                    patch_props["iTunes_ID"] = {"number": int(new_itunes)} if new_itunes else {"number": None}
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

            # ── 演奏会（出演）セットリスト編集 ──
            if page_media == "演奏会（出演）":
                st.divider()
                st.caption("🎻 セットリスト編集")
                existing_memo_full = "".join(t["plain_text"] for t in (props.get("メモ") or {}).get("rich_text", []))
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
            st.caption("📍 ロケーション")
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
            st.caption("🖼 カバー画像を差し替え")
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

            # ── TMDB_ID修正（映画・ドラマのみ） ──
            if is_tmdb_media:
                st.divider()
                st.caption("🔧 TMDB_ID を手動で修正")
                id_col, save_col = st.columns([3, 1])
                new_tmdb_id = id_col.number_input(
                    "TMDB_ID",
                    value=int(saved_tmdb_id) if saved_tmdb_id else 0,
                    min_value=0,
                    step=1,
                    key=f"tmdb_id_input_{page_id}",
                )
                new_media_type = "movie" if page_media == "映画" else "tv"
                with save_col:
                    st.write("")
                    st.write("")
                    if st.button("💾 保存", key=f"save_id_{page_id}"):
                        if new_tmdb_id > 0:
                            with st.spinner("更新中..."):
                                top = fetch_tmdb_by_id(new_tmdb_id, new_media_type)
                                if top:
                                    cover_url        = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{top['poster_path']}"
                                    tmdb_release     = top.get("release_date") or top.get("first_air_date")
                                    date_prop        = props.get("リリース日", {}).get("date")
                                    existing_release = date_prop.get("start") if date_prop else None
                                    season_number    = get_season_number(props)
                                    st.session_state.tmdb_id_cache[page_id] = new_tmdb_id
                                    n_ok, d_ok, meta_ok, updated = update_all(
                                        page_id, cover_url, tmdb_release, existing_release,
                                        log_title, new_tmdb_id, new_media_type, True, True,
                                        force_meta=True, props=props, season_number=season_number,
                                    )
                                    parts = []
                                    parts.append("Notion " + ("✅" if n_ok else "❌"))
                                    parts.append("Drive "  + ("✅" if d_ok else "❌"))
                                    if updated: parts.append("メタデータ[" + " / ".join(updated) + "] " + ("✅" if meta_ok else "❌"))
                                    if n_ok and d_ok:
                                        st.success("保存完了！ " + "　".join(parts))
                                        for p in st.session_state.pages:
                                            if p["id"] == page_id:
                                                p["cover"]                    = {"type": "external", "external": {"url": cover_url}}
                                                p["properties"]["TMDB_ID"]    = {"number": new_tmdb_id}
                                        sync_notion_after_update(page_id=page_id)
                                        time.sleep(1.5)
                                        st.rerun()
                                    else:
                                        st.error("一部失敗: " + "　".join(parts))
                                else:
                                    st.error("TMDBでIDが見つかりませんでした")
                        else:
                            st.warning("TMDB_IDを入力してください")

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
                                      need_notion, need_drive = True, True
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
