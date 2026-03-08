import re
import requests
import time
import streamlit as st
from datetime import date
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
GOOGLE_BOOKS_API_KEY = st.secrets.get("GOOGLE_BOOKS_API_KEY", "")
RAKUTEN_APP_ID = st.secrets.get("RAKUTEN_APP_ID", "")
DRIVE_FOLDER_ID = st.secrets["DRIVE_FOLDER_ID"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ============================================================
# 媒体マッピング
# ============================================================
BOOTSTRAP_CDN = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/icons/{}.svg"

MEDIA_ICON_MAP = {
    "映画":          ("🎬 映画",          "https://raw.githubusercontent.com/attituderko-design/notion-poster-sync/refs/heads/main/camera-reels.svg"),
    "ドラマ":        ("📺 ドラマ",        "https://raw.githubusercontent.com/attituderko-design/notion-poster-sync/refs/heads/main/display.svg"),
    "演奏会":        ("🎻 演奏会",        "https://raw.githubusercontent.com/attituderko-design/notion-poster-sync/refs/heads/main/music-note-beamed.svg"),
    "展示会":        ("🖼️ 展示会",        "https://raw.githubusercontent.com/attituderko-design/notion-poster-sync/refs/heads/main/image.svg"),
    "ライブ/ショー": ("🎤 ライブ/ショー", "https://raw.githubusercontent.com/attituderko-design/notion-poster-sync/refs/heads/main/mic.svg"),
    "書籍":          ("📖 書籍",          "https://raw.githubusercontent.com/attituderko-design/notion-poster-sync/refs/heads/main/book.svg"),
}

RATING_OPTIONS = ["", "★", "★★", "★★★", "★★★★", "★★★★★"]

def get_media_icon_url(media_label: str) -> str:
    return MEDIA_ICON_MAP.get(media_label, ("", ""))[1]

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

# ============================================================
# ユーティリティ
# ============================================================

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", name)

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
    props = page["properties"]
    if is_unreleased(page):                                return False
    if not page.get("cover"):                              return True
    if not props.get("TMDB_ID", {}).get("number"):         return True
    if not props.get("ジャンル", {}).get("multi_select"):  return True
    if not props.get("キャスト・関係者", {}).get("rich_text"):  return True
    if not props.get("クリエイター", {}).get("rich_text"):  return True
    if props.get("TMDB_score", {}).get("number") is None:  return True
    return False

# ============================================================
# Drive ファイル一覧（session_stateで管理）
# ============================================================

def get_drive_files() -> dict:
    if "drive_files_cache" not in st.session_state:
        refresh_drive_files()
    return st.session_state.drive_files_cache

def refresh_drive_files():
    service = get_drive_service()
    results = service.files().list(
        q=f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name)",
        pageSize=1000,
    ).execute()
    st.session_state.drive_files_cache = {f["name"]: f["id"] for f in results.get("files", [])}

def drive_exists(title: str, tmdb_id) -> bool:
    return make_filename(title, tmdb_id) in get_drive_files()

def drive_exists_fuzzy(title: str) -> bool:
    prefix = sanitize_filename(title) + "_"
    return any(name.startswith(prefix) and name.endswith(".jpg") for name in get_drive_files())

def save_to_drive(cover_url: str, title: str, tmdb_id, image_bytes: bytes | None = None, mimetype: str = "image/jpeg") -> str | None:
    """Drive保存成功時はfile_idを返す、失敗時はNone"""
    try:
        if image_bytes is None:
            img_url = cover_url.replace("w600_and_h900_bestv2", "original")
            img_res = api_request("get", img_url)
            if img_res is None or img_res.status_code != 200:
                return None
            image_bytes = img_res.content
            mimetype    = "image/jpeg"
        service = get_drive_service()
        fname   = make_filename(title, tmdb_id)
        files   = get_drive_files()
        media   = MediaIoBaseUpload(io.BytesIO(image_bytes), mimetype=mimetype, resumable=False)
        if fname in files:
            file_id = files[fname]
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            result  = service.files().create(
                body={"name": fname, "parents": [DRIVE_FOLDER_ID]},
                media_body=media,
                fields="id",
            ).execute()
            file_id = result["id"]
            st.session_state.drive_files_cache[fname] = file_id
        return file_id
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
        service = get_drive_service()
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
        service = get_drive_service()
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
    fn = {"get": requests.get, "post": requests.post, "patch": requests.patch, "delete": requests.delete}[method]
    for attempt in range(max_retries):
        try:
            res = fn(url, **kwargs)
            if res.status_code == 429:
                time.sleep(int(res.headers.get("Retry-After", 5)))
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
    while has_more:
        payload = {"page_size": 100}
        if next_cursor:
            payload["start_cursor"] = next_cursor
        res = api_request("post", url, headers=NOTION_HEADERS, json=payload)
        if res is None:
            break
        data = res.json()
        all_results.extend(data.get("results", []))
        has_more    = data.get("has_more", False)
        next_cursor = data.get("next_cursor")
    return all_results

def filter_target_pages(all_pages: list) -> list:
    return [
        p for p in all_pages
        if any(m["name"] in ["映画", "ドラマ"] for m in p["properties"].get("媒体", {}).get("multi_select", []))
    ]

def get_tmdb_id_from_notion(props) -> tuple:
    tmdb_id_val    = props.get("TMDB_ID", {}).get("number")
    media_type_val = props.get("MEDIA_TYPE", {}).get("multi_select", [])
    media_type     = media_type_val[0]["name"] if media_type_val else None
    return (int(tmdb_id_val) if tmdb_id_val else None), media_type

def save_tmdb_id_to_notion(page_id: str, tmdb_id: int, media_type: str) -> bool:
    res = api_request(
        "patch",
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {
            "TMDB_ID":    {"number": tmdb_id},
            "MEDIA_TYPE": {"multi_select": [{"name": media_type}]},
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

def search_tmdb(query: str, year=None) -> list:
    params = {"api_key": TMDB_API_KEY, "query": query, "language": "en-US"}
    if year:
        params["primary_release_year"] = year
    res = api_request("get", "https://api.themoviedb.org/3/search/multi", params=params)
    if res is None:
        return []
    return [r for r in res.json().get("results", []) if r.get("poster_path") and r.get("media_type") in ["movie", "tv"]]


def search_tmdb_by_person(person_query: str) -> list:
    """クリエイター/キャスト名でTMDB人物検索→その人の作品一覧を返す"""
    res = api_request("get", "https://api.themoviedb.org/3/search/person",
                      params={"api_key": TMDB_API_KEY, "query": person_query, "language": "en-US"})
    if res is None:
        return []
    people = res.json().get("results", [])
    if not people:
        return []
    # 最初にヒットした人物の出演・監督作品を取得
    person_id = people[0]["id"]
    res2 = api_request("get", f"https://api.themoviedb.org/3/person/{person_id}/combined_credits",
                       params={"api_key": TMDB_API_KEY, "language": "en-US"})
    if res2 is None:
        return []
    credits = res2.json()
    works = credits.get("cast", []) + credits.get("crew", [])
    # ポスターありのmovie/tvのみ、人気順で重複除去
    seen_ids = set()
    results = []
    for w in sorted(works, key=lambda x: x.get("popularity", 0), reverse=True):
        if w.get("poster_path") and w.get("media_type") in ["movie", "tv"] and w["id"] not in seen_ids:
            seen_ids.add(w["id"])
            results.append(w)
        if len(results) >= 20:
            break
    return results


def search_books(query: str, author: str = None) -> list:
    """楽天ブックスAPIで書籍検索（タイトル直接検索）"""
    import urllib.parse as _up, re as _re

    rk_params = {
        "applicationId": RAKUTEN_APP_ID,
        "accessKey":     st.secrets.get("RAKUTEN_ACCESS_KEY", ""),
        "hits":          20,
        "formatVersion": 2,
        "sort":          "sales",
        "outOfStockFlag": 1,
    }
    if query:
        rk_params["title"] = query
    if author:
        rk_params["author"] = author
    rk_headers = {
        "Referer":       "https://notion-poster-sync-5wr4mgqdksey3z8tttbk9u.streamlit.app",
        "Origin":        "https://notion-poster-sync-5wr4mgqdksey3z8tttbk9u.streamlit.app",
        "User-Agent":    "Mozilla/5.0",
        "Authorization": f"Bearer {st.secrets.get('RAKUTEN_ACCESS_KEY', '')}",
    }
    url_rk = "https://openapi.rakuten.co.jp/services/api/BooksBook/Search/20170404?" + _up.urlencode(rk_params)
    try:
        res_rk = requests.get(url_rk, timeout=8, headers=rk_headers)
    except Exception as e:
        st.warning(f"⚠️ 楽天ブックスAPI エラー: {e}")
        return []

    if res_rk.status_code != 200:
        st.warning(f"⚠️ 楽天ブックスAPI {res_rk.status_code}: {res_rk.text[:200]}")
        return []

    items = res_rk.json().get("Items", [])
    results = []
    for item in items:
        cover = item.get("largeImageUrl") or item.get("mediumImageUrl") or item.get("smallImageUrl", "")
        cover = cover.replace("http://", "https://") if cover else ""
        # 著者名から接尾語を除去
        raw_authors = [a.strip() for a in (item.get("author", "") or "").split("/") if a.strip()]
        authors = [clean_author(a) for a in raw_authors]
        isbn_val = item.get("isbn", "")
        results.append({
            "id":         isbn_val or item.get("title", ""),
            "isbn":       isbn_val,
            "title":      item.get("title", ""),
            "authors":    authors,
            "publisher":  item.get("publisherName", ""),
            "published":  (item.get("salesDate", "") or "")[:4],
            "genres":     [],
            "cover_url":  cover,
            "media_type": "book",
        })
    return results


def fetch_book_ja_title(book_id: str) -> str:
    return book_id

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

def create_notion_page(jp_title: str, en_title: str, media_type_label: str,
                       tmdb_id: int, media_type: str, cover_url: str,
                       tmdb_release: str, details: dict,
                       wlflg: bool = False, watched_date: str | None = None,
                       rating: str | None = None,
                       isbn: str | None = None,
                       location: str | None = None,
                       event_end: str | None = None) -> bool:
    """Notionに新規ページを作成してポスター・メタデータも一括登録"""
    properties = {
        "タイトル":            {"title": [{"type": "text", "text": {"content": jp_title}}]},
        "International Title": {"rich_text": [{"type": "text", "text": {"content": en_title}, "annotations": {"italic": True}}]},
        "媒体":               {"multi_select": [{"name": media_type_label}]},
        **({"TMDB_ID": {"number": tmdb_id}} if tmdb_id else {}),
        **({"MEDIA_TYPE": {"multi_select": [{"name": media_type}]}} if media_type and media_type not in ("book",) else {}),
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
    # ロケーションはNotionのフィールド型に応じて要調整
    # if location:
    #     properties["ロケーション"] = {"rich_text": [{"type": "text", "text": {"content": location}}]}

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
    return True

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

st.set_page_config(page_title="ArtéMis", page_icon="favicon.png", layout="wide")
st.image("logo.png", width=320)
st.caption("v1.88")

for key, default in {
    "is_running":         False,
    "pages_loaded":       False,
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
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.header("操作パネル")
    st.caption("モードを選択してからデータを取得してください")

    st.divider()
    st.header("動作モード")
    mode = st.radio("モード", ["新規登録", "手動確認", "自動同期"])
    if mode != "新規登録":
        sync_scope = st.radio("同期範囲", ["未設定のみ更新", "全件走査"])
    else:
        sync_scope = "未設定のみ更新"

    st.divider()
    if st.button("📥 Notionデータ取得", use_container_width=True, disabled=(mode == "新規登録")):
        with st.spinner("Notionからデータ取得中..."):
            all_pages = load_notion_data()
            st.session_state.all_pages      = all_pages
            st.session_state.pages          = filter_target_pages(all_pages)
            st.session_state.pages_loaded   = True
            st.session_state.search_results = {}
            st.session_state.manual_page    = 0
            refresh_drive_files()
        st.success(f"{len(st.session_state.pages)} 件取得しました（全媒体: {len(st.session_state.all_pages)} 件）")

    if mode != "新規登録":
        st.divider()
        st.header("差分フィルタ")
        st.caption("🟢=登録済　🔴=未登録")
        diff_filter = st.radio(
            "対象を絞り込む",
            [
                "フィルタなし",
                "Notionのみ更新（Driveあり・Notionカバーなし）",
                "Driveのみ更新（Notionカバーあり・Driveなし）",
                "どちらも更新（両方なし）",
            ],
            index=0,
        )
    else:
        diff_filter = "フィルタなし"

    if mode == "自動同期":
        st.divider()
        if st.button("🚀 自動同期開始", use_container_width=True, disabled=not st.session_state.pages_loaded):
            st.session_state.is_running = True
            st.session_state.sync_mode  = "normal"
            st.rerun()
        if st.button("🔄 リフレッシュ", use_container_width=True, disabled=not st.session_state.pages_loaded):
            st.session_state.is_running = True
            st.session_state.sync_mode  = "refresh"
            st.rerun()
        if st.button("⏹ 停止", use_container_width=True):
            st.session_state.is_running = False
            st.rerun()

    if mode != "新規登録":
        st.divider()
        confirm_delete = st.checkbox("カバー全削除を許可")
        delete_btn = st.button("🗑 カバー全削除", disabled=not confirm_delete or not st.session_state.pages_loaded)
    else:
        delete_btn = False

# ============================================================
# 新規登録モード
# ============================================================
if mode == "新規登録":
    st.subheader("➕ 新規登録")

    # ── 媒体選択を最初に ──
    media_display = st.selectbox("媒体", [v[0] for v in MEDIA_ICON_MAP.values()], key="reg_media")
    media_label   = next(k for k, v in MEDIA_ICON_MAP.items() if v[0] == media_display)

    # ── イベント系（演奏会・展示会・ライブ/ショー） ──
    EVENT_MEDIA = ["演奏会", "展示会", "ライブ/ショー"]

    if media_label in EVENT_MEDIA:
        st.divider()

        # タイトル
        event_title = st.text_input("公演名 / 展示名 *", placeholder="例: 大阪フィルハーモニー交響楽団 第588回定期演奏会")

        # ロケーション・クリエイター
        st.caption("📍 ロケーション情報はNotion上で入力してください。")
        event_creator = st.text_input(
            "クリエイター",
            placeholder="例: 指揮者・キュレーターなど",
        )
        event_location = None

        # 出演者・ジャンル
        col_cast, col_genre = st.columns([1, 1])
        event_cast  = col_cast.text_input("出演者・演奏者", placeholder="例: 山田太郎 / 鈴木花子")
        event_genre = col_genre.text_input("ジャンル", placeholder="例: クラシック / 印象派")

        # 日付
        if media_label == "展示会":
            col_start, col_end, col_watch = st.columns([1, 1, 1])
            event_start = col_start.date_input("開催開始日", value=None, key="ev_start")
            event_end   = col_end.date_input("開催終了日",   value=None, key="ev_end")
            event_watch = col_watch.date_input("鑑賞日",     value=None, key="ev_watch")
        else:
            col_watch2, _ = st.columns([1, 1])
            event_watch = col_watch2.date_input("鑑賞日", value=None, key="ev_watch2")
            event_start = event_watch
            event_end   = None

        # 評価・WLflg
        col_rating, col_wl = st.columns([2, 1])
        rating_sel = col_rating.selectbox("評価", RATING_OPTIONS, key="ev_rating")
        wlflg      = col_wl.checkbox("WLflg", value=False, key="ev_wl")

        st.caption("🖼 カバー画像・ロケーション情報はNotion上で入力してください。")

        st.divider()
        if st.button("📥 登録する", type="primary", key="event_register", disabled=not event_title):
            if not event_title:
                st.warning("公演名 / 展示名は必須です")
            else:
                with st.spinner("登録中..."):
                    # 日付
                    start_str = event_start.isoformat() if event_start else None
                    end_str   = event_end.isoformat()   if event_end   else None
                    watch_str = event_watch.isoformat()  if event_watch  else None

                    # ジャンル
                    genres = [g.strip() for g in event_genre.split("/") if g.strip()] if event_genre else []

                    details = {
                        "genres":   genres,
                        "cast":     event_cast or "",
                        "director": event_creator or "",
                        "score":    None,
                    }

                    ok = create_notion_page(
                        jp_title=event_title,
                        en_title="",
                        media_type_label=media_label,
                        tmdb_id=0,
                        media_type="",
                        cover_url="",
                        tmdb_release=start_str or "",
                        details=details,
                        wlflg=wlflg,
                        watched_date=watch_str,
                        rating=rating_sel if rating_sel else None,
                        location=event_location or None,
                        event_end=end_str,
                    )



                    if ok:
                        st.success(f"✅ 登録完了！「{event_title}」をNotionに追加しました")
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.error("登録失敗しました")

        st.stop()

    # ── 映画・ドラマ・書籍（検索フロー） ──
    col_jp, col_en = st.columns([1, 1])
    jp_input     = col_jp.text_input("日本語タイトル", placeholder="例: 千と千尋の神隠し")
    en_input     = col_en.text_input("英語タイトル（検索用）", placeholder="例: Spirited Away")
    col_creator, col_cast = st.columns([1, 1])
    creator_input = col_creator.text_input("クリエイター（著者・監督）", placeholder="例: 宮崎駿 / 道尾秀介")
    cast_input    = col_cast.text_input("キャスト・関係者", placeholder="例: 木村拓哉")

    col_wl, col_date, col_rating = st.columns([1, 2, 2])
    wlflg        = col_wl.checkbox("WLflg", value=False)
    watched_date = col_date.date_input("鑑賞日", value=None)
    rating_sel   = col_rating.selectbox("評価", RATING_OPTIONS)

    if st.button("🔍 検索", key="new_search"):
        query = en_input if en_input else jp_input
        if query or creator_input or cast_input:
            if media_label == "書籍":
                rk_q    = query or None
                rk_auth = creator_input or None
                results = search_books(rk_q or "", author=rk_auth)
            else:
                if creator_input or cast_input:
                    results = search_tmdb_by_person(creator_input or cast_input)
                else:
                    results = search_tmdb(query)
            st.session_state.new_search_results = results[:20]
            st.session_state.new_search_done    = True
            st.session_state.confirm_reg        = None
            st.session_state.bulk_checked       = {}
        else:
            st.warning("タイトルまたはクリエイター/キャストを入力してください")

    # ── 確認・修正ステップ ──
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
            final_jp = st.text_input("日本語タイトル（修正可）", value=reg.get("jp_input", jp_input), key="final_jp")
            final_en = st.text_input("英語タイトル（修正可）",   value=reg["cand_en"],                key="final_en")
            if media_label == "書籍":
                final_isbn = st.text_input("ISBN", value=reg.get("isbn", ""), key="final_isbn")
            else:
                final_isbn = None

            # 重複チェック
            if not st.session_state.pages_loaded:
                with st.spinner("重複チェック中..."):
                    all_pages = load_notion_data()
                    st.session_state.pages        = filter_target_pages(all_pages)
                    st.session_state.pages_loaded = True
            dupes = check_duplicate(reg.get("tmdb_id", 0), st.session_state.pages)
            if dupes:
                dupe_titles = "、".join([get_title(d["properties"])[0] for d in dupes])
                st.warning(f"⚠️ 登録済のデータがあります：{dupe_titles}\nそれでも登録しますか？")

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
                    if reg["media_type"] == "book":
                        details = {
                            "genres":   reg.get("book_genres", []),
                            "cast":     "",
                            "director": clean_author_list(reg.get("book_authors", [])),
                            "score":    None,
                        }
                    else:
                        details = fetch_tmdb_details(reg["tmdb_id"], reg["media_type"])
                    watched_str  = watched_date.isoformat() if watched_date else None
                    page_tmdb_id = 0 if reg["media_type"] == "book" else reg["tmdb_id"]
                    ok = create_notion_page(
                        jp_title=final_jp,
                        en_title=final_en,
                        media_type_label=media_label,
                        tmdb_id=page_tmdb_id,
                        media_type=reg["media_type"],
                        cover_url=reg["cover_url"],
                        tmdb_release=reg["tmdb_release"],
                        details=details,
                        wlflg=wlflg,
                        watched_date=watched_str,
                        rating=rating_sel if rating_sel else None,
                        isbn=final_isbn or None,
                    )
                    st.session_state.registering = False
                    if ok:
                        save_to_drive(reg["cover_url"], final_jp if final_jp else final_en, reg["tmdb_id"])
                        st.session_state.confirm_reg        = None
                        st.session_state.new_search_results = []
                        st.session_state.new_search_done    = False
                        st.success(f"✅ 登録完了！「{final_jp or final_en}」をNotionに追加しました")
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.error("登録失敗しました")

    # ── 候補一覧 ──
    elif st.session_state.new_search_results == [] and "new_search_done" in st.session_state and st.session_state.new_search_done:
        if media_label == "書籍":
            st.warning("候補が見つかりませんでした。検索ワードを変えて再試行してください。\nGoogle Booksで直接検索する方法もあります → https://books.google.co.jp")
        else:
            st.warning("候補が見つかりませんでした。検索ワードを変えて再試行してください。\nTMDBで直接検索してIDを確認する方法もあります → https://www.themoviedb.org")
    elif st.session_state.new_search_results:
        results_list = st.session_state.new_search_results
        if "bulk_checked" not in st.session_state:
            st.session_state.bulk_checked = {}

        st.caption(f"{len(results_list)} 件の候補　チェックして一括登録できます")

        # チェックボックス＋カード一覧
        for row_start in range(0, len(results_list), 3):
            cols = st.columns(3)
            for col_idx, cand in enumerate(results_list[row_start:row_start + 3]):
                abs_idx = row_start + col_idx
                with cols[col_idx]:
                    if media_label == "書籍":
                        cover_url    = cand["cover_url"]
                        tmdb_release = cand.get("published", "")
                        media_type   = "book"
                        cand_en      = ""
                        display_title = cand["title"]
                        authors      = " / ".join(cand.get("authors", []))
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

                    if media_label == "書籍":
                        if cover_url and "placeholder" not in cover_url:
                            try:
                                st.image(cover_url)
                            except Exception:
                                st.caption("📷 表紙取得失敗")
                        else:
                            st.caption("📷 表紙なし")
                        if authors: st.caption(f"著者: {authors}")
                        if tmdb_release: st.caption(f"出版: {tmdb_release}")
                    else:
                        st.image(cover_url)
                        caption = cand_en
                        if tmdb_release: caption += f" {tmdb_release}"
                        caption += f" 🆔 {cand['id']}"
                        st.caption(caption)

                    if st.button("✅ これで登録", key=f"new_reg_{abs_idx}"):
                        if media_label == "書籍":
                            st.session_state.confirm_reg = {
                                "tmdb_id":      cand["id"],
                                "cover_url":    cand["cover_url"],
                                "tmdb_release": cand.get("published", ""),
                                "media_type":   "book",
                                "cand_en":      "",
                                "jp_input":     cand["title"],
                                "book_authors": cand["authors"],
                                "book_genres":  cand["genres"],
                                "isbn":         cand.get("isbn", ""),
                            }
                        else:
                            with st.spinner("日本語タイトル取得中..."):
                                ja_title = fetch_tmdb_ja_title(cand["id"], media_type)
                            st.session_state.confirm_reg = {
                                "tmdb_id":      cand["id"],
                                "cover_url":    cover_url,
                                "tmdb_release": tmdb_release,
                                "media_type":   media_type,
                                "cand_en":      cand_en,
                                "jp_input":     ja_title or jp_input,
                            }
                        st.rerun()

        # 一括登録ボタン
        checked_indices = [i for i, v in st.session_state.bulk_checked.items() if v]
        if checked_indices:
            st.divider()
            st.caption(f"✅ {len(checked_indices)} 件選択中")
            if st.button(f"📥 {len(checked_indices)} 件を一括登録", type="primary", key="bulk_register"):
                if not st.session_state.pages_loaded:
                    with st.spinner("Notionデータ取得中..."):
                        all_pages = load_notion_data()
                        st.session_state.pages        = filter_target_pages(all_pages)
                        st.session_state.pages_loaded = True
                watched_str = watched_date.isoformat() if watched_date else None
                success_count = 0
                with st.spinner(f"一括登録中... (0/{len(checked_indices)})"):
                    prog = st.progress(0)
                    for n, i in enumerate(checked_indices):
                        cand = results_list[i]
                        if media_label == "書籍":
                            c_cover    = cand["cover_url"]
                            c_release  = cand.get("published", "")
                            c_jp       = cand["title"]
                            c_en       = ""
                            c_details  = {
                                "genres":   cand.get("genres", []),
                                "cast":     "",
                                "director": clean_author_list(cand.get("authors", [])),
                                "score": None,
                            }
                            c_tmdb_id  = 0
                            c_media    = "book"
                        else:
                            c_cover    = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{cand['poster_path']}"
                            c_release  = cand.get("release_date") or cand.get("first_air_date") or ""
                            c_en       = cand.get("title") or cand.get("name", "")
                            c_jp       = fetch_tmdb_ja_title(cand["id"], cand.get("media_type","movie")) or c_en
                            c_details  = fetch_tmdb_details(cand["id"], cand.get("media_type","movie"))
                            c_tmdb_id  = cand["id"]
                            c_media    = cand.get("media_type", "movie")
                        ok = create_notion_page(
                            jp_title=c_jp, en_title=c_en,
                            media_type_label=media_label,
                            tmdb_id=c_tmdb_id, media_type=c_media,
                            cover_url=c_cover, tmdb_release=c_release,
                            details=c_details, wlflg=wlflg,
                            watched_date=watched_str,
                            rating=rating_sel if rating_sel else None,
                        )
                        if ok:
                            save_to_drive(c_cover, c_jp or c_en, c_tmdb_id)
                            success_count += 1
                        prog.progress((n + 1) / len(checked_indices))
                        time.sleep(0.3)
                st.success(f"✅ {success_count}/{len(checked_indices)} 件登録完了！")
                st.session_state.new_search_results = []
                st.session_state.new_search_done    = False
                st.session_state.bulk_checked       = {}
                time.sleep(1.5)
                st.rerun()
    st.stop()

# ============================================================
# データ未取得ガード
# ============================================================
if not st.session_state.pages_loaded:
    st.info("👈 サイドバーの「Notionデータ取得」ボタンを押してください")
    st.stop()

target_pages = st.session_state.pages

def get_display_pages():
    if sync_scope == "未設定のみ更新":
        base = [p for p in target_pages if is_incomplete(p)]
    else:
        base = target_pages
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
        sync_targets = st.session_state.all_pages if st.session_state.all_pages else target_pages
    else:
        sync_targets = get_display_pages()
    label_mode   = "🔄 リフレッシュ" if is_refresh else "⚙️ 自動同期"

    with st.status(f"{label_mode}中... 0 / {len(sync_targets)} 件", expanded=True) as status:
        pbar, count = st.progress(0), 0
        success_log: list[str] = []
        maintain_log: list[str] = []
        error_log:   list[str] = []

        for i, item in enumerate(sync_targets):
            if not st.session_state.is_running:
                break

            props     = item["properties"]
            log_title, jp, en = get_title(props)
            notion_ok_now, drive_ok_now = get_diff_status(item)
            is_movie_drama = any(
                m["name"] in ["映画", "ドラマ"]
                for m in props.get("媒体", {}).get("multi_select", [])
            )
            if is_refresh and not is_movie_drama:
                # 映画・ドラマ以外: アイコン更新 + クリエイター名正規化（ISBN有りのみ）
                media_labels = [m["name"] for m in props.get("媒体", {}).get("multi_select", [])]
                media_label_val = media_labels[0] if media_labels else None
                icon_url = get_media_icon_url(media_label_val) if media_label_val else None
                patch_body = {}
                if icon_url:
                    patch_body["icon"] = {"type": "external", "external": {"url": icon_url}}

                # ISBNが存在する書籍はクリエイター名を正規化
                isbn_val = "".join(t["plain_text"] for t in props.get("ISBN", {}).get("rich_text", []))
                if isbn_val:
                    raw_creator = "".join(t["plain_text"] for t in props.get("クリエイター", {}).get("rich_text", []))
                    if raw_creator:
                        cleaned = " / ".join(clean_author(a) for a in raw_creator.split("/") if a.strip())
                        if cleaned != raw_creator:
                            patch_body.setdefault("properties", {})["クリエイター"] = {
                                "rich_text": [{"type": "text", "text": {"content": cleaned}}]
                            }

                if patch_body:
                    api_request("patch", f"https://api.notion.com/v1/pages/{item['id']}",
                                headers=NOTION_HEADERS, json=patch_body)
                msg = f"🎨 アイコン更新: {log_title}"
                st.write(msg)
                success_log.append(msg)
                count += 1
                pbar.progress((i + 1) / len(sync_targets))
                status.update(label=f"{label_mode}中... {i + 1} / {len(sync_targets)} 件", state="running")
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
                    error_log.append(msg)
                    pbar.progress((i + 1) / len(sync_targets))
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
                    pbar.progress((i + 1) / len(sync_targets))
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
                    pbar.progress((i + 1) / len(sync_targets))
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
                    count += 1
                else:
                    fail_parts = []
                    if need_notion and not n_ok: fail_parts.append("Notion更新失敗")
                    if need_drive  and not d_ok: fail_parts.append("Drive保存失敗")
                    if not meta_ok:              fail_parts.append("メタデータ失敗")
                    msg = f"❌ {log_title}（{' / '.join(fail_parts)}）"
                    st.write(msg)
                    error_log.append(msg)

            except Exception as e:
                msg = f"⚠️ エラー: {log_title}（{e}）"
                st.write(msg)
                error_log.append(msg)

            pbar.progress((i + 1) / len(sync_targets))
            status.update(label=f"{label_mode}中... {i + 1} / {len(sync_targets)} 件", state="running")
            time.sleep(0.1)

        status.update(
            label=f"{label_mode}完了！✅ {len(success_log)}件　⏸️ {len(maintain_log)}件　❌ {len(error_log)}件",
            state="complete"
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
            for msg in error_log:
                st.write(msg)
    if not error_log:
        st.success("すべて正常に処理されました ✅")

    st.session_state.is_running = False

# ============================================================
# 手動確認モード
# ============================================================
if mode == "手動確認":
    display_pages = get_display_pages()

    st.subheader(f"🛠 手動修正　表示: {len(display_pages)} 件 / 全 {len(target_pages)} 件")
    if diff_filter != "フィルタなし":
        st.caption(f"差分フィルタ適用中: {diff_filter}")

    search_query = st.text_input(
        "🔎 タイトルで絞り込む",
        placeholder="日本語・英語どちらでも可（部分一致）",
        key="manual_search_query",
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

        with st.expander(f"{diff_badge(item)}  {log_title}"):
            col_s1, col_s2, col_s3 = st.columns(3)
            col_s1.metric("Notionカバー", "登録済" if notion_ok_now else "未登録")
            col_s2.metric("Drive画像",   "あり"   if drive_ok_now  else "なし")
            col_s3.metric("TMDB_ID", str(saved_tmdb_id) if saved_tmdb_id else "未登録")

            current_url = get_current_notion_url(item)
            if current_url:
                st.caption(f"現在のURL: `{current_url}`")

            st.caption("🔧 TMDB_ID / MEDIA_TYPE を手動で修正")
            id_col, type_col, save_col = st.columns([2, 2, 1])
            new_tmdb_id = id_col.number_input(
                "TMDB_ID",
                value=int(saved_tmdb_id) if saved_tmdb_id else 0,
                min_value=0,
                step=1,
                key=f"tmdb_id_input_{page_id}",
            )
            new_media_type = type_col.selectbox(
                "MEDIA_TYPE",
                options=["movie", "tv"],
                index=0 if saved_media_type != "tv" else 1,
                key=f"media_type_input_{page_id}",
            )
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
                                            p["properties"]["MEDIA_TYPE"] = {"multi_select": [{"name": new_media_type}]}
                                    time.sleep(1.5)
                                    st.rerun()
                                else:
                                    st.error("一部失敗: " + "　".join(parts))
                            else:
                                st.error("TMDBでIDが見つかりませんでした")
                    else:
                        st.warning("TMDB_IDを入力してください")

            st.divider()

            default_query = re.sub(r'[Ss]eason\s*\d+', '', en if en else jp).strip()
            search_col, btn_col = st.columns([4, 1])
            custom_query = search_col.text_input(
                "🔍 検索ワード",
                value=default_query,
                key=f"custom_query_{page_id}",
                placeholder="英語タイトルで検索すると精度UP",
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
                                                p["properties"]["MEDIA_TYPE"] = {"multi_select": [{"name": media_type}]}
                                        time.sleep(1.5)
                                        st.rerun()
                                    else:
                                        st.error("一部失敗しました: " + "　".join(parts))
