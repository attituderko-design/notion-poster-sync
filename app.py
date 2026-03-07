import os
import re
import json
import requests
import time
import streamlit as st
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

# ============================================================
# 設定（secrets.toml から読み込み）
# ============================================================
NOTION_API_KEY = st.secrets["NOTION_API_KEY"]
NOTION_DB_ID   = st.secrets["NOTION_DB_ID"]
TMDB_API_KEY   = st.secrets["TMDB_API_KEY"]
DRIVE_FOLDER_ID = st.secrets["DRIVE_FOLDER_ID"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

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

def make_filename(title: str, tmdb_id) -> str:
    return f"{sanitize_filename(title)}_{tmdb_id}.jpg"

def get_title(props):
    jp = "".join([t["plain_text"] for t in props.get("タイトル", {}).get("title", [])])
    en = "".join([t["plain_text"] for t in props.get("International Title", {}).get("rich_text", [])])
    return (jp if jp else en), jp, en

def get_current_notion_url(item) -> str | None:
    cover = item.get("cover")
    if cover and cover.get("type") == "external":
        return cover.get("external", {}).get("url")
    return None

def is_incomplete(page) -> bool:
    props = page["properties"]
    if not page.get("cover"):
        return True
    if not props.get("TMDB_ID", {}).get("number"):
        return True
    if not props.get("ジャンル", {}).get("multi_select"):
        return True
    if not props.get("出演者・主催", {}).get("rich_text"):
        return True
    if not props.get("監督・指揮者", {}).get("rich_text"):
        return True
    return False

# ============================================================
# Drive ファイル一覧（session_stateで管理）
# ============================================================

def get_drive_files() -> dict:
    """session_stateからDriveファイル一覧を取得。なければ取得してキャッシュ。"""
    if "drive_files_cache" not in st.session_state:
        refresh_drive_files()
    return st.session_state.drive_files_cache

def refresh_drive_files():
    """Driveファイル一覧を強制再取得してsession_stateに保存。"""
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

def save_to_drive(cover_url: str, title: str, tmdb_id) -> bool:
    try:
        img_url = cover_url.replace("w600_and_h900_bestv2", "original")
        img_res = api_request("get", img_url)
        if img_res is None or img_res.status_code != 200:
            return False

        service = get_drive_service()
        fname   = make_filename(title, tmdb_id)
        files   = get_drive_files()
        media   = MediaIoBaseUpload(io.BytesIO(img_res.content), mimetype="image/jpeg", resumable=False)

        if fname in files:
            service.files().update(fileId=files[fname], media_body=media).execute()
        else:
            service.files().create(
                body={"name": fname, "parents": [DRIVE_FOLDER_ID]},
                media_body=media,
                fields="id",
            ).execute()

        # session_stateのキャッシュに即時反映
        st.session_state.drive_files_cache[fname] = True
        return True

    except Exception as e:
        st.warning(f"Drive保存失敗 ({title}): {e}")
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
    return ("🟢N" if notion_ok else "🔴N") + " " + ("🟢D" if drive_ok else "🔴D")

# ============================================================
# APIリトライラッパー
# ============================================================

def api_request(method: str, url: str, max_retries: int = 3, **kwargs):
    fn = {"get": requests.get, "post": requests.post, "patch": requests.patch}[method]
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
        st.warning(f"TMDB_ID保存失敗 ({tmdb_id}): {res.status_code if res else 'None'} {res.text if res else ''}")
        return False
    return True

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

def fetch_tmdb_details(tmdb_id: int, media_type: str) -> dict:
    base      = "https://api.themoviedb.org/3"
    params_ja = {"api_key": TMDB_API_KEY, "language": "ja-JP"}

    detail_res = api_request("get", f"{base}/{media_type}/{tmdb_id}", params=params_ja)
    genres = []
    if detail_res and detail_res.status_code == 200:
        genres = [g["name"] for g in detail_res.json().get("genres", [])]

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

    # スコア取得（英語版で取得）
    score = None
    score_res = api_request("get", f"{base}/{media_type}/{tmdb_id}", params={"api_key": TMDB_API_KEY, "language": "en-US"})
    if score_res and score_res.status_code == 200:
        score = score_res.json().get("vote_average")

    return {
        "genres":   genres,
        "cast":     " / ".join(cast_names),
        "director": director_name,
        "score":    round(score, 1) if score else None,
    }

def update_notion_metadata(page_id: str, details: dict) -> bool:
    properties = {}
    if details["genres"]:
        properties["ジャンル"] = {"multi_select": [{"name": g} for g in details["genres"]]}
    if details["cast"]:
        properties["出演者・主催"] = {"rich_text": [{"type": "text", "text": {"content": details["cast"]}}]}
    if details["director"]:
        properties["監督・指揮者"] = {"rich_text": [{"type": "text", "text": {"content": details["director"]}}]}
    if details.get("score") is not None:
        properties["TMDB_score"] = {"number": details["score"]}
    if not properties:
        return True
    res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS, json={"properties": properties})
    return res is not None and res.status_code == 200

def update_notion_cover(page_id: str, cover_url: str, tmdb_release, existing_release) -> bool:
    payload = {"cover": {"type": "external", "external": {"url": cover_url}}}
    if tmdb_release and not existing_release:
        payload["properties"] = {"公開": {"date": {"start": tmdb_release}}}
    res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS, json=payload)
    return res is not None and res.status_code == 200

def build_meta_log(details: dict) -> str:
    parts = []
    if details.get("genres"):   parts.append(f"ジャンル: {' / '.join(details['genres'])}")
    if details.get("cast"):     parts.append(f"出演: {details['cast']}")
    if details.get("director"): parts.append(f"監督: {details['director']}")
    return "　".join(parts) if parts else "（取得データなし）"

def update_all(page_id, cover_url, tmdb_release, existing_release,
               title, tmdb_id, media_type, need_notion, need_drive) -> tuple:
    notion_ok = update_notion_cover(page_id, cover_url, tmdb_release, existing_release) if need_notion else True
    drive_ok  = save_to_drive(cover_url, title, tmdb_id) if need_drive else True
    save_tmdb_id_to_notion(page_id, tmdb_id, media_type)
    meta_ok, meta_log = False, "（取得失敗）"
    try:
        details  = fetch_tmdb_details(tmdb_id, media_type)
        meta_ok  = update_notion_metadata(page_id, details)
        meta_log = build_meta_log(details)
    except Exception as e:
        st.warning(f"メタデータ更新失敗 ({title}): {e}")
    return notion_ok, drive_ok, meta_ok, meta_log

# ============================================================
# アプリ初期化
# ============================================================

st.set_page_config(page_title="Notion Movie Master", page_icon="🎬", layout="wide")
st.title("🎬 Notion ポスター同期")

for key, default in {
    "is_running":     False,
    "pages_loaded":   False,
    "pages":          [],
    "search_results": {},
    "tmdb_id_cache":  {},
    "manual_page":    0,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.header("操作パネル")

    if st.button("📥 Notionデータ取得", use_container_width=True):
        with st.spinner("Notionからデータ取得中..."):
            all_pages = load_notion_data()
            st.session_state.pages          = filter_target_pages(all_pages)
            st.session_state.pages_loaded   = True
            st.session_state.search_results = {}
            st.session_state.manual_page    = 0
            # Driveキャッシュも同時にリフレッシュ
            refresh_drive_files()
        st.success(f"{len(st.session_state.pages)} 件取得しました")

    st.divider()
    st.header("動作モード")
    mode       = st.radio("モード",   ["手動確認", "自動同期"])
    sync_scope = st.radio("同期範囲", ["未設定のみ更新", "全件走査"])

    st.divider()
    st.header("差分フィルタ")
    st.caption("🟢=登録済　🔴=未登録　N=Notion　D=Drive")
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

    if mode == "自動同期":
        st.divider()
        if st.button("🚀 自動同期開始", use_container_width=True, disabled=not st.session_state.pages_loaded):
            st.session_state.is_running = True
            st.rerun()
        if st.button("⏹ 停止", use_container_width=True):
            st.session_state.is_running = False
            st.rerun()

    st.divider()
    confirm_delete = st.checkbox("カバー全削除を許可")
    delete_btn = st.button("🗑 カバー全削除", disabled=not confirm_delete or not st.session_state.pages_loaded)

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
# 1. カバー全削除
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
# 2. 自動同期
# ============================================================
if mode == "自動同期" and st.session_state.is_running:
    sync_targets = get_display_pages()

    with st.status(f"⚙️ 自動同期中... 対象 {len(sync_targets)} 件", expanded=True) as status:
        pbar, count = st.progress(0), 0
        error_log: list[str] = []

        for i, item in enumerate(sync_targets):
            if not st.session_state.is_running:
                break

            props     = item["properties"]
            log_title, jp, en = get_title(props)
            notion_ok_now, drive_ok_now = get_diff_status(item)
            need_notion, need_drive = resolve_needs(notion_ok_now, drive_ok_now)

            date_prop        = props.get("公開", {}).get("date")
            existing_release = date_prop.get("start") if date_prop else None
            query            = en if en else jp

            try:
                saved_tmdb_id, saved_media_type = get_tmdb_id_from_notion(props)

                if saved_tmdb_id and saved_media_type:
                    top = fetch_tmdb_by_id(saved_tmdb_id, saved_media_type)
                    src = "🆔 ID参照"
                else:
                    results = search_tmdb(query, existing_release[:4] if existing_release else None)
                    top     = results[0] if results else None
                    src     = "🔍 検索"

                if not top:
                    st.write(f"候補なし ({src}): {log_title}")
                    error_log.append(f"候補なし ({src}): {log_title}")
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

                if url_matched and not need_drive:
                    st.write(f"⏸️ 維持(OK): {log_title}")
                    pbar.progress((i + 1) / len(sync_targets))
                    time.sleep(0.1)
                    continue

                if url_matched and need_drive:
                    d_ok = save_to_drive(cover_url, log_title, tmdb_id)
                    save_tmdb_id_to_notion(item["id"], tmdb_id, media_type)
                    # メタデータは空のときだけ更新
                    need_meta = (
                        not props.get("ジャンル", {}).get("multi_select")
                        or not props.get("出演者・主催", {}).get("rich_text")
                        or not props.get("監督・指揮者", {}).get("rich_text")
                    )
                    meta_ok, meta_log = True, ""
                    if need_meta:
                        meta_ok, meta_log = False, "（取得失敗）"
                        try:
                            details  = fetch_tmdb_details(tmdb_id, media_type)
                            meta_ok  = update_notion_metadata(item["id"], details)
                            meta_log = build_meta_log(details)
                        except Exception:
                            pass
                    label = "📥 補充" if d_ok else "❌ Drive保存失敗"
                    meta_label = f"　メタ {'✅' if meta_ok else '❌'}" if need_meta else ""
                    st.write(f"{label} {'✅' if d_ok else '❌'}{meta_label}: {log_title}")
                    if need_meta and meta_ok:
                        st.caption(f"　　↳ {meta_log}")
                    if d_ok:
                        count += 1
                    else:
                        error_log.append(f"❌ Drive保存失敗: {log_title}")
                    pbar.progress((i + 1) / len(sync_targets))
                    time.sleep(0.1)
                    continue

                n_ok, d_ok, meta_ok, meta_log = update_all(
                    item["id"], cover_url, tmdb_release, existing_release,
                    log_title, tmdb_id, media_type, need_notion, need_drive,
                )
                parts = []
                if need_notion: parts.append("Notion " + ("✅" if n_ok else "❌"))
                if need_drive:  parts.append("Drive "  + ("✅" if d_ok else "❌"))
                parts.append("メタ " + ("✅" if meta_ok else "❌"))
                st.write(f"{log_title}　{src}　{'　'.join(parts)}")
                if meta_ok:
                    st.caption(f"　　↳ {meta_log}")
                if n_ok and d_ok and meta_ok:
                    count += 1
                else:
                    fail_parts = []
                    if need_notion and not n_ok: fail_parts.append("Notion更新失敗")
                    if need_drive  and not d_ok: fail_parts.append("Drive保存失敗")
                    if not meta_ok:              fail_parts.append("メタデータ失敗")
                    error_log.append(f"❌ {log_title}（{' / '.join(fail_parts)}）")

            except Exception as e:
                st.write(f"⚠️ エラー ({log_title}): {e}")
                error_log.append(f"⚠️ エラー: {log_title}（{e}）")

            pbar.progress((i + 1) / len(sync_targets))
            time.sleep(0.1)

        status.update(label=f"自動同期完了！計 {count} 件更新　失敗 {len(error_log)} 件", state="complete")
        if error_log:
            st.error(f"**⚠️ 要確認リスト（{len(error_log)} 件）**")
            for msg in error_log:
                st.write(msg)
        else:
            st.success("すべて正常に処理されました ✅")

    st.session_state.is_running = False

# ============================================================
# 3. 手動確認モード
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

            # ── TMDB_ID / MEDIA_TYPE 手動編集フォーム ──
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
                        ok = save_tmdb_id_to_notion(page_id, new_tmdb_id, new_media_type)
                        if ok:
                            st.success("保存しました！")
                            for p in st.session_state.pages:
                                if p["id"] == page_id:
                                    p["properties"]["TMDB_ID"]    = {"number": new_tmdb_id}
                                    p["properties"]["MEDIA_TYPE"] = {"multi_select": [{"name": new_media_type}]}
                    else:
                        st.warning("TMDB_IDを入力してください")

            st.divider()

            # ── 候補検索・表示 ──
            default_query = en if en else jp
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
                                    st.markdown(
                                        '<div style="border: 3px solid red; padding: 4px; border-radius: 6px;">',
                                        unsafe_allow_html=True,
                                    )
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
                                    date_prop        = props.get("公開", {}).get("date")
                                    existing_release = date_prop.get("start") if date_prop else None
                                    media_type       = cand.get("media_type", "movie")
                                    need_notion, need_drive = resolve_needs(notion_ok_now, drive_ok_now)
                                    if url_match and need_notion:
                                        need_notion = False
                                        need_drive  = True
                                    st.session_state.tmdb_id_cache[page_id] = tmdb_id
                                    n_ok, d_ok, meta_ok, meta_log = update_all(
                                        page_id, cover_url, tmdb_release, existing_release,
                                        log_title, tmdb_id, media_type, need_notion, need_drive,
                                    )
                                    parts = []
                                    if need_notion: parts.append("Notion " + ("✅" if n_ok else "❌失敗"))
                                    if need_drive:  parts.append("Drive "  + ("✅" if d_ok else "❌失敗"))
                                    parts.append("メタ " + ("✅" if meta_ok else "❌失敗"))
                                    if n_ok and d_ok and meta_ok:
                                        st.success("保存完了！ " + "　".join(parts))
                                        st.caption(f"↳ {meta_log}")
                                        st.session_state.search_results.pop(page_id, None)
                                        if need_notion and n_ok:
                                            for p in st.session_state.pages:
                                                if p["id"] == page_id:
                                                    p["cover"] = {"type": "external", "external": {"url": cover_url}}
                                        time.sleep(0.5)
                                        st.rerun()
                                    else:
                                        st.error("一部失敗しました: " + "　".join(parts))
