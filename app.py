import os
import re
import json
import requests
import time
import streamlit as st
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

# ============================================================
# 設定（secrets.toml から読み込み）
# ============================================================
NOTION_API_KEY = st.secrets["NOTION_API_KEY"]
NOTION_DB_ID   = st.secrets["NOTION_DB_ID"]
TMDB_API_KEY   = st.secrets["TMDB_API_KEY"]
DRIVE_FOLDER_ID = st.secrets["DRIVE_FOLDER_ID"]  # postersフォルダのID

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ============================================================
# Google Drive API クライアント（キャッシュして使い回す）
# ============================================================
@st.cache_resource
def get_drive_service():
    """サービスアカウントJSONからDrive APIクライアントを生成"""
    sa_info = json.loads(st.secrets["GCP_SERVICE_ACCOUNT_JSON"])
    creds   = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)

# ============================================================
# ユーティリティ
# ============================================================

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', "_", name)

def make_filename(title: str, tmdb_id) -> str:
    """保存ファイル名: タイトル_tmdbid.jpg"""
    return f"{sanitize_filename(title)}_{tmdb_id}.jpg"

def get_title(props):
    """(表示用タイトル, 日本語, 英語) を返す"""
    jp = "".join([t["plain_text"] for t in props.get("タイトル", {}).get("title", [])])
    en = "".join([t["plain_text"] for t in props.get("International Title", {}).get("rich_text", [])])
    return (jp if jp else en), jp, en

def get_current_notion_url(item) -> str | None:
    cover = item.get("cover")
    if cover and cover.get("type") == "external":
        return cover.get("external", {}).get("url")
    return None

# ============================================================
# Drive ファイル操作
# ============================================================

@st.cache_data(ttl=300)
def list_drive_files() -> dict:
    """
    postersフォルダ内のファイル一覧を {ファイル名: file_id} で返す。
    5分キャッシュ。
    """
    service = get_drive_service()
    results = service.files().list(
        q=f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
        fields="files(id, name)",
        pageSize=1000,
    ).execute()
    return {f["name"]: f["id"] for f in results.get("files", [])}

def drive_exists(title: str, tmdb_id) -> bool:
    """Drive上に タイトル_tmdbid.jpg が存在するか"""
    files = list_drive_files()
    return make_filename(title, tmdb_id) in files

def drive_exists_fuzzy(title: str) -> bool:
    """tmdb_id不明時: タイトル_*.jpg が1つでもあればOK"""
    prefix = sanitize_filename(title) + "_"
    files  = list_drive_files()
    return any(name.startswith(prefix) and name.endswith(".jpg") for name in files)

def save_to_drive(cover_url: str, title: str, tmdb_id) -> bool:
    """Drive の postersフォルダに タイトル_tmdbid.jpg を最高画質で保存"""
    try:
        img_url = cover_url.replace("w600_and_h900_bestv2", "original")
        img_res = api_request("get", img_url)
        if img_res is None or img_res.status_code != 200:
            return False

        service   = get_drive_service()
        fname     = make_filename(title, tmdb_id)
        files     = list_drive_files()

        media = MediaIoBaseUpload(
            io.BytesIO(img_res.content),
            mimetype="image/jpeg",
            resumable=False,
        )
        if fname in files:
            # 既存ファイルを上書き更新
            service.files().update(
                fileId=files[fname],
                media_body=media,
                supportsAllDrives=True,
            ).execute()
        else:
            # 新規アップロード
            file_metadata = service.files().create(
                body={"name": fname, "parents": [DRIVE_FOLDER_ID]},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()

            # オーナーをあなたのアカウントに移譲
            owner_email = st.secrets.get("OWNER_EMAIL", "")
            if owner_email:
                try:
                    service.permissions().create(
                        fileId=file_metadata["id"],
                        transferOwnership=True,
                        body={
                            "type": "user",
                            "role": "owner",
                            "emailAddress": owner_email,
                        },
                        supportsAllDrives=True,
                    ).execute()
                except Exception as perm_err:
                    st.warning(f"オーナー移譲失敗 ({title}): {perm_err}")

        # キャッシュを破棄して次回取得時に最新化
        list_drive_files.clear()
        return True

    except Exception as e:
        st.warning(f"Drive保存失敗 ({title}): {e}")
        return False

# ============================================================
# 差分判定
# ============================================================

def get_diff_status(item) -> tuple:
    """(notion_has_cover: bool, drive_has_file: bool) を返す"""
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
    """
    - 429: Retry-After 待機してリトライ
    - 5xx: 指数バックオフ (2^n 秒) でリトライ
    - 例外: 指数バックオフでリトライ
    戻り値: Response or None（全リトライ失敗）
    """
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
        if any(
            m["name"] in ["映画", "ドラマ"]
            for m in p["properties"].get("媒体", {}).get("multi_select", [])
        )
    ]

def get_tmdb_id_from_notion(props) -> tuple:
    """
    NotionプロパティからTMDB_IDとMEDIA_TYPEを取得する。
    戻り値: (tmdb_id: int|None, media_type: str|None)
    """
    tmdb_id_val = props.get("TMDB_ID", {}).get("number")
    media_type_val = props.get("MEDIA_TYPE", {}).get("select", {})
    media_type = media_type_val.get("name") if media_type_val else None
    return (int(tmdb_id_val) if tmdb_id_val else None), media_type

def save_tmdb_id_to_notion(page_id: str, tmdb_id: int, media_type: str) -> bool:
    """TMDB_IDとMEDIA_TYPEをNotionに保存する"""
    res = api_request(
        "patch",
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": {
            "TMDB_ID":    {"number": tmdb_id},
            "MEDIA_TYPE": {"select": {"name": media_type}},
        }},
    )
    return res is not None and res.status_code == 200

def fetch_tmdb_by_id(tmdb_id: int, media_type: str) -> dict | None:
    """
    TMDB_IDで直接ポスター情報を取得する。
    戻り値: TMDBのレスポンスdict（poster_pathなければNone）
    """
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
    # search_tmdb と同じ形式に合わせる
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
    """
    TMDBからジャンル・キャスト・監督を日本語で取得する。
    戻り値: {
        "genres":   ["アクション", "SF", ...],
        "cast":     "山田太郎 / 田中花子 / 鈴木一郎",
        "director": "スティーブン・スピルバーグ",
    }
    """
    base = "https://api.themoviedb.org/3"
    params_ja = {"api_key": TMDB_API_KEY, "language": "ja-JP"}

    # 基本情報（ジャンル）
    detail_res = api_request("get", f"{base}/{media_type}/{tmdb_id}", params=params_ja)
    genres = []
    if detail_res and detail_res.status_code == 200:
        genres = [g["name"] for g in detail_res.json().get("genres", [])]

    # クレジット（キャスト・監督）
    credit_endpoint = "credits" if media_type == "movie" else "aggregate_credits"
    credit_res = api_request("get", f"{base}/{media_type}/{tmdb_id}/{credit_endpoint}", params=params_ja)
    cast_names, director_name = [], ""

    if credit_res and credit_res.status_code == 200:
        data = credit_res.json()

        # キャスト（主演3名）: 日本語名があれば使う
        for member in data.get("cast", [])[:3]:
            name = member.get("name", "")
            cast_names.append(name)

        # 監督
        if media_type == "movie":
            for member in data.get("crew", []):
                if member.get("job") == "Director":
                    director_name = member.get("name", "")
                    break
        else:
            # ドラマは created_by から取得
            tv_res = api_request("get", f"{base}/tv/{tmdb_id}", params=params_ja)
            if tv_res and tv_res.status_code == 200:
                creators = tv_res.json().get("created_by", [])
                if creators:
                    director_name = creators[0].get("name", "")

    return {
        "genres":   genres,
        "cast":     " / ".join(cast_names),
        "director": director_name,
    }

def update_notion_metadata(page_id: str, details: dict) -> bool:
    """ジャンル・出演者・監督をNotionに上書き更新する"""
    properties = {}

    if details["genres"]:
        properties["ジャンル"] = {
            "multi_select": [{"name": g} for g in details["genres"]]
        }
    if details["cast"]:
        properties["出演者・主催"] = {
            "rich_text": [{"type": "text", "text": {"content": details["cast"]}}]
        }
    if details["director"]:
        properties["監督・指揮者"] = {
            "rich_text": [{"type": "text", "text": {"content": details["director"]}}]
        }

    if not properties:
        return True  # 更新するものがなければスキップ

    res = api_request(
        "patch",
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=NOTION_HEADERS,
        json={"properties": properties},
    )
    return res is not None and res.status_code == 200

def update_notion_cover(page_id: str, cover_url: str, tmdb_release, existing_release) -> bool:
    payload = {"cover": {"type": "external", "external": {"url": cover_url}}}
    if tmdb_release and not existing_release:
        payload["properties"] = {"公開": {"date": {"start": tmdb_release}}}
    res = api_request("patch", f"https://api.notion.com/v1/pages/{page_id}",
                      headers=NOTION_HEADERS, json=payload)
    return res is not None and res.status_code == 200

def build_meta_log(details: dict) -> str:
    """更新したメタデータの内容を1行のログ文字列にまとめる"""
    parts = []
    if details.get("genres"):
        parts.append(f"ジャンル: {' / '.join(details['genres'])}")
    if details.get("cast"):
        parts.append(f"出演: {details['cast']}")
    if details.get("director"):
        parts.append(f"監督: {details['director']}")
    return "　".join(parts) if parts else "（取得データなし）"

def update_all(page_id, cover_url, tmdb_release, existing_release,
               title, tmdb_id, media_type, need_notion, need_drive) -> tuple:
    """
    need_notion / need_drive フラグに応じて必要な更新を実行。
    メタデータ（ジャンル・出演者・監督）・TMDB_IDは常に更新。
    戻り値: (notion_ok, drive_ok, meta_ok, meta_log)
    """
    notion_ok = update_notion_cover(page_id, cover_url, tmdb_release, existing_release) if need_notion else True
    drive_ok  = save_to_drive(cover_url, title, tmdb_id) if need_drive else True

    # TMDB_ID / MEDIA_TYPE を常に保存（ID中心設計）
    save_tmdb_id_to_notion(page_id, tmdb_id, media_type)

    # メタデータは常に上書き更新
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
        if st.button("🚀 自動同期開始", use_container_width=True,
                     disabled=not st.session_state.pages_loaded):
            st.session_state.is_running = True
            st.rerun()
        if st.button("⏹ 停止", use_container_width=True):
            st.session_state.is_running = False
            st.rerun()

    st.divider()
    confirm_delete = st.checkbox("カバー全削除を許可")
    delete_btn = st.button(
        "🗑 カバー全削除",
        disabled=not confirm_delete or not st.session_state.pages_loaded,
    )

# ============================================================
# データ未取得ガード
# ============================================================
if not st.session_state.pages_loaded:
    st.info("👈 サイドバーの「Notionデータ取得」ボタンを押してください")
    st.stop()

target_pages = st.session_state.pages

def get_display_pages():
    base = [p for p in target_pages if not p.get("cover")] if sync_scope == "未設定のみ更新" else target_pages
    return apply_diff_filter(base, diff_filter)

def resolve_needs(notion_ok_now, drive_ok_now):
    if diff_filter == "Notionのみ更新（Driveあり・Notionカバーなし）": return True, False
    if diff_filter == "Driveのみ更新（Notionカバーあり・Driveなし）":  return False, True
    if sync_scope  == "全件走査": return True, True
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
                # --- TMDB_IDがあればID直接取得、なければ検索 ---
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
                    meta_ok, meta_log = False, "（取得失敗）"
                    try:
                        details  = fetch_tmdb_details(tmdb_id, media_type)
                        meta_ok  = update_notion_metadata(item["id"], details)
                        meta_log = build_meta_log(details)
                    except Exception:
                        pass
                    label = "📥 補充" if d_ok else "❌ Drive保存失敗"
                    st.write(f"{label} {'✅' if d_ok else '❌'}　メタ {'✅' if meta_ok else '❌'}: {log_title}")
                    if meta_ok:
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

        with st.expander(f"{diff_badge(item)}  {log_title}"):
            col_s1, col_s2, col_s3 = st.columns(3)
            col_s1.metric("Notionカバー", "登録済" if notion_ok_now else "未登録")
            col_s2.metric("Drive画像",   "あり"   if drive_ok_now  else "なし")

            # TMDB_ID表示
            saved_tmdb_id, saved_media_type = get_tmdb_id_from_notion(props)
            col_s3.metric("TMDB_ID", str(saved_tmdb_id) if saved_tmdb_id else "未登録")

            current_url = get_current_notion_url(item)
            if current_url:
                st.caption(f"現在のURL: `{current_url}`")

            btn_label = "🆔 ID参照で取得" if saved_tmdb_id else "🔍 候補を検索"
            if st.button(btn_label, key=f"search_{page_id}"):
                date_prop        = props.get("公開", {}).get("date")
                existing_release = date_prop.get("start") if date_prop else None
                query            = en if en else jp
                try:
                    if saved_tmdb_id and saved_media_type:
                        # ID直接取得（1件確定）
                        top = fetch_tmdb_by_id(saved_tmdb_id, saved_media_type)
                        st.session_state.search_results[page_id] = [top] if top else []
                    else:
                        results = search_tmdb(query, existing_release[:4] if existing_release else None)
                        st.session_state.search_results[page_id] = results[:3]
                except Exception as e:
                    st.error(f"検索エラー: {e}")
                    st.session_state.search_results[page_id] = []

            candidates = st.session_state.search_results.get(page_id)
            if candidates is not None:
                if not candidates:
                    st.warning("候補が見つかりませんでした")
                else:
                    cols = st.columns(3)
                    for idx, cand in enumerate(candidates):
                        with cols[idx]:
                            tmdb_id      = cand["id"]
                            cover_url    = f"https://image.tmdb.org/t/p/w600_and_h900_bestv2{cand['poster_path']}"
                            tmdb_release = cand.get("release_date") or cand.get("first_air_date") or "不明"
                            url_match    = (current_url == cover_url)
                            st.image(cover_url)
                            st.caption(
                                f"{'✅ 現在と同じURL ' if url_match else ''}"
                                f"{cand.get('title') or cand.get('name', '?')} "
                                f"({cand.get('media_type','?')}) {tmdb_release}"
                            )
                            if st.button("✅ 決定", key=f"sel_{page_id}_{idx}"):
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
